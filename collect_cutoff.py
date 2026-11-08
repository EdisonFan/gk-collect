# -*- coding: utf-8 -*-
"""CNB 流水线生产采集器 v4：院校投档线 score/province（补齐版）。

与本地 collect_proxy.py 完全错开，互不抢数据、互不抢代理：
  本地  = 按 2970 校遍历 plan/school、score/special
  CNB   = 按 省×年 拉 score/province 全量院校投档线（对应 T08 admission_cutoff）

接口实测约束：
  - size 只能 20；单维度查询硬翻页上限 200 页 / 4000 条，201 页起返回空
  - 可用筛选参数：local_type_name(科类)、local_batch_name(批次)、province_id(院校所在省)
  - 其余参数(nature_name/level_name/sg_type/sort 等)被接口忽略

v4 相对 v3 的修正：
  1. 三级拆分：(type) → (type,batch) → (type,batch,province_id)，
     解决 v3 里「物理类/本科批 4249 条」超上限被整块跳过的问题。
  2. 失败页补采：页任务跑完后对失败页再跑 2 轮（换代理起点），
     解决 v3 里 43~99 页因代理连接失败/限流丢失的问题。
  3. 合并上轮结果：读 out/prev.json（.cnb.yml 从上轮分支 fetch 得到），
     两轮数据按记录内容哈希去重，覆盖率叠加。
  4. probe 并发化，避免 34 个 province_id 串行探测拖慢。

环境变量：PROV(13/41/44/37) YEAR(2021..2026)
输出：out/cutoff_{PROV}_{YEAR}.json，由 .cnb.yml 强制覆盖 push 到独立分支。
仅用 Python 标准库；代理来自仓库内 proxies_cnb.txt（CNB 专属，与本地池零交集）。
"""
import os, json, time, random, threading, hashlib, urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

U = 'https://api.zjzw.cn/web/api/'
H = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Origin': 'https://gkcx.eol.cn',
    'Referer': 'https://gkcx.eol.cn/',
    'Accept': 'application/json,text/plain,*/*',
    'Content-Type': 'application/json',
}
PROV = os.environ.get('PROV', '13')
YEAR = int(os.environ.get('YEAR', '2026'))
SIZE = 20
PAGE_CAP = 200
ITEM_CAP = PAGE_CAP * SIZE
TIMEOUT = 3
MAX_RETRY = 4
WORKERS = 8
API = 'apidata/api/gk/score/province'
CANDIDATE_TYPES = ['物理类', '历史类', '文科', '理科', '综合', '理工', '文史',
                   '不分文理', '艺术类', '体育类', '物理科目组合', '历史科目组合']
PROVINCE_IDS = ['11', '12', '13', '14', '15', '21', '22', '23', '31', '32',
                '33', '34', '35', '36', '37', '41', '42', '43', '44', '45',
                '46', '50', '51', '52', '53', '54', '61', '62', '63', '64',
                '65', '71', '81', '82']
# 批次候选兜底：样本前 30 页常常只覆盖「本科批」，不补齐会漏掉同科类的专科/提前批
CANDIDATE_BATCHES = [
    '本科批', '专科批', '本科提前批', '专科提前批',
    '本科批A段', '本科批B段', '本科批C段', '专科批A段', '专科批B段', '专科批C段',
    '本科提前批A段', '本科提前批B段', '本科提前批C段',
    '本科一批', '本科二批', '本科三批', '高职专科批', '高职(专科)批',
    '普通类一段', '普通类二段', '提前批A类', '提前批B类', '提前批C类',
    '国家专项计划批', '地方专项计划批', '高校专项计划批', '特殊类型批',
    '本科普通批', '专科普通批', '提前批', '艺术类本科批', '体育类本科批',
]


def opener(proxy):
    ph = urllib.request.ProxyHandler({'http': proxy, 'https': proxy} if proxy else {})
    return urllib.request.build_opener(ph)


def fetch(proxy, page, extra):
    q = {'uri': API, 'local_province_id': PROV, 'year': YEAR,
         'page': page, 'size': SIZE}
    q.update(extra)
    req = urllib.request.Request(U, data=json.dumps(q).encode(), headers=H, method='POST')
    with opener(proxy).open(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode('utf-8'))


def fetch_page(job, proxies, seed, stats, lock):
    """job = (page, extra, label)。返回 (page, label, items, numFound, ok)。"""
    page, extra, label = job
    tag = sum(ord(c) for c in label)
    n = len(proxies)
    start = (page * 7 + seed + tag) % n
    for k in range(MAX_RETRY):
        px = proxies[(start + k) % n]
        try:
            d = fetch(px, page, extra)
            code = str(d.get('code'))
            dat = d.get('data')
            if code == '0000' and isinstance(dat, dict):
                with lock:
                    stats['ok'] += 1
                return page, label, (dat.get('item') or []), dat.get('numFound'), True
            with lock:
                if code in ('1069', '1074') or (code == '0000' and not isinstance(dat, dict)):
                    stats['throttled'] += 1
                else:
                    stats['other'] += 1
            if code == '1069':
                time.sleep(0.3)
        except Exception:
            with lock:
                stats['conn_err'] += 1
    try:
        d = fetch(None, page, extra)
        if str(d.get('code')) == '0000' and isinstance(d.get('data'), dict):
            with lock:
                stats['direct'] += 1
            return page, label, (d['data'].get('item') or []), d['data'].get('numFound'), True
    except Exception:
        pass
    with lock:
        stats['failed_pages'] += 1
    return page, label, [], None, False


def run_jobs(jobs, proxies, seed, stats, lock, t0, phase):
    """并发跑页任务，返回 ({(label,page): items}, failed_jobs)。"""
    out, failed = {}, []
    total = len(jobs)
    if not total:
        return out, failed
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_page, j, proxies, seed, stats, lock): j for j in jobs}
        done = 0
        for f in as_completed(futs):
            p, label, batch, _, ok = f.result()
            if ok:
                out[(label, p)] = batch
            else:
                failed.append(futs[f])
            done += 1
            if done % 40 == 0 or done == total:
                cnt = sum(len(v) for v in out.values())
                print('  [%s] %d/%d 页 累计 %d 条 | ok=%d err=%d thr=%d 直连=%d 失败=%d | %.0fs' %
                      (phase, done, total, cnt, stats['ok'], stats['conn_err'],
                       stats['throttled'], stats['direct'], stats['failed_pages'],
                       time.time() - t0), flush=True)
    return out, failed


def probe(extra, label, proxies, seed, stats, lock):
    _, _, items, nf, ok = fetch_page((1, extra, label), proxies, seed, stats, lock)
    return (int(nf) if nf else 0), items, ok


def probe_all(specs, proxies, seed, stats, lock):
    """specs: [(extra,label)]，并发探测，返回 [(extra,label,n,items,ok)]。"""
    res = []
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(probe, e, l, proxies, seed, stats, lock): (e, l) for e, l in specs}
        for f in as_completed(futs):
            e, l = futs[f]
            n, items, ok = f.result()
            res.append((e, l, n, items, ok))
    return res


def main():
    proxies = [x.strip() for x in open('proxies_cnb.txt', encoding='utf-8') if x.strip()]
    random.seed('cnb-%s-%s' % (PROV, YEAR))
    random.shuffle(proxies)
    if not proxies:
        proxies = [None]
    seed = sum(ord(c) for c in '%s-%s' % (PROV, YEAR))
    lock = threading.Lock()
    stats = {'ok': 0, 'throttled': 0, 'other': 0, 'conn_err': 0,
             'direct': 0, 'failed_pages': 0}
    t0 = time.time()
    os.makedirs('out', exist_ok=True)
    print('=== CNB 投档线采集 v4 PROV=%s YEAR=%s 代理=%d 并发=%d 页上限=%d ===' %
          (PROV, YEAR, len(proxies), WORKERS, PAGE_CAP), flush=True)

    # ⓪ 上轮结果作为增量基础
    prev_items = []
    if os.path.exists('out/prev.json'):
        try:
            prev_items = json.load(open('out/prev.json', encoding='utf-8')).get('items', [])
        except Exception:
            pass
    print('  上轮结果 %d 条作为增量基础' % len(prev_items), flush=True)

    # ① 探测总量 + 采前 30 页样本用于发现科类/批次
    num_found, sample, ok = 0, [], False
    for attempt in range(4):          # 首屏偶发代理/直连全挂，重试几轮避免整片作废
        num_found, sample, ok = probe({}, 'BASE%d' % attempt, proxies, seed, stats, lock)
        if ok and num_found:
            break
        time.sleep(1.5)
    if not (ok and num_found):
        raise SystemExit('第 1 页连续 4 轮取不到 numFound，放弃（避免产生空结果分支）')
    print('  原始 numFound=%d（单维度最多返回 %d 条）' % (num_found, ITEM_CAP), flush=True)
    sample_jobs = [(p, {}, 'BASE') for p in range(2, min(31, (num_found + SIZE - 1) // SIZE) + 1)]
    sample_res, _ = run_jobs(sample_jobs, proxies, seed, stats, lock, t0, '样本')
    base_items = sample + [it for v in sample_res.values() for it in v] + prev_items

    # ② 发现科类：样本实际值 + 候选并发探测
    types = {it.get('local_type_name') for it in base_items if it.get('local_type_name')}
    cand = [({'local_type_name': t}, 'P:' + t) for t in CANDIDATE_TYPES]
    for e, l, n, _, ok2 in probe_all(cand, proxies, seed, stats, lock):
        if ok2 and n > 0:
            types.add(e['local_type_name'])
    types = sorted(types)
    print('  发现科类 %d 个: %s' % (len(types), types), flush=True)

    # ③ 构造子任务：type → (type,batch) → (type,batch,province_id)
    jobs, incomplete, submeta = [], [], []
    type_specs = [({'local_type_name': t}, 'S:' + t) for t in types]
    type_res = probe_all(type_specs, proxies, seed, stats, lock)
    need_batch = []
    for e, l, n, _, ok2 in type_res:
        t = e['local_type_name']
        if not ok2 or not n:
            continue
        if n <= ITEM_CAP:
            pages = min(PAGE_CAP, (n + SIZE - 1) // SIZE)
            jobs += [(p, e, t) for p in range(1, pages + 1)]
            submeta.append((t, n, pages))
        else:
            need_batch.append(t)
    # 二级：按批次拆
    if need_batch:
        batch_specs = []
        for t in need_batch:
            bts = {it.get('local_batch_name') for it in base_items
                   if it.get('local_type_name') == t and it.get('local_batch_name')}
            bts |= set(CANDIDATE_BATCHES)   # 样本常只覆盖本科批，必须补齐候选
            for bt in sorted(bts):
                batch_specs.append(({'local_type_name': t, 'local_batch_name': bt},
                                    'S:%s/%s' % (t, bt)))
        need_prov = []
        for e, l, n, _, ok2 in probe_all(batch_specs, proxies, seed, stats, lock):
            if not ok2 or not n:
                continue
            if n <= ITEM_CAP:
                pages = min(PAGE_CAP, (n + SIZE - 1) // SIZE)
                jobs += [(p, e, e['local_type_name'] + '/' + e['local_batch_name'])
                         for p in range(1, pages + 1)]
                submeta.append((e['local_type_name'] + '/' + e['local_batch_name'], n, pages))
            else:
                need_prov.append(e)
        # 三级：按院校所在省拆
        if need_prov:
            prov_specs = []
            for e in need_prov:
                for pid in PROVINCE_IDS:
                    e2 = dict(e, province_id=pid)
                    prov_specs.append((e2, 'S:%s/%s/p%s' %
                                      (e['local_type_name'], e['local_batch_name'], pid)))
            for e, l, n, _, ok2 in probe_all(prov_specs, proxies, seed, stats, lock):
                if not ok2 or not n:
                    continue
                if n > ITEM_CAP:
                    incomplete.append((e['local_type_name'], e['local_batch_name'],
                                       e['province_id'], n))
                    continue
                pages = min(PAGE_CAP, (n + SIZE - 1) // SIZE)
                jobs += [(p, e, l[2:]) for p in range(1, pages + 1)]
                submeta.append((l[2:], n, pages))
    print('  子任务 %d 个 / 页任务 %d 个 / 仍超限子集 %d 个' %
          (len(submeta), len(jobs), len(incomplete)), flush=True)

    # ④ 全量采集 + 失败页补采 2 轮
    res, failed = run_jobs(jobs, proxies, seed, stats, lock, t0, '全量')
    for rnd in (1, 2):
        if not failed:
            break
        print('  → 补采第 %d 轮：%d 页' % (rnd, len(failed)), flush=True)
        retry = [(p, e, l + '#r%d' % rnd) for p, e, l in failed]
        r2, f2 = run_jobs(retry, proxies, seed, stats, lock, t0, '补采%d' % rnd)
        res.update(r2)
        failed = f2

    # ⑤ 合并去重（上轮 + 本轮样本 + 本轮全量）
    all_items = prev_items + base_items + [it for v in res.values() for it in v]
    seen, uniq = set(), []
    for it in all_items:
        k = hashlib.md5(json.dumps(it, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        if k not in seen:
            seen.add(k)
            uniq.append(it)
    coverage = round(100.0 * len(uniq) / num_found, 1) if num_found else 0

    out = {
        'prov': PROV, 'year': YEAR, 'api': API,
        'num_found': num_found, 'count': len(uniq), 'coverage_pct': coverage,
        'prev_count': len(prev_items), 'pages_total': len(jobs), 'pages_ok': len(res),
        'retry_failed': len(failed), 'subtasks': len(submeta), 'unresolved': incomplete,
        'workers': WORKERS, 'timeout': TIMEOUT, 'page_cap': PAGE_CAP,
        'elapsed': round(time.time() - t0, 1), 'stats': stats,
        'collected_at': time.strftime('%F %T'),
        'build_id': os.environ.get('CNB_BUILD_ID', ''),
        'pipeline': os.environ.get('CNB_PIPELINE_NAME', ''),
        'items': uniq,
    }
    path = 'out/cutoff_%s_%s.json' % (PROV, YEAR)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)
    print('=== 完成: %d 条 / numFound %d / 覆盖率 %.1f%% / 仍失败 %d 页 / %.0fs -> %s ===' %
          (len(uniq), num_found, coverage, len(failed), time.time() - t0, path), flush=True)
    print('RESULT_JSON ' + json.dumps(
        {'prov': PROV, 'year': YEAR, 'count': len(uniq), 'num_found': num_found,
         'coverage_pct': coverage, 'retry_failed': len(failed)},
        ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
