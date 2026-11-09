# -*- coding: utf-8 -*-
"""投档线采集器 v5（零丢失版）：gk/score/province。

与 v4 的根本区别：
  v4 = 靠样本猜科类/批次做启发式三级切分 + 探测失败片静默跳过
       （if not ok2 or not n: continue）+ 覆盖率 >=95% 就落盘算达标
       -> 会丢数据，且把不完整结果标成"完成"
  v5 = province_id 34 省确定性完备切分 + 四道闸门，任一不过不算完成

零丢失定义（单个 PROV-YEAR）：
  true_nf  = 应采总条数（BASE 未封顶时取 BASE，封顶时取 34 省分片 Σ）
  count    = 落盘的原始记录条数（含接口内重复，不去重）
  必须 count == true_nf，且中间每一步可审计、可复算。

BASE 封顶特例（v5.1，实测 41-2025 / 51-2025）：
  无筛选查询的 numFound 被接口截断在 10000，是假总数；而 34 省分片各自
  未封顶，Σ=10097 / 11327 才是真值。旧逻辑强制「Σ == BASE」使 G2 永远
  判死、一条页都没采（partition_count=0，count 只是上轮 prev 增量）。
  现改为：BASE 达到 NUMFOUND_CAP 且所有分片均未封顶时，以 Σ 分片为守恒
  基准；若某分片自身仍封顶，则 Σ 也不可信，明确判失败等待更细切分，
  绝不拿假数凑合、绝不降级为"覆盖率够就算完成"。

四道闸门：
  G1 base      无筛选首页成功取到 numFound（重试 PROBE_ROUNDS 轮）
  G2 partition 34 省探测零失败 + 无封顶分片 + 守恒基准成立
               （BASE 未封顶：Σ == BASE；BASE 封顶：Σ >= BASE 且以 Σ 为真值）
               >4000 条的片递归按科类/批次切分，每层同样验证 Σ 守恒
  G3 pages     所有页任务成功，失败页换代理补采到 0（MAX_PAGE_ROUNDS 轮）
  G4 count     页任务原始记录总数 part_count == true_nf

gate_pass = G1 && G2 && G3 && G4。
闸门未全过仍写结果文件并推分支（保留增量，绝不丢已采数据），但
collect.yml 会把该 job 标为失败，巡航据此重派；只有 gate_pass=true 才算达标。
G1 失败（连 BASE 都没取到）时不写结果文件，避免用空壳覆盖云端旧结果。

实测依据：province_id 是接口真实生效的筛选参数，34 个值构成确定性
完备划分，不依赖样本猜科类/批次。65-2026 7066 / 62-2025 7140 /
62-2024 6534 / 65-2025 7867 / 13-2026 5778 / 44-2025 8495 /
11-2021 1442 的 34 省 Σ 精确等于 BASE numFound，且无片 >4000 条。
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
ITEM_CAP = PAGE_CAP * SIZE          # 4000：单维度硬翻页上限
NUMFOUND_CAP = 10000               # 接口无筛选 numFound 封顶值，达到即为不可信假总数
TIMEOUT = 3
WORKERS = 8
QUERY_ROUNDS = 10                  # 单次查询最多轮换 10 个代理
PROBE_ROUNDS = 8                   # 探测整体重试轮次
MAX_PAGE_ROUNDS = 15               # 页补采轮次
API = 'apidata/api/gk/score/province'

# 34 个院校所在省级行政区代码，构成确定性完备划分
PROVINCE_IDS = ['11', '12', '13', '14', '15', '21', '22', '23', '31', '32',
                '33', '34', '35', '36', '37', '41', '42', '43', '44', '45',
                '46', '50', '51', '52', '53', '54', '61', '62', '63', '64',
                '65', '71', '81', '82']
# 仅当某省片 >4000 条时才启用，作为二级切分兜底
CANDIDATE_TYPES = ['物理类', '历史类', '文科', '理科', '综合', '理工', '文史',
                   '不分文理', '艺术类', '体育类', '物理科目组合', '历史科目组合']
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


def call(proxy, page, extra):
    q = {'uri': API, 'local_province_id': PROV, 'year': YEAR,
         'page': page, 'size': SIZE}
    q.update(extra)
    req = urllib.request.Request(U, data=json.dumps(q).encode(), headers=H, method='POST')
    with opener(proxy).open(req, timeout=TIMEOUT) as r:
        return json.loads(r.read().decode('utf-8'))


def query_strict(page, extra, proxies, stats, lock, seed=0, min_items=0):
    """严格查询：轮换代理直到拿到合法且完整的响应，绝不静默吞掉失败。

    返回 (items, numFound|None, ok)。numFound=0 是合法成功（空片），
    与 ok=False（探测失败）严格区分——v4 把两者都 continue 掉，正是丢数据源头。

    min_items>0 时做页级完整性校验：返回的记录数不足即视为失败并换代理，
    防止「code=0000 但 item 残缺/为空」的页混进 G4 造成隐性丢数据。
    """
    tag = (sum(ord(c) for c in json.dumps(extra, sort_keys=True, ensure_ascii=False))
           + page * 7 + seed)
    n = len(proxies)
    for k in range(QUERY_ROUNDS):
        px = proxies[(tag + k) % n]
        try:
            d = call(px, page, extra)
            code, dat = str(d.get('code')), d.get('data')
            if code == '0000' and isinstance(dat, dict):
                it = dat.get('item') or []
                if len(it) < min_items:
                    with lock:
                        stats['short_page'] += 1
                    continue              # 页残缺，换下一个代理重来
                with lock:
                    stats['ok'] += 1
                return it, dat.get('numFound'), True
            with lock:
                stats['throttled' if code in ('1069', '1074') else 'other'] += 1
            if code == '1069':
                time.sleep(0.25)
        except Exception:
            with lock:
                stats['conn_err'] += 1
    try:                                  # 代理全挂，直连兜底
        d = call(None, page, extra)
        if str(d.get('code')) == '0000' and isinstance(d.get('data'), dict):
            it = d['data'].get('item') or []
            if len(it) >= min_items:
                with lock:
                    stats['direct'] += 1
                return it, d['data'].get('numFound'), True
    except Exception:
        pass
    with lock:
        stats['query_failed'] += 1
    return [], None, False


def probe_strict_all(specs, proxies, stats, lock, phase, t0):
    """并发严格探测：所有 spec 必须成功；未成功的换 seed 重试。

    specs: [(extra, label)]，label 必须唯一。
    返回 [(extra, label, n, items, ok)]，顺序与 specs 一致；
    探测失败的片明确 ok=False（不再静默跳过），交由 G2 判死。
    """
    done = {}
    pending = list(specs)
    for rnd in range(PROBE_ROUNDS):
        if not pending:
            break
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(query_strict, 1, e, proxies, stats, lock, rnd * 131): (e, l)
                    for e, l in pending}
            for f in as_completed(futs):
                e, l = futs[f]
                items, nf, ok = f.result()
                if ok and nf is not None:
                    done[l] = (e, l, int(nf), items, True)
        pending = [(e, l) for e, l in pending if l not in done]
        print('  [探测 %s 轮%d] 成功 %d/%d | %.0fs' %
              (phase, rnd + 1, len(done), len(specs), time.time() - t0), flush=True)
    res = []
    for e, l in specs:
        res.append(done[l] if l in done else (e, l, 0, [], False))
    return res


def split_oversize(extra, label, parent_n, sample_items,
                   proxies, stats, lock, t0, audit):
    """>4000 条的省片：先按科类切、仍超限再按批次切。

    每层都验证「Σ 子片 numFound == 父片 numFound」且探测零失败；
    任何一层不守恒即返回 False，绝不凑合。
    """
    def one_level(e0, l0, pn, dim, candidates):
        vals = {it.get(dim) for it in sample_items if it.get(dim)} | set(candidates)
        specs = [(dict(e0, **{dim: v}), '%s|%s=%s' % (l0, dim, v))
                 for v in sorted(vals)]
        res = probe_strict_all(specs, proxies, stats, lock, l0 + '/' + dim, t0)
        audit.setdefault('splits', []).append({
            'parent': l0, 'dim': dim, 'parent_n': pn,
            'sum': sum(x[2] for x in res),
            'failed': [x[1] for x in res if not x[4]],
        })
        if any(not x[4] for x in res) or sum(x[2] for x in res) != pn:
            return None
        return [(e2, l2, n) for e2, l2, n, it2, ok in res if n > 0]

    lv1 = one_level(extra, label, parent_n, 'local_type_name', CANDIDATE_TYPES)
    if lv1 is None:
        return [], False
    leaves = []
    for e2, l2, n in lv1:
        if n <= ITEM_CAP:
            leaves.append((e2, n, (n + SIZE - 1) // SIZE, l2))
        else:
            lv2 = one_level(e2, l2, n, 'local_batch_name', CANDIDATE_BATCHES)
            if lv2 is None:
                return [], False
            for e3, l3, n3 in lv2:
                if n3 > ITEM_CAP:
                    return [], False      # 再超限 -> G2 失败，等待重派
                leaves.append((e3, n3, (n3 + SIZE - 1) // SIZE, l3))
    return leaves, True


def build_partition(base_nf, base_capped, sample_items, proxies, stats, lock, t0, audit):
    """34 省确定性完备切分 + Σ 守恒校验。返回 (leaves, complete, true_nf)。

    leaves = [(extra, n, pages, label)]
    true_nf = 本组合的应采总条数，G4 与结果 num_found 都以它为准。

    BASE 未封顶：沿用原严格判据 Σ 省 == BASE。
    BASE 封顶（>= NUMFOUND_CAP）：BASE 是接口截断产生的假总数，改以
    「34 省分片 Σ」为真值。前提是每个分片自身都未封顶且探测零失败——
    完备划分 + 各片未封顶，Σ 才等于真实总量；否则判 G2 失败。
    """
    specs = [({'province_id': pid}, 'p%s' % pid) for pid in PROVINCE_IDS]
    res = probe_strict_all(specs, proxies, stats, lock, '省', t0)
    audit['provinces'] = {x[1][1:]: x[2] for x in res}
    audit['prov_failed'] = [x[1] for x in res if not x[4]]
    audit['prov_sum'] = sum(x[2] for x in res)
    audit['base'] = base_nf
    audit['base_capped'] = base_capped
    if audit['prov_failed']:
        return [], False, 0
    # 分片自身封顶 -> Σ 同样不可信，绝不拿它当基准
    capped_provs = [(x[1], x[2]) for x in res if x[2] >= NUMFOUND_CAP]
    audit['capped_provs'] = capped_provs
    if capped_provs:
        return [], False, 0
    if base_capped:
        # 真值只能 >= 被截断的 BASE；若反而更小，说明有片异常，判失败
        if audit['prov_sum'] < base_nf:
            return [], False, 0
        true_nf = audit['prov_sum']
    else:
        if audit['prov_sum'] != base_nf:
            return [], False, 0
        true_nf = base_nf
    audit['true_nf'] = true_nf
    leaves = []
    for e, l, n, it, ok in res:
        if n == 0:
            continue                      # 合法空片，不产生页任务
        if n <= ITEM_CAP:
            leaves.append((e, n, (n + SIZE - 1) // SIZE, l))
        else:
            sub, ok2 = split_oversize(e, l, n, sample_items,
                                      proxies, stats, lock, t0, audit)
            if not ok2:
                return [], False, 0
            leaves += sub
    # 递归切分后的叶子若仍封顶，同样不能作为守恒计数基准
    if any(n >= NUMFOUND_CAP for _, n, _, _ in leaves):
        audit['capped_leaves'] = True
        return [], False, 0
    # 完备切分自检：所有叶子计划条数之和必须等于真值
    if sum(n for _, n, _, _ in leaves) != true_nf:
        audit['leaf_sum'] = sum(n for _, n, _, _ in leaves)
        return [], False, 0
    audit['leaves'] = len(leaves)
    audit['pages'] = sum(x[2] for x in leaves)
    return leaves, True, true_nf


def dedupe(items):
    """按整条记录内容哈希去重，保持原顺序。"""
    seen, out = set(), []
    for it in items:
        k = hashlib.md5(json.dumps(it, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        if k not in seen:
            seen.add(k)
            out.append(it)
    return out


def collect_pages(leaves, proxies, stats, lock, t0):
    """页任务全采，失败页换代理补采到 0。返回 (res, failed)。

    每个页任务带 expected：非末页须满 SIZE 条，末页须为该片余数条；
    不足即由 query_strict 换代理重试，保证 G4 的原始条数守恒可达成。
    """
    jobs = []
    for extra, n, pages, label in leaves:
        for p in range(1, pages + 1):
            exp = SIZE if p < pages else n - (pages - 1) * SIZE
            jobs.append((p, extra, label, exp))
    total = len(jobs)
    res, failed = {}, list(jobs)
    for rnd in range(1, MAX_PAGE_ROUNDS + 1):
        if not failed:
            break
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(query_strict, p, e, proxies, stats, lock,
                              rnd * 977, exp): (p, e, l, exp)
                    for p, e, l, exp in failed}
            new_failed = []
            for f in as_completed(futs):
                p, e, l, exp = futs[f]
                items, nf, ok = f.result()
                if ok:
                    res[(l, p)] = items
                else:
                    new_failed.append((p, e, l, exp))
        failed = new_failed
        print('  [页 轮%d] 成功 %d/%d，仍失败 %d | %.0fs' %
              (rnd, len(res), total, len(failed), time.time() - t0), flush=True)
    return res, failed


def main():
    proxies = [x.strip() for x in open('proxies_cnb.txt', encoding='utf-8') if x.strip()]
    random.seed('v5-%s-%s' % (PROV, YEAR))
    random.shuffle(proxies)
    if not proxies:
        proxies = [None]
    lock = threading.Lock()
    stats = {'ok': 0, 'throttled': 0, 'other': 0, 'conn_err': 0,
             'direct': 0, 'query_failed': 0, 'short_page': 0}
    t0 = time.time()
    os.makedirs('out', exist_ok=True)
    print('=== v5 零丢失采集 PROV=%s YEAR=%s 代理=%d 并发=%d ===' %
          (PROV, YEAR, len(proxies), WORKERS), flush=True)

    # 上轮结果作为增量基础（闸门未过时重派，已采数据累积不丢）
    prev_items = []
    if os.path.exists('out/prev.json'):
        try:
            prev_items = json.load(open('out/prev.json', encoding='utf-8')).get('items', [])
        except Exception:
            pass
    print('  上轮增量基础 %d 条' % len(prev_items), flush=True)

    # ---- G1：BASE numFound ----
    base_ok, base_nf, sample = False, 0, []
    for rnd in range(PROBE_ROUNDS):
        items, nf, ok = query_strict(1, {}, proxies, stats, lock, rnd * 31)
        if ok and nf is not None:
            base_ok, base_nf, sample = True, int(nf), list(items)
            break
        time.sleep(1.0)
    base_capped = bool(base_ok and base_nf >= NUMFOUND_CAP)
    print('  [G1] BASE numFound=%d ok=%s capped=%s%s' %
          (base_nf, base_ok, base_capped,
           '（接口截断假数，将以 34 省 Σ 为真值）' if base_capped else ''),
          flush=True)

    # BASE 前 30 页样本：仅用于超限片递归切分时发现科类/批次实际值
    sample_items = list(sample)
    if base_ok:
        last = min(31, (base_nf + SIZE - 1) // SIZE)
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(query_strict, p, {}, proxies, stats, lock, 0)
                    for p in range(2, last + 1)]
            for f in as_completed(futs):
                it, _, ok = f.result()
                if ok:
                    sample_items += it
        print('  样本 %d 条（仅用于超限片递归切分兜底）' % len(sample_items), flush=True)

    # ---- G2：34 省完备切分 ----
    audit = {}
    leaves, part_ok, true_nf = [], False, 0
    if base_ok:
        leaves, part_ok, true_nf = build_partition(
            base_nf, base_capped, sample_items, proxies, stats, lock, t0, audit)
    print('  [G2] 完备切分 ok=%s 省Σ=%s BASE=%s capped=%s 真值=%s 叶子=%s 页=%s' %
          (part_ok, audit.get('prov_sum'), audit.get('base'),
           audit.get('base_capped'), audit.get('true_nf'),
           audit.get('leaves'), audit.get('pages')), flush=True)
    if audit.get('prov_failed'):
        print('  [G2] 探测失败省: %s' % ' '.join(audit['prov_failed']), flush=True)
    if audit.get('capped_provs'):
        print('  [G2] 分片自身封顶，Σ 不可信，需更细切分: %s' %
              audit['capped_provs'], flush=True)
    if audit.get('capped_leaves'):
        print('  [G2] 递归切分后仍有封顶叶子，判失败', flush=True)
    if audit.get('leaf_sum') is not None:
        print('  [G2] 叶子计划条数 Σ=%s != 真值 %s' %
              (audit['leaf_sum'], audit.get('true_nf')), flush=True)
    if part_ok is False and not base_capped and audit.get('prov_sum') is not None \
            and audit.get('prov_sum') != audit.get('base'):
        print('  [G2] Σ 不守恒，差 %s 条' %
              (audit.get('base', 0) - audit.get('prov_sum', 0)), flush=True)

    # ---- G3：页零失败 ----
    res, failed = {}, []
    if part_ok:
        res, failed = collect_pages(leaves, proxies, stats, lock, t0)
    print('  [G3] 页成功 %d / 失败 %d' % (len(res), len(failed)), flush=True)

    # ---- G4：条数守恒（按接口原始返回条数，而非去重数）----
    # 实测修正：接口 numFound 统计的是「原始记录条数」，其中可能含内容
    # 完全相同的重复记录（65-2026 实测 18 条）。v5 初版按整条内容 md5
    # 去重后只有 7048/7066 被 G4 判死，但那 18 条并未丢失，只是接口数据
    # 本身重复。零丢失的正确判据是「接口返回的每一条都采到」：
    #   part_count（完备切分页任务的原始记录总数）== numFound
    # 跨页抖动造成的重复会让 part_count > numFound，同样判失败并重派；
    # 某页返回空却算成功，part_count < numFound，也会被这里拦住。
    # BASE 样本页不参与 G4，避免把失败页记录补上造成误判。
    part_items = []
    for key in sorted(res.keys()):
        part_items += res[key]
    part_count = len(part_items)
    part_uniq_count = len(dedupe(part_items))

    gate = {
        'G1_base': base_ok,
        'G2_partition': part_ok,
        'G3_pages_zero_fail': bool(part_ok and len(failed) == 0),
        'G4_count_match': bool(base_ok and part_ok and part_count == true_nf),
    }
    gate_pass = all(gate.values())
    print('  [G4] raw=%d uniq=%d true_nf=%d (BASE=%d capped=%s) ok=%s' %
          (part_count, part_uniq_count, true_nf, base_nf,
           base_capped, gate['G4_count_match']), flush=True)
    print('  === 闸门 %s gate_pass=%s ===' % (gate, gate_pass), flush=True)

    # 达标 -> 落盘原始全量（一条不少，含接口内重复，count 严格等于 numFound）
    # 未达标 -> 增量暂存，最大化保留已采数据供下轮续采
    if gate_pass:
        out_items = part_items
    else:
        out_items = dedupe(prev_items + sample_items + part_items)

    path = 'out/cutoff_%s_%s.json' % (PROV, YEAR)
    if not base_ok:
        # 连 BASE 都取不到：不写结果文件，避免空壳覆盖云端旧结果
        print('  G1 失败，本轮不落盘（云端旧结果保持不变，等待重派）', flush=True)
        print('RESULT_JSON ' + json.dumps(
            {'prov': PROV, 'year': YEAR, 'gate_pass': False, 'gate': gate,
             'num_found': 0, 'count': 0}, ensure_ascii=False), flush=True)
        return

    # num_found 写守恒基准真值：封顶组合以 34 省 Σ 为准，不再是假 BASE。
    # 未达标（G2 不过、true_nf=0）时保留 BASE，避免验收侧出现 nf=0 的误判。
    nf_out = true_nf if part_ok else base_nf
    out = {
        'version': 'v5', 'prov': PROV, 'year': YEAR, 'api': API,
        'collector_rev': 'v5.1-capped-base',  # 仅审计标识，version 保持 v5 以兼容验收
        'num_found': nf_out, 'count': len(out_items),
        'base_num_found': base_nf,       # 接口原始返回值（封顶时为假数，仅作审计）
        'base_capped': base_capped,
        'true_num_found': true_nf,       # 守恒基准真值（G4 判据）
        'partition_count': part_count,          # 页任务原始条数，G4 判据
        'dup_in_api': part_count - part_uniq_count,   # 接口数据内含重复条数，供审计
        'coverage_pct': round(100.0 * len(out_items) / nf_out, 1) if nf_out else 0,
        'gate_pass': gate_pass, 'gate': gate,
        'partition': audit,
        'pages_total': audit.get('pages', 0), 'pages_ok': len(res),
        'pages_failed': len(failed), 'prev_count': len(prev_items),
        'workers': WORKERS, 'timeout': TIMEOUT, 'item_cap': ITEM_CAP,
        'query_rounds': QUERY_ROUNDS, 'probe_rounds': PROBE_ROUNDS,
        'page_rounds': MAX_PAGE_ROUNDS,
        'elapsed': round(time.time() - t0, 1), 'stats': stats,
        'collected_at': time.strftime('%F %T'),
        'build_id': os.environ.get('GITHUB_RUN_ID', os.environ.get('CNB_BUILD_ID', '')),
        'items': out_items,
    }
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(out, f, ensure_ascii=False)
    print('=== 完成: %d/%d 条 (BASE=%d capped=%s) / gate_pass=%s / 接口内重复 %d / %.0fs -> %s ===' %
          (len(out_items), nf_out, base_nf, base_capped, gate_pass,
           out['dup_in_api'], time.time() - t0, path), flush=True)
    print('RESULT_JSON ' + json.dumps(
        {'prov': PROV, 'year': YEAR, 'count': len(out_items), 'num_found': nf_out,
         'base_num_found': base_nf, 'base_capped': base_capped,
         'partition_count': part_count, 'dup_in_api': out['dup_in_api'],
         'gate_pass': gate_pass, 'gate': gate, 'pages_failed': len(failed)},
        ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
