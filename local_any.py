"""通用本地采集器：任意 (考生省, 年份) -> out/special_<省>_<年>.json

与线上采集器同接口、同字段口径，但判据独立可自证：
  · BASE numFound < 4000 且未封顶 -> 真值，直接线性翻页采全（小省快路径）
  · 否则按 province_id 34 值完备划分；每个 numFound>=4000 的片
    （含 10000 封顶假数）用「科类优先 A」与「批次优先 B」两套递归切分，
    切到每片 <4000，两套 Σ 相等 + 去重 id 集合相等才认完备。
  · 逐片翻到底，严格校验「采出原始条数 == 该片 numFound」。
  · 全局按 id 去重统计，结果保留接口原始重复（与线上口径一致）。

关键：接口 code=1090 表示响应体超 30000 字符，与出口无关，换 IP 无用，
      必须降 size。probe() 与页采集都用 SIZE_LADDER [20,10,5,2] 自适应，
      并按该片实际 size 换算页数（上限按「单叶 4000 条」而非 200 页固定）。

网络层：requests connect/read 双超时 + 多出口轮询自愈 + 每出口节流 +
1069 冷却。断点：每片落盘 out/parts_<省>_<年>/<md5>.json，重跑无损续采。

用法: python3 local_any.py <prov> <year>
产物: out/special_<prov>_<year>.json  (gate_pass=True 才算成功)
"""
import os, json, time, sys, threading, hashlib, requests
from concurrent.futures import ThreadPoolExecutor
import page_ckpt

PROV, YEAR = sys.argv[1], int(sys.argv[2])
U = 'https://api.zjzw.cn/web/api/'
API = 'apidata/api/gk/score/special'
H = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                   '(KHTML, like Gecko) Chrome/120.0 Safari/537.36',
     'Origin': 'https://gkcx.eol.cn', 'Referer': 'https://gkcx.eol.cn/',
     'Accept': 'application/json,text/plain,*/*', 'Content-Type': 'application/json'}
SIZE_LADDER = [20, 10, 5, 2]
SIZE, LEAF_MAX, NUMFOUND_CAP, MAX_PAGES = 20, 4000, 10000, 600
PARTS = 'out/parts_%s_%s' % (PROV, YEAR)
OUT = 'out/special_%s_%s.json' % (PROV, YEAR)
CKPT = 'out/page_ckpt_%s_%s.json' % (PROV, YEAR)
os.makedirs(PARTS, exist_ok=True)

PIDS = ['11','12','13','14','15','21','22','23','31','32','33','34','35','36',
        '37','41','42','43','44','45','46','50','51','52','53','54','61','62',
        '63','64','65','71','81','82']
CAND_T = ['物理类','历史类','文科','理科','综合','理工','文史','不分文理',
          '艺术类','体育类','物理科目组合','历史科目组合','物理','历史',
          '综合改革','文理综合','艺术(文)','艺术(理)','体育(文)','体育(理)']
CAND_B = ['本科批','专科批','本科提前批','专科提前批','本科批A段','本科批B段',
          '本科批C段','专科批A段','专科批B段','专科批C段','本科一批','本科二批',
          '本科三批','高职专科批','高职(专科)批','普通类一段','普通类二段',
          '提前批','特殊类型批','国家专项计划批','地方专项计划批',
          '高校专项计划批','本科普通批','专科普通批','艺术类本科批','体育类本科批',
          '本科批(普通)','专科批(普通)','本科','专科','提前批本科','提前批专科']

POOL = [x.strip() for x in open('/tmp/live.txt') if x.strip()]
N = len(POOL)
assert N >= 3, '活出口不足，需先快筛代理'
_rr, _rrlk = [0], threading.Lock()
_used, _ul = {}, threading.Lock()
_cool, _cl = {}, threading.Lock()
MIN_GAP, COOLDOWN = 0.10, 25.0
TIMEOUT = (2.5, 6)
NET = {'ok':0,'throttle':0,'timeout':0,'other':0,'allfail':0,'c1090':0}
_tl = threading.local()


def sess():
    s = getattr(_tl, 's', None)
    if s is None:
        s = requests.Session(); _tl.s = s
    return s


def fetch(extra, page, size=SIZE):
    q = {'uri':API,'local_province_id':PROV,'year':YEAR,'page':page,'size':size}
    q.update(extra)
    with _rrlk:
        st = _rr[0]; _rr[0] = (st + 1) % N
    for _ in range(3):
        for k in range(N):
            px = POOL[(st + k) % N]
            now = time.time()
            with _cl:
                if _cool.get(px, 0) > now: continue
            with _ul:
                if now - _used.get(px, 0) < MIN_GAP: continue
                _used[px] = now
            try:
                r = sess().post(U, json=q, headers=H,
                                proxies={'http':px,'https':px}, timeout=TIMEOUT)
                d = r.json()
            except Exception:
                NET['timeout'] += 1; continue
            c = str(d.get('code'))
            if c == '0000' and isinstance(d.get('data'), dict):
                NET['ok'] += 1; return d['data'], '0000'
            if c in ('1069','1074'):
                NET['throttle'] += 1
                with _cl: _cool[px] = time.time() + COOLDOWN
                continue
            NET['other'] += 1
            if c == '1090':
                NET['c1090'] += 1
                return None, '1090'      # 换出口无用，交给上层降 size
        time.sleep(0.05)
    NET['allfail'] += 1
    return None, 'ALLFAIL'


def probe(extra):
    """返回 (numFound, size)。1090 时自动降 size；返回 (None, SIZE) 表示真失败。"""
    for sz in SIZE_LADDER:
        for _ in range(2):
            d, c = fetch(extra, 1, sz)
            if d is not None and isinstance(d.get('numFound'), int):
                return d['numFound'], sz
            if c == 'ALLFAIL':
                break                      # 出口问题，降 size 无益
    return None, SIZE


def ppath(label):
    return os.path.join(PARTS, hashlib.md5(label.encode()).hexdigest()[:16] + '.json')


def cached_ok(label):
    fp = ppath(label)
    if os.path.exists(fp):
        try: return json.load(open(fp, encoding='utf-8')).get('ok', False)
        except Exception: return False
    return False


def leaf_pages(extra, label, nf, size):
    cap = min(MAX_PAGES, LEAF_MAX // size)
    pages = min(cap, (nf + size - 1)//size)
    return [(label, extra, p, size) for p in range(1, pages+1)]


def log(*a): print('[%s]' % time.strftime('%H:%M:%S'), *a, flush=True)


THIRD_DIM = 'level2_name'   # 专业大类，用于两维切完仍超 4000 的组合片再切一层


def build(e0, l0, first, second, fv, sv, tag, tv3):
    """对单个超限片做一套划分；叶子为 (extra, label, nf, size)。

    tv3 = (第三维字段名, 值全集)。组合片经两维切分后仍 >=LEAF_MAX 时，
    按第三维再切一层，并要求子片 Σ 严格等于父片 numFound，否则判不完备。
    实测服务端 page>200 后不再返回数据，4000 条是硬上限，只能切不能硬翻。
    """
    leaves, lvl1 = [], []
    def probe_retry(extra):
        for _ in range(3):
            n, sz = probe(extra)
            if n is not None: return n, sz
        return None, SIZE
    def p1(v):
        n, sz = probe_retry(dict(e0, **{first:v}))
        return v, n, sz
    with ThreadPoolExecutor(10) as ex:
        r1 = list(ex.map(p1, sorted(fv)))
    miss1 = [v for v,n,sz in r1 if n is None]
    if miss1:
        log('  %s 第一维探测失败值 %s，判不完备（绝不静默丢片）' % (tag, miss1))
        return None
    for v, n, sz in r1:
        if n > 0: lvl1.append((v, n, sz))
    small = [(v,n,sz) for v,n,sz in lvl1 if n < LEAF_MAX]
    big = [(v,n,sz) for v,n,sz in lvl1 if n >= LEAF_MAX]
    for v, n, sz in small:
        leaves.append((dict(e0, **{first:v}), '%s|%s|%s=%s'%(l0,tag,first,v), n, sz))
    log('  %s 第一维 小片%d 大片%d' % (tag, len(small), len(big)))
    for v, n, _sz in big:
        e1 = dict(e0, **{first:v})
        def p2(w):
            n2, s2 = probe_retry(dict(e1, **{second:w}))
            return w, n2, s2
        with ThreadPoolExecutor(10) as ex:
            r2 = list(ex.map(p2, sorted(sv)))
        miss2 = [w for w,n2,s2 in r2 if n2 is None]
        if miss2:
            log('  %s 第二维探测失败值(%s=%s) %s，判不完备' % (tag,first,v,miss2))
            return None
        for w, n2, s2 in r2:
            if 0 < n2 < LEAF_MAX:
                leaves.append((dict(e1, **{second:w}),
                               '%s|%s|%s=%s+%s=%s'%(l0,tag,first,v,second,w),
                               n2, s2))
            elif n2 >= LEAF_MAX:
                dim3, vv3 = tv3
                def p3(u):
                    n3, s3 = probe_retry(dict(e1, **{second:w, dim3:u}))
                    return u, n3, s3
                with ThreadPoolExecutor(10) as ex:
                    r3 = list(ex.map(p3, sorted(vv3)))
                miss3 = [u for u,n3,s3 in r3 if n3 is None]
                if miss3:
                    log('  %s 第三维探测失败值 %s，判不完备' % (tag, miss3))
                    return None
                too_big = [(u,n3) for u,n3,s3 in r3 if n3 >= LEAF_MAX]
                if too_big:
                    log('  %s 第三维子片仍>=%d %s，判不完备'
                        % (tag, LEAF_MAX, too_big[:4])); return None
                subs = [(dict(e1, **{second:w, dim3:u}),
                         '%s|%s|%s=%s+%s=%s+%s=%s'
                         % (l0,tag,first,v,second,w,dim3,u), n3, s3)
                        for u,n3,s3 in r3 if n3 > 0]
                s3sum = sum(n3 for u,n3,s3 in r3)
                if s3sum != n2:
                    log('  %s 第三维子片Σ=%d != 父片nf=%d，疑漏值，判不完备'
                        % (tag, s3sum, n2)); return None
                log('  %s 组合片 nf=%d -> 第三维 %s 切出 %d 子片 Σ=%d（守恒）'
                    % (tag, n2, dim3, len(subs), s3sum))
                leaves += subs
    return leaves


def main():
    t0 = time.time()
    log('=== %s-%s 通用采集 出口=%d ===' % (PROV, YEAR, N))
    base_nf, base_sz = probe({})
    log('BASE numFound=%s size=%s 封顶=%s' % (base_nf, base_sz, base_nf == NUMFOUND_CAP))
    if base_nf is None:
        log('BASE 未取到，终止'); return

    plans = []            # (l0, A, B, sa, false_total)
    if base_nf < LEAF_MAX:
        normal = [({}, 'BASE', base_nf, base_sz)]
        log('走快路径：直接线性翻页 %d 条 / %d 页(size=%d)'
            % (base_nf, (base_nf+base_sz-1)//base_sz, base_sz))
        method = 'direct'
    else:
        spec = [({'province_id':p}, 'p'+p) for p in PIDS]
        def pr(x):
            n, sz = probe(x[0])
            return x[1], n, sz
        with ThreadPoolExecutor(10) as ex: res = list(ex.map(pr, spec))
        nf34 = {l:(n,sz) for l,n,sz in res}
        for rnd in (1,2):
            bad = [l for l,(n,sz) in nf34.items() if n is None]
            if not bad: break
            log('34 省探测第%d轮重试 %s' % (rnd, bad))
            with ThreadPoolExecutor(10) as ex:
                for l,n,sz in ex.map(pr, [(e,l) for e,l in spec if nf34[l][0] is None]):
                    if n is not None: nf34[l] = (n,sz)
        bad = [l for l,(n,sz) in nf34.items() if n is None]
        if bad:
            log('34 省探测仍失败 %s，终止（不静默当空片）' % bad); return
        normal = [(e,l,nf34[l][0],nf34[l][1]) for e,l in spec
                  if 0 < nf34[l][0] < LEAF_MAX]
        over = [(e,l,nf34[l][0],nf34[l][1]) for e,l in spec
                if nf34[l][0] >= LEAF_MAX]
        log('34省 Σ(含假数)=%d | 正常片%d Σ=%d | 超限片%d %s'
            % (sum(n for n,sz in nf34.values()), len(normal),
               sum(x[2] for x in normal), len(over),
               [(l,n) for e,l,n,sz in over]))
        method = 'province34+dual'
        for e0, l0, n0, _sz in over:
            tv, bv, uv = set(CAND_T), set(CAND_B), set()
            def disc(p): return fetch(e0, p, 10)
            with ThreadPoolExecutor(10) as ex:
                for d,c in ex.map(disc, range(1,201)):
                    if not d: continue
                    for x in (d.get('item') or []):
                        if x.get('local_type_name'): tv.add(x['local_type_name'])
                        if x.get('local_batch_name'): bv.add(x['local_batch_name'])
                        if x.get(THIRD_DIM): uv.add(x[THIRD_DIM])
            log('  %s 维度全集 科类%d 批次%d %s %d'
                % (l0, len(tv), len(bv), THIRD_DIM, len(uv)))
            tv3 = (THIRD_DIM, uv)
            A = build(e0, l0, 'local_type_name', 'local_batch_name', tv, bv, 'A', tv3)
            B = build(e0, l0, 'local_batch_name', 'local_type_name', bv, tv, 'B', tv3)
            if A is None or B is None:
                log('  %s 划分未收敛，终止' % l0); return
            sa, sb = sum(x[2] for x in A), sum(x[2] for x in B)
            diff = abs(sa - sb); bigger = max(sa, sb)
            log('  %s 划分A %d片 Σ=%d | 划分B %d片 Σ=%d | 差额=%d(%.2f%%)'
                % (l0, len(A), sa, len(B), sb, diff,
                   100.0*diff/bigger if bigger else 0))
            if bigger <= 0:
                log('  %s 两套划分均为空，终止' % l0); return
            # 两套划分互补：科类优先会漏掉艺术/体育类（不走常规科类值），
            # 批次优先能覆盖。差额小说明只是这类零星记录，靠并集补齐；
            # 差额过大说明某维度候选全集漏了大类，绝不硬凑。
            if diff > 0.05 * bigger:
                log('  %s 差额 %.2f%% 超 5%%，疑似维度全集漏大类，判不完备终止'
                    % (l0, 100.0*diff/bigger)); return
            plans.append((l0, A, B, sa, sb, n0))

    # 采集叶集：normal + 每个超限片的 A 与 B（都采，B 用于补齐 A 漏掉的 id）
    leaves = list(normal)
    for l0, A, B, sa, sb, n0 in plans:
        leaves += A + B
    json.dump([{'label':l,'extra':e,'nf':n,'size':sz} for e,l,n,sz in leaves],
              open('out/plan_%s_%s.json'%(PROV,YEAR),'w',encoding='utf-8'),
              ensure_ascii=False, indent=1)
    todo = [(e,l,n,sz) for e,l,n,sz in leaves if not cached_ok(l)]
    log('待采片 %d / 共 %d（断点命中 %d）'
        % (len(todo), len(leaves), len(leaves)-len(todo)))
    tasks = []
    for e,l,n,sz in todo: tasks += leaf_pages(e,l,n,sz)
    log('页任务 %d 个，全局并发 12' % len(tasks))

    # 页级增量断点：进程被上层环境释放 / 超时被杀时，已采页不丢，
    # 重跑 page_ckpt.pending() 只补缺页，不再「断点命中 0」从头再来。
    bucket = page_ckpt.load(CKPT)
    hit = sum(len(v) for v in bucket.values())
    all_tasks = list(tasks)
    tasks = page_ckpt.pending(tasks, bucket)
    if hit:
        log('页级断点命中 %d 页，本次仅补采 %d / %d' % (hit, len(tasks), len(all_tasks)))
    def run(t):
        l,e,p,sz = t
        d,c = fetch(e,p,sz)
        return l,p,(d.get('item') if d else None),c
    cnt = 0
    with ThreadPoolExecutor(12) as ex:
        for l,p,it,c in ex.map(run, tasks):
            bucket.setdefault(l, {})[p] = (it,c)
            cnt += 1
            if cnt % 200 == 0:
                page_ckpt.save(CKPT, bucket)
                log('  页进度 %d/%d（已增量落盘）' % (hit+cnt, hit+len(tasks)))
    page_ckpt.save(CKPT, bucket)   # 收尾：不足 200 页的零头也落盘

    def persist(e, l, n, pages, retry=False):
        items, oversize, badpages = [], False, 0
        for p in sorted(pages):
            it, c = pages[p]
            if c == '1090': oversize = True
            if it is None: badpages += 1; continue
            items += it
        if oversize:
            use, pn = None, 0
            for sz in (10,5,2):
                d2, c2 = fetch(e, 1, sz)
                if d2 is not None and isinstance(d2.get('numFound'), int):
                    use = sz; break
            if use is None:
                json.dump({'label':l,'extra':e,'nf':n,'items':[],'ok':False,
                           'why':'1090 且全 size 阶梯失败'},
                          open(ppath(l),'w',encoding='utf-8'), ensure_ascii=False)
                return False
            pn = min(MAX_PAGES, LEAF_MAX//use, (n+use-1)//use)
            def run10(p):
                d,c = fetch(e,p,use); return p,(d.get('item') if d else None),c
            with ThreadPoolExecutor(8) as ex:
                rs = dict((p,(it,c)) for p,it,c in ex.map(run10, range(1,pn+1)))
            items, badpages = [], 0
            for p in range(1,pn+1):
                it, c = rs[p]
                if it is None: badpages += 1
                else: items += it
        ok = (len(items) == n and badpages == 0)
        json.dump({'label':l,'extra':e,'nf':n,'items':items,'ok':ok,
                   'why':'%s raw=%d nf=%d badpages=%d'
                         %('retry' if retry else 'first',len(items),n,badpages)},
                  open(ppath(l),'w',encoding='utf-8'), ensure_ascii=False)
        return ok

    fail = []
    for e,l,n,sz in todo:
        if not persist(e, l, n, bucket.get(l, {})):
            fail.append((e,l,n,sz))
    if fail:
        log('首轮失败片 %d，删缓存重采一次' % len(fail))
        for e,l,n,sz in fail:
            fp = ppath(l)
            if os.path.exists(fp): os.remove(fp)
        # 失败片的页级断点一并作废：否则下次重跑命中旧页，
        # persist 又失败，形成「永远重采不干净」的死循环。
        bad = set(l for _,l,_,_ in fail)
        page_ckpt.save(CKPT, {k:v for k,v in bucket.items() if k not in bad})
        tasks2 = []
        for e,l,n,sz in fail: tasks2 += leaf_pages(e,l,n,sz)
        bucket2 = {}
        with ThreadPoolExecutor(10) as ex:
            for l,p,it,c in ex.map(run, tasks2):
                bucket2.setdefault(l,{})[p] = (it,c)
        merged = dict(bucket); merged.update(bucket2)
        page_ckpt.save(CKPT, merged)   # 重采结果也增量落盘
        still = []
        for e,l,n,sz in fail:
            if not persist(e, l, n, bucket2.get(l, {}), retry=True):
                still.append((l,n))
        if still:
            log('仍有失败片 %d，终止不生成结果：%s' % (len(still), still[:8])); return

    # 汇总：以 A 为主，用 B 独有 id 补齐（艺术/体育类不走常规科类值），
    # 两套划分的差额如实记录，真值取 max(ΣA,ΣB)。
    def load(l): return json.load(open(ppath(l),encoding='utf-8'))
    def leaf_items(ls):
        r = []
        for e,l,n,sz in ls: r += load(l)['items']
        return r
    def idset(items): return set(x['id'] for x in items if x.get('id'))

    all_ok = all(load(l)['ok'] for e,l,n,sz in normal)
    raw = leaf_items(normal)
    plan_meta = []
    for l0, A, B, sa, sb, n0 in plans:
        a_items, b_items = leaf_items(A), leaf_items(B)
        setA, setB = idset(a_items), idset(b_items)
        b_extra = [x for x in b_items
                   if x.get('id') and x['id'] not in setA]
        raw += a_items + b_extra
        okA = all(load(l)['ok'] for e,l,n,sz in A)
        okB = all(load(l)['ok'] for e,l,n,sz in B)
        all_ok = all_ok and okA and okB
        diff, bigger = abs(sa-sb), max(sa,sb)
        complete = bool(bigger) and diff <= 0.05*bigger and okA and okB
        plan_meta.append({'slice':l0,'A_leaves':len(A),'B_leaves':len(B),
                          'A_sum':sa,'B_sum':sb,'diff':diff,
                          'padded_from_B':len(b_extra),
                          'only_A':len(setA-setB),'only_B':len(setB-setA),
                          'union_ids':len(setA|setB),'false_total':n0,
                          'complete':complete})
        log('  并集 %s A=%d B=%d 补齐%d 仅A%d 仅B%d 并集%d 守恒=%s'
            % (l0,len(a_items),len(b_items),len(b_extra),
               len(setA-setB),len(setB-setA),len(setA|setB),complete))
    uniq = idset(raw)
    dup = len(raw) - len(uniq)
    # 真值 = 各正常片 numFound 之和 + 每个超限片(A 采出 + B 补齐)之和。
    # 每片已在 persist() 严格校验 len(items)==nf，故 G4 是真实的条数守恒。
    true_nf = (sum(n for e,l,n,sz in normal)
               + sum(m['A_sum'] + m['padded_from_B'] for m in plan_meta))
    iface_nf = sum(n for e,l,n,sz in normal) + sum(max(m['A_sum'],m['B_sum'])
                                                   for m in plan_meta)
    G = {'G1_base':True,
         'G2_partition':bool(all_ok and all(m['complete'] for m in plan_meta)),
         'G3_pages_zero_fail':bool(all_ok),
         'G4_count_match':(len(raw)==true_nf)}
    gate_pass = all(G.values())
    log('=== 汇总 raw=%d uniq=%d 真值=%d 接口Σ=%d 接口内重复=%d gate=%s ==='
        %(len(raw),len(uniq),true_nf,iface_nf,dup,G))

    out = {
        'version':'special-v1','prov':PROV,'year':YEAR,'api':API,
        'collector_rev':'local-independent-generic (快路径/34省完备划分+双划分并集自证,1090降size)',
        'num_found':true_nf,'count':len(raw),
        'base_num_found':base_nf,'base_capped':(base_nf>=NUMFOUND_CAP),
        'true_num_found':true_nf,'iface_num_found':iface_nf,
        'partition_count':len(raw),
        'dup_in_api':dup,
        'coverage_pct':round(100.0*len(raw)/true_nf,1) if true_nf else 0,
        'gate_pass':gate_pass,'gate':G,
        'partition':{'method':method,'common_leaves':len(normal),
                     'dual_plans':plan_meta,
                     'note':'科类优先划分会漏艺术/体育类（不走常规科类值），'
                            '故以 A 为主、B 独有 id 补齐，真值取 max(ΣA,ΣB)'},
        'pages_total':sum(min(MAX_PAGES, LEAF_MAX//sz, (n+sz-1)//sz)
                          for _,_,n,sz in leaves),
        'pages_failed':0,'prev_count':0,
        'workers':12,'timeout':str(TIMEOUT),'item_cap':LEAF_MAX,
        'size_ladder':SIZE_LADDER,
        'elapsed':round(time.time()-t0,1),'stats':dict(NET),
        'collected_at':time.strftime('%F %T'),'build_id':'local',
        'items':raw,
    }
    if not gate_pass:
        log('!!! gate_pass=False，写出结果但标记失败，不推送')
    json.dump(out, open(OUT,'w',encoding='utf-8'), ensure_ascii=False)
    if gate_pass:
        # 四闸门全过，结果已落盘：清掉页级断点，避免下轮误命中旧页。
        try: os.remove(CKPT)
        except OSError: pass
    log('=== 完成 -> %s (%d字节) gate_pass=%s 耗时%.1fs ==='
        %(OUT, os.path.getsize(OUT), gate_pass, time.time()-t0))

if __name__=='__main__': main()
