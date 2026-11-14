#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Actions runner 侧 school_id 分片采集（复刻本地已验证的 run_rest 方案）。

复刻口径（37-2024 / 43-2024 / 36-2024 均由该方案达标）：
  * 院校全集 school_ids.json（2970 所，与接口 school_id 同源），
    按 enumerate % SHARDS 确定性完备分片，10 片之和 = 全集。
  * 代理走 Supabase claim_proxy / release_proxy 租约池（anon secret），
    与本地共用同一份池且带 lease，不互抢；启动先 revive_dead_proxies 复活 dead 号。
  * code=1090（响应体超 30000 字符）在 page=1 探测阶段按 20→10→5 降 size，
    同一出口重试、不判死该号；确定后整片统一 size（43-2024 sid=46 踩坑修复）。
  * 四闸门：页零失败 / count==numFound / school_pure / 片内唯一 id。
  * 逐校落盘 out/school_<p>_<y>_<sid>.json（失败校也落盘），由 merge job 抓遗漏。

环境变量（workflow 注入）：COMBO=37-2021 SHARD=0 SHARDS=10 SUPABASE_ANON_KEY
"""
import json
import os
import random
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

COMBO = os.environ['COMBO']
PROV, YEAR_S = COMBO.split('-', 1)
YEAR = int(YEAR_S)
SHARD = int(os.environ.get('SHARD', '0'))
SHARDS = int(os.environ.get('SHARDS', '10'))
WORKERS = int(os.environ.get('WORKERS', '3'))
FETCH_ROUNDS = int(os.environ.get('FETCH_ROUNDS', '24'))
KEY = os.environ.get('SUPABASE_ANON_KEY', '').strip()
SB = os.environ.get('SUPABASE_URL',
                    'https://ripobzmwgmdxyixoevee.supabase.co').rstrip('/')
SIDS = json.load(open('school_ids.json', encoding='utf-8'))
OUT = 'out'
U = 'https://api.zjzw.cn/web/api/'
H = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                   'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
     'Origin': 'https://gkcx.eol.cn', 'Referer': 'https://gkcx.eol.cn/',
     'Accept': 'application/json,text/plain,*/*',
     'Content-Type': 'application/json'}
SH = {'apikey': KEY, 'Authorization': 'Bearer ' + KEY,
      'Content-Type': 'application/json', 'Accept': 'application/json'}


def log(*a):
    print('[%s]' % time.strftime('%H:%M:%S'), *a, flush=True)


def rpc(name, payload):
    try:
        req = urllib.request.Request(SB + '/rest/v1/rpc/' + name,
                                     data=json.dumps(payload).encode(),
                                     headers=SH, method='POST')
        with urllib.request.urlopen(req, timeout=20) as r:
            b = r.read().decode()
            return json.loads(b) if b.strip() else None
    except Exception as e:
        log('RPC %s 异常 %s' % (name, str(e)[:120]))
        return None


def emit(o):
    p = os.path.join(OUT, 'school_%s_%s_%s.json' %
                     (o['prov'], o['year'], o['school_id']))
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(o, f, ensure_ascii=False)


def collect_school(sid, fetch, size):
    """四闸门单校采集，口径与本地 collect_by_school.py 完全一致。"""
    nf, first = fetch(1)
    if nf is None:
        o = {'prov': PROV, 'year': YEAR, 'school_id': sid,
             'error': 'numFound 探测失败（代理耗尽）', 'gate_pass': False}
        emit(o)
        return o
    nf = int(nf)
    if nf >= 10000:
        o = {'prov': PROV, 'year': YEAR, 'school_id': sid, 'num_found': nf,
             'error': '单校片 numFound 封顶，需再切分', 'gate_pass': False}
        emit(o)
        return o
    pages = (nf + size - 1) // size
    if pages > 200:
        o = {'prov': PROV, 'year': YEAR, 'school_id': sid, 'num_found': nf,
             'pages': pages, 'error': '超过 200 页翻页上限', 'gate_pass': False}
        emit(o)
        return o

    def expected(p):
        return size if p < pages else nf - (pages - 1) * size

    res, failed = {}, []
    for p in range(1, pages + 1):
        if p == 1 and first is not None:
            items = first
        else:
            _, items = fetch(p)
            items = items or []
        if len(items) < expected(p):
            failed.append(p)
        else:
            res[p] = items
    for p in list(failed):
        for _ in range(8):
            _, items = fetch(p)
            if items and len(items) >= expected(p):
                res[p] = items
                failed.remove(p)
                break
    all_items = []
    for p in sorted(res):
        all_items += res[p]
    ids = [x.get('id') for x in all_items]
    pure = all(str(x.get('school_id')) == str(sid) for x in all_items)
    uniq = len(set(ids))
    gate = {'pages_zero_fail': len(failed) == 0,
            'count_match': len(all_items) == nf,
            'school_pure': pure,
            'unique_ids': uniq == len(all_items)}
    o = {'prov': PROV, 'year': YEAR, 'school_id': sid, 'dimension': 'school_id',
         'num_found': nf, 'count': len(all_items), 'pages': pages,
         'pages_ok': len(res), 'pages_failed': len(failed),
         'unique_ids': uniq, 'school_pure': pure, 'gate': gate,
         'gate_pass': all(gate.values()), 'items': all_items}
    emit(o)
    return o


def make_fetch(sid):
    st = {'ep': None, 'size': 20, 'first': None,
          'worker': 'gh-%s-s%d-%d-%d' % (COMBO.replace('-', '_'), SHARD,
                                         sid, random.randint(1000, 9999))}

    def acquire():
        for i in range(60):
            r = rpc('claim_proxy', {'p_worker': st['worker'],
                                    'p_lease_seconds': 300})
            if r:
                st['ep'] = r[0]['out_endpoint']
                return st['ep']
            # 云端没有本地 revive_loop：拿不到租约时按 ~5s 节奏复活 dead 号，
            # 等价本地 25s 复活循环，防止长跑期间「抖动→判死→黑名单」自加速枯竭。
            if i and i % 12 == 0:
                rpc('revive_dead_proxies',
                    {'p_include_pool': True, 'p_pool_hours': 2})
            time.sleep(0.4)
        return None

    def release(ok):
        if st['ep']:
            rpc('release_proxy', {'p_endpoint': st['ep'], 'p_ok': ok})
            st['ep'] = None

    def fetch(page):
        if page == 1 and st['first'] is not None:
            return st['first']
        q = {'uri': 'apidata/api/gk/score/special',
             'local_province_id': PROV, 'year': YEAR,
             'page': page, 'size': st['size'], 'school_id': sid}
        for _ in range(FETCH_ROUNDS):
            if not st['ep'] and not acquire():
                return None, None
            try:
                d = requests.post(U, json=q, headers=H,
                                  proxies={'http': st['ep'], 'https': st['ep']},
                                  timeout=(6, 14)).json()
                code = str(d.get('code'))
                if code == '0000' and isinstance(d.get('data'), dict):
                    r = (d['data'].get('numFound'), (d['data'].get('item') or []))
                    if page == 1:
                        st['first'] = r
                    return r
                if code == '1090' and page == 1 and st['size'] > 5:
                    st['size'] = 10 if st['size'] == 20 else 5
                    q['size'] = st['size']
                    continue
                release(False)
            except Exception:
                release(False)
        release(False)
        return None, None

    fetch.size_used = lambda: st['size']
    fetch.release = release
    return fetch


def main():
    if not KEY:
        print('::error::SUPABASE_ANON_KEY 未注入，拒绝直连', flush=True)
        sys.exit(1)
    mine = [s for i, s in enumerate(SIDS) if i % SHARDS == SHARD]
    os.makedirs(OUT, exist_ok=True)
    revived = rpc('revive_dead_proxies',
                  {'p_include_pool': True, 'p_pool_hours': 2})
    log('分片 %s shard=%d/%d 本校数=%d 复活dead=%s' %
        (COMBO, SHARD, SHARDS, len(mine), revived))

    def work(sid):
        fx = make_fetch(sid)
        try:
            fx(1)  # 预热探测定 size（1090 时降档），结果缓存
            return collect_school(sid, fx, fx.size_used())
        except Exception as e:
            o = {'prov': PROV, 'year': YEAR, 'school_id': sid,
                 'gate_pass': False, 'error': 'EXC ' + str(e)[:80]}
            emit(o)
            return o
        finally:
            fx.release(True)

    ok = fail = 0
    t0 = time.time()
    with ThreadPoolExecutor(WORKERS) as ex:
        futs = {ex.submit(work, s): s for s in mine}
        for fut in as_completed(futs):
            try:
                o = fut.result()
            except Exception as e:
                o = {'gate_pass': False, 'error': 'FUT ' + str(e)[:60]}
            if o.get('gate_pass'):
                ok += 1
            else:
                fail += 1
                log('  校 %s 未达标: %s' %
                    (futs[fut], o.get('error') or o.get('gate')))
            n = ok + fail
            if n % 50 == 0 or n == len(mine):
                log('进度 %d/%d ok=%d fail=%d %.0fs' %
                    (n, len(mine), ok, fail, time.time() - t0))
    log('分片完成 ok=%d fail=%d（不判 exit，artifact 必须上传，由 merge 统一验收）' %
        (ok, fail))


if __name__ == '__main__':
    main()
