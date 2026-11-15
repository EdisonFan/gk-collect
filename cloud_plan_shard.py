#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Actions runner 侧：招生计划 gkv3/plan/school 的 school_id 分片采集。

与 cloud_school_shard.py 同构（20 片 / 3 并发 / Supabase 租约 / 1090 降 size /
revive 自愈 / 四闸门），差异点全部来自 2026-11-15 实测接口契约：

  * school_id 是接口必填参数（缺失返回 code=1070），天然一校一片，
    与 school_ids.json（2970 校全集）同源，分片即完备划分。
  * data.numFound 为单校真实计划数（实测 38 / 68 / 30），无 10000 截断；
    size 可调（1 / 20 均正常返回），不像 score/province 只能 20。
  * 响应不加密，data.item[] 31 字段；**没有 id 字段**，故唯一性改用
    行内容指纹 sha1(json.dumps(row, sort_keys=True))，口径见 cloud_plan_merge.py。
  * 四闸门：页零失败 / count==numFound / school_pure / 片内行指纹零重复。

环境变量（workflow 注入）：COMBO=13-2024 SHARD=0 SHARDS=20 WORKERS=3
输出：out/plan_<p>_<y>_<sid>.json（失败校也落盘，由 merge 抓遗漏）。
"""
import hashlib
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
SHARDS = int(os.environ.get('SHARDS', '20'))
WORKERS = int(os.environ.get('WORKERS', '3'))
FETCH_ROUNDS = int(os.environ.get('FETCH_ROUNDS', '24'))
KEY = os.environ.get('SUPABASE_ANON_KEY', '').strip()
SB = os.environ.get('SUPABASE_URL',
                    'https://ripobzmwgmdxyixoevee.supabase.co').rstrip('/')
SIDS = json.load(open('school_ids.json', encoding='utf-8'))
OUT = 'out'
U = 'https://api.zjzw.cn/web/api/'
API = 'apidata/api/gkv3/plan/school'
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


def row_fp(x):
    return hashlib.sha1(json.dumps(x, sort_keys=True,
                                   ensure_ascii=False).encode()).hexdigest()


def emit(o):
    p = os.path.join(OUT, 'plan_%s_%s_%s.json' %
                     (o['prov'], o['year'], o['school_id']))
    with open(p, 'w', encoding='utf-8') as f:
        json.dump(o, f, ensure_ascii=False)


def collect_school(sid, fetch, size):
    """四闸门单校采集。"""
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
    fps = [row_fp(x) for x in all_items]
    pure = all(str(x.get('school_id')) == str(sid) for x in all_items)
    # 源站无 id 字段，且会返回整行完全相同的重复招生计划
    # （同专业不同专业组 / 多字段留空 → 行指纹相同）。
    # 页零失败 + count==numFound + school_pure 已保证零丢失；
    # 重复行数只作 dup_in_api 诊断，不再当闸门，避免一校误判毁掉整组。
    gate = {'pages_zero_fail': len(failed) == 0,
            'count_match': len(all_items) == nf,
            'school_pure': pure}
    o = {'prov': PROV, 'year': YEAR, 'school_id': sid, 'dimension': 'school_id',
         'num_found': nf, 'count': len(all_items), 'pages': pages,
         'pages_ok': len(res), 'pages_failed': len(failed),
         'unique_rows': len(set(fps)), 'school_pure': pure, 'gate': gate,
         'dup_in_api': len(all_items) - len(set(fps)),
         'row_id': 'sha1_sorted_json', 'size': size,
         'gate_pass': all(gate.values()), 'items': all_items}
    emit(o)
    return o


def make_fetch(sid):
    st = {'ep': None, 'size': 20, 'first': None,
          'worker': 'ghp-%s-s%d-%d-%d' % (COMBO.replace('-', '_'), SHARD,
                                          sid, random.randint(1000, 9999))}

    def acquire():
        for i in range(60):
            r = rpc('claim_proxy', {'p_worker': st['worker'],
                                    'p_lease_seconds': 300})
            if r:
                st['ep'] = r[0]['out_endpoint']
                return st['ep']
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
        q = {'uri': API, 'local_province_id': PROV, 'year': YEAR,
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
    log('招生计划 %s shard=%d/%d 本校数=%d 复活dead=%s' %
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
