#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""L2 真实 API 契约（只读）：验证 GitHub 实际返回码，不猜。

重点：不存在的结果分支究竟返回 404 还是 422、cf.api 有没有偷偷重试。
用法: python3 tests/api_contract.py
"""
import os
import sys
import time
import urllib.error
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

import cloud_feed as cf

FAILS = []


def chk(name, cond, extra=''):
    print(('  PASS  ' if cond else '  FAIL  ') + name +
          ((' | ' + str(extra)) if extra else ''), flush=True)
    if not cond:
        FAILS.append(name)


def main():
    print('=== L2 真实 API 契约（只读）===', flush=True)
    t = cf.tok()

    # 1) 不存在的分支：拿原始状态码，并确认 cf.api 不重试（耗时应 < 6s）
    path = '/repos/%s/commits/special-99-2099' % cf.R
    req = urllib.request.Request('https://api.github.com' + path,
                                 headers={'Authorization': 'Bearer ' + t,
                                          'User-Agent': 'cb',
                                          'Accept': 'application/vnd.github+json'})
    code = None
    try:
        urllib.request.urlopen(req, timeout=30)
        code = 200
    except urllib.error.HTTPError as e:
        code = e.code
    chk('不存在分支返回 404 或 422', code in (404, 422), '实际=%s' % code)

    t0 = time.time()
    r = cf.api(path, t)
    dt = time.time() - t0
    chk('cf.api 对不存在分支返回 None', r is None, 'got=%r' % type(r).__name__)
    chk('cf.api 未触发重试退避（<6s）', dt < 6, '耗时=%.2fs' % dt)

    # 2) 已达标分支必须能被 gate_ok 识别
    good = cf.api('/repos/%s/commits/special-46-2026' % cf.R, t)
    ok = bool(good and 'gate_pass=True' in
              good['commit']['message'].split('\n')[0])
    chk('达标分支首行含 gate_pass=True', ok)
    chk('gate_ok(46-2026) = True', cf.gate_ok('46-2026', t) is True)

    # 3) 不存在分支 gate_ok 不崩
    try:
        v = cf.gate_ok('99-2099', t)
        chk('gate_ok(不存在分支) = False 且不抛异常', v is False, 'got=%r' % v)
    except Exception as e:
        chk('gate_ok(不存在分支) 不抛异常', False, '%s: %s' % (type(e).__name__, e))

    # 4) jobs 接口结构符合 run_done 假设
    d = cf.api('/repos/%s/actions/runs/43005866920/jobs?per_page=100' % cf.R, t)
    jobs = (d or {}).get('jobs', [])
    chk('jobs 接口返回非空数组', isinstance(jobs, list) and len(jobs) > 0,
        'n=%d' % len(jobs))
    chk('每个 job 有 status 字段', all('status' in j for j in jobs))
    chk('run_done(第一批) 与 jobs 实际一致',
        cf.run_done(43005866920, t) ==
        all(j['status'] == 'completed' for j in jobs))

    # 5) branches 列表分页字段（remote_ok 依赖）
    bs = cf.api('/repos/%s/branches?per_page=100&page=1' % cf.R, t)
    chk('branches 返回数组且含 name', isinstance(bs, list) and
        all('name' in b for b in bs), 'n=%d' % len(bs))

    print('\nL2 结论：%s' % ('全部通过' if not FAILS else
          '%d 项失败 -> %s' % (len(FAILS), FAILS)), flush=True)
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(main())
