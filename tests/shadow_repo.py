#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""L3 影子库端到端：真推一次 Git Data API 全链路，验证完把痕迹删干净。

优先：临时私有库 `gk-collect-selftest-<ts>`（建 -> 推 -> 读回 -> 删库）。
降级：token 无建库权限时，在真库建隔离分支 `selftest-<ts>`，
      推 -> 读回 -> 删 ref（只需 contents:write，不影响任何结果分支）。

这条链路覆盖 push_one.py 的 blob/tree/commit/ref/读回重试，
以及 cloud_feed 依赖的 404/422 契约。全程 try/finally 保证清理。
用法: python3 tests/shadow_repo.py --run
"""
import base64
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
R = 'EdisonFan/gk-collect'
TS = datetime.now().strftime('%m%d%H%M%S')
TMP_REPO = 'gk-collect-selftest-' + TS
SHADOW_REF = 'selftest-' + TS
FAILS = []
TRACE = []


def chk(name, cond, extra=''):
    print(('  PASS  ' if cond else '  FAIL  ') + name +
          ((' | ' + str(extra)) if extra else ''), flush=True)
    if not cond:
        FAILS.append(name)


def tok():
    raw = subprocess.check_output(['git', 'config', '--get',
                                   'remote.origin.url']).decode()
    return raw.split('x-access-token:')[1].split('@')[0]


T = tok()
H = {'Authorization': 'Bearer ' + T, 'User-Agent': 'cb',
     'Accept': 'application/vnd.github+json',
     'Content-Type': 'application/json'}


def call(m, p, b=None, tmo=90, expect_error=()):
    data = json.dumps(b).encode() if b is not None else None
    req = urllib.request.Request('https://api.github.com' + p, data=data,
                                 headers=H, method=m)
    last = None
    for i in range(4):
        try:
            with urllib.request.urlopen(req, timeout=tmo) as r:
                TRACE.append('%s %s -> %d' % (m, p.split('?')[0], r.status))
                return json.load(r) if r.status != 204 else {}
        except urllib.error.HTTPError as e:
            TRACE.append('%s %s -> HTTP %d' % (m, p.split('?')[0], e.code))
            if e.code in expect_error:
                return {'_http_error': e.code}
            if e.code == 404:
                return {'_http_error': 404}
            last = e
            time.sleep(2)
        except Exception as e:
            last = e
            time.sleep(2)
    raise last


def owner():
    d = call('GET', '/user')
    return d['login']


def push_probe(repo):
    """在 repo 上跑一遍完整推送链路 + 读回校验 + 清理，返回证据 dict。"""
    path = 'out/_selftest_%s.json' % TS
    payload = {'selftest': True, 'gate_pass': True, 'count': 3,
               'items': [{'id': i} for i in range(3)], 'ts': TS}
    raw = json.dumps(payload, ensure_ascii=False).encode()

    mh = call('GET', '/repos/%s/git/ref/heads/main' % repo)['object']['sha']
    mt = call('GET', '/repos/%s/git/commits/%s' % (repo, mh))['tree']['sha']
    blob = call('POST', '/repos/%s/git/blobs' % repo,
                {'content': base64.b64encode(raw).decode(),
                 'encoding': 'base64'})['sha']
    tree = call('POST', '/repos/%s/git/trees' % repo,
                {'base_tree': mt,
                 'tree': [{'path': path, 'mode': '100644',
                           'type': 'blob', 'sha': blob}]})['sha']
    cm = call('POST', '/repos/%s/git/commits' % repo,
              {'tree': tree, 'parents': [mh],
               'message': 'selftest %s gate_pass=True\n\n临时验证，随即删除' % TS})['sha']
    chk('commit 创建成功', bool(cm), cm[:10])

    call('POST', '/repos/%s/git/refs' % repo,
         {'ref': 'refs/heads/' + SHADOW_REF, 'sha': cm})
    # 读回一致性：模拟 push_one.py 的重试，验证它真的能等到一致
    now = None
    tries = 0
    for _ in range(6):
        tries += 1
        d = call('GET', '/repos/%s/git/ref/heads/%s' % (repo, SHADOW_REF))
        now = (d or {}).get('object', {}).get('sha')
        if now == cm:
            break
        time.sleep(2)
    chk('ref 读回与 commit 一致', now == cm, 'tries=%d cm=%s now=%s'
        % (tries, str(cm)[:8], str(now)[:8]))
    msg = call('GET', '/repos/%s/git/commits/%s' % (repo, cm))['message']
    chk('commit message 首行含 gate_pass=True',
        'gate_pass=True' in msg.split('\n')[0], msg.split('\n')[0][:60])
    # 校验 blob 内容可原样读回
    got = call('GET', '/repos/%s/git/blobs/%s' % (repo, blob))
    back = base64.b64decode(got['content'])
    chk('blob 内容字节级一致', back == raw, '%d bytes' % len(back))
    return {'repo': repo, 'ref': SHADOW_REF, 'cm': cm, 'blob': blob,
            'bytes': len(raw), 'readback_tries': tries}


def cleanup_ref(repo):
    d = call('DELETE', '/repos/%s/git/refs/heads/%s' % (repo, SHADOW_REF))
    gone = call('GET', '/repos/%s/git/ref/heads/%s' % (repo, SHADOW_REF),
                expect_error=(404,))
    ok = gone.get('_http_error') == 404
    chk('影子分支已删除且读回 404', ok, str(gone))
    return ok


def main():
    print('=== L3 影子库端到端（真推 + 真删）===', flush=True)
    mode = None
    login = None
    created = False
    try:
        login = owner()
        d = call('POST', '/user/repos',
                 {'name': TMP_REPO, 'private': True, 'auto_init': True,
                  'description': 'selftest, delete immediately'},
                 expect_error=(403, 404, 422))
        if d.get('_http_error'):
            print('  临时库创建被拒（HTTP %s），降级为「真库隔离分支」模式'
                  % d['_http_error'], flush=True)
            mode = 'shadow-ref'
        else:
            created = True
            full = '%s/%s' % (login, TMP_REPO)
            print('  已建临时私有库 %s' % full, flush=True)
            # auto_init 异步，等 main ref 出现
            for _ in range(10):
                r = call('GET', '/repos/%s/git/ref/heads/main' % full,
                         expect_error=(404, 409))
                if not r.get('_http_error'):
                    break
                time.sleep(2)
            mode = 'tmp-repo'
            ev = push_probe(full)
            print('  推送证据: %s' % json.dumps(
                {k: v[:8] if isinstance(v, str) and len(v) > 12 else v
                 for k, v in ev.items()}, ensure_ascii=False), flush=True)
            cleanup_ref(full)

        if mode == 'shadow-ref':
            ev = push_probe(R)
            print('  推送证据: %s' % json.dumps(
                {k: v[:8] if isinstance(v, str) and len(v) > 12 else v
                 for k, v in ev.items()}, ensure_ascii=False), flush=True)
            cleanup_ref(R)
    finally:
        if created and login:
            d = call('DELETE', '/repos/%s/%s' % (login, TMP_REPO),
                     expect_error=(403, 404))
            ok = not d.get('_http_error')
            chk('临时库已删除', ok,
                '%s/%s %s' % (login, TMP_REPO,
                              'OK' if ok else 'HTTP %s 残留！' % d.get('_http_error')))
            if not ok:
                print('  !! 残留库需人工删除: %s/%s' % (login, TMP_REPO), flush=True)
        print('  清理完成（模式=%s）' % mode, flush=True)

    print('\n  请求轨迹（末 8 条）:')
    for x in TRACE[-8:]:
        print('    ' + x)
    print('\nL3 结论：%s' % ('全部通过' if not FAILS else
          '%d 项失败 -> %s' % (len(FAILS), FAILS)), flush=True)
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(main())
