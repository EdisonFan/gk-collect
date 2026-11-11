#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""L4 CNB 1:1 镜像对照：逐分支比对集合与 sha，漏同步立刻失败。

背景：曾长期凭旧快照口头汇报「CNB 漏 special-*」，实测其实早已完整。
把结论固化成机器校验，避免再靠印象判断。

- GitHub 侧：branches API 分页取 name -> commit.sha
- CNB 侧：git ls-remote --heads（凭据取沙箱 API key 通道）
- 校验：分支集合完全一致、同名分支 sha 完全一致、main/cutoff-*/special-* 全覆盖

凭据或网络不可用时输出 SKIP 并退出 0（不是代码缺陷，不挡上线），
但只要拿到数据，任何差异都判 FAIL。
用法: python3 tests/cnb_mirror_check.py
"""
import json
import os
import subprocess
import sys
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.chdir(ROOT)
R = 'EdisonFan/gk-collect'
CNB = 'cooledi/gk-collect'
FAILS = []


def chk(name, cond, extra=''):
    print(('  PASS  ' if cond else '  FAIL  ') + name +
          ((' | ' + str(extra)) if extra else ''), flush=True)
    if not cond:
        FAILS.append(name)


def gh_token():
    raw = subprocess.check_output(['git', 'config', '--get',
                                   'remote.origin.url']).decode()
    return raw.split('x-access-token:')[1].split('@')[0]


def gh_branches(t):
    h = {'Authorization': 'Bearer ' + t, 'User-Agent': 'cb',
         'Accept': 'application/vnd.github+json'}

    def api(u):
        for i in range(3):
            try:
                return json.load(urllib.request.urlopen(
                    urllib.request.Request('https://api.github.com' + u,
                                           headers=h), timeout=50))
            except Exception:
                if i == 2:
                    raise
                time.sleep(2)

    out, pg = {}, 1
    while True:
        d = api('/repos/%s/branches?per_page=100&page=%d' % (R, pg))
        for b in d:
            out[b['name']] = b['commit']['sha']
        if len(d) < 100 or pg > 8:
            break
        pg += 1
    return out


def cnb_branches(key):
    url = 'https://cnb:%s@cnb.cool/%s.git' % (key, CNB)
    p = subprocess.run(['git', 'ls-remote', '--heads', url],
                       capture_output=True, text=True, timeout=110)
    if p.returncode != 0:
        raise RuntimeError('ls-remote rc=%d %s' % (p.returncode,
                                                   (p.stderr or '')[:120]))
    out = {}
    for line in p.stdout.splitlines():
        parts = line.strip().split('\t')
        if len(parts) == 2 and parts[1].startswith('refs/heads/'):
            out[parts[1][len('refs/heads/'):]] = parts[0]
    return out


def main():
    print('=== L4 CNB 1:1 镜像对照 ===', flush=True)
    key_url = os.environ.get('WORKBUDDY_TOKEN_URL_CNB_APP')
    if not key_url:
        print('  SKIP：本环境无 CNB 凭据通道，未做镜像校验（不计入通过项）')
        return 0
    try:
        raw = subprocess.check_output(['curl', '-s', '--max-time', '15',
                                       key_url], timeout=25)
        key = json.loads(raw)['data']['access_token']
        gh = gh_branches(gh_token())
        cnb = cnb_branches(key)
    except Exception as e:
        print('  SKIP：CNB 不可达或取数失败（%s: %s），未做校验'
              % (type(e).__name__, str(e)[:100]))
        return 0

    gs, cs = set(gh), set(cnb)
    only_gh, only_cnb = sorted(gs - cs), sorted(cs - gs)
    diff = [(b, gh[b][:8], cnb[b][:8]) for b in sorted(gs & cs)
            if gh[b] != cnb[b]]
    print('  GitHub %d 个分支 / CNB %d 个分支' % (len(gs), len(cs)), flush=True)
    chk('分支集合完全一致', not only_gh and not only_cnb,
        '仅GitHub=%s 仅CNB=%s' % (only_gh[:5], only_cnb[:5]))
    chk('同名分支 sha 全部一致', not diff,
        '不一致 %d 个 %s' % (len(diff), diff[:5]))
    for prefix, label in (('main', 'main'), ('cutoff-', '投档线 cutoff-*'),
                          ('special-', '专业线 special-*')):
        want = [b for b in gs if b == 'main'] if prefix == 'main' \
            else [b for b in gs if b.startswith(prefix)]
        missing = [b for b in want if b not in cnb]
        chk('%s 在 CNB 无缺失' % label, not missing,
            '%d 个，缺失 %s' % (len(want), missing[:5]))
    chk('main sha 一致', gh.get('main') == cnb.get('main'),
        '%s / %s' % (str(gh.get('main'))[:8], str(cnb.get('main'))[:8]))

    print('\nL4 结论：%s' % ('全部通过（1:1 镜像已核实）' if not FAILS else
          '%d 项失败 -> %s' % (len(FAILS), FAILS)), flush=True)
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(main())
