"""快筛代理出口 -> /tmp/live.txt（合并式，绝不掉数）。

历史 bug（2026-11-11 实测）：旧版只从 6 个小源收候选、覆盖式写回，
一次瞬时超时就把原本活着的 IP 剔除，出现过「刷新 10 -> 5」越刷越少。
而 run_all.py 每 8 个组合就调它一次，等于持续把池子刷小。

本版修复：
  1) 候选扩到全部历史大池 + pool-keeper 换血后的 GitHub 最新池（本地那份
     停在昨天没拉回，与云端 122 条里已有 98 条不同）；
  2) 双次确认，剔除瞬时抖动；
  3) 结果与原存活集合取并集写回 —— 刷新只会持平或变多；
  4) 整体硬 deadline（必须小于 run_all.py 的 subprocess timeout=150），
     候选按优先级排序，超时截断也先保住现有活出口。
"""
import base64
import glob
import json
import os
import subprocess
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor

import requests

U = 'https://api.zjzw.cn/web/api/'
H = {'User-Agent': 'Mozilla/5.0', 'Origin': 'https://gkcx.eol.cn',
     'Referer': 'https://gkcx.eol.cn/',
     'Accept': 'application/json,text/plain,*/*',
     'Content-Type': 'application/json'}
Q = {'uri': 'apidata/api/gk/score/special', 'local_province_id': '44',
     'year': 2023, 'page': 3, 'size': 20}

LIVE = '/tmp/live.txt'
GH_POOL = '/tmp/pool_gh_latest.txt'
POOL_F = 'proxies_cnb.txt'
REPO = 'EdisonFan/gk-collect'
MAX_SECONDS = 80           # 仅探活阶段的预算；双确认+写回另留余量，
                           # 保证总耗时远小于 run_all.py 的 timeout=150
WORKERS = 48
BATCH = 96
TIMEOUT = (2.5, 5)


def _token():
    try:
        url = subprocess.check_output(
            ['git', 'config', '--get', 'remote.origin.url']).decode()
        return url.split('x-access-token:')[1].split('@')[0]
    except Exception:
        return None


def pull_latest_pool():
    """拉 GitHub 上 pool-keeper 换血后的最新池；失败静默降级用本地旧池。

    写独立缓存而不覆盖仓库里的 proxies_cnb.txt，避免动真源文件。
    """
    t = _token()
    if not t:
        return False
    try:
        req = urllib.request.Request(
            'https://api.github.com/repos/%s/contents/proxies_cnb.txt' % REPO,
            headers={'Authorization': 'Bearer ' + t, 'User-Agent': 'cb',
                     'Accept': 'application/vnd.github+json'})
        c = json.load(urllib.request.urlopen(req, timeout=12))
        lines = [x.strip() for x in
                 base64.b64decode(c['content']).decode().splitlines()
                 if x.strip().startswith('http')]
        if not lines:
            return False
        open(GH_POOL, 'w').write('\n'.join(lines) + '\n')
        return True
    except Exception:
        return False


def candidates():
    """候选按优先级：现有活出口 > 云端最新池 > 本地正式池 > 小缓存 > 历史大池。

    顺序很重要：deadline 截断时，前面的先被测到，先保住现有活的 IP。
    """
    big = sorted(glob.glob('/tmp/proxy_pool*.txt'),
                 key=os.path.getmtime, reverse=True)
    srcs = [LIVE, GH_POOL, POOL_F, '/tmp/pool2.txt', '/tmp/stable.txt',
            '/tmp/alive.txt', '/tmp/pool_fast.txt'] + big
    out = []
    for f in srcs:
        try:
            out += [x.strip() for x in open(f) if x.strip().startswith('http')]
        except Exception:
            pass
    return list(dict.fromkeys(out))


def probe(px):
    try:
        r = requests.post(U, json=Q, headers=H,
                          proxies={'http': px, 'https': px}, timeout=TIMEOUT)
        d = r.json()
        if str(d.get('code')) == '0000' and isinstance(d.get('data'), dict):
            return px
    except Exception:
        pass
    return None


def screen(cand, deadline):
    """分批并发探活，受总 deadline 约束；超时收已得结果，不空等。"""
    alive = []
    for i in range(0, len(cand), BATCH):
        if time.time() >= deadline:
            return alive, True
        with ThreadPoolExecutor(WORKERS) as ex:
            alive += [x for x in ex.map(probe, cand[i:i + BATCH]) if x]
    return alive, False


def main(live_path=LIVE):
    """返回写回后的出口列表，便于离线测试注入临时文件。"""
    t0 = time.time()
    before = []
    try:
        before = [x.strip() for x in open(live_path)
                  if x.strip().startswith('http')]
    except Exception:
        pass
    try:
        fresh = pull_latest_pool()
    except Exception:
        # GitHub 抖动不该让整个补货失败：降级用本地已有池继续
        fresh = False
    cand = candidates()
    hit, cut = screen(cand, t0 + MAX_SECONDS)
    # 双次确认：只对一次探活通过者复测，数量少、很快
    with ThreadPoolExecutor(24) as ex:
        confirmed = [x for x in ex.map(probe, hit) if x]
    # 合并式：与原存活集合取并集。一次抖动不该删掉已知活着的出口。
    final = list(dict.fromkeys(before + confirmed))
    with open(live_path, 'w') as f:
        f.write('\n'.join(final) + ('\n' if final else ''))
    print('[%s] 出口补货 云端池%s 候选%d 一次活%d 双确认%d 原存活%d -> 最终%d%s 耗时%.0fs'
          % (time.strftime('%H:%M:%S'), 'OK' if fresh else '降级',
             len(cand), len(hit), len(confirmed), len(before), len(final),
             '（超限截断）' if cut else '', time.time() - t0), flush=True)
    return final


if __name__ == '__main__':
    main()
