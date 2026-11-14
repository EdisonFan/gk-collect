#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""GitHub Actions runner 上的代理池维护执行体（零直连目标站）。

与 CNB 共享层 pool_keeper.py / proxy_refresh.py 的关键差异：

  1. 全程经代理访问 api.zjzw.cn。runner 出口是 GitHub 共享 IP，绝不能让它
     直连目标站（被限流后会影响后续所有采集任务）。共享层的
     build_variants() 用「直连基线」挑探活查询，这里已移除；_probe 另加
     硬性断言：proxy 为空直接抛错，任何路径都不可能直连。
  2. 改用 api.github.com 检查 runner 出站是否可达，不碰目标站。
  3. 池全烂时仍执行补充换血。共享层「闸门 3：DEAD>=90% 整轮跳过写回」
     会造成死锁：池越烂越不写回（14:31~21:03 连续多轮全灭即此）。
  4. 单轮预算压缩到约 3 分钟，控制私有仓库 Actions 分钟配额。

运行：GH_TOKEN=<GITHUB_TOKEN> python3 pool_keeper_run.py
输出：GITHUB_OUTPUT 写 changed=true/false，供后续步骤决定是否触发镜像。
"""
import base64
import hashlib
import json
import os
import random
import re
import sys
import time
import urllib.error
import urllib.request
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

U = 'https://api.zjzw.cn/web/api/'
HDR = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36',
    'Origin': 'https://gkcx.eol.cn',
    'Referer': 'https://gkcx.eol.cn/',
    'Accept': 'application/json,text/plain,*/*',
    'Content-Type': 'application/json',
}
PROBE_URI = 'apidata/api/gk/score/province'
PROBE_VARIANTS = [
    {'local_province_id': '41', 'year': 2025, 'province_id': '11'},
    {'local_province_id': '11', 'year': 2026, 'province_id': '41'},
    {'local_province_id': '51', 'year': 2025, 'province_id': '65'},
    {'local_province_id': '65', 'year': 2026, 'province_id': '51'},
    {'local_province_id': '37', 'year': 2024, 'province_id': '13'},
    {'local_province_id': '42', 'year': 2026, 'province_id': '11'},
]
SOURCES = [
    ('TheSpeedX', 'https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt'),
    ('clarketm', 'https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt'),
    ('proxifly', 'https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/all/data.txt'),
    ('geonode', 'https://proxylist.geonode.com/api/proxy-list?limit=1000&page=1&sort_by=lastChecked&sort_type=desc'),
]
OWNER, REPO = 'EdisonFan', 'gk-collect'
API_CONTENT = 'https://api.github.com/repos/%s/%s/contents/proxies_cnb.txt' % (OWNER, REPO)

TARGET = 120
MAX_POOL = 200
TIMEOUT = 6
WORKERS = 48               # 探活是 IO 密集，提高并发才能在预算内验证足够多候选
BATCH = 60
CANDIDATE_BUDGET = 1200
MAX_SECONDS = 210          # 整轮硬上限（workflow timeout-minutes: 7）
                           # 首轮 180s 只验证 480 个候选、补到 82；放宽到 210s
                           # 约可验证 1100 个，命中 ~70 个可用，足以补回 120。
IP_PORT = re.compile(r'^\d{1,3}(?:\.\d{1,3}){3}:\d{2,5}$')


def log(msg):
    print('[%s] %s' % (time.strftime('%H:%M:%S'), msg), flush=True)


def set_output(changed):
    out = os.environ.get('GITHUB_OUTPUT')
    if out:
        with open(out, 'a', encoding='utf-8') as f:
            f.write('changed=%s\n' % ('true' if changed else 'false'))


def _gh(path, token, data=None, method='GET', timeout=25):
    headers = {'Accept': 'application/vnd.github+json',
               'User-Agent': 'gk-pool-keeper-runner'}
    if token:
        headers['Authorization'] = 'Bearer ' + token
    body = None
    if data is not None:
        headers['Content-Type'] = 'application/json'
        body = json.dumps(data, ensure_ascii=False).encode()
    req = urllib.request.Request(path, data=body, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        raw = r.read().decode()
    return json.loads(raw) if raw.strip() else {}


def read_pool(token):
    d = _gh(API_CONTENT + '?ref=main', token)
    txt = base64.b64decode(d['content']).decode('utf-8')
    return [x.strip() for x in txt.splitlines() if x.strip()], d['sha']


def write_pool(token, lines, sha):
    content = base64.b64encode(('\n'.join(lines) + '\n').encode()).decode()
    for attempt in range(2):
        try:
            _gh(API_CONTENT, token,
                {'message': 'proxy pool keeper (runner): %d (target %d)' % (len(lines), TARGET),
                 'content': content, 'sha': sha, 'branch': 'main'},
                method='PUT', timeout=30)
            return True, ''
        except urllib.error.HTTPError as e:
            if e.code == 409 and attempt == 0:
                _, sha = read_pool(token)
                continue
            return False, 'HTTP %d' % e.code
        except Exception as e:
            return False, type(e).__name__
    return False, 'retry exhausted'


def _body(variant, page=1):
    d = {'uri': PROBE_URI, 'page': page, 'size': 20}
    d.update(variant)
    return json.dumps(d).encode()


def _probe(proxy, body):
    """三态探活：OK / THROTTLE / DEAD。硬性禁止直连。"""
    if not proxy:
        raise RuntimeError('runner 禁止直连目标站（proxy 为空）')
    try:
        op = urllib.request.build_opener(
            urllib.request.ProxyHandler({'http': proxy, 'https': proxy}))
        req = urllib.request.Request(U, data=body, headers=HDR, method='POST')
        with op.open(req, timeout=TIMEOUT) as r:
            d = json.loads(r.read().decode('utf-8', 'replace'))
        code = str(d.get('code'))
        if (code == '0000' and isinstance(d.get('data'), dict)
                and len(d['data'].get('item') or []) > 0):
            return 'OK'
        if code in ('1069', '1074'):
            return 'THROTTLE'
        return 'DEAD'
    except RuntimeError:
        raise
    except Exception:
        return 'DEAD'


def _h(px):
    return int(hashlib.md5(px.encode()).hexdigest()[:8], 16)


def verdict_existing(px, variants):
    """在池代理：双次确认，OK 优先。A2 后 keep 只收 OK，THROTTLE 剔除（仅计数观察）。"""
    h = _h(px)
    r1 = _probe(px, _body(variants[h % len(variants)]))
    if r1 == 'OK':
        return 'OK'
    r2 = _probe(px, _body(variants[(h + 1) % len(variants)]))
    if r2 == 'OK':
        return 'OK'
    if 'THROTTLE' in (r1, r2):
        return 'THROTTLE'
    return 'DEAD'


def verdict_new(px, variants):
    """A2: 新增候选双次确认（两个不同变体），只收两次都 OK 的号。
    对齐本地补货口径，避免单次 OK 的抖动号入池。"""
    h = _h(px)
    r1 = _probe(px, _body(variants[h % len(variants)]))
    if r1 != 'OK':
        return 'DEAD'
    r2 = _probe(px, _body(variants[(h + 1) % len(variants)]))
    return 'OK' if r2 == 'OK' else 'DEAD'


def _parse_source(name, body):
    out = []
    if name == 'geonode':
        try:
            for x in json.loads(body).get('data', []):
                protos = x.get('protocols') or []
                if 'http' in protos or 'http' in str(x.get('protocol', '')).lower():
                    ip, port = x.get('ip'), x.get('port')
                    if ip and port:
                        out.append('http://%s:%s' % (ip, port))
        except Exception:
            pass
        return out
    for line in body.splitlines():
        s = line.strip()
        if s.startswith('http://') and s.count('://') == 1:
            out.append(s)
        elif IP_PORT.match(s):
            out.append('http://' + s)
    return out


def fetch_candidates(exclude):
    cand, seen = [], set(exclude)
    for name, url in SOURCES:
        try:
            req = urllib.request.Request(url, headers={'User-Agent': 'gk-pool-keeper-runner'})
            with urllib.request.urlopen(req, timeout=20) as r:
                body = r.read().decode('utf-8', 'replace')
            added = 0
            for p in _parse_source(name, body):
                if p not in seen:
                    seen.add(p)
                    cand.append(p)
                    added += 1
            log('  [源] %-10s 新增 %d' % (name, added))
        except Exception as e:
            log('  [源] %-10s 失败 %s' % (name, type(e).__name__))
    random.shuffle(cand)
    return cand


def network_ok():
    """runner 出站连通性检查：只访问 api.github.com，不碰目标站。"""
    try:
        urllib.request.urlopen('https://api.github.com', timeout=8).read(1)
        return True
    except Exception:
        return False


def main():
    token = os.environ.get('GH_TOKEN', '').strip()
    if not token:
        log('缺少 GH_TOKEN，整轮跳过')
        set_output(False)
        return
    t0 = time.time()
    if not network_ok():
        log('runner 出站不可达，整轮跳过（不写回、不删代理）')
        set_output(False)
        return
    log('runner 出站 OK；全程零直连目标站')

    # 全 6 个变体直接用于错峰探活，不做「直连基线」筛选
    variants = list(PROBE_VARIANTS)

    try:
        pool, sha = read_pool(token)
    except Exception as e:
        log('读取池文件失败 %s，整轮跳过' % type(e).__name__)
        set_output(False)
        return
    log('现有池 %d 个，开始全池探活（%ds 超时 / %d 并发 / 双次确认）'
        % (len(pool), TIMEOUT, WORKERS))
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        verdicts = list(ex.map(lambda p: verdict_existing(p, variants), pool))
    keep = [p for p, v in zip(pool, verdicts) if v == 'OK']
    log('探活结果 %s：保留 %d / 剔除 %d，耗时 %.0fs'
        % (dict(Counter(verdicts)), len(keep), len(pool) - len(keep), time.time() - t0))

    need = max(0, TARGET - len(keep))
    added, checked = [], 0
    if need > 0 and time.time() - t0 < MAX_SECONDS:
        cand = [c for c in fetch_candidates(set(pool)) if c not in set(pool)][:CANDIDATE_BUDGET]
        log('候选 %d 个，需补 %d，开始验证' % (len(cand), need))
        i = 0
        while (i < len(cand) and len(added) < need
               and time.time() - t0 < MAX_SECONDS):
            batch = cand[i:i + BATCH]
            i += BATCH
            checked += len(batch)
            with ThreadPoolExecutor(max_workers=WORKERS) as ex:
                for px, r in zip(batch, ex.map(lambda q: verdict_new(q, variants), batch)):
                    if r == 'OK':
                        added.append(px)
        log('补充结束：验证 %d，可用 %d，耗时 %.0fs'
            % (checked, len(added), time.time() - t0))
    else:
        log('已达目标规模或超出预算，跳过补充')

    new_pool = list(dict.fromkeys(keep + added))[:MAX_POOL]
    log('最终池 %d 个（保留 %d + 新增 %d）' % (len(new_pool), len(keep), len(added)))
    if not new_pool:
        log('合并后为空池，放弃写回（保住原池）')
        set_output(False)
        return
    if set(new_pool) == set(pool):
        log('池内容无变化，跳过写回')
        set_output(False)
        return
    ok, err = write_pool(token, new_pool, sha)
    log('写回 GitHub：%s %s' % (ok, err))
    set_output(ok)
    if not ok:
        sys.exit(1)


if __name__ == '__main__':
    main()
