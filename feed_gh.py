# -*- coding: utf-8 -*-
"""GitHub Actions 派发器：把 (省,年) 组合铺进 gk-cutoff workflow。

用法：
  python3 feed_gh.py --combos 23-2023 23-2024 31-2021
  python3 feed_gh.py --file gh_todo.json --limit 48   # 从缺口清单派
  python3 feed_gh.py --status                        # 只查近期 run 状态

特性：
  - 一次 dispatch 可带多个组合，GitHub 按 20 并发自动排队，无需轮询触发频率
  - dispatch 撞限流(403/422)时指数退避重试
  - 台账 gh_queue.json 记录已派组合，避免重复派发造成分支写竞态
"""
import os, sys, json, time, urllib.request, urllib.error, argparse

HERE = os.path.dirname(os.path.abspath(__file__))
OWNER, REPO = 'EdisonFan', 'gk-collect'
WORKFLOW_ID = 410987787
API = 'https://api.github.com/repos/%s/%s/actions/workflows/%d/dispatches' % (OWNER, REPO, WORKFLOW_ID)
RUNS_API = 'https://api.github.com/repos/%s/%s/actions/workflows/%d/runs?per_page=10' % (OWNER, REPO, WORKFLOW_ID)
TOKEN_FILE = '/workspace/.secrets/github_pat'
LOG = os.path.join(HERE, 'gh_queue.json')
RETRY_GAPS = (15, 30, 60, 120)


def token():
    return open(TOKEN_FILE, encoding='utf-8').read().strip()


def load_log():
    if os.path.exists(LOG):
        try:
            d = json.load(open(LOG, encoding='utf-8'))
            d.setdefault('dispatched', {})
            d.setdefault('runs', [])
            return d
        except Exception:
            pass
    return {'dispatched': {}, 'runs': []}


def save_log(d):
    tmp = LOG + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    os.replace(tmp, LOG)


def dispatch(combos, tag='', relay=None):
    inputs = {'combos': json.dumps(combos, ensure_ascii=False),
              'tag': tag or ('batch-%s' % time.strftime('%H%M%S'))}
    # 必须显式传 relay：workflow_dispatch 未带该 input 时，job 级
    # if: inputs.relay == 'on' 取到空值，relay job 根本不会创建（实测 #14
    # 167 个 job 全是 collect，自接力链条断了）。显式传值才能稳定生效。
    inputs['relay'] = relay or 'on'
    body = json.dumps({'ref': 'main', 'inputs': inputs},
                      ensure_ascii=False).encode('utf-8')
    tok = token()
    for gap in [0] + list(RETRY_GAPS):
        if gap:
            print('  限流，%ds 后重试...' % gap, flush=True)
            time.sleep(gap)
        req = urllib.request.Request(
            API, data=body,
            headers={'Authorization': 'Bearer ' + tok,
                     'Accept': 'application/vnd.github+json',
                     'Content-Type': 'application/json'}, method='POST')
        try:
            urllib.request.urlopen(req, timeout=30)
            return True
        except urllib.error.HTTPError as e:
            msg = e.read().decode(errors='ignore')[:200]
            print('  HTTP %d %s' % (e.code, msg), flush=True)
            if e.code not in (403, 422, 429):
                return False
        except Exception as e:
            print('  %s: %s' % (type(e).__name__, str(e)[:120]), flush=True)
            return False
    return False


def status():
    req = urllib.request.Request(
        RUNS_API, headers={'Authorization': 'Bearer ' + token(),
                           'Accept': 'application/vnd.github+json'})
    try:
        d = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
    except Exception as e:
        print('查询失败', type(e).__name__); return
    print('近期 run %d 个：' % len(d.get('workflow_runs', [])))
    for r in d.get('workflow_runs', [])[:10]:
        print('  #%-4s %-12s %-12s %s' %
              (r['run_number'], r['status'], r.get('conclusion') or '-',
               r['created_at']))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--combos', nargs='+')
    ap.add_argument('--file', help='缺口清单 json，含 missing/low 数组')
    ap.add_argument('--limit', type=int, default=40)
    ap.add_argument('--status', action='store_true')
    ap.add_argument('--force', action='store_true', help='忽略台账，强制重派')
    ap.add_argument('--relay', choices=['on', 'off'],
                    help='是否启用云端自接力（单组合验证可设 off）')
    a = ap.parse_args()
    if a.status:
        status(); return

    log = load_log()
    if a.combos:
        combos = a.combos
    elif a.file:
        d = json.load(open(a.file, encoding='utf-8'))
        combos = list(dict.fromkeys(d.get('missing', []) + d.get('low', [])))
    else:
        raise SystemExit('需指定 --combos 或 --file')

    if not a.force:
        combos = [c for c in combos if c not in log['dispatched']]
    combos = combos[:a.limit]
    if not combos:
        print('无可派组合（台账已覆盖）', flush=True); return

    print('派发 %d 个组合: %s ...' % (len(combos), ' '.join(combos[:5])), flush=True)
    tag = 'gh-%s' % time.strftime('%m%d-%H%M%S')
    if not dispatch(combos, tag, a.relay):
        raise SystemExit('派发失败，组合保留在待派清单')
    now = time.time()
    for c in combos:
        prev = log['dispatched'].get(c, {})
        # 保留/累加 tries：--force 重派时不能清零，否则巡航的 MAX_RETRY
        # 永远不生效，会对同一批低覆盖组合无限重派。
        log['dispatched'][c] = {'at': now, 'tag': tag,
                                'tries': prev.get('tries', 0) + 1}
    log['runs'].append({'tag': tag, 'n': len(combos),
                        'combos': combos, 'at': time.strftime('%F %T')})
    save_log(log)
    print('✅ 已派发 %s，台账累计 %d 个组合' % (tag, len(log['dispatched'])), flush=True)


if __name__ == '__main__':
    main()
