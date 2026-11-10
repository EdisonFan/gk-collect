# -*- coding: utf-8 -*-
"""专业录取线派发器：把 (省,年) 组合铺进 gk-special workflow。

与 feed_gh.py（投档线）完全隔离：不同 workflow、不同台账、不同结果分支。
用 workflow 文件名 dispatch，无需预先查 WORKFLOW_ID。

v2：派发前先读 scale_profile.json 拿到每个组合的预估工作量，按档位分批，
避免「大小省混装」——大省单 job 跑 150+ 分钟会占满 max-parallel:3 的槽，
让 8 分钟就能过的小省在后面干等。这是 163 个组合积压的直接原因。

分批规则：
  1. 轻档优先（快速提高覆盖率），重档单独成批、不与其他档混装；
  2. 每批按「预估总分钟数」预算截断，而不是按组合个数；
     预算默认 180 分钟（≈ 3 并发下 60 分钟墙钟），重档组合强制单批；
  3. 台账记录 est_pages / est_minutes / tier，采完后可回填真实值对比。

用法：
  python3 feed_special.py --plan --tier light --limit 30     # 先看分批计划，不派
  python3 feed_special.py --tier light --limit 30            # 正式派发轻档
  python3 feed_special.py --combos 13-2023 --force           # 指定组合强制重派
  python3 feed_special.py --status
"""
import os, json, time, urllib.request, urllib.error, argparse

HERE = os.path.dirname(os.path.abspath(__file__))
OWNER, REPO = 'EdisonFan', 'gk-collect'
WF = 'collect-special.yml'
API = 'https://api.github.com/repos/%s/%s/actions/workflows/%s/dispatches' % (OWNER, REPO, WF)
RUNS_API = 'https://api.github.com/repos/%s/%s/actions/workflows/%s/runs?per_page=10' % (OWNER, REPO, WF)
TOKEN_FILE = '/workspace/.secrets/github_pat'
LOG = os.path.join(HERE, 'gh_queue_special.json')
PROFILE = os.path.join(HERE, 'scale_profile.json')
RETRY_GAPS = (15, 30, 60, 120)
DEFAULT_BUDGET = 180          # 每批预估总分钟数预算
TIER_ORDER = {'light': 0, 'medium': 1, 'heavy': 2}


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


def load_profile():
    """读取规模档位表。文件缺失时返回空表，派发退化为按个数分批并给出警告。"""
    if not os.path.exists(PROFILE):
        print('⚠ 未找到 scale_profile.json，本次按组合个数分批（无工作量预估）', flush=True)
        return {}
    try:
        return json.load(open(PROFILE, encoding='utf-8'))['combos']
    except Exception as e:
        print('⚠ 档位表读取失败 %s，退化为按个数分批' % str(e)[:60], flush=True)
        return {}


def estimate(combo, profile):
    """返回 (档位, 预估页数, 预估分钟)。无档位表时给保守默认值。"""
    x = profile.get(combo)
    if x:
        return x['tier'], x['est_pages'], x['est_minutes']
    return 'unknown', 0, 0.0


def plan_batches(combos, profile, budget, tier_filter):
    """按档位 + 工作量预算生成分批清单，返回 [(档位, [组合], 预估总分钟)]。"""
    if tier_filter != 'all':
        combos = [c for c in combos
                  if estimate(c, profile)[0] == tier_filter]
    # 轻档优先；同档内按预估分钟升序，短活先落地
    combos = sorted(combos, key=lambda c: (
        TIER_ORDER.get(estimate(c, profile)[0], 3),
        estimate(c, profile)[2], c))

    batches, cur, cur_min, cur_tier = [], [], 0.0, None
    for c in combos:
        tier, _pg, mins = estimate(c, profile)
        # 不同档位绝不混装：大省不能和小省挤在同一批抢并发槽
        if cur and tier != cur_tier:
            batches.append((cur_tier, cur, cur_min))
            cur, cur_min = [], 0.0
        # 重档一律单独成批，不合并（每个都可能跑满 180 分钟）
        if tier == 'heavy' and cur:
            batches.append((cur_tier, cur, cur_min))
            cur, cur_min = [], 0.0
        # 预算截断：预估总分钟超预算就另起一批
        if cur and cur_min + mins > budget:
            batches.append((cur_tier, cur, cur_min))
            cur, cur_min = [], 0.0
        cur.append(c)
        cur_min += mins
        cur_tier = tier
        if tier == 'heavy':
            batches.append((cur_tier, cur, cur_min))
            cur, cur_min, cur_tier = [], 0.0, None
    if cur:
        batches.append((cur_tier, cur, cur_min))
    return batches


def dispatch(combos, tag):
    body = json.dumps({'ref': 'main', 'inputs': {
        'combos': json.dumps(combos, ensure_ascii=False), 'tag': tag,
    }}, ensure_ascii=False).encode('utf-8')
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
        print('查询失败', type(e).__name__)
        return
    print('近期 gk-special run %d 个：' % len(d.get('workflow_runs', [])))
    for r in d.get('workflow_runs', [])[:10]:
        print('  #%-4s %-12s %-12s %s' %
              (r['run_number'], r['status'], r.get('conclusion') or '-', r['created_at']))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--combos', nargs='+')
    ap.add_argument('--limit', type=int, default=40,
                    help='最多派发多少个组合（在分批之后生效）')
    ap.add_argument('--status', action='store_true')
    ap.add_argument('--force', action='store_true')
    ap.add_argument('--plan', action='store_true',
                    help='只打印分批计划与预估工作量，不实际派发')
    ap.add_argument('--tier', choices=('light', 'medium', 'heavy', 'all'), default='all',
                    help='只派指定档位；默认全部（仍按档位分批）')
    ap.add_argument('--budget', type=int, default=DEFAULT_BUDGET,
                    help='每批预估总分钟数预算，默认 %d' % DEFAULT_BUDGET)
    ap.add_argument('--include-dispatched', action='store_true',
                    help='连台账里已派发过的组合一起纳入（配合 --plan 做全量盘点）')
    a = ap.parse_args()
    if a.status:
        status()
        return

    log = load_log()
    profile = load_profile()
    if not a.combos:
        # 未显式给组合时，从档位表取全部组合
        allc = sorted(profile.keys()) if profile else []
    else:
        allc = list(a.combos)
    if not a.force and not a.include_dispatched:
        allc = [c for c in allc if c not in log['dispatched']]
    if not allc:
        print('无可派组合（台账已覆盖）', flush=True)
        return

    batches = plan_batches(allc, profile, a.budget, a.tier)
    # 按批展开，再受 --limit 约束
    selected = []
    for tier, cs, mins in batches:
        if len(selected) + len(cs) > a.limit:
            cs = cs[:max(0, a.limit - len(selected))]
        if cs:
            selected.append((tier, cs, mins))
        if len([c for _, x, _ in selected for c in x]) >= a.limit:
            break

    total_min = sum(m for _, _, m in selected)
    print('=== 分批计划（预估工作量）===')
    print('档位: %s | 预算: %d 分钟/批 | 共 %d 批 / %d 组合 / 预估合计 %.0f 分钟' %
          (a.tier, a.budget, len(selected),
           sum(len(x) for _, x, _ in selected), total_min), flush=True)
    for i, (tier, cs, mins) in enumerate(selected, 1):
        detail = ', '.join('%s(%s页/%.0fmin)' % (
            c, estimate(c, profile)[1], estimate(c, profile)[2]) for c in cs[:6])
        if len(cs) > 6:
            detail += ' ...共%d个' % len(cs)
        print('  批%-2d [%-6s] %2d 组合 预估 %6.0f min | %s' %
              (i, tier, len(cs), mins, detail), flush=True)
    # 超时风险提示：单组合预估超过 workflow 的 timeout-minutes
    risky = [c for _, cs, _ in selected for c in cs
             if estimate(c, profile)[2] > 180]
    if risky:
        print('  ⚠ 以下组合预估超过 180 分钟超时线，需先做探测计划复用: %s' %
              ', '.join(risky), flush=True)

    if a.plan:
        print('\n（--plan 预览模式，未派发）', flush=True)
        return

    for tier, cs, mins in selected:
        tag = 'gs-%s-%s' % (tier, time.strftime('%m%d-%H%M%S'))
        print('派发 [%s] %d 个组合 预估 %.0f min: %s ...' %
              (tier, len(cs), mins, ' '.join(cs[:5])), flush=True)
        if not dispatch(cs, tag):
            raise SystemExit('派发失败，组合保留在待派清单')
        now = time.time()
        for c in cs:
            t, pg, mi = estimate(c, profile)
            log['dispatched'][c] = {
                'at': now, 'tag': tag,
                'tries': log['dispatched'].get(c, {}).get('tries', 0) + 1,
                'tier': t, 'est_pages': pg, 'est_minutes': mi,
            }
        log['runs'].append({'tag': tag, 'n': len(cs), 'combos': cs,
                            'tier': tier, 'est_minutes': mins,
                            'at': time.strftime('%F %T')})
        save_log(log)
        time.sleep(2)
    print('✅ 已派发 %d 批，台账累计 %d 个组合' %
          (len(selected), len(log['dispatched'])), flush=True)


if __name__ == '__main__':
    main()
