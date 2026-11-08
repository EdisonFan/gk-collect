# -*- coding: utf-8 -*-
"""平台定时任务执行体（单次运行，非常驻）：巡航一轮。

由 WorkBuddy 平台定时任务每 5 分钟拉起一次 AI 会话执行本脚本；
脚本跑完即退出，不存在 gh_pilot.py 那种「本地常驻进程被沙箱杀死」的问题。

一轮做四件事：
  ① 回收：fetch_gh_results.py 完整跑（本地落盘 + 云端差集增量）
  ② 验收：按 v5 gate_pass 判定全量组合，统计达标 / 未达标 / 结果版本
  ③ 兜底补派：无活跃 run 时，对未达标组合按指数退避冷却补派
  ④ 写日志：追加 巡航日志.md + stdout 原样回报给定时任务会话

与 GitHub relay 自接力的分工：
  relay 负责连续加速（每个 run 结束立即接力未达标组合，最多 5 轮）；
  本脚本负责链条断裂兜底（relay 达上限、dispatch 失败、run 被取消），
  只在「无活跃 run 且近期无新 run」时补派，避免与 relay 重复派发。

达标口径（不再是覆盖率 >=95%）：
  gate_pass == true 且 count == num_found 且 num_found > 0。
  旧 v4 结果没有 gate_pass 字段，一律按未达标处理，需全量重采。
"""
import os, sys, json, time, glob, subprocess, urllib.request
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import feed_gh

CUTOFF_DIR = '/workspace/高考填报数据模型与流程/采集原始数据/gk_cutoff'
QUEUE = os.path.join(HERE, 'gh_queue.json')
JOURNAL = os.path.join(HERE, '巡航日志.md')
BATCH = 60                      # 单次兜底补派上限
FETCH_TIMEOUT = 300             # 完整回收超时，失败不中断本轮
BASE_COOLDOWN = 300             # 5 分钟
MAX_COOLDOWN = 3600             # 指数退避封顶 1 小时
RECENT_WINDOW = 300             # 近 5 分钟内有新 run 视同链条未断

PROVS = ['13', '41', '44', '37', '11', '12', '21', '22', '23', '31',
         '32', '33', '34', '35', '36', '42', '43', '45', '46', '50',
         '51', '52', '53', '61', '62', '65', '14', '15']
YEARS = ['2021', '2022', '2023', '2024', '2025', '2026']
RUNS_API = ('https://api.github.com/repos/EdisonFan/gk-collect/actions/'
            'workflows/%d/runs?per_page=20' % feed_gh.WORKFLOW_ID)


def parse_ts(s):
    try:
        return datetime.strptime(s, '%Y-%m-%dT%H:%M:%SZ').replace(
            tzinfo=timezone.utc).timestamp()
    except Exception:
        return 0


def fetch_results():
    """完整回收；失败降级为纯本地读取，保证本轮日志仍产出。"""
    try:
        r = subprocess.run([sys.executable, 'fetch_gh_results.py'], cwd=HERE,
                           capture_output=True, text=True, timeout=FETCH_TIMEOUT)
        tail = [l.strip() for l in r.stdout.splitlines() if l.strip()][-4:]
        return 'OK（本地落盘 + 云端差集增量）', tail
    except Exception:
        try:
            r = subprocess.run([sys.executable, 'fetch_gh_results.py', '--local-only'],
                               cwd=HERE, capture_output=True, text=True, timeout=120)
            tail = [l.strip() for l in r.stdout.splitlines() if l.strip()][-3:]
            return 'PARTIAL（完整回收超时，已纯本地读取）', tail
        except Exception as e2:
            return 'FAIL（%s）' % type(e2).__name__, []


def load_status():
    out = {}
    for f in glob.glob(os.path.join(CUTOFF_DIR, 'cutoff_*.json')):
        try:
            d = json.load(open(f, encoding='utf-8'))
            key = '%s-%s' % (d.get('prov'), d.get('year'))
            ok = (d.get('gate_pass') is True
                  and d.get('count') == d.get('num_found')
                  and d.get('num_found', 0) > 0)
            out[key] = {'ok': ok, 'count': d.get('count', 0),
                        'num_found': d.get('num_found', 0),
                        'version': d.get('version', 'v4')}
        except Exception:
            pass
    return out


def all_combos(status):
    combos = ['%s-%s' % (p, y) for p in PROVS for y in YEARS]
    for k in status:                  # 落盘里出现的额外组合也纳入监控
        if k not in combos:
            combos.append(k)
    return sorted(combos)


def query_runs():
    req = urllib.request.Request(
        RUNS_API, headers={'Authorization': 'Bearer ' + feed_gh.token(),
                           'Accept': 'application/vnd.github+json'})
    try:
        d = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
        runs = d.get('workflow_runs', [])
        active = sum(1 for r in runs
                     if r['status'] in ('queued', 'in_progress', 'waiting', 'pending'))
        return active, runs
    except Exception:
        return -1, []


def load_queue():
    if os.path.exists(QUEUE):
        try:
            d = json.load(open(QUEUE, encoding='utf-8'))
            d.setdefault('dispatched', {})
            d.setdefault('runs', [])
            return d
        except Exception:
            pass
    return {'dispatched': {}, 'runs': []}


def save_queue(d):
    tmp = QUEUE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    os.replace(tmp, QUEUE)


def main():
    ts = time.strftime('%F %T')
    now = time.time()
    if not os.path.exists(JOURNAL):
        with open(JOURNAL, 'w', encoding='utf-8') as f:
            f.write('# 高考采集巡航日志（v5 零丢失）\n\n'
                    '> 由平台定时任务每 5 分钟拉起，执行 `pilot_once.py` 追加。\n'
                    '> 达标口径：`gate_pass=true` 且 `count==numFound`；旧 v4 结果一律未达标。\n\n')

    L = ['## %s' % ts]
    st, tail = fetch_results()
    L.append('- 回收：%s' % st)
    for l in tail:
        L.append('  - `%s`' % l)

    status = load_status()
    combos = all_combos(status)
    ok_list = [c for c in combos if status.get(c, {}).get('ok')]
    bad_list = [c for c in combos if not status.get(c, {}).get('ok')]
    v5 = sum(1 for c in combos if status.get(c, {}).get('version') == 'v5')
    L.append('- 零丢失验收：**%d / %d 达标**；未达标 %d' %
             (len(ok_list), len(combos), len(bad_list)))
    L.append('- 结果版本：v5 %d / 旧 v4 %d（旧结果需全量重采）' %
             (v5, len(combos) - v5))

    nr, runs = query_runs()
    recent = [r for r in runs if now - parse_ts(r.get('created_at', '')) < RECENT_WINDOW]
    L.append('- 活跃 run：%d；近 5 分钟新建 run：%d' % (nr, len(recent)))
    if runs[:5]:
        L.append('  - 近期：%s' % '，'.join(
            '#%s %s' % (r['run_number'], r['status']) for r in runs[:5]))

    q = load_queue()
    disp = q['dispatched']
    chain_busy = nr > 0 or bool(recent)

    if nr < 0:
        L.append('- 补派：跳过（run 状态查询失败）')
    elif chain_busy:
        L.append('- 补派：跳过（链条仍在推进，交给 relay 自接力）')
    else:
        cand = []
        for c in bad_list:
            rec = disp.get(c, {})
            tries = rec.get('tries', 0)
            last = rec.get('at', 0)
            cd = min(BASE_COOLDOWN * (2 ** min(tries, 4)), MAX_COOLDOWN)
            if now - last < cd:
                continue
            cand.append(c)
        L.append('- 符合补派条件（已过指数退避冷却）：%d 个' % len(cand))
        batch = cand[:BATCH]
        if not batch:
            L.append('- 补派：无（全部在冷却中或已达标）')
        else:
            tag = 'pilot-%s' % time.strftime('%m%d-%H%M%S')
            if '--dry-run' in sys.argv:
                L.append('- 补派：[DRY-RUN] 将派 %d 个 -> `%s`，实际未派发' %
                         (len(batch), tag))
                L.append('  - %s' % ' '.join(batch[:15]))
            elif feed_gh.dispatch(batch, tag):
                for c in batch:
                    disp[c] = {'at': now, 'tag': tag,
                               'tries': disp.get(c, {}).get('tries', 0) + 1}
                q['dispatched'] = disp
                q['runs'].append({'tag': tag, 'n': len(batch), 'combos': batch,
                                  'at': ts, 'by': 'pilot_once'})
                save_queue(q)
                L.append('- 补派：**已派 %d 个** -> `%s`' % (len(batch), tag))
                L.append('  - %s' % ' '.join(batch[:15]))
            else:
                L.append('- 补派：**派发失败**，组合保留在下轮重试清单')

    if bad_list:
        det = []
        for c in bad_list[:20]:
            s = status.get(c, {})
            det.append('%s(%s/%s,%s)' % (c, s.get('count', '-'),
                                         s.get('num_found', '-'), s.get('version', '-')))
        L.append('- 未达标明细（前 20）：%s' % ' '.join(det))
        if len(bad_list) > 20:
            L.append('- …另有 %d 个未达标' % (len(bad_list) - 20))

    text = '\n'.join(L) + '\n\n'
    with open(JOURNAL, 'a', encoding='utf-8') as f:
        f.write(text)
    print(text, end='')


if __name__ == '__main__':
    main()
