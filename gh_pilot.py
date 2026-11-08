# -*- coding: utf-8 -*-
"""【已废弃，勿再使用】本地常驻巡航。

用户已明确否定本方案：常驻进程容易被沙箱杀死，且达标口径是旧的
「覆盖率 >=95%」，不满足 100% 零丢失要求。

替代方案：
  - 平台定时任务每 5 分钟拉起一次 AI 会话执行 pilot_once.py（单次即退出）
  - GitHub collect.yml 的 relay job 做连续自接力（workflow_dispatch 例外）
  - 达标口径统一为 gate_pass=true 且 count == num_found

保留本文件仅作历史参考，不要重新运行 python3 gh_pilot.py。

────────────────────────────────────────────────────────────
以下为原说明：
GitHub 采集自动巡航：持续回收结果 + 补派缺口，保证 20 并发队列不空。

与 CNB 版差异：
  - GitHub 公开仓库 Actions 无限免费，但一次 workflow_dispatch 可带最多 256 个 matrix job
  - 因此采用「大批量少触发」：一轮派 40~60 组合，GitHub 按 20 并发自动排队
  - 有活跃 run 时不再触发，避免堆积与滥用检测
  - 台账 gh_queue.json 可重入，进程被杀后重跑即续

用法：python3 gh_pilot.py
"""
import os, sys, json, time, glob, subprocess, urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import feed_gh

CUTOFF_DIR = '/workspace/高考填报数据模型与流程/采集原始数据/gk_cutoff'
LOG = os.path.join(HERE, 'gh_queue.json')
INTERVAL = 300          # 每轮 5 分钟
BATCH = 40              # 每轮最多补派组合数
INFLIGHT_COOLDOWN = 7200   # 已派组合 2 小时内不重派
LOWCOV_COOLDOWN = 900      # 低覆盖组合 15 分钟冷却（公开仓库免费，别让云端空转）
MAX_RETRY = 4

PROVS = ['13', '41', '44', '37', '11', '12', '21', '22', '23', '31',
         '32', '33', '34', '35', '36', '42', '43', '45', '46', '50',
         '51', '52', '53', '61', '62', '65', '14', '15']
YEARS = ['2021', '2022', '2023', '2024', '2025', '2026']
ALL = ['%s-%s' % (p, y) for p in PROVS for y in YEARS]
RUNS_API = ('https://api.github.com/repos/EdisonFan/gk-collect/actions/'
            'workflows/%d/runs?per_page=20' % feed_gh.WORKFLOW_ID)


def coverages():
    out = {}
    for f in glob.glob(CUTOFF_DIR + '/*.json'):
        try:
            d = json.load(open(f, encoding='utf-8'))
            out['%s-%s' % (d['prov'], d['year'])] = d.get('coverage_pct', 0)
        except Exception:
            pass
    return out


def active_runs():
    """返回仍在 queued/in_progress 的 run 数量。"""
    req = urllib.request.Request(
        RUNS_API, headers={'Authorization': 'Bearer ' + feed_gh.token(),
                           'Accept': 'application/vnd.github+json'})
    try:
        d = json.loads(urllib.request.urlopen(req, timeout=30).read().decode())
        return sum(1 for r in d.get('workflow_runs', [])
                   if r['status'] in ('queued', 'in_progress', 'waiting', 'pending'))
    except Exception:
        return -1


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


def main():
    print('=== GitHub 自动巡航启动 | 全量 %d 组合 | 每 %ds 一轮 | 每批 %d ===' %
          (len(ALL), INTERVAL, BATCH), flush=True)
    rnd = 0
    while True:
        rnd += 1
        ts = time.strftime('%F %T')
        # 每 3 轮做一次增量 fetch 拉新分支（慢，单独包住，不打断补派）
        if rnd % 3 == 1:
            try:
                subprocess.run([sys.executable, 'fetch_gh_results.py'], cwd=HERE,
                               capture_output=True, text=True, timeout=900)
            except Exception as e:
                print('[%s 轮%d] 增量 fetch 未完成 %s（不影响本地读取）' %
                      (ts, rnd, type(e).__name__), flush=True)
        try:
            r = subprocess.run([sys.executable, 'fetch_gh_results.py', '--local-only'],
                               cwd=HERE, capture_output=True, text=True, timeout=120)
            tail = [l for l in r.stdout.splitlines()
                    if '覆盖写入' in l or '结果分支' in l or '本地快速读取' in l]
            print('[%s 轮%d] %s' % (ts, rnd, ' | '.join(tail[-2:])), flush=True)
        except Exception as e:
            print('[%s 轮%d] 回收异常 %s' % (ts, rnd, type(e).__name__), flush=True)

        cov = coverages()
        ok = sum(1 for c in ALL if cov.get(c, 0) >= 95)
        print('  覆盖率达标 %d/%d，活跃 run %d' % (ok, len(ALL), active_runs()), flush=True)

        nr = active_runs()
        if nr > 0:
            print('  有 %d 个 run 在跑，本轮不补派' % nr, flush=True)
            time.sleep(INTERVAL)
            continue
        if nr < 0:
            print('  状态查询失败，本轮不补派', flush=True)
            time.sleep(INTERVAL)
            continue

        log = load_log()
        disp = log['dispatched']
        now = time.time()
        cand = []
        for c in ALL:
            v = cov.get(c)
            rec = disp.get(c, {})
            tries = rec.get('tries', 0)
            last = rec.get('at', 0)
            if v is None:
                if last and now - last < INFLIGHT_COOLDOWN:
                    continue
                if tries >= MAX_RETRY:
                    continue
            elif v >= 95:
                continue
            else:
                if now - last < LOWCOV_COOLDOWN or tries >= MAX_RETRY:
                    continue
            cand.append(c)

        print('  待补 %d 个' % len(cand), flush=True)
        if not cand:
            print('  ✅ 全部达标，巡航进入空转', flush=True)
            time.sleep(INTERVAL)
            continue

        batch = cand[:BATCH]
        tag = 'pilot-%s' % time.strftime('%m%d-%H%M%S')
        if not feed_gh.dispatch(batch, tag):
            print('  ✗ 派发失败，下轮重试', flush=True)
            time.sleep(INTERVAL)
            continue
        for c in batch:
            disp[c] = {'tries': disp.get(c, {}).get('tries', 0) + 1,
                       'at': now, 'tag': tag}
        log['dispatched'] = disp
        log['runs'].append({'tag': tag, 'n': len(batch), 'combos': batch,
                            'at': ts, 'by': 'gh_pilot'})
        save_log(log)
        print('  ✓ 补派 %d 条 -> %s' % (len(batch), tag), flush=True)
        time.sleep(INTERVAL)


if __name__ == '__main__':
    main()
