# -*- coding: utf-8 -*-
"""主会话侧的共享层同步器（只在主会话运行，平行世界不碰）。

职责：
  --refresh   从云端 168 个 cutoff-* 结果分支现算零丢失真值，刷新
              /workspace/gk-collect-share/state/verify_snapshot.json
  --pull      拉取平行世界写回的日志/台账
  --push      提交并推送 state/ 到 CNB
  默认（无参数）= refresh + push，并打印共享仓库里巡航日志的最近若干行

数据源统一（重要）：
  早期本脚本扫描主会话 /workspace 的落盘结果生成快照，但主会话不常驻、
  落盘回收常滞后，导致快照冻结在 6/168、巡航日志永远显示 6，而云端其实
  已 166/168。现改为委托共享层 cloud_verify 模块，从 GitHub 结果分支头部
  （Range 1500B）现算真值 —— 与定时任务完全同一口径，杜绝「主会话把真值
  打回旧值」。共享仓库仍只存轻量摘要，clone/push 秒级完成。
  - CNB 凭证从同源网关动态取，不写死在文件或仓库里。
"""
import os, sys, json, glob, time, subprocess, urllib.request

SHARE = '/workspace/gk-collect-share'
DEST = '/workspace/高考填报数据模型与流程/采集原始数据/gk_cutoff'
STATE = os.path.join(SHARE, 'state')
SNAPSHOT = os.path.join(STATE, 'verify_snapshot.json')
JOURNAL = os.path.join(STATE, '巡航日志.md')
REPO_SLUG = 'cooledi/gk-collect-share'

PROVS = ['13', '41', '44', '37', '11', '12', '21', '22', '23', '31',
         '32', '33', '34', '35', '36', '42', '43', '45', '46', '50',
         '51', '52', '53', '61', '62', '65', '14', '15']
YEARS = ['2021', '2022', '2023', '2024', '2025', '2026']
GATES = ('G1_base', 'G2_partition', 'G3_pages_zero_fail', 'G4_count_match')


def cnb_token():
    u = os.environ.get('WORKBUDDY_TOKEN_URL_CNB_APP', '')
    return json.loads(urllib.request.urlopen(u, timeout=15).read().decode())['data']['access_token']


def git(*args, timeout=120):
    return subprocess.run(['git'] + list(args), cwd=SHARE,
                          capture_output=True, text=True, timeout=timeout)


def set_remote():
    tok = cnb_token()
    url = 'https://cnb:%s@cnb.cool/%s.git' % (tok, REPO_SLUG)
    git('remote', 'set-url', 'origin', url)
    return tok


def refresh_snapshot():
    """委托共享层 cloud_verify，从云端结果分支现算零丢失真值。

    与主会话/定时任务使用完全同一口径，避免本地落盘滞后把达标数打回旧值。
    cloud_verify 已负责写 SNAPSHOT；这里只补写 criteria/source 便于阅读。
    """
    # cloud_verify 在共享仓库根目录，临时加入模块搜索路径
    if SHARE not in sys.path:
        sys.path.insert(0, SHARE)
    import cloud_verify
    combos, lines = cloud_verify.refresh()
    for l in lines:
        print('  ' + l.strip(), flush=True)
    snap = json.load(open(SNAPSHOT, encoding='utf-8'))
    snap['source'] = 'main-session 委托 cloud_verify（云端真值，与定时任务同口径）'
    snap['criteria'] = 'v5 + gate_pass=true + count==numFound>0（逐组合云端文件头判定）'
    tmp = SNAPSHOT + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(snap, f, ensure_ascii=False, indent=1)
    os.replace(tmp, SNAPSHOT)
    return snap


def journal_tail(n=24):
    if not os.path.exists(JOURNAL):
        return '（共享层尚无巡航日志）'
    lines = open(JOURNAL, encoding='utf-8').read().splitlines()
    return '\n'.join(lines[-n:])


def main():
    args = sys.argv[1:]
    do_all = not args
    set_remote()
    if do_all or '--pull' in args or '--refresh' in args:
        git('pull', '--rebase', '-q', 'origin', 'HEAD')
    if do_all or '--refresh' in args:
        s = refresh_snapshot()
        print('验收快照已刷新：%d/%d 达标，%s' %
              (s['ok'], s['total'], s['generated_at']), flush=True)
    if do_all or '--push' in args:
        git('add', 'state/')
        git('commit', '-q', '-m', 'main-session sync %s' % time.strftime('%F %T'))
        r = git('push', '-q', 'origin', 'HEAD', timeout=120)
        print('推送共享层：%s' % ('OK' if r.returncode == 0
                              else r.stderr.strip()[:120]), flush=True)
    print('--- 共享层巡航日志（尾部）---')
    print(journal_tail())


if __name__ == '__main__':
    main()
