# -*- coding: utf-8 -*-
"""汇总 10 个分片 artifact -> 主仓 special_<p>_<y>.json -> 推 special-<combo> 分支。

验收口径与本地 run_rest.summarize 一致（完备性硬校验）：
  * 院校全集 2970 校逐校文件齐全，且每校 gate_pass=True；
  * ΣnumFound == Σitems，跨校全局唯一 id 零重复；
  * count == num_found，四闸门全过才写 gate_pass=True。
幂等：主仓已有 gate_pass=True 的结果分支则跳过推送，绝不降级。
"""
import glob
import json
import os
import subprocess
import sys
import tempfile
import time
import urllib.request

COMBO = os.environ['COMBO']
PROV, YEAR = COMBO.split('-', 1)
SIDS = json.load(open('school_ids.json', encoding='utf-8'))
OUT = 'out'
TOK = os.environ.get('GH_TOKEN', '')
REPO = os.environ.get('GITHUB_REPOSITORY', 'EdisonFan/gk-collect')
BR = 'special-' + COMBO
FNAME = 'special_%s_%s.json' % (PROV, YEAR)
H = {'Authorization': 'Bearer ' + TOK,
     'Accept': 'application/vnd.github+json', 'User-Agent': 'cb'}


def log(*a):
    print('[%s]' % time.strftime('%H:%M:%S'), *a, flush=True)


def already_ok():
    """主仓已有达标结果？查分支 + 该分支最后一条 commit message。"""
    try:
        u = 'https://api.github.com/repos/%s/branches/%s' % (REPO, BR)
        with urllib.request.urlopen(urllib.request.Request(u, headers=H), timeout=20) as r:
            sha = json.load(r)['commit']['sha']
        u = 'https://api.github.com/repos/%s/commits/%s' % (REPO, sha)
        with urllib.request.urlopen(urllib.request.Request(u, headers=H), timeout=20) as r:
            msg = json.load(r)['commit']['message']
        return 'gate_pass=True' in msg
    except Exception:
        return False


def push_result(result):
    """孤立分支推结果文件（大文件走 HTTP/1.1 + 放大 postBuffer，见 push_result.sh）。"""
    tmp = tempfile.mkdtemp()
    fp = os.path.join(tmp, 'out', FNAME)
    os.makedirs(os.path.dirname(fp), exist_ok=True)
    with open(fp, 'w', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False)
    log('结果文件 %.1f MB' % (os.path.getsize(fp) / 1e6))

    def g(*a, **kw):
        return subprocess.run(['git', '-C', tmp] + list(a),
                              capture_output=True, text=True, **kw)

    g('init', '-q')
    for k, v in (('user.email', 'gk-cloud@users.noreply.github.com'),
                 ('user.name', 'gk-cloud'),
                 ('http.version', 'HTTP/1.1'),
                 ('http.postBuffer', '524288000'),
                 ('http.lowSpeedLimit', '0'),
                 ('http.lowSpeedTime', '999999')):
        g('config', k, v)
    g('remote', 'add', 'origin',
      'https://x-access-token:%s@github.com/%s.git' % (TOK, REPO))
    g('checkout', '-q', '--orphan', BR)
    g('add', '-f', 'out/' + FNAME)
    r = g('-c', 'user.name=gk-cloud', '-c',
          'user.email=gk-cloud@users.noreply.github.com',
          'commit', '-q', '-m',
          'dataset %s result(gate_pass=True) via school-cloud' % COMBO)
    if r.returncode:
        log('commit 失败: ' + (r.stderr or '')[:300])
        return False
    for i in range(1, 4):
        r = g('push', '-f', 'origin', BR)
        if r.returncode == 0:
            log('已推送 %s（第 %d 次尝试）' % (BR, i))
            return True
        log('push 第 %d 次失败: %s' % (i, (r.stderr or '')[:200]))
        time.sleep(10)
    return False


def dispatch_next():
    """本组合已达标入库 -> 派发队列中的下一个组合（云端串行自驱）。

    GITHUB_TOKEN 触发的 workflow_dispatch 是 GitHub 防递归规则的例外，
    会正常创建新 run；因此整条链不需要任何 cron / 外部定时器。
    组合之间串行，避免多组合并行抢同一份 Supabase 代理池。
    """
    chain = [x.strip() for x in os.environ.get('CHAIN', '').split(',') if x.strip()]
    if not chain:
        log('链条已结束（无后续组合）')
        return
    nxt, rest = chain[0], ','.join(chain[1:])
    body = json.dumps({'ref': 'main', 'inputs': {
        'combo': nxt, 'chain': rest, 'tag': 'school-cloud-chain'}}).encode()
    u = ('https://api.github.com/repos/%s/actions/workflows/'
         'collect-school-cloud.yml/dispatches') % REPO
    req = urllib.request.Request(u, data=body, headers={
        'Authorization': 'Bearer ' + TOK,
        'Accept': 'application/vnd.github+json',
        'Content-Type': 'application/json',
        'User-Agent': 'cb'}, method='POST')
    try:
        with urllib.request.urlopen(req, timeout=25) as r:
            log('已派发下一组合 %s (HTTP %s)，剩余队列 %s' %
                (nxt, r.status, rest or '无'))
    except Exception as e:
        log('::warning::派发下一组合 %s 失败: %s' % (nxt, str(e)[:150]))


def main():
    if already_ok():
        log('%s 主仓已有 gate_pass=True 结果，跳过推送' % BR)
        dispatch_next()
        return 0
    files = glob.glob(os.path.join(OUT, 'school_%s_%s_*.json' % (PROV, YEAR)))
    by_sid = {}
    for p in files:
        try:
            o = json.load(open(p, encoding='utf-8'))
            by_sid[int(o['school_id'])] = o
        except Exception:
            pass
    missing = [s for s in SIDS if s not in by_sid]
    bad = [s for s in SIDS if s in by_sid and not by_sid[s].get('gate_pass')]
    if missing or bad:
        log('::error::%s 分片不齐：缺失 %d 校 / 未达标 %d 校' %
            (COMBO, len(missing), len(bad)))
        log('缺失示例 %s | 未达标示例 %s' % (missing[:12], bad[:12]))
        return 1

    items, sum_nf, pages = [], 0, 0
    for s in SIDS:
        o = by_sid[s]
        sum_nf += int(o['num_found'])
        pages += int(o.get('pages', 0))
        items += o.get('items', [])
    ids = [x.get('id') for x in items if x.get('id') is not None]
    uniq = len(set(ids))
    if not (len(items) == sum_nf == uniq):
        log('::error::Σ 不守恒：items=%d ΣnumFound=%d unique=%d cross_dup=%d' %
            (len(items), sum_nf, uniq, len(items) - uniq))
        return 1

    result = {
        'version': 'special-v1', 'prov': PROV, 'year': int(YEAR),
        'api': 'apidata/api/gk/score/special',
        'collector_rev': 'school-id-cloud-v1-shard10',
        'num_found': sum_nf, 'count': len(items),
        'base_num_found': 10000, 'base_capped': True,
        'true_num_found': sum_nf, 'partition_count': sum_nf,
        'dup_in_api': 0, 'coverage_pct': 100.0, 'gate_pass': True,
        'gate': {'G1_base': True, 'G2_partition': True,
                 'G3_pages_zero_fail': True, 'G4_count_match': True},
        'partition': {'dimension': 'school_id', 'universe': len(SIDS),
                      'nonempty': sum(1 for s in SIDS if by_sid[s]['num_found'] > 0),
                      'leaves': sum(1 for s in SIDS if by_sid[s]['num_found'] > 0)},
        'pages_total': pages, 'pages_ok': pages, 'pages_failed': 0,
        'prev_count': 0, 'workers': 3, 'timeout': 14, 'item_cap': 4000,
        'collected_at': time.strftime('%Y-%m-%d %H:%M:%S'), 'build_id': '',
        'items': items,
    }
    log('%s 验收通过：%d 校 / %d 条 / %d 页 / 唯一 id %d' %
        (COMBO, len(SIDS), len(items), pages, uniq))
    okp = push_result(result)
    if okp:
        dispatch_next()
    return 0 if okp else 1


if __name__ == '__main__':
    sys.exit(main())
