#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""L1 离线回归：把本次踩过的每个坑固化成用例，全程不发网络请求。

覆盖：422 不重试 / run 顶层状态滞后 / run_id 丢失重复派发 /
      gate_ok 对异常分支 / 第三维 Σ 守恒 / 危险静态写法。
用法: python3 tests/regression.py  （退出码 0=全过）
"""
import io
import os
import re
import sys
import tempfile
import urllib.error
from unittest import mock

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
os.chdir(ROOT)

FAILS = []


def chk(name, cond, extra=''):
    print(('  PASS  ' if cond else '  FAIL  ') + name +
          ((' | ' + str(extra)) if extra else ''), flush=True)
    if not cond:
        FAILS.append(name)


class StopLoop(Exception):
    """用于从 while True 主循环里中断测试。"""


def http_error(code):
    return urllib.error.HTTPError('https://api.github.com/x', code, 'err',
                                  {}, io.BytesIO(b'{}'))


# ---------- 1. api() 对 404/422 不重试（20:19 卡 14 分钟的根因） ----------
def test_api():
    import cloud_feed as cf
    for code in (404, 422):
        with mock.patch.object(cf.urllib.request, 'urlopen',
                               side_effect=http_error(code)) as u, \
             mock.patch.object(cf.time, 'sleep') as sl:
            r = cf.api('/x', 't')
            chk('api() HTTP %d 返回 None' % code, r is None, 'got=%r' % r)
            chk('api() HTTP %d 只请求 1 次' % code, u.call_count == 1,
                'calls=%d' % u.call_count)
            chk('api() HTTP %d 不 sleep' % code, sl.call_count == 0,
                'sleep=%d' % sl.call_count)
    with mock.patch.object(cf.urllib.request, 'urlopen',
                           side_effect=http_error(500)) as u, \
         mock.patch.object(cf.time, 'sleep') as sl:
        raised = False
        try:
            cf.api('/x', 't')
        except urllib.error.HTTPError:
            raised = True
        chk('api() HTTP 500 重试 4 次后抛出', raised and u.call_count == 4,
            'calls=%d' % u.call_count)
        chk('api() HTTP 500 退避 3 次', sl.call_count == 3,
            'sleep=%d' % sl.call_count)


# ---------- 2. run_done 只看 jobs（run.status 会长期卡 queued） ----------
def test_run_done():
    import cloud_feed as cf
    cases = [
        ({'jobs': [{'status': 'completed'}, {'status': 'in_progress'}]}, False,
         '有 job 在跑 -> False'),
        ({'jobs': [{'status': 'completed'}, {'status': 'completed'}]}, True,
         '全部 completed -> True'),
        ({'jobs': []}, False, 'jobs 为空 -> False'),
        (None, False, '响应为空 -> False'),
    ]
    for payload, want, name in cases:
        with mock.patch.object(cf, 'api', return_value=payload):
            chk('run_done ' + name, cf.run_done(1, 't') is want)
    # 顶层 status 完全不参与判断：即使它是 queued 也应看 jobs
    with mock.patch.object(cf, 'api',
                           return_value={'jobs': [{'status': 'completed'}]}):
        chk('run_done 不依赖 run.status', cf.run_done(1, 't') is True)


# ---------- 3. gate_ok 对不存在分支/异常一律 False，不崩 ----------
def test_gate_ok():
    import cloud_feed as cf
    with mock.patch.object(cf, 'api', return_value=None):
        chk('gate_ok 分支不存在 -> False', cf.gate_ok('99-2099', 't') is False)
    with mock.patch.object(cf, 'api', side_effect=http_error(422)):
        chk('gate_ok 422 不崩 -> False', cf.gate_ok('99-2099', 't') is False)
    good = {'commit': {'message': 'special 46-2026 gate_pass=True（云端）\n\n正文'}}
    with mock.patch.object(cf, 'api', return_value=good):
        chk('gate_ok 达标 -> True', cf.gate_ok('46-2026', 't') is True)
    bad = {'commit': {'message': 'special 13-2023 gate_pass=False\n\n正文'}}
    with mock.patch.object(cf, 'api', return_value=bad):
        chk('gate_ok 未达标 -> False', cf.gate_ok('13-2023', 't') is False)
    # 只看首行，正文里出现 gate_pass=True 不算
    with mock.patch.object(cf, 'api',
                           return_value={'commit': {'message': 'x gate_pass=False\n正文 gate_pass=True'}}):
        chk('gate_ok 只判首行', cf.gate_ok('x', 't') is False)


# ---------- 4. find_run 按 tag 定位，找不到返回 None ----------
def test_find_run():
    import cloud_feed as cf
    runs = {'workflow_runs': [
        {'id': 111, 'name': '采集专业录取线 other'},
        {'id': 222, 'name': '采集专业录取线 cloud-feed-1111-2035'},
    ]}
    with mock.patch.object(cf, 'api', return_value=runs):
        chk('find_run 命中 tag', cf.find_run('cloud-feed-1111-2035', 't') == 222)
    with mock.patch.object(cf, 'api',
                           return_value={'workflow_runs': []}), \
         mock.patch.object(cf.time, 'sleep'):
        chk('find_run 找不到 -> None', cf.find_run('nope', 't') is None)


# ---------- 5. 主循环：waiting 状态绝不重复派发 ----------
def test_main_no_duplicate():
    import cloud_feed as cf
    tmp = tempfile.mkdtemp(prefix='cf_reg_')
    state = {'run_id': None, 'waiting': True, 'tag': 'tag-x', 'batch': ['a']}
    with mock.patch.object(cf, 'STATE_F', tmp + '/state.json'), \
         mock.patch.object(cf, 'QUEUE_F', tmp + '/queue.json'), \
         mock.patch.object(cf, 'DISP_F', tmp + '/disp.json'), \
         mock.patch.object(cf, 'DONE_F', tmp + '/done'), \
         mock.patch.object(cf, 'LOG_F', tmp + '/feed.log'):
        cf.save(cf.STATE_F, state)
        cf.save(cf.QUEUE_F, ['b', 'c'])
        with mock.patch.object(cf, 'tok', return_value='t'), \
             mock.patch.object(cf, 'find_run', return_value=None) as fr, \
             mock.patch.object(cf, 'dispatch') as dp, \
             mock.patch.object(cf.time, 'sleep', side_effect=StopLoop):
            try:
                cf.main()
            except StopLoop:
                pass
        chk('run_id 丢失时 dispatch 调用 0 次', dp.call_count == 0,
            'calls=%d' % dp.call_count)
        chk('run_id 丢失时会重试找回', fr.call_count >= 1,
            'find_run=%d' % fr.call_count)
        chk('run_id 丢失不动待派队列', cf.load(cf.QUEUE_F, []) == ['b', 'c'])


# ---------- 6. 主循环：正常派一批，台账/state 全落盘 ----------
def test_main_dispatch_once():
    import cloud_feed as cf
    tmp = tempfile.mkdtemp(prefix='cf_reg_')
    with mock.patch.object(cf, 'STATE_F', tmp + '/state.json'), \
         mock.patch.object(cf, 'QUEUE_F', tmp + '/queue.json'), \
         mock.patch.object(cf, 'DISP_F', tmp + '/disp.json'), \
         mock.patch.object(cf, 'DONE_F', tmp + '/done'), \
         mock.patch.object(cf, 'LOG_F', tmp + '/feed.log'):
        cf.save(cf.STATE_F, {})
        cf.save(cf.QUEUE_F, ['a', 'b'])
        cf.save(cf.DISP_F, [])
        with mock.patch.object(cf, 'tok', return_value='t'), \
             mock.patch.object(cf, 'gate_ok', return_value=False), \
             mock.patch.object(cf, 'dispatch', return_value='tag-y') as dp, \
             mock.patch.object(cf, 'find_run', return_value=999), \
             mock.patch.object(cf.time, 'sleep', side_effect=StopLoop):
            try:
                cf.main()
            except StopLoop:
                pass
        chk('正常路径只派 1 批', dp.call_count == 1, 'calls=%d' % dp.call_count)
        chk('派出的 batch 正确', dp.call_args[0][0] == ['a', 'b'])
        chk('队列已清空', cf.load(cf.QUEUE_F, None) == [])
        chk('已派台账写入', sorted(cf.load(cf.DISP_F, [])) == ['a', 'b'])
        st = cf.load(cf.STATE_F, {})
        chk('state 记录 run_id=999', st.get('run_id') == 999, st)
        chk('state 记录 batch', st.get('batch') == ['a', 'b'])
        chk('state waiting=False', st.get('waiting') is False)


# ---------- 7. 上一批核对：未达标的回归本地队列 ----------
def test_main_reconcile():
    import cloud_feed as cf
    tmp = tempfile.mkdtemp(prefix='cf_reg_')
    with mock.patch.object(cf, 'STATE_F', tmp + '/state.json'), \
         mock.patch.object(cf, 'QUEUE_F', tmp + '/queue.json'), \
         mock.patch.object(cf, 'DISP_F', tmp + '/disp.json'), \
         mock.patch.object(cf, 'DONE_F', tmp + '/done'), \
         mock.patch.object(cf, 'LOG_F', tmp + '/feed.log'):
        cf.save(cf.STATE_F, {'run_id': 7, 'batch': ['a', 'b', 'c'], 'tag': 'z'})
        cf.save(cf.QUEUE_F, [])
        cf.save(cf.DISP_F, ['a', 'b', 'c'])
        with mock.patch.object(cf, 'tok', return_value='t'), \
             mock.patch.object(cf, 'run_done', return_value=True), \
             mock.patch.object(cf, 'gate_ok',
                               side_effect=lambda c, t: c in ('a', 'c')), \
             mock.patch.object(cf.time, 'sleep', side_effect=StopLoop):
            try:
                cf.main()
            except StopLoop:
                pass
        chk('未达标组合从已派台账移除',
            cf.load(cf.DISP_F, []) == ['a', 'c'],
            cf.load(cf.DISP_F, []))


# ---------- 8. local_any 第三维切分：Σ 守恒 + 漏值判不完备 ----------
def test_third_dimension():
    sys.argv = ['local_any.py', '37', '2021']
    import local_any as la
    UV = ['大类%d' % i for i in range(23)]
    BASE = 4060
    per = BASE // len(UV)
    rem = BASE - per * len(UV)

    def probe_ok(extra):
        t = extra.get('local_type_name')
        b = extra.get('local_batch_name')
        u = extra.get('level2_name')
        ht, hb, hu = ('local_type_name' in extra, 'local_batch_name' in extra,
                      'level2_name' in extra)
        if ht and hb and hu:
            return ((per + (1 if UV.index(u) < rem else 0))
                    if (t == '综合' and b == '普通类二段') else 0, 20)
        if ht and hb:
            return ({('综合', '普通类二段'): 4060, ('物理类', '本科批'): 100,
                     ('历史类', '本科批'): 200}.get((t, b), 0), 20)
        if ht:
            return ({'综合': 4060, '物理类': 100, '历史类': 200}.get(t, 0), 20)
        if hb:
            return ({'普通类二段': 4060, '本科批': 300}.get(b, 0), 20)
        return (4360, 20)

    tv3 = ('level2_name', set(UV))
    la.probe = probe_ok
    A = la.build({}, 'p37', 'local_type_name', 'local_batch_name',
                 {'综合', '物理类', '历史类'}, {'普通类二段', '本科批'}, 'A', tv3)
    B = la.build({}, 'p37', 'local_batch_name', 'local_type_name',
                 {'普通类二段', '本科批'}, {'综合', '物理类', '历史类'}, 'B', tv3)
    chk('第三维 A/B 均收敛', A is not None and B is not None)
    if A and B:
        sa, sb = sum(x[2] for x in A), sum(x[2] for x in B)
        chk('双划分 Σ 相等且守恒 4360', sa == sb == 4360, 'A=%d B=%d' % (sa, sb))

    def probe_leak(extra):
        if 'level2_name' in extra:
            return (100, 20)
        return probe_ok(extra)
    la.probe = probe_leak
    C = la.build({}, 'p37', 'local_type_name', 'local_batch_name',
                 {'综合', '物理类', '历史类'}, {'普通类二段', '本科批'}, 'A', tv3)
    chk('第三维漏值(Σ不守恒)判不完备', C is None)

    la.probe = lambda e: (100, 20) if 'local_batch_name' in e else (300, 20)
    D = la.build({}, 'p99', 'local_type_name', 'local_batch_name',
                 {'物理类'}, {'本科批'}, 'A', tv3)
    chk('普通场景(无需第三维)行为不变', D is not None and len(D) == 1)


# ---------- 9. 静态契约：禁止把人坑过的写法再写回来 ----------
def test_static_contracts():
    def read(p):
        return open(p, encoding='utf-8').read()

    def code_lines(p):
        # 静态契约只看真实代码行；注释里提到危险写法是「提醒」，不算违规。
        return '\n'.join(l for l in read(p).splitlines()
                         if l.strip() and not l.strip().startswith('#'))
    ra = read('run_all.py')
    chk('run_all.py 声明 global done, failed',
        'global done, failed' in ra)
    chk('run_all.py 组合开始前重读云端台账',
        "c in json.load(open(CLOUD_F))" in ra)
    chk('run_all.py 失败项优先重采',
        '0 if c in failed_set else 1' in ra)
    cf = read('cloud_feed.py')
    chk('cloud_feed.py run_done 走 jobs', "/jobs?per_page=100" in cf and
        "r['status']" not in cf)
    chk('cloud_feed.py 防重复派发分支', "st.get('waiting')" in cf)
    chk('cloud_feed.py 404/422 都判不存在', 'e.code in (404, 422)' in cf)
    po = read('push_one.py')
    chk('push_one.py 仅 gate_pass=True 可推送',
        "assert d.get('gate_pass') is True" in po)
    chk('push_one.py 读回重试 6 次', 'for _ in range(6):' in po)
    chk('push_one.py ref 不一致即失败', 'assert now == cm' in po)
    wf = read('.github/workflows/collect-special.yml')
    chk('工作流超时后仍推送(if: always())', 'always()' in wf)
    # pkill -f 'python3 -u ...' 会匹配到执行它的 shell 自身，曾把自己杀掉
    for f in ('supervise.sh', 'safe_restart.sh', 'verify.sh'):
        if os.path.exists(f):
            chk('%s 不含危险 pkill -f' % f,
                "pkill -f 'python3 -u" not in code_lines(f))
    # 反向自证：这条检查能抓到真违规，也不会被注释误伤（造文件实测，非恒真）
    import tempfile

    def probe(content):
        p = tempfile.mkdtemp(prefix='reg_') + '/probe.sh'
        open(p, 'w', encoding='utf-8').write(content)
        return "pkill -f 'python3 -u" in code_lines(p)

    chk('静态检查能抓到真实违规行',
        probe("pkill -f 'python3 -u run_all.py'\n"))
    chk('静态检查不被注释误伤',
        not probe("# 别用 pkill -f 'python3 -u run_all.py'\n"))

    # CNB 同步契约：曾凭旧快照反复误报「漏 special-*」，实测三条 refspec 齐全。
    # 固化成检查，防止以后改工作流时真的漏掉而无人发现。
    sy = read('.github/workflows/sync-to-cnb.yml')
    chk('CNB 同步含 main refspec',
        "'+refs/remotes/origin/main:refs/heads/main'" in sy)
    chk('CNB 同步含 cutoff-* refspec',
        "'+refs/remotes/origin/cutoff-*:refs/heads/cutoff-*'" in sy)
    chk('CNB 同步含 special-* refspec',
        "'+refs/remotes/origin/special-*:refs/heads/special-*'" in sy)
    chk('CNB 同步显式 fetch 全部远程分支',
        "refs/heads/*:refs/remotes/origin/*" in sy)
    chk('CNB 同步为 force+prune 镜像',
        '--force' in sy and '--prune' in sy)
    m = re.search(r'workflows:\s*\[([^\]]+)\]', sy)
    listed = set(re.findall(r"'([^']+)'", m.group(1))) if m else set()
    actual = set()
    for f in ('collect.yml', 'collect-special.yml'):
        mm = re.search(r'^name:\s*(\S+)',
                       read('.github/workflows/' + f), re.M)
        if mm:
            actual.add(mm.group(1))
    chk('workflow_run 名单与采集工作流 name 完全对应',
        listed == actual and len(actual) == 2,
        '配置=%s 实际=%s' % (sorted(listed), sorted(actual)))
    chk('main 变更也触发同步（on.push）',
        re.search(r'push:\s*\n\s*branches:', sy) is not None)


def main():
    print('=== L1 离线回归 ===', flush=True)
    tests = [test_api, test_run_done, test_gate_ok, test_find_run,
             test_main_no_duplicate, test_main_dispatch_once,
             test_main_reconcile, test_third_dimension, test_static_contracts]
    for t in tests:
        print('-- %s' % t.__name__, flush=True)
        try:
            t()
        except Exception as e:
            chk('%s 未抛异常' % t.__name__, False, '%s: %s' % (type(e).__name__, e))
    print('\nL1 结论：%s' % ('全部通过' if not FAILS else
          '%d 项失败 -> %s' % (len(FAILS), FAILS)), flush=True)
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(main())
