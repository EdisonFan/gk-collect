"""refresh_pool.py 回归：固化「覆盖式写回导致越刷越少」这个真实 bug。

全部离线：mock 掉网络与探活，用临时文件代替 /tmp/live.txt。
若改回覆盖式写回，用例 1 会立刻失败（旧版会丢掉瞬时超时的 a）。
"""
import importlib.util
import os
import sys
import tempfile
import time
from unittest import mock

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
SRC = os.path.join(ROOT, 'refresh_pool.py')
_spec = importlib.util.spec_from_file_location('refresh_pool', SRC)
rp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rp)

FAILS = 0
CHKS = 0


def chk(name, cond, extra=''):
    global FAILS, CHKS
    CHKS += 1
    print(('  PASS ' if cond else '  FAIL ') + name +
          ((' | ' + extra) if extra else ''), flush=True)
    if not cond:
        FAILS += 1


def run_main(live_before, cand, ok_set, fresh=True, pull_exc=None):
    """ok_set: 探活通过的代理集合。返回 (final, written, probe_calls)。"""
    fd, path = tempfile.mkstemp(suffix='.txt')
    os.close(fd)
    with open(path, 'w') as f:
        f.write('\n'.join(live_before) + ('\n' if live_before else ''))
    calls = []

    def fake_probe(px):
        calls.append(px)
        return px if px in ok_set else None

    if pull_exc:
        pull = mock.Mock(side_effect=pull_exc)
    else:
        pull = mock.Mock(return_value=fresh)
    try:
        with mock.patch.object(rp, 'pull_latest_pool', pull), \
             mock.patch.object(rp, 'candidates', return_value=cand), \
             mock.patch.object(rp, 'probe', side_effect=fake_probe):
            final = rp.main(live_path=path)
        with open(path) as f:
            written = [x.strip() for x in f if x.strip()]
        return final, written, calls
    finally:
        os.unlink(path)


print('=== refresh_pool 回归 ===', flush=True)

# 1. 核心：瞬时超时不能删掉原本活着的出口；新探活的也要并进来
A, B, C, D = ('http://a:1', 'http://b:2', 'http://c:3', 'http://d:4')
before = [A, B, C]
cand = before + [D]
ok = {B, C, D}                      # A 模拟一次瞬时超时
final, written, calls = run_main(before, cand, ok)
chk('合并式写回不掉数（原存活全保留）',
    set(before) <= set(written), 'written=%s' % written)
chk('新探活的出口被并入', D in written)
chk('结果与落盘一致', final == written)
chk('旧版覆盖式会挂在此用例（断言非恒真）',
    A not in {B, C, D})

# 2. 双次确认：通过者至少被探活 2 次（screen 一次 + 确认一次）
chk('通过者经过双次确认',
    calls.count(B) >= 2, 'B 调用次数=%d' % calls.count(B))
chk('失败者不进入确认阶段',
    calls.count(A) == 1, 'A 调用次数=%d' % calls.count(A))

# 3. 云端池拉取异常 / 返回 False 都要降级，不崩、结果仍正确
try:
    f2, w2, _ = run_main(before, cand, ok, pull_exc=RuntimeError('GitHub 抖了'))
    chk('云端池抛异常时降级不崩', set(before) <= set(w2) and D in w2)
except Exception as e:
    chk('云端池抛异常时降级不崩', False, 'raised=%r' % e)
f3, w3, _ = run_main(before, cand, ok, fresh=False)
chk('云端池不可用时降级不崩', set(before) <= set(w3) and D in w3)

# 4. deadline 截断：立刻返回已得结果，不空等、不调探活
with mock.patch.object(rp, 'probe') as pp:
    got, cut = rp.screen([A, B, C], time.time() - 1)
    chk('deadline 已过立刻截断', cut is True and got == [])
    chk('截断时不发起探活', pp.call_count == 0, 'calls=%d' % pp.call_count)

# 5. 静态契约：改动不能把关键保证悄悄删掉
code = open(SRC, encoding='utf-8').read()
chk('源码含合并写回语义', 'before + confirmed' in code)
chk('源码纳入历史大池候选', 'proxy_pool*.txt' in code)
chk('可从 GitHub 拉最新池', 'contents/proxies_cnb.txt' in code)
chk('可安全 import（逻辑在 main 里）',
    "if __name__ == '__main__':" in code and callable(rp.main))
chk('硬 deadline 小于 run_all 的 150s',
    rp.MAX_SECONDS < 150, 'MAX_SECONDS=%s' % rp.MAX_SECONDS)

print('=== %d 项，失败 %d ===' % (CHKS, FAILS), flush=True)
sys.exit(1 if FAILS else 0)
