#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""改动门禁：跑全套测试，全过才落 verify_stamp；--check 卡住未验证的上线。

规则：所有受管脚本（根 *.py/*.sh、tests/*.py、工作流 yml）的 mtime+size
会算成指纹。任何文件改动后指纹变化，--check 立即失败，
safe_restart.sh 和 supervise.sh 都不会拉起没测过的代码。

用法:
  python3 verify.py           跑全套测试，通过写 stamp
  python3 verify.py --check   只校验 stamp 是否覆盖当前代码
  python3 verify.py --list    列出受管文件
"""
import glob
import hashlib
import json
import os
import subprocess
import sys
import time
from datetime import datetime

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
STAMP_F = 'out/verify_stamp.json'


def managed():
    fs = []
    fs += glob.glob('*.py') + glob.glob('*.sh')
    fs += glob.glob('tests/*.py')
    fs += glob.glob('.github/workflows/*.yml')
    return sorted(set(f for f in fs if os.path.isfile(f)))


def fp():
    h = hashlib.sha256()
    for f in managed():
        st = os.stat(f)
        h.update(f.encode())
        h.update(str(st.st_mtime_ns).encode())
        h.update(str(st.st_size).encode())
    return h.hexdigest()


def check():
    if not os.path.exists(STAMP_F):
        print('门禁未通过：从无测试记录，请先跑 ./verify.sh')
        return False
    d = json.load(open(STAMP_F, encoding='utf-8'))
    cur = fp()
    if d.get('fp') != cur:
        print('门禁未通过：代码在上次测试之后被改动，必须重跑 ./verify.sh')
        old = set(d.get('files', {}))
        now = {f: os.stat(f).st_mtime_ns for f in managed()}
        changed = [f for f in sorted(now) if d.get('files', {}).get(f) != now[f]]
        for f in changed[:20]:
            print('  改动: ' + f)
        if not changed:
            print('  （新增/删除了受管文件：%s）' %
                  sorted(set(now) ^ old))
        return False
    print('门禁通过：上次全套测试 %s，覆盖 %d 个文件，%d 步全过'
          % (d.get('ts'), len(d.get('files', {})), len(d.get('steps', []))))
    return True


def run_step(name, cmd, timeout=300):
    t0 = time.time()
    try:
        p = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        out = (p.stdout or '') + (p.stderr or '')
        rc = p.returncode
    except subprocess.TimeoutExpired:
        out, rc = 'TIMEOUT after %ds' % timeout, 124
    sec = time.time() - t0
    tail = [l for l in out.strip().splitlines() if l.strip()][-12:]
    print('  [%s] %s (%.1fs)' % ('OK' if rc == 0 else 'FAIL', name, sec),
          flush=True)
    for l in tail[-6:]:
        print('      ' + l[:160], flush=True)
    if rc != 0:
        print('  ---- %s 完整输出尾部 ----' % name, flush=True)
        for l in tail:
            print('      ' + l[:200], flush=True)
    return {'name': name, 'rc': rc, 'sec': round(sec, 1)}


def main():
    if '--check' in sys.argv:
        return 0 if check() else 1
    if '--list' in sys.argv:
        for f in managed():
            print(f)
        return 0

    print('=== 全套验证 === %s' % datetime.now().strftime('%F %T'), flush=True)
    py = sys.executable
    pys = sorted(set(glob.glob('*.py') + glob.glob('tests/*.py')))
    shs = sorted(glob.glob('*.sh'))
    steps = []
    steps.append(run_step('Python 语法编译', [py, '-m', 'py_compile'] + pys, 120))
    if shs:
        steps.append(run_step('Shell 语法检查',
                              ['bash', '-n'] + shs, 60))
    steps.append(run_step('L1 离线回归（含历史 bug）',
                          [py, 'tests/regression.py'], 180))
    steps.append(run_step('L2 真实 API 契约（只读）',
                          [py, 'tests/api_contract.py'], 120))
    for f in ('test_special_1090.py', 'test_special_plan_reuse.py',
              'test_special_capped.py'):
        if os.path.exists(f):
            steps.append(run_step('专项 ' + f, [py, f], 300))
    steps.append(run_step('L3 影子库端到端（真推真删）',
                          [py, 'tests/shadow_repo.py', '--run'], 240))

    bad = [s for s in steps if s['rc'] != 0]
    total = round(sum(s['sec'] for s in steps), 1)
    print('\n=== 汇总：%d 步，通过 %d，失败 %d，耗时 %.1fs ==='
          % (len(steps), len(steps) - len(bad), len(bad), total), flush=True)
    if bad:
        for s in bad:
            print('  失败: %s (rc=%s)' % (s['name'], s['rc']), flush=True)
        print('未写 stamp，禁止上线。', flush=True)
        return 1

    d = {'fp': fp(), 'ts': datetime.now().strftime('%F %T'),
         'steps': steps, 'total_sec': total,
         'files': {f: os.stat(f).st_mtime_ns for f in managed()}}
    json.dump(d, open(STAMP_F, 'w', encoding='utf-8'),
              ensure_ascii=False, indent=1)
    print('已写 %s，覆盖 %d 个受管文件。之后不得再改代码；'
          '改动必须重跑 ./verify.sh' % (STAMP_F, len(d['files'])), flush=True)
    return 0


if __name__ == '__main__':
    sys.exit(main())
