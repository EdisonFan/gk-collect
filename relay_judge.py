# -*- coding: utf-8 -*-
"""relay fan-in：判定本轮哪些组合未通过零丢失闸门，交给下一轮自接力。

由 collect.yml 的 relay job 调用（GITHUB_TOKEN，GitHub 内网拉取，秒级）。
输入环境变量：COMBOS(JSON 数组)、ROUND(当前轮次)
输出 GITHUB_OUTPUT：todo(JSON 数组)、next_round(int)

判定口径（不再是覆盖率 >=95%）：
  gate_pass == true 且 count == num_found 才算达标；
  分支不存在 / 文件读不出 / 字段缺失（旧 v4 结果），一律视为未达标并要求重采。
轮次达 MAX_RELAY 自动停手，避免无限递归；剩余未达标组合交给平台定时任务兜底。
"""
import os, json, subprocess

MAX_RELAY = 5
COMBOS = json.loads(os.environ.get('COMBOS', '[]'))
ROUND = int(os.environ.get('ROUND', '0') or 0)


def git(*a):
    return subprocess.run(['git'] + list(a), capture_output=True)


def set_output(todo, nxt):
    with open(os.environ['GITHUB_OUTPUT'], 'a', encoding='utf-8') as f:
        f.write('todo=%s\n' % json.dumps(todo, ensure_ascii=False))
        f.write('next_round=%d\n' % nxt)


def main():
    if ROUND >= MAX_RELAY:
        print('自接力轮次达上限 %d，停止（剩余未达标组合交给平台定时任务兜底）' % MAX_RELAY)
        set_output([], ROUND)
        return
    # 精确 refspec，只拉本轮组合对应的结果分支，避免通配符协商上百 ref
    rs = ['+refs/heads/cutoff-%s:refs/remotes/origin/cutoff-%s' % (c, c)
          for c in COMBOS]
    if rs:
        q = git('fetch', '-q', 'origin', *rs)
        print('拉取本轮 %d 个结果分支: %s' %
              (len(rs), 'OK' if q.returncode == 0 else 'FAILED(按未达标处理)'))
    todo = []
    for c in COMBOS:
        try:
            prov, year = c.split('-', 1)
        except ValueError:
            todo.append(c)
            continue
        ref = 'refs/remotes/origin/cutoff-%s' % c
        path = 'out/cutoff_%s_%s.json' % (prov, year)
        q = git('show', '%s:%s' % (ref, path))
        ok = False
        if q.returncode == 0 and q.stdout:
            try:
                d = json.loads(q.stdout.decode('utf-8'))
                ok = (d.get('gate_pass') is True
                      and d.get('count') == d.get('num_found')
                      and d.get('num_found', 0) > 0)
            except Exception:
                ok = False
        if not ok:
            todo.append(c)
    print('本轮 %d 组合 / 未达标 %d 个%s' %
          (len(COMBOS), len(todo), (': ' + ' '.join(todo[:15])) if todo else ''))
    set_output(todo, ROUND + 1)


if __name__ == '__main__':
    main()
