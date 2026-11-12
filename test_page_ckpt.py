#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""专项：页级增量断点 + 超时按页量估算。

复现历史 bug：固定 3000s 超时 / 上层释放进程时，local_any.py 的页结果
全在内存 bucket，parts 目录为空，重跑「断点命中 0」从头再来
（41-2024 / 52-2025 / 32-2025）。本测试验证断点真能续采、且不会
把失败片锁死成死循环。
"""
import json
import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import page_ckpt

FAILS = []


def chk(cond, msg, detail=''):
    if cond:
        print('  PASS %s' % msg, flush=True)
    else:
        FAILS.append(msg)
        print('  FAIL %s %s' % (msg, detail), flush=True)


def test_roundtrip():
    print('-- 断点 save/load 往返', flush=True)
    d = tempfile.mkdtemp(prefix='ckpt_')
    p = os.path.join(d, 'pages.json')
    b = {
        'p62|科类=物理类': {
            1: ([{'id': 'a1'}, {'id': 'a2'}], '0000'),
            2: (None, '1090'),
            10: ([{'id': 'a3'}], None),
        },
        'p62|批次=本科批': {1: ([], '0000')},
    }
    n = page_ckpt.save(p, b)
    chk(n == 4, 'save 返回写入页数', 'got=%s' % n)
    chk(os.path.exists(p) and not os.path.exists(p + '.tmp'),
        '原子替换后无残留 .tmp')
    b2 = page_ckpt.load(p)
    chk(set(b2) == set(b), 'label 集合一致（含竖线 label）')
    chk(isinstance(list(b2['p62|科类=物理类'])[0], int),
        '页号还原为 int（persist 需 sorted(pages)）')
    # persist() 依赖 it, c = pages[p]，且要能识别 1090
    for pg in sorted(b2['p62|科类=物理类']):
        it, c = b2['p62|科类=物理类'][pg]
        chk(isinstance(c, (str, type(None))), '页 %d code 类型可判 1090' % pg)
    chk(b2['p62|科类=物理类'][2] == (None, '1090'),
        'None item + 1090 code 无损往返')
    chk(b2['p62|科类=物理类'][1][0][1]['id'] == 'a2', 'item 内容无损')


def test_pending():
    print('-- 断点过滤（模拟采到一半被杀）', flush=True)
    d = tempfile.mkdtemp(prefix='ckpt_')
    p = os.path.join(d, 'pages.json')
    tasks = [('L', 'e', i, 20) for i in range(1, 11)]
    b = {'L': {i: ([{'id': 'x%d' % i}], '0000') for i in range(1, 7)}}
    page_ckpt.save(p, b)
    rest = page_ckpt.pending(tasks, page_ckpt.load(p))
    chk([t[2] for t in rest] == [7, 8, 9, 10],
        '重跑只补 7~10 页，不重采已完成的 6 页',
        'got=%s' % [t[2] for t in rest])
    # 反向自证：空断点必须返回全部任务（不能恒真）
    chk(len(page_ckpt.pending(tasks, {})) == 10, '空断点不过滤任何页')
    # 不同 label 同页号互不干扰
    tasks2 = [('L', 'e', 1, 20), ('M', 'e', 1, 20)]
    b2 = {'L': {1: ([], '0000')}}
    chk(page_ckpt.pending(tasks2, b2) == [('M', 'e', 1, 20)],
        '按 (label,page) 精确命中')


def test_corrupt():
    print('-- 损坏/版本不符自愈', flush=True)
    d = tempfile.mkdtemp(prefix='ckpt_')
    p = os.path.join(d, 'pages.json')
    open(p, 'w').write('{ broken json')
    chk(page_ckpt.load(p) == {}, '损坏文件返回空而不抛异常')
    json.dump({'v': 999, 'pages': {'L|1': [[], '0000']}}, open(p, 'w'))
    chk(page_ckpt.load(p) == {}, '版本不符返回空')
    chk(page_ckpt.load(os.path.join(d, 'nope.json')) == {}, '文件不存在返回空')


def test_keep():
    print('-- 失败片剔除（防死循环）', flush=True)
    d = tempfile.mkdtemp(prefix='ckpt_')
    p = os.path.join(d, 'pages.json')
    b = {'A': {1: ([], '0000')}, 'B': {1: ([], '0000')}}
    n = page_ckpt.save(p, b, keep={'A'})
    chk(n == 1 and set(page_ckpt.load(p)) == {'A'},
        'keep 只保留指定 label，失败片断点作废', 'got=%s' % n)
    # 反向：keep=None 全保留
    chk(page_ckpt.save(p, b) == 2, 'keep=None 全量保留')


def test_timeout():
    print('-- 超时按页量估算', flush=True)
    chk(page_ckpt.timeout_for(0) == 3000, '无画像给下限 3000s')
    chk(page_ckpt.timeout_for(1363) > 3000,
        '62-2025 画像 1363 页突破旧固定 3000s',
        'got=%s' % page_ckpt.timeout_for(1363))
    chk(page_ckpt.timeout_for(1238) < page_ckpt.timeout_for(2848),
        '页多超时更长')
    chk(page_ckpt.timeout_for(999999) == 6000, '封顶 6000s 防失控')
    chk(page_ckpt.timeout_for(None) == 3000, 'None 画像不抛异常')


def contract_of(la, ra, pc):
    """可被影子用例复用的接入判定：主链路 7 项契约是否全部满足。"""
    return [
        ('os.replace' in pc, '断点原子落盘'),
        ('page_ckpt.load(CKPT)' in la, 'local_any 启动加载断点'),
        ('page_ckpt.pending(' in la, 'local_any 按断点过滤页任务'),
        ('page_ckpt.save(CKPT' in la, 'local_any 采集中增量落盘'),
        ('bad = set(' in la, '失败片从断点剔除'),
        ('page_ckpt.timeout_for' in ra, 'run_all 超时按页量估算'),
        ('timeout=3000' not in ra, '固定 3000s 已移除'),
    ]


def test_static_contracts():
    print('-- 静态契约：主链路确实接入（防改了模块没用上）', flush=True)
    la = open('local_any.py', encoding='utf-8').read()
    ra = open('run_all.py', encoding='utf-8').read()
    pc = open('page_ckpt.py', encoding='utf-8').read()
    for ok, name in contract_of(la, ra, pc):
        chk(ok, name)
    # 反向自证：造一份「退回旧版」的假源码，判定必须抓到退化（非恒真）
    old_la = 'bucket = {}\n    with ThreadPoolExecutor(12) as ex:\n'
    old_ra = 'stderr=subprocess.STDOUT, timeout=3000)'
    degraded = [ok for ok, _ in contract_of(old_la, old_ra, pc)]
    chk(sum(degraded) < len(degraded) and not all(degraded),
        '静态检查能抓到真实退化', 'degraded=%s' % degraded)


def main():
    print('=== 专项：页级断点 + 动态超时 ===', flush=True)
    for t in (test_roundtrip, test_pending, test_corrupt, test_keep,
              test_timeout, test_static_contracts):
        t()
    print('\n结论：%s' % ('全部通过' if not FAILS else
          '%d 项失败 -> %s' % (len(FAILS), FAILS)), flush=True)
    return 1 if FAILS else 0


if __name__ == '__main__':
    sys.exit(main())
