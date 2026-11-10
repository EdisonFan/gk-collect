# -*- coding: utf-8 -*-
"""单个省分片自身 numFound 封顶（>=10000，接口截断假数）时的 G2 行为。

背景：44 省 2022~2025 按 34 省切分后，p44 自身就顶到 10000，
旧逻辑在 build_partition 里直接判死，只采 620 条。

修复后要求：
  1) 封顶片按科类、批次两个维度分别二次切分，两次 Σ 相等才认完备，
     用该 Σ 替换假数作为真值，G2 通过且条数守恒；
  2) 两维度 Σ 不等（说明候选值有漏）时必须判失败，绝不静默丢数据；
  3) 非封顶路径行为不变（由 test_special_1090.py / test_special_plan_reuse.py 覆盖）。

纯单元 mock：不启 HTTP、不发网络请求，直接替换 probe_strict_all / query_strict。
"""
import sys
import threading
import time

import collect_special as cs

TYPE_VALS = ['物理类', '历史类', '艺术类', '体育类']
BATCH_VALS = ['本科批', '专科批', '提前批', '专项批', '艺术批']
TRUE_TOTAL = 15000          # 两个维度都切成这个真值（父片假数 10000）
FAILS = []


def check(cond, msg):
    print(('  PASS  ' if cond else '  FAIL  ') + msg)
    if not cond:
        FAILS.append(msg)


class MockAPI:
    """按调用来源返回预设 numFound；batch_sum_equal=False 模拟维度漏值。"""

    def __init__(self, batch_sum_equal=True):
        self.batch_sum_equal = batch_sum_equal
        self.probe_tags = []

    def probe(self, specs, proxies, stats, lock, tag, t0):
        self.probe_tags.append(tag)
        out = []
        for extra, label in specs:
            if tag == '省':
                n = 10000 if extra.get('province_id') == '44' else 0
            else:
                dim = tag.rsplit('/', 1)[-1]
                v = extra.get(dim)
                if dim == 'local_type_name' and v in TYPE_VALS:
                    n = TRUE_TOTAL // len(TYPE_VALS)
                elif dim == 'local_batch_name' and v in BATCH_VALS:
                    n = (TRUE_TOTAL // len(BATCH_VALS)
                         if self.batch_sum_equal else 2400)
                else:
                    n = 0       # 兜底候选里没数据的值，合法空片
            out.append((extra, label, n, [], True, 20))
        return out

    def query(self, page, extra, proxies, stats, lock, delay):
        # discover_vals 翻页：第 1 页给出两个维度的全部取值，之后空页收尾
        if page > 1:
            return [], 0, True, '0000', 20
        items = [{'local_type_name': v, 'local_batch_name': BATCH_VALS[0]}
                 for v in TYPE_VALS]
        items += [{'local_type_name': TYPE_VALS[0], 'local_batch_name': v}
                  for v in BATCH_VALS]
        return items, TRUE_TOTAL, True, '0000', 20


def run_case(batch_sum_equal):
    m = MockAPI(batch_sum_equal)
    cs.probe_strict_all = m.probe
    cs.query_strict = m.query
    audit = {}
    leaves, ok, true_nf = cs.build_partition(
        10000, True, [], [], {}, threading.Lock(), time.time(), audit)
    return m, audit, leaves, ok, true_nf


print('=== 场景 1：两维度 Σ 相等，封顶片应被正确切分 ===')
m, audit, leaves, ok, true_nf = run_case(True)
leaf_sum = sum(x[1] for x in leaves)
check(ok is True, 'G2 通过（旧版在这里直接判死）')
check(true_nf == TRUE_TOTAL, '真值用子片 Σ=%d 替换假数 10000' % TRUE_TOTAL)
check(leaf_sum == TRUE_TOTAL, '叶子计划条数 Σ == 真值（条数守恒）')
check(len(audit.get('capped_provs', [])) == 1, '识别出 1 个封顶省片')
check(bool(audit.get('capped_true')), '记录了假数→真值的替换')
check(audit.get('capped_cross', [{}])[0].get('ok') is True,
      '科类/批次两维度交叉校验通过')
check(all(x[1] < cs.NUMFOUND_CAP for x in leaves),
      '所有叶子自身均未封顶')
print('     审计 capped_true=%s' % audit.get('capped_true'))
print('     审计 capped_cross=%s' % audit.get('capped_cross'))

print('')
print('=== 场景 2：两维度 Σ 不等，必须判失败（不静默丢数据）===')
m2, audit2, leaves2, ok2, true_nf2 = run_case(False)
check(ok2 is False, 'G2 判失败')
check(leaves2 == [], '不产生任何叶子（不拿残缺切分去采集）')
check(audit2.get('capped_cross', [{}])[0].get('ok') is False,
      '交叉校验如实记录不一致')
print('     审计 sums=%s' % audit2.get('capped_cross', [{}])[0].get('sums'))

print('')
if FAILS:
    print('结论：%d 项失败 ❌' % len(FAILS))
    for f in FAILS:
        print('  - ' + f)
    sys.exit(1)
print('结论：全部通过 ✅')
