# -*- coding: utf-8 -*-
"""v5 零丢失验收：逐组合核对落盘结果，产出未达标清单供补派。

判定（全部满足才算达标）：
  version == v5
  gate_pass == true 且四道闸门各自为 true
  count == num_found 且 partition_count == num_found
  pages_failed == 0
  partition.prov_sum == num_found 且 partition.prov_failed 为空

用法：
  python3 verify_v5.py                      汇总 + 未达标明细
  python3 verify_v5.py --combo 65-2026      单组合完整详情
  python3 verify_v5.py --todo gh_todo_v5.json  输出未达标清单(feed_gh 可消费)
  python3 verify_v5.py --json               机器可读汇总
"""
import os, sys, json, glob, argparse

DEST = '/workspace/高考填报数据模型与流程/采集原始数据/gk_cutoff'
PROVS = ['13', '41', '44', '37', '11', '12', '21', '22', '23', '31',
         '32', '33', '34', '35', '36', '42', '43', '45', '46', '50',
         '51', '52', '53', '61', '62', '65', '14', '15']
YEARS = ['2021', '2022', '2023', '2024', '2025', '2026']
GATES = ('G1_base', 'G2_partition', 'G3_pages_zero_fail', 'G4_count_match')


def load():
    out = {}
    for f in glob.glob(os.path.join(DEST, 'cutoff_*.json')):
        try:
            d = json.load(open(f, encoding='utf-8'))
            out['%s-%s' % (d.get('prov'), d.get('year'))] = d
        except Exception:
            pass
    return out


def check(d):
    r = []
    if d.get('version') != 'v5':
        r.append('非v5(旧结果需重采)')
    g = d.get('gate') or {}
    for k in GATES:
        if g.get(k) is not True:
            r.append('%s!=true' % k)
    if d.get('gate_pass') is not True:
        r.append('gate_pass!=true')
    nf, c, pc = d.get('num_found'), d.get('count'), d.get('partition_count')
    if not nf:
        r.append('numFound缺失')
    if c != nf:
        r.append('count%s!=numFound%s' % (c, nf))
    if pc != nf:
        r.append('part%s!=numFound%s' % (pc, nf))
    if d.get('pages_failed'):
        r.append('失败页%s' % d['pages_failed'])
    part = d.get('partition') or {}
    if part.get('prov_sum') != nf:
        r.append('Σ%s!=BASE%s' % (part.get('prov_sum'), nf))
    if part.get('prov_failed'):
        r.append('探测失败省%s' % len(part['prov_failed']))
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--combo')
    ap.add_argument('--todo')
    ap.add_argument('--json', action='store_true')
    a = ap.parse_args()
    data = load()
    allc = sorted(['%s-%s' % (p, y) for p in PROVS for y in YEARS] +
                  [k for k in data if k not in
                   ['%s-%s' % (p, y) for p in PROVS for y in YEARS]])
    bad, reasons = [], {}
    for c in allc:
        d = data.get(c)
        rs = check(d) if d else ['无结果文件']
        if rs:
            bad.append(c)
            for x in rs:
                reasons[x] = reasons.get(x, 0) + 1
    ok = len(allc) - len(bad)
    total = sum(d.get('count', 0) for d in data.values())

    if a.combo:
        d = data.get(a.combo)
        if not d:
            print('%s 无结果文件' % a.combo); return
        print('=== %s 零丢失验收 ===' % a.combo)
        print('  version=%s  gate_pass=%s' % (d.get('version'), d.get('gate_pass')))
        print('  闸门: %s' % json.dumps(d.get('gate'), ensure_ascii=False))
        p = d.get('partition') or {}
        print('  BASE=%s  34省Σ=%s  探测失败省=%s  叶子=%s  页任务=%s' %
              (d.get('num_found'), p.get('prov_sum'), p.get('prov_failed'),
               p.get('leaves'), p.get('pages')))
        print('  count=%s  partition_count=%s  pages_ok=%s  pages_failed=%s' %
              (d.get('count'), d.get('partition_count'),
               d.get('pages_ok'), d.get('pages_failed')))
        print('  耗时=%ss  采集时间=%s' % (d.get('elapsed'), d.get('collected_at')))
        print('  判定: %s' % ('✅ 达标' if not check(d) else '❌ ' + '; '.join(check(d))))
        return

    if a.json:
        print(json.dumps({'total': len(allc), 'ok': ok, 'bad': len(bad),
                          'records': total, 'bad_list': bad,
                          'reasons': reasons}, ensure_ascii=False))
        return

    print('=== v5 零丢失验收 ===')
    print('组合 %d / 达标 %d / 未达标 %d / 落盘记录 %d 条' %
          (len(allc), ok, len(bad), total))
    print('未达标原因分布:')
    for k, v in sorted(reasons.items(), key=lambda x: -x[1]):
        print('  %-40s %d' % (k, v))
    if bad:
        print('未达标清单(前 40): %s' % ' '.join(bad[:40]))
        if len(bad) > 40:
            print('  …另有 %d 个' % (len(bad) - 40))
    if a.todo:
        with open(a.todo, 'w', encoding='utf-8') as f:
            json.dump({'missing': bad, 'low': []}, f, ensure_ascii=False)
        print('已写出未达标清单 -> %s (%d 个)' % (a.todo, len(bad)))


if __name__ == '__main__':
    main()
