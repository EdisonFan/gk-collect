# -*- coding: utf-8 -*-
"""生成专业线派发用的规模档位表 scale_profile.json。

数据源全部是已落盘的真实采集结果，不是拍脑袋估计：
  - 投档线 186 个组合：out/cutoff_*.json 的 num_found，全覆盖、零成本可得。
  - 专业线已达标组合：out/special_*.json 的 pages_total / elapsed，用于标定。

局限（如实标注，不要当精确工作量用）：
  专业线/投档线的条数比在同省不同年份之间可差 0.8~10 倍
  （例如 12 省 2021 年 3.5 倍、2026 年 0.8 倍），因此本表只用于
  「粗分档 + 分批排序」，避免大小省混装堵住并发槽。
  真实页数/耗时在采集完成后由 feed_special.py 回填到台账。

用法：
  python3 build_scale_profile.py            # 从本地 git 分支重算
  python3 build_scale_profile.py --check    # 只校验现有文件不重写
"""
import json
import os
import re
import subprocess
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, 'scale_profile.json')

# 依据已达标专业线的真实表现标定：页数 >1000 或耗时 >100 分钟的大省。
# 这些省即使投档线条数不高（如 33-2026 投档仅 2574 条、专业线却 1309 页），
# 也必须归入重档，否则会被误判成轻省塞进大批次。
KNOWN_HEAVY = {'13', '41', '14', '23', '33'}
HEAVY_CUTOFF = 6000
MEDIUM_CUTOFF = 2500
HEAVY_REAL_PAGES = 1000
MEDIUM_REAL_PAGES = 600


def git_branches(prefix):
    out = subprocess.check_output(
        ['git', 'branch', '-r', '--format=%(refname:short)'], cwd=HERE, timeout=60
    ).decode()
    return [b for b in out.split() if b.startswith('origin/' + prefix)]


def read_head(ref, path, nbytes=65536):
    """只读产物文件头部提取标量字段，避免对 40MB 结果文件做全量解析。"""
    raw = subprocess.check_output(
        ['git', 'show', '%s:%s' % (ref, path)], cwd=HERE, timeout=90
    )[:nbytes].decode('utf-8', errors='ignore')
    return raw


def num_of(raw, key):
    m = re.search(r'"%s"\s*:\s*(-?\d+(?:\.\d+)?)' % key, raw)
    return float(m.group(1)) if m else None


def pick_result_file(ref, prefix):
    tree = subprocess.check_output(
        ['git', 'ls-tree', '-r', '--name-only', ref], cwd=HERE, timeout=40
    ).decode().splitlines()
    js = [x for x in tree if x.startswith('out/' + prefix) and x.endswith('.json')]
    if not js:
        return None
    return max(js, key=lambda x: int(subprocess.check_output(
        ['git', 'cat-file', '-s', '%s:%s' % (ref, x)], cwd=HERE, timeout=40
    ).decode().strip()))


def collect_cutoff():
    """投档线 186 组合真实条数。"""
    data = {}
    for b in git_branches('cutoff-'):
        parts = b.split('-')
        prov, year = parts[1], parts[2]
        f = pick_result_file(b, 'cutoff_')
        if not f:
            continue
        raw = read_head(b, f, 4096)
        if '"gate_pass": true' not in raw and '"gate_pass":true' not in raw:
            continue
        nf = num_of(raw, 'num_found') or num_of(raw, 'count')
        if nf:
            data['%s-%s' % (prov, year)] = int(nf)
    return data


def collect_special():
    """专业线已达标组合的真实页数与耗时。"""
    data = {}
    for b in git_branches('special-'):
        parts = b.split('-')
        prov, year = parts[1], parts[2]
        f = pick_result_file(b, 'special_')
        if not f:
            continue
        raw = read_head(b, f, 65536)
        if '"gate_pass": true' not in raw and '"gate_pass":true' not in raw:
            continue
        pg = num_of(raw, 'pages_total')
        el = num_of(raw, 'elapsed')
        data['%s-%s' % (prov, year)] = {
            'real_pages': int(pg) if pg else None,
            'real_minutes': round(el / 60, 1) if el else None,
        }
    return data


def build():
    cut = collect_cutoff()
    sp = collect_special()
    if not cut:
        raise SystemExit('未读到投档线产物，请先 fetch cutoff-* 分支')

    # 同省标定：页/投档条 的比，用于给未采组合估页数
    by_prov = defaultdict(list)
    for combo, real in sp.items():
        c = cut.get(combo)
        if c and real['real_pages']:
            by_prov[combo.split('-')[0]].append(real['real_pages'] / c)
    global_ratio = sorted(
        r for rs in by_prov.values() for r in rs
    )[len([r for rs in by_prov.values() for r in rs]) // 2] if by_prov else 0.25

    # 每页耗时标定（分钟/页），同样按省取中位
    mins_per_page = defaultdict(list)
    for combo, real in sp.items():
        if real['real_pages'] and real['real_minutes']:
            mins_per_page[combo.split('-')[0]].append(
                real['real_minutes'] / real['real_pages'])
    global_mpp = 0.06

    out = {
        '_meta': {
            'desc': '专业线派发规模档位表；粗分档用，非精确工作量',
            'source': '投档线 186 组合 num_found + 专业线已达标组合真实 pages/elapsed',
            'cutoff_combos': len(cut),
            'special_calibrated': len(sp),
            'known_heavy_provs': sorted(KNOWN_HEAVY),
            'limits': '专业线/投档线比在 0.8~10 倍间波动，est_pages 误差可达数倍',
        },
        'combos': {},
    }
    for combo in sorted(cut):
        prov, year = combo.split('-')
        c = cut[combo]
        real = sp.get(combo, {})
        rp, rm = real.get('real_pages'), real.get('real_minutes')

        ratio = sorted(by_prov[prov])[len(by_prov[prov]) // 2] if by_prov.get(prov) else global_ratio
        mpp = sorted(mins_per_page[prov])[len(mins_per_page[prov]) // 2] if mins_per_page.get(prov) else global_mpp
        est_pages = int(c * ratio) if rp is None else rp
        est_minutes = round(est_pages * mpp, 1) if rm is None else rm

        # 档位以「预估页数」为主判据，投档线条数只作兜底。
        # 反例：44 广东投档线 9379 条（很大），专业线却只有 370 页，
        # 纯按投档线会把 262 页的轻活误判成 heavy。
        # 已知重省额外提一档，防止同省外推低估（如 33-2026）。
        if est_pages >= HEAVY_REAL_PAGES:
            tier = 'heavy'
        elif est_pages >= MEDIUM_REAL_PAGES:
            tier = 'medium'
        elif prov in KNOWN_HEAVY and est_pages >= 400:
            tier = 'medium'
        else:
            tier = 'light'

        out['combos'][combo] = {
            'prov': prov, 'year': year,
            'cutoff_num_found': c,
            'tier': tier,
            'est_pages': est_pages,
            'est_minutes': est_minutes,
            'basis': '实测' if rp is not None else '同省标定外推' if by_prov.get(prov) else '全局标定外推',
            'real_pages': rp,
            'real_minutes': rm,
        }
    return out


def main():
    if '--check' in sys.argv:
        d = json.load(open(OUT, encoding='utf-8'))
        cs = d['combos']
        from collections import Counter
        print('档位表校验: 组合=%d  分布=%s' % (len(cs), dict(Counter(x['tier'] for x in cs.values()))))
        print('实测标定=%d  外推=%d' % (
            sum(1 for x in cs.values() if x['basis'] == '实测'),
            sum(1 for x in cs.values() if x['basis'] != '实测')))
        return
    d = build()
    with open(OUT, 'w', encoding='utf-8') as f:
        json.dump(d, f, ensure_ascii=False, indent=1)
    from collections import Counter
    print('已生成 %s' % OUT)
    print('组合=%d 档位分布=%s 实测标定=%d' % (
        len(d['combos']), dict(Counter(x['tier'] for x in d['combos'].values())),
        d['_meta']['special_calibrated']))


if __name__ == '__main__':
    main()
