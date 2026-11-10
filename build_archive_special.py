# -*- coding: utf-8 -*-
"""专业录取线归档层生成器：拉取云端 special-* 分支结果 → 生成归档三件套。

用法：
  python3 build_archive_special.py fetch            # 从 GitHub 拉取全部 special-* 分支结果
  python3 build_archive_special.py fetch 44-2026     # 只拉指定组合
  python3 build_archive_special.py build            # 解析本地原始数据 → 统计/manifest

产物（写到 专业录取线/）：
  原始数据/special_<省>_<年>.json
  采集统计.csv / 采集统计.md / _manifest.json
"""
import base64, hashlib, io, json, os, sys, time, urllib.parse, urllib.request

ROOT = os.path.dirname(os.path.abspath(__file__))
OUT_DIR = os.path.join(ROOT, '专业录取线')
RAW_DIR = os.path.join(OUT_DIR, '原始数据')
REPO = 'EdisonFan/gk-collect'
PAT = open('/workspace/.secrets/github_pat').read().strip()
H = {'Authorization': 'Bearer ' + PAT, 'Accept': 'application/vnd.github+json',
     'X-GitHub-Api-Version': '2022-11-28', 'User-Agent': 'archive-special'}
API = 'https://api.github.com/repos/%s' % REPO

PROV_NAME = {
    '11': '北京市', '12': '天津市', '13': '河北省', '14': '山西省', '15': '内蒙古自治区',
    '21': '辽宁省', '22': '吉林省', '23': '黑龙江省', '31': '上海市', '32': '江苏省',
    '33': '浙江省', '34': '安徽省', '35': '福建省', '36': '江西省', '37': '山东省',
    '41': '河南省', '42': '湖北省', '43': '湖南省', '44': '广东省', '45': '广西壮族自治区',
    '46': '海南省', '50': '重庆市', '51': '四川省', '52': '贵州省', '53': '云南省',
    '54': '西藏自治区', '61': '陕西省', '62': '甘肃省', '63': '青海省',
    '64': '宁夏回族自治区', '65': '新疆维吾尔自治区',
}


def get(p, t=90):
    last = None
    for _ in range(3):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request(API + p, headers=H), timeout=t) as r:
                return json.load(r)
        except Exception as e:
            last = e
            time.sleep(1.2)
    raise last


def get_blob(sha, t=180):
    for _ in range(3):
        try:
            with urllib.request.urlopen(
                    urllib.request.Request('%s/git/blobs/%s' % (API, sha),
                                           headers=H), timeout=t) as r:
                return base64.b64decode(json.load(r)['content'])
        except Exception as e:
            time.sleep(1.5)
    raise RuntimeError('blob 下载失败 %s' % sha)


def do_fetch(only=None):
    os.makedirs(RAW_DIR, exist_ok=True)
    combos = only or []
    if not combos:
        brs = []
        for pg in (1, 2, 3, 4):
            b = get('/branches?per_page=100&page=%d' % pg)
            if not b:
                break
            brs += [x['name'] for x in b]
        combos = sorted(x[len('special-'):] for x in brs if x.startswith('special-'))
    print('待拉取 %d 个组合' % len(combos))
    ok, fail = 0, []
    for c in combos:
        p, y = c.split('-')
        br = 'special-%s-%s' % (p, y)
        path = 'out/special_%s_%s.json' % (p, y)
        dst = os.path.join(RAW_DIR, path)
        try:
            d = get('/contents/%s?ref=%s' % (urllib.parse.quote(path), br))
            raw = (base64.b64decode(d['content']) if d.get('content')
                   else get_blob(d['sha']))
            j = json.loads(raw.decode('utf-8'))
            with open(dst, 'wb') as f:
                f.write(raw)
            print('  OK   %-10s %8d bytes  gate_pass=%s count=%s'
                  % (c, len(raw), j.get('gate_pass'), j.get('count')))
            ok += 1
        except Exception as e:
            print('  FAIL %-10s %s' % (c, str(e)[:80]))
            fail.append(c)
    print('拉取完成：成功 %d，失败 %d %s' % (ok, len(fail), fail))


def do_build():
    files = sorted(f for f in os.listdir(RAW_DIR) if f.endswith('.json'))
    rows, manifest = [], {}
    for fn in files:
        p, y = os.path.splitext(fn)[0].split('_')[1:3]
        combo = '%s-%s' % (p, y)
        fp = os.path.join(RAW_DIR, fn)
        raw = open(fp, 'rb').read()
        sha16 = hashlib.sha256(raw).hexdigest()[:16]
        j = json.loads(raw.decode('utf-8'))
        gate = j.get('gate', {}) or {}
        meta = {
            'version': j.get('version'),
            'gate_pass': j.get('gate_pass'),
            'count': j.get('count'),
            'num_found': j.get('num_found'),
            'items': len(j.get('items') or []),
            'pages_total': j.get('pages_total'),
            'pages_ok': j.get('pages_ok'),
            'pages_failed': j.get('pages_failed'),
            'base_num_found': j.get('base_num_found'),
            'base_capped': j.get('base_capped'),
            'dup_in_api': j.get('dup_in_api'),
            'partition_count': j.get('partition_count'),
            'gate': gate,
            'collector_rev': j.get('collector_rev'),
            'collected_at': j.get('collected_at'),
            'elapsed': j.get('elapsed'),
            'sha256': sha16,
        }
        manifest[combo] = {'status': 'ok', 'bytes': len(raw), 'meta': meta}
        rows.append({
            'combo': combo, 'prov': p, 'year': y,
            'name': PROV_NAME.get(p, p),
            'count': j.get('count') or 0,
            'num_found': j.get('num_found') or 0,
            'pages_total': j.get('pages_total') or 0,
            'pages_ok': j.get('pages_ok') or 0,
            'pages_failed': j.get('pages_failed') or 0,
            'partition_count': j.get('partition_count') or 0,
            'dup_in_api': j.get('dup_in_api') or 0,
            'base_capped': '是' if j.get('base_capped') else '否',
            'G1': gate.get('G1_base'), 'G2': gate.get('G2_partition'),
            'G3': gate.get('G3_pages_zero_fail'), 'G4': gate.get('G4_count_match'),
            'ok': '是' if j.get('gate_pass') else '否',
            'version': j.get('version'),
            'collected_at': j.get('collected_at'),
            'bytes': len(raw), 'sha16': sha16,
        })
    rows.sort(key=lambda r: (r['prov'], r['year']))

    # ---- CSV ----
    hdr = ['省代码', '省份', '年份', '记录数', 'num_found', '分页总数', '分页成功',
           '分页失败', '分片条数', '接口内重复', 'BASE封顶', 'G1_base', 'G2_partition',
           'G3_pages_zero_fail', 'G4_count_match', '达标', '版本', '采集时间',
           '字节数', 'sha256前16']
    csv_path = os.path.join(OUT_DIR, '采集统计.csv')
    with io.open(csv_path, 'w', encoding='utf-8-sig', newline='') as f:
        f.write(','.join(hdr) + '\n')
        for r in rows:
            f.write(','.join(str(r[k]) for k in
                             ['prov', 'name', 'year', 'count', 'num_found', 'pages_total',
                              'pages_ok', 'pages_failed', 'partition_count', 'dup_in_api',
                              'base_capped', 'G1', 'G2', 'G3', 'G4', 'ok', 'version',
                              'collected_at', 'bytes', 'sha16']) + '\n')

    # ---- manifest ----
    with io.open(os.path.join(OUT_DIR, '_manifest.json'), 'w', encoding='utf-8') as f:
        json.dump(manifest, f, ensure_ascii=False, indent=1, sort_keys=True)

    # ---- MD ----
    total = sum(r['count'] for r in rows)
    nbytes = sum(r['bytes'] for r in rows)
    okrows = [r for r in rows if r['ok'] == '是']
    years, provs = {}, {}
    for r in rows:
        years.setdefault(r['year'], [0, 0])
        years[r['year']][0] += 1
        years[r['year']][1] += r['count']
        provs.setdefault(r['prov'], [0, 0, r['name']])
        provs[r['prov']][0] += 1
        provs[r['prov']][1] += r['count']
    md = []
    md.append('# 专业录取线 · 采集统计\n')
    md.append('> 由 `原始数据/` 下全部 JSON 文件**重新解析**生成，统计口径与归档文件一一对应。'
              '逐单元明细见 [`采集统计.csv`](采集统计.csv)。\n')
    md.append('## 一、总览\n')
    md.append('| 指标 | 值 |')
    md.append('| --- | --- |')
    md.append('| 采集单元数 | **%d** |' % len(rows))
    md.append('| 记录总条数 | **%s** 条 |' % format(total, ','))
    md.append('| 原始数据总体积 | %.1f MB（%s 字节） |' % (nbytes / 1048576.0, format(nbytes, ',')))
    md.append('| 达标单元 | **%d / %d** |' % (len(okrows), len(rows)))
    md.append('| 分页请求总数 / 失败数 | %s / **%d** |'
              % (format(sum(r['pages_total'] for r in rows), ','),
                 sum(r['pages_failed'] for r in rows)))
    md.append('| 接口内完全重复记录合计 | %s 条 |'
              % format(sum(r['dup_in_api'] for r in rows), ','))
    if rows:
        md.append('| 单份记录数范围 | %s ～ %s 条 |'
                  % (format(min(r['count'] for r in rows), ','),
                     format(max(r['count'] for r in rows), ',')))
        ts = [r['collected_at'] for r in rows if r['collected_at']]
        if ts:
            md.append('| 采集时间范围 | %s ～ %s |' % (min(ts), max(ts)))
    md.append('')
    md.append('**完整性结论**：%d 个采集单元中 %d 个满足 `version=special-v1.1`、四道闸门全通过、'
              '分页零失败、`count = num_found = len(items)`。'
              % (len(rows), len(okrows)))
    md.append('')
    md.append('## 二、按年份汇总\n')
    md.append('| 年份 | 采集单元 | 记录数 | 占比 |')
    md.append('| --- | --- | --- | --- |')
    for y in sorted(years):
        n, c = years[y]
        md.append('| %s | %d | %s | %.1f%% |'
                  % (y, n, format(c, ','), 100.0 * c / total if total else 0))
    md.append('| **合计** | **%d** | **%s** | 100.0%% |' % (len(rows), format(total, ',')))
    md.append('')
    md.append('## 三、按省份汇总\n')
    md.append('| 代码 | 省份 | 采集单元 | 记录数 |')
    md.append('| --- | --- | --- | --- |')
    for p in sorted(provs):
        n, c, nm = provs[p]
        md.append('| %s | %s | %d | %s |' % (p, nm, n, format(c, ',')))
    md.append('')
    bad = [r for r in rows if r['ok'] != '是']
    md.append('## 四、未达标单元\n')
    if not bad:
        md.append('无。全部单元四道闸门通过、条数守恒。')
    else:
        md.append('| 组合 | 记录数 | num_found | 分页失败 | 未过闸门 |')
        md.append('| --- | --- | --- | --- | --- |')
        for r in bad:
            g = [k for k in ('G1', 'G2', 'G3', 'G4') if r[k] is not True]
            md.append('| %s | %s | %s | %s | %s |'
                      % (r['combo'], r['count'], r['num_found'], r['pages_failed'],
                         ','.join(g) or '-'))
    md.append('')
    md_path = os.path.join(OUT_DIR, '采集统计.md')
    with io.open(md_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md))
    print('生成完成：%d 单元 / %s 条 / %.1f MB' % (len(rows), format(total, ','), nbytes / 1048576.0))
    print('  %s' % csv_path)
    print('  %s' % md_path)
    print('  %s' % os.path.join(OUT_DIR, '_manifest.json'))


if __name__ == '__main__':
    cmd = sys.argv[1] if len(sys.argv) > 1 else 'build'
    if cmd == 'fetch':
        do_fetch(sys.argv[2:] or None)
    else:
        do_build()
