# -*- coding: utf-8 -*-
"""ETL: 掌上高考已采分支 -> Supabase (T05/T06/T08)

云端版（GitHub Actions runner 内运行）。与本地版口径完全一致：
  * 归一函数与键构造全部从 build_basic_data.py import（随仓库 tools/ 携带一份副本），
    杜绝「两份实现各自漂移」。
  * plan_key 11 段拼接，_prog_seg 对长文本取 sha1 摘要（2026-11-23 修正，
    原实现会让 plan_key 超 VARCHAR(160)）。

用法:
  python3 tools/etl_to_supabase.py --kind plan --combo 44-2024
  python3 tools/etl_to_supabase.py --kind plan --combo 44-2024 --workdir /tmp/etl
"""
import argparse
import hashlib
import json
import os
import re
import sys
import time
import urllib.request
import urllib.error

# ---------------------------------------------------------------- 口径（与 build_basic_data.py 一致）
CAT_MAP = {
    '物理类': '物理类', '历史类': '历史类', '理科': '理科', '文科': '文科', '综合': '综合',
    '艺术类': '艺术类', '体育类': '体育类', '艺术类（历史）': '艺术类', '艺术类（物理）': '艺术类',
    '艺术文': '艺术类', '艺术理': '艺术类', '体育文': '体育类', '体育理': '体育类',
}


# ---- 选科要求解析（与 subject_req_parser.py 同一实现，内联避免云端多文件依赖）----
# -*- coding: utf-8 -*-
"""选科要求明文 -> (first_subject, recheck_rule, recheck_subjects)

按 sys_dict_data(dict_type='gk_recheck_rule') 的值域解析：
  UNLIMITED     不限
  CHEM_REQUIRED 必选化学
  CHOOSE_1_OF_2 二选一
  CHOOSE_2_OF_3 三选二
  OTHER         其他

原 build_basic_data.py 的粗判（'化学' in raw -> CHEM_REQUIRED）会把
「物理/化学(2选1)」「化学/生物(2选1)」误判成「必选化学」——这会让选科过滤
错误剔除合法考生（静默失效，最危险的一类错误）。本模块按明文语义精确解析。

先科编码对照（掌上高考 sp_xuanke / sg_fxk 里的数字）：
  70001 物理  70002 历史  70003 思想政治  70004 地理
  70005 生物  70006 化学
"""
import re

# 科目名 -> 平台编码
SUBJ_CODE = {
    '物理': '70001', '历史': '70002', '思想政治': '70003', '政治': '70003',
    '地理': '70004', '生物': '70005', '化学': '70006',
}
# 常见缩写单字（北京等省份的「物/化/生(3选1)」这类写法）
SUBJ_ALIAS = {
    '物': '物理', '化': '化学', '生': '生物',
    '史': '历史', '政': '思想政治', '地': '地理',
}
CODE_SUBJ = {v: k for k, v in SUBJ_CODE.items()}
# 归一科目名（思想政治 与 政治 同义）
_SUBJ_ALIAS = {'政治': '思想政治'}


def _norm_subject(name):
    n = (name or '').strip()
    return _SUBJ_ALIAS.get(n, n)


def parse_subject_requirement(raw):
    """解析选科要求明文。

    返回 (first_subject, recheck_rule, recheck_subjects)
      first_subject     'PHYSICS' / 'HISTORY' / 'UNLIMITED'
      recheck_rule      'UNLIMITED' / 'CHEM_REQUIRED' / 'CHOOSE_1_OF_2'
                        / 'CHOOSE_2_OF_3' / 'OTHER'
      recheck_subjects  list[str]，再选科目编码数组（[] 表示不限）
    """
    text = (raw or '').strip()
    if not text or text in ('不限', '无限制', '不提科目要求'):
        return 'UNLIMITED', 'UNLIMITED', []

    # 拆「首选X，再选Y」/「首选X」两段
    first = 'UNLIMITED'
    recheck_part = text
    m = re.match(r'^首选\s*([^，,]+)\s*[，,]\s*(.+)$', text)
    if m:
        first_txt, recheck_part = m.group(1), m.group(2)
        for name in ('物理', '历史'):
            if name in first_txt:
                first = 'PHYSICS' if name == '物理' else 'HISTORY'
                break
    else:
        m2 = re.match(r'^首选\s*([^，,]+)$', text)
        if m2:
            first_txt = m2.group(1)
            recheck_part = ''
            for name in ('物理', '历史'):
                if name in first_txt:
                    first = 'PHYSICS' if name == '物理' else 'HISTORY'
                    break

    rp = (recheck_part or '').strip()
    if not rp:
        return first, 'UNLIMITED', []

    if '不限' in rp:
        return first, 'UNLIMITED', []

    # 拆出「N选M」结构：中文数字或阿拉伯数字
    pick = re.search(r'(\d+)\s*选\s*(\d+)', rp)
    n_sel, m_need = (int(pick.group(1)), int(pick.group(2))) if pick else (None, None)

    # 提取科目列表：按「、」「/」「或」「和」分隔
    subj_part = rp
    if pick:
        subj_part = rp[:pick.start()] + rp[pick.end():]
    # 去掉「(2科必选)」「（2科必选）」这类括号说明
    subj_part = re.sub(r'[（(][^）)]*[）)]', '', subj_part)
    subj_part = re.sub(r'(再选|必选|选考|不限科目要求)', '', subj_part)
    parts = re.split(r'[、,/或和及]|与', subj_part)
    subjects = []
    for p in parts:
        p = p.strip()
        if not p:
            continue
        hit = None
        for name in ('物理', '历史', '思想政治', '政治', '地理', '生物', '化学'):
            if name in p:
                hit = name
                break
        if hit is None:
            # 缩写单字：物/化/生/史/政/地（需长度==1，避免误伤）
            for short, full in SUBJ_ALIAS.items():
                if short in p and len(p) <= 2:
                    hit = full
                    break
        if hit:
            nn = _norm_subject(hit)
            if nn not in subjects:
                subjects.append(nn)
    codes = [SUBJ_CODE[s] for s in subjects if s in SUBJ_CODE]

    # 判定规则（recheck_rule 只用字典值域；具体科目靠 recheck_subjects 承载）
    if n_sel and m_need:
        if n_sel == 2 and m_need == 1:
            rule = 'CHOOSE_1_OF_2'
        elif n_sel == 3 and m_need == 2:
            rule = 'CHOOSE_2_OF_3'
        else:
            rule = 'OTHER'
    elif subjects == ['化学']:
        rule = 'CHEM_REQUIRED'
    elif len(subjects) == 1:
        # 单科必选但不是化学（地理/生物/思想政治等）：
        # 字典无对应值 -> OTHER，具体科目见 recheck_subjects
        rule = 'OTHER'
    elif subjects:
        # 列了多科但没写选几 -> 语义不明 -> OTHER（codes 已承载科目信息）
        rule = 'OTHER'
    else:
        rule = 'OTHER'

    return first, rule, codes




def cat_of(s):
    return CAT_MAP.get((s or '').strip(), '综合')


def batch_of(s):
    s = s or ''
    if '提前' in s:
        return '本科提前批'
    if '专科' in s or '高职' in s:
        return '高职专科批'
    return '本科批'


PTYPE_MAP = {
    '普通类': 'NORMAL', '-': 'NORMAL', '': 'NORMAL',
    '国家专项计划': 'NATIONAL_SPECIAL', '高校专项计划': 'UNIV_SPECIAL',
    '地方专项计划': 'LOCAL_SPECIAL', '中外合作办学': 'SINO_FOREIGN',
    '定向': 'DIRECTED', '民族班': 'ETHNIC_CLASS', '预科': 'PREP',
    '强基计划': 'STRONG_BASE', '艺术类': 'ART', '艺术类专业': 'ART', '体育类': 'SPORT',
}


def ptype_of(s):
    return PTYPE_MAP.get((s or '').strip(), 'OTHER')


def group_of(sg_name):
    m = re.search(r'(\d+)', sg_name or '')
    return m.group(1) if m else ''


MODE_MAJOR_SCHOOL = {'33', '37', '13', '50', '21', '63', '52'}
REFORM_YEAR = {
    '31': 2017, '33': 2017, '11': 2020, '12': 2020, '37': 2020, '46': 2020,
    '13': 2021, '21': 2021, '32': 2021, '35': 2021, '42': 2021, '43': 2021, '44': 2021, '50': 2021,
    '23': 2024, '62': 2024, '22': 2024, '34': 2024, '36': 2024, '52': 2024, '45': 2024,
    '14': 2025, '15': 2025, '41': 2025, '51': 2025, '53': 2025, '61': 2025, '64': 2025, '63': 2025,
}


def mode_of(region, year):
    ry = REFORM_YEAR.get(region)
    if not ry or year < ry:
        return 'TRADITIONAL'
    if region in MODE_MAJOR_SCHOOL:
        return 'MAJOR_SCHOOL'
    return 'SCHOOL_GROUP'


def _detail_seg(detail):
    d = (detail or '').strip()
    return d if d else '-'


def _prog_seg(prog):
    """培养项目摘要：短文本原样，长文本取 sha1 前 12 位（确定性、保留区分能力）。

    与 build_basic_data.py 的同名函数完全一致（2026-11-23 同步修正）。
    """
    p = (prog or '').strip()
    if not p:
        return '-'
    if len(p) <= 40:
        return p
    return 'h' + hashlib.sha1(p.encode('utf-8')).hexdigest()[:12]


def unit_key(mode, region, year, cat, batch, school, group='', major='', ptype='NORMAL',
             detail='', prog=''):
    base = '%s|%s|%s|%s|%s' % (region, year, cat, batch, school)
    d = _detail_seg(detail)
    if mode == 'SCHOOL_GROUP':
        return '%s|%s|%s|%s' % (base, group, ptype, d)
    if mode == 'MAJOR_SCHOOL':
        return '%s|%s|%s|%s' % (base, major, ptype, d)
    return '%s|%s|%s' % (base, ptype, d)


def plan_key(region, year, cat, batch, school, group, major, ptype, detail='', prog='',
             mname=''):
    return '%s|%s|%s|%s|%s|%s|%s|%s|%s|%s|%s' % (
        region, year, cat, batch, school, group, major, ptype,
        _detail_seg(detail), _prog_seg(prog), (mname or '').strip() or '-')


# ---------------------------------------------------------------- HTTP
SB_URL = 'https://ripobzmwgmdxyixoevee.supabase.co/rest/v1'
SB_KEY = os.environ.get('SUPABASE_ANON_KEY', '').strip()
GH_TOKEN = os.environ.get('GH_TOKEN', '').strip()
REPO = 'EdisonFan/gk-collect'
BATCH_SIZE = 500

# 各表列宽（与建表 DDL 一致）。用于插入前的长度守卫 —— PostgreSQL 对 varchar
# 超长直接报错（22001），且报错信息不含列名，只能靠本地预检兜住。
COL_WIDTH = {
    'subject_requirement': {
        'region_code': 12, 'year': 6, 'school_code': 16, 'unit_key': 96,
        'major_code': 16, 'first_subject': 12, 'recheck_rule': 24,
        'requirement_code': 24, 'raw_text': 255, 'source': 64, 'data_version': 32,
    },
    'enrollment_plan': {
        'plan_key': 160, 'unit_key': 96, 'region_code': 12, 'year': 6,
        'category': 24, 'batch': 40, 'school_code': 16, 'school_name': 120,
        'group_code': 32, 'group_name': 120, 'major_code': 24, 'major_name': 200,
        'major_note': 500, 'program_note': 500, 'plan_type': 24,
        'plan_type_detail': 64, 'campus': 120, 'status': 12, 'source': 64,
        'data_version': 32,
    },
    'admission_cutoff': {
        'unit_key': 96, 'region_code': 12, 'year': 6, 'category': 24, 'batch': 40,
        'school_code': 16, 'school_name': 120, 'group_code': 32, 'group_name': 120,
        'plan_type': 24, 'plan_type_detail': 64, 'subject_req_raw': 255,
        'source': 64, 'data_version': 32,
    },
    'special_plan': {
        'plan_type': 32, 'region_code': 12, 'year': 6, 'school_code': 16,
        'admit_rule': 500, 'volunteer_mode': 20, 'source': 64, 'data_version': 32,
    },
}


def clamp_rows(table, rows):
    """按列宽截断字符串列（超长则截断并保留前缀，保证可读与确定性）。"""
    widths = COL_WIDTH.get(table)
    if not widths:
        return rows, 0
    n = 0
    for r in rows:
        for k, w in widths.items():
            v = r.get(k)
            if isinstance(v, str) and len(v) > w:
                r[k] = v[:w]
                n += 1
    return rows, n


def _req(url, data=None, headers=None, method=None, retries=5):
    h = {'apikey': SB_KEY, 'Authorization': 'Bearer ' + SB_KEY}
    if headers:
        h.update(headers)
    body = None
    if data is not None:
        body = json.dumps(data, ensure_ascii=False).encode('utf-8')
        h['Content-Type'] = 'application/json'
    last = None
    for i in range(retries):
        try:
            req = urllib.request.Request(url, data=body, headers=h, method=method)
            return urllib.request.urlopen(req, timeout=180).read().decode('utf-8', 'ignore')
        except urllib.error.HTTPError as e:
            last = '%s %s' % (e.code, e.read().decode('utf-8', 'ignore')[:400])
            if e.code in (400, 409, 412, 422):
                raise RuntimeError('HTTP %s: %s' % (e.code, last))
            time.sleep(4 + i * 5)
        except Exception as e:
            last = str(e)[:200]
            time.sleep(4 + i * 5)
    raise RuntimeError('请求失败 %s (%s)' % (url[:140], last))


def upsert(table, rows, conflict=None):
    """PostgREST upsert（幂等）。

    两个坑：
    1) on_conflict 必须是 URL query 参数，塞进 Prefer 头会被忽略并退化成按主键 upsert。
    2) PostgreSQL 的 ON CONFLICT DO UPDATE 不允许「同一批次内」出现重复冲突键
       (500: "ON CONFLICT DO UPDATE command cannot affect row a second time")。
       因此插入前必须按最终唯一键做批内去重。
    """
    if not rows:
        return 0
    rows, clamped = clamp_rows(table, rows)
    if clamped:
        print('    [截断] %s %d 个超长字段已按列宽截断' % (table, clamped), flush=True)
    if conflict:
        cols = conflict.split(',')
        seen = set()
        dedup = []
        for r in rows:
            k = tuple(str(r.get(c, '')) for c in cols)
            if k in seen:
                continue
            seen.add(k)
            dedup.append(r)
        if len(dedup) != len(rows):
            print('    [去重] %s 批内 %d -> %d 行（重复唯一键）' % (
                table, len(rows), len(dedup)), flush=True)
        rows = dedup
    url = '%s/%s' % (SB_URL, table)
    if conflict:
        url += '?on_conflict=' + conflict
    _req(url, data=rows,
         headers={'Prefer': 'resolution=merge-duplicates,return=minimal'}, method='POST')
    return len(rows)


# ---------------------------------------------------------------- 下载
def fetch_branch_file(kind, combo, out_path):
    prov, year = combo.split('-')
    if kind == 'plan':
        br, path = 'plan-' + combo, 'out/plan_%s_%s.json' % (prov, year)
    else:
        br, path = 'cutoff-' + combo, 'out/cutoff_%s_%s.json' % (prov, year)
    url = 'https://raw.githubusercontent.com/%s/%s/%s' % (REPO, br, path)
    hdr = {'Authorization': 'Bearer ' + GH_TOKEN} if GH_TOKEN else {}
    for attempt in range(10):
        try:
            req = urllib.request.Request(url, headers=hdr)
            data = urllib.request.urlopen(req, timeout=900).read()
            with open(out_path, 'wb') as f:
                f.write(data)
            json.loads(data.decode('utf-8', 'ignore'))
            return out_path, len(data)
        except Exception as e:
            print('  [下载重试 %d] %s %s: %s' % (attempt + 1, kind, combo, str(e)[:90]),
                  flush=True)
            time.sleep(5 + attempt * 5)
    raise RuntimeError('下载失败 %s %s' % (kind, combo))


# ---------------------------------------------------------------- 映射
def plan_rows(items, prov, year):
    plans, subreq_rows, subreq_by_unit = [], [], {}
    seen_pk = set()

    for x in items:
        cat = cat_of(x.get('local_type_name'))
        batch = batch_of(x.get('local_batch_name'))
        ptype = ptype_of(x.get('zslx_name'))
        detail = (x.get('zslx_name') or '').strip()
        group = group_of(x.get('sg_name'))
        major = (x.get('spcode') or '').strip()
        major = '' if major in ('-',) else major
        sid = str(x.get('school_id') or '')
        mode = mode_of(prov, year)
        uk = unit_key(mode, prov, year, cat, batch, sid, group, major, ptype, detail)
        req_code = str(x.get('sp_fxk') or x.get('sp_xuanke') or '').strip()
        # 专业级编码缺失时回退到专业组级编码 sg_fxk。
        # 实测 44-2024：sp_fxk 在 25152 行里仅 1 行有值，而 sg_fxk 100% 有值
        # （70000=物理类 / 70004=历史类）。sg_fxk 是「首选科目」级别，
        # 再选科目的差异靠 sg_info 明文区分（明文有 5 种，编码只有 2 种）。
        if not req_code:
            req_code = str(x.get('sg_fxk') or '').strip()
        # 长度守卫：requirement_code 列是 VARCHAR(24)。
        # 北京的 sp_xuanke 是「首选+再选的所有合法组合」枚举串，最长 269 字符
        # （实测 11-2023），远超列宽。明文已存在 raw_text / sp_info 里，
        # 这里用 sha1 摘要占位，既不丢信息又保证唯一。
        if len(req_code) > 24:
            req_code = 'X' + hashlib.sha1(req_code.encode('utf-8')).hexdigest()[:23]
        raw = str(x.get('sp_info') or x.get('sg_info') or '').strip()
        if not req_code and not raw:
            continue
        # 选科要求语义解析（精确区分「必选化学」/「二选一」/「三选二」）。
        # 原粗判 '化学' in raw 会把「物理/化学(2选1)」误判成必选化学，
        # 导致选科过滤错误剔除合法考生 —— 静默失效，必须精确。
        fs, rr, subj_codes = parse_subject_requirement(raw)
        # requirement_code 必须与「批内唯一键」一致，否则 PostgreSQL 会报
        # "ON CONFLICT DO UPDATE command cannot affect row a second time"。
        # 真实冲突形态（2026-11-23 实测 77868 行）：同一专业组内不同专业有不同选科
        # 要求（组 102 里既有「再选化学」也有「再选不限」），而平台 encoding 为空，
        # 去重键若用 raw、插入列若用 ''，二者不一致就撞唯一索引。
        # 修法：encoding 为空时用 raw 的 sha1 摘要占位（确定性、唯一、不丢信息）。
        rc = req_code
        if not rc:
            rc = ('T' + hashlib.sha1(raw.encode('utf-8')).hexdigest()[:10]) if raw else ''
        k = (prov, year, uk, '', rc)
        if k in subreq_by_unit:
            continue
        subreq_by_unit[k] = len(subreq_rows) + 1
        subreq_rows.append({
            'region_code': prov, 'year': year, 'school_code': sid, 'unit_key': uk,
            'major_code': '', 'first_subject': fs, 'recheck_rule': rr,
            'recheck_subjects': (json.dumps(subj_codes, ensure_ascii=False)
                                  if subj_codes else None),
            'requirement_code': rc,
            'raw_text': raw[:255], 'source': 'gaokao.cn/gkv3/plan/school',
            'data_version': str(year),
        })

    for x in items:
        cat = cat_of(x.get('local_type_name'))
        batch = batch_of(x.get('local_batch_name'))
        ptype = ptype_of(x.get('zslx_name'))
        detail = (x.get('zslx_name') or '').strip()
        group = group_of(x.get('sg_name'))
        major = (x.get('spcode') or '').strip()
        major = '' if major in ('-',) else major
        mname = (x.get('sp_name') or x.get('spname') or '').strip()[:200]
        raw_info = (x.get('info') or '').strip()
        note = raw_info[:500]
        prog = raw_info[:500]
        cnt = x.get('num')
        try:
            cnt = int(cnt) if cnt not in (None, '', '-') else None
        except Exception:
            cnt = None
        dur = {'四年': 4, '五年': 5, '三年': 3, '二年': 2, '六年': 6}.get(x.get('length'))
        try:
            tui = float(x.get('tuition')) if x.get('tuition') not in (None, '', '-') else None
        except Exception:
            tui = None
        sid = str(x.get('school_id') or '')
        mode = mode_of(prov, year)
        uk = unit_key(mode, prov, year, cat, batch, sid, group, major, ptype, detail)
        pk = plan_key(prov, year, cat, batch, sid, group, major, ptype, detail, prog, mname)
        if pk in seen_pk:
            continue
        seen_pk.add(pk)
        plans.append({
            'plan_key': pk, 'unit_key': uk, 'region_code': prov, 'year': year,
            'category': cat, 'batch': batch, 'school_code': sid,
            'school_name': (x.get('name') or '')[:120], 'group_code': group,
            'group_name': '', 'major_code': major, 'major_name': mname,
            'major_note': note, 'program_note': (prog or '').strip(),
            'catalog_id': None, 'plan_type': ptype, 'plan_type_detail': detail,
            'plan_count': cnt, 'duration': dur, 'tuition': tui, 'campus': '',
            'subject_req_id': None, 'status': 'ACTIVE',
            'source': 'gaokao.cn/gkv3/plan/school', 'published_at': None,
            'data_version': str(year),
        })
    return plans, subreq_rows


def cutoff_rows(items, prov, year):
    out = []
    for x in items:
        sid = str(x.get('school_id') or '')
        cat = cat_of(x.get('local_type_name'))
        batch = batch_of(x.get('local_batch_name'))
        ptype = ptype_of(x.get('zslx_name'))
        detail = (x.get('zslx_name') or '').strip()
        group = group_of(x.get('sg_name'))
        mode = mode_of(prov, year)
        uk = unit_key(mode, prov, year, cat, batch, sid, group, '', ptype, detail)

        def iv(v):
            return int(v) if isinstance(v, (int, float)) and v not in (-1, 9999) else None

        out.append({
            'unit_key': uk, 'region_code': prov, 'year': year, 'category': cat,
            'batch': batch, 'school_code': sid,
            'school_name': str(x.get('name') or '')[:120], 'group_code': group,
            'group_name': group, 'plan_type': ptype, 'plan_type_detail': detail,
            'min_score': iv(x.get('min')), 'min_rank': iv(x.get('min_section')),
            'control_score': iv(x.get('proscore')), 'score_diff': iv(x.get('diff')),
            'admit_count': None, 'filing_ratio': None,
            'subject_req_raw': str(x.get('zslx_name') or '')[:255],
            'source': 'gaokao.cn录取明细(真实)', 'data_version': str(year),
        })
    return out


# ---------------------------------------------------------------- 主流程
def process(kind, combo, workdir='/tmp/etl_work'):
    os.makedirs(workdir, exist_ok=True)
    prov, year = combo.split('-')
    year = int(year)
    out_path = os.path.join(workdir, '%s_%s.json' % (kind, combo))
    if not os.path.exists(out_path) or os.path.getsize(out_path) < 100:
        fetch_branch_file(kind, combo, out_path)
    doc = json.load(open(out_path, encoding='utf-8'))
    items = doc.get('items') or doc.get('item') or []
    print('[%s %s] 源 %d 行 num_found=%s count=%s' % (
        kind, combo, len(items), doc.get('num_found'), doc.get('count')), flush=True)

    if kind == 'plan':
        plans, subs = plan_rows(items, prov, year)
        # 诊断：源行数与去重后行数的差异（plan_key 去重会合并完全相同的行）
        if len(plans) != len(items):
            print('  [诊断] 源 %d 行 -> 去重后 T06 %d 行（合并 %d 条同键行）' % (
                len(items), len(plans), len(items) - len(plans)), flush=True)
        if subs:
            for i in range(0, len(subs), BATCH_SIZE):
                upsert('subject_requirement', subs[i:i + BATCH_SIZE],
                       conflict='region_code,year,unit_key,major_code,requirement_code')
            print('  T05 upsert %d' % len(subs), flush=True)
        # 回填 subject_req_id：按 unit_key 建立 id 映射（一个 unit_key 可能对应
        # 多条不同选科要求的 T05 行，取第一条作为该计划行的引用）。
        #
        # 关键：PostgREST 单次查询默认最多返回 1000 行，超过就静默截断 ——
        # 实测 44-2024 的 T05 有 7714 行，不分页则只有前 1000 行进映射，
        # 回填率从 100% 掉到 13.7%。这里显式分页拉全。
        id_by_unit = {}
        try:
            page = 0
            while True:
                q = ('subject_requirement?select=id,unit_key'
                     '&region_code=eq.%s&year=eq.%s'
                     '&order=id&limit=1000&offset=%d' % (prov, year, page * 1000))
                raw = _req('%s/%s' % (SB_URL, q))
                rows = json.loads(raw)
                if not rows:
                    break
                for r in rows:
                    id_by_unit.setdefault(r['unit_key'], r['id'])
                if len(rows) < 1000:
                    break
                page += 1
                if page > 50:
                    break
            print('  T05 id 映射 %d unit_key（%d 页）' % (len(id_by_unit), page + 1), flush=True)
        except Exception as e:
            print('  [warn] 回填查询失败:', str(e)[:140], flush=True)
        hit = 0
        for p in plans:
            sid_ = id_by_unit.get(p['unit_key'])
            if sid_:
                p['subject_req_id'] = sid_
                hit += 1
        if plans:
            for i in range(0, len(plans), BATCH_SIZE):
                upsert('enrollment_plan', plans[i:i + BATCH_SIZE], conflict='plan_key')
            print('  T06 upsert %d (回填 %d)' % (len(plans), hit), flush=True)
        return {'combo': combo, 'plan': len(plans), 'subreq': len(subs)}

    if kind == 'cutoff':
        rows = cutoff_rows(items, prov, year)
        for i in range(0, len(rows), BATCH_SIZE):
            upsert('admission_cutoff', rows[i:i + BATCH_SIZE],
                   conflict='unit_key,region_code,year,category,batch,school_code,group_code,plan_type,plan_type_detail')
        print('  T08 upsert %d' % len(rows), flush=True)
        return {'combo': combo, 'cutoff': len(rows)}

    return {'combo': combo}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--kind', required=True, choices=['plan', 'cutoff'])
    ap.add_argument('--combo', required=True)
    ap.add_argument('--workdir', default='/tmp/etl_work')
    a = ap.parse_args()
    t0 = time.time()
    try:
        r = process(a.kind, a.combo, a.workdir)
        print('=== 完成 %s 用时 %.0fs ===' % (json.dumps(r, ensure_ascii=False), time.time() - t0),
              flush=True)
    except Exception as e:
        print('=== 失败 %s: %s ===' % (a.combo, str(e)[:400]), flush=True)
        sys.exit(1)


if __name__ == '__main__':
    main()
