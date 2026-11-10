# -*- coding: utf-8 -*-
"""collect_special.py v1.1 本地端到端验证：mock 接口复现 code=1090 两条路径。

路径 A（探测期命中）：p44 片 size=20 一律返回 1090
    → probe_slice 应自动降到 size=10 拿到 numFound=1328
路径 B（采集中途命中）：p11 片 size=20 且 page==3 返回 1090
    → collect_pages 应整片降 size=10 重采，且条目不重不漏

校验：gate_pass=True / count==num_found==1663 / 两片 idx 全覆盖且唯一 /
      prov_sizes 正确 / oversize 计数>0
"""
import json, os, shutil, subprocess, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

P44_N, P11_N = 1328, 335
LONG = 'x' * 1600          # 长记录：20 条 → 32000 字符 > 接口 30000 上限
SHORT = 'y' * 60
CALLS = []                 # 记录所有请求
LOCK = threading.Lock()

SLICES = {'44': (P44_N, LONG), '11': (P11_N, SHORT)}


def make_items(pid, page, size):
    n, filler = SLICES[pid]
    out = []
    for i in range((page - 1) * size, min(page * size, n)):
        out.append({'school_id': pid, 'idx': i, 'spname': filler,
                    'prov': pid, 'tag': '%s-%d' % (pid, i)})
    return out


class H(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *a):
        pass

    def do_POST(self):
        n = int(self.headers.get('Content-Length') or 0)
        raw = self.rfile.read(n)
        try:
            q = json.loads(raw.decode())
        except Exception:
            q = {}
        pid = q.get('province_id')
        page = int(q.get('page') or 1)
        size = int(q.get('size') or 20)
        with LOCK:
            CALLS.append({'pid': pid, 'page': page, 'size': size,
                           'has_prov': 'province_id' in q})
        # BASE（无 province_id）：p44 + p11 合并
        if pid is None:
            items = make_items('44', page, size) + make_items('11', page, size)
            body = json.dumps({'code': '0000', 'data': {
                'numFound': P44_N + P11_N, 'item': items}}, ensure_ascii=False)
            # BASE 也按真实逻辑：size=20 时 p44 长记录让响应体超长 → 1090
            if size * (len(LONG) + 40) > 30000:
                body = json.dumps({'code': '1090',
                                   'message': '(api)数据长度超过30000个字符'})
            self._reply(body)
            return
        if pid not in SLICES:
            self._reply(json.dumps({'code': '0000',
                                    'data': {'numFound': 0, 'item': []}}))
            return
        n, filler = SLICES[pid]
        # 路径 A：p44 在 size=20 时一律 1090
        if pid == '44' and size == 20:
            self._reply(json.dumps({'code': '1090',
                                    'message': '(api)数据长度超过30000个字符'}))
            return
        # 路径 B：p11 在 size=20 且第 3 页 1090（模拟中途才超长）
        if pid == '11' and size == 20 and page == 3:
            self._reply(json.dumps({'code': '1090',
                                    'message': '(api)数据长度超过30000个字符'}))
            return
        self._reply(json.dumps({'code': '0000', 'data': {
            'numFound': n, 'item': make_items(pid, page, size)}},
            ensure_ascii=False))

    def _reply(self, body):
        b = body.encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Content-Length', str(len(b)))
        self.end_headers()
        self.wfile.write(b)


srv = ThreadingHTTPServer(('127.0.0.1', 0), H)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()
print('mock 接口已启动 :%d' % port)

WORK = '/tmp/sp_mock_run'
shutil.rmtree(WORK, ignore_errors=True)
os.makedirs(WORK + '/out')
HERE = os.path.dirname(os.path.abspath(__file__))
shutil.copy(os.path.join(HERE, 'collect_special.py'), WORK)
with open(WORK + '/proxies_cnb.txt', 'w') as f:
    f.write('http://127.0.0.1:%d\n' % port)

env = dict(os.environ)
env.update({'PROV': '44', 'YEAR': '2026', 'GITHUB_RUN_ID': 'local-test',
            'GK_API_BASE': 'http://api.zjzw.cn/web/api/'})
r = subprocess.run([sys.executable, 'collect_special.py'], cwd=WORK, env=env,
                   capture_output=True, text=True, timeout=900)
out = r.stdout
print('--- 采集器输出（尾部 40 行）---')
for l in out.splitlines()[-40:]:
    print('   ', l)
if r.returncode != 0:
    print('退出码', r.returncode, '\nSTDERR:', r.stderr[-1500:])

# ---------- 校验 ----------
fails = []
try:
    data = json.load(open(WORK + '/out/special_44_2026.json', encoding='utf-8'))
except Exception as e:
    print('结果文件读取失败:', e)
    print('STDOUT 全文:\n', out[-3000:])
    sys.exit(1)

print('\n=== 校验 ===')
def chk(name, cond, detail=''):
    print(('  PASS  ' if cond else '  FAIL  ') + name + ('  ' + detail if detail else ''))
    if not cond:
        fails.append(name)

chk('gate_pass=True', data.get('gate_pass') is True, str(data.get('gate')))
chk('count == num_found == 1663',
    data.get('count') == 1663 and data.get('num_found') == 1663,
    'count=%s num_found=%s' % (data.get('count'), data.get('num_found')))
chk('partition_count == 1663', data.get('partition_count') == 1663,
    str(data.get('partition_count')))

items = data.get('items', [])
idx44 = sorted(int(it['idx']) for it in items if it['prov'] == '44')
idx11 = sorted(int(it['idx']) for it in items if it['prov'] == '11')
chk('p44 条目 0..1327 全覆盖且唯一',
    idx44 == list(range(P44_N)), '实际 %d 条' % len(idx44))
chk('p11 条目 0..334 全覆盖且唯一',
    idx11 == list(range(P11_N)), '实际 %d 条' % len(idx11))
chk('总条目数 = 1663（不重不漏）', len(items) == 1663, str(len(items)))

ps = data.get('partition', {}).get('prov_sizes', {})
chk('prov_sizes: p44=10（路径A 降级）, p11=20', ps.get('44') == 10 and ps.get('11') == 20,
    str({k: ps.get(k) for k in ('44', '11')}))
chk('stats.oversize > 0（确实撞到 1090）',
    data.get('stats', {}).get('oversize', 0) > 0,
    'oversize=%s' % data.get('stats', {}).get('oversize'))

# 路径 B 证据：p11 应先以 size=20 采到第 3 页失败，再整片以 size=10 重采
p11_sz20 = [c for c in CALLS if c['pid'] == '11' and c['size'] == 20]
p11_sz10 = [c for c in CALLS if c['pid'] == '11' and c['size'] == 10]
chk('路径B：p11 先用 size=20 采过（含 page=3）',
    any(c['page'] == 3 for c in p11_sz20), 'size20 请求 %d 次' % len(p11_sz20))
chk('路径B：p11 随后整片以 size=10 重采',
    len(p11_sz10) >= 30, 'size10 请求 %d 次' % len(p11_sz10))
chk('路径B：p11 的 size=10 页码覆盖 1..34',
    sorted(set(c['page'] for c in p11_sz10)) == list(range(1, 35)),
    str(sorted(set(c['page'] for c in p11_sz10))[:5]))
# 路径 A 证据：p44 从未以 size=20 成功取到数据
p44_sz20_ok = [c for c in CALLS if c['pid'] == '44' and c['size'] == 20]
chk('路径A：p44 的 size=20 请求全部返回 1090（无 item）',
    len(p44_sz20_ok) >= 1, 'size20 请求 %d 次（全部1090）' % len(p44_sz20_ok))

print('\n结论：%s' % ('全部通过 ✅' if not fails else '失败 %d 项 ❌ %s' % (len(fails), fails)))
sys.exit(1 if fails else 0)
