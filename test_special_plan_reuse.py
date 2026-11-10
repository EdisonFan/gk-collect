# -*- coding: utf-8 -*-
"""验证 v1.6 探测计划复用：绿灯复用 + 篡改回退 + 闸门不降级。"""
import json, os, shutil, subprocess, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

P44_N, P11_N = 1328, 335
LONG, SHORT = 'x' * 1600, 'y' * 60
SLICES = {'44': (P44_N, LONG), '11': (P11_N, SHORT)}
LOCK = threading.Lock()
PROBE_CALLS = []          # 记录带 province_id 的探测型请求（page==1）

def make_items(pid, page, size):
    n, filler = SLICES[pid]
    return [{'school_id': pid, 'idx': i, 'spname': filler, 'prov': pid,
             'tag': '%s-%d' % (pid, i)}
            for i in range((page - 1) * size, min(page * size, n))]

class H(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'
    def log_message(self, *a): pass
    def do_POST(self):
        raw = self.rfile.read(int(self.headers.get('Content-Length') or 0))
        try: q = json.loads(raw.decode())
        except Exception: q = {}
        pid = q.get('province_id'); page = int(q.get('page') or 1)
        size = int(q.get('size') or 20)
        with LOCK:
            if pid is not None and page == 1: PROBE_CALLS.append(pid)
        if pid is None:
            items = make_items('44', page, size) + make_items('11', page, size)
            body = json.dumps({'code': '0000', 'data': {'numFound': P44_N + P11_N, 'item': items}}, ensure_ascii=False)
            if size * (len(LONG) + 40) > 30000:
                body = json.dumps({'code': '1090', 'message': '(api)数据长度超过30000个字符'})
            return self._reply(body)
        if pid not in SLICES:
            return self._reply(json.dumps({'code':'0000','data':{'numFound':0,'item':[]}}))
        n, filler = SLICES[pid]
        if pid == '44' and size == 20:
            return self._reply(json.dumps({'code':'1090','message':'超长'}))
        if pid == '11' and size == 20 and page == 3:
            return self._reply(json.dumps({'code':'1090','message':'超长'}))
        self._reply(json.dumps({'code':'0000','data':{
            'numFound': n, 'item': make_items(pid, page, size)}}, ensure_ascii=False))
    def _reply(self, body):
        b = body.encode(); self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(b))); self.end_headers()
        self.wfile.write(b)

srv = ThreadingHTTPServer(('127.0.0.1', 0), H)
port = srv.server_address[1]
threading.Thread(target=srv.serve_forever, daemon=True).start()

WORK = '/tmp/sp_plan_reuse'
shutil.rmtree(WORK, ignore_errors=True); os.makedirs(WORK + '/out')
HERE = os.getcwd()
shutil.copy(HERE + '/collect_special.py', WORK)
open(WORK + '/proxies_cnb.txt','w').write('http://127.0.0.1:%d\n' % port)
env = dict(os.environ, PROV='44', YEAR='2026', GITHUB_RUN_ID='plan-test',
           GK_API_BASE='http://api.zjzw.cn/web/api/')

def run(tag):
    r = subprocess.run([sys.executable,'collect_special.py'], cwd=WORK, env=env,
                       capture_output=True, text=True, timeout=900)
    out = r.stdout
    print('--- %s: 关键日志 ---' % tag)
    for l in out.splitlines():
        if any(k in l for k in ('[计划','[G2]','[G4]','闸门','样本采集跳过','校验未通过')):
            print('   ', l[:170])
    try: return json.load(open(WORK + '/out/special_44_2026.json', encoding='utf-8')), out
    except Exception as e:
        print('   结果读取失败', e, '\n', out[-1500:]); return None, out

fails = []
def chk(name, cond, detail=''):
    print(('  PASS  ' if cond else '  FAIL  ') + name + ('  ' + detail if detail else ''))
    if not cond: fails.append(name)

# 第 1 轮：无计划 -> 正常探测并落盘
d1, _ = run('第1轮 正常探测')
chk('第1轮达标', d1 and d1.get('gate_pass') is True)
chk('第1轮生成计划文件', os.path.exists(WORK + '/out/partition_plan.json'))
plan = json.load(open(WORK + '/out/partition_plan.json', encoding='utf-8'))
chk('计划含 2 个叶子且真值守恒',
    len(plan['leaves']) == 2 and sum(x[1] for x in plan['leaves']) == plan['true_nf'] == 1663)

# 第 2 轮：模拟 workflow 从分支取回计划
shutil.copy(WORK + '/out/partition_plan.json', WORK + '/out/partition_plan_prev.json')
PROBE_CALLS.clear()
d2, o2 = run('第2轮 复用计划')
chk('第2轮达标', d2 and d2.get('gate_pass') is True)
chk('第2轮条数仍 1663', d2 and d2.get('count') == 1663)
chk('第2轮日志出现计划复用', '校验通过，跳过完备切分探测' in o2)
chk('第2轮跳过样本采集', '样本采集跳过' in o2)
chk('第2轮版本标识 v1.6', d2 and d2.get('collector_rev') == 'special-v1.6-plan-reuse')
chk('第2轮跳过 34 省完备切分探测', '[探测 省' not in o2,
    '省片探测日志=%s' % ('有' if '[探测 省' in o2 else '无'))
chk('第2轮四闸门全绿', d2 and all(d2.get('gate', {}).values()))
chk('第2轮 audit 标记 plan_reused', d2 and (d2.get('partition') or {}).get('plan_reused') is True)

# 第 3 轮：篡改计划（BASE 不一致）-> 必须回退正常探测，且仍达标
bad = json.load(open(WORK + '/out/partition_plan_prev.json', encoding='utf-8'))
bad['base_num_found'] = 999
json.dump(bad, open(WORK + '/out/partition_plan_prev.json','w',encoding='utf-8'), ensure_ascii=False)
d3, o3 = run('第3轮 篡改计划')
chk('第3轮拒绝篡改计划', '校验未通过' in o3)
chk('第3轮回退后仍达标', d3 and d3.get('gate_pass') is True)
chk('第3轮未标记 v1.6', d3 and d3.get('collector_rev') != 'special-v1.6-plan-reuse')

# 第 4 轮：Σ 不守恒的计划 -> 也不得放行
bad2 = json.load(open(WORK + '/out/partition_plan.json', encoding='utf-8'))
bad2['leaves'][0][1] += 5
json.dump(bad2, open(WORK + '/out/partition_plan_prev.json','w',encoding='utf-8'), ensure_ascii=False)
d4, o4 = run('第4轮 Σ不守恒计划')
chk('第4轮拒绝 Σ 不守恒', '校验未通过' in o4)
chk('第4轮回退后仍达标', d4 and d4.get('gate_pass') is True)

print('\n结论：%s' % ('全部通过' if not fails else '失败 %d 项 %s' % (len(fails), fails)))
sys.exit(1 if fails else 0)
