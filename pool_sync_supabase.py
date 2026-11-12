"""把当前 proxies_cnb.txt 池 upsert 进 Supabase public.proxy_pool。

每 2 小时由 pool-keeper 调度。新出口插入、失效复活、已被剔除的旧出口标 dead；
在租约中（leased）的行不受影响，第三仓正在用的出口不会被打断。
"""
import json
import os
import urllib.request

BASE = os.environ.get('SUPABASE_URL', 'https://ripobzmwgmdxyixoevee.supabase.co').rstrip('/')
KEY = os.environ.get('SUPABASE_ANON_KEY', '')

if not KEY:
    print('SUPABASE_ANON_KEY 未配置，跳过同步')
    raise SystemExit(0)

endpoints = [l.strip() for l in open('proxies_cnb.txt', encoding='utf-8')
             if l.strip() and not l.startswith('#')]
if not endpoints:
    print('池文件为空，跳过同步')
    raise SystemExit(0)

req = urllib.request.Request(
    BASE + '/rest/v1/rpc/sync_proxy_pool',
    data=json.dumps({'p_endpoints': endpoints}).encode(),
    headers={'apikey': KEY, 'Authorization': 'Bearer ' + KEY,
             'Content-Type': 'application/json'},
    method='POST')
with urllib.request.urlopen(req, timeout=60) as r:
    affected = r.read().decode().strip()
print('sync_proxy_pool 影响 %s 行（本地池 %d 条）' % (affected, len(endpoints)))
