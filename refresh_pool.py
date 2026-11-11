"""快筛代理出口 -> /tmp/live.txt（requests 双超时，全并发）。"""
import os, json, time, requests
from concurrent.futures import ThreadPoolExecutor

U = 'https://api.zjzw.cn/web/api/'
H = {'User-Agent':'Mozilla/5.0','Origin':'https://gkcx.eol.cn',
     'Referer':'https://gkcx.eol.cn/','Accept':'application/json,text/plain,*/*',
     'Content-Type':'application/json'}
q = {'uri':'apidata/api/gk/score/special','local_province_id':'44',
     'year':2023,'page':3,'size':20}

allpx = []
for f in ('proxies_cnb.txt','/tmp/live.txt','/tmp/pool2.txt','/tmp/stable.txt',
          '/tmp/alive.txt','/tmp/pool_fast.txt'):
    try: allpx += [x.strip() for x in open(f) if x.strip()]
    except Exception: pass
seen = []
for p in allpx:
    if p not in seen: seen.append(p)

def t(px):
    try:
        r = requests.post(U, json=q, headers=H,
                          proxies={'http':px,'https':px}, timeout=(2.5,5))
        d = r.json()
        if str(d.get('code')) == '0000' and isinstance(d.get('data'), dict):
            return px
    except Exception: pass
    return None

with ThreadPoolExecutor(24) as ex:
    alive = [x for x in ex.map(t, seen) if x]
open('/tmp/live.txt','w').write('\n'.join(alive))
print('[%s] 出口刷新 候选%d 活%d'
      % (time.strftime('%H:%M:%S'), len(seen), len(alive)), flush=True)
