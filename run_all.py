"""批量驱动：把 186 组合里未达标的全部本地采完并推送到 GitHub。

断点：out/done_all.json（已 gate_pass=True 且推送成功）、
      out/failed_all.json（本轮失败，可下轮重试）。
每 8 个组合刷新一次代理出口池；推送成功后删除该组合 parts 断点目录省空间。
进程被上层环境释放后，直接重跑本脚本即可续采。

用法: python3 -u run_all.py [--limit N] [--only 13-2023,44-2025]
"""
import os, sys, json, time, subprocess, shutil, urllib.request
from concurrent.futures import ThreadPoolExecutor
import page_ckpt

ROOT = os.path.dirname(os.path.abspath(__file__))
os.chdir(ROOT)
R = 'EdisonFan/gk-collect'
TOKEN = os.popen("git config --get remote.origin.url").read().strip()
TOKEN = TOKEN.split('x-access-token:')[1].split('@')[0]
H = {'Authorization':'Bearer '+TOKEN,'User-Agent':'x',
     'Accept':'application/vnd.github+json'}

DONE_F, FAIL_F = 'out/done_all.json', 'out/failed_all.json'
CLOUD_F = 'out/cloud_dispatched.json'   # 已交给 GitHub Actions 的组合，本地不再重复采
DEFER_F = 'out/deferred.json'           # 划分口径未解、暂不参与本轮的组合
done = json.load(open(DONE_F)) if os.path.exists(DONE_F) else []
failed = json.load(open(FAIL_F)) if os.path.exists(FAIL_F) else []
cloud = json.load(open(CLOUD_F)) if os.path.exists(CLOUD_F) else []
deferred = json.load(open(DEFER_F)) if os.path.exists(DEFER_F) else []


def save():
    json.dump(sorted(set(done)), open(DONE_F,'w'), ensure_ascii=False, indent=1)
    json.dump(sorted(set(failed)), open(FAIL_F,'w'), ensure_ascii=False, indent=1)


def log(*a): print('[%s]' % time.strftime('%H:%M:%S'), *a, flush=True)


def remote_ok():
    """拉取远端已达标分支，合并进 done（避免重复采）。"""
    def api(p):
        return json.load(urllib.request.urlopen(
            urllib.request.Request('https://api.github.com'+p, headers=H), timeout=40))
    bs, pg = [], 1
    while True:
        d = api('/repos/%s/branches?per_page=100&page=%d' % (R, pg))
        bs += [x['name'] for x in d]
        if len(d) < 100 or pg > 8: break
        pg += 1
    bs = [b for b in bs if b.startswith('special-')]
    def chk(b):
        try:
            m = api('/repos/%s/commits/%s' % (R, b))['commit']['message'].split('\n')[0]
            return b.replace('special-','') if 'gate_pass=True' in m else None
        except Exception: return None
    with ThreadPoolExecutor(10) as ex:
        ok = [x for x in ex.map(chk, bs) if x]
    return set(ok)


def main():
    global done, failed
    args = sys.argv[1:]
    limit = None; only = None
    if '--limit' in args: limit = int(args[args.index('--limit')+1])
    if '--only' in args: only = set(args[args.index('--only')+1].split(','))

    prof = json.load(open('scale_profile.json'))['combos']
    allc = sorted(prof.keys())
    log('=== 批量采集启动 全量%d 组合 ===' % len(allc))

    try:
        rok = remote_ok()
        log('远端已达标 %d，合并进断点' % len(rok))
        done.extend(rok); save()
    except Exception as e:
        log('远端达标清单拉取失败 %s，仅用本地断点' % str(e)[:60])

    order = {'light':0,'medium':1,'heavy':2}
    # 云端已派发的组合本轮跳过：等它们推回 gate_pass=True 后，下轮 remote_ok()
    # 会自动并入 done；若云端失败，从 CLOUD_F 删掉即可回归本地队列。
    cloud_set, defer_set = set(cloud), set(deferred)
    if cloud_set:
        log('云端已派发 %d 个，本地本轮跳过：%s' % (len(cloud_set), sorted(cloud_set)))
    if defer_set:
        log('划分口径未解、暂缓 %d 个：%s' % (len(defer_set), sorted(defer_set)))
    pending = [c for c in allc if c not in set(done)
               and c not in cloud_set and c not in defer_set]
    if only: pending = [c for c in pending if c in only]
    # 上一轮失败项优先重采：它们多半已留片级断点，且含「代码缺口待验证」
    # 的组合（如 37 省需第三维切分），排最前能让修复尽快被真实数据检验。
    failed_set = set(failed)
    pending.sort(key=lambda c:(0 if c in failed_set else 1,
                               order.get(prof[c].get('tier'),3),
                               prof[c].get('est_minutes',0), c))
    if limit: pending = pending[:limit]
    log('本轮待采 %d 组合，按 轻→中→重 排序，前10: %s' % (len(pending), pending[:10]))

    t_all = time.time(); n_ok = 0; n_fail = 0
    for i, c in enumerate(pending, 1):
        # 云端续派守护可能在此期间派了新组合：每个组合开始前重读台账，
        # 已交给云端的就让路，避免两边重复采同一个。台账只在启动时读一次不够。
        if os.path.exists(CLOUD_F):
            try:
                if c in json.load(open(CLOUD_F)):
                    log('--- [%d/%d] %s 云端已接手，本地跳过 ---' % (i, len(pending), c))
                    continue
            except Exception:
                pass
        p, y = c.split('-')
        F = 'out/special_%s_%s.json' % (p, y)
        log('--- [%d/%d] %s 开始 ---' % (i, len(pending), c))
        if (i-1) % 2 == 0:
            subprocess.run([sys.executable, 'refresh_pool.py'], timeout=150)
        # 若已有合格结果文件（上轮生成但未推送），跳过采集直接推
        need_collect = True
        if os.path.exists(F):
            try:
                if json.load(open(F,encoding='utf-8')).get('gate_pass') is True:
                    need_collect = False
            except Exception: pass
        if need_collect:
            # 超时按预估页量给，不再固定 3000s：1800 页级重档实测需要
            # 3600~4700s，固定值会在采到 1600/1838 页时误杀且页结果全丢。
            tmo = page_ckpt.timeout_for(prof.get(c, {}).get('est_pages', 0))
            try:
                r = subprocess.run([sys.executable,'-u','local_any.py',p,y],
                                   stdout=open('out/run_%s_%s.log'%(p,y),'w'),
                                   stderr=subprocess.STDOUT, timeout=tmo)
                if r.returncode != 0: log('  采集脚本退出码 %d' % r.returncode)
            except subprocess.TimeoutExpired:
                # 页级断点已增量落盘，下次重跑只补缺页，不再从头白跑。
                log('  采集超时 %ds，标记失败（页级断点已保留）' % tmo)
                failed.append(c); save(); n_fail += 1; continue
        gp = False
        try: gp = json.load(open(F,encoding='utf-8')).get('gate_pass') is True
        except Exception as e: log('  结果文件异常 %s' % str(e)[:60])
        if not gp:
            log('  gate_pass 非 True，标记失败（parts 保留供重试）')
            failed.append(c); save(); n_fail += 1; continue
        cnt = json.load(open(F,encoding='utf-8'))['count']
        try:
            r = subprocess.run([sys.executable,'-u','push_one.py',p,y],
                               capture_output=True, text=True, timeout=600)
            print(r.stdout[-600:], flush=True)
            if r.returncode != 0:
                log('  推送失败 rc=%d %s' % (r.returncode, r.stderr[-300:]))
                failed.append(c); save(); n_fail += 1; continue
        except Exception as e:
            log('  推送异常 %s' % str(e)[:80]); failed.append(c); save(); n_fail += 1; continue
        done.append(c); failed = [x for x in failed if x != c]; save()
        n_ok += 1
        shutil.rmtree('out/parts_%s_%s'%(p,y), ignore_errors=True)
        log('  ✅ %s 已推送 count=%d | 累计成功%d 失败%d | 总耗时%.1f分'
            % (c, cnt, n_ok, n_fail, (time.time()-t_all)/60))
    log('=== 本轮结束 成功%d 失败%d 耗时%.1f分 | 全局达标 %d/%d ==='
        % (n_ok, n_fail, (time.time()-t_all)/60, len(set(done)), len(allc)))

if __name__ == '__main__': main()
