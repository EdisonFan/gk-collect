"""把 out/special_<省>_<年>.json 推到 GitHub special-<省>-<年> 分支。

用 Git Data API 建 blob（避免大文件 git push 被网络重置）；分支不存在时
基于 main 创建。仅当结果 gate_pass=True 才允许推送。
用法: python3 push_one.py <prov> <year>
"""
import os, json, sys, base64, time, urllib.request, urllib.error

PROV, YEAR = sys.argv[1], sys.argv[2]
BR = 'special-%s-%s' % (PROV, YEAR)
F = 'out/special_%s_%s.json' % (PROV, YEAR)
R = 'EdisonFan/gk-collect'
TOKEN = os.popen("git config --get remote.origin.url").read().strip()
TOKEN = TOKEN.split('x-access-token:')[1].split('@')[0]
H = {'Authorization':'Bearer '+TOKEN, 'User-Agent':'x',
     'Accept':'application/vnd.github+json', 'Content-Type':'application/json'}


def call(m, p, b=None, tmo=240):
    data = json.dumps(b).encode() if b is not None else None
    req = urllib.request.Request('https://api.github.com'+p, data=data,
                                 headers=H, method=m)
    last = None
    for i in range(4):
        try:
            return json.load(urllib.request.urlopen(req, timeout=tmo))
        except urllib.error.HTTPError as e:
            if e.code == 404: raise
            last = e; time.sleep(3)
        except Exception as e:
            last = e; time.sleep(3)
    raise last


def main():
    d = json.load(open(F, encoding='utf-8'))
    assert d.get('gate_pass') is True, 'gate_pass 非 True，拒绝推送'
    raw = open(F,'rb').read()
    cnt = d['count']
    print('[%s] %s gate_pass=True count=%d %d 字节，开始推送'
          % (time.strftime('%H:%M:%S'), BR, cnt, len(raw)), flush=True)

    main_head = call('GET', '/repos/%s/git/ref/heads/main' % R)['object']['sha']
    main_tree = call('GET', '/repos/%s/git/commits/%s' % (R, main_head))['tree']['sha']
    try:
        head = call('GET', '/repos/%s/git/ref/heads/%s' % (R, BR))['object']['sha']
        tree0 = call('GET', '/repos/%s/git/commits/%s' % (R, head))['tree']['sha']
        parents = [head]; exists = True
    except urllib.error.HTTPError as e:
        if e.code != 404: raise
        head = main_head; tree0 = main_tree; parents = [main_head]; exists = False

    blob = call('POST', '/repos/%s/git/blobs' % R,
                {'content':base64.b64encode(raw).decode(),'encoding':'base64'})['sha']
    tree = call('POST', '/repos/%s/git/trees' % R,
                {'base_tree':tree0,
                 'tree':[{'path':F,'mode':'100644','type':'blob','sha':blob}]})['sha']
    msg = ('special %s-%s gate_pass=True（本地通用采集，%s 条）\n\n'
           '复用同一接口与字段口径，判据独立可自证：\n'
           '- %s\n'
           '- 逐片翻到底并校验「采出条数 == 该片 numFound」，零失败页；\n'
           '- 完备自证：%s\n'
           '- 全局 %s 条、%s 唯一 id、接口内重复 %s。\n'
           '四闸门 G1/G2/G3/G4 全过，gate_pass=True。'
           % (PROV, YEAR, format(cnt,','),
              d['partition']['method'],
              ('双划分 Σ 相等且 id 集合完全相等'
               if d['partition'].get('dual_plans') else 'BASE 真值逐页守恒'),
              format(cnt,','), format(d['count']-d['dup_in_api'],','),
              d['dup_in_api']))
    cm = call('POST', '/repos/%s/git/commits' % R,
              {'tree':tree,'parents':parents,'message':msg})['sha']
    if exists:
        call('PATCH', '/repos/%s/git/refs/heads/%s' % (R, BR), {'sha':cm,'force':False})
    else:
        call('POST', '/repos/%s/git/refs' % R, {'ref':'refs/heads/'+BR,'sha':cm})
    now = call('GET', '/repos/%s/git/ref/heads/%s' % (R, BR))['object']['sha']
    assert now == cm, 'ref 更新后不一致'
    print('[%s] === 已推送 %s -> %s（%s）==='
          % (time.strftime('%H:%M:%S'), BR, cm[:7],
             '更新' if exists else '新建分支'), flush=True)


if __name__ == '__main__': main()
