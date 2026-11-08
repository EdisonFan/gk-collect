# -*- coding: utf-8 -*-
"""把 GitHub 各 cutoff-{省}-{年} 分支的采集结果拉回本地持久化目录。

v4：本地 git 对象库优先 + 差集精确 refspec 增量补齐。

背景（踩过的坑，均为实测数据）：
  - v1 全量通配符 fetch '+refs/heads/cutoff-*:...'：让服务器协商全部
    141 个 ref，实测 100~133 秒甚至连接超时，撞巡航 300s subprocess
    超时，导致 for-each-ref 读到旧引用、漏回收。
  - v2 GitHub Contents API / raw 直链：大文件(1.2MB)单次要 40~85 秒；
    DNS 返回 4 个 IPv6，curl 挑到不通的 v6 会卡死，强制 -4 后 256KB
    只要 2.6s、完整 1.2MB 仍需 48s（跨境带宽 ~25KB/s）。
  - 换 IP / 固定解析实测：绑定 185.199.x 只有 23~34 KB/s，反而比系统
    IPv4 自动选择更慢；DNS 解析本身 0.1~0.25s，从来不是瓶颈。
  - 强制 HTTP/1.1 无效（曾因管道取错退出码误判，严格复测已推翻）。
  - 真因与解法：精确 refspec 只列新增分支，POST 极小，实测 3 个分支
    1.0 秒完成；全热（无新增）时连 fetch 都不用发。
  - git show <ref>:<path> 是纯本地读取，毫秒级、零网络。

流程：
  ① scan() 本地引用全部落盘（秒级，必成功）
  ② ls-remote 拿远端分支列表（GET，约 1s）→ 与本地 refs 求差集
  ③ 只对差集构造精确 refspec fetch（新增几个就拉几个）
  ④ 再 scan() 落盘；仅当新结果条数 >= 旧结果才覆盖

数据安全性：结果在云端分支 + 制品双份存在，本地落盘是第三份。
"""
import os, sys, json, subprocess, re, urllib.request

REPO_SLUG = 'EdisonFan/gk-collect'
REPO_DIR = '/workspace/gh-gk-collect'
DEST = '/workspace/高考填报数据模型与流程/采集原始数据/gk_cutoff'
REMOTE = 'https://github.com/%s.git' % REPO_SLUG
REFS = 'refs/remotes/gh/cutoff-*'


def git(*args, **kw):
    return subprocess.run(['git'] + list(args), cwd=REPO_DIR,
                          capture_output=True, **kw)


def scan(verbose=True):
    """纯本地：遍历跟踪引用，git show 读出结果文件并落盘。"""
    p = git('for-each-ref', '--format=%(refname)', REFS, text=True)
    refs = [x.strip() for x in p.stdout.splitlines() if x.strip()]
    total, replaced, skipped, failed = 0, 0, 0, []
    for ref in refs:
        tag = ref.split('cutoff-')[-1]
        try:
            prov, year = tag.split('-', 1)
        except ValueError:
            continue
        path = 'out/cutoff_%s_%s.json' % (prov, year)
        q = git('show', '%s:%s' % (ref, path))
        if q.returncode != 0 or not q.stdout:
            failed.append(tag)
            continue
        try:
            d = json.loads(q.stdout.decode('utf-8'))
        except Exception:
            failed.append(tag)
            continue
        fn = os.path.join(DEST, 'cutoff_%s_%s.json' % (prov, year))
        n = d.get('count', 0)
        # v5：已达标(gate_pass)的本地结果，绝不被后续「闸门未过」的重跑结果覆盖
        new_ok = (d.get('gate_pass') is True
                  and d.get('count') == d.get('num_found')
                  and d.get('num_found', 0) > 0)
        if os.path.exists(fn):
            try:
                od = json.load(open(fn, encoding='utf-8'))
                old = od.get('count', 0)
                old_ok = (od.get('gate_pass') is True
                          and od.get('count') == od.get('num_found')
                          and od.get('num_found', 0) > 0)
                if old_ok and not new_ok:
                    skipped += 1
                    continue              # 旧结果已零丢失达标，保留
                if not new_ok and n < old:
                    skipped += 1
                    continue              # 双方都未达标时，旧数据更全则保留
                # 新结果已达标(new_ok) 时无条件覆盖，不再受 count 大小干扰
            except Exception:
                pass
        with open(fn, 'wb') as f:
            f.write(q.stdout)            # 原样落盘，不重新序列化
        replaced += 1
        total += n
    return len(refs), replaced, total, skipped, failed


def remote_shas():
    """用 GitHub API 拿全部 cutoff-* 分支的 sha（实测约 2s）。

    不用 `git ls-remote --heads ... 'refs/heads/cutoff-*'`：通配符会让服务端
    协商 168 个 ref，跨境实测 120s 超时；而 API matching-refs 一次返回全部，
    稳定 2s。这是「达标数卡住不涨」的直接原因之一。
    """
    tok = open('/workspace/.secrets/github_pat', encoding='utf-8').read().strip()
    H = {'Authorization': 'Bearer ' + tok,
         'Accept': 'application/vnd.github+json'}
    url = ('https://api.github.com/repos/%s/git/matching-refs/heads/cutoff-'
           '?per_page=100' % REPO_SLUG)
    out = {}
    while url:
        req = urllib.request.Request(url, headers=H)
        r = urllib.request.urlopen(req, timeout=30)
        for x in json.loads(r.read().decode()):
            out[x['ref']] = x['object']['sha']
        m = re.search(r'<([^>]+)>;\s*rel="next"', r.headers.get('Link', ''))
        url = m.group(1) if m else None
    return out


def fetch_missing(chunk=5):
    """拉「远端新增」或「同名分支 sha 变了」的 cutoff 分支。

    返回 (需更新数, 远端总数)；新增数为负表示 API / fetch 失败。
    绝不能用 '+refs/heads/cutoff-*:...' 通配符 refspec —— 那是 100s+ 的根源。

    关键：workflow 用 git push -f 覆盖同名结果分支，重跑低覆盖组合时
    分支名不变、sha 会变。只按名字求差会漏掉全部重跑成果（曾导致
    run #5/#6 的数据滞留在云端、达标数卡住不涨）。
    """
    try:
        remote = remote_shas()
    except Exception as e:
        print('  [远端 sha] API 失败: %s %s' % (type(e).__name__, e), flush=True)
        return -1, 0
    have = git('for-each-ref', '--format=%(refname) %(objectname)', REFS, text=True)
    local = {}
    for l in have.stdout.splitlines():
        parts = l.strip().split()
        if len(parts) == 2:
            local['refs/heads/' + parts[0].split('refs/remotes/gh/')[-1]] = parts[1]
    miss = [r for r, s in remote.items() if local.get(r) != s]
    if not miss:
        return 0, len(remote)
    # 逐批独立容错：某批跨境超时只丢该批，不放弃其余分支；
    # 下一轮 sha 差集会自动补齐失败的批。
    got, fail_batches = 0, 0
    for i in range(0, len(miss), chunk):
        part = miss[i:i + chunk]
        rs = ['+%s:refs/remotes/gh/%s' % (r, r.split('refs/heads/')[-1])
              for r in part]
        try:
            q = git('fetch', '-q', REMOTE, *rs, timeout=120)
            ok_batch = (q.returncode == 0)
        except subprocess.TimeoutExpired:
            ok_batch = False       # 单批超时不拖垮整轮，下轮 sha 差集自动补齐
        if ok_batch:
            got += len(part)
        else:
            fail_batches += 1
            print('  [增量 fetch] 第 %d 批(%d 个)失败，跳过待下轮补齐' %
                  (i // chunk + 1, len(part)), flush=True)
    print('  [增量 fetch] 成功 %d / 需更新 %d，失败批次 %d' %
          (got, len(miss), fail_batches), flush=True)
    return got, len(remote)


def main():
    os.makedirs(DEST, exist_ok=True)
    if '--list' in sys.argv:
        fetch_missing()
        p = git('for-each-ref', '--format=%(refname)', REFS, text=True)
        for x in p.stdout.splitlines():
            print(' ', x.strip())
        return

    if '--local-only' in sys.argv:
        nrefs, r1, t1, s1, f1 = scan()
        print('[本地快速读取] %d 个引用 -> 写入 %d 文件 / %d 条，跳过 %d' %
              (nrefs, r1, t1, s1), flush=True)
        return

    # ① 先把本地已有的全部落盘 —— 不依赖网络，永远最快且必成功
    nrefs, r1, t1, s1, f1 = scan()
    print('[本地读取] %d 个引用 -> 写入 %d 文件 / %d 条，跳过 %d' %
          (nrefs, r1, t1, s1), flush=True)

    # ② 差集增量补齐：只拉新增分支（全热 0 网络等待，有新增约 1s）
    added, nremote = fetch_missing()
    if added < 0:
        print('[增量 fetch] 失败（不影响 ① 的本地结果）', flush=True)
    else:
        print('[增量 fetch] 远端 %d 分支 / 需更新 %d 个（新增或 sha 变化）' %
              (nremote, added), flush=True)

    # ③ 仅在真的拉回了内容时再读一次（全热可省一遍本地 git show）
    if added > 0:
        nrefs2, r2, t2, s2, f2 = scan()
        print('[本地读取] %d 个引用 -> 写入 %d 文件 / %d 条，跳过 %d' %
              (nrefs2, r2, t2, s2), flush=True)
    else:
        nrefs2, r2, t2, s2, f2 = nrefs, 0, 0, 0, f1
        print('[本地读取] 无新增分支，跳过二次读取', flush=True)

    # 两遍 scan 都是全量读取，r1+r2 / t1+t2 相加会把总量显示成双倍
    n_final, files_written, records_total = (
        (nrefs2, r2, t2) if added > 0 else (nrefs, r1, t1))
    print('-' * 78)
    print('合计本地引用 %d 个 / 本轮落盘 %d 文件 / 当前总量 %d 条记录 / 读取失败 %d' %
          (n_final, files_written, records_total, len(f2)), flush=True)
    if f2:
        print('读取失败(可能分支刚推送): %s' % ' '.join(f2[:10]), flush=True)
    print('落盘目录: %s' % DEST, flush=True)


if __name__ == '__main__':
    main()
