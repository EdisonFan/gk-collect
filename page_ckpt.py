"""页级增量断点：把已完成的页结果落盘，进程被杀后重跑无损续采。

背景：local_any.py 原先只在「所有页任务跑完」后才逐片 persist()，页结果
全在内存 bucket 里。固定 3000s 超时或上层环境释放进程时，1600+ 页全部
白跑，parts 目录为空，下轮「断点命中 0」从头再来，形成死循环
（41-2024 / 52-2025 / 32-2025 均如此）。

本模块提供与采集口径解耦的四件事，供 local_any.py 与 run_all.py 共用：
  save/load  bucket <-> 原子落盘文件（含 it=None、code=1090 等边界）
  pending    按 (label, page) 精确过滤已完成页
  timeout_for 按预估页量给单组合超时，替代固定 3000s
"""
import json
import os

V = 1


def save(path, bucket, keep=None):
    """bucket: {label: {page: (it, c)}} -> 原子落盘，返回写入页数。

    keep 为需保留的 label 集合；None 表示全保留。用于剔除已判失败的片，
    避免重跑命中旧页后 persist 反复判失败。
    """
    pages = {}
    for l, ps in bucket.items():
        if keep is not None and l not in keep:
            continue
        for p, v in ps.items():
            pages['%s|%d' % (l, p)] = [v[0], v[1]]
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump({'v': V, 'pages': pages}, f, ensure_ascii=False)
    os.replace(tmp, path)
    return len(pages)


def load(path):
    """返回 {label: {page: (it, c)}}；文件缺失/损坏/版本不符一律返回空 dict。

    label 用 rsplit('|', 1) 还原，故 label 内含竖线也不会错切。
    """
    try:
        with open(path, encoding='utf-8') as f:
            d = json.load(f)
        if d.get('v') != V or not isinstance(d.get('pages'), dict):
            return {}
        out = {}
        for k, v in d['pages'].items():
            l, p = k.rsplit('|', 1)
            out.setdefault(l, {})[int(p)] = (v[0], v[1])
        return out
    except Exception:
        return {}


def pending(tasks, bucket):
    """tasks: [(label, extra, page, size)] -> 过滤已完成页后的待采任务。"""
    done = set((l, p) for l, ps in bucket.items() for p in ps)
    return [t for t in tasks if (t[0], t[2]) not in done]


def timeout_for(est_pages, lo=3000, hi=6000):
    """按预估页量给单组合超时（秒）。

    实测速率约 1.5~2.5s/页（全局并发 12），另加划分探测约 900s、
    落盘/重试/汇总余量。固定 3000s 会在 1800 页级重档上误杀。
    est_pages 是粗估（误差可达数倍），故给 2.8 系数并封顶 6000s。
    """
    ep = est_pages or 0
    return int(min(hi, max(lo, 900 + ep * 2.8)))
