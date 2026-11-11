#!/usr/bin/env bash
# 唯一「上线/重启」入口：先卡门禁，没测过的代码一律不许拉起。
# 用法:
#   ./safe_restart.sh --check                 只校验
#   ./safe_restart.sh <命令> [参数...]        校验通过后执行
# 注意：不用 pkill -f 'python3 -u ...'——那会匹配到执行它的 shell 自身（踩过）。
cd /workspace/gh-gk-collect || exit 1
if [ $# -eq 0 ] || [ "$1" = "--check" ]; then
  exec python3 verify.py --check
fi
python3 verify.py --check || {
  echo "拒绝执行：代码未通过验证。先跑 ./verify.sh" >&2
  exit 1
}
echo "[$(date +%F' '%T)] 门禁通过，执行: $*"
exec "$@"
