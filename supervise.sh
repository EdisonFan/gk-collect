#!/usr/bin/env bash
# 批量采集守护：检测 run_all.py 是否在跑，挂了就从断点自动重启。
# 断点在 out/done_all.json（已推送）与各组合 parts/ 片级缓存，
# run_all.py 每次启动还会重新拉远端达标清单合并，故重启无损续采。
# 本脚本自身也是普通进程，被环境释放后由调用方再次拉起即可。
cd /workspace/gh-gk-collect || exit 1
LOG=out/supervise.log
log(){ echo "[$(date +%F' '%T)] $*" >> "$LOG"; }
# 硬门禁：代码比上次全套测试新，就不拉起未验证的进程。
# 这样「改完忘了测」不会靠自觉，而是直接被卡住。
gate(){ python3 verify.py --check >/dev/null 2>&1; }

log "守护启动 PID=$$"
while true; do
  # 精确匹配 run_all.py，排除本守护与 shell 包装
  n=$(pgrep -f 'python3 -u run_all.py' | wc -l)
  if [ "$n" -eq 0 ]; then
    if gate; then
      log "run_all.py 不在运行，门禁通过，从断点重启"
      nohup python3 -u run_all.py >> out/run_all.log 2>&1 &
      log "已拉起 PID=$!"
    else
      log "拒绝拉起 run_all.py：代码未通过全套验证，请先 ./verify.sh"
    fi
  fi
  # 云端自动续派：队列未派完时保证它在跑（被环境释放后自动拉起）
  if [ ! -f out/cloud_feed_done ]; then
    n2=$(pgrep -f 'python3 -u cloud_feed.py' | wc -l)
    if [ "$n2" -eq 0 ]; then
      if gate; then
        log "cloud_feed.py 不在运行，门禁通过，拉起"
        # cloud_feed.py 自己写 out/cloud_feed.log，stdout 丢弃避免双写
        nohup python3 -u cloud_feed.py >> /dev/null 2>&1 &
        log "已拉起续派 PID=$!"
      else
        log "拒绝拉起 cloud_feed.py：代码未通过全套验证，请先 ./verify.sh"
      fi
    fi
  fi
  # 代理出口水位：低于 12 且距上次刷新超过 10 分钟，异步补货。
  # refresh_pool.py 是合并式写回，只会持平或变多；锁防并发、陈旧锁自清。
  live_n=$(wc -l < /tmp/live.txt 2>/dev/null || echo 0)
  last=$(stat -c %Y /tmp/live.txt 2>/dev/null || echo 0)
  age=$(( $(date +%s) - last ))
  lock=out/pool_refresh.lock
  if [ -f "$lock" ]; then
    lk=$(stat -c %Y "$lock" 2>/dev/null || echo 0)
    if [ $(( $(date +%s) - lk )) -gt 300 ]; then
      rm -f "$lock"
      log "代理补货锁超过 5 分钟，按陈旧锁清理"
    fi
  fi
  if [ "$live_n" -lt 12 ] && [ "$age" -ge 600 ] && [ ! -f "$lock" ]; then
    if gate; then
      touch "$lock"
      ( python3 -u refresh_pool.py >> out/pool_refresh.log 2>&1
        rm -f "$lock" ) &
      log "代理仅 $live_n 个（距上次刷新 ${age}s），异步补货 PID=$!"
    else
      log "代理仅 $live_n 个，但代码未通过验证，补货暂缓"
    fi
  fi
  sleep 60
done
