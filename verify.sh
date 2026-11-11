#!/usr/bin/env bash
# 全套验证唯一入口。通过才写 out/verify_stamp.json；失败禁止上线。
# 用法: ./verify.sh           跑全套
#       ./verify.sh --check   只校验当前代码是否已测过
cd /workspace/gh-gk-collect || exit 1
exec python3 verify.py "$@"
