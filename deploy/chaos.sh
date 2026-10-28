#!/usr/bin/env bash
# 混沌脚本：随机/指定 kill 一个中间件容器再拉起，模拟服务器崩溃（spec §16 演练工具）
# 用法：./chaos.sh           随机杀一个
#       ./chaos.sh kafka     指定杀 kafka（可用名见 ./chaos.sh list）
#       ./chaos.sh list      列出可杀对象
set -euo pipefail

TARGETS=(demo-mysql demo-kafka demo-connect demo-es demo-redis)

case "${1:-random}" in
  list)   printf '%s\n' "${TARGETS[@]}"; exit 0 ;;
  random) target="${TARGETS[$((RANDOM % ${#TARGETS[@]}))]}" ;;
  *)      target="demo-$1" ;;
esac

echo "⚡ chaos: kill ${target}"
docker kill "${target}" || { echo "跳过：容器 ${target} 未运行"; exit 0; }
sleep 5
echo "⚡ chaos: 重启 ${target}"
docker start "${target}"
echo "⚡ chaos: 完成。观察业务是否受影响、能否自愈（对应 bug 卡 C1/K1/E1…）"
