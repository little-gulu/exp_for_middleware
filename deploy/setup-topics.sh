#!/usr/bin/env bash
# 显式创建 demo 的 2 个业务 topic（compose 里关了 auto.create，教学 kafka-topics 用法）
# --partitions 3：并行上限=3（bug K4 的"加分区"就是改这里）
# --replication-factor 1：单节点只能 1（生产 3——demo 显式降级点，多副本见 docs/diagrams 架构图）
# 注意：apache/kafka 镜像 CLI 在 /opt/kafka/bin（不在 PATH），保留 .sh 后缀
# MSYS_NO_PATHCONV=1：防止 Git Bash 把容器内路径 /opt/... 转成 Windows 路径（经典坑）
set -euo pipefail
export MSYS_NO_PATHCONV=1

docker exec demo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic product.changed --partitions 3 --replication-factor 1

docker exec demo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic order.events --partitions 3 --replication-factor 1

docker exec demo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
