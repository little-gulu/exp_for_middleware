#!/usr/bin/env bash
# 显式创建 demo 的 2 个业务 topic（compose 里关了 auto.create，教学 kafka-topics 用法）
# --partitions 3：并行上限=3（bug K4 的"加分区"就是改这里）
# --replication-factor 1：单节点只能 1（生产 3——demo 显式降级点，多副本见 docs/diagrams 架构图）
# 注意：Kafka 4.x CLI 已去掉 .sh 后缀（KIP-884）
set -euo pipefail

docker exec demo-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic product.changed --partitions 3 --replication-factor 1

docker exec demo-kafka kafka-topics --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic order.events --partitions 3 --replication-factor 1

docker exec demo-kafka kafka-topics --bootstrap-server localhost:9092 --list
