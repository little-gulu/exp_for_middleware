# exp_for_middleware — 电商「搜索 + 订单」故障演练场

用 Go + MySQL + CDC(Debezium) + Kafka + ES + Redis 搭建的贴近真实生产的电商 demo，内置 41 个可人为注入的生产故障，配合造数/压测/混沌工具，用于：

1. 搭环境时学习搭建写法与搭建常见 bug
2. 理解一个完整产品对外服务时的整体架构
3. 模拟真实生产的数据量、性能瓶颈、突发流量
4. 在压力与故障中训练"手动保障稳定性"的能力

## 目录

- 设计文档：[docs/superpowers/specs/2026-10-28-ecommerce-search-order-demo-design.md](docs/superpowers/specs/2026-10-28-ecommerce-search-order-demo-design.md)
- 架构图：[docs/diagrams/kraft-kafka-architecture.html](docs/diagrams/kraft-kafka-architecture.html)（KRaft 架构，浏览器打开）
- 学习材料：`docs/learning/`（倒排索引、布隆过滤器原理等，随实现逐步补充）

## 技术栈

| 组件 | 用途 |
|---|---|
| MySQL | 事实来源（products / orders） |
| Debezium (CDC) | MySQL 变更 → Kafka |
| Kafka (KRaft) | 事件总线（product.changed / order.events） |
| Elasticsearch | 商品倒排索引 |
| Redis | 缓存 / 计数 / 幂等 / 布隆位图 |
| Go | 5 个服务 + load-gen + kafka-inspector |

## 状态

🟡 设计已确认（v2），实现计划进行中。
