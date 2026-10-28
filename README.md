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

🟢 阶段一基础设施已跑通（计划 1 完成：MySQL/Kafka/ES/Redis/Debezium + CDC 验收通过）。

## 部署与验证（计划 1 成果）

**前置**
- Docker Desktop 已启动；代理开着（首次拉镜像约 2GB）。
- Windows 每次 Docker/WSL 重启后执行一次（否则 ES 起不来，bug S2）：
  ```bash
  wsl -d docker-desktop -u root sysctl -w vm.max_map_count=262144
  ```
- 端口注意：本机 3306/6379 被原生 MySQL 与其它项目占用，demo 用 **13306/16379**（9200/8083/9094 直连）。

**步骤**
```bash
cd deploy
docker compose up -d              # 拉起 5 个中间件
bash setup-topics.sh              # 创建 2 个业务 topic（3 分区）
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors -d @kafka-connect/mysql-connector.json   # 部署 CDC
```

**CDC 验收**（一条 SQL 自动出现在 Kafka）
```bash
docker exec demo-mysql mysql -uroot -proot123456 -e "INSERT INTO demo.category (id,parent_id,name,level,path,is_leaf) VALUES (9998,0,'CDC验收',1,'0/9998/',1);"
docker exec demo-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic product.changed --from-beginning --max-messages 1
```

**常用速查**
```bash
docker compose ps
docker exec demo-kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
docker exec demo-redis redis-cli INFO memory
curl -s localhost:9200/_cluster/health
curl -s localhost:8083/connectors/product-center-connector/status
bash deploy/chaos.sh random     # 随机杀一个中间件，观察业务反应
```

**踩坑记录（都是真实的"搭建类 bug"，对应故障清单 S 系列）**
1. apache/kafka 镜像 CLI 在 `/opt/kafka/bin`（不在 PATH），且 Git Bash 会把容器内路径转成 Windows 路径 → 用全路径 + `MSYS_NO_PATHCONV=1`。
2. 镜像默认数据目录是 `/tmp/kafka-logs`（容器层，重建即丢）→ 必须 `KAFKA_LOG_DIRS=/var/lib/kafka/data` 钉到挂载卷。
3. 单节点集群必须把内部主题副本因子降为 1（`offsets.topic.replication.factor` 等），否则 `__consumer_offsets` 创建失败、offset 提交卡死。
4. Debezium 默认把 DDL 事件发到 `topic=<prefix>` 这个 topic；`auto.create.topics=false` 时 producer 卡死拖垮整条管线 → `include.schema.changes=false`。
5. 删 connector 不清 offset；同名重建不会重新快照 → 换名或清 offset。
