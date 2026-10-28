# 电商「搜索 + 订单」故障演练场 — 设计文档 v2

日期：2026-10-28
状态：v2（按学习目标对齐后），待用户审阅

## 变更记录

- v1 → v2：新增订单状态机（下单/支付/超时/取消/崩溃演练）；新增数据生成与规模设计（zipf 分布、百万级、突发流量）；新增 Redis 手搓布隆过滤器设计；新增 MySQL 性能优化 Lab；新增「环境搭建」「MySQL 性能」「业务架构」三类 bug（总数 26 → 41）；新增硬件极限说明、参数手册约定、学习材料计划、kafka-inspector 工具、chaos 脚本。

## 1. 目标与背景

**一句话**：用 Go + MySQL + CDC(Debezium) + Kafka + ES + Redis 搭一个贴近真实生产的电商 demo，内置 41 个可人为注入的生产故障，配合造数/压测/混沌工具，在"模拟生产环境"中完成从理论理解到手动保障稳定性的完整训练。

**硬性要求**：
- 每个 Go 服务是**独立 Go module + 独立 Dockerfile**，各自可单独 build 成镜像。
- 后续迁移到 k3s，接 Prometheus + Grafana。

**学习路径（四阶段，贯穿本项目）**：
1. **搭环境**：学搭建写法 + 搭建类常见 bug（镜像拉取、配置、连接）。
2. **懂架构**：理解一个完整产品能对外提供服务时的整体架构（组件职责、数据流向）。
3. **模拟生产**：尽最大程度模拟真实数据量、性能瓶颈、突发流量。
4. **保稳定**：在压力与故障中，从理论理解到手动保障产品稳定性。

**代码交付约定（重要）**：不要求每行代码手敲，但**每一块代码、每一个参数的选择，都必须注释清楚：它为了实现哪个需求、为了防止哪类问题、改错会触发哪个 bug（交叉引用 bug 编号）**。示例约定：

```go
// acks=all：等 ISR 全部副本确认才算发送成功。
// 实现的需求：下单事件"不丢"（链路 B 正确性的前提）。
// 防止的问题：K1 消息丢失。改成 0/1 即可复现该 bug。
```

## 2. 总体架构

```
                        用户请求
                          │
    ┌──────────────┬───────┴──────┬──────────────────┐
    ▼              ▼              ▼                  ▼
 product-service order-service search-service   load-gen(造数/压测)
    │ 写MySQL      │ 写MySQL       │ 查Redis→ES      │
    │ 布隆过滤器   │ 订单状态机    │                 │
    │              │ (支付/超时/取消)│                 │
    ▼              ▼              │                 │
  MySQL ──CDC──▶ Kafka ──▶ indexer ──▶ ES ◀─────────┘
 (products,      │             (消费 product.changed)
  orders)        │
                 └──▶ order-worker (消费 order.events)
                        ├─ 扣库存/回补库存(MySQL)
                        ├─ 通知(日志)
                        └─ 积分(Redis)
```

- **MySQL** = 事实来源；**Kafka** = 唯一事件总线；**ES** = 搜索投影；**Redis** = 读缓存 + 计数器 + 幂等去重 + 布隆位图。
- **两种消息模式并存**：搜索链路用 CDC 捕获（业务零侵入），订单链路用服务直发（显式 producer），方便对比优劣。
- **chaos 脚本**：随机 kill/重启服务，模拟服务器崩溃。
- **kafka-inspector**：查看 topic/分区/消费组/lag/key 路由的小工具。

## 3. 组件清单与服务划分

### 3.1 基础设施

| 组件 | 版本参考 | 说明 |
|---|---|---|
| MySQL | 8.x | 开 binlog（ROW 格式）、独立 server-id、建复制账号给 Debezium |
| Kafka | KRaft 模式（无 ZK） | 单节点起步；topic：`product.changed`、`order.events` |
| Kafka Connect + Debezium | Debezium 2.x | MySQL connector，捕获 binlog → `product.changed` |
| Elasticsearch | 8.x | `products` 倒排索引 |
| Redis | 7.x（已有镜像） | 缓存/计数/去重/布隆位图 |

> 用户没有 Kafka/ES/Prometheus/Grafana 镜像且未用过——实现阶段第一步是拉镜像 + 逐个上手引导（含在实现计划中）。

### 3.2 Go 服务与工具（每个都是独立 module + 独立 Dockerfile）

| 服务 | 端口 | 职责 | 关键依赖 |
|---|---|---|---|
| `product-service` | 8080 | 商品 CRUD + 布隆过滤器维护；商品详情（Redis 缓存） | mysql、redis |
| `order-service` | 8081 | 下单/模拟支付/取消/**超时扫描器**；状态机事件发 Kafka | mysql、kafka |
| `indexer` | — | 消费 `product.changed` → ES | kafka、es |
| `search-service` | 8082 | `/search?q=` → Redis 缓存 → ES | redis、es |
| `order-worker` | — | 消费 `order.events`：扣/回补库存、积分、通知；可多实例演示 rebalance | kafka、mysql、redis |
| `load-gen`（工具） | — | 造数（zipf 分布、百万级）、压测、突发流量 | 各客户端 |
| `kafka-inspector`（工具） | — | 查看 topic/分区/消费组/lag/key→partition 路由演示 | kafka |
| `chaos`（脚本） | — | 随机 kill/重启服务，模拟崩溃 | — |

### 3.3 目录结构

```
D:\think_twice\experiment_env\
├── services/
│   ├── product-service/   # module product-service
│   │   ├── go.mod / main.go / internal/ / Dockerfile
│   ├── order-service/     # module order-service（含状态机+超时扫描器）
│   ├── indexer/           # module indexer
│   ├── search-service/    # module search-service
│   └── order-worker/      # module order-worker
├── tools/
│   ├── load-gen/          # module load-gen
│   └── kafka-inspector/   # module kafka-inspector
├── deploy/
│   ├── docker-compose.yml
│   ├── mysql/             # init.sql, my.cnf
│   ├── kafka-connect/     # connector 配置
│   └── chaos.sh           # 混沌脚本
├── docs/
│   ├── superpowers/specs/ # 设计文档
│   ├── learning/          # 倒排索引/布隆过滤器原理等手把手材料
│   └── diagrams/          # 架构图
└── README.md
```

> 各服务不共享代码包，少量公共小工具（config 解析、日志封装）在各服务内各自实现，保持服务自包含、bug 注入局部化。

## 4. 数据模型

### 4.1 MySQL 表

```sql
CREATE TABLE products (
  id          BIGINT AUTO_INCREMENT PRIMARY KEY,
  title       VARCHAR(255) NOT NULL,
  description TEXT,
  price       DECIMAL(10,2) NOT NULL,
  category    VARCHAR(64),
  stock       INT NOT NULL DEFAULT 0,
  created_at  DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_category (category)
);

CREATE TABLE orders (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_no   VARCHAR(64) NOT NULL UNIQUE,   -- 幂等键 + Kafka 分区 key
  product_id BIGINT NOT NULL,
  quantity   INT NOT NULL,
  amount     DECIMAL(10,2) NOT NULL,
  status     VARCHAR(24) NOT NULL DEFAULT 'CREATED',
    -- CREATED → PAID / CANCELLED / TIMEOUT_CANCELLED
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_status_created (status, created_at)   -- 超时扫描器专用索引
);
```

### 4.2 订单状态机（链路 B 核心）

```
           下单                模拟支付成功
 CREATED ─────────▶ PAID ───────────▶ （加积分）
    │                  ▲
    │ 手动取消/超时     │ 支付失败（可配失败率）
    ▼                  │
 CANCELLED / TIMEOUT_CANCELLED ───▶ order-worker 回补库存
```

- **模拟支付网关**：`POST /orders/{order_no}/pay`，随机延迟 50–500ms + 可配失败率（默认 0，演练时调 30%）。
- **超时扫描器**：order-service 内 ticker 每 5s 扫 `status='CREATED' AND created_at < NOW()-INTERVAL 60 SECOND`（窗口可配），批量置为 `TIMEOUT_CANCELLED` 并发事件。生产一般用延迟队列，此简化为定时扫描，文档注明差异。
- **并发安全**：状态迁移用条件更新 `UPDATE ... WHERE status='CREATED'`，防支付/取消竞态（bug B3 即破坏它）。
- **幂等**：重复支付 PAID 订单 → no-op；支付 CANCELLED 订单 → 拒绝。

### 4.3 Kafka topic

| topic | 分区 | key | 来源 | 事件类型 |
|---|---|---|---|---|
| `product.changed` | 3 | product id | Debezium | CREATE/UPDATE |
| `order.events` | 3 | order_no | order-service 直发 | ORDER_CREATED / ORDER_PAID / ORDER_CANCELLED / ORDER_TIMEOUT_CANCELLED |

> `order.events` 以 order_no 为 key：**同一订单的生命周期事件必然落同一分区、严格有序**——这本身就是 K3（乱序）教学点的正确示范。

### 4.4 消息信封格式（统一，带 label 便于理解 partition/topic）

```json
{
  "event_id": "uuid",
  "event_type": "ORDER_CREATED",
  "key": "ORDER20261028...",        // 分区 key，hash(key)%分区数 决定落点
  "occurred_at": "2026-10-28T15:00:00Z",
  "payload": { "order_no": "...", "product_id": 123, "quantity": 1, "amount": 99.9 }
}
```

消费者统一日志格式（教学用，每条消息可见）：

```
[order.events] partition=1 offset=42 key=ORDER... event=ORDER_CREATED payload=...
```

### 4.5 ES 索引 `products`

- `title`、`description`：text（倒排，全文检索）
- `category`：keyword / `price`：double / `stock`：integer

### 4.6 Redis key

| key | 用途 | TTL |
|---|---|---|
| `search:{q}` | 搜索结果缓存 | 随机 5–10min |
| `product:{id}` | 商品详情缓存 | 10min |
| `order:dedup:{order_no}` | 消费幂等去重 | 24h |
| `points:user:{id}` | 积分计数 | 不过期 |
| `bf:products` | **手搓布隆过滤器位图**（Redis bitmap） | 不过期 |

## 5. 数据流

### 链路 A（搜索，CDC 模式）

```
product-service → 写 products（同时 SETBIT 布隆）→ MySQL binlog
→ Debezium → Kafka(product.changed) → indexer → ES(products)
search-service: /search?q= → Redis(search:{q}) 命中返回
                             └ 未命中 → ES → 回写 Redis
product-service: /products/{id} → 先查布隆(不存在直接404) → Redis → MySQL
```

### 链路 B（订单，直发模式 + 状态机）

```
order-service: 下单 → 写 orders + 发 ORDER_CREATED
  ├─ 支付(模拟) → 条件更新 PAID + 发 ORDER_PAID
  ├─ 超时扫描器 → 条件更新 TIMEOUT_CANCELLED + 发事件
  └─ 手动取消 → 条件更新 CANCELLED + 发事件
order-worker（消费组 order-worker-group）:
  ├─ ORDER_CREATED → 扣库存（条件扣减防超卖）
  ├─ ORDER_CANCELLED/TIMEOUT_CANCELLED → 回补库存
  └─ ORDER_PAID → 加积分 + 通知(日志)
```

## 6. 数据生成与规模（load-gen 能力）

- **真实感数据**：商品标题按品类模板生成（数码/服装/食品…），订单引用商品/用户 ID。
- **zipf 分布**（热销商品/热用户）：`--zipf 0.9` 生成长尾分布——天然制造**热 Key（R6）**与 **Kafka 分区倾斜（B1）**，这是真实电商数据的本质特征。
- **规模模式**：批量造数（`--count 1000000 --batch 5000`，多行 INSERT，百万行分钟级）；流式压测（`--rate 500/s`）。
- **突发流量**：`burst --base 100 --peak 2000 --duration 30s` 模拟秒杀/大促尖峰。
- **路由报告**：造数后打印"key→partition"落点统计，直观验证 `hash(key)%3` 与倾斜程度。
- **kafka-inspector**：列出 topic/分区/消费组，展示每分区 offset 与 lag——lag 是判断"堆积（K4）"的眼睛。

## 7. Redis 手搓布隆过滤器（穿透防护）

- **原理**（docs/learning/bloom-filter.md 手把手推导）：m 位位数组 + k 个哈希；误判率 `p ≈ (1-e^(-kn/m))^k`；只误报不漏报、不可删除。
- **实现**：Go 手写——双重哈希 `h_i(x) = h1(x) + i·h2(x) mod m`（h1=fnv、h2=murmur），位图落在 **Redis bitmap**（`SETBIT/GETBIT bf:products`），**不用 RedisBloom 模块**，现有镜像即可。
- **用途**：商品 ID 查询先过布隆——不存在直接 404，挡住 MySQL（R1 穿透的正确解）。
- **参数选择注释**：m、k 怎么定（按预期商品量 10^6 与目标误判率 1% 反推），写进代码注释。

## 8. MySQL 性能优化 Lab（百万行练习）

数据量：`products` 1M–10M 行、`orders` 1M–10M 行（load-gen 批量造）。练习项（每项都是 bug 卡）：

1. 无索引全表扫 vs 建索引——`EXPLAIN` 对比执行计划
2. 复合索引最左前缀原则
3. 深分页 `LIMIT 1000000,10` → 游标/延迟关联
4. 回表 vs 覆盖索引（`SELECT title` vs `SELECT *` 的 Using index 差异）
5. 隐式类型转换索引失效（`WHERE varchar_col = 123`）
6. `LIKE '%x%'` 前导通配符索引失效 → **引出"为什么搜索不用 LIKE 而用 ES"**（呼应链路 A 价值）

## 9. Go 服务技术要点（重点示范）

- goroutine 消费循环 + `channel`；`context` + `signal.NotifyContext` 优雅退出
- Kafka 消费者组、手动提交 offset、幂等消费（Redis SETNX + 状态机条件更新）
- 生产者 `acks=all` + 幂等；Redis `singleflight` 防击穿；随机 TTL 防雪崩
- 超时扫描器 ticker 模式；条件更新（乐观锁）防并发竞态
- HTTP：超时、连接池、响应体关闭、结构化日志

## 10. 可靠性设计（「正确版」基线，bug 即破坏它）

- **Kafka**：producer `acks=all` + 幂等；consumer 手动提交 offset + 按 order_no 幂等去重；毒丸消息进死信/带重试退避。
- **Redis**：Cache-Aside；穿透→布隆+空值缓存；击穿→`singleflight`；雪崩→随机 TTL；一致性→先更新 DB 再删缓存。
- **MySQL**：索引设计、条件更新防竞态、批量操作。
- **服务**：优雅退出、goroutine 不泄漏、连接池上限、响应体关闭、结构化日志。

## 11. 故障清单（41 个，五要素）

> 用法：按「注入」改代码/配置 → 观察「症状」→ 用「排查」线索定位 → 自己修复 → 再对照「根因/修复」核对。

### Kafka 类（8）

**K1 消息丢失**
- 注入：order-service 把 producer `acks` 从 `all` 改成 `0`
- 症状：压测中 kill broker，部分订单「写库成功但下游没收到」，库存/积分对不上
- 排查：对比 `orders` 表行数 vs order-worker 处理数；topic 消息数偏少
- 根因：`acks=0` 不等确认即返回成功，leader 故障时消息未落盘
- 修复：`acks=all` + `min.insync.replicas≥2`

**K2 消息重复**
- 注入：order-worker 改成「先处理业务、后提交 offset」，处理中途随机崩溃
- 症状：重启后同一订单被扣两次库存/加两次积分
- 排查：同一 order_no 被处理多次；dedup key 命中
- 根因：at-least-once 语义，已处理未提交
- 修复：处理前 `SETNX order:dedup:{order_no}` 幂等去重

**K3 消息乱序**
- 注入：order-service 发事件时 key 用随机值/不设 key
- 症状：同一订单 CREATED 与 CANCELLED 顺序颠倒，状态机错乱（先取消后创建）
- 排查：打印每条消息的 (partition, offset, order_no)，同订单跨分区
- 根因：key 不稳定导致同实体消息路由到不同 partition，跨分区无序
- 修复：用 order_no 做 key，同 key 同分区

**K4 消息堆积**
- 注入：order-worker 处理逻辑加 `time.Sleep`；同时把分区设成 1
- 症状：lag 持续增长，下单很久后才扣库存
- 排查：kafka-inspector 看 lag；分区数与消费者数
- 根因：消费速率 < 生产速率，分区数限制并行上限
- 修复：去掉慢逻辑/改异步，加分区并扩容消费者实例

**K5 rebalance 风暴**
- 注入：`max.poll.interval.ms` 设 3s，处理逻辑耗时
- 症状：日志刷「(Re-)joining group」，几乎不消费
- 排查：消费者日志频繁 rebalance
- 根因：两次 poll 间隔超时被踢出组，反复重加入
- 修复：调大 `max.poll.interval.ms` 或加快单批处理

**K6 毒丸消息**
- 注入：order.events 混入一条反序列化失败/字段非法（price 为空）的消息
- 症状：消费循环卡在报错处，该 partition 不再前进
- 排查：日志停在同一条消息报错；该分区 lag 不减少
- 根因：坏消息未隔离，异常阻塞消费
- 修复：解析失败写死信 topic，继续消费后续

**K7 offset 重置导致重复/丢**
- 注入：改 `group.id`，或 `auto.offset.reset=earliest`
- 症状：历史消息重放（库存多扣）或从最新开始丢老消息
- 排查：对比消费组 offset 与 topic 位置
- 根因：group id 是 offset 存储键，改了即定位变化
- 修复：不随意改 group id；按需选 reset 策略

**K8 生产者未 flush/未 close**
- 注入：order-service 退出前不 `producer.Close()`
- 症状：退出瞬间最后几条消息丢失
- 排查：退出日志 vs topic 消息数，末尾缺失
- 根因：缓冲未刷盘即退出
- 修复：优雅退出时 `defer producer.Close()`

### Redis 类（8）

**R1 缓存穿透**
- 注入：product-service 去掉布隆/空值缓存，大量查不存在商品 id
- 症状：不存在的 key 全部打穿到 MySQL，DB 被打爆
- 排查：Redis 命中率骤降，MySQL 慢查询激增
- 根因：查不存在 key 无防护
- 修复：布隆过滤器 + 空值短 TTL 缓存

**R2 缓存击穿**
- 注入：热点商品详情缓存到期，去掉 `singleflight`/锁
- 症状：过期瞬间大量请求同时打到 MySQL
- 排查：DB 慢查询时间点尖峰，对应某商品缓存过期
- 根因：热点 key 过期无互斥
- 修复：`singleflight` 或互斥锁，只让一个请求重建

**R3 缓存雪崩**
- 注入：所有缓存 TTL 设相同值
- 症状：某时刻大量 key 同时过期，后端被打垮
- 排查：缓存过期时间戳集中在同一时刻
- 根因：过期时间无随机化
- 修复：TTL 加随机偏移（10min ± 30%）

**R4 缓存与 DB 不一致**
- 注入：更新商品时「先删缓存、再更新 DB」且并发读
- 症状：更新后立刻读，偶发读到旧值
- 排查：回填旧值的时序交错
- 根因：删/改顺序与并发读时序交错
- 修复：先更新 DB 再删缓存；严格场景延迟双删/版本号

**R5 Big Key**
- 注入：某商品描述塞几 MB 大 value 存 Redis
- 症状：GET/SET 该 key 变慢，阻塞 Redis 单线程
- 排查：`redis-cli --bigkeys`、`slowlog`
- 根因：大 value 网络/内存/序列化开销大，阻塞单线程
- 修复：拆分/压缩/改存对象存储

**R6 热 Key**
- 注入：load-gen zipf 集中狂打同一热门商品
- 症状：该 key 所在节点 CPU 打满，其它 key 响应变慢
- 排查：monitor/统计热点 key QPS
- 根因：单 key 超高频访问，单点瓶颈
- 修复：本地缓存（多级缓存）、key 拆分分摊

**R7 内存打满 / 淘汰误伤**
- 注入：关 TTL + `maxmemory` 很小 + `allkeys-lru`
- 症状：内存打满后热点也被淘汰，命中率暴跌
- 排查：`info memory`、`evicted_keys` 上升
- 根因：无 TTL + 淘汰策略不当
- 修复：合理 TTL、分优先级、扩容/分片

**R8 布隆过滤器参数错误**
- 注入：把布隆的 m（位数）设过小/k 不对（如按 10 万商品设计却灌 100 万）
- 症状：误判率飙升——大量不存在的 ID 被放过打穿 DB，或正常请求被误拦
- 排查：计算实际误判率 vs 设计值；Redis `bf:products` 位填充率接近 100%
- 根因：m/k 与数据量不匹配，位数组过载
- 修复：按预期数据量 n 和目标误判率 p 反推 `m = -n·ln p/(ln 2)²`，`k = m/n·ln 2`

### CDC 类（4）

**C1 CDC 断流不恢复**
- 注入：`docker stop` kafka-connect，商品继续上架
- 症状：新商品搜不到，ES 停止更新，无人察觉
- 排查：Connect 状态；`product.changed` 无新消息
- 根因：连接器挂了无告警/无自动恢复
- 修复：健康检查 + 告警 + 自动重启

**C2 位点丢失导致重复/丢**
- 注入：重置 Debezium offset 存储（或改 connector 名重建）
- 症状：全量重放（ES 重复）或跳过中间段（ES 缺数据）
- 排查：ES 文档数 vs MySQL 行数
- 根因：offset 是消费进度唯一依据
- 修复：offset 存 Kafka 内部 topic，勿随意清；重建前做快照

**C3 schema 不匹配**
- 注入：`ALTER TABLE products ADD COLUMN` 新字段
- 症状：connector 报错停止，后续变更不同步
- 排查：Connect 日志 schema 校验失败
- 根因：表结构变更后 schema 历史未跟上
- 修复：schema 演进配置或重建 connector

**C4 全量快照与增量切换丢数据**
- 注入：初始快照期间持续写入新数据
- 症状：快照窗口内的部分写入缺失于 ES
- 排查：快照时间窗口内的写入未进 ES
- 根因：快照与 binlog 切换存在窗口
- 修复：snapshot 一致性策略（`snapshot.mode=when_needed` + 位点衔接）

### Go 并发类（5）

**G1 goroutine 泄漏**
- 注入：每收到一条消息 `go` 一个 goroutine，无 context 取消
- 症状：goroutine 数持续增长，内存上涨至 OOM
- 排查：`pprof` goroutine 数单调递增
- 根因：goroutine 无退出机制
- 修复：传 `context` + `select` 监听取消，或 worker pool

**G2 map 并发写 panic**
- 注入：search-service 用全局 `map` 缓存且多 goroutine 并发写
- 症状：panic `concurrent map writes`
- 排查：panic 栈指向 map 写入
- 根因：Go map 非并发安全
- 修复：`sync.Map` / 加锁 / channel 串行化

**G3 未优雅退出丢消息**
- 注入：收到 SIGTERM 直接 `os.Exit`
- 症状：发布时正在处理的消息丢失或重复
- 排查：发布时日志中断，offset 未提交
- 根因：未捕获退出信号收尾
- 修复：`signal.NotifyContext` + 等当前批完成 + 提交 offset 再退出

**G4 连接池耗尽**
- 注入：`SetMaxOpenConns(1)` 且请求阻塞
- 症状：高并发下请求大量超时/排队
- 排查：`db.Stats()` 连接数打满
- 根因：连接池太小
- 修复：合理 MaxOpenConns/MaxIdleConns + 超时释放

**G5 HTTP 响应体未关闭**
- 注入：调 ES 后不 `defer resp.Body.Close()`
- 症状：连接泄漏，连接数增长，端口耗尽
- 排查：`netstat` 连接数增长
- 根因：响应体未关闭，连接无法复用
- 修复：`defer resp.Body.Close()` + 读空 body

### ES 类（2）

**E1 索引不一致**
- 注入：indexer 处理一半 `kill -9`
- 症状：搜索漏结果，ES 文档数 < MySQL 行数
- 排查：ES count vs MySQL count
- 根因：崩溃且 offset 未提交，处理过的没重放
- 修复：处理+写 ES+提交 offset 顺序正确且幂等，可安全重放

**E2 刷新延迟搜不到**
- 注入：`refresh_interval` 调大（30s），写入后立刻搜
- 症状：刚上架商品立刻搜不到，几秒后才出现
- 排查：写入后 `_count` vs 实际搜索差异
- 根因：ES 写入先入 buffer，refresh 后才可搜
- 修复：理解 refresh 语义；必要场景 `refresh=true`

### 环境搭建类（6，新增）

**S1 Kafka advertised.listeners 配错**
- 注入：容器内外地址不一致（把 `localhost:9092` 配给容器内消费者）
- 症状：broker 返回给客户端的地址连不上，客户端全部超时
- 排查：客户端日志「Connection refused」指向 localhost；`kafka-topics` 报 leader 不可用
- 根因：advertised.listeners 是"告诉客户端从哪连我"的地址，容器内外不通用
- 修复：内外分离两个 listener（内网用服务名、外网用宿主机 IP）

**S2 ES bootstrap 检查失败**
- 注入：宿主机 `vm.max_map_count` 不调（Windows/WSL2 常见）
- 症状：ES 容器启动即退，日志报 bootstrap checks failed
- 排查：容器日志 `max virtual memory areas vm.max_map_count [65530] is too low`
- 根因：ES 内存映射要求内核参数，默认值太小
- 修复：`sysctl -w vm.max_map_count=262144`（WSL2 里）/ 挂载调整

**S3 MySQL binlog 未开/格式不对**
- 注入：`binlog_format` 是 STATEMENT 或未开 binlog、server-id 未设
- 症状：Debezium connector 启动失败，捕获不到任何变更
- 排查：Connect 日志要求 binlog ROW；`SHOW VARIABLES LIKE 'binlog%'`
- 根因：CDC 依赖 ROW 格式 binlog 与唯一 server-id
- 修复：`binlog_format=ROW` + `server-id=1` + 复制账号权限

**S4 Debezium connector 配置错误**
- 注入：连接串/账号密码/`database.history` 配错，或两个 connector 抢同一 server-id
- 症状：connector 反复 FAILED/RUNNING 循环
- 排查：Connect REST `/connectors/{name}/status` 与日志
- 根因：配置项不匹配或 server-id 冲突
- 修复：核对连接参数；不同 connector 用不同 server-id

**S5 Docker 网络不通**
- 注入：服务间用 `localhost:3306` 而非容器名 `mysql:3306`
- 症状：服务起不来，连接拒绝
- 排查：容器内 `curl` 探测；compose 网络隔离
- 根因：localhost 指容器自己，不是宿主机
- 修复：compose 内用服务名互访，宿主机访问才用 localhost

**S6 Redis 持久化配置缺失**
- 注入：纯内存无 RDB/AOF，或 AOF 配置错误
- 症状：重启 Redis 后缓存/计数/布隆位图全丢
- 排查：重启前后 `DBSIZE` 对比；`config get save/appendonly`
- 根因：未配置持久化或策略不当
- 修复：按场景配 RDB 快照 + AOF；缓存类数据可接受丢，计数/位图不能

### MySQL 性能类（5，新增）

**M1 缺索引慢查询**
- 注入：百万行表上按无索引列查询（如 `WHERE title LIKE 'x%'` 且 title 无索引，或按未索引列等值查）
- 症状：查询从毫秒变秒级，接口超时
- 排查：`EXPLAIN` 看 `type=ALL` 全表扫、`rows` 百万级
- 根因：无可用索引，全表扫描
- 修复：按查询模式建索引（注意最左前缀）

**M2 深分页**
- 注入：`LIMIT 1000000, 10` 翻页
- 症状：越往后翻越慢，甚至秒级/超时
- 排查：`EXPLAIN` 看扫描行数 = offset+limit
- 根因：MySQL 需跳过前面 100 万行
- 修复：游标分页（`WHERE id > last_id LIMIT 10`）/ 延迟关联

**M3 回表过多**
- 注入：`SELECT *` 且索引不含所需列
- 症状：索引扫描快但整体仍慢，CPU 高
- 排查：`EXPLAIN` 看 `Using index condition` 缺失、`rows` 与回表次数
- 根因：二级索引只存键，取完整行要回聚簇索引
- 修复：覆盖索引（索引含 SELECT 列）/ 减少返回列

**M4 隐式类型转换索引失效**
- 注入：字符串列（如 order_no）用数字查：`WHERE order_no = 123456`
- 症状：走了全表扫，慢查询
- 排查：`EXPLAIN` 索引没生效；MySQL 对列做了类型转换
- 根因：列上发生隐式转换，无法用索引
- 修复：查询值类型与列一致（加引号）

**M5 前导通配符索引失效**
- 注入：`LIKE '%关键词%'` 搜商品
- 症状：全表扫，百万行表秒级响应
- 排查：`EXPLAIN` 索引失效；B+ 树无法用前导未知的模糊匹配
- 根因：前导通配符无法利用 B+ 树有序性
- 修复：这就是**搜索为什么交给 ES 倒排索引**（呼应链路 A）；轻量场景可用前缀索引/全文索引

### 业务/架构类（3，新增）

**B1 Kafka 分区倾斜**
- 注入：load-gen 用高 zipf 系数造数（少数热 key 占大头）
- 症状：单分区 lag 暴涨，其它分区几乎空载；部分消费者忙死部分闲死
- 排查：kafka-inspector 看每分区消息数/lag 分布
- 根因：key 分布不均，hash 后少数分区承载绝大部分流量
- 修复：key 加盐/二次路由（生产）；理解"分区是并行上限"的代价

**B2 超时扫描器失效**
- 注入：把扫描器 ticker 注释掉，或扫描条件写错（如 `>` 写成 `<`）
- 症状：超时订单永远停在 CREATED，库存被永久占用
- 排查：观察超过超时窗口的 CREATED 订单数持续增长
- 根因：后台任务失效且无监控
- 修复：恢复扫描器 + 加"滞留订单数"指标告警

**B3 支付/取消竞态**
- 注入：状态迁移改非条件更新（`UPDATE orders SET status='PAID'` 不带 `WHERE status='CREATED'`）
- 症状：并发支付+取消时，已取消订单被置为 PAID 或反之，库存/积分错乱
- 排查：压测同时发 pay 和 cancel，观察状态异常迁移
- 根因：无乐观锁，后写覆盖先写
- 修复：条件更新（`WHERE status='CREATED'`）+ 检查 affected rows

## 12. 硬件极限与边界（诚实版）

**32G 单机天花板**（D 盘剩余 388G，磁盘不是瓶颈）：

| 资源 | 极限 |
|---|---|
| MySQL | 千万行级（每百万行约百 MB–GB） |
| Kafka | 百万级消息、单 broker 十万级 msg/s（小消息） |
| ES | 单节点 5M–20M 文档（heap 2–4G） |
| Redis | 百万级 key |
| 并发 | Go 服务数千 QPS |

**能忠实模拟的**：架构模式、故障模式、性能现象（索引失效/缓存击穿/分区倾斜/rebalance/连接池耗尽）、调优手法——这些在百万级、千 QPS 下就会真实显现，不需要生产规模。
**模拟不了的**：跨机房网络分区、真多机物理资源隔离（k3s 阶段可模拟编排逻辑，但共享同一台物理机）。

## 13. 参数手册约定（贯穿所有代码）

每个参数/代码块的注释模板：

```go
// 参数/这段代码做什么
// 实现的需求：xxx（链路/功能的哪一部分）
// 防止的问题：xxx（bug 编号交叉引用）
// 改错的后果：改成什么值会复现哪个 bug
```

实现阶段所有 Go 代码、compose 配置、中间件配置都按此约定编写；同时在 `docs/learning/` 维护一份「参数速查表」，汇总所有关键参数与其对应 bug 编号。

## 14. 学习材料计划（docs/learning/）

| 材料 | 内容 |
|---|---|
| `bloom-filter.md` | 布隆过滤器原理推导（m/k/误判率公式）+ Go 手搓实现逐步讲解 |
| `inverted-index.md` | 倒排索引原理（分词、倒排表、相关性排序）+ ES 对应概念映射 |
| `kafka-quickstart.md` | KRaft 快速上手（topic/分区/消费组/lag 实操） |
| `es-quickstart.md` | ES 上手（索引/映射/查询 DSL） |
| `prometheus-grafana.md` | 监控栈上手（阶段二） |
| `parameters.md` | 参数速查表（参数 → 需求 → bug 编号） |

## 15. 部署

### 阶段一：本地 Docker Compose

- 服务：mysql、kafka(KRaft)、kafka-connect(debezium)、elasticsearch、redis、5 个 Go 服务、load-gen/kafka-inspector（按需运行）、chaos.sh。
- **第一步是拉镜像**（用户尚无 Kafka/ES 等镜像）：kafka、kafka-connect+debezium、elasticsearch、grafana、prometheus，共约 3–5GB。
- MySQL 初始化：`init.sql` 建表；`my.cnf` 开 `binlog_format=ROW`、`server-id`；Debezium 复制账号。

### 阶段二：k3s + 可观测

- k3s 跑在 WSL2 / Linux VM（Windows 不能原生跑 k3s）。
- 各服务 Deployment；order-worker 多副本演示 rebalance/HPA。
- Prometheus（`promhttp` 暴露 `/metrics`）+ Grafana 面板；Kafka lag、Redis、ES 指标接入。

## 16. 验证与演练方法

1. **正常流**：造商品 → 搜索命中（缓存生效）→ 下单 → 支付 → 扣库存/加积分。
2. **状态机**：下单 → 等超时 → 自动取消 → 库存回补；支付失败重试；重复支付幂等。
3. **rebalance**：order-worker 起 2 实例，kill 一个，观察分区重分配。
4. **混沌**：`chaos.sh` 随机杀服务，观察系统如何（不）自我恢复。
5. **压测**：burst 突发流量 → 观察堆积/缓存/连接池/倾斜。
6. **性能 lab**：百万行表 EXPLAIN 专项练习。
7. **故障演练**：从第 11 节挑 bug → 注入 → 观察 → 排查 → 修复 → 对照核对。

## 17. 待办 / 后续

- 阶段一落地后，单独出阶段二（k3s + Prometheus/Grafana）的 spec。
- 学习材料随实现进度逐步撰写。
