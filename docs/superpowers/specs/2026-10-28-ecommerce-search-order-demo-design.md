# 电商「商品中心 + 订单」故障演练场 — 设计文档 v3

日期：2026-10-28
状态：v3.1（已批准，待实现计划）

## 变更记录

- v1 → v2：订单状态机、zipf 造数、布隆过滤器、MySQL 性能 Lab、41 个 bug、参数手册约定。
- v2 → v3：**业务锚点升级**——以场景题《淘宝商品如何支持几十种商品类型（SPU/SKU/类目/动态属性）》为核心：SPU/SKU 两层建模、属性元数据驱动、JSON 异构属性、可检索属性倒排化（ES + MySQL 倒排表双实现）、热点库存 Redis 预扣（演进）、三态库存、Redis 挂三道防线、发布幂等、ZSET 榜单；bug 数 41 → **50**。
- v3 → v3.1：新增 §18「简化内容与扩展路线」（7 个简化点 + 场景题完整版扩展方向）；§13 改为内存预算制——**不榨干 32G**（堆栈预算 10–12GB，留 20GB+ 给其他进程），默认数据量降为 SPU 10 万 / SKU 50 万 / orders 100 万。

## 1. 目标与背景

**一句话**：以「商品中心」场景题为业务内核，用 Go + MySQL + CDC(Debezium) + Kafka + ES + Redis 搭一个贴近真实生产的电商 demo，内置 50 个可人为注入的生产故障，配合造数/压测/混沌工具，在模拟生产环境中完成从理论到手动保障稳定性的训练。

**业务锚点**（`Desktop\learn\场景题-系统设计\01-商品中心.md`）一句话总纲：**核心代码只认「属性元数据」，不认「具体属性」**——类目/属性/SPU/SKU 拆成「关系表骨架 + JSON 异构属性 + 元数据驱动校验」，可检索属性抽出来拍平成倒排（ES / 倒排表）。新增一种商品类型 = 加配置，不改代码。

**硬性要求**：
- 每个 Go 服务是**独立 Go module + 独立 Dockerfile**，各自可单独 build 成镜像。
- 后续迁移到 k3s，接 Prometheus + Grafana。

**学习路径（四阶段）**：
1. **搭环境**：学搭建写法 + 搭建类 bug。
2. **懂架构**：理解完整产品对外服务的整体架构（本题的读写分离、强一致/最终一致分层）。
3. **模拟生产**：真实数据量、性能瓶颈、突发流量。
4. **保稳定**：压力与故障中保障稳定性。

**代码交付约定**：每一块代码、每一个参数都必须注释清楚「实现的需求 / 防止的问题 / 改错触发哪个 bug（交叉引用编号）」。示例：

```go
// acks=all：等 ISR 全部副本确认才算发送成功。
// 实现的需求：发布/订单事件"不丢"。
// 防止的问题：K1 消息丢失。改成 0/1 即可复现。
```

## 2. 总体架构

```
                        用户请求 / 商家发布
                          │
    ┌──────────────┬───────┴──────┬──────────────────┐
    ▼              ▼              ▼                  ▼
 product-service order-service search-service   load-gen(造数/压测)
 │ 发布(元数据驱动) │ 订单状态机     │ SearchEngine接口  │ 属性驱动生成
 │ SPU归一/SKU笛卡尔│ 支付/超时/取消 │ ├ MySQL倒排实现   │ 类目/meta/SPU/SKU
 │ 布隆/详情缓存    │              │ └ ES实现(facet)  │ zipf 热SKU
    │ 写MySQL      │ 写MySQL       │ 查Redis缓存→引擎  │
    ▼              ▼              │                 │
  MySQL ──CDC──▶ Kafka ──▶ indexer ──▶ ES ◀─────────┘
 (category,     │   (消费 spu/sku/meta 变更,
  attribute_meta,│    拍平JSON→倒排文档,
  spu, sku,      │    维护MySQL倒排表)
  orders)        │
                 └──▶ order-worker (消费 order.events)
                        ├─ 扣库存(sku, 防超卖; 热点走Redis预扣演进)
                        ├─ 回补库存(取消时)
                        ├─ 销量榜 ZINCRBY (支付时)
                        └─ 积分/通知
```

- **MySQL** = 事实来源（关系列扛强一致）；**Kafka** = 唯一事件总线；**ES** = 可检索属性的倒排引擎；**Redis** = 详情缓存 + 三态库存 + 榜单 ZSET + 幂等 + 布隆位图。
- **两种消息模式并存**：商品链路 CDC 捕获（发布零侵入），订单链路服务直发。
- **SearchEngine 接口两个实现**：MySQL 倒排表（小团队方案）与 ES（上规模方案），显式能力协商。

## 3. 组件清单与服务划分

### 3.1 基础设施

| 组件 | 版本参考 | 说明 |
|---|---|---|
| MySQL | 8.x | binlog（ROW）、server-id、Debezium 复制账号；表见 §4 |
| Kafka | KRaft | topic：`product.changed`、`order.events` |
| Kafka Connect + Debezium | 2.x | 捕获 category/attribute_meta/spu/sku 变更 → `product.changed` |
| Elasticsearch | 8.x | `sku` 倒排文档（属性拍平） |
| Redis | 7.x | 缓存/三态库存/榜单/幂等/布隆位图 |

### 3.2 Go 服务与工具（独立 module + 独立 Dockerfile）

| 服务 | 端口 | 职责 |
|---|---|---|
| `product-service` | 8080 | 发布（幂等键、元数据驱动校验、SPU 归一、SKU 笛卡尔积、布隆 SETBIT）；详情（布隆 → Redis → MySQL）；类目/属性 meta 查询 |
| `order-service` | 8081 | 下单（写 orders + 发 Kafka）；模拟支付；取消；超时扫描器 |
| `indexer` | — | 消费 `product.changed`：spu/sku/meta 事件 → 缓存 spu/枚举映射 → 拍平写 ES + 维护 `product_attr_index` 表 |
| `search-service` | 8082 | 搜索/筛选（`SearchEngine` 接口，`--engine=mysql|es`）；facet 聚合；热搜词 ZINCRBY；搜索缓存；本地 LRU + singleflight + 熔断 + 限流 |
| `order-worker` | — | 消费 `order.events`：扣/回补库存（条件更新防超卖）、销量榜 ZINCRBY、积分、通知；可多实例演示 rebalance |
| `load-gen` | — | 属性驱动造数（类目/meta/SPU/SKU/订单，zipf）、压测、突发流量 |
| `kafka-inspector` | — | topic/分区/消费组/lag/key 路由演示 |
| `chaos`（脚本） | — | 随机 kill/重启服务 |

### 3.3 目录结构

```
D:\think_twice\experiment_env\
├── services/
│   ├── product-service/   # 发布引擎（元数据驱动核心）
│   ├── order-service/     # 状态机 + 超时扫描器
│   ├── indexer/           # CDC 消费 + JSON 拍平 + 双写(ES/倒排表)
│   ├── search-service/    # SearchEngine 接口 + 缓存/防线
│   └── order-worker/      # 库存 + 榜单
├── tools/
│   ├── load-gen/          # 属性驱动造数
│   └── kafka-inspector/
├── deploy/
│   ├── docker-compose.yml
│   ├── mysql/             # init.sql（建表+种子类目/meta/枚举）, my.cnf
│   ├── kafka-connect/     # connector 配置
│   └── chaos.sh
├── docs/
│   ├── superpowers/specs/ # 设计文档
│   ├── learning/          # 倒排索引/布隆原理/商品中心场景题精讲
│   └── diagrams/
└── README.md
```

## 4. 数据模型

### 4.1 MySQL 表（商品中心，全部来自场景题）

```sql
-- 类目树：path 前缀匹配查子树，避免递归 CTE
CREATE TABLE category (
  id        BIGINT PRIMARY KEY,
  parent_id BIGINT NOT NULL DEFAULT 0,
  name      VARCHAR(128) NOT NULL,
  level     TINYINT NOT NULL,
  path      VARCHAR(256) NOT NULL,            -- "0/12/34/"
  is_leaf   TINYINT NOT NULL DEFAULT 0,
  status    TINYINT NOT NULL DEFAULT 1,
  KEY idx_parent (parent_id), KEY idx_path (path)
) ENGINE=InnoDB;

-- 属性元数据：整题的"驱动引擎"，新增类型=加配置
CREATE TABLE attribute_meta (
  id          BIGINT PRIMARY KEY,
  category_id BIGINT NOT NULL,                -- 绑定类目；0=全局
  name        VARCHAR(64) NOT NULL,
  value_type  ENUM('int','string','decimal','enum','range','date') NOT NULL,
  is_key      TINYINT NOT NULL DEFAULT 0,     -- 关键属性→SPU 归一
  is_sale     TINYINT NOT NULL DEFAULT 0,     -- 销售属性→SKU 笛卡尔积
  is_search   TINYINT NOT NULL DEFAULT 0,     -- 进倒排
  is_filter   TINYINT NOT NULL DEFAULT 0,     -- 做 facet 筛选
  is_multi    TINYINT NOT NULL DEFAULT 0,
  required    TINYINT NOT NULL DEFAULT 0,
  enum_values JSON,                           -- [{"vid":1001,"text":"黑"}]
  unit        VARCHAR(16),
  sort_order  INT NOT NULL DEFAULT 0,
  version     BIGINT NOT NULL DEFAULT 0,
  KEY idx_cat (category_id, is_search, is_filter, is_sale)
) ENGINE=InnoDB;

-- SPU：标准化产品；JSON 存属性快照，用 vid 不用文本
CREATE TABLE spu (
  id           BIGINT PRIMARY KEY,
  merchant_id  BIGINT NOT NULL DEFAULT 1,     -- 商家维度（简化版归属）
  category_id  BIGINT NOT NULL,
  brand_id     BIGINT NOT NULL,
  title        VARCHAR(256) NOT NULL,
  key_props    JSON NOT NULL,                 -- {"brand":100,"model":5100}
  base_props   JSON,
  key_hash     CHAR(32) NOT NULL,             -- key_props 排序后 MD5
  status       TINYINT NOT NULL DEFAULT 1,
  publish_time DATETIME NOT NULL,
  UNIQUE KEY uk_spu (merchant_id, category_id, key_hash),  -- 带 merchant，防 B5
  KEY idx_cat_time (category_id, publish_time),
  KEY idx_keyhash (key_hash)                  -- 普通索引，归一候选检索
) ENGINE=InnoDB;

-- SKU：可售卖最小单元；price/stock 留关系列（交易强一致）
CREATE TABLE sku (
  id           BIGINT PRIMARY KEY,
  spu_id       BIGINT NOT NULL,
  sku_code     VARCHAR(64) NOT NULL,
  sale_props   JSON NOT NULL,                 -- {"color":1001,"size":2003} 存 vid
  sale_hash    CHAR(32) NOT NULL,
  price        DECIMAL(12,2) NOT NULL,        -- 关系列：交易强一致
  stock        INT NOT NULL,                  -- 关系列：扣库存强一致
  frozen_stock INT NOT NULL DEFAULT 0,        -- 热点预扣演进用
  status       TINYINT NOT NULL DEFAULT 1,
  UNIQUE KEY uk_sku (spu_id, sale_hash),
  KEY idx_spu (spu_id)
) ENGINE=InnoDB;

-- MySQL 倒排表：SearchEngine 的"小团队实现"
CREATE TABLE product_attr_index (
  attr_id        BIGINT NOT NULL,
  attr_value_id  BIGINT NOT NULL,
  product_type   TINYINT NOT NULL,            -- 1=spu 2=sku
  product_id     BIGINT NOT NULL,
  attr_value     VARCHAR(128),
  PRIMARY KEY (attr_id, attr_value_id, product_id)
) ENGINE=InnoDB;

-- 订单：交易侧只认 sku 强一致行
CREATE TABLE orders (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_no   VARCHAR(64) NOT NULL UNIQUE,     -- 幂等键 + Kafka 分区 key
  sku_id     BIGINT NOT NULL,
  quantity   INT NOT NULL,
  amount     DECIMAL(10,2) NOT NULL,
  status     VARCHAR(24) NOT NULL DEFAULT 'CREATED',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_status_created (status, created_at)
) ENGINE=InnoDB;
```

> 种子数据（deploy/mysql/seed.sql）：类目树（3 层、几十个叶子类目：男装>T恤、3C>手机、家电>冰箱…）、每个叶子类目的 attribute_meta（服饰：款号/颜色/尺码/材质；3C：品牌/型号/颜色/内存/上市时间）、枚举值字典（颜色：黑/白/蓝…，内存：128/256/512…）。

### 4.2 订单状态机（同 v2，订单引用 sku）

```
 CREATED ──支付──▶ PAID ──▶（加积分 + 销量榜 ZINCRBY）
    │
    ├─ 手动取消 / 超时(60s) ──▶ CANCELLED / TIMEOUT_CANCELLED ──▶ 回补库存
```

- 模拟支付网关：随机延迟 + 可配失败率；超时扫描器：ticker 扫 `status='CREATED' AND created_at < NOW()-60s`。
- 并发安全：状态迁移条件更新 `WHERE status='CREATED'`（bug B3 即破坏它）。
- 幂等：重复支付 no-op；支付已取消订单拒绝。

### 4.3 Kafka topic

| topic | 分区 | key | 来源 | 内容 |
|---|---|---|---|---|
| `product.changed` | 3 | 主键 id | Debezium | category/attribute_meta/spu/sku 的 INSERT/UPDATE（含来源表名） |
| `order.events` | 3 | order_no | order-service | ORDER_CREATED/PAID/CANCELLED/TIMEOUT_CANCELLED |

消息信封同 v2：`{event_id, event_type, key, occurred_at, payload}`；消费者逐条打印 `(topic, partition, offset, key)`。

### 4.4 ES 文档（SKU 粒度，属性拍平）

```json
{
  "sku_id": 5001, "spu_id": 500, "title": "iPhone 15 Pro 256G 黑色",
  "category_id": 12, "category_path": "0/5/12/",
  "price": 8999.0, "stock": 120, "status": 1,
  "props": { "color": "黑", "memory": "256G" }     // vid→文本映射后的可检索字段
}
```

- 索引消费者把 CDC 事件里的 JSON 属性**拍平成字段**（vid 用缓存的 enum_values 换成文本）。
- 筛选「颜色=黑 且 内存=256G」= ES `bool filter`；筛选栏计数 = `terms agg`（facet）。

### 4.5 Redis key

| key | 用途 | TTL |
|---|---|---|
| `spu:{id}` | 详情页正排缓存（spu+sku 列表） | 10min |
| `search:{engine}:{category_id}:{filter_hash}:{page}` | 搜索结果缓存 | 随机 5–10min |
| `stock_state:sku:{id}` | **库存三态**（有货/无货/紧张），不存精确 int | 5–15s |
| `stock:sku:{id}` | 热点库存预扣（演进 drill，Lua DECR） | 不过期 |
| `rank:sales` | **ZSET 销量榜**（支付时 ZINCRBY spu_id） | 不过期 |
| `rank:hotsearch` | **ZSET 热搜词**（搜索时 ZINCRBY 词） | 不过期 |
| `order:dedup:{order_no}` | 消费幂等 | 24h |
| `idempotency:{merchant_id}:{client_req_id}` | 发布幂等键 | 24h |
| `bf:spu` | 手搓布隆过滤器位图 | 不过期 |
| `points:user:{id}` | 积分 | 不过期 |

## 5. 核心业务逻辑（场景题的 Go 落地）

### 5.1 发布（product-service 写路径）

1. 商家选叶子类目 → 拉该类目全部 `attribute_meta`。
2. **幂等抢占**：`SET idempotency:{merchant}:{req_id} 1 NX EX 86400`（防 B4）。
3. **元数据驱动校验**：通用 `switch value_type`（enum 查 enum_values、range 查区间…），代码里永远不出现 `if name=="颜色"`。
4. **SPU 归一**：关键属性排序后 `key_hash=md5`，`INSERT ... ON DUPLICATE KEY`（唯一键带 merchant_id）。
5. **SKU 笛卡尔积**：销售属性组合递归生成；护栏：维度 ≤3、每维枚举 ≤50（防 B6）。
6. 落库 spu/sku + 布隆 `SETBIT bf:spu {id}`。
7. 发布后：binlog → CDC → Kafka → indexer → ES（最终一致，无需改发布代码）。

### 5.2 SearchEngine 接口（两个实现 + 能力协商）

```go
type SearchEngine interface {
    Search(ctx, Query) (Hits, error)   // 交集语义：category + filters + sort + page
    Facet(ctx, Query) (Facets, error)  // 只有 ES 实现支持实时聚合
}
// 实现一：MySQL product_attr_index —— 等值命中交集；不支持 facet、深翻页
// 实现二：ES —— bool filter + terms agg；分页用 search_after（防 E3）
```

- 筛选「颜色=黑 且 内存=256G」在 MySQL 实现是 `(attr_id,attr_value_id)` 两次等值命中求交集，主键天然覆盖；ES 实现是 term 查询。
- **能力协商**：深翻页/实时 facet 显式标注只有 ES 有（对应场景题追问 4）。

### 5.3 库存（三层语义，对应追问 2）

- **权威**：`sku.stock` 关系列，条件更新防超卖：`UPDATE sku SET stock=stock-? WHERE id=? AND stock>=?`。
- **展示**：详情页只展示三态（有货/无货/紧张），`stock_state:sku:{id}` 短 TTL——**绝不缓存精确 int**（防 B9）。
- **演进 drill**：压测暴露单行热点后，按答辩 2 实现 Redis Lua 预扣（`frozen_stock` 冻结 + `DECR` + 异步落库 + 对账），体会"强一致 → 最终一致"的取舍（B8 即该链路的 bug）。

### 5.4 Redis 挂掉三道防线（对应追问 3）

1. **本地缓存**：进程内 LRU（热点 spu/搜索结果），TTL 短于 Redis。
2. **熔断降级**：Redis 连续失败 N 次 → 熔断 → 快速失败/兜底数据。
3. **限流**：令牌桶把打到 MySQL 的 QPS 压到阈值内，超出 429。
4. 配套：singleflight 防击穿、负缓存防穿透、TTL 随机抖动防雪崩。

### 5.5 ZSET 榜单

- `rank:sales`：order-worker 收到 ORDER_PAID → `ZINCRBY rank:sales {qty} spu:{id}`；热销榜 API → `ZREVRANGE 0 99 WITHSCORES`。
- `rank:hotsearch`：search-service 每次查询 → `ZINCRBY rank:hotsearch 1 {词}`；热搜榜 API → top 10。
- 教学点：ZSET 跳表、score 与 member、top-N 与范围查询（`ZREVRANGEBYSCORE`）。

## 6. 数据流

### 链路 A（商品上架 → 可搜索，CDC 模式）

```
发布 → product-service(校验/SKU/幂等) → MySQL(spu/sku JSON)
→ binlog → Debezium → Kafka(product.changed)
→ indexer：spu/sku/meta 事件 → 缓存 spu 与枚举映射 → 拍平 JSON
→ ES(sku 文档) + product_attr_index 表
搜索 → search-service：Redis 搜索缓存 → 命中返回
       └ 未命中 → SearchEngine(ES/MySQL) → 回写缓存 + 热搜词 ZINCRBY
详情 → product-service：布隆(不存在404) → Redis spu:{id} → MySQL
```

### 链路 B（订单，直发模式 + 状态机）

```
order-service: 下单 → 写 orders + 发 ORDER_CREATED
  ├─ 支付 → 条件更新 PAID + 发 ORDER_PAID
  ├─ 超时/手动取消 → 条件更新 + 发事件
order-worker:
  ├─ ORDER_CREATED → 扣库存(sku 条件更新) + 写三态缓存
  ├─ ORDER_CANCELLED/TIMEOUT_CANCELLED → 回补库存 + 写三态缓存
  └─ ORDER_PAID → 销量榜 ZINCRBY + 积分 + 通知
```

## 7. 数据生成与规模（属性驱动，真实实体）

- **种子**：类目树 + attribute_meta + 枚举值（deploy/mysql/seed.sql，几十个叶子类目）。
- **SPU/SKU 生成**：按类目模板（3C>手机：品牌×型号×颜色×内存；服饰：款号×颜色×尺码）生成真实感标题；SKU 由销售属性笛卡尔积产生。**默认目标（内存友好）**：SPU 10 万、SKU 50 万、orders 100 万；`--count` 可调，上限见 §13 内存预算。
- **zipf 分布**：订单/搜索按 zipf 打热 SKU——天然制造热 Key（R6）、分区倾斜（B1）、单行库存热点（追问 2 的靶子）。
- **突发流量**：`burst --base 100 --peak 2000 --duration 30s`。
- **路由报告**：key→partition 落点统计；kafka-inspector 看 lag/分区分布。

## 8. Redis 手搓布隆过滤器

- 原理（docs/learning/bloom-filter.md）：m 位 + k 哈希，`p ≈ (1-e^(-kn/m))^k`；只误报不漏报、不可删。
- 实现：Go 手写双重哈希（fnv+murmur），位图落 Redis bitmap（`SETBIT/GETBIT bf:spu`），不用 RedisBloom 模块。
- 用途：`GET /spu/{id}` 先查布隆，不存在直接 404；发布时 SETBIT。
- 参数选择（m/k 按 10^6 商品、1% 误判反推）写进代码注释；误设 = bug R8。

## 9. MySQL 性能优化 Lab（百万行，真实实体）

数据：SKU 50 万、orders 100 万（默认，可调）。练习项（每项对应 bug 卡）：

1. 无索引全表扫 vs 建索引——`EXPLAIN` 对比
2. 复合索引最左前缀（`idx_status_created`）
3. 深分页 `LIMIT 1000000,10` → 游标/延迟关联
4. 回表 vs 覆盖索引（`SELECT sku_id` vs `SELECT *`）
5. 隐式类型转换索引失效（`WHERE varchar = 123`）
6. **JSON_EXTRACT 直接 where → 慢查询**（M6，场景题坑 1）→ 对比倒排表/ES，这就是"为什么搜索交给 ES"
7. `LIKE '%x%'` 索引失效
8. 批量插入优化（多行 INSERT、关 autocommit）

## 10. Go 服务技术要点

- 元数据驱动校验（通用 switch + enum_values）
- SKU 笛卡尔积递归生成 + 护栏
- SearchEngine interface 多态（两实现 + 能力协商）
- goroutine 消费循环、context 优雅退出、手动提交 offset、幂等消费
- Redis Lua 脚本（预扣演进）、singleflight、熔断、令牌桶限流、本地 LRU
- 条件更新（乐观锁）防并发竞态

## 11. 可靠性设计（「正确版」基线）

- **Kafka**：acks=all + 幂等；手动提交 offset；幂等去重；毒丸进死信。
- **Redis**：Cache-Aside；穿透→布隆+负缓存；击穿→singleflight；雪崩→随机 TTL；一致性→先更新 DB 再删缓存；库存只缓存三态。
- **MySQL**：关系列扛强一致；JSON 只存不查；索引设计；条件更新。
- **服务**：发布幂等键、归属校验（merchant_id 拼进 WHERE）、优雅退出、连接池上限、结构化日志。

## 12. 故障清单（50 个，五要素）

> 用法：按「注入」改代码/配置 → 观察「症状」→ 按「排查」线索定位 → 自己修复 → 对照「根因/修复」核对。

### Kafka 类（8）

**K1 消息丢失**：注入 `acks` 改 0｜症状 kill broker 部分事件下游没收到｜排查 对比订单数 vs 处理数｜根因 acks=0 不等确认｜修复 `acks=all` + `min.insync.replicas≥2`
**K2 消息重复**：注入 先处理业务后提交 offset + 中途崩溃｜症状 同一订单扣两次库存｜排查 同一 order_no 处理多次｜根因 at-least-once｜修复 `SETNX order:dedup` 幂等
**K3 消息乱序**：注入 key 随机/不设｜症状 同订单 CREATED/CANCELLED 颠倒｜排查 同订单跨分区｜根因 key 不稳定路由到不同分区｜修复 用 order_no 做 key
**K4 消息堆积**：注入 消费加 sleep + 分区设 1｜症状 lag 持续涨｜排查 kafka-inspector 看 lag｜根因 消费速率 < 生产速率｜修复 去慢逻辑/加分区加实例
**K5 rebalance 风暴**：注入 `max.poll.interval.ms=3s` + 处理慢｜症状 反复 joining group｜排查 日志频繁 rebalance｜根因 poll 超时被踢出组｜修复 调大间隔/加快处理
**K6 毒丸消息**：注入 混入反序列化失败消息｜症状 该分区卡死不再前进｜排查 日志停在同一条报错｜根因 坏消息未隔离｜修复 死信 topic 隔离
**K7 offset 重置**：注入 改 group.id / `auto.offset.reset=earliest`｜症状 历史重放或丢老消息｜排查 对比 group offset 与 topic 位置｜根因 group id 是 offset 存储键｜修复 不随意改 group id
**K8 未 flush**：注入 退出前不 `producer.Close()`｜症状 退出瞬间丢最后几条｜排查 末尾消息缺失｜根因 缓冲未刷盘｜修复 `defer producer.Close()`

### Redis 类（9）

**R1 缓存穿透**：注入 去布隆/负缓存，狂查不存在 ID｜症状 DB 被打爆｜排查 Redis 命中率骤降｜根因 不存在 key 无防护｜修复 布隆 + 负缓存
**R2 缓存击穿**：注入 热点 spu 过期且去 singleflight｜症状 过期瞬间并发打 MySQL｜排查 DB 慢查询时间点尖峰｜根因 热点过期无互斥｜修复 singleflight/互斥锁
**R3 缓存雪崩**：注入 全部 TTL 相同｜症状 齐过期后端打垮｜排查 过期时间戳集中｜根因 无随机化｜修复 TTL 随机偏移
**R4 缓存与 DB 不一致**：注入 先删缓存再更新 DB + 并发读｜症状 偶发读到旧值｜排查 回填旧值时序｜根因 删/改顺序交错｜修复 先更新 DB 再删缓存
**R5 Big Key**：注入 某 spu 详情塞几 MB｜症状 GET/SET 慢、阻塞单线程｜排查 `--bigkeys`/slowlog｜根因 大 value 开销｜修复 拆分/压缩
**R6 热 Key**：注入 zipf 狂打同一 SKU 详情｜症状 单 key CPU 打满｜排查 monitor 热点 QPS｜根因 单点高频｜修复 本地缓存/key 拆分
**R7 内存打满**：注入 关 TTL + 小 maxmemory + allkeys-lru｜症状 热点被淘汰命中率暴跌｜排查 `info memory`/evicted_keys｜根因 无 TTL + 策略不当｜修复 合理 TTL/分优先级
**R8 布隆参数错误**：注入 m 太小/k 不对（按 10 万设计灌 100 万）｜症状 误判率飙升穿透 DB｜排查 位填充率近 100%｜根因 m/k 与数据量不匹配｜修复 按 n、p 反推 m/k
**R9 本地缓存失效不一致**：注入 上本地 LRU 后改价/下架不广播｜症状 多实例返回新旧不同价格｜排查 轮询实例结果不同｜根因 进程内缓存无失效｜修复 CDC 广播失效 / 本地只缓存静态字段

### CDC 类（4）

**C1 CDC 断流**：注入 stop kafka-connect｜症状 新商品搜不到无感知｜排查 Connect 状态、topic 无新消息｜根因 无告警/自愈｜修复 健康检查 + 告警 + 重启
**C2 位点丢失**：注入 重置 Debezium offset / 改 connector 名｜症状 全量重放或跳跃缺失｜排查 ES 文档数 vs MySQL 行数｜根因 offset 是进度唯一依据｜修复 勿清 offset、重建前快照
**C3 schema 不匹配**：注入 `ALTER TABLE` 加字段｜症状 connector 报错停｜排查 Connect schema 日志｜根因 schema 历史未跟上｜修复 schema 演进配置/重建 connector
**C4 快照/增量切换丢数据**：注入 初始快照期间持续写入｜症状 快照窗口写入缺失｜排查 窗口内写入未进 ES｜根因 切换存在窗口｜修复 一致性快照策略 + 位点衔接

### Go 并发类（5）

**G1 goroutine 泄漏**：注入 每消息 go 一个无 context｜症状 goroutine/内存涨到 OOM｜排查 pprof 递增｜根因 无退出机制｜修复 context + select / worker pool
**G2 map 并发写 panic**：注入 全局 map 多 goroutine 写｜症状 `concurrent map writes`｜排查 panic 栈｜根因 map 非并发安全｜修复 sync.Map/锁/channel
**G3 未优雅退出**：注入 SIGTERM 直接 os.Exit｜症状 发布时消息丢/重｜排查 offset 未提交｜根因 未收尾｜修复 NotifyContext + 提交后再退
**G4 连接池耗尽**：注入 MaxOpenConns=1｜症状 高并发超时排队｜排查 db.Stats 打满｜根因 池太小｜修复 合理配置 + 超时
**G5 响应体未关闭**：注入 不 defer Body.Close()｜症状 连接泄漏端口耗尽｜排查 netstat 连接增长｜根因 连接无法复用｜修复 defer Close + 读空 body

### ES 类（3）

**E1 索引不一致**：注入 indexer 处理一半 kill -9｜症状 搜索漏结果｜排查 ES count vs MySQL count｜根因 崩溃漏写未重放｜修复 处理+写+提交 offset 顺序正确幂等
**E2 刷新延迟**：注入 refresh_interval 调 30s｜症状 刚上架搜不到几秒后才出现｜排查 写入后立即查 vs 稍后查｜根因 refresh 后才可搜｜修复 理解 refresh 语义
**E3 深翻页**：注入 from+size 翻过 10000｜症状 直接报错/延迟随页线性涨｜排查 ES `Result window is too large`｜根因 from+size 全捞再截断｜修复 search_after 游标

### 环境搭建类（6）

**S1 Kafka advertised.listeners 配错**：注入 容器内外地址不一致｜症状 客户端连不上超时｜排查 客户端 refused 指向 localhost｜根因 advertised 是"告诉客户端从哪连"｜修复 内外两个 listener
**S2 ES bootstrap 失败**：注入 不调 vm.max_map_count｜症状 ES 启动即退｜排查 日志 `vm.max_map_count too low`｜根因 内核参数太小｜修复 sysctl 262144
**S3 MySQL binlog 未开**：注入 STATEMENT 格式/未开/server-id 缺｜症状 connector 起不来｜排查 Connect 要求 ROW｜根因 CDC 依赖 ROW binlog｜修复 ROW + server-id + 复制账号
**S4 Connector 配置错**：注入 连接串/账号错或 server-id 冲突｜症状 FAILED 循环｜排查 `/connectors/{name}/status`｜根因 配置不匹配｜修复 核对配置、server-id 唯一
**S5 Docker 网络不通**：注入 服务间用 localhost｜症状 连接拒绝｜排查 容器内 curl 探测｜根因 localhost 指容器自己｜修复 用服务名互访
**S6 Redis 持久化缺失**：注入 无 RDB/AOF 或配错｜症状 重启缓存/榜单/布隆全丢｜排查 重启前后 DBSIZE｜根因 未配持久化｜修复 按场景配 RDB+AOF

### MySQL 性能类（6）

**M1 缺索引慢查询**：注入 百万行按无索引列查｜症状 毫秒变秒级｜排查 EXPLAIN type=ALL｜根因 全表扫｜修复 建索引（注意最左前缀）
**M2 深分页**：注入 LIMIT 1000000,10｜症状 越翻越慢｜排查 EXPLAIN 扫描行数=offset+limit｜根因 跳过百万行｜修复 游标/延迟关联
**M3 回表过多**：注入 SELECT * 非覆盖｜症状 索引快但整体慢｜排查 EXPLAIN 无 Using index｜根因 二级索引回聚簇｜修复 覆盖索引/减列
**M4 隐式类型转换**：注入 字符串列用数字查｜症状 索引失效全表扫｜排查 EXPLAIN 索引未生效｜根因 列上隐式转换｜修复 类型一致
**M5 前导通配符**：注入 LIKE '%x%'｜症状 全表扫秒级｜排查 索引失效｜根因 前导模糊无法用 B+Tree｜修复 交给 ES 倒排（链路 A 价值）
**M6 JSON_EXTRACT 直接 where**：注入 `WHERE JSON_EXTRACT(sale_props,'$.color')=...`｜症状 筛选慢查询｜排查 EXPLAIN 全表扫、无索引｜根因 JSON 只存不查｜修复 倒排层（product_attr_index/ES）或 generated column

### 业务/架构类（3）

**B1 Kafka 分区倾斜**：注入 高 zipf 造数｜症状 单分区 lag 暴涨其余空载｜排查 inspector 看每分区消息数｜根因 key 分布不均｜修复 key 加盐/二次路由
**B2 超时扫描器失效**：注入 注释 ticker / 条件写错｜症状 超时订单永远 CREATED 库存被占｜排查 滞留 CREATED 数增长｜根因 后台任务失效无监控｜修复 恢复 + 滞留数告警
**B3 支付/取消竞态**：注入 状态迁移改非条件更新｜症状 已取消被置 PAID 或反之｜排查 并发 pay/cancel 状态异常｜根因 无乐观锁｜修复 条件更新 + 检查 affected rows

### 商品中心类（6，新增）

**B4 发布接口无幂等键**：注入 去掉 client_req_id 的 SETNX 抢占｜症状 网络重试生成两套 SKU｜排查 同请求两次 → sku 行翻倍｜根因 重复提交各执行一次｜修复 `SET idempotency:{merchant}:{req_id} NX EX 86400`
**B5 唯一键焊死撞键**：注入 spu 唯一键去掉 merchant_id｜症状 第二家发布同款撞键失败｜排查 duplicate entry；两商家 key_hash 同｜根因 标品与商家货焊死｜修复 唯一键带 merchant_id；平台归一用普通索引+异步归并
**B6 笛卡尔积爆炸**：注入 去掉发布护栏｜症状 20×15×10=3000 SKU 拖垮链路｜排查 单 spu 的 sku 行数暴涨｜根因 组合数无上限｜修复 维度≤3、每维≤50 护栏
**B7 枚举文本改名漏数**：注入 sale_props 存文本 + 运营改名｜症状 筛新名漏历史 SKU｜排查 筛选结果骤减；ES term 不匹配｜根因 文本改名即失联｜修复 存 vid 文本仅展示；改名只改 enum_values
**B8 热点预扣漂移**：注入 预扣异步落库失败/关对账｜症状 超卖或库存锁死｜排查 Redis 已扣 vs MySQL frozen 比对｜根因 乐观放行无对账｜修复 定期对账 + 下单二次校验 + 超卖拒绝退款
**B9 详情页缓存精确库存**：注入 缓存精确 int 长 TTL｜症状 详情显示旧库存，下单失败｜排查 详情数字 vs MySQL 实际｜根因 强一致字段做长缓存｜修复 只缓存三态短 TTL；精确值查 MySQL

## 13. 硬件极限与内存预算

**原则：不榨干 32G**——本机还有其他进程占用内存，整个堆栈默认预算控制在 **10–12GB**，留 20GB+ 余量。

| 组件 | 内存配置 | 说明 |
|---|---|---|
| ES | JVM heap 1g（`-Xms1g -Xmx1g`） | 50 万文档单节点足够；加量可调 2g |
| MySQL | `innodb_buffer_pool_size=2G` | 百万行数据足够 |
| Kafka | `KAFKA_HEAP_OPTS=-Xmx1g` | 单 broker 百万消息够 |
| Connect+Debezium | `-Xmx1g` | |
| Redis | `maxmemory 512mb` | 缓存+榜单+位图 |
| Go 服务 ×7 | 各 ~150MB | |
| **合计** | **约 10–12GB** | |

各组件实际极限（超出预算才碰得到）：MySQL 千万行、Kafka 百万消息、ES 单节点百万文档、Redis 百万 key、数千 QPS。默认数据目标（SPU 10 万 / SKU 50 万 / orders 100 万）下全部组件都在舒适区，且索引失效/击穿/倾斜/rebalance/单行热点等性能现象**照常真实显现**。

模拟不了：跨机房网络分区、真多机物理资源隔离（k3s 阶段补编排逻辑）。

## 14. 参数手册约定

每个参数/代码块注释模板：`做什么 / 实现的需求 / 防止的问题(bug 编号) / 改错的后果`。实现阶段所有代码与配置按此编写；docs/learning/parameters.md 维护「参数速查表」。

## 15. 学习材料计划（docs/learning/）

| 材料 | 内容 |
|---|---|
| `product-center.md` | 场景题精讲：逐段对应到 demo 代码与表（"这个 DDL 为什么这样"） |
| `bloom-filter.md` | 布隆原理推导 + Go 手搓逐步讲解 |
| `inverted-index.md` | 倒排索引原理（分词/倒排表/相关性）+ ES 映射 |
| `redis-zset.md` | ZSET 跳表原理 + 榜单实战 |
| `kafka-quickstart.md` / `es-quickstart.md` | 中间件上手（用户首次使用，手把手） |
| `prometheus-grafana.md` | 监控栈（阶段二） |
| `parameters.md` | 参数速查表（参数→需求→bug 编号） |

## 16. 部署

### 阶段一：本地 Docker Compose

- mysql、kafka(KRaft)、kafka-connect(debezium)、elasticsearch、redis、5 个 Go 服务、load-gen/kafka-inspector、chaos.sh。
- **第一步拉镜像**（用户尚无）：kafka、kafka-connect+debezium、elasticsearch、grafana、prometheus，约 3–5GB。
- MySQL：init.sql 建表 + seed.sql 种子（类目/属性 meta/枚举值）；my.cnf 开 binlog ROW + server-id；复制账号。

### 阶段二：k3s + 可观测（单独出 spec）

## 17. 验证与演练方法

1. **正常流**：发布商品（走元数据校验+SKU 生成）→ CDC → 可搜索（facet 筛选正确）→ 下单 → 支付 → 扣库存/销量榜/积分。
2. **状态机**：超时自动取消、支付失败重试、重复支付幂等、库存回补。
3. **rebalance**：order-worker 2 实例 kill 一个。
4. **混沌**：chaos.sh 随机杀服务。
5. **压测**：burst 突发流量 → 堆积/倾斜/连接池。
6. **性能 lab**：百万行 EXPLAIN 专项（含 JSON_EXTRACT vs 倒排对比）。
7. **演进 drill**：热点 SKU 压出单行瓶颈 → 实现 Redis Lua 预扣（frozen_stock+对账）。
8. **故障演练**：50 个 bug 挑着注入 → 观察 → 排查 → 修复 → 对照核对。

## 18. 简化内容与扩展路线

本 demo 对场景题做了如下简化（实现时按简化版落地；扩展路线写清，想升级哪块随时可做）：

| # | 简化点 | demo 简化版 | 扩展路线（场景题完整版） |
|---|---|---|---|
| 1 | 三层 SPU 模型 | 两层 spu/sku + merchant_id 列，唯一键 `(merchant_id, category_id, key_hash)` | platform_spu/merchant_spu/merchant_sku 三层拆分；key_hash 降普通索引；异步归并任务 + 属性相似度二级确认（Jaccard）；拆分/合并运营工具 |
| 2 | 归属鉴权 | 请求头带 merchant_id，写操作拼进 WHERE | JWT + RBAC 白名单；运营「代商家改价」授权接口 + 审计日志 |
| 3 | 热点库存预扣 | 默认条件更新防超卖；预扣（frozen_stock + Lua + 对账）为演进 drill | 预扣作为正式链路 + 冻结量动态调参 + 对账任务组件化 + 超卖拒绝退款链路 |
| 4 | 订单超时 | ticker 轮询扫描器 | Kafka 延迟消息 / 时间轮 / 独立调度服务 |
| 5 | 支付 | 模拟网关（随机延迟 + 可配失败率） | 真实第三方网关、支付回调、渠道路由、对账 |
| 6 | 分库分表 | 单库单表（本 demo 数据量无需分片） | shard key=spu_id 分片 + 「类目→spu_id」二级索引表路由（追问 4B） |
| 7 | 部署 | Docker Compose 单机 | k3s 多副本 + HPA + 真多机（阶段二） |

## 19. 待办 / 后续

- 阶段一落地后，单独出阶段二（k3s + Prometheus/Grafana）spec。
- 学习材料随实现进度撰写。
