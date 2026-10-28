# 计划 1：中间件基础设施搭建 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 一条命令拉起全套中间件（MySQL/Kafka/ES/Redis/Debezium，含建表与种子数据、内存预算），并验证「MySQL 变更 → Debezium → Kafka(product.changed)」CDC 链路端到端打通。

**Architecture:** Docker Compose 编排 5 个中间件容器；MySQL 开 binlog(ROW) 并预置 6 张表 + 种子类目/属性元数据；Kafka 单节点 KRaft，容器内/宿主机双 listener（教学 S1）；Debezium Connect 用 RegexRouter 把 4 张表的变更统一路由到 `product.changed`；ES 单节点 heap 1g；Redis 512mb + RDB/AOF 双持久化。开发期 Go 服务跑宿主机，中间件跑 Docker。

**Tech Stack:** Docker Compose、MySQL 8.0.36、apache/kafka:4.2.0（KRaft，本机已有）、debezium/connect:2.5、Elasticsearch 8.13.4、redis:7.4.9-alpine（本机已有）

## 执行期调整记录（2026-10-28 环境探测后）

1. **镜像换本机已有**（省约 1.4GB 下载）：`bitnami/kafka:3.6` → `apache/kafka:4.2.0`（官方镜像，KRaft，4.x 已彻底移除 ZooKeeper）；`redis:7` → `redis:7.4.9-alpine`。
2. **Kafka CLI 无 `.sh` 后缀**（Kafka 4.x 完成 KIP-884）：全文 `kafka-topics.sh`→`kafka-topics`、`kafka-console-consumer.sh`→`kafka-console-consumer`。
3. **端口避让本机占用**：探测发现 3306=本机原生 mysqld.exe（Windows 服务）、6379=其它项目 Redis 容器 → demo 改映射 **MySQL 13306:3306、Redis 16379:6379**（容器内端口不变，服务间互访用服务名不受影响；Go 服务在宿主机侧连 13306/16379）。9200/8083/9094 空闲照旧。
4. `mysql:8.0` 钉死为 `mysql:8.0.36`。

## 全局约束（全项目 5 个计划通用，逐条来自 spec）

- 所有 Go 服务**独立 module + 独立 Dockerfile**，不允许共用一个 go.mod（用户硬性要求）。
- 代码/配置注释约定：每块注释写「做什么 / 实现的需求 / 防止的问题(bug 编号) / 改错的后果」。
- 内存预算：全栈 ≤12GB——ES heap 1g、MySQL buffer 2G、Kafka 1g、Connect 1g、Redis 512mb。
- 端口（宿主侧）：MySQL **13306**、Redis **16379**（本机 3306/6379 被原生 mysqld 与其它项目占用）、Kafka 9094（容器内 9092）、Connect 8083、ES 9200；Go 服务 8080/8081/8082。容器内端口不变。
- Kafka topic：`product.changed`、`order.events`，均 3 分区（`auto.create.topics.enable=false`，显式创建）。
- MySQL：库 `demo`；凭据**三处一致**（`.env` 记录 / `init.sql` 建账号 / connector JSON 连接）——学习项目明文硬编码，改密码三处同步，生产换 secrets 注入。
- 默认数据量：SPU 10 万 / SKU 50 万 / orders 100 万（load-gen 可调，计划 2 落地）。
- 开发期 Go 服务用 `go run` 跑宿主机；镜像打包在计划 5。
- Git：每个任务一次 commit，提交信息末尾带 `Co-Authored-By: Claude Code <noreply@anthropic.com>`。
- 注释语言：中文。Go 版本 1.26.4；Kafka 客户端选纯 Go 的 `segmentio/kafka-go`（禁 cgo，Windows 友好）。
- 网络说明：本机代理工具需开启时才能拉镜像/推 GitHub（git push 曾因代理关闭失败）。

---

### Task 1: Docker Compose 骨架 + 环境变量

**Files:**
- Create: `deploy/.env`
- Create: `deploy/docker-compose.yml`

**Interfaces:**
- Produces: 容器名 `demo-mysql`/`demo-kafka`/`demo-connect`/`demo-es`/`demo-redis`（后续所有任务、脚本、chaos 都按此名引用）；Compose 默认网络（容器间互访用服务名）。

- [ ] **Step 1: 写 deploy/.env**

```bash
# deploy/.env —— 凭据清单（学习项目：值硬编码在 init.sql 与 connector JSON 中，
# 此处集中记录，改密码时三处同步；生产应换 secrets 注入）
MYSQL_ROOT_PASSWORD=root123456
MYSQL_DEBEZIUM_PASSWORD=dbz123456
```

- [ ] **Step 2: 写 deploy/docker-compose.yml**

```yaml
# 中间件基础设施编排（Go 服务在计划 2-4 以宿主机 go run 方式开发，计划 5 才进 compose）
# 内存预算说明：全栈 ≤12GB，本机 32G 还要留 20G+ 给其他进程（spec §13）
services:
  mysql:
    image: mysql:8.0
    container_name: demo-mysql
    env_file: .env
    environment:
      MYSQL_DATABASE: demo
    ports:
      - "3306:3306"
    command: >
      --server-id=1
      --binlog-format=ROW
      --binlog-row-image=FULL
      --log-bin=mysql-bin
      --innodb-buffer-pool-size=2147483648
    # ↑ server-id=1 + binlog ROW：CDC 捕获的前提（改错=bug S3）
    # ↑ buffer pool 2G：内存预算内的性能底线（改错=MySQL 性能类 bug M1-M6 变本加厉）
    volumes:
      - ./mysql/init.sql:/docker-entrypoint-initdb.d/01-init.sql:ro
      - ./mysql/seed.sql:/docker-entrypoint-initdb.d/02-seed.sql:ro
      - mysql-data:/var/lib/mysql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost", "-uroot", "-proot123456"]
      interval: 5s
      timeout: 3s
      retries: 30

  kafka:
    image: bitnami/kafka:3.6
    container_name: demo-kafka
    ports:
      - "9094:9094"          # 宿主机访问走 9094（EXTERNAL）
    environment:
      # KRaft 单节点：同时是 broker 和 controller（生产应拆开，见 docs/diagrams 架构图）
      - KAFKA_CFG_NODE_ID=0
      - KAFKA_CFG_PROCESS_ROLES=controller,broker
      - KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka:9093
      - KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER
      # 双 listener：容器内服务用 kafka:9092，宿主机工具用 localhost:9094
      # 二者地址不能互换——配错即 bug S1（advertised.listeners 是最经典的 Kafka 部署坑）
      - KAFKA_CFG_LISTENERS=INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:9094
      - KAFKA_CFG_ADVERTISED_LISTENERS=INTERNAL://kafka:9092,EXTERNAL://localhost:9094
      - KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP=INTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT
      - KAFKA_CFG_INTER_BROKER_LISTENER_NAME=INTERNAL
      # 关自动建 topic：强制显式创建（分区数可控，教学 kafka-topics 用法）
      - KAFKA_CFG_AUTO_CREATE_TOPICS_ENABLE=false
      - KAFKA_HEAP_OPTS=-Xms512m -Xmx1g
    healthcheck:
      test: ["CMD-SHELL", "kafka-topics.sh --bootstrap-server localhost:9092 --list >/dev/null 2>&1 || exit 1"]
      interval: 10s
      timeout: 10s
      retries: 30

  connect:
    image: debezium/connect:2.5
    container_name: demo-connect
    ports:
      - "8083:8083"          # Kafka Connect REST 管理端口
    environment:
      - BOOTSTRAP_SERVERS=kafka:9092
      - GROUP_ID=connect-cluster
      - CONFIG_STORAGE_TOPIC=connect-configs
      - OFFSET_STORAGE_TOPIC=connect-offsets
      - STATUS_STORAGE_TOPIC=connect-status
      # 单节点 Kafka，副本因子只能 1（生产是 3，这是 demo 的显式降级点）
      - CONFIG_STORAGE_REPLICATION_FACTOR=1
      - OFFSET_STORAGE_REPLICATION_FACTOR=1
      - STATUS_STORAGE_REPLICATION_FACTOR=1
      - KAFKA_HEAP_OPTS=-Xmx1g
    depends_on:
      kafka: { condition: service_healthy }
      mysql: { condition: service_healthy }

  es:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.13.4
    container_name: demo-es
    ports:
      - "9200:9200"
    environment:
      - discovery.type=single-node
      - xpack.security.enabled=false   # 学习环境关安全认证，生产必须开
      - ES_JAVA_OPTS=-Xms1g -Xmx1g
    volumes:
      - es-data:/usr/share/elasticsearch/data
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9200 || exit 1"]
      interval: 10s
      timeout: 5s
      retries: 30

  redis:
    image: redis:7
    container_name: demo-redis
    ports:
      - "6379:6379"
    command: >
      redis-server
      --maxmemory 512mb
      --maxmemory-policy allkeys-lru
      --appendonly yes
      --save 900 1 --save 300 10
    # ↑ 内存上限 + 淘汰策略：防内存打满（bug R7 的正确基线）
    # ↑ AOF + RDB 双开：重启不丢榜单/位图/幂等键（bug S6 的正确基线）
    volumes:
      - redis-data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 30

volumes:
  mysql-data:
  es-data:
  redis-data:
```

- [ ] **Step 3: 校验 compose 语法**

Run: `cd deploy && docker compose config -q`
Expected: 无输出（语法合法），退出码 0。

- [ ] **Step 4: 拉取镜像（约 3–5GB，需代理通畅）**

Run: `cd deploy && docker compose pull`
Expected: 5 个镜像 Pull complete。若卡住/失败 → 检查本机代理是否开启（本项目 git push 曾因代理关闭失败，镜像拉取同理）。

- [ ] **Step 5: Commit**

```bash
git add deploy/.env deploy/docker-compose.yml
git commit -m "feat(infra): docker-compose 编排 5 个中间件（内存预算版）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: MySQL 建表 + 种子数据（类目/属性元数据/枚举）

**Files:**
- Create: `deploy/mysql/init.sql`
- Create: `deploy/mysql/seed.sql`

**Interfaces:**
- Produces: 库 `demo` 内 6 张表（DDL 与 spec §4.1 完全一致）——`category`/`attribute_meta`/`spu`/`sku`/`orders`/`product_attr_index`；复制账号 `debezium`；种子数据（16 个类目、41 条属性元数据、枚举 vid 全局唯一）。

- [ ] **Step 1: 写 deploy/mysql/init.sql**

```sql
-- 建表 + Debezium 复制账号（只在数据卷首次初始化时执行）
-- 表结构 = spec §4.1，逐列注释"为什么"：

CREATE TABLE category (
  id        BIGINT PRIMARY KEY,
  parent_id BIGINT NOT NULL DEFAULT 0,
  name      VARCHAR(128) NOT NULL,
  level     TINYINT NOT NULL,              -- 1/2/3，层深有限
  path      VARCHAR(256) NOT NULL,         -- "0/1/11/111/"，前缀匹配查子树，避免递归 CTE
  is_leaf   TINYINT NOT NULL DEFAULT 0,    -- 只有叶子类目能挂商品
  status    TINYINT NOT NULL DEFAULT 1,
  KEY idx_parent (parent_id),
  KEY idx_path (path)                      -- 前缀索引：WHERE path LIKE '0/1/%' 走索引
) ENGINE=InnoDB;

-- 属性元数据：整题的"驱动引擎"。新增商品类型=加类目+配属性，不改核心代码
CREATE TABLE attribute_meta (
  id          BIGINT PRIMARY KEY,
  category_id BIGINT NOT NULL,             -- 绑定类目；0=全局通用属性
  name        VARCHAR(64) NOT NULL,
  value_type  ENUM('int','string','decimal','enum','range','date') NOT NULL,
  is_key      TINYINT NOT NULL DEFAULT 0,  -- 关键属性→SPU 归一（决定"是不是同一款"）
  is_sale     TINYINT NOT NULL DEFAULT 0,  -- 销售属性→SKU 笛卡尔积（下单选规格）
  is_search   TINYINT NOT NULL DEFAULT 0,  -- 进倒排（ES/倒排表）
  is_filter   TINYINT NOT NULL DEFAULT 0,  -- 做筛选 facet
  is_multi    TINYINT NOT NULL DEFAULT 0,
  required    TINYINT NOT NULL DEFAULT 0,
  enum_values JSON,                        -- 枚举字典 [{"vid":1001,"text":"黑"}]；vid 稳定、文本可改（bug B7）
  unit        VARCHAR(16),                 -- 单位：GB/cm/天
  sort_order  INT NOT NULL DEFAULT 0,
  version     BIGINT NOT NULL DEFAULT 0,   -- 属性定义版本，枚举改名后历史快照回刷用
  KEY idx_cat (category_id, is_search, is_filter, is_sale)
) ENGINE=InnoDB;

-- SPU：标准化产品。JSON 存属性快照（只存不查，bug M6）；price 不在这层（在 SKU）
CREATE TABLE spu (
  id           BIGINT PRIMARY KEY,
  merchant_id  BIGINT NOT NULL DEFAULT 1,  -- 商家维度；唯一键带 merchant 防 bug B5（多商家卖同款撞键）
  category_id  BIGINT NOT NULL,
  brand_id     BIGINT NOT NULL DEFAULT 0,
  title        VARCHAR(256) NOT NULL,
  key_props    JSON NOT NULL,              -- {"品牌":3001,"型号":"iPhone 15 Pro"}，值用 vid
  base_props   JSON,                       -- 普通描述属性快照
  key_hash     CHAR(32) NOT NULL,          -- key_props 排序后 MD5，同款判定的候选键
  status       TINYINT NOT NULL DEFAULT 1,
  publish_time DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uk_spu (merchant_id, category_id, key_hash),
  KEY idx_cat_time (category_id, publish_time),
  KEY idx_keyhash (key_hash)               -- 普通索引：归一候选检索，不是约束（三层模型扩展点 §18-1）
) ENGINE=InnoDB;

-- SKU：可售卖最小单元。price/stock 留关系列=交易强一致（塞 JSON 就废了）
CREATE TABLE sku (
  id           BIGINT PRIMARY KEY,
  spu_id       BIGINT NOT NULL,
  sku_code     VARCHAR(64) NOT NULL,
  sale_props   JSON NOT NULL,              -- {"颜色":1001,"尺码":4002}，值用 vid（bug B7）
  sale_hash    CHAR(32) NOT NULL,
  price        DECIMAL(12,2) NOT NULL,     -- 关系列：交易强一致
  stock        INT NOT NULL,               -- 关系列：扣库存行级锁
  frozen_stock INT NOT NULL DEFAULT 0,     -- 热点 Redis 预扣演进用（§5.3、§18-3）
  status       TINYINT NOT NULL DEFAULT 1,
  UNIQUE KEY uk_sku (spu_id, sale_hash),   -- 同 SPU 下规格组合唯一
  KEY idx_spu (spu_id)
) ENGINE=InnoDB;

-- 订单：交易侧只认 sku 强一致行（场景题：交易不碰 JSON）
CREATE TABLE orders (
  id         BIGINT AUTO_INCREMENT PRIMARY KEY,
  order_no   VARCHAR(64) NOT NULL UNIQUE,  -- 幂等键 + Kafka 分区 key（同订单事件同分区=有序，bug K3）
  sku_id     BIGINT NOT NULL,
  quantity   INT NOT NULL,
  amount     DECIMAL(10,2) NOT NULL,
  status     VARCHAR(24) NOT NULL DEFAULT 'CREATED',
  created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
  updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY idx_status_created (status, created_at)   -- 超时扫描器专用复合索引（最左前缀教学点）
) ENGINE=InnoDB;

-- MySQL 倒排表：SearchEngine 的"小团队实现"（spec §5.2）
-- 主键 (attr_id, attr_value_id, product_id) 天然覆盖筛选交集查询，不用扫 JSON
CREATE TABLE product_attr_index (
  attr_id        BIGINT NOT NULL,
  attr_value_id  BIGINT NOT NULL,
  product_type   TINYINT NOT NULL,         -- 1=spu 2=sku
  product_id     BIGINT NOT NULL,
  attr_value     VARCHAR(128),             -- 数值/文本原值，用于范围查询
  PRIMARY KEY (attr_id, attr_value_id, product_id)
) ENGINE=InnoDB;

-- Debezium 复制账号：CDC 读取 binlog 所需最小权限
CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz123456';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
```

- [ ] **Step 2: 写 deploy/mysql/seed.sql**

```sql
-- 种子数据：类目树 + 属性元数据 + 枚举值（真实实体，load-gen 按此模板造数）
-- 枚举 vid 全局唯一：值用 vid 存、文本仅展示（改名不影响倒排——bug B7 的正确基线）

-- 类目树（3 层，8 个叶子类目）
INSERT INTO category (id, parent_id, name, level, path, is_leaf) VALUES
(1,  0,  '数码',     1, '0/1/',         0),
(11, 1,  '手机通讯', 2, '0/1/11/',      0),
(111,11, '手机',     3, '0/1/11/111/',  1),
(112,11, '平板',     3, '0/1/11/112/',  1),
(12, 1,  '电脑办公', 2, '0/1/12/',      0),
(121,12, '笔记本',   3, '0/1/12/121/',  1),
(2,  0,  '服饰鞋包', 1, '0/2/',         0),
(21, 2,  '男装',     2, '0/2/21/',      0),
(211,21, 'T恤',      3, '0/2/21/211/',  1),
(212,21, '夹克',     3, '0/2/21/212/',  1),
(22, 2,  '女装',     2, '0/2/22/',      0),
(221,22, '连衣裙',   3, '0/2/22/221/',  1),
(3,  0,  '家用电器', 1, '0/3/',         0),
(31, 3,  '大家电',   2, '0/3/31/',      0),
(311,31, '冰箱',     3, '0/3/31/311/',  1),
(312,31, '洗衣机',   3, '0/3/31/312/',  1);

-- 属性元数据：每个叶子类目一套模板（新增类型=往这里 INSERT，不改代码）
INSERT INTO attribute_meta
(id, category_id, name, value_type, is_key, is_sale, is_search, is_filter, required, enum_values, unit) VALUES
-- 手机（关键=品牌+型号；销售=颜色+内存）
(1,  111, '品牌',     'enum',   1,0,0,1,1, '[{"vid":3001,"text":"Apple"},{"vid":3002,"text":"华为"},{"vid":3003,"text":"小米"},{"vid":3009,"text":"海信"}]', NULL),
(2,  111, '型号',     'string', 1,0,0,0,1, NULL, NULL),
(3,  111, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1002,"text":"白"},{"vid":1003,"text":"蓝"},{"vid":1005,"text":"深空灰"}]', NULL),
(4,  111, '内存',     'enum',   0,1,1,1,1, '[{"vid":2001,"text":"128G"},{"vid":2002,"text":"256G"},{"vid":2003,"text":"512G"},{"vid":2004,"text":"1T"}]', NULL),
(5,  111, '上市时间', 'date',   0,0,1,0,0, NULL, NULL),
-- 平板
(6,  112, '品牌',     'enum',   1,0,0,1,1, '[{"vid":3001,"text":"Apple"},{"vid":3002,"text":"华为"},{"vid":3003,"text":"小米"}]', NULL),
(7,  112, '型号',     'string', 1,0,0,0,1, NULL, NULL),
(8,  112, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1002,"text":"白"},{"vid":1006,"text":"银"}]', NULL),
(9,  112, '容量',     'enum',   0,1,1,1,1, '[{"vid":5001,"text":"64G"},{"vid":5002,"text":"128G"},{"vid":5003,"text":"256G"}]', NULL),
(10, 112, '屏幕尺寸', 'decimal',0,0,0,1,0, NULL, '英寸'),
-- 笔记本
(11, 121, '品牌',     'enum',   1,0,0,1,1, '[{"vid":3001,"text":"Apple"},{"vid":3002,"text":"华为"},{"vid":3004,"text":"联想"},{"vid":3003,"text":"小米"}]', NULL),
(12, 121, '型号',     'string', 1,0,0,0,1, NULL, NULL),
(13, 121, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1006,"text":"银"},{"vid":1005,"text":"深空灰"}]', NULL),
(14, 121, '内存',     'enum',   0,1,0,1,1, '[{"vid":2002,"text":"256G"},{"vid":2003,"text":"512G"},{"vid":2004,"text":"1T"}]', NULL),
(15, 121, '硬盘',     'enum',   0,1,0,1,1, '[{"vid":2101,"text":"512G"},{"vid":2102,"text":"1T"},{"vid":2103,"text":"2T"}]', NULL),
(16, 121, 'CPU',      'string', 0,0,1,0,0, NULL, NULL),
-- T恤（关键=款号；销售=颜色+尺码）
(17, 211, '款号',     'string', 1,0,0,0,1, NULL, NULL),
(18, 211, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1002,"text":"白"},{"vid":1003,"text":"蓝"},{"vid":1004,"text":"红"}]', NULL),
(19, 211, '尺码',     'enum',   0,1,1,1,1, '[{"vid":4001,"text":"S"},{"vid":4002,"text":"M"},{"vid":4003,"text":"L"},{"vid":4004,"text":"XL"},{"vid":4005,"text":"XXL"}]', NULL),
(20, 211, '材质',     'enum',   0,0,0,1,0, '[{"vid":7001,"text":"纯棉"},{"vid":7002,"text":"聚酯纤维"}]', NULL),
(21, 211, '产地',     'string', 0,0,0,0,0, NULL, NULL),
-- 夹克
(22, 212, '款号',     'string', 1,0,0,0,1, NULL, NULL),
(23, 212, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1003,"text":"蓝"},{"vid":1004,"text":"红"}]', NULL),
(24, 212, '尺码',     'enum',   0,1,1,1,1, '[{"vid":4001,"text":"S"},{"vid":4002,"text":"M"},{"vid":4003,"text":"L"},{"vid":4004,"text":"XL"}]', NULL),
(25, 212, '材质',     'enum',   0,0,0,1,0, '[{"vid":7002,"text":"聚酯纤维"},{"vid":7003,"text":"羊毛"}]', NULL),
(26, 212, '版型',     'enum',   0,0,0,1,0, '[{"vid":9001,"text":"修身"},{"vid":9002,"text":"宽松"}]', NULL),
-- 连衣裙
(27, 221, '款号',     'string', 1,0,0,0,1, NULL, NULL),
(28, 221, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1002,"text":"白"},{"vid":1004,"text":"红"}]', NULL),
(29, 221, '尺码',     'enum',   0,1,1,1,1, '[{"vid":4001,"text":"S"},{"vid":4002,"text":"M"},{"vid":4003,"text":"L"}]', NULL),
(30, 221, '材质',     'enum',   0,0,0,1,0, '[{"vid":7001,"text":"纯棉"},{"vid":7002,"text":"聚酯纤维"}]', NULL),
(31, 221, '风格',     'enum',   0,0,0,1,0, '[{"vid":9101,"text":"通勤"},{"vid":9102,"text":"休闲"},{"vid":9103,"text":"宴会"}]', NULL),
-- 冰箱
(32, 311, '品牌',     'enum',   1,0,0,1,1, '[{"vid":3005,"text":"美的"},{"vid":3006,"text":"海尔"},{"vid":3010,"text":"格力"}]', NULL),
(33, 311, '型号',     'string', 1,0,0,0,1, NULL, NULL),
(34, 311, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1002,"text":"白"},{"vid":1006,"text":"银"}]', NULL),
(35, 311, '容量',     'enum',   0,1,0,1,1, '[{"vid":6001,"text":"300L"},{"vid":6002,"text":"450L"},{"vid":6003,"text":"600L"}]', NULL),
(36, 311, '能效等级', 'enum',   0,0,0,1,0, '[{"vid":8001,"text":"一级"},{"vid":8002,"text":"二级"},{"vid":8003,"text":"三级"}]', NULL),
-- 洗衣机
(37, 312, '品牌',     'enum',   1,0,0,1,1, '[{"vid":3005,"text":"美的"},{"vid":3006,"text":"海尔"}]', NULL),
(38, 312, '型号',     'string', 1,0,0,0,1, NULL, NULL),
(39, 312, '颜色',     'enum',   0,1,1,1,1, '[{"vid":1001,"text":"黑"},{"vid":1002,"text":"白"},{"vid":1006,"text":"银"}]', NULL),
(40, 312, '容量',     'enum',   0,1,0,1,1, '[{"vid":6101,"text":"8kg"},{"vid":6102,"text":"10kg"},{"vid":6103,"text":"12kg"}]', NULL),
(41, 312, '类型',     'enum',   0,0,0,1,0, '[{"vid":9201,"text":"滚筒"},{"vid":9202,"text":"波轮"}]', NULL);
```

- [ ] **Step 3: 启动 MySQL 并验证建表/种子**

Run:
```bash
cd deploy && docker compose up -d mysql
docker exec demo-mysql mysql -uroot -proot123456 -e "USE demo; SHOW TABLES;"
docker exec demo-mysql mysql -uroot -proot123456 -e "SELECT COUNT(*) AS cats FROM demo.category; SELECT COUNT(*) AS metas FROM demo.attribute_meta;"
```
Expected: 6 张表；cats=16、metas=41。

- [ ] **Step 4: 验证 CDC 前置条件（binlog + 复制账号）**

Run:
```bash
docker exec demo-mysql mysql -uroot -proot123456 -e "SHOW VARIABLES LIKE 'binlog_format'; SHOW VARIABLES LIKE 'server_id';"
docker exec demo-mysql mysql -uroot -proot123456 -e "SELECT user,host FROM mysql.user WHERE user='debezium';"
```
Expected: `binlog_format=ROW`、`server_id=1`、`debezium`@`%`。

- [ ] **Step 5: Commit**

```bash
git add deploy/mysql/init.sql deploy/mysql/seed.sql
git commit -m "feat(mysql): 商品中心 6 表 DDL + 类目/属性元数据种子

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

> 注意：init.sql/seed.sql 只在数据卷**首次初始化**时执行。改表结构后想重跑，需 `docker compose down -v`（会清空数据，别误伤造数成果）。

---

### Task 3: Kafka 启动 + 显式创建 topic

**Files:**
- Create: `deploy/setup-topics.sh`

**Interfaces:**
- Produces: topic `product.changed`、`order.events`（各 3 分区）——后续 indexer/order-service/order-worker 直接按此名订阅。

- [ ] **Step 1: 写 deploy/setup-topics.sh**

```bash
#!/usr/bin/env bash
# 显式创建 demo 的 2 个业务 topic（compose 里关了 auto.create，教学 kafka-topics 用法）
# --partitions 3：并行上限=3（bug K4 的"加分区"就是改这里）
# --replication-factor 1：单节点只能 1（生产 3——demo 显式降级点，多副本见 docs/diagrams 架构图）
set -euo pipefail

docker exec demo-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic product.changed --partitions 3 --replication-factor 1

docker exec demo-kafka kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --if-not-exists --topic order.events --partitions 3 --replication-factor 1

docker exec demo-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
```

- [ ] **Step 2: 启动 Kafka 并等它就绪**

Run: `cd deploy && docker compose up -d kafka && docker compose ps kafka`
Expected: kafka 状态 healthy（首次启动等 30–60s，KRaft 要格式化元数据盘）。

- [ ] **Step 3: 运行建 topic 脚本并验证**

Run: `cd deploy && bash setup-topics.sh`
Expected: 列出 `connect-configs`、`connect-offsets`、`connect-status`（Connect 启动后出现）、`product.changed`、`order.events`。

- [ ] **Step 4: 验证分区数**

Run:
```bash
docker exec demo-kafka kafka-topics.sh --bootstrap-server localhost:9092 --describe --topic product.changed
```
Expected: `Topic: product.changed  PartitionCount: 3  ReplicationFactor: 1`。

- [ ] **Step 5: Commit**

```bash
git add deploy/setup-topics.sh
git commit -m "feat(kafka): topic 创建脚本（3 分区显式创建）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: Elasticsearch 启动验证（含 bootstrap 前置修复）

**Files:** 无新增（compose 已含 es）。

**Interfaces:**
- Produces: `http://localhost:9200` 可用（后续 indexer/search-service 直接连）。

- [ ] **Step 1: 宿主机修 vm.max_map_count（Windows + Docker Desktop 前置，bug S2 的正确基线）**

Run（PowerShell 或 Git Bash，**每次 WSL2/Docker 重启后需重跑**，写进 README 备忘）:
```bash
wsl -d docker-desktop -u root sysctl -w vm.max_map_count=262144
```
Expected: `vm.max_map_count = 262144`。

- [ ] **Step 2: 启动并验证**

Run:
```bash
cd deploy && docker compose up -d es
curl -s http://localhost:9200
curl -s http://localhost:9200/_cluster/health
```
Expected: 第一个 JSON 带 `version.build_flavor: default`；第二个 `"status":"green"`。

- [ ] **Step 3: 验证内存预算生效**

Run: `docker stats demo-es --no-stream --format "{{.MemUsage}}"`
Expected: 约 1.2–1.5GB（heap 1g + 进程自身），符合预算。

- [ ] **Step 4: Commit**（README 变更合并到 Task 7；本任务无文件变更则并入下个 commit）

---

### Task 5: Redis 启动 + 持久化验证

**Files:** 无新增。

**Interfaces:**
- Produces: `localhost:6379` 可用，AOF+RDB 已开（bug S6 的正确基线）。

- [ ] **Step 1: 启动并验证参数**

Run:
```bash
cd deploy && docker compose up -d redis
docker exec demo-redis redis-cli PING
docker exec demo-redis redis-cli CONFIG GET maxmemory
docker exec demo-redis redis-cli CONFIG GET appendonly
```
Expected: `PONG`；`maxmemory=536870912`（512mb）；`appendonly=yes`。

- [ ] **Step 2: 重启持久性验证（S6 反例演练的正确基线）**

Run:
```bash
docker exec demo-redis redis-cli SET persist:test hello
docker restart demo-redis && sleep 3
docker exec demo-redis redis-cli GET persist:test
docker exec demo-redis redis-cli DEL persist:test
```
Expected: 重启后 `GET` 仍返回 `hello`。

- [ ] **Step 3: Commit**（无文件变更则并入 Task 7 commit）

---

### Task 6: Debezium connector 部署 + CDC 端到端验收

**Files:**
- Create: `deploy/kafka-connect/mysql-connector.json`

**Interfaces:**
- Produces: `product.changed` topic 上有 category/attribute_meta/spu/sku 四表的变更事件（indexer 计划 3 直接订阅）；Connect REST `localhost:8083`。

- [ ] **Step 1: 写 deploy/kafka-connect/mysql-connector.json**

```json
{
  "name": "product-center-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "debezium",
    "database.password": "dbz123456",
    "database.server.id": "54001",
    "database.include.list": "demo",
    "table.include.list": "demo.category,demo.attribute_meta,demo.spu,demo.sku",
    "topic.prefix": "demo",
    "schema.history.internal.kafka.topic": "schema-history",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "decimal.handling.mode": "double",
    "snapshot.mode": "initial",
    "transforms": "route",
    "transforms.route.type": "org.apache.kafka.connect.transforms.RegexRouter",
    "transforms.route.regex": "demo.demo.(category|attribute_meta|spu|sku)",
    "transforms.route.replacement": "product.changed"
  }
}
```

> 参数注释（为什么这么配）：
> - `snapshot.mode=initial`：首次启动对存量表做全量快照（种子数据也会进 Kafka，正好供计划 3 的 indexer 建索引）；快照与增量的切换窗口=bug C4。
> - `RegexRouter`：把 Debezium 默认的 `demo.demo.<table>` 四类 topic 统一路由到 `product.changed`，消费者只订一个 topic，用消息里的 `source.table` 字段区分来源表。
> - `server.id=54001`：binlog 消费端的唯一 id，两个 connector 抢同一 id 会互踢（bug S4）。
> - 内存预算 1g 在 compose 里（`KAFKA_HEAP_OPTS`）。

- [ ] **Step 2: 启动 Connect 并等待就绪**

Run: `cd deploy && docker compose up -d connect && docker compose ps connect`
Expected: healthy。Connect 首次启动会自建 3 个内部 topic（configs/offsets/status）。

- [ ] **Step 3: 部署 connector**

Run:
```bash
curl -s -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors -d @deploy/kafka-connect/mysql-connector.json
```
Expected: HTTP 201，返回 connector 配置 JSON。

- [ ] **Step 4: 验证 connector 状态**

Run: `curl -s http://localhost:8083/connectors/product-center-connector/status`
Expected: `"state":"RUNNING"`。

- [ ] **Step 5: CDC 端到端验收（本计划的验收标准）**

Run:
```bash
# 1) 在 MySQL 插一条测试类目（模拟"业务写库"）
docker exec demo-mysql mysql -uroot -proot123456 -e \
  "INSERT INTO demo.category (id,parent_id,name,level,path,is_leaf) VALUES (9999,0,'CDC测试',1,'0/9999/',1);"
# 2) 从 Kafka 消费到这条变更（--from-beginning 连快照一起看，最后一条应是我们的 INSERT）
docker exec demo-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic product.changed --from-beginning --max-messages 1 2>/dev/null | head -c 800
# 3) 清理测试数据（这条 DELETE 也应产生 op=d 事件）
docker exec demo-mysql mysql -uroot -proot123456 -e "DELETE FROM demo.category WHERE id=9999;"
```
Expected: 消费到的 JSON 里 `"payload":{"op":"c"|"r"|"d"}`、`"source":{"table":"category"}`（op=c 表示 INSERT）。**这一条 SQL → Kafka 的自动流转，就是「CDC 捕获」的活体证明。**

- [ ] **Step 6: Commit**

```bash
git add deploy/kafka-connect/mysql-connector.json
git commit -m "feat(cdc): Debezium MySQL connector 配置 + CDC 端到端验收

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: chaos 脚本 + README 部署指引

**Files:**
- Create: `deploy/chaos.sh`
- Modify: `README.md`（追加部署指引章节）

**Interfaces:**
- Produces: `deploy/chaos.sh <服务名|random|list>` 命令（故障演练用）；README 完整部署手册。

- [ ] **Step 1: 写 deploy/chaos.sh**

```bash
#!/usr/bin/env bash
# 混沌脚本：随机/指定 kill 一个中间件容器再拉起，模拟服务器崩溃（spec §16 演练工具）
# 用法：./chaos.sh           随机杀一个
#       ./chaos.sh kafka     指定杀 kafka
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
```

- [ ] **Step 2: README.md 追加部署章节**

在 README.md 末尾追加：

````markdown
## 部署与验证（计划 1 成果）

前置：
- Docker Desktop 已启动；代理开着（拉镜像约 3–5GB）。
- Windows 每次 Docker/WSL 重启后执行一次：`wsl -d docker-desktop -u root sysctl -w vm.max_map_count=262144`（否则 ES 起不来，bug S2）。

步骤：
```bash
cd deploy
docker compose up -d          # 拉起 5 个中间件
bash setup-topics.sh          # 创建 2 个业务 topic（3 分区）
curl -X POST -H "Content-Type: application/json" \
  http://localhost:8083/connectors -d @kafka-connect/mysql-connector.json   # 部署 CDC
```

CDC 验收（一条 SQL 自动出现在 Kafka）：
```bash
docker exec demo-mysql mysql -uroot -proot123456 -e "INSERT INTO demo.category (id,parent_id,name,level,path,is_leaf) VALUES (9999,0,'CDC测试',1,'0/9999/',1);"
docker exec demo-kafka kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic product.changed --from-beginning --max-messages 1
```

常用速查：
```bash
docker exec demo-kafka kafka-topics.sh --bootstrap-server localhost:9092 --list --describe
docker exec demo-redis redis-cli INFO memory
curl -s localhost:9200/_cluster/health
curl -s localhost:8083/connectors/product-center-connector/status
bash deploy/chaos.sh random    # 随机杀一个中间件，看业务反应
```
````

- [ ] **Step 3: Commit + push**

```bash
git add deploy/chaos.sh README.md
git commit -m "docs(infra): chaos 脚本 + README 部署手册

Co-Authored-By: Claude Code <noreply@anthropic.com>"
git push origin main
```

Expected: push 成功（需代理开启；失败则本地已提交，稍后重推）。

---

## 计划 1 验收清单（全部通过 = 计划 1 完成）

- [ ] `docker compose up -d` 五个容器全 healthy
- [ ] MySQL：6 表 + 16 类目 + 41 属性元数据；binlog ROW
- [ ] Kafka：`product.changed`/`order.events` 各 3 分区
- [ ] ES：`_cluster/health` green；heap 1g
- [ ] Redis：maxmemory 512mb + AOF，重启数据不丢
- [ ] **CDC：一条 INSERT 自动出现在 `product.changed`**
- [ ] chaos.sh 可用；README 可照做

## 后续计划路线图（各自开工前写详细版）

- **计划 2 · 商品中心核心**：product-service（发布引擎：幂等键 → 元数据校验 → SPU 归一 → SKU 笛卡尔积 + 护栏 → 布隆；详情 API 缓存）+ load-gen 种子造数（类目模板 → SPU 10 万/SKU 50 万）。验收：发布一个商品 → spu/sku 落库 → 布隆生效。
- **计划 3 · 搜索链路**：indexer（消费 product.changed，spu/sku/meta 事件缓存 + JSON 拍平 → ES + product_attr_index）+ search-service（SearchEngine 双实现、facet、search_after、搜索缓存、热搜 ZSET、本地 LRU/singleflight/熔断/限流）。验收：发布 → 两个引擎都能搜到、facet 计数正确、缓存命中。
- **计划 4 · 订单链路**：order-service（下单/模拟支付/取消/超时扫描器，条件更新状态机，acks=all 发 Kafka）+ order-worker（幂等 dedup、手动提交 offset、扣库存条件更新、三态缓存、销量榜 ZSET、积分）。验收：订单全生命周期 + 库存/榜单正确；kill 消费者验证 rebalance。
- **计划 5 · 演练工具集**：load-gen burst/压测模式 + 路由报告、kafka-inspector、chaos 完善、50 故障卡片手册（docs/bugs/）、学习材料（布隆/倒排/ZSET/参数速查）、全部服务 Dockerfile + compose 集成。验收：完整故障演练流程跑通。
