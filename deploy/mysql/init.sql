-- 建表 + Debezium 复制账号（只在数据卷首次初始化时执行）
-- 表结构 = spec §4.1，逐列注释"为什么"：

-- ★ 必须显式指定连接字符集：docker-entrypoint 执行 init 脚本时 mysql 客户端默认 latin1，
--   会把 UTF-8 中文二次编码成乱码（今天实测踩过：'型号' 存成了 'åž‹'）——排查手段：SELECT HEX(name)
SET NAMES utf8mb4;

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
-- id 用 AUTO_INCREMENT：demo 简化（生产用雪花算法应用层生成分布式 id，见场景题）
CREATE TABLE spu (
  id           BIGINT PRIMARY KEY AUTO_INCREMENT,
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
  id           BIGINT PRIMARY KEY AUTO_INCREMENT,
  spu_id       BIGINT NOT NULL,
  sku_code     VARCHAR(64) NOT NULL,
  sale_props   JSON NOT NULL,              -- {"颜色":1001,"尺码":4002}，值用 vid（bug B7）
  sale_hash    CHAR(32) NOT NULL,
  price        DECIMAL(12,2) NOT NULL,     -- 关系列：交易强一致
  stock        INT NOT NULL,               -- 关系列：扣库存行级锁
  frozen_stock INT NOT NULL DEFAULT 0,     -- 热点 Redis 预扣演进用（spec §5.3、§18-3）
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
-- mysql_native_password：MySQL 8.0.x 仍支持该插件（8.4+ 已移除，故镜像钉死在 8.0.36）
CREATE USER 'debezium'@'%' IDENTIFIED WITH mysql_native_password BY 'dbz123456';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
