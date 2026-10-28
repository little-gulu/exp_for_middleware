// Package store：MySQL 访问层（业务背景：商品中心 6 张表的读写）。
// 本任务要体会的：连接池参数语义、rows.Scan 的正确姿势、upsert 技巧、批量 INSERT 的网络往返差异。
package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// 实现 TODO 时需要补的 import：encoding/json（enum_values 解析）、strings（批量 INSERT 拼接）

// EnumVal / AttributeMeta：attribute_meta 表一行的 Go 形态（供 publish 包消费）
type EnumVal struct {
	VID  int64  `json:"vid"`
	Text string `json:"text"`
}

type AttributeMeta struct {
	ID         int64
	CategoryID int64
	Name       string
	ValueType  string // int/string/decimal/enum/range/date
	IsKey      bool
	IsSale     bool
	IsSearch   bool
	IsFilter   bool
	Required   bool
	EnumValues []EnumVal
	Unit       string
}

// SPU / SKU / SPUDetail：spu/sku 表的行（JSON 列用字符串装，不引 ORM）
type SPU struct {
	ID         int64
	MerchantID int64
	CategoryID int64
	Title      string
	KeyProps   string // JSON: {"1":"3001","2":"iPhone 15 Pro"}（key=attr_id）
	BaseProps  string // JSON
	KeyHash    string
}

type SKU struct {
	SpuID     int64
	SKUCode   string
	SaleProps string // JSON
	SaleHash  string
	Price     float64
	Stock     int
}

type SPUDetail struct {
	SPU
	SKUs []SKU
}

type Store struct{ db *sql.DB }

// NewStore：完整给出（读 + 理解连接池参数——这是本任务第一课）：
//   MaxOpenConns=50：最多同时 50 个连接，打满后新请求排队/超时（bug 卡 G4 就是把这里调极端）
//   MaxIdleConns=10：空闲保留 10 个，避免频繁建连（建连=TCP握手+鉴权，很贵）
//   ConnMaxLifetime=30min：单连接最长寿命，超时回收，防止连上已被杀掉的旧连接
//   Ping 一次：启动即失败，不带病运行
func NewStore(dsn string) (*Store, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("open mysql: %w", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping mysql: %w", err)
	}
	return &Store{db: db}, nil
}

// GetCategoryMetas：拉某类目的全部属性模板（发布校验的输入）。
//
// TODO 你的实现（SQL 已给，练的是 Go 侧）：
//   SELECT id, category_id, name, value_type, is_key, is_sale, is_search, is_filter, required, enum_values, IFNULL(unit,'')
//   FROM attribute_meta WHERE category_id = ? ORDER BY sort_order, id
//   提示：
//     1. QueryContext 拿 rows，defer rows.Close()（必须！）
//     2. Scan 顺序必须与 SELECT 列序一致；TINYINT 扫进 int（1/0），再转 bool（m.IsKey = isKey == 1）
//     3. enum_values 是 JSON 列 → 扫进 []byte → json.Unmarshal 进 []EnumVal（可能为 NULL，len==0 跳过）
//     4. 最后 return metas, rows.Err()（rows.Err 捕获迭代中途的错误，别漏）
func (s *Store) GetCategoryMetas(ctx context.Context, categoryID int64) ([]AttributeMeta, error) {
	panic("TODO: 你的实现")
}

// UpsertSPU：SPU 归一落库（业务背景：同款复用不重复建）。
//
// TODO 你的实现（SQL 已给，体会这个技巧）：
//   INSERT INTO spu (merchant_id, category_id, title, key_props, base_props, key_hash, publish_time)
//   VALUES (?, ?, ?, ?, ?, ?, NOW())
//   ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), title = VALUES(title)
//   然后 SELECT LAST_INSERT_ID() 拿 id 并返回。
//   提示：撞唯一键 (merchant_id, category_id, key_hash) 时不是报错，
//         而是更新并让 LAST_INSERT_ID() 返回"已存在那行"的 id——一行 SQL 实现"新建或复用"。
func (s *Store) UpsertSPU(ctx context.Context, spu SPU) (int64, error) {
	panic("TODO: 你的实现")
}

// InsertSKUs：批量多行 INSERT。
//
// TODO 你的实现：
//   INSERT INTO sku (spu_id, sku_code, sale_props, sale_hash, price, stock) VALUES (?,?,?,?,?,?),(?,?,?,?,?,?),...
//   提示：
//     1. strings.Builder 拼 "VALUES " + 逗号连接的 "(?,?,?,?,?,?)"
//     2. args 切片按顺序 append 6 个值/行，占位符个数必须与值数一致
//     3. 一条 SQL 塞 N 组 VALUES = 一次网络往返；循环单条 INSERT = N 次往返——百万行造数的唯一可行路径
func (s *Store) InsertSKUs(ctx context.Context, skus []SKU) error {
	panic("TODO: 你的实现")
}

// GetSPUWithSKUs：SPU + 它的 SKU 列表。
//
// TODO 你的实现：
//   1. QueryRow 查 spu：SELECT id, merchant_id, category_id, title, key_props, base_props, key_hash FROM spu WHERE id=?
//      err == sql.ErrNoRows → return (nil, nil)（不存在不是错误）
//   2. Query 查 sku：SELECT spu_id, sku_code, sale_props, sale_hash, price, stock FROM sku WHERE spu_id=?
//   3. return &d, rows.Err()
func (s *Store) GetSPUWithSKUs(ctx context.Context, spuID int64) (*SPUDetail, error) {
	panic("TODO: 你的实现")
}

// Close：进程退出时释放连接池
func (s *Store) Close() error { return s.db.Close() }
