// Package store：MySQL 访问层（业务背景：商品中心 6 张表的读写）。
// 本任务要体会的：连接池参数语义、rows.Scan 的正确姿势、upsert 技巧、批量 INSERT 的网络往返差异。
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

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
func (s *Store) GetCategoryMetas(ctx context.Context, categoryID int64) ([]AttributeMeta, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, category_id, name, value_type, is_key, is_sale, is_search, is_filter, required, enum_values, IFNULL(unit,'')
		 FROM attribute_meta WHERE category_id = ? ORDER BY sort_order, id`, categoryID)
	if err != nil {
		return nil, fmt.Errorf("query attribute_meta: %w", err)
	}
	defer rows.Close()

	var metas []AttributeMeta
	for rows.Next() {
		var m AttributeMeta
		var isKey, isSale, isSearch, isFilter, required int
		var enumJSON []byte
		if err := rows.Scan(&m.ID, &m.CategoryID, &m.Name, &m.ValueType,
			&isKey, &isSale, &isSearch, &isFilter, &required, &enumJSON, &m.Unit); err != nil {
			return nil, fmt.Errorf("scan attribute_meta: %w", err)
		}
		m.IsKey = isKey == 1 // TINYINT → bool
		m.IsSale = isSale == 1
		m.IsSearch = isSearch == 1
		m.IsFilter = isFilter == 1
		m.Required = required == 1
		if len(enumJSON) > 0 {
			if err := json.Unmarshal(enumJSON, &m.EnumValues); err != nil {
				return nil, fmt.Errorf("unmarshal enum_values: %w", err)
			}
		}
		metas = append(metas, m)
	}
	return metas, rows.Err()
}

// UpsertSPU：SPU 归一落库（业务背景：同款复用不重复建）。
// 撞唯一键 (merchant_id, category_id, key_hash) 时不是报错，
// 而是更新并让 LAST_INSERT_ID() 返回"已存在那行"的 id——一行 SQL 实现"新建或复用"。
func (s *Store) UpsertSPU(ctx context.Context, spu SPU) (int64, error) {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO spu (merchant_id, category_id, title, key_props, base_props, key_hash, publish_time)
		VALUES (?, ?, ?, ?, ?, ?, NOW())
		ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), title = VALUES(title)`,
		spu.MerchantID, spu.CategoryID, spu.Title, spu.KeyProps, spu.BaseProps, spu.KeyHash)
	if err != nil {
		return 0, fmt.Errorf("upsert spu: %w", err)
	}
	var id int64
	if err := s.db.QueryRowContext(ctx, "SELECT LAST_INSERT_ID()").Scan(&id); err != nil {
		return 0, err
	}
	return id, nil
}

// InsertSKUs：批量多行 upsert（一条 SQL 多组 VALUES = 一次网络往返；循环单条 = N 次往返）。
// ON DUPLICATE KEY UPDATE：SPU 被归一复用（同款重发布）时，已存在的 SKU 更新价格/库存而不是报错——
// 今天实测踩过：SPU upsert 了但 SKU 纯 INSERT，重发同款直接撞 uk_sku 唯一键报 1062。
func (s *Store) InsertSKUs(ctx context.Context, skus []SKU) error {
	if len(skus) == 0 {
		return nil
	}
	var b strings.Builder
	b.WriteString("INSERT INTO sku (spu_id, sku_code, sale_props, sale_hash, price, stock) VALUES ")
	args := make([]any, 0, len(skus)*6)
	for i, sk := range skus {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString("(?,?,?,?,?,?)")
		args = append(args, sk.SpuID, sk.SKUCode, sk.SaleProps, sk.SaleHash, sk.Price, sk.Stock)
	}
	b.WriteString(" ON DUPLICATE KEY UPDATE price=VALUES(price), stock=VALUES(stock), sku_code=VALUES(sku_code)")
	_, err := s.db.ExecContext(ctx, b.String(), args...)
	return err
}

// GetSPUWithSKUs：SPU + 它的 SKU 列表；不存在返回 (nil, nil)。
func (s *Store) GetSPUWithSKUs(ctx context.Context, spuID int64) (*SPUDetail, error) {
	var d SPUDetail
	err := s.db.QueryRowContext(ctx,
		`SELECT id, merchant_id, category_id, title, key_props, base_props, key_hash FROM spu WHERE id=?`, spuID).
		Scan(&d.ID, &d.MerchantID, &d.CategoryID, &d.Title, &d.KeyProps, &d.BaseProps, &d.KeyHash)
	if err == sql.ErrNoRows {
		return nil, nil // 不存在不是错误
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT spu_id, sku_code, sale_props, sale_hash, price, stock FROM sku WHERE spu_id=?`, spuID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var sk SKU
		if err := rows.Scan(&sk.SpuID, &sk.SKUCode, &sk.SaleProps, &sk.SaleHash, &sk.Price, &sk.Stock); err != nil {
			return nil, err
		}
		d.SKUs = append(d.SKUs, sk)
	}
	return &d, rows.Err()
}

// Close：进程退出时释放连接池
func (s *Store) Close() error { return s.db.Close() }
