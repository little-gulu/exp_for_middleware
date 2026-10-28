# 计划 2 参考答案

> ⚠️ 规则：**先自己做，测试全绿后再对答案**。答案的作用是"对照思路差异"，不是"抄"。
> 做完一个阶段对一次，别提前看后面的。

## 阶段 1b：validate.go

```go
package publish

import (
	"errors"
	"fmt"

	"product-service/internal/store"
)

// 校验错误哨兵：调用方用 errors.Is(err, ErrRequired) 判断错误类别，而不是比较错误字符串。
var (
	ErrRequired    = errors.New("必填属性缺失")
	ErrIllegalEnum = errors.New("枚举值非法")
	ErrInvalidType = errors.New("属性值类型不匹配")
)

// Validate：元数据驱动的通用校验。
// 设计原理：代码只认 value_type 这类"描述数据的数据"，
// 永远不出现 if name=="颜色" 这种具体属性分支——新增属性类型=加配置，零改码。
func Validate(metas []store.AttributeMeta, attrs []AttrValue) error {
	byID := make(map[int64]AttrValue, len(attrs))
	for _, a := range attrs {
		byID[a.AttrID] = a
	}
	for _, m := range metas {
		v, ok := byID[m.ID]
		if m.Required && !ok {
			return fmt.Errorf("%w: %s(id=%d)", ErrRequired, m.Name, m.ID)
		}
		if !ok {
			continue
		}
		switch m.ValueType {
		case "enum":
			// 枚举：ValueID 必须在 enum_values 字典里
			if !hasEnum(m.EnumValues, v.ValueID) {
				return fmt.Errorf("%w: %s 无 vid=%d", ErrIllegalEnum, m.Name, v.ValueID)
			}
		case "string":
			// 字符串：非空即可（长度/格式校验是扩展点）
			if v.Raw == "" {
				return fmt.Errorf("%w: %s 不能为空", ErrInvalidType, m.Name)
			}
		default:
			// int/decimal/range/date：本 demo 提交侧按原值透传
			if v.Raw == "" {
				return fmt.Errorf("%w: %s 不能为空", ErrInvalidType, m.Name)
			}
		}
	}
	return nil
}

func hasEnum(enums []store.EnumVal, vid int64) bool {
	for _, e := range enums {
		if e.VID == vid {
			return true
		}
	}
	return false
}
```

## 阶段 1c：keyhash.go

```go
package publish

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// KeyHash：关键属性指纹（背景需求：同款判定）。
// 两个关键性质：确定性（同输入同输出）；顺序无关（提交顺序不同指纹一致）。
func KeyHash(categoryID int64, keyVals []AttrValue) string {
	sorted := append([]AttrValue(nil), keyVals...) // 拷贝，别改调用方的切片
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].AttrID < sorted[j].AttrID })
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", categoryID)
	for _, v := range sorted {
		if v.ValueID != 0 {
			fmt.Fprintf(&b, "%d=%d;", v.AttrID, v.ValueID) // enum 用 vid
		} else {
			fmt.Fprintf(&b, "%d=%s;", v.AttrID, v.Raw) // string 用原值
		}
	}
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
```

## 阶段 1d：group.go

```go
package publish

// GroupByAttrID：把提交值按属性 id 聚合。
func GroupByAttrID(attrs []AttrValue) map[int64][]AttrValue {
	m := make(map[int64][]AttrValue)
	for _, a := range attrs {
		m[a.AttrID] = append(m[a.AttrID], a)
	}
	return m
}
```

## 阶段 1e：sku.go

```go
package publish

import (
	"errors"
	"fmt"
)

// 护栏常量：防组合爆炸（颜色20×尺码15×版本10=3000 个 SKU 会拖垮整条链路）
const (
	maxSaleDims  = 3
	maxDimValues = 50
)

var ErrComboExplode = errors.New("组合爆炸")

// GenSKUs：销售属性笛卡尔积。dims[i] = 第 i 个销售属性的所有取值。
func GenSKUs(dims [][]AttrValue) ([][]AttrValue, error) {
	if len(dims) > maxSaleDims {
		return nil, fmt.Errorf("%w: 销售属性维度 %d > %d", ErrComboExplode, len(dims), maxSaleDims)
	}
	for i, d := range dims {
		if len(d) > maxDimValues {
			return nil, fmt.Errorf("%w: 第 %d 维取值 %d > %d", ErrComboExplode, i+1, len(d), maxDimValues)
		}
	}
	var out [][]AttrValue
	var rec func(depth int, cur []AttrValue)
	rec = func(depth int, cur []AttrValue) {
		if depth == len(dims) {
			// 必须拷贝：append 可能复用底层数组
			out = append(out, append([]AttrValue(nil), cur...))
			return
		}
		for _, v := range dims[depth] {
			rec(depth+1, append(cur, v))
		}
	}
	rec(0, nil)
	return out, nil
}
```

## 阶段 2：bloom.go

```go
package bloom

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/redis/go-redis/v9"
)

type Filter struct {
	rdb *redis.Client
	key string
	m   uint64 // 位数组长度
	k   uint64 // 哈希个数
}

func New(rdb *redis.Client, key string, m, k uint64) *Filter {
	return &Filter{rdb: rdb, key: key, m: m, k: k}
}

// locations：双重哈希 h_i = (h1 + i*h2) mod m
func (f *Filter) locations(id int64) []uint64 {
	h1 := hash64(id, 0x9e3779b9) // 黄金比例常数，随便两个不同的大 seed 即可
	h2 := hash64(id, 0x85ebca6b)
	locs := make([]uint64, 0, f.k)
	for i := uint64(0); i < f.k; i++ {
		locs = append(locs, (h1+i*h2)%f.m)
	}
	return locs
}

func hash64(id int64, seed uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprintf("%d:%d", id, seed)))
	return h.Sum64()
}

// Add：k 个位置 SETBIT 1（Pipeline 打包成一次网络往返）
func (f *Filter) Add(ctx context.Context, id int64) error {
	_, err := f.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, loc := range f.locations(id) {
			pipe.SetBit(ctx, f.key, int64(loc), 1)
		}
		return nil
	})
	return err
}

// MayContain：k 位全 1 才 true（false="肯定不存在"，true="可能存在"）
func (f *Filter) MayContain(ctx context.Context, id int64) (bool, error) {
	pipe := f.rdb.Pipeline()
	cmds := make([]*redis.IntCmd, 0, f.k)
	for _, loc := range f.locations(id) {
		cmds = append(cmds, pipe.GetBit(ctx, f.key, int64(loc)))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return false, err
	}
	for _, c := range cmds {
		if c.Val() == 0 {
			return false, nil
		}
	}
	return true, nil
}
```

## 阶段 3：idempotency.go

```go
package publish

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

type Idempotency struct{ rdb *redis.Client }

func NewIdempotency(addr string) *Idempotency {
	return &Idempotency{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (i *Idempotency) key(merchantID int64, reqID string) string {
	return fmt.Sprintf("idempotency:%d:%s", merchantID, reqID)
}

func (i *Idempotency) TryAcquire(ctx context.Context, merchantID int64, reqID string) (bool, error) {
	// SETNX：原子抢占——"检查+写入"合成一个命令，并发只有一个成功
	return i.rdb.SetNX(ctx, i.key(merchantID, reqID), "1", 24*time.Hour).Result()
}

func (i *Idempotency) SetResult(ctx context.Context, merchantID int64, reqID string, spuID int64) error {
	return i.rdb.Set(ctx, i.key(merchantID, reqID)+":spu", spuID, 24*time.Hour).Err()
}

func (i *Idempotency) GetResult(ctx context.Context, merchantID int64, reqID string) (int64, error) {
	v, err := i.rdb.Get(ctx, i.key(merchantID, reqID)+":spu").Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}
```

## 阶段 4：store.go（四个 TODO 方法）

```go
// GetCategoryMetas：拉某类目的全部属性模板
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

// UpsertSPU：归一落库。撞唯一键 → LAST_INSERT_ID(id) 返回已存在行的 id（同款复用）
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

// InsertSKUs：批量多行 INSERT（一条 SQL 多组 VALUES = 一次网络往返）
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
	_, err := s.db.ExecContext(ctx, b.String(), args...)
	return err
}

// GetSPUWithSKUs：SPU + 它的 SKU 列表；不存在返回 (nil, nil)
func (s *Store) GetSPUWithSKUs(ctx context.Context, spuID int64) (*SPUDetail, error) {
	var d SPUDetail
	err := s.db.QueryRowContext(ctx,
		`SELECT id, merchant_id, category_id, title, key_props, base_props, key_hash FROM spu WHERE id=?`, spuID).
		Scan(&d.ID, &d.MerchantID, &d.CategoryID, &d.Title, &d.KeyProps, &d.BaseProps, &d.KeyHash)
	if err == sql.ErrNoRows {
		return nil, nil
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
```

## 阶段 5：api.go（两个 handler）

```go
// Publish：POST /publish —— 7 步流程
func (srv *Server) Publish(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req PublishReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if req.ClientReqID == "" {
		writeErr(w, http.StatusBadRequest, "client_req_id 必填（发布幂等的凭据）")
		return
	}

	// ① 幂等：抢不到标记=重复请求 → 返回首次结果
	if ok, err := srv.idem.TryAcquire(ctx, req.MerchantID, req.ClientReqID); err != nil {
		writeErr(w, http.StatusInternalServerError, "幂等检查失败: "+err.Error())
		return
	} else if !ok {
		spuID, err := srv.idem.GetResult(ctx, req.MerchantID, req.ClientReqID)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, "查询首次结果失败: "+err.Error())
			return
		}
		writeJSON(w, PublishResp{SPUID: spuID, Duplicate: true})
		return
	}

	// ② 拉该类目属性模板
	metas, err := srv.store.GetCategoryMetas(ctx, req.CategoryID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "读取类目属性失败: "+err.Error())
		return
	}
	if len(metas) == 0 {
		writeErr(w, http.StatusBadRequest, "类目不存在或无属性模板")
		return
	}

	// ③ 元数据驱动校验
	if err := publish.Validate(metas, req.Attrs); err != nil {
		writeErr(w, http.StatusBadRequest, "属性校验失败: "+err.Error())
		return
	}

	// ④ 按元数据分类：关键属性→归一；销售属性→笛卡尔积维度；其它→普通属性
	grouped := publish.GroupByAttrID(req.Attrs)
	var keyVals, baseVals []publish.AttrValue
	var saleDims [][]publish.AttrValue
	for _, m := range metas {
		vals := grouped[m.ID]
		switch {
		case m.IsKey:
			keyVals = append(keyVals, vals...)
		case m.IsSale:
			saleDims = append(saleDims, vals)
		case len(vals) > 0:
			baseVals = append(baseVals, vals[0])
		}
	}

	// ⑤ SPU 归一落库
	keyHash := publish.KeyHash(req.CategoryID, keyVals)
	spuID, err := srv.store.UpsertSPU(ctx, store.SPU{
		MerchantID: req.MerchantID,
		CategoryID: req.CategoryID,
		Title:      req.Title,
		KeyProps:   propsJSON(keyVals),
		BaseProps:  propsJSON(baseVals),
		KeyHash:    keyHash,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "SPU 落库失败: "+err.Error())
		return
	}

	// ⑥ SKU 笛卡尔积 + 批量落库
	combos, err := publish.GenSKUs(saleDims)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	skus := make([]store.SKU, 0, len(combos))
	for _, combo := range combos {
		saleHash := publish.KeyHash(0, combo) // 同套指纹规则
		skus = append(skus, store.SKU{
			SpuID:     spuID,
			SKUCode:   fmt.Sprintf("SKU-%d-%s", spuID, saleHash[:8]),
			SaleProps: propsJSON(combo),
			SaleHash:  saleHash,
			Price:     99.00, // demo 默认值；真实场景由商家提交
			Stock:     100,
		})
	}
	if err := srv.store.InsertSKUs(ctx, skus); err != nil {
		writeErr(w, http.StatusInternalServerError, "SKU 落库失败: "+err.Error())
		return
	}

	// ⑦ 布隆置位（失败只打日志——布隆是优化，不是主流程的命脉，降级可接受）
	if err := srv.filter.Add(ctx, spuID); err != nil {
		log.Printf("布隆置位失败 spu=%d: %v（不影响发布主流程）", spuID, err)
	}

	_ = srv.idem.SetResult(ctx, req.MerchantID, req.ClientReqID, spuID)
	writeJSON(w, PublishResp{SPUID: spuID, SKUCount: len(skus)})
}

// Detail：GET /spu/{id} —— 三级读路径：布隆 → 缓存 → 回源
func (srv *Server) Detail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	spuID, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "非法商品 id")
		return
	}

	// ① 布隆拦截：肯定不存在 → 404（防大量不存在 ID 打穿 MySQL，bug 卡 R1）
	if ok, err := srv.filter.MayContain(ctx, spuID); err == nil && !ok {
		writeErr(w, http.StatusNotFound, "商品不存在")
		return
	}

	// ② 缓存命中直接回
	ckey := fmt.Sprintf("spu:%d", spuID)
	if v, err := srv.rdb.Get(ctx, ckey).Result(); err == nil {
		w.Header().Set("X-Cache", "hit")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(v))
		return
	}

	// ③ 回源 MySQL + 写缓存（TTL 加随机偏移防雪崩，bug 卡 R3）
	d, err := srv.store.GetSPUWithSKUs(ctx, spuID)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "查询失败: "+err.Error())
		return
	}
	if d == nil {
		writeErr(w, http.StatusNotFound, "商品不存在")
		return
	}
	b, _ := json.Marshal(d)
	ttl := 10*time.Minute + time.Duration(rand.Int63n(int64(3*time.Minute)))
	_ = srv.rdb.Set(ctx, ckey, b, ttl)
	w.Header().Set("X-Cache", "miss")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
```

## 阶段 8：load-gen 核心实现

```go
func genCategory(ctx context.Context, db *sql.DB, t catTemplate, count, batch int) error {
	metas, err := loadMetas(ctx, db, t.CategoryID)
	if err != nil {
		return err
	}
	var keyMetas, saleMetas []attrMeta
	for _, m := range metas {
		if m.IsKey {
			keyMetas = append(keyMetas, m)
		}
		if m.IsSale {
			saleMetas = append(saleMetas, m)
		}
	}
	// 品牌枚举的文本→vid 映射
	textToVID := map[string]int64{}
	for _, m := range metas {
		for _, e := range m.Enums {
			textToVID[e.Text] = e.VID
		}
	}

	spuBuf := make([]string, 0, batch)
	skuBuf := make([]string, 0, batch)
	spuID, skuCode, totalSPU := int64(0), int64(0), 0
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	flush := func() error {
		if len(spuBuf) == 0 {
			return nil
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO spu (merchant_id,category_id,title,key_props,base_props,key_hash,publish_time) VALUES "+strings.Join(spuBuf, ",")); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO sku (spu_id,sku_code,sale_props,sale_hash,price,stock) VALUES "+strings.Join(skuBuf, ",")); err != nil {
			return err
		}
		spuBuf, skuBuf = spuBuf[:0], skuBuf[:0]
		return nil
	}

	for i := 0; i < count; i++ {
		brand := t.Brands[rng.Intn(len(t.Brands))]
		model := t.Models[rng.Intn(len(t.Models))]

		keyVals := map[int64]string{}
		for _, m := range keyMetas {
			if m.ValueType == "enum" {
				keyVals[m.ID] = fmt.Sprintf("%d", textToVID[brand])
			} else {
				keyVals[m.ID] = model
			}
		}
		keyHash := keyHashOf(t.CategoryID, keyVals)
		keyProps, _ := json.Marshal(keyVals)
		spuID++
		spuBuf = append(spuBuf, fmt.Sprintf("(1,%d,'%s','%s','{}','%s',NOW())",
			t.CategoryID, esc(brand+" "+model+" "+t.Suffix), esc(string(keyProps)), keyHash))

		combos := [][]enumVal{{}}
		for _, m := range saleMetas {
			var next [][]enumVal
			for _, c := range combos {
				for _, e := range m.Enums {
					next = append(next, append(append([]enumVal(nil), c...), e))
				}
			}
			combos = next
		}
		for _, combo := range combos {
			saleVals := map[int64]string{}
			for i, m := range saleMetas {
				saleVals[m.ID] = fmt.Sprintf("%d", combo[i].VID)
			}
			saleHash := keyHashOf(0, saleVals)
			saleProps, _ := json.Marshal(saleVals)
			skuCode++
			skuBuf = append(skuBuf, fmt.Sprintf("(%d,'SKU-G-%d','%s','%s',%.2f,%d)",
				spuID, skuCode, esc(string(saleProps)), saleHash,
				t.PriceBase+float64(rng.Intn(3000)), rng.Intn(500)))
		}
		totalSPU++
		if len(spuBuf) >= batch {
			if err := flush(); err != nil {
				return err
			}
			log.Printf("类目 %d：已生成 %d SPU", t.CategoryID, totalSPU)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	log.Printf("类目 %d 完成：SPU=%d", t.CategoryID, totalSPU)
	return nil
}

func keyHashOf(categoryID int64, vals map[int64]string) string {
	ids := make([]int64, 0, len(vals))
	for id := range vals {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", categoryID)
	for _, id := range ids {
		fmt.Fprintf(&b, "%d=%s;", id, vals[id])
	}
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func esc(s string) string { return strings.ReplaceAll(s, "'", "''") }

func loadMetas(ctx context.Context, db *sql.DB, categoryID int64) ([]attrMeta, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT id, value_type, is_key, is_sale, enum_values FROM attribute_meta WHERE category_id=? AND (is_key=1 OR is_sale=1)",
		categoryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []attrMeta
	for rows.Next() {
		var m attrMeta
		var isKey, isSale int
		var enumJSON []byte
		if err := rows.Scan(&m.ID, &m.ValueType, &isKey, &isSale, &enumJSON); err != nil {
			return nil, err
		}
		m.IsKey, m.IsSale = isKey == 1, isSale == 1
		if len(enumJSON) > 0 {
			_ = json.Unmarshal(enumJSON, &m.Enums)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
```
