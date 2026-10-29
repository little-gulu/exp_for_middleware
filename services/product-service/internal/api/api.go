// Package api：HTTP 层（业务背景：商品发布与详情接口）。
// 本任务要体会的：Go 服务怎么把前面写好的"积木"（store/publish/bloom/idempotency）
// 组装成对外接口——构造函数依赖注入、context 透传（r.Context() 一路传到底）、错误分类返回。
package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"product-service/internal/bloom"
	"product-service/internal/publish"
	"product-service/internal/store"
)

const (
	bloomM = 9600000 // 布隆位数组长度（n=10^6, p=1% 反推：m = -n·ln(p)/(ln2)²）
	bloomK = 7       // 哈希个数（k = m/n·ln2）
)

type Server struct {
	store  *store.Store
	rdb    *redis.Client
	idem   *publish.Idempotency
	filter *bloom.Filter
}

// NewRouter：依赖注入——所有依赖在启动时传入，handler 不自己 new 客户端
func NewRouter(s *store.Store, rdb *redis.Client, idem *publish.Idempotency, filter *bloom.Filter) http.Handler {
	srv := &Server{store: s, rdb: rdb, idem: idem, filter: filter}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /publish", srv.Publish)
	mux.HandleFunc("GET /spu/{id}", srv.Detail)
	return mux
}

// writeJSON / writeErr：统一响应工具
func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code) // 先写状态码再写 body（顺序反了客户端会拿到 200）
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// propsJSON：属性快照 JSON（key=attr_id，enum 存 vid、其余存原值）
// 存 vid 不存文本：文本改名不影响历史快照与倒排（bug 卡 B7 的正确基线）
func propsJSON(attrs []publish.AttrValue) string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		if a.ValueID != 0 {
			m[strconv.FormatInt(a.AttrID, 10)] = strconv.FormatInt(a.ValueID, 10)
		} else {
			m[strconv.FormatInt(a.AttrID, 10)] = a.Raw
		}
	}
	b, _ := json.Marshal(m)
	return string(b)
}

// PublishReq / PublishResp：请求与响应结构
type PublishReq struct {
	MerchantID  int64               `json:"merchant_id"`
	ClientReqID string              `json:"client_req_id"`
	CategoryID  int64               `json:"category_id"`
	Title       string              `json:"title"`
	Attrs       []publish.AttrValue `json:"attrs"`
}

type PublishResp struct {
	SPUID     int64 `json:"spu_id"`
	SKUCount  int   `json:"sku_count"`
	Duplicate bool  `json:"duplicate"`
}

// Publish：POST /publish —— 发布一个商品，7 步流程：
// ① 幂等抢占 → ② 拉类目模板 → ③ 元数据校验 → ④ 属性归类 → ⑤ SPU 归一落库
// → ⑥ SKU 笛卡尔积落库 → ⑦ 布隆置位 + 记录结果
func (srv *Server) Publish(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context() // 透传：取消/超时会顺 ctx 传导到 DB/Redis

	var req PublishReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if req.ClientReqID == "" {
		writeErr(w, http.StatusBadRequest, "client_req_id 必填（发布幂等的凭据）")
		return
	}

	// ① 幂等：抢不到标记=重复请求 → 返回首次结果（防网络重试重复生成 SKU，bug 卡 B4）
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

	// ③ 元数据驱动校验（错误分类：哨兵 → 400）
	if err := publish.Validate(metas, req.Attrs); err != nil {
		if errors.Is(err, publish.ErrRequired) || errors.Is(err, publish.ErrIllegalEnum) || errors.Is(err, publish.ErrInvalidType) {
			writeErr(w, http.StatusBadRequest, "属性校验失败: "+err.Error())
		} else {
			writeErr(w, http.StatusInternalServerError, "属性校验失败: "+err.Error())
		}
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

	// ⑤ SPU 归一落库（撞唯一键=同款 → 复用已有行）
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

	// ⑥ SKU 笛卡尔积（护栏在 GenSKUs 内，bug 卡 B6）+ 批量落库
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

// Detail：GET /spu/{id} —— 三级读路径：布隆 → 缓存 → 回源。
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

	// ③ 回源 MySQL + 写缓存（TTL 加随机偏移防齐过期，bug 卡 R3）
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
	ttl := 10*time.Minute + time.Duration(rand.Int63n(int64(3*time.Minute))) // 10min ± 3min 随机
	_ = srv.rdb.Set(ctx, ckey, b, ttl)
	w.Header().Set("X-Cache", "miss")
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(b)
}
