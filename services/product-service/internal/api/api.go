// Package api：HTTP 层（业务背景：商品发布与详情接口）。
// 本任务要体会的：Go 服务怎么把前面写好的"积木"（store/publish/bloom/idempotency）
// 组装成对外接口——构造函数依赖注入、context 透传（r.Context() 一路传到底）、错误分类返回。
package api

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/redis/go-redis/v9"

	"product-service/internal/bloom"
	"product-service/internal/publish"
	"product-service/internal/store"
)

// 实现 TODO 时需要补的 import：
//   fmt（sku_code/缓存 key 拼接）、log（布隆置位失败日志）、
//   math/rand（缓存 TTL 随机偏移）、time（TTL 计算）

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

// Publish：POST /publish —— 发布一个商品，7 步流程。
//
// TODO 你的实现（按注释骨架补全每一步）：
//   ① 解析请求体（json.NewDecoder(r.Body).Decode(&req)）；ClientReqID 为空 → 400
//   ② 幂等抢占：TryAcquire 失败 → GetResult 取首次 spu_id → 返回 {spu_id, duplicate:true}
//   ③ 拉类目模板：GetCategoryMetas；空 → 400 "类目不存在或无属性模板"
//   ④ 校验：publish.Validate(metas, req.Attrs)；失败 → 400（错误信息带属性名）
//   ⑤ 归类：publish.GroupByAttrID(req.Attrs) → 按 m.IsKey/m.IsSale/其它 分成 keyVals/baseVals/saleDims
//   ⑥ SPU 归一：publish.KeyHash(categoryID, keyVals) → UpsertSPU
//   ⑦ SKU：publish.GenSKUs(saleDims)（护栏在函数内）→ 每个组合 sale_hash=KeyHash(0, combo) → InsertSKUs
//   ⑧ 布隆置位：filter.Add(spuID)——失败只打日志不阻塞（布隆是优化，降级可接受）
//   ⑨ SetResult + 返回 {spu_id, sku_count, duplicate:false}
//   提示：
//     - ctx := r.Context() 一路透传（取消/超时会顺 ctx 传导到 DB/Redis）
//     - 错误分类：errors.Is(err, publish.ErrRequired) → 400；其余按 500 处理
func (srv *Server) Publish(w http.ResponseWriter, r *http.Request) {
	panic("TODO: 你的实现")
}

// Detail：GET /spu/{id} —— 三级读路径：布隆 → 缓存 → 回源。
//
// TODO 你的实现（按注释骨架补全）：
//   ① r.PathValue("id") + strconv.ParseInt 拿 spuID；非法 → 400
//   ② 布隆：MayContain=false → 404（防大量不存在 ID 打穿 MySQL——bug 卡 R1）
//   ③ Redis 缓存 spu:{id}：命中 → 响应头 X-Cache: hit → 原样写回
//   ④ 回源：GetSPUWithSKUs；nil → 404；err → 500
//   ⑤ 写缓存：TTL = 10*time.Minute + 随机 0~3min（加随机偏移防齐过期——bug 卡 R3）→ X-Cache: miss 返回
//   提示：缓存 value 用 json.Marshal(d) 的字节串
func (srv *Server) Detail(w http.ResponseWriter, r *http.Request) {
	panic("TODO: 你的实现")
}
