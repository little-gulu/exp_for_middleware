# 计划 2：商品中心核心（product-service + load-gen）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用"商品发布/详情"这个业务背景，把 **Go 和 MySQL/Redis 组装**起来，通过一个可运行的服务体会：连接池、Redis 原子命令/bitmap/pipeline、缓存读写路径、幂等与防穿透等**中间件能力与边界**；load-gen 批量造数同时制造一条 **MySQL→binlog→CDC→Kafka** 的真实数据流供观察。

**背景说明（不重要，仅提供真实感）**：业务锚点是商品中心的 SPU/SKU/属性元数据模型（场景题只是把这种建模讲得最透的教材）。**任务名里的"元数据/归一/笛卡尔积"都是背景词汇**——它们只是让代码"有事可做"的载体，真正的学习对象是下表右侧的中间件能力与 Go 写法。

**Architecture:** 单个 Go module `product-service`，stdlib `net/http`（Go 1.22+ 方法路由），`go-sql-driver/mysql` + `redis/go-redis/v9`。测试直接打本机真实中间件（localhost:13306/16379），每任务 TDD。

**Tech Stack:** Go 1.26、net/http、go-sql-driver/mysql v1.8.1、go-redis/v9 v9.7.0

## 本计划真正练什么（学习主线对照表）

> 读任务列表时以本表为准：左列只是背景，右列才是本计划的目的。

| 任务 | 练的中间件能力与边界 | 练的 Go 写法 | 对应 bug 卡 |
|---|---|---|---|
| 1 骨架 | （无）服务基础 | **优雅退出**：`signal.NotifyContext` + `Shutdown` | G3 |
| 2 store | **MySQL**：连接池语义（max/idle/lifetime）、TINYINT 扫描 | `database/sql` 连接池参数、`rows.Scan` | G4、M1-M6 |
| 3 校验 | （纯逻辑）演示"**数据驱动 vs 代码分支**"设计原理 | `switch` 通用分支、`errors.Is` 错误哨兵 | — |
| 4 keyhash | （纯逻辑）确定性指纹 | `sort` + `md5` | — |
| 5 笛卡尔积 | （纯逻辑）组合枚举 | 递归回溯 | B6 |
| 6 幂等 | **Redis**：`SETNX NX EX` 的**原子抢占**与过期语义 | go-redis 客户端用法 | B4 |
| 7 布隆 | **Redis**：**bitmap** `SETBIT/GETBIT` 位图能力、`Pipeline` 批量命令；误判率数学=边界在哪 | pipeline 批量 | R1、R8 |
| 8 发布 API | **MySQL**：upsert 唯一键技巧（`ON DUPLICATE KEY UPDATE`）+ 以上全部**组装** | HTTP handler、依赖注入、JSON 编解码 | B5 |
| 9 详情 | **Redis 缓存读写路径**：TTL 语义、cache-aside、随机过期 | 随机 TTL、缓存命中响应头 | R1、R3、R4 |
| 10 load-gen | **MySQL binlog → CDC → Kafka 数据流**：批量写入的放大效应（Kafka UI 观察） | 批量多行 INSERT | K4、C1-C4 |

## 全局约束（同计划 1，沿用）

- 独立 module + 独立 Dockerfile（Dockerfile 在计划 5 统一写）。
- 代码注释约定：每块注释写「做什么 / 实现的需求 / 防止的问题(bug 编号) / 改错的后果」。
- 端口：product-service 8080；load-gen 无端口。
- MySQL `root:root123456@tcp(localhost:13306)/demo`；Redis `localhost:16379`（无密码）。
- 开发期 `go run` 跑宿主机。
- Git：每个任务一次 commit，末尾 `Co-Authored-By: Claude Code <noreply@anthropic.com>`。
- 注释中文；Go 1.26；Kafka 客户端纯 Go（本计划不涉及 Kafka）。

---

### Task 1: product-service 骨架（module + config + 优雅退出 + /healthz）

**Files:**
- Create: `services/product-service/go.mod`
- Create: `services/product-service/internal/config/config.go`
- Create: `services/product-service/main.go`

**Interfaces:**
- Produces: `config.Config{HTTPAddr, MySQLDSN, RedisAddr string}`；`main` 启动 HTTP 服务监听 8080，`GET /healthz` 返回 200；SIGINT/SIGTERM 时优雅退出（`signal.NotifyContext` + `srv.Shutdown`，5s 超时）。

- [ ] **Step 1: 初始化 module**

```bash
cd services/product-service && go mod init product-service
```

- [ ] **Step 2: 写 internal/config/config.go**

```go
package config

import "os"

// Config：进程级配置，全部来自环境变量（12-factor；Docker/k8s 部署时注入）
type Config struct {
	HTTPAddr  string // HTTP 监听地址（含端口）
	MySQLDSN  string // MySQL 连接串（宿主机开发用 13306；容器化后换 mysql:3306）
	RedisAddr string // Redis 地址（宿主机开发用 16379）
}

// Load：读环境变量并套默认值。默认值=本机开发环境（deploy/.env 对应值）
func Load() Config {
	return Config{
		HTTPAddr:  getenv("HTTP_ADDR", ":8080"),
		MySQLDSN:  getenv("MYSQL_DSN", "root:root123456@tcp(localhost:13306)/demo?charset=utf8mb4&parseTime=true"),
		RedisAddr: getenv("REDIS_ADDR", "localhost:16379"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
```

- [ ] **Step 3: 写 main.go**

```go
package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"product-service/internal/config"
)

func main() {
	cfg := config.Load()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second, // 防慢连接占资源（生产标配）
	}

	// 优雅退出：捕获 SIGINT/SIGTERM → Shutdown（等正在处理的请求完成，最长 5s）
	// 防的问题：G3 未优雅退出丢消息/丢请求；直接 os.Exit 即可复现该 bug
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Println("收到退出信号，开始优雅退出…")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("优雅退出超时: %v", err)
		}
	}()

	log.Printf("product-service listening on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	log.Println("已退出")
}
```

- [ ] **Step 4: 跑起来验证**

```bash
cd services/product-service && go mod tidy && go run . &
curl -s localhost:8080/healthz   # 期望: ok
kill -TERM %1                     # 期望: 日志出现"收到退出信号…已退出"
```

- [ ] **Step 5: Commit**

```bash
git add services/product-service/ && git commit -m "feat(product-service): 骨架 + 优雅退出 + healthz

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 2: MySQL 访问层 + 元数据读取

**Files:**
- Create: `services/product-service/internal/store/store.go`

**Interfaces:**
- Produces: `type Store struct{ db *sql.DB }`；`NewStore(dsn string) (*Store, error)`（open + Ping + 连接池参数）；`(s *Store) GetCategoryMetas(ctx context.Context, categoryID int64) ([]AttributeMeta, error)`。
- 类型定义（本包导出，后续任务复用）：
```go
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
```

- [ ] **Step 1: 写 store.go**

```go
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type EnumVal struct {
	VID  int64  `json:"vid"`
	Text string `json:"text"`
}

// AttributeMeta：attribute_meta 表一行 = 一条属性模板（spec §4.1）
// 元数据驱动：业务代码只认这套通用结构，不出现"颜色/尺码"等具体属性名（背景模型的约束，顺带演示"数据驱动 vs 代码分支"原理）
type AttributeMeta struct {
	ID         int64
	CategoryID int64
	Name       string
	ValueType  string
	IsKey      bool
	IsSale     bool
	IsSearch   bool
	IsFilter   bool
	Required   bool
	EnumValues []EnumVal
	Unit       string
}

type Store struct{ db *sql.DB }

// NewStore：打开连接并 Ping 一次（启动即失败，不带到运行期）
// 连接池参数：MaxOpen 50/MaxIdle 10/ConnMaxLifetime 30min
// 防的问题：G4 连接池耗尽（把这些参数调极端即可复现）
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

// GetCategoryMetas：拉某类目的全部属性模板（发布校验的输入）
// 实现的需求：元数据驱动——新增商品类型=新增类目+配属性，本查询不变
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
		var enumJSON []byte
		if err := rows.Scan(&m.ID, &m.CategoryID, &m.Name, &m.ValueType,
			&m.IsKey, &m.IsSale, &m.IsSearch, &m.IsFilter, &m.Required, &enumJSON, &m.Unit); err != nil {
			return nil, fmt.Errorf("scan attribute_meta: %w", err)
		}
		m.IsKey = m.IsKey == 1   // MySQL TINYINT → 转换语义
		m.IsSale = m.IsSale == 1
		m.IsSearch = m.IsSearch == 1
		m.IsFilter = m.IsFilter == 1
		m.Required = m.Required == 1
		if len(enumJSON) > 0 {
			if err := json.Unmarshal(enumJSON, &m.EnumValues); err != nil {
				return nil, fmt.Errorf("unmarshal enum_values: %w", err)
			}
		}
		metas = append(metas, m)
	}
	return metas, rows.Err()
}

func (s *Store) Close() error { return s.db.Close() }
```

- [ ] **Step 2: 写集成测试 store_test.go（打真实 MySQL）**

```go
package store

import (
	"context"
	"testing"
)

func TestGetCategoryMetasPhone(t *testing.T) {
	s, err := NewStore("root:root123456@tcp(localhost:13306)/demo?charset=utf8mb4&parseTime=true")
	if err != nil {
		t.Fatalf("连不上 demo-mysql（先跑 docker compose up -d）: %v", err)
	}
	defer s.Close()

	metas, err := s.GetCategoryMetas(context.Background(), 111) // 手机类目
	if err != nil {
		t.Fatalf("GetCategoryMetas: %v", err)
	}
	if len(metas) != 5 {
		t.Fatalf("手机类目应有 5 条属性模板, 实际 %d", len(metas))
	}
	// 找"颜色"属性验证枚举解析（测试用名字找，业务代码永远不用名字分支）
	for _, m := range metas {
		if m.Name == "颜色" {
			if !m.IsSale || !m.IsSearch || !m.IsFilter {
				t.Errorf("颜色应 is_sale/is_search/is_filter: %+v", m)
			}
			if len(m.EnumValues) != 4 {
				t.Errorf("颜色应有 4 个枚举值, 实际 %d", len(m.EnumValues))
			}
			if m.EnumValues[0].Text != "黑" {
				t.Errorf("第一个枚举值应是 黑, 实际 %s", m.EnumValues[0].Text)
			}
		}
	}
}
```

- [ ] **Step 3: 跑测试**

```bash
cd services/product-service && go get github.com/go-sql-driver/mysql && go mod tidy && go test ./internal/store/ -v -run TestGetCategoryMetasPhone
```
Expected: PASS（前提：demo-mysql 运行中）。

- [ ] **Step 4: Commit**

```bash
git add services/product-service/internal/store/ && git commit -m "feat(product-service): MySQL 访问层 + 类目属性模板读取

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 3: 元数据驱动校验（publish/validate.go）

**Files:**
- Create: `services/product-service/internal/publish/publish.go`（类型定义）
- Create: `services/product-service/internal/publish/validate.go`
- Test: `services/product-service/internal/publish/validate_test.go`

**Interfaces:**
- Produces: `type AttrValue struct{ AttrID, ValueID int64; Raw string }`；`func Validate(metas []store.AttributeMeta, attrs []AttrValue) error`

- [ ] **Step 1: 写类型定义 publish.go**

```go
package publish

import "product-service/internal/store"

// AttrValue：提交的一个属性值。
// enum 类属性用 ValueID（vid）；string/decimal/date 用 Raw 原值。
// 值用 vid 而非文本——改名不影响倒排与归一（bug B7 的正确基线）
type AttrValue struct {
	AttrID  int64
	ValueID int64
	Raw     string
}
```

- [ ] **Step 2: 先写失败的测试 validate_test.go**

```go
package publish

import (
	"errors"
	"testing"

	"product-service/internal/store"
)

func meta(id int64, name, vt string, key, sale, required bool, enums []store.EnumVal) store.AttributeMeta {
	return store.AttributeMeta{ID: id, Name: name, ValueType: vt, IsKey: key, IsSale: sale, Required: required, EnumValues: enums}
}

func TestValidateRequiredMissing(t *testing.T) {
	metas := []store.AttributeMeta{meta(1, "品牌", "enum", true, false, true, []store.EnumVal{{VID: 3001, Text: "Apple"}})}
	err := Validate(metas, nil)
	if err == nil || !errors.Is(err, ErrRequired) {
		t.Fatalf("缺必填属性应返回 ErrRequired, 实际 %v", err)
	}
}

func TestValidateIllegalEnum(t *testing.T) {
	metas := []store.AttributeMeta{meta(3, "颜色", "enum", false, true, true, []store.EnumVal{{VID: 1001, Text: "黑"}})}
	err := Validate(metas, []AttrValue{{AttrID: 3, ValueID: 9999}}) // 9999 不在枚举里
	if err == nil || !errors.Is(err, ErrIllegalEnum) {
		t.Fatalf("非法枚举应返回 ErrIllegalEnum, 实际 %v", err)
	}
}

func TestValidateOK(t *testing.T) {
	metas := []store.AttributeMeta{
		meta(1, "品牌", "enum", true, false, true, []store.EnumVal{{VID: 3001, Text: "Apple"}}),
		meta(2, "型号", "string", true, false, true, nil),
	}
	err := Validate(metas, []AttrValue{
		{AttrID: 1, ValueID: 3001},
		{AttrID: 2, Raw: "iPhone 15 Pro"},
	})
	if err != nil {
		t.Fatalf("合法提交不应报错: %v", err)
	}
}
```

- [ ] **Step 3: 跑测试确认失败**

```bash
cd services/product-service && go test ./internal/publish/ -v -run TestValidate
```
Expected: FAIL（Validate 未定义）。

- [ ] **Step 4: 写 validate.go 让测试通过**

```go
package publish

import (
	"errors"
	"fmt"

	"product-service/internal/store"
)

// 错误哨兵：调用方用 errors.Is 判断错误类型（业务错误码的底层）
var (
	ErrRequired    = errors.New("必填属性缺失")
	ErrIllegalEnum = errors.New("枚举值非法")
	ErrInvalidType = errors.New("属性值类型不匹配")
)

// Validate：元数据驱动的通用校验。
// 核心设计（数据驱动 vs 代码分支的原理演示）：switch 只认 value_type 这类"元数据的类型"，
// 代码里永远不出现 if name=="颜色" 之类的具体属性分支——新增类型=加配置，零改码。
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
			// int/decimal/range/date：本 demo 提交侧按原值透传，数值范围校验是扩展点
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

- [ ] **Step 5: 跑测试确认通过 + Commit**

```bash
go test ./internal/publish/ -v -run TestValidate   # 期望 3 个 PASS
git add services/product-service/internal/publish/ && git commit -m "feat(product-service): 元数据驱动校验（通用 switch，零具体属性分支）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 4: SPU 归一 key_hash（publish/keyhash.go）

**Files:**
- Create: `services/product-service/internal/publish/keyhash.go`
- Test: `services/product-service/internal/publish/keyhash_test.go`

**Interfaces:**
- Produces: `func KeyHash(categoryID int64, keyVals []AttrValue) string`（排序后 "cat:attr=vid;" MD5 十六进制 32 字符）。

- [ ] **Step 1: 先写失败的测试**

```go
package publish

import "testing"

func TestKeyHashOrderInsensitive(t *testing.T) {
	a := []AttrValue{{AttrID: 1, ValueID: 3001}, {AttrID: 2, Raw: "iPhone 15 Pro"}}
	b := []AttrValue{{AttrID: 2, Raw: "iPhone 15 Pro"}, {AttrID: 1, ValueID: 3001}}
	if KeyHash(111, a) != KeyHash(111, b) {
		t.Fatal("同一组关键属性，提交顺序不同，key_hash 必须一致（否则同款被判成两款）")
	}
}

func TestKeyHashDifferent(t *testing.T) {
	a := []AttrValue{{AttrID: 1, ValueID: 3001}, {AttrID: 2, Raw: "iPhone 15 Pro"}}
	b := []AttrValue{{AttrID: 1, ValueID: 3002}, {AttrID: 2, Raw: "Mate 60"}}
	if KeyHash(111, a) == KeyHash(111, b) {
		t.Fatal("不同属性组合不应得到相同 key_hash")
	}
}
```

- [ ] **Step 2: 跑测试确认失败（KeyHash 未定义）**

- [ ] **Step 3: 写 keyhash.go**

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
// 实现的需求：判断"这是不是同一款商品"，同款复用 SPU、不同款新建。
// 顺序无关：先按 AttrID 排序再拼接——商家提交顺序不同不影响归一。
// 防的问题：B5 唯一键焊死（本 hash 只做候选检索；唯一约束在 (merchant_id, category_id, key_hash)）
func KeyHash(categoryID int64, keyVals []AttrValue) string {
	sorted := append([]AttrValue(nil), keyVals...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].AttrID < sorted[j].AttrID })
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", categoryID)
	for _, v := range sorted {
		fmt.Fprintf(&b, "%d=%d;", v.AttrID, v.ValueID) // 用 vid 不用文本（bug B7）
	}
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
```

> 注：string 型关键属性（如"型号"）的 Raw 也要进 hash——string 型在 AttrValue 里没有 vid，需要把 Raw 也纳入指纹。实现时统一处理：enum 用 ValueID、string 用 Raw。见 Step 3 代码修正：
```go
func KeyHash(categoryID int64, keyVals []AttrValue) string {
	sorted := append([]AttrValue(nil), keyVals...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].AttrID < sorted[j].AttrID })
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", categoryID)
	for _, v := range sorted {
		if v.ValueID != 0 {
			fmt.Fprintf(&b, "%d=%d;", v.AttrID, v.ValueID)
		} else {
			fmt.Fprintf(&b, "%d=%s;", v.AttrID, v.Raw)
		}
	}
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
```

- [ ] **Step 4: 跑测试确认通过 + Commit**

```bash
go test ./internal/publish/ -v -run TestKeyHash   # 期望 2 个 PASS
git add services/product-service/internal/publish/ && git commit -m "feat(product-service): SPU 归一 key_hash（顺序无关指纹）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 5: SKU 笛卡尔积 + 护栏（publish/sku.go）

**Files:**
- Create: `services/product-service/internal/publish/sku.go`
- Test: `services/product-service/internal/publish/sku_test.go`

**Interfaces:**
- Produces: `func GenSKUs(dims [][]AttrValue) ([][]AttrValue, error)`；护栏：维度数 ≤3、每维值数 ≤50（超限返回 `ErrComboExplode`，对应 bug B6）。

- [ ] **Step 1: 先写失败的测试**

```go
package publish

import (
	"errors"
	"testing"
)

func TestGenSKUsCartesian(t *testing.T) {
	// 颜色×尺码 = 2×3 = 6 个 SKU
	dims := [][]AttrValue{
		{{AttrID: 3, ValueID: 1001}, {AttrID: 3, ValueID: 1002}},
		{{AttrID: 19, ValueID: 4001}, {AttrID: 19, ValueID: 4002}, {AttrID: 19, ValueID: 4003}},
	}
	got, err := GenSKUs(dims)
	if err != nil {
		t.Fatalf("GenSKUs: %v", err)
	}
	if len(got) != 6 {
		t.Fatalf("期望 6 个 SKU, 实际 %d", len(got))
	}
}

func TestGenSKUsGuardrailTooManyDims(t *testing.T) {
	dims := make([][]AttrValue, 4) // 4 个销售属性维度
	for i := range dims {
		dims[i] = []AttrValue{{AttrID: int64(i + 1), ValueID: 1}}
	}
	_, err := GenSKUs(dims)
	if err == nil || !errors.Is(err, ErrComboExplode) {
		t.Fatalf("超过 3 维应返回 ErrComboExplode, 实际 %v", err)
	}
}

func TestGenSKUsGuardrailTooManyValues(t *testing.T) {
	dims := [][]AttrValue{{make([]AttrValue, 51)}}
	_, err := GenSKUs(dims)
	if err == nil || !errors.Is(err, ErrComboExplode) {
		t.Fatalf("单维超过 50 值应返回 ErrComboExplode, 实际 %v", err)
	}
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 写 sku.go**

```go
package publish

import (
	"errors"
	"fmt"
)

// 护栏常量：防 SKU 笛卡尔积爆炸（bug B6：20色×15码×10版=3000 个 SKU 拖垮整条链路）
// 改小/删掉这两个判断即可复现 B6
const (
	maxSaleDims   = 3
	maxDimValues  = 50
)

var ErrComboExplode = errors.New("SKU 组合爆炸")

// GenSKUs：销售属性笛卡尔积（递归回溯；背景需求：规格组合生成 SKU）
// dims[i] = 第 i 个销售属性（如"颜色"）的所有取值
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

- [ ] **Step 4: 跑测试确认通过 + Commit**

```bash
go test ./internal/publish/ -v -run TestGenSKUs   # 期望 3 个 PASS
git add services/product-service/internal/publish/ && git commit -m "feat(product-service): SKU 笛卡尔积生成 + 爆炸护栏（bug B6 靶点）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 6: 发布幂等键（Redis）

**Files:**
- Create: `services/product-service/internal/publish/idempotency.go`
- Test: `services/product-service/internal/publish/idempotency_test.go`

**Interfaces:**
- Produces: `type Idempotency struct{ rdb *redis.Client }`；`NewIdempotency(addr string) *Idempotency`；`(i *Idempotency) TryAcquire(ctx, merchantID int64, reqID string) (bool, error)`；`(i *Idempotency) SetResult(ctx, merchantID int64, reqID string, spuID int64) error`；`(i *Idempotency) GetResult(ctx, merchantID int64, reqID string) (int64, error)`。

- [ ] **Step 1: 先写失败的测试**

```go
package publish

import (
	"context"
	"testing"
)

func TestIdempotencyAcquireOnce(t *testing.T) {
	idem := NewIdempotency("localhost:16379")
	ctx := context.Background()
	key := "itest:" + t.Name()

	ok, err := idem.TryAcquire(ctx, 1, key)
	if err != nil || !ok {
		t.Fatalf("第一次抢占应成功: ok=%v err=%v", ok, err)
	}
	ok, err = idem.TryAcquire(ctx, 1, key)
	if err != nil || ok {
		t.Fatalf("第二次抢占应失败（重复请求）: ok=%v err=%v", ok, err)
	}
	// 清理
	idem.rdb.Del(ctx, idem.key(1, key))
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 写 idempotency.go**

```go
package publish

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Idempotency：发布接口幂等键（防网络重试重复提交）。
// 实现的需求：网络抖动重试时，不重复生成 SKU（防 bug B4——去掉抢占即可复现"重试生成两套 SKU"）。
// 机制：SET key 1 NX EX 86400 原子抢占——NX=不存在才写；抢占失败=重复请求，返回首次结果。
type Idempotency struct{ rdb *redis.Client }

func NewIdempotency(addr string) *Idempotency {
	return &Idempotency{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (i *Idempotency) key(merchantID int64, reqID string) string {
	return fmt.Sprintf("idempotency:%d:%s", merchantID, reqID)
}

// TryAcquire：抢占幂等标记。true=本次是首次请求，可以继续处理
func (i *Idempotency) TryAcquire(ctx context.Context, merchantID int64, reqID string) (bool, error) {
	ok, err := i.rdb.SetNX(ctx, i.key(merchantID, reqID), "1", 24*time.Hour).Result()
	return ok, err
}

// SetResult：成功后记录本次请求产生的 spu_id（重复请求凭它返回"上次的结果"）
func (i *Idempotency) SetResult(ctx context.Context, merchantID int64, reqID string, spuID int64) error {
	return i.rdb.Set(ctx, i.key(merchantID, reqID)+":spu", spuID, 24*time.Hour).Err()
}

// GetResult：重复请求取回首次结果；不存在返回 0
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

- [ ] **Step 4: 跑测试确认通过 + Commit**

```bash
cd services/product-service && go get github.com/redis/go-redis/v9 && go mod tidy && go test ./internal/publish/ -v -run TestIdempotency   # 期望 PASS
git add services/product-service/internal/publish/ && git commit -m "feat(product-service): 发布幂等键（Redis SETNX 抢占，bug B4 靶点）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 7: 手搓布隆过滤器（bloom/bloom.go）

**Files:**
- Create: `services/product-service/internal/bloom/bloom.go`
- Test: `services/product-service/internal/bloom/bloom_test.go`

**Interfaces:**
- Produces: `func New(rdb *redis.Client, key string, m, k uint64) *Filter`；`(f *Filter) Add(ctx, id int64) error`；`(f *Filter) MayContain(ctx, id int64) (bool, error)`。
- 参数：n=10^6、p=1% → `m = -n·ln(p)/(ln2)² ≈ 9.6e6` 位、`k = m/n·ln2 ≈ 7`。

- [ ] **Step 1: 先写失败的测试**

```go
package bloom

import (
	"context"
	"math"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestAddAndMayContain(t *testing.T) {
	f := New(redis.NewClient(&redis.Options{Addr: "localhost:16379"}), "bf:test", 10000, 7)
	ctx := context.Background()
	if ok, _ := f.MayContain(ctx, 42); ok {
		t.Fatal("空过滤器不应命中任何元素")
	}
	if err := f.Add(ctx, 42); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if ok, _ := f.MayContain(ctx, 42); !ok {
		t.Fatal("加入过的元素必须命中（布隆不误报'存在'，只误报'不存在'）")
	}
}

// 误判率统计：加 10000 个元素，查 10000 个未加入的元素，误判率应远小于理论 1% 的数量级
func TestFalsePositiveRate(t *testing.T) {
	const n, m, k = 10000, 100000, 7 // m/n=10 → 理论误判率约 0.8%
	f := New(redis.NewClient(&redis.Options{Addr: "localhost:16379"}), "bf:fptest", m, k)
	ctx := context.Background()
	for i := int64(0); i < n; i++ {
		_ = f.Add(ctx, i)
	}
	fp := 0
	for i := int64(n); i < 2*n; i++ {
		if ok, _ := f.MayContain(ctx, i); ok {
			fp++
		}
	}
	rate := float64(fp) / n
	if rate > 0.05 {
		t.Fatalf("误判率 %.3f 超出预期（应约 0.8%%）", rate)
	}
	t.Logf("实测误判率 %.3f（理论约 %.3f）", rate, math.Pow(1-math.Exp(-k*n/m), k))
}
```

- [ ] **Step 2: 跑测试确认失败**

- [ ] **Step 3: 写 bloom.go（手搓核心，逐段注释原理）**

```go
package bloom

import (
	"context"
	"fmt"
	"hash/fnv"

	"github.com/redis/go-redis/v9"
)

// Filter：手搓布隆过滤器（Redis bitmap 版，不用 RedisBloom 模块）。
// 原理（docs/learning/bloom-filter.md 有推导）：
//   - m 位位数组 + k 个哈希；Add 把 k 个位置 SETBIT 1；MayContain 查 k 位全 1。
//   - 误判率 p ≈ (1-e^(-kn/m))^k：只误报"可能存在"、绝不漏报"肯定存在"；不可删除。
//   - 参数反推：n 元素、目标 p → m = -n·ln(p)/(ln2)²，k = m/n·ln2。
//     n=10^6、p=1% → m≈9.6M 位（约 1.2MB）、k≈7。
// 用途：GET /spu/{id} 先查布隆，不存在直接 404——挡住"查不存在商品"打穿 MySQL（bug R1 穿透的第一道防线）。
type Filter struct {
	rdb *redis.Client
	key string
	m   uint64 // 位数组长度
	k   uint64 // 哈希个数
}

func New(rdb *redis.Client, key string, m, k uint64) *Filter {
	return &Filter{rdb: rdb, key: key, m: m, k: k}
}

// locations：双重哈希 h_i(x) = (h1 + i*h2) mod m
// 用 fnv64a 两个不同 seed 得到 h1/h2——不引第三方哈希库，足够均匀
func (f *Filter) locations(id int64) []uint64 {
	h1 := hash64(id, 0x9e3779b9)
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

// Add：k 个位置 SETBIT 1（位图存在 Redis，进程重启不丢——S6 持久化的受益者之一）
func (f *Filter) Add(ctx context.Context, id int64) error {
	pipe := f.rdb.Pipeline()
	for _, loc := range f.locations(id) {
		pipe.SetBit(ctx, f.key, int64(loc), 1)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// MayContain：k 位全 1 才返回 true（false 是"肯定不存在"，true 是"可能存在"）
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

- [ ] **Step 4: 跑测试确认通过 + Commit**

```bash
go test ./internal/bloom/ -v   # 期望 2 个 PASS（注意误判率日志）
git add services/product-service/internal/bloom/ && git commit -m "feat(product-service): 手搓布隆过滤器（Redis bitmap + 双重哈希）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 8: 发布 API（串联全链路）

**Files:**
- Create: `services/product-service/internal/api/api.go`（router + Publish handler）
- Modify: `services/product-service/internal/store/store.go`（+ UpsertSPU / InsertSKUs）
- Modify: `services/product-service/main.go`（装配 store/idempotency/bloom + 挂路由）

**Interfaces:**
- Produces: `POST /publish`，请求体 `{"merchant_id":1,"client_req_id":"uuid","category_id":111,"title":"...","attrs":[{"attr_id":1,"value_id":3001},{"attr_id":2,"raw":"iPhone 15 Pro"},...]}`（销售属性按"每值一条"提交，同 attr_id 可多条）。
- 响应：`{"spu_id":123,"sku_count":6,"duplicate":false}`；重复请求 `duplicate=true` 且返回首次 spu_id。

- [ ] **Step 1: store.go 追加两个方法**

```go
// SPU / SKU 落库结构（本 demo 用字符串装 JSON，避免引入 ORM）
type SPU struct {
	ID         int64
	MerchantID int64
	CategoryID int64
	Title      string
	KeyProps   string // JSON: {"品牌":3001,"型号":"iPhone 15 Pro"}
	BaseProps  string // JSON
	KeyHash    string
}

type SKU struct {
	SpuID      int64
	SKUCode    string
	SaleProps  string // JSON: {"颜色":1001,"尺码":4002}
	SaleHash   string
	Price      float64
	Stock      int
}

// UpsertSPU：归一落库。ON DUPLICATE KEY UPDATE id=LAST_INSERT_ID(id) 技巧：
// 撞 (merchant_id, category_id, key_hash) 唯一键时返回已有行 id——同款复用 SPU 不重复建
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

// InsertSKUs：批量多行 INSERT（造数/发布的性能基线；单行循环=性能 bug 素材）
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
```

- [ ] **Step 2: 写 api.go（发布全链路）**

```go
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"

	"product-service/internal/bloom"
	"product-service/internal/publish"
	"product-service/internal/store"
)

const (
	bloomM = 9600000 // 布隆位数组长度（n=10^6, p=1% 反推）
	bloomK = 7       // 哈希个数
)

type Server struct {
	store  *store.Store
	rdb    *redis.Client      // 详情缓存/写接口用（Task 9 用到）
	idem   *publish.Idempotency
	filter *bloom.Filter
}

func NewRouter(s *store.Store, rdb *redis.Client, idem *publish.Idempotency, filter *bloom.Filter) http.Handler {
	srv := &Server{store: s, rdb: rdb, idem: idem, filter: filter}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("POST /publish", srv.Publish)
	mux.HandleFunc("GET /spu/{id}", srv.Detail)
	return mux
}

// writeJSON / writeErr：统一响应工具（业务错误码的生产写法）
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
// 存 vid 不存文本：文本改名不影响历史快照与倒排（bug B7 的正确基线）
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

type PublishReq struct {
	MerchantID  int64              `json:"merchant_id"`
	ClientReqID string             `json:"client_req_id"`
	CategoryID  int64              `json:"category_id"`
	Title       string             `json:"title"`
	Attrs       []publish.AttrValue `json:"attrs"`
}

type PublishResp struct {
	SPUID     int64 `json:"spu_id"`
	SKUCount  int   `json:"sku_count"`
	Duplicate bool  `json:"duplicate"`
}

// Publish：发布一个商品（spec §5.1 全链路）
// ① 幂等抢占 → ② 拉元数据 → ③ 元数据校验 → ④ SPU 归一 → ⑤ SKU 笛卡尔积 → ⑥ 落库 → ⑦ 布隆置位
func (srv *Server) Publish(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	var req PublishReq
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeErr(w, http.StatusBadRequest, "请求体解析失败: "+err.Error())
		return
	}
	if req.ClientReqID == "" {
		writeErr(w, http.StatusBadRequest, "client_req_id 必填（发布幂等的凭据，防 bug B4）")
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

	// ② 拉该类目属性模板（元数据驱动：代码不认识"颜色/尺码"，只认模板）
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

	// ④ SPU 归一：关键属性 → key_hash
	grouped := publish.GroupByAttrID(req.Attrs) // 按属性 id 聚合（销售属性"每值一条"提交）
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

	// ⑤ SKU 笛卡尔积（护栏在 GenSKUs 内，bug B6）
	combos, err := publish.GenSKUs(saleDims)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	skus := make([]store.SKU, 0, len(combos))
	for _, combo := range combos {
		saleHash := publish.KeyHash(0, combo) // 同套规则：排序+MD5
		skus = append(skus, store.SKU{
			SpuID:     spuID,
			SKUCode:   fmt.Sprintf("SKU-%d-%s", spuID, saleHash[:8]),
			SaleProps: propsJSON(combo),
			SaleHash:  saleHash,
			Price:     99.00, // demo 默认价；真实场景由商家提交，交易侧再校验
			Stock:     100,   // demo 默认库存
		})
	}
	if err := srv.store.InsertSKUs(ctx, skus); err != nil {
		writeErr(w, http.StatusInternalServerError, "SKU 落库失败: "+err.Error())
		return
	}

	// ⑥ 布隆置位（商品 ID 存在性索引，防 R1 穿透）
	if err := srv.filter.Add(ctx, spuID); err != nil {
		log.Printf("布隆置位失败 spu=%d: %v（不影响发布主流程）", spuID, err)
	}

	_ = srv.idem.SetResult(ctx, req.MerchantID, req.ClientReqID, spuID)
	writeJSON(w, PublishResp{SPUID: spuID, SKUCount: len(skus)})
}
```

- [ ] **Step 3: publish 包补 GroupByAttrID + 测试**

`services/product-service/internal/publish/group.go`：

```go
package publish

// GroupByAttrID：提交值按属性 id 聚合。
// 销售属性采用"每值一条"提交（颜色=黑 一条、颜色=白 一条），这里聚合成笛卡尔积的维度
func GroupByAttrID(attrs []AttrValue) map[int64][]AttrValue {
	m := make(map[int64][]AttrValue)
	for _, a := range attrs {
		m[a.AttrID] = append(m[a.AttrID], a)
	}
	return m
}
```

`group_test.go`：

```go
func TestGroupByAttrID(t *testing.T) {
	attrs := []AttrValue{
		{AttrID: 3, ValueID: 1001}, {AttrID: 3, ValueID: 1005},
		{AttrID: 4, ValueID: 2002},
	}
	m := GroupByAttrID(attrs)
	if len(m[3]) != 2 || len(m[4]) != 1 {
		t.Fatalf("聚合结果错误: %+v", m)
	}
}
```

- [ ] **Step 4: main.go 装配（store/rdb/idempotency/bloom → NewRouter）**

```go
	st, err := store.NewStore(cfg.MySQLDSN)
	if err != nil { log.Fatalf("mysql: %v", err) }
	defer st.Close()
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()
	idem := publish.NewIdempotency(cfg.RedisAddr)
	filter := bloom.New(rdb, "bf:spu", 9600000, 7)
	mux := api.NewRouter(st, rdb, idem, filter)
```

- [ ] **Step 5: 端到端手动验收（真发一个商品）**

```bash
cd services/product-service && go run . &
# 发一个"iPhone 15 Pro"（手机类目 111）：品牌=3001 Apple、型号=iPhone 15 Pro、
# 销售属性 颜色=[黑1001, 深空灰1005] × 内存=[128G 2001, 256G 2002] = 4 个 SKU
curl -s -X POST localhost:8080/publish -d '{
  "merchant_id":1, "client_req_id":"demo-001", "category_id":111, "title":"iPhone 15 Pro",
  "attrs":[{"attr_id":1,"value_id":3001},{"attr_id":2,"raw":"iPhone 15 Pro"},
           {"attr_id":3,"value_id":1001},{"attr_id":3,"value_id":1005},
           {"attr_id":4,"value_id":2001},{"attr_id":4,"value_id":2002}]}'
# 期望：{"spu_id":1,"sku_count":4,"duplicate":false}
# 重发同一 client_req_id → duplicate=true 且 spu_id 相同（幂等生效）
# 查库：spu 1 行、sku 4 行；Kafka UI 里 product.changed 出现 spu/sku 事件（CDC 自动同步！）
```

- [ ] **Step 6: Commit**

```bash
git add services/product-service/ && git commit -m "feat(product-service): 发布 API 全链路（幂等→校验→归一→笛卡尔积→落库→布隆）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 9: 详情 API（布隆 → Redis 缓存 → MySQL）

**Files:**
- Create: `services/product-service/internal/api/detail.go`
- Modify: `services/product-service/internal/store/store.go`（+ GetSPUWithSKUs）
- Test: `services/product-service/internal/api/detail_test.go`（打真实栈的端到端测试）

**Interfaces:**
- Produces: `GET /spu/{id}` → 200 JSON `{spu, skus[]}`；不存在 → 404。
- 缓存 key `spu:{id}`，TTL 10 分钟（随机 ±30% 防雪崩，bug R3 靶点）。

- [ ] **Step 1: store.go 追加 GetSPUWithSKUs**

```go
type SPUDetail struct {
	SPU
	SKUs []SKU
}

func (s *Store) GetSPUWithSKUs(ctx context.Context, spuID int64) (*SPUDetail, error) {
	var d SPUDetail
	err := s.db.QueryRowContext(ctx, `SELECT id, merchant_id, category_id, title, key_props, base_props, key_hash FROM spu WHERE id=?`, spuID).
		Scan(&d.ID, &d.MerchantID, &d.CategoryID, &d.Title, &d.KeyProps, &d.BaseProps, &d.KeyHash)
	if err == sql.ErrNoRows {
		return nil, nil // 不存在
	}
	if err != nil {
		return nil, err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT spu_id, sku_code, sale_props, sale_hash, price, stock FROM sku WHERE spu_id=?`, spuID)
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

- [ ] **Step 2: 写 detail.go（三级读路径）**

```go
// Detail：GET /spu/{id}
// 读路径三级（spec §5.3/§4.5）：
//   ① 布隆：查都不查直接 404（防 R1 穿透——大量不存在 ID 打穿 MySQL）
//   ② Redis 缓存 spu:{id}：命中直接返回（TTL 10min±30% 随机，防 R3 雪崩）
//   ③ MySQL 回源：写回缓存（Cache-Aside：先更新 DB 再删缓存的对应读侧）
func (srv *Server) Detail(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	spuID, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)

	// ① 布隆拦截（bug R1 修复点：去掉这段即可复现穿透）
	if ok, err := srv.filter.MayContain(ctx, spuID); err == nil && !ok {
		writeErr(w, http.StatusNotFound, "商品不存在")
		return
	}

	// ② 缓存
	ckey := fmt.Sprintf("spu:%d", spuID)
	if v, err := srv.rdb.Get(ctx, ckey).Result(); err == nil {
		w.Header().Set("X-Cache", "hit")
		w.Write([]byte(v))
		return
	}

	// ③ 回源
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
	ttl := 10*time.Minute + time.Duration(rand.Int63n(int64(3*time.Minute))) // ±30% 随机（R3）
	srv.rdb.Set(ctx, ckey, b, ttl)
	w.Header().Set("X-Cache", "miss")
	w.Write(b)
}
```

- [ ] **Step 3: 端到端验收**

```bash
curl -s localhost:8080/spu/1        # 第一次 X-Cache: miss（回源 MySQL 并写缓存）
curl -s localhost:8080/spu/1        # 第二次 X-Cache: hit（缓存命中）
curl -s localhost:8080/spu/999999   # 404（布隆拦截——从没上架过的 ID）
docker exec demo-redis redis-cli -p 6379 TTL spu:1   # 看到 600±180 秒
```

- [ ] **Step 4: Commit**

```bash
git add services/product-service/ && git commit -m "feat(product-service): 详情 API（布隆→缓存→回源三级读路径）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

### Task 10: load-gen 属性驱动造数

**Files:**
- Create: `tools/load-gen/go.mod`（module load-gen）
- Create: `tools/load-gen/main.go`

**Interfaces:**
- Produces: CLI `load-gen seed --products 100000 --batch 1000`：按 8 个叶子类目模板批量生成 SPU/SKU 并多行 INSERT；进度日志每 1 万行一条；`--zipf 0.9` 生成幂律订单（本计划只做 seed 造数，订单造数在计划 4 前补）。

- [ ] **Step 1: 写 main.go（完整生成逻辑）**

```go
package main

// load-gen：属性驱动造数工具（spec §7）
// 设计取舍：类目模板（品牌/型号池）硬编码在本工具里——它是"数据"，不是核心业务代码；
// "哪些属性是 key/sale、枚举值有哪些"从 attribute_meta 表读——生成逻辑本身与具体属性解耦。
// 直写 MySQL 的原因：批量多行 INSERT 是造百万行的唯一可行路径；
// 且直写同样进 binlog → CDC → Kafka（计划 3 的 indexer 会消费到全部数据，正好喂搜索链路）。

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const dsn = "root:root123456@tcp(localhost:13306)/demo?charset=utf8mb4&parseTime=true"

// catTemplate：一个叶子类目的造数模板（真实感数据池）
type catTemplate struct {
	CategoryID int64
	Brands     []string // 品牌枚举文本（生成时查 vid）
	Models     []string // 型号/款号池
	Suffix     string   // 标题后缀
	PriceBase  float64  // 基准价
}

var templates = []catTemplate{
	{111, []string{"Apple", "华为", "小米", "海信"}, []string{"15 Pro", "Mate 60", "14 Ultra", "A7", "Fold 3", "Note 12"}, "手机", 4999},
	{112, []string{"Apple", "华为", "小米"}, []string{"Pad 11", "MatePad 12", "Pad 6 Pro", "Mini 8"}, "平板", 2999},
	{121, []string{"Apple", "华为", "联想", "小米"}, []string{"Book Air 13", "MateBook 14", "ThinkPad X1", "RedmiBook 15"}, "笔记本", 5999},
	{211, []string{"优衣库", "太平鸟"}, []string{"D-2401", "D-2402", "D-2403", "D-2404"}, "T恤", 99},
	{212, []string{"优衣库", "太平鸟"}, []string{"J-3101", "J-3102", "J-3103"}, "夹克", 399},
	{221, []string{"太平鸟"}, []string{"L-2201", "L-2202", "L-2203"}, "连衣裙", 299},
	{311, []string{"美的", "海尔", "格力"}, []string{"BCD-300", "BCD-450", "BCD-600"}, "冰箱", 2999},
	{312, []string{"美的", "海尔"}, []string{"XQB-80", "XQG-100", "XQG-120"}, "洗衣机", 1999},
}

type attrMeta struct {
	ID        int64
	ValueType string
	IsKey     bool
	IsSale    bool
	Enums     []enumVal
}
type enumVal struct {
	VID  int64  `json:"vid"`
	Text string `json:"text"`
}

func main() {
	products := flag.Int("products", 10000, "目标 SPU 数量（默认 1 万，全量 10 万）")
	batch := flag.Int("batch", 1000, "每批 INSERT 行数")
	flag.Parse()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(20)
	ctx := context.Background()

	perCat := *products / len(templates)
	for _, t := range templates {
		if err := genCategory(ctx, db, t, perCat, *batch); err != nil {
			log.Fatalf("类目 %d 造数失败: %v", t.CategoryID, err)
		}
	}
	log.Printf("造数完成：目标 SPU=%d", *products)
}

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

	// 品牌枚举的文本→vid 映射（品牌是 key 属性之一，按名字找）
	textToVID := map[string]int64{}
	for _, m := range metas {
		for _, e := range m.Enums {
			textToVID[e.Text] = e.VID
		}
	}

	spuBuf := make([]string, 0, batch)
	skuBuf := make([]string, 0, batch)
	spuID := int64(0)
	skuCode := int64(0)
	totalSPU := 0

	flush := func() error {
		if len(spuBuf) == 0 {
			return nil
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO spu (merchant_id,category_id,title,key_props,base_props,key_hash,publish_time) VALUES "+strings.Join(spuBuf, ",")); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx, "INSERT INTO sku (spu_id,sku_code,sale_props,sale_hash,price,stock) VALUES "+strings.Join(skuBuf, ",")); err != nil {
			return err
		}
		spuBuf, skuBuf = spuBuf[:0], skuBuf[:0]
		return nil
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	for i := 0; i < count; i++ {
		brand := t.Brands[rng.Intn(len(t.Brands))]
		model := t.Models[rng.Intn(len(t.Models))]

		// 关键属性：品牌(vid) + 型号/款号(文本)
		keyVals := map[int64]string{}
		for _, m := range keyMetas {
			switch m.ValueType {
			case "enum":
				keyVals[m.ID] = fmt.Sprintf("%d", textToVID[brand]) // 品牌枚举
			default:
				keyVals[m.ID] = model // string 型 key（型号/款号）
			}
		}
		keyHash := keyHashOf(t.CategoryID, keyVals)
		keyProps, _ := json.Marshal(keyVals)
		title := brand + " " + model + " " + t.Suffix

		spuID++
		spuBuf = append(spuBuf, fmt.Sprintf("(1,%d,'%s','%s','{}','%s',NOW())",
			t.CategoryID, esc(title), esc(string(keyProps)), keyHash))

		// 销售属性枚举笛卡尔积
		combos := [][]enumVal{{}}
		for _, m := range saleMetas {
			var next [][]enumVal
			for _, c := range combos {
				for _, e := range m.Enums {
					nc := append(append([]enumVal(nil), c...), e)
					next = append(next, nc)
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
			price := t.PriceBase + float64(rng.Intn(3000))
			stock := rng.Intn(500)
			skuBuf = append(skuBuf, fmt.Sprintf("(%d,'SKU-G-%d','%s','%s',%.2f,%d)",
				spuID, skuCode, esc(string(saleProps)), saleHash, price, stock))
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

// keyHashOf：与 product-service 相同的指纹算法（独立 module，按约定复制一份）
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

- [ ] **Step 2: 跑造数并验证**

```bash
cd tools/load-gen && go mod init load-gen && go get github.com/go-sql-driver/mysql && go mod tidy
go run . seed --products 10000 --batch 1000   # 先小批量试
# 验证：SPU/SKU 行数、抽查一条 sale_props JSON
docker exec demo-mysql mysql -uroot -proot123456 -e "SELECT COUNT(*) AS spu_cnt FROM demo.spu; SELECT COUNT(*) AS sku_cnt FROM demo.sku; SELECT spu_id, sale_props, price, stock FROM demo.sku LIMIT 2;"
# 打开 Kafka UI：product.changed 消息量暴涨（CDC 正在自动同步这批造数！这是链路 A 的第一次全量演练）
# 全量：go run . seed --products 100000 --batch 2000（SPU 10 万，SKU 约 30-50 万，几分钟）
```

- [ ] **Step 3: Commit**

```bash
git add tools/load-gen/ && git commit -m "feat(load-gen): 属性驱动批量造数（SPU 10万/SKU 50万 量级）

Co-Authored-By: Claude Code <noreply@anthropic.com>"
```

---

## 计划 2 验收清单（全部通过 = 计划 2 完成）

- [ ] `go test ./...` 全绿（validate/keyhash/sku/idempotency/bloom）
- [ ] POST /publish 发布手机（黑/深空灰 × 128G/256G = 4 SKU）→ spu 1 行 + sku 4 行
- [ ] 同 client_req_id 重发 → duplicate=true 同 spu_id
- [ ] 非法枚举/缺必填 → 400 且错误信息带属性名
- [ ] GET /spu/{id} → miss → hit；GET /spu/999999 → 404（布隆）
- [ ] **Kafka UI 里 product.changed 出现刚才发布的 spu/sku 事件**（CDC 自动同步，写路径零侵入）
- [ ] load-gen 造数 10 万 SPU → 库行数正确、Kafka UI 消息量可见增长
- [ ] 中途 kill product-service 再重启：无 goroutine 泄漏日志、能继续服务

## 后续计划（路线图不变）

- 计划 3 · 搜索链路：indexer（消费 product.changed → 拍平写 ES + product_attr_index）+ search-service（SearchEngine 双实现/facet/缓存/热搜 ZSET/三道防线）。验收：发布 → 两个引擎都能搜到。
- 计划 4 · 订单链路：order-service（状态机）+ order-worker（库存/销量榜/幂等消费）。验收：订单全生命周期。
- 计划 5 · 演练工具集：burst 压测、kafka-inspector、chaos 完善、50 故障卡手册、学习材料、Dockerfile 打包。
