# 计划 2 实战练习：商品发布服务

> **学习主线**（背景是商品发布，学习对象是右边）：Go 怎么组装 MySQL/Redis、中间件的能力与边界在哪。
> 答案在 `answers.md`——**先自己做，测试全绿后再对**。卡住先看本文件「提示」，超过 15 分钟再看答案。

## 规则

1. 按下面的顺序图，一个文件一个文件地做。
2. 每个任务：读本文件的「概念」→ 打开带 `TODO: 你的实现` 的文件 → 补全 → 跑预置测试。
3. **测试全绿 = 做对了**（测试已全部写好，是判定器）。绿了再对答案，对比思路差异。
4. 结构体/签名/测试我全部给好了——你写的是函数体里的实现。
5. 卡住随时问我；做完一个阶段提交一次 git。

## 顺序图（先写什么 → 再写什么）

```
阶段 0  初始化：go.mod 已就绪（无需动手）
         │
阶段 1  纯逻辑热身（publish 包）—— 不需要 Docker，纯 Go 单测
         │   1a 类型定义 publish.go     ← 已给，先读一遍（后面全依赖它）
         │   1b validate.go  + 测试      ← 题目 1：错误哨兵 + switch 分派
         │   1c keyhash.go   + 测试      ← 题目 2：确定性 + 顺序无关
         │   1d group.go     + 测试      ← 题目 3：map 聚合（热身）
         │   1e sku.go       + 测试      ← 题目 4：递归回溯 + 护栏
         ▼
阶段 2  bloom.go + 测试                 ← 题目 5：手搓布隆（Redis bitmap）
         │                               第一个碰中间件的任务，详见下方详解
阶段 3  idempotency.go + 测试           ← 题目 6：Redis SETNX 原子抢占
         │
阶段 4  store.go + 测试                 ← 题目 7：MySQL 连接池/upsert/批量
         │
阶段 5  api.go                          ← 题目 8：组装（发布 7 步 + 详情三级读）
         │
阶段 6  main.go                         ← 已给，读一遍看装配
         │
阶段 7  端到端验收：发布 → Kafka UI 看到 CDC 事件
         │
阶段 8  load-gen                        ← 题目 9：批量造数 → 观察数据流
```

**依赖关系决定了顺序**：类型 → 纯逻辑 → 接 Redis → 接 MySQL → 组装。前 5 个题目不需要 Docker 在跑，后 4 个需要（`docker compose up -d`）。

## 阶段 1：纯逻辑热身（publish 包）

### 1a 类型定义（已给，读一遍即可）

`internal/publish/publish.go`：`AttrValue`（一次属性提交）的定义。要点：

- `ValueID`（vid）与 `Raw` 的分工：枚举类属性用 vid（数字 id），文本类用原值。
- **为什么值用 vid 不用文本**：文本会随运营改名变（"深空灰"→"钛灰"），vid 不变——这就是后面 bug 卡 B7 的根基。

### 1b 题目 1：validate.go —— 错误哨兵 + switch 分派

**概念：errors.Is 与哨兵错误**

Go 里判断"这是什么错"不靠比较错误字符串，靠哨兵（`var ErrRequired = errors.New(...)`）+ `fmt.Errorf("%w", sentinel)` 包装 + `errors.Is(err, sentinel)` 判断。`%w` 会把哨兵"藏"进错误链里，`errors.Is` 能沿链找到它。调用的上层（api 层）就可以写 `if errors.Is(err, publish.ErrRequired) { 400 }`。

**概念：数据驱动 vs 代码分支**（本计划的第一个设计原理）

这份校验代码只认识 `m.ValueType`（"enum"/"string"/…）——它是**描述数据的数据**。代码里永远不会出现 `if m.Name == "颜色"`。好处：数据库里新增一种属性类型（比如"range 区间"），只需要在 switch 里加一个 case，甚至不用加——所有属性一视同仁。

**提示（卡住再看）**
1. 先建 `map[int64]AttrValue` 索引，校验时 O(1) 查。
2. 遍历 metas 时注意：**缺提交**（required 且 map 里没有）和**值非法**（有但不对）是两类错误。
3. `hasEnum` 小助手：线性扫枚举即可（枚举值最多几十个，不值得二分）。

**验证**：`cd services/product-service && go test ./internal/publish/ -run TestValidate -v` → 3 个 PASS。

### 1c 题目 2：keyhash.go —— 确定性指纹

**概念**：MD5 之类哈希的用途不是加密，是**压缩 + 指纹**——任意输入 → 固定长度串，同输入必同输出。这里用它做"同款判定"：一组关键属性 → 32 字符指纹，撞指纹 ≈ 同款。

**提示**
1. 先 `sort.Slice` 按 AttrID 升序——这是"顺序无关"的关键。
2. **拷贝切片再排序**：别把调用方的切片顺序改掉（副作用是 bug 温床）。
3. 拼接时 enum 用 `ValueID`、string 用 `Raw`（和 vid/文本分工一致）。
4. `md5.Sum([]byte(s))` 返回 `[16]byte`，`hex.EncodeToString(sum[:])` 转 32 字符。

**验证**：`go test ./internal/publish/ -run TestKeyHash -v` → 2 个 PASS。

### 1d 题目 3：group.go —— map 聚合（热身）

一句话：同 attr_id 的多条提交聚成一组。5 行代码的题，顺手热个身。

**验证**：`go test ./internal/publish/ -run TestGroupByAttrID -v` → 1 个 PASS。

### 1e 题目 4：sku.go —— 递归回溯 + 护栏

**概念：组合爆炸与护栏**。2 维×2 值=4，3 维×20 值=8000——指数增长。真实生产的护栏是"≤3 维 × ≤50 值"，超了就报错而不是硬算（bug 卡 B6 就是把护栏拆掉的后果）。

**提示**
1. 递归骨架：`rec(depth, cur)`，`depth == len(dims)` 时是叶子，产出一种组合。
2. **`append([]AttrValue(nil), cur...)` 必须拷贝**：Go 的 append 可能复用底层数组，不拷贝的话所有组合会指向同一块内存，最后全变成同一个值——这是 Go 切片最经典的坑。
3. 护栏检查放最前面，错误用 `fmt.Errorf("%w: 维度 %d > %d", ErrComboExplode, ...)`。

**验证**：`go test ./internal/publish/ -run TestGenSKUs -v` → 3 个 PASS。

## 阶段 2：题目 5 —— 手搓布隆过滤器（Redis bitmap）

**这是本计划的重头戏，引导写详细点。**

### 2.1 先想清楚问题

商品详情接口要快速回答"这个 ID 存在吗"。全查 MySQL 太慢；全塞 Redis 占内存（一个商品详情几百字节，1000 万个就几 GB）。布隆用**约 1.2MB**（n=100 万、1% 误判）就敢说"这个 ID 大概率存在 / 肯定不存在"。

### 2.2 原理（五句话）

1. 一个 m 位的位数组，初始全 0；k 个哈希函数。
2. **Add(x)**：算出 x 的 k 个哈希位置，全置 1。
3. **MayContain(x)**：查这 k 个位置——**全 1 → "可能存在"；有 0 → "肯定不存在"**。
4. 为什么可能误报：不同元素的位会撞（你的 3 个位置恰好都被别人置过）。误判率 `p ≈ (1-e^(-kn/m))^k`。
5. 为什么绝不漏报：存在的元素，它的位一定被自己置过——所以"肯定不存在"永远可信。**不可删除**也是同一原因：清了别人的位会误伤。

### 2.3 参数怎么定（你要会反推）

- 元素数 n、目标误判率 p → `m = -n·ln(p)/(ln2)²`、`k = m/n·ln2`。
- 代入 n=10^6、p=1% → m≈9.6×10^6 位（约 1.2MB）、k≈7。
- 测试里的误判率统计会验证这个数学：m/n=10、k=7 时理论误判率 ≈0.8%，实测应远小于 5%。
- **bug 卡 R8 就是把这套参数配错**（按 10 万元素设计却灌 100 万），误判率飙升的后果。

### 2.4 为什么落在 Redis bitmap

Redis 的 `SETBIT key offset 1` / `GETBIT key offset` 让你把一个大 key 当位数组用：offset 是第几位，Redis 内部按字节串存。布隆的"位数组"放 Redis 里，进程重启不丢（RDB/AOF 持久化的受益者之一），多实例共享同一个过滤器。

### 2.5 双重哈希（不引第三方库的 k 个哈希）

- `h1 = fnv(id, seed1)`、`h2 = fnv(id, seed2)`（标准库 `hash/fnv`，两个不同 seed）。
- 第 i 个哈希：`(h1 + i*h2) % m`——线性组合出 k 个"看起来独立"的值，数学上足够用。
- 标准库 fnv 在 `hash/fnv`，`fnv.New64a()` 返回一个 `hash.Hash64`。

### 2.6 Pipeline：为什么值得学

Add 一次要 SETBIT 7 次。一次一条命令 = 7 次网络往返（每次往返 ≈ 0.1~1ms RTT）；**Pipeline = 7 条命令打包成一次往返**。Redis 单线程处理快，瓶颈在网络，所以批量命令是 Redis 性能优化的第一课。go-redis 里两种写法：

```go
// 方式 A：Pipelined 封装
_, err := rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
    pipe.SetBit(ctx, key, off1, 1)
    pipe.SetBit(ctx, key, off2, 1)
    return nil
})
// 方式 B：手动 Pipeline（适合收集每条命令的结果，MayContain 要用这个）
pipe := rdb.Pipeline()
cmd1 := pipe.GetBit(ctx, key, off1)
cmd2 := pipe.GetBit(ctx, key, off2)
_, err := pipe.Exec(ctx)
// cmd1.Val(), cmd2.Val() 才是结果
```

MayContain 需要**收集每个 GETBIT 的结果**，用方式 B。

### 2.7 提示（卡住再看）

1. `locations`：先实现，单独在测试里验证"k 个位置都在 [0,m) 内且确定"。
2. `Add`：方式 A；`MayContain`：方式 B，逐个检查 `cmd.Val() == 0`。
3. `hash64` 已给（fnv 封装），直接用。
4. 测试会统计误判率——跑通后**看那行日志**：实测 vs 理论，这就是布隆的"边界"具象化。

**验证**：`go test ./internal/bloom/ -v` → 2 个 PASS（注意读误判率日志）。需要 Docker 里 redis 在跑（`docker compose up -d redis`）。

## 阶段 3：题目 6 —— idempotency.go（Redis SETNX 原子抢占）

**概念：为什么 SETNX 而不是"先 GET 再 SET"**

网络重试可能把同一个请求发两次，服务端要"同一 client_req_id 只处理一次"。朴素写法是：GET 标记 → 没有 → 处理 → SET 标记。**竞态**：两个并发重试同时 GET 到"没有"，都往下处理 → 双写。`SET key val NX EX ttl` 把"检查+写入"合成**一个原子命令**（NX = 不存在才写），并发抢同一 key 只有一个成功——这就是"抢占"语义。`EX` 是过期秒数 = 幂等窗口，防标记永久残留。

**提示**
1. go-redis 的方法名就是 `SetNX(ctx, key, val, ttl)`。
2. `GetResult` 里 `err == redis.Nil` 表示 key 不存在——不是错误，返回 `(0, nil)`。
3. key 设计：`idempotency:{merchantID}:{reqID}`，结果存在 `...:spu` 后缀 key 里（防覆盖标记本身）。

**验证**：`go test ./internal/publish/ -run TestIdempotency -v` → 1 个 PASS。

## 阶段 4：题目 7 —— store.go（MySQL 连接池 / upsert / 批量）

### 4.1 连接池参数（NewStore 已给，读懂它们）

```go
db.SetMaxOpenConns(50)               // 最多同时开 50 个连接——打满后新请求排队/超时（bug 卡 G4）
db.SetMaxIdleConns(10)               // 空闲时保留 10 个，避免频繁建连（建连=TCP握手+鉴权，很贵）
db.SetConnMaxLifetime(30 * time.Minute) // 单个连接最长寿命——超时回收，防止连上已被杀掉的旧连接
```

`database/sql` 的连接池是**懒建立**的：没人用就不建。压测时 `db.Stats()` 能看池子状态。

### 4.2 题目要写的 4 个方法

**GetCategoryMetas**（读属性模板）——SQL 已给，练的是 rows.Scan：
- Scan 的顺序必须与 SELECT 列序一致；`IFNULL(unit,'')` 把 NULL 转空串。
- MySQL 的 TINYINT 扫进 Go 是 int（1/0），要手动转 bool（`m.IsKey = isKey == 1`）。
- `enum_values` 是 JSON 列 → `[]byte` → `json.Unmarshal` 进 `[]EnumVal`。

**UpsertSPU**（SPU 归一落库）——练的是一个 SQL 技巧：
```sql
INSERT INTO spu (...) VALUES (...)
ON DUPLICATE KEY UPDATE id = LAST_INSERT_ID(id), title = VALUES(title)
```
撞唯一键时不是报错，而是更新并让 `LAST_INSERT_ID()` 返回**已存在那行的 id**——一行 SQL 同时实现"新建或复用"。

**InsertSKUs**（批量多行 INSERT）——练的是网络往返：
- 一条 SQL 塞 N 组 VALUES（`INSERT ... VALUES (?,?,...),(?,?,...)...`）vs 循环 N 次单条 INSERT——N 倍网络往返的差别。批量是百万行造数的唯一可行路径。

**GetSPUWithSKUs**（spu + 它的 sku）——练的是 1+N 查询的组合：先 `QueryRow` 拿 SPU（`sql.ErrNoRows` 表示不存在，返回 `(nil, nil)`），再 `Query` 拿 SKU 列表。

**提示**：`strings.Builder` 拼批量 SQL；占位符个数要和值数一致。

**验证**：`go test ./internal/store/ -v` → 1 个 PASS（需要 demo-mysql 在跑）。

## 阶段 5：题目 8 —— api.go（组装）

**概念：依赖注入 + context 透传**

`NewRouter` 把 store/rdb/idem/filter 一次传进来（构造函数注入），handler 用 `r.Context()` 拿请求上下文一路传到数据库/Redis——这是 Go 服务的标准姿势，取消/超时会顺着这个 ctx 传导。

**Publish 的 7 步流程**（注释里已给骨架，按顺序补全）：
① 幂等抢占 → ② 拉类目模板 → ③ 校验 → ④ GroupByAttrID 分类 → ⑤ KeyHash+UpsertSPU → ⑥ GenSKUs+InsertSKUs → ⑦ 布隆置位 → SetResult 返回。

**Detail 的三级读路径**：布隆（不存在→404）→ Redis 缓存（命中→直接回）→ MySQL（回源+写缓存）。TTL 记得**加随机偏移**（防同一时刻齐过期——bug 卡 R3）。

**提示**：`writeJSON`/`writeErr`/`propsJSON` 已给；错误分类用 `errors.Is`（第 1 题学的哨兵在这里派上用场）。

**验证**：`go build ./...` 编译过 → `go run .` → 手动验收（命令见阶段 7）。

## 阶段 6：main.go（已给，读懂装配）

读一遍，看 5 块积木怎么拼成一个进程：config 加载 → store/rdb 初始化 → router 装配 → 优雅退出。

## 阶段 7：端到端验收（发布 → CDC → Kafka UI）

```bash
cd services/product-service && go run . &

# 发布一个 iPhone 15 Pro：品牌=Apple(3001)、型号=iPhone 15 Pro、颜色=[黑1001,深空灰1005]×内存=[128G 2001,256G 2002]
curl -s -X POST localhost:8080/publish -d '{
  "merchant_id":1, "client_req_id":"demo-001", "category_id":111, "title":"iPhone 15 Pro",
  "attrs":[{"attr_id":1,"value_id":3001},{"attr_id":2,"raw":"iPhone 15 Pro"},
           {"attr_id":3,"value_id":1001},{"attr_id":3,"value_id":1005},
           {"attr_id":4,"value_id":2001},{"attr_id":4,"value_id":2002}]}'
# 期望 {"spu_id":1,"sku_count":4,"duplicate":false}
# 再发一次同样请求 → duplicate=true（幂等生效）

curl -s localhost:8080/spu/1   # 两次：先 miss 后 hit（看 X-Cache 响应头）
curl -s localhost:8080/spu/999999   # 404（布隆拦截）

# ★ 打开 http://localhost:8088（Kafka UI）→ Topics → product.changed：
#   你刚才发布的 spu/sku 事件已经在那里了——CDC 在你不知情的情况下完成了同步
```

## 阶段 8：题目 9 —— load-gen（造数 → 观察数据流）

批量造 SPU 10 万 / SKU 几十万。**重点不在代码**（结构体和模板池已给），在于造完后的观察：

1. 造数期间看 Kafka UI 的 `product.changed` 消息量暴涨——**批量 INSERT 的每一行都在经 binlog → CDC → Kafka**，这就是"数据流"的实感。
2. 造数后 `SELECT COUNT(*) FROM spu/sku` 对数。
3. 想想：几十万条事件涌进 Kafka，如果消费者处理慢会怎样？（这就是计划 3 的 indexer 和 bug 卡 K4 的舞台。）

**验证**：`cd tools/load-gen && go run . seed --products 10000 --batch 1000`（先小批量）→ 查库对数 → Kafka UI 观察。

---

## 做完后

- 全部测试绿了、端到端验收过了 → 找我**批改**：把 git diff 给我看（或直接说"做完了"），我逐个文件 review，和参考答案对照，指出可以改进的地方。
- 做完一个阶段就 commit 一次，git 历史就是你的学习轨迹。
