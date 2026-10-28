package publish

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// 实现 TODO 时需要补的 import：time（24h 过期）、strconv（GetResult 解析 spu_id）

// Idempotency：发布接口幂等（业务背景：网络抖动重试时，别重复生成 SKU）。
//
// 核心机制（本任务要体会的）：Redis SET key val NX EX 86400 ——
//   NX = Not eXists：只有 key 不存在时才写，且"检查+写入"是原子的：
//   两个并发请求同时抢，只有一个能成功——这就是"抢占"。
//   拆成"先 GET 判断再 SET"会有竞态（两个请求同时通过判断→双写）。
//   EX = 过期秒数 = 幂等窗口（24h 后同 key 再提交会被当成新请求——时间窗口的取舍）。
type Idempotency struct{ rdb *redis.Client }

func NewIdempotency(addr string) *Idempotency {
	return &Idempotency{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (i *Idempotency) key(merchantID int64, reqID string) string {
	return fmt.Sprintf("idempotency:%d:%s", merchantID, reqID)
}

// TryAcquire：抢占幂等标记。true=本次是首次请求，可以继续处理；false=重复请求。
//
// TODO 你的实现：go-redis 的方法名就是 SetNX(ctx, key, val, ttl)。
//   提示：ttl 用 24*time.Hour；返回值是 (bool, error)
func (i *Idempotency) TryAcquire(ctx context.Context, merchantID int64, reqID string) (bool, error) {
	panic("TODO: 你的实现")
}

// SetResult：成功后记录本次请求产生的 spu_id（重复请求凭它返回"上次的结果"）。
//
// TODO 你的实现：SET key+":spu" = spu_id，同样 24h 过期。
func (i *Idempotency) SetResult(ctx context.Context, merchantID int64, reqID string, spuID int64) error {
	panic("TODO: 你的实现")
}

// GetResult：重复请求取回首次结果。
//
// TODO 你的实现：
//   1. GET key+":spu"
//   2. err == redis.Nil（key 不存在）→ return (0, nil)——这不是错误
//   3. 其它 err 上抛；成功 strconv.ParseInt(v, 10, 64)
func (i *Idempotency) GetResult(ctx context.Context, merchantID int64, reqID string) (int64, error) {
	panic("TODO: 你的实现")
}
