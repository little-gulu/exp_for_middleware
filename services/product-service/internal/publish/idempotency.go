package publish

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Idempotency：发布接口幂等（业务背景：网络抖动重试时，别重复生成 SKU）。
//
// 核心机制（本任务要体会的）：Redis SET key val NX EX 86400 ——
//
//	NX = Not eXists：只有 key 不存在时才写，且"检查+写入"是原子的：
//	两个并发请求同时抢，只有一个能成功——这就是"抢占"。
//	拆成"先 GET 判断再 SET"会有竞态（两个请求同时通过判断→双写）。
//	EX = 过期秒数 = 幂等窗口（24h 后同 key 再提交会被当成新请求——时间窗口的取舍）。
type Idempotency struct{ rdb *redis.Client }

func NewIdempotency(addr string) *Idempotency {
	return &Idempotency{rdb: redis.NewClient(&redis.Options{Addr: addr})}
}

func (i *Idempotency) key(merchantID int64, reqID string) string {
	return fmt.Sprintf("idempotency:%d:%s", merchantID, reqID)
}

// TryAcquire：抢占幂等标记。true=本次是首次请求，可以继续处理；false=重复请求。
// 关键：必须把 SetNX 返回的 bool 原样传出去——丢掉它（用 _ 忽略）就会"每次都判成首次"，幂等失效。
func (i *Idempotency) TryAcquire(ctx context.Context, merchantID int64, reqID string) (bool, error) {
	return i.rdb.SetNX(ctx, i.key(merchantID, reqID), "1", 24*time.Hour).Result()
}

// SetResult：成功后记录本次请求产生的 spu_id（重复请求凭它返回"上次的结果"）。
func (i *Idempotency) SetResult(ctx context.Context, merchantID int64, reqID string, spuID int64) error {
	return i.rdb.Set(ctx, i.key(merchantID, reqID)+":spu", spuID, 24*time.Hour).Err()
}

// GetResult：重复请求取回首次结果。
func (i *Idempotency) GetResult(ctx context.Context, merchantID int64, reqID string) (int64, error) {
	v, err := i.rdb.Get(ctx, i.key(merchantID, reqID)+":spu").Result()
	if err == redis.Nil {
		return 0, nil // key 不存在不是错误
	}
	if err != nil {
		return 0, err
	}
	return strconv.ParseInt(v, 10, 64)
}
