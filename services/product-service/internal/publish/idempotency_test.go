package publish

import (
	"context"
	"testing"
)

// 需要 demo-redis 在跑（localhost:16379）
func TestIdempotencyAcquireOnce(t *testing.T) {
	idem := NewIdempotency("localhost:16379")
	ctx := context.Background()
	reqID := "itest-" + t.Name()
	t.Cleanup(func() {
		idem.rdb.Del(ctx, idem.key(1, reqID), idem.key(1, reqID)+":spu")
	})

	ok, err := idem.TryAcquire(ctx, 1, reqID)
	if err != nil || !ok {
		t.Fatalf("第一次抢占应成功: ok=%v err=%v", ok, err)
	}
	ok, err = idem.TryAcquire(ctx, 1, reqID)
	if err != nil || ok {
		t.Fatalf("第二次抢占应失败（重复请求）: ok=%v err=%v", ok, err)
	}
}
