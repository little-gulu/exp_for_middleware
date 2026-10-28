package bloom

import (
	"context"
	"math"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestAddAndMayContain(t *testing.T) {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:16379"})
	t.Cleanup(func() { rdb.Del(context.Background(), "bf:test") })
	f := New(rdb, "bf:test", 10000, 7)
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

// 误判率统计：加 10000 个元素，查 10000 个未加入的元素。
// m/n=10、k=7 → 理论误判率约 0.8%。实测应远小于 5%（有随机性，留足余量）。
func TestFalsePositiveRate(t *testing.T) {
	const n, m, k = 10000, 100000, 7
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:16379"})
	t.Cleanup(func() { rdb.Del(context.Background(), "bf:fptest") })
	f := New(rdb, "bf:fptest", m, k)
	ctx := context.Background()

	for i := int64(0); i < n; i++ {
		if err := f.Add(ctx, i); err != nil {
			t.Fatalf("Add %d: %v", i, err)
		}
	}
	fp := 0
	for i := int64(n); i < 2*n; i++ {
		if ok, _ := f.MayContain(ctx, i); ok {
			fp++
		}
	}
	rate := float64(fp) / n
	if rate > 0.05 {
		t.Fatalf("误判率 %.3f 超出预期", rate)
	}
	t.Logf("实测误判率 %.3f（理论约 %.3f）", rate, math.Pow(1-math.Exp(-k*float64(n)/m), k))
}
