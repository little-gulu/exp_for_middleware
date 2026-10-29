// Package bloom：手搓布隆过滤器（Redis bitmap 版，不用 RedisBloom 模块）。
//
// 概念速览（详细推导见 docs/learning/plan2/guide.md 阶段 2）：
//   - 一个 m 位的大位数组 + k 个哈希函数
//   - Add(x)：把 x 的 k 个哈希位置全置 1
//   - MayContain(x)：查这 k 个位置是否全 1——全 1 说"可能存在"，有 0 说"肯定不存在"
//   - 误判率 p ≈ (1-e^(-kn/m))^k：只误报"存在"，绝不漏报
//   - 不可删除：清了某位会误伤共用该位的其它元素
//   - 参数反推：n 元素、目标误判 p → m = -n·ln(p)/(ln2)²，k = m/n·ln2
//     （n=10^6、p=1% → m≈9.6M 位≈1.2MB、k≈7）
//
// 与 Redis 的关系（本任务要体会的）：
//   - SETBIT/GETBIT 把一个大 key 当位数组用——布隆的"位数组"落哪、怎么存，由你决定
//   - Pipeline：k 个命令打包成一次网络往返（k 次往返 vs 1 次的差别就是 RTT 累积）
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

// locations：把 id 映射成 k 个位位置（双重哈希 h_i = (h1 + i*h2) mod m）。
// 双重哈希 = 用同一个哈希函数取两个不同 seed 的结果，线性组合出 k 个"看起来独立"的哈希，
// 不用引入第三方哈希库。
//
// TODO 你的实现：
//  1. h1 := hash64(id, 0x9e3779b9)；h2 := hash64(id, 0x85ebca6b)（seed 是两个不同大常数即可）
//  2. 循环 i=0..k-1：位置 = (h1 + i*h2) % m，收集进切片
//     提示：uint64 运算溢出是回绕，无符号数没有负数问题
func (f *Filter) locations(id int64) []uint64 {
	h1 := hash64(id, 0x9e3779b9)
	h2 := hash64(id, 0x85ebca6b)
	locs := make([]uint64, 0, f.k) // 预分配 k 个位置
	for i := uint64(0); i < f.k; i++ {
		locs = append(locs, (h1+i*h2)%f.m)
	}
	return locs
}

// hash64：fnv-1a 哈希 + murmur3 fmix64 终结器。
// 为什么必须加终结器（今天的实测教训）：布隆取 mod m 用的是哈希值的低位；
// fnv 对短 key 的低位雪崩不足，位置会聚集——实测误判率 7.7% vs 理论 0.8%（差 9 倍）。
// fmix64 把整个 64 位再搅一遍，低位也充分混合。
func hash64(id int64, seed uint64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(fmt.Sprintf("%d:%d", id, seed)))
	return mix64(h.Sum64())
}

// mix64：murmur3 的 fmix64 终结器（纯算术、无依赖、全位雪崩）
func mix64(x uint64) uint64 {
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Add：把 id 的 k 个位置全 SETBIT 1。
//
// TODO 你的实现：用 Pipeline 把 k 个 SetBit 打包一次执行。
//
//	提示：redis.Pipelined(ctx, func(pipe redis.Pipeliner) error { ... }) 是最常用的封装
func (f *Filter) Add(ctx context.Context, id int64) error {
	_, err := f.rdb.Pipelined(ctx, func(pipe redis.Pipeliner) error {
		for _, loc := range f.locations(id) {
			pipe.SetBit(ctx, f.key, int64(loc), 1)
		}
		return nil
	})
	return err
}

// MayContain：k 个位置全 1 → true（"可能存在"）；任一为 0 → false（"肯定不存在"）。
//
// TODO 你的实现：手动 Pipeline（方式 B——需要收集每条命令的结果）：
//
//	pipe := f.rdb.Pipeline()
//	对每个位置：cmds = append(cmds, pipe.GetBit(ctx, f.key, int64(loc)))
//	pipe.Exec(ctx) 后逐个检查 cmds[i].Val() == 0 → 有 0 就 return false, nil
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
