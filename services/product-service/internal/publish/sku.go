package publish

import (
	"errors"
	"fmt"
)

// 实现 TODO 时需要补的 import：fmt（护栏错误信息）

// 护栏常量：防组合爆炸（业务背景：颜色20×尺码15×版本10=3000 个 SKU 会拖垮整条链路）。
// 本任务要体会的：递归枚举是"指数增长"，必须有人为护栏——把这两个值改掉/删掉判断即可复现 bug 卡 B6。
const (
	maxSaleDims  = 3
	maxDimValues = 50
)

var ErrComboExplode = errors.New("组合爆炸")

// GenSKUs：销售属性笛卡尔积。dims[i] = 第 i 个销售属性的所有取值；返回所有组合。
//
// TODO 你的实现：
//  1. 护栏：len(dims) > maxSaleDims 或任一维 len(d) > maxDimValues
//     → return nil, fmt.Errorf("%w: ...", ErrComboExplode, ...)
//  2. 递归回溯：
//     rec(depth, cur)：
//     depth == len(dims) → 叶子：out = append(out, append([]AttrValue(nil), cur...))
//     否则 → 对 dims[depth] 每个值 v：rec(depth+1, append(cur, v))
//     提示：append 可能复用底层数组——产出每个组合必须独立拷贝，
//     否则所有组合会指向同一块内存、最后全变成同一个值（Go 切片最经典的坑）
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
