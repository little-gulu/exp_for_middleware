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
	dims := [][]AttrValue{make([]AttrValue, 51)} // 外层切片的一个元素 = 51 个零值 AttrValue 的切片
	_, err := GenSKUs(dims)
	if err == nil || !errors.Is(err, ErrComboExplode) {
		t.Fatalf("单维超过 50 值应返回 ErrComboExplode, 实际 %v", err)
	}
}
