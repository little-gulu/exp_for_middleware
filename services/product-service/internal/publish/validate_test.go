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
