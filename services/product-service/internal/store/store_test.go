package store

import (
	"context"
	"testing"
)

// 需要 demo-mysql 在跑（localhost:13306）
func TestGetCategoryMetasPhone(t *testing.T) {
	s, err := NewStore("root:root123456@tcp(localhost:13306)/demo?charset=utf8mb4&parseTime=true")
	if err != nil {
		t.Fatalf("连不上 demo-mysql（先 docker compose up -d）: %v", err)
	}
	defer s.Close()

	metas, err := s.GetCategoryMetas(context.Background(), 111) // 手机类目
	if err != nil {
		t.Fatalf("GetCategoryMetas: %v", err)
	}
	if len(metas) != 5 {
		t.Fatalf("手机类目应有 5 条属性模板, 实际 %d", len(metas))
	}
	// 找"颜色"验证枚举解析（测试用名字找；业务代码永远不用名字分支——那是 Validate 的设计原理）
	found := false
	for _, m := range metas {
		if m.Name == "颜色" {
			found = true
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
	if !found {
		t.Error("没找到'颜色'属性")
	}
}
