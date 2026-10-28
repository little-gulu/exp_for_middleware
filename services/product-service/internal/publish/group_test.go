package publish

import "testing"

func TestGroupByAttrID(t *testing.T) {
	attrs := []AttrValue{
		{AttrID: 3, ValueID: 1001}, {AttrID: 3, ValueID: 1005},
		{AttrID: 4, ValueID: 2002},
	}
	m := GroupByAttrID(attrs)
	if len(m[3]) != 2 || len(m[4]) != 1 {
		t.Fatalf("聚合结果错误: %+v", m)
	}
}
