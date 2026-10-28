package publish

import "testing"

func TestKeyHashOrderInsensitive(t *testing.T) {
	a := []AttrValue{{AttrID: 1, ValueID: 3001}, {AttrID: 2, Raw: "iPhone 15 Pro"}}
	b := []AttrValue{{AttrID: 2, Raw: "iPhone 15 Pro"}, {AttrID: 1, ValueID: 3001}}
	if KeyHash(111, a) != KeyHash(111, b) {
		t.Fatal("同一组关键属性，提交顺序不同，key_hash 必须一致（否则同款被判成两款）")
	}
}

func TestKeyHashDifferent(t *testing.T) {
	a := []AttrValue{{AttrID: 1, ValueID: 3001}, {AttrID: 2, Raw: "iPhone 15 Pro"}}
	b := []AttrValue{{AttrID: 1, ValueID: 3002}, {AttrID: 2, Raw: "Mate 60"}}
	if KeyHash(111, a) == KeyHash(111, b) {
		t.Fatal("不同属性组合不应得到相同 key_hash")
	}
}
