package publish

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// KeyHash：关键属性指纹（背景需求：判断"是不是同一款商品"，同款复用不重复建）。
// 两个关键性质（本任务要体会的）：
//  1. 确定性：同一组输入永远同一结果（哈希的基本性质）
//  2. 顺序无关：提交顺序不同，指纹必须一致——否则同款会被判成两款
//
// TODO 你的实现：
//  1. 拷贝一份 keyVals（append([]AttrValue(nil), keyVals...)——别改调用方的切片！）并按 AttrID 升序排序
//  2. 拼字符串：fmt.Fprintf "categoryID:" + 每个 "attrID=值;"
//     （enum 用 ValueID、其余用 Raw——与 vid/文本分工一致）
//  3. md5.Sum → hex 编码（32 字符）
//     提示：
//     - sort.Slice(sorted, func(i,j int) bool { return sorted[i].AttrID < sorted[j].AttrID })
//     - crypto/md5 的 md5.Sum 返回 [16]byte；encoding/hex 的 hex.EncodeToString 转字符串
func KeyHash(categoryID int64, keyVals []AttrValue) string {
	sorted := append([]AttrValue(nil), keyVals...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].AttrID < sorted[j].AttrID })
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", categoryID)
	for _, v := range sorted {
		if v.ValueID != 0 {
			fmt.Fprintf(&b, "%d=%d;", v.AttrID, v.ValueID) // enum 用 vid
		} else {
			fmt.Fprintf(&b, "%d=%s;", v.AttrID, v.Raw) // string 用原值
		}
	}
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}
