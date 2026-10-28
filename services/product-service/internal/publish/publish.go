// Package publish：商品发布的领域逻辑。
// 业务背景（不重要）：商品属性千差万别，用"属性元数据"统一描述——本包就是处理这套逻辑的纯代码。
// 注意：本包类型校验等核心逻辑不 import 中间件客户端，阶段 1 的单测无需 Docker。
package publish

// AttrValue：提交的一个属性值。
// enum 类属性用 ValueID（vid：枚举值 id，稳定标识——文本改名不影响数据）；
// string/decimal/date 类用 Raw 存原值。
type AttrValue struct {
	AttrID  int64
	ValueID int64
	Raw     string
}
