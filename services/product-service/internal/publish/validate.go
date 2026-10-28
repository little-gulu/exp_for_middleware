package publish

import (
	"errors"

	"product-service/internal/store"
)

// 实现 TODO 时需要补的 import：fmt（%w 包装错误信息）

// 校验错误哨兵：调用方用 errors.Is(err, ErrRequired) 判断错误类别，而不是比较错误字符串。
// 这是 Go 错误处理的标准姿势：fmt.Errorf 的 %w 把哨兵包进错误链，errors.Is 沿链查找。
var (
	ErrRequired    = errors.New("必填属性缺失")
	ErrIllegalEnum = errors.New("枚举值非法")
	ErrInvalidType = errors.New("属性值类型不匹配")
)

// Validate：元数据驱动的通用校验。
// 设计原理（本任务要体会的）：代码只认 value_type 这类"描述数据的数据"，
// 永远不出现 if name=="颜色" 这种具体属性分支——新增属性类型=加配置，零改码。
//
// TODO 你的实现：
//   1. 把 attrs 按 AttrID 建索引（map[int64]AttrValue，校验时 O(1) 查找）
//   2. 遍历 metas：
//      - m.Required 且提交里没有 → return fmt.Errorf("%w: %s(id=%d)", ErrRequired, m.Name, m.ID)
//      - 有值则 switch m.ValueType：
//          case "enum":  v.ValueID 必须在 m.EnumValues 里（写个 hasEnum 小助手，线性扫即可）
//          case "string": v.Raw 非空
//          default:      v.Raw 非空
//   提示：
//     - 错误信息带上属性名和 id，发布方才能定位是哪个属性填错了
//     - %w 是 errors.Is 生效的关键，别用 %v
func Validate(metas []store.AttributeMeta, attrs []AttrValue) error {
	panic("TODO: 你的实现")
}

func hasEnum(enums []store.EnumVal, vid int64) bool {
	panic("TODO: 你的实现")
}
