package publish

// GroupByAttrID：把提交值按属性 id 聚合。
// 业务背景：销售属性"每值一条"提交（颜色=黑 一条、颜色=白 一条），
// 发布流程需要把它们聚成"每个属性一个列表"（即笛卡尔积的维度）。
//
// TODO 你的实现：一次遍历 + map[int64][]AttrValue 即可（约 5 行）。
func GroupByAttrID(attrs []AttrValue) map[int64][]AttrValue {
	grouped := make(map[int64][]AttrValue)
	for _, a := range attrs {
		grouped[a.AttrID] = append(grouped[a.AttrID], a)
	}
	return grouped
}
