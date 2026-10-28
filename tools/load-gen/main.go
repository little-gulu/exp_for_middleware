// load-gen：属性驱动造数工具。
// 设计取舍：类目模板（品牌/型号池）硬编码在本工具里——它是"数据"，不是核心业务代码；
// "哪些属性是 key/sale、枚举值有哪些"从 attribute_meta 表读——生成逻辑与具体属性解耦。
// 直写 MySQL 的原因：批量多行 INSERT 是造百万行的唯一可行路径；
// 且直写同样进 binlog → CDC → Kafka（造数同时喂给计划 3 的搜索链路）。
//
// 学习主线（本任务要体会的）：批量写入的放大效应——
// 造数期间去 Kafka UI 看 product.changed 的消息量暴涨，那就是"数据流"的实感。
package main

import (
	"context"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"

	_ "github.com/go-sql-driver/mysql"
)

// 实现 genCategory 时需要补的 import：math/rand（随机品牌/价格/库存）、time（随机种子）

const dsn = "root:root123456@tcp(localhost:13306)/demo?charset=utf8mb4&parseTime=true"

// catTemplate：一个叶子类目的造数模板（真实感数据池）
type catTemplate struct {
	CategoryID int64
	Brands     []string // 品牌枚举文本（生成时查 vid）
	Models     []string // 型号/款号池
	Suffix     string   // 标题后缀
	PriceBase  float64  // 基准价
}

var templates = []catTemplate{
	{111, []string{"Apple", "华为", "小米", "海信"}, []string{"15 Pro", "Mate 60", "14 Ultra", "A7", "Fold 3", "Note 12"}, "手机", 4999},
	{112, []string{"Apple", "华为", "小米"}, []string{"Pad 11", "MatePad 12", "Pad 6 Pro", "Mini 8"}, "平板", 2999},
	{121, []string{"Apple", "华为", "联想", "小米"}, []string{"Book Air 13", "MateBook 14", "ThinkPad X1", "RedmiBook 15"}, "笔记本", 5999},
	{211, []string{"优衣库", "太平鸟"}, []string{"D-2401", "D-2402", "D-2403", "D-2404"}, "T恤", 99},
	{212, []string{"优衣库", "太平鸟"}, []string{"J-3101", "J-3102", "J-3103"}, "夹克", 399},
	{221, []string{"太平鸟"}, []string{"L-2201", "L-2202", "L-2203"}, "连衣裙", 299},
	{311, []string{"美的", "海尔", "格力"}, []string{"BCD-300", "BCD-450", "BCD-600"}, "冰箱", 2999},
	{312, []string{"美的", "海尔"}, []string{"XQB-80", "XQG-100", "XQG-120"}, "洗衣机", 1999},
}

type attrMeta struct {
	ID        int64
	ValueType string
	IsKey     bool
	IsSale    bool
	Enums     []enumVal
}
type enumVal struct {
	VID  int64  `json:"vid"`
	Text string `json:"text"`
}

func main() {
	products := flag.Int("products", 10000, "目标 SPU 数量（默认 1 万，全量 10 万）")
	batch := flag.Int("batch", 1000, "每批 INSERT 行数")
	flag.Parse()

	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatal(err)
	}
	db.SetMaxOpenConns(20)
	ctx := context.Background()

	perCat := *products / len(templates)
	for _, t := range templates {
		if err := genCategory(ctx, db, t, perCat, *batch); err != nil {
			log.Fatalf("类目 %d 造数失败: %v", t.CategoryID, err)
		}
	}
	log.Printf("造数完成：目标 SPU=%d", *products)
}

// genCategory：对一个类目生成 count 个 SPU + 其全部 SKU。
//
// TODO 你的实现（骨架和思路如下）：
//   1. loadMetas 读该类目 key/sale 属性（函数在下方，已给）
//   2. 建品牌"文本→vid"映射（枚举在 attribute_meta 的 enum_values 里）
//   3. 循环 count 次，每次：
//      a. 随机挑 brand + model；关键属性拼 keyVals map（enum 用 vid、string 用 model 文本）
//      b. keyHash := keyHashOf(categoryID, keyVals)（已给）；title = brand+" "+model+" "+Suffix
//      c. spuID 自增；spuBuf 追加一行 "(1,{cat},'{title}','{keyProps}','{}','{keyHash}',NOW())"
//      d. 销售属性枚举笛卡尔积（双层循环展开，或递归）→ 每个组合拼 saleVals map
//      e. skuBuf 追加 "(spuID,'SKU-G-{n}','{saleProps}','{saleHash}',{price},{stock})"
//         price = PriceBase + 随机 0~3000；stock = 随机 0~500
//   4. spuBuf 满 batch 行 → flush() 两条批量 INSERT（spu 一条、sku 一条）→ 清空缓冲 + 进度日志
//   5. 循环结束再 flush 一次（尾巴）
//   提示：
//     - esc() 把单引号转义（标题可能含单引号）
//     - 每批先插 spu 再插 sku（sku.spu_id 引用 spu.id——顺序反了会外键错乱/孤儿行）
func genCategory(ctx context.Context, db *sql.DB, t catTemplate, count, batch int) error {
	panic("TODO: 你的实现")
}

// keyHashOf：与 product-service 相同的指纹算法（独立 module，按约定复制一份）
func keyHashOf(categoryID int64, vals map[int64]string) string {
	ids := make([]int64, 0, len(vals))
	for id := range vals {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	var b strings.Builder
	fmt.Fprintf(&b, "%d:", categoryID)
	for _, id := range ids {
		fmt.Fprintf(&b, "%d=%s;", id, vals[id])
	}
	sum := md5.Sum([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// esc：SQL 字符串转义（单引号翻倍）
func esc(s string) string { return strings.ReplaceAll(s, "'", "''") }

// loadMetas：读某类目的 key/sale 属性（含枚举）
func loadMetas(ctx context.Context, db *sql.DB, categoryID int64) ([]attrMeta, error) {
	rows, err := db.QueryContext(ctx,
		"SELECT id, value_type, is_key, is_sale, enum_values FROM attribute_meta WHERE category_id=? AND (is_key=1 OR is_sale=1)",
		categoryID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []attrMeta
	for rows.Next() {
		var m attrMeta
		var isKey, isSale int
		var enumJSON []byte
		if err := rows.Scan(&m.ID, &m.ValueType, &isKey, &isSale, &enumJSON); err != nil {
			return nil, err
		}
		m.IsKey, m.IsSale = isKey == 1, isSale == 1
		if len(enumJSON) > 0 {
			_ = json.Unmarshal(enumJSON, &m.Enums)
		}
		out = append(out, m)
	}
	return out, rows.Err()
}
