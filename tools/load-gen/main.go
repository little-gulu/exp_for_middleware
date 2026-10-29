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
	"math/rand"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

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

// genCategory：对一个类目生成 count 个 SPU + 其全部 SKU（批量多行 INSERT）。
func genCategory(ctx context.Context, db *sql.DB, t catTemplate, count, batch int) error {
	metas, err := loadMetas(ctx, db, t.CategoryID)
	if err != nil {
		return err
	}
	var keyMetas, saleMetas []attrMeta
	for _, m := range metas {
		if m.IsKey {
			keyMetas = append(keyMetas, m)
		}
		if m.IsSale {
			saleMetas = append(saleMetas, m)
		}
	}

	// 品牌枚举的文本→vid 映射（key 属性里的枚举就是品牌）
	textToVID := map[string]int64{}
	for _, m := range metas {
		for _, e := range m.Enums {
			textToVID[e.Text] = e.VID
		}
	}

	spuBuf := make([]string, 0, batch)
	skuBuf := make([]string, 0, batch)
	spuID, skuCode, totalSPU := int64(0), int64(0), 0
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// flush：两条批量 INSERT（先 spu 后 sku——sku.spu_id 引用 spu.id）
	flush := func() error {
		if len(spuBuf) == 0 {
			return nil
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO spu (merchant_id,category_id,title,key_props,base_props,key_hash,publish_time) VALUES "+strings.Join(spuBuf, ",")); err != nil {
			return err
		}
		if _, err := db.ExecContext(ctx,
			"INSERT INTO sku (spu_id,sku_code,sale_props,sale_hash,price,stock) VALUES "+strings.Join(skuBuf, ",")); err != nil {
			return err
		}
		spuBuf, skuBuf = spuBuf[:0], skuBuf[:0]
		return nil
	}

	for i := 0; i < count; i++ {
		brand := t.Brands[rng.Intn(len(t.Brands))]
		model := t.Models[rng.Intn(len(t.Models))]

		// 关键属性：enum（品牌）用 vid，string（型号/款号）用文本
		keyVals := map[int64]string{}
		for _, m := range keyMetas {
			if m.ValueType == "enum" {
				keyVals[m.ID] = fmt.Sprintf("%d", textToVID[brand])
			} else {
				keyVals[m.ID] = model
			}
		}
		keyHash := keyHashOf(t.CategoryID, keyVals)
		keyProps, _ := json.Marshal(keyVals)
		title := brand + " " + model + " " + t.Suffix

		spuID++
		spuBuf = append(spuBuf, fmt.Sprintf("(1,%d,'%s','%s','{}','%s',NOW())",
			t.CategoryID, esc(title), esc(string(keyProps)), keyHash))

		// 销售属性枚举笛卡尔积
		combos := [][]enumVal{{}}
		for _, m := range saleMetas {
			var next [][]enumVal
			for _, c := range combos {
				for _, e := range m.Enums {
					next = append(next, append(append([]enumVal(nil), c...), e))
				}
			}
			combos = next
		}
		for _, combo := range combos {
			saleVals := map[int64]string{}
			for i, m := range saleMetas {
				saleVals[m.ID] = fmt.Sprintf("%d", combo[i].VID)
			}
			saleHash := keyHashOf(0, saleVals)
			saleProps, _ := json.Marshal(saleVals)
			skuCode++
			skuBuf = append(skuBuf, fmt.Sprintf("(%d,'SKU-G-%d','%s','%s',%.2f,%d)",
				spuID, skuCode, esc(string(saleProps)), saleHash,
				t.PriceBase+float64(rng.Intn(3000)), rng.Intn(500)))
		}

		totalSPU++
		if len(spuBuf) >= batch {
			if err := flush(); err != nil {
				return err
			}
			log.Printf("类目 %d：已生成 %d SPU", t.CategoryID, totalSPU)
		}
	}
	if err := flush(); err != nil {
		return err
	}
	log.Printf("类目 %d 完成：SPU=%d", t.CategoryID, totalSPU)
	return nil
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
