package config

import "os"

// Config：进程级配置，全部来自环境变量（12-factor；容器化部署时注入）
type Config struct {
	HTTPAddr  string // HTTP 监听地址（含端口）
	MySQLDSN  string // MySQL 连接串（宿主机开发用 13306；容器化后换 mysql:3306）
	RedisAddr string // Redis 地址（宿主机开发用 16379）
}

// Load：读环境变量并套默认值。默认值=本机开发环境（对应 deploy/.env）
func Load() Config {
	return Config{
		HTTPAddr:  getenv("HTTP_ADDR", ":8080"),
		MySQLDSN:  getenv("MYSQL_DSN", "root:root123456@tcp(localhost:13306)/demo?charset=utf8mb4&parseTime=true"),
		RedisAddr: getenv("REDIS_ADDR", "localhost:16379"),
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}
