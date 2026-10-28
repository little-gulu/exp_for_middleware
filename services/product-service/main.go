package main

import (
	"context"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"

	"product-service/internal/api"
	"product-service/internal/bloom"
	"product-service/internal/config"
	"product-service/internal/publish"
	"product-service/internal/store"
)

func main() {
	cfg := config.Load()

	// 装配依赖（读一遍，看 5 块积木怎么拼成一个进程）：
	st, err := store.NewStore(cfg.MySQLDSN)
	if err != nil {
		log.Fatalf("mysql: %v", err)
	}
	defer st.Close()

	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	defer rdb.Close()

	idem := publish.NewIdempotency(cfg.RedisAddr)
	filter := bloom.New(rdb, "bf:spu", 9600000, 7) // n=10^6, p=1% 反推的 m/k

	mux := api.NewRouter(st, rdb, idem, filter)

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second, // 防慢连接占资源
	}

	// 优雅退出：捕获 SIGINT/SIGTERM → Shutdown（等正在处理的请求完成，最长 5s）。
	// 防的问题：bug 卡 G3 未优雅退出丢消息/丢请求；直接 os.Exit 即可复现该 bug。
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		<-ctx.Done()
		log.Println("收到退出信号，开始优雅退出…")
		shutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("优雅退出超时: %v", err)
		}
	}()

	log.Printf("product-service listening on %s", cfg.HTTPAddr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
	log.Println("已退出")
}
