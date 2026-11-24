package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"time"

	"temple/internal/demo"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP 监听地址")
	festival := flag.Bool("festival", false, "启动即进入节庆（高负载）模式")
	flag.Parse()

	svc := demo.NewService(demo.DefaultEndpoints(), 20240101)
	if *festival {
		svc.SetFestival(true)
	}
	defer svc.Stop()

	srv := &http.Server{
		Addr:              *addr,
		Handler:           svc.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	fmt.Printf("寺庙香客服务自适应限流演示已启动\n")
	fmt.Printf("监控页面： http://localhost%s/\n", *addr)
	fmt.Printf("状态接口： http://localhost%s/api/state\n", *addr)
	fmt.Printf("切换算法： /api/algo?algo=vegas|gradient\n")
	fmt.Printf("节庆开关： /api/festival?on=1|0\n")
	log.Fatal(srv.ListenAndServe())
}
