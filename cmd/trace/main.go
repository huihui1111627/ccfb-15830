package main

import (
	"flag"
	"fmt"
	"time"

	"temple/internal/sim"
)

// 调试工具：跑单个场景，每 200ms 打印两层限流器与下游的真实轨迹。
// 用法：go run ./cmd/trace -scene 19 -mode adaptive-vegas -dur 10
func main() {
	sceneID := flag.Int("scene", 19, "场景 ID（1-30）")
	modeStr := flag.String("mode", "vegas", "vegas|gradient|fixed")
	durSec := flag.Int("dur", 10, "模拟秒数")
	flag.Parse()

	var mode sim.Mode
	switch *modeStr {
	case "gradient":
		mode = sim.ModeAdaptiveGradient
	case "fixed":
		mode = sim.ModeFixed
	default:
		mode = sim.ModeAdaptiveVegas
	}

	var sc *sim.Scenario
	for _, s := range sim.BuildScenarios() {
		if s.ID == *sceneID {
			sc = s
		}
	}
	if sc == nil {
		panic("场景不存在")
	}
	fmt.Printf("场景 %d %s 模式 %s 时长 %ds\n", sc.ID, sc.Name, mode, *durSec)
	fmt.Printf("初始容量=%d 服务时间=%s 尖峰=%.0fx [%s~%s]\n\n",
		sc.InitialCap, sc.BaseService, sc.SpikeMul, sc.SpikeStart, sc.SpikeEnd)

	r := sim.NewRunner(sim.RunConfig{
		Scenario:      sc,
		Mode:          mode,
		Duration:      time.Duration(*durSec) * time.Second,
		FixedLimit:    sc.InitialCap,
		InitialLimit:  sc.InitialCap,
		SnapshotEvery: 200 * time.Millisecond,
		Workers:       4096,
	})
	_, clientL := r.Limiters()

	stop := make(chan struct{})
	go func() {
		t := time.NewTicker(200 * time.Millisecond)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				cs := clientL.Snapshot()
				fmt.Printf("%5.1f 容%3d 上限%3d 在途%3d 等%4d 基线%6.1f p50%6.1f 比%4.2f 强%3d 预留%3d 率%7.1f\n",
					snapT(r), r.Downstream().CapacityAt(snapDur(r)),
					cs.Limit, cs.InFlight, cs.Waiters,
					float64(cs.Baseline.Microseconds())/1000,
					float64(cs.CurrentRTT.Microseconds())/1000,
					cs.QueueRatio, cs.StrongEvents, cs.HighReserve, cs.HighRate)
			}
		}
	}()

	m := r.Run()
	close(stop)
	cs := clientL.Snapshot()

	fmt.Println()
	fmt.Printf("到达=%d 入口拒=%d 客户端拒=%d 成功=%d 超时=%d 过载=%d 成功率=%.2f%%\n",
		m.TotalArrived(), m.RejEntry[0]+m.RejEntry[1]+m.RejEntry[2],
		m.RejClient[0]+m.RejClient[1]+m.RejClient[2],
		m.TotalSuccess(), m.Timeout[0]+m.Timeout[1]+m.Timeout[2],
		m.Overload[0]+m.Overload[1]+m.Overload[2], m.SuccessRate()*100)
	for p, name := range []string{"低", "中", "高"} {
		fmt.Printf("  %s档: 到达=%d 成功=%d 入口拒=%d 客户拒=%d(队列满=%d 等超时=%d) 成功率=%.1f%%\n",
			name, m.Arrived[p], m.Success[p], m.RejEntry[p], m.RejClient[p],
			cs.RejectQueueFull[p], cs.RejectWaitTimeout[p], m.SuccessRatePrio(p)*100)
	}
	fmt.Printf("窗口高档: 到达=%d 成功=%d 成功率=%.1f%%\n",
		m.WinArrived[2], m.WinSuccess[2],
		float64(m.WinSuccess[2])/float64(max64(1, m.WinArrived[2]))*100)
	fmt.Printf("p50=%.1fms p90=%.1fms p99=%.1fms\n", m.Percentile(50), m.Percentile(90), m.Percentile(99))
}

func snapT(r *sim.Runner) float64         { return r.Elapsed().Seconds() }
func snapDur(r *sim.Runner) time.Duration { return r.Elapsed() }
func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
