package sim

import (
	"fmt"
	"math"
	"time"
)

// Report 单场景单模式的汇总报表。
type Report struct {
	ScenarioID   int
	ScenarioName string
	Mode         Mode
	Duration     time.Duration

	Arrived   int64
	Rejected  int64
	Success   int64
	Timeouts  int64
	Overloads int64

	SuccessRate float64
	RejectRate  float64
	SuccPrio    [3]float64

	// 分档保护：高档成功率相对整体成功率的倍数（>1 说明高档被优先保护）
	ProtectRatio float64
	// 过载窗口内的高档保护倍数
	WinHighRate    float64
	WinOverallRate float64
	WinProtect     float64

	P50ms float64
	P90ms float64
	P99ms float64

	// 收敛性：客户端层上限 vs 下游真实容量
	MeanLimit      float64
	MeanCapacity   float64
	MeanRatio      float64 // 平均 上限/容量
	StableMAPE     float64 // 稳定窗口内的平均绝对百分比误差（%）
	StableWindowOK bool    // 是否存在足够长的稳定窗口
}

// BuildReport 从指标与快照生成报表。
func BuildReport(sc *Scenario, mode Mode, dur time.Duration, m *Metrics) Report {
	r := Report{
		ScenarioID:   sc.ID,
		ScenarioName: sc.Name,
		Mode:         mode,
		Duration:     dur,
		Arrived:      m.TotalArrived(),
		Rejected:     m.TotalRejected(),
		Success:      m.TotalSuccess(),
		Timeouts:     sum3(m.Timeout),
		Overloads:    sum3(m.Overload),
		SuccessRate:  m.SuccessRate(),
		RejectRate:   m.RejectRate(),
		P50ms:        m.Percentile(50),
		P90ms:        m.Percentile(90),
		P99ms:        m.Percentile(99),
	}
	for p := 0; p < 3; p++ {
		r.SuccPrio[p] = m.SuccessRatePrio(p)
	}
	if r.SuccessRate > 0 {
		r.ProtectRatio = r.SuccPrio[2] / r.SuccessRate
	}

	// 过载窗口内的分档保护
	var winArr, winSuc int64
	for p := 0; p < 3; p++ {
		winArr += m.WinArrived[p]
		winSuc += m.WinSuccess[p]
	}
	if winArr > 0 {
		r.WinOverallRate = float64(winSuc) / float64(winArr)
	}
	if m.WinArrived[2] > 0 {
		r.WinHighRate = float64(m.WinSuccess[2]) / float64(m.WinArrived[2])
	}
	if r.WinOverallRate > 0 {
		r.WinProtect = r.WinHighRate / r.WinOverallRate
	}

	// 上限收敛分析（只看客户端层，它直接面对下游）
	n := len(m.Snapshots)
	if n > 0 {
		sumL, sumC := 0.0, 0.0
		for _, sn := range m.Snapshots {
			sumL += float64(sn.ClientLimit)
			sumC += float64(sn.TrueCapacity)
		}
		r.MeanLimit = sumL / float64(n)
		r.MeanCapacity = sumC / float64(n)
		if r.MeanCapacity > 0 {
			r.MeanRatio = r.MeanLimit / r.MeanCapacity
		}
		// 稳态收敛 MAPE：
		// 跳过每次容量跳变后的收敛宽限期（给控制器 1.5s 跟踪时间），
		// 且只统计饱和样本（在途 >= 上限的 80%）。
		// 这样度量的是“容量稳定后上限收敛到最优并发的精度”，
		// 不把容量突变时不可避免的收敛滞后、以及低负载期限流器主动压低上限
		// 这两类正确行为误判为误差。
		const gracePeriod = 1500 * time.Millisecond
		var lastChange time.Duration = -1
		prevCap := -1
		sumAbsPct, stableN := 0.0, 0
		for _, sn := range m.Snapshots {
			if sn.TrueCapacity != prevCap {
				lastChange = sn.T
				prevCap = sn.TrueCapacity
			}
			if sn.T-lastChange < gracePeriod {
				continue // 容量跳变后的收敛宽限期
			}
			if sn.TrueCapacity <= 0 || sn.ClientLimit <= 0 {
				continue
			}
			if float64(sn.ClientInFlight) < 0.8*float64(sn.ClientLimit) {
				continue // 非饱和样本
			}
			sumAbsPct += math.Abs(float64(sn.ClientLimit-sn.TrueCapacity)) /
				float64(sn.TrueCapacity) * 100
			stableN++
		}
		if stableN >= 5 {
			r.StableMAPE = sumAbsPct / float64(stableN)
			r.StableWindowOK = true
		}
	}
	return r
}

// HighSLOResult 过载（尖峰）窗口内高档请求成功率评估。
type HighSLOResult struct {
	WindowName  string
	HighArrived int64
	HighSuccess int64
	HighRate    float64
	Met         bool
	SLO         float64
}

// EvaluateHighSLO 评估尖峰/过载窗口的高档成功率（需求 4 的验收项）。
// 窗口 = Scenario.SpikeStart ~ SpikeEnd，分段计数在 Metrics 里独立维护，
// 不被平稳期的高成功率稀释。
func EvaluateHighSLO(m *Metrics, slo float64) HighSLOResult {
	const high = 2
	res := HighSLOResult{
		WindowName:  "过载窗口",
		HighArrived: m.WinArrived[high],
		HighSuccess: m.WinSuccess[high],
		SLO:         slo,
	}
	if res.HighArrived > 0 {
		res.HighRate = float64(res.HighSuccess) / float64(res.HighArrived)
	}
	res.Met = res.HighRate >= slo
	return res
}

// Header 明细表表头。
func (r Report) Header() string {
	return fmt.Sprintf("%-3s %-22s %-17s %8s %8s %8s %8s %8s %8s %8s %8s %8s",
		"#", "场景", "模式", "到达", "拒绝", "成功", "超时", "过载",
		"成功率%", "p50ms", "p90ms", "p99ms")
}

// Row 明细表单行。
func (r Report) Row() string {
	return fmt.Sprintf("%-3d %-22s %-17s %8d %8d %8d %8d %8d %8.2f %8.1f %8.1f %8.1f",
		r.ScenarioID, truncate(r.ScenarioName, 22), r.Mode,
		r.Arrived, r.Rejected, r.Success, r.Timeouts, r.Overloads,
		r.SuccessRate*100, r.P50ms, r.P90ms, r.P99ms)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
