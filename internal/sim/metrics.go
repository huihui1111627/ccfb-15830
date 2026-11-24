package sim

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics 一次完整运行的全部统计指标，按优先级分档计数。
type Metrics struct {
	Arrived   [3]int64 // 到达（尝试）总数
	RejEntry  [3]int64 // 入口层拒绝
	RejClient [3]int64 // 客户端层拒绝
	Success   [3]int64
	Timeout   [3]int64
	Overload  [3]int64
	OtherErr  [3]int64

	rttMu     sync.Mutex
	rttAll    []int64 // 所有真正打到下游的请求 RTT（微秒），含超时请求（需求 8）
	rttByPrio [3][]int64

	// 过载/尖峰窗口内的分档计数（窗口由 Scenario 的 SpikeStart/SpikeEnd 定义）
	WinArrived [3]int64
	WinSuccess [3]int64
	WinReject  [3]int64
	WinTimeout [3]int64

	Snapshots []LimitSnapshot
}

// LimitSnapshot 某一时刻限流器状态与下游真实容量的对照样本。
type LimitSnapshot struct {
	T              time.Duration // 相对运行起点
	EntryLimit     int
	EntryInFlight  int
	ClientLimit    int
	ClientInFlight int
	TrueCapacity   int // 下游此刻真实容量（排队论最优并发的基准）
	QueueLen       int // 下游排队长度
}

func newMetrics(snapCap int) *Metrics {
	return &Metrics{Snapshots: make([]LimitSnapshot, 0, snapCap)}
}

func (m *Metrics) addArrival(p int)   { atomic.AddInt64(&m.Arrived[p], 1) }
func (m *Metrics) addRejEntry(p int)  { atomic.AddInt64(&m.RejEntry[p], 1) }
func (m *Metrics) addRejClient(p int) { atomic.AddInt64(&m.RejClient[p], 1) }
func (m *Metrics) addSuccess(p int)   { atomic.AddInt64(&m.Success[p], 1) }
func (m *Metrics) addTimeout(p int)   { atomic.AddInt64(&m.Timeout[p], 1) }
func (m *Metrics) addOverload(p int)  { atomic.AddInt64(&m.Overload[p], 1) }
func (m *Metrics) addOtherErr(p int)  { atomic.AddInt64(&m.OtherErr[p], 1) }

// 过载窗口分段计数（仅在尖峰时间窗内由 runner 调用）。
func (m *Metrics) winArrival(p int) { atomic.AddInt64(&m.WinArrived[p], 1) }
func (m *Metrics) winSuccess(p int) { atomic.AddInt64(&m.WinSuccess[p], 1) }
func (m *Metrics) winReject(p int)  { atomic.AddInt64(&m.WinReject[p], 1) }
func (m *Metrics) winTimeout(p int) { atomic.AddInt64(&m.WinTimeout[p], 1) }

// recordRTT 记录一条完成请求的 RTT；超时请求同样记录（需求 8）。
func (m *Metrics) recordRTT(p int, rtt time.Duration) {
	v := rtt.Microseconds()
	m.rttMu.Lock()
	m.rttAll = append(m.rttAll, v)
	m.rttByPrio[p] = append(m.rttByPrio[p], v)
	m.rttMu.Unlock()
}

// Percentile 全量 RTT 的 p 分位（毫秒）。
func (m *Metrics) Percentile(p float64) float64 {
	m.rttMu.Lock()
	s := append([]int64(nil), m.rttAll...)
	m.rttMu.Unlock()
	if len(s) == 0 {
		return 0
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(p / 100 * float64(len(s)-1))
	return float64(s[idx]) / 1000.0
}

// PercentilePrio 指定分档的 p 分位（毫秒）。
func (m *Metrics) PercentilePrio(prio int, p float64) float64 {
	m.rttMu.Lock()
	s := append([]int64(nil), m.rttByPrio[prio]...)
	m.rttMu.Unlock()
	if len(s) == 0 {
		return 0
	}
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })
	idx := int(p / 100 * float64(len(s)-1))
	return float64(s[idx]) / 1000.0
}

// TotalArrived 三档到达总数。
func (m *Metrics) TotalArrived() int64 { return sum3(m.Arrived) }
func (m *Metrics) TotalRejected() int64 {
	return sum3(m.RejEntry) + sum3(m.RejClient)
}
func (m *Metrics) TotalSuccess() int64 { return sum3(m.Success) }

// SuccessRate 成功率 = 成功 / 到达（被拒也算失败，符合香客视角）。
func (m *Metrics) SuccessRate() float64 {
	if m.TotalArrived() == 0 {
		return 0
	}
	return float64(m.TotalSuccess()) / float64(m.TotalArrived())
}

// SuccessRatePrio 指定分档成功率。
func (m *Metrics) SuccessRatePrio(p int) float64 {
	if m.Arrived[p] == 0 {
		return 0
	}
	return float64(m.Success[p]) / float64(m.Arrived[p])
}

// RejectRate 总拒绝率。
func (m *Metrics) RejectRate() float64 {
	if m.TotalArrived() == 0 {
		return 0
	}
	return float64(m.TotalRejected()) / float64(m.TotalArrived())
}

func sum3(a [3]int64) int64 { return a[0] + a[1] + a[2] }
