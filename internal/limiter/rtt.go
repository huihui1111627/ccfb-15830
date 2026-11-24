package limiter

import (
	"sort"
	"time"
)

// rttSample 一条延迟样本：观测时刻、往返延迟、采样瞬间的在途数。
type rttSample struct {
	at      time.Time
	rtt     time.Duration
	inflown int
}

// rttRing 定长环形缓冲，写入零分配、不做整体搬移。
// 满了之后覆盖最老样本；读取时再按时间窗口裁剪。
type rttRing struct {
	samples []rttSample
	head    int // 下一个写入位置
	size    int // 已存样本数
}

func newRTTRing(capacity int) *rttRing {
	if capacity < 4 {
		capacity = 4
	}
	return &rttRing{samples: make([]rttSample, capacity)}
}

func (r *rttRing) add(at time.Time, rtt time.Duration, inflight int) {
	r.samples[r.head] = rttSample{at: at, rtt: rtt, inflown: inflight}
	r.head = (r.head + 1) % len(r.samples)
	if r.size < len(r.samples) {
		r.size++
	}
}

// recent 返回观测时刻晚于 cutoff 的样本（顺序不保证）。
func (r *rttRing) recent(cutoff time.Time) []rttSample {
	out := make([]rttSample, 0, r.size)
	for i := 0; i < r.size; i++ {
		idx := (r.head - 1 - i + len(r.samples)*2) % len(r.samples)
		s := r.samples[idx]
		if s.at.Before(cutoff) {
			break // 环形里新→老遍历，遇到窗口外即可停止
		}
		out = append(out, s)
	}
	return out
}

// stats 在给定时间窗口上汇总：最小延迟、p50、p99、平均在途数、样本数。
func (r *rttRing) stats(now time.Time, window time.Duration) (minRTT, p50, p99 time.Duration, avgInFlight float64, n int) {
	samples := r.recent(now.Add(-window))
	if len(samples) == 0 {
		return 0, 0, 0, 0, 0
	}
	rtts := make([]int, len(samples))
	minRTT = samples[0].rtt
	inflightSum := 0
	for i, s := range samples {
		rtts[i] = int(s.rtt)
		if s.rtt < minRTT {
			minRTT = s.rtt
		}
		inflightSum += s.inflown
	}
	sort.Ints(rtts)
	p50 = time.Duration(percentileSorted(rtts, 50))
	p99 = time.Duration(percentileSorted(rtts, 99))
	avgInFlight = float64(inflightSum) / float64(len(samples))
	return minRTT, p50, p99, avgInFlight, len(samples)
}

// percentileSorted 对已排序整数切片取 p 分位（p 取 0~100，最近秩法）。
func percentileSorted(sorted []int, p float64) int {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(p) / 100 * float64(len(sorted)-1))
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}
