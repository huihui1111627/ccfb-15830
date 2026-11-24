package limiter

import "time"

// vegasTarget 算法一：最小延迟基线 + 排队比（需求 1）。
//
// 记基线延迟 B（无排队时的最小 RTT），当前 p50 延迟 R，排队比 q = R/B。
// 控制环借鉴 TCP Vegas/AIMD 的成熟结构，区分三种状态：
//
//  1. 有排队（q 明显 >1）：说明在途已经越过容量拐点，
//     由 Little 定律，无排队并发 ≈ 当前在途 / q，直接收缩到该值。
//  2. 无排队 + 满载（q≈1 且平均在途贴着上限）：系统健康且请求在被拒绝，
//     按加性增（AI）缓慢抬上限，试探更多容量。
//  3. 无排队 + 不满载：流量本身不足，保持上限不动，避免空载漂移。
//
// 关键：容量估计用“在途/排队比”，而在途被上限夹住时该估计与上限同量级，
// 因此加性增的幅度由满载条件独立驱动，保证高负载拒绝期上限能持续上涨，
// 不会出现只缩不涨的棘轮。
func (l *Limiter) vegasTarget(now time.Time) float64 {
	_, p50, _, avgInFlight, n := l.ring.stats(now, l.cfg.RTTWindow)
	if n < l.cfg.MinSamples {
		// 样本不足，只温和扩张，不盲目收缩
		return float64(l.baseLimit) * 1.15
	}
	q := float64(p50) / float64(l.baseline)

	const (
		qNoQueue = 1.12 // q <= 1.12 视为基本无排队
		qLight   = 1.45 // 1.12 < q <= 1.45 轻度排队
		utilFull = 0.80 // 平均在途 / 上限 >= 0.80 视为满载
	)

	switch {
	case q > qLight:
		// 明显排队：Little 定律给出无排队并发，仅打 97 折轻微保守。
		// 注意 avgInFlight 在满载时贴近上限，因此该目标与上限同量级，
		// 配合 q>1 才收缩，能把稳态上限稳定在真实容量附近而非系统性偏低。
		return (avgInFlight / q) * 0.97
	case q > qNoQueue:
		// 轻度排队：小幅收缩 4%，避免过度反应
		return float64(l.baseLimit) * 0.96
	default:
		// 无排队
		if avgInFlight >= float64(l.baseLimit)*utilFull {
			// 满载且健康：加性增，每轮 +12.5%（至少 +1），寻找更高容量
			return float64(l.baseLimit)*1.125 + 1
		}
		// 不满载：流量不足，维持当前上限
		return float64(l.baseLimit)
	}
}

// gradientTarget 算法二：延迟对在途数的梯度（需求 3）。
//
// 把当前 RTT 窗口对半切成“老半段”和“新半段”，比较两段的平均 RTT 与平均在途数：
//   - 在途数上升的同时 RTT 也上升（正梯度）→ 已越过拐点，按斜率强度收缩
//   - RTT 平稳但在途贴着上限（满载）→ 还没触达容量，慢慢放大
//   - 在途不满 → 流量不足，保持不动
//
// 与 Vegas 的区别：不依赖长期最小基线，对下游缓慢漂移天然免疫，
// 但对突发的反应略慢。两种算法可在运行中热切换。
func (l *Limiter) gradientTarget(now time.Time) float64 {
	samples := l.ring.recent(now.Add(-l.cfg.RTTWindow))
	if len(samples) < l.cfg.MinSamples*2 {
		return float64(l.baseLimit) * 1.15
	}
	mid := len(samples) / 2
	// recent() 返回新→老，因此 [0,mid) 是新半段，[mid:] 是老半段
	newHalf := samples[:mid]
	oldHalf := samples[mid:]

	newRTT, newIF := avgSample(newHalf)
	oldRTT, oldIF := avgSample(oldHalf)
	if oldRTT <= 0 {
		return float64(l.baseLimit) * 1.15
	}

	dRTT := (newRTT - oldRTT) / oldRTT // 延迟相对变化
	dIF := newIF - oldIF               // 在途数变化（绝对量）
	flat := dRTT < 0.05                // RTT 涨幅小于 5% 视为平稳

	switch {
	case dIF > 0 && !flat:
		// 加并发后延迟明显抬头：按延迟涨幅等比例收缩，至少收 5%，最多 50%
		cut := dRTT
		if cut < 0.05 {
			cut = 0.05
		}
		if cut > 0.5 {
			cut = 0.5
		}
		return float64(l.baseLimit) * (1 - cut)
	case dIF < 0 && !flat:
		// 在途数下降但延迟还在涨：下游本身在变慢（漂移/卡顿），小幅收缩
		return float64(l.baseLimit) * 0.92
	default:
		// 延迟平稳：满载才缓慢放大，空载不动
		_, _, _, avgInFlight, _ := l.ring.stats(now, l.cfg.RTTWindow)
		if avgInFlight >= float64(l.baseLimit)*0.85 {
			return float64(l.baseLimit)*1.08 + 1
		}
		return float64(l.baseLimit)
	}
}

func avgSample(s []rttSample) (avgRTT float64, avgIF float64) {
	if len(s) == 0 {
		return 0, 0
	}
	var sumRTT, sumIF float64
	for _, x := range s {
		sumRTT += float64(x.rtt)
		sumIF += float64(x.inflown)
	}
	return sumRTT / float64(len(s)), sumIF / float64(len(s))
}
