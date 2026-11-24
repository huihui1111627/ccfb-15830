package limiter

import (
	"math"
	"sync"
	"time"
)

// strongCooldown 强信号触发后的冷却期：冷却期内只做线性 +1 恢复。
const strongCooldown = 2 * time.Second

// Limiter 单接口自适应并发限流器。
// 每个接口一个实例，实例之间无任何通信（需求 7）。
type Limiter struct {
	mu sync.Mutex

	cfg Config

	// 并发状态
	inFlight     int
	inFlightPrio [3]int

	// 上限状态
	baseLimit     int // 平滑后的基础上限
	probeLimit    int // 探底期间的临时上限
	probeUntil    time.Time
	probeStart    time.Time
	lastProbe     time.Time
	cooldownUntil time.Time

	// 统计
	ring              *rttRing
	baseline          time.Duration
	completed         int64
	strongEvents      int64
	rejected          [3]int64
	rejectQueueFull   [3]int64
	rejectWaitTimeout [3]int64
	total             [3]int64
	algo              Algorithm

	// 到达率 EWMA（请求/秒），驱动动态预留（需求 4）
	highRateEWMA   float64
	allRateEWMA    float64
	lastArrival    time.Time
	lastAnyArrival time.Time

	// 有界优先级等待队列（需求 4 的等待承接）
	waitQ    priorityQueue
	waitRing *rttRing // 本地等待排队时长样本（与下游 RTT 分开统计）
}

// New 按配置构造限流器。
func New(cfg Config) *Limiter {
	if cfg.MinLimit < 1 {
		cfg.MinLimit = 1
	}
	if cfg.MaxLimit < cfg.MinLimit {
		cfg.MaxLimit = cfg.MinLimit
	}
	if cfg.InitialLimit < cfg.MinLimit {
		cfg.InitialLimit = cfg.MinLimit
	}
	if cfg.UpdateEveryN < 1 {
		cfg.UpdateEveryN = 1
	}
	if cfg.SmoothAlpha <= 0 || cfg.SmoothAlpha > 1 {
		cfg.SmoothAlpha = 0.5
	}
	if cfg.MaxChangeRate <= 0 {
		cfg.MaxChangeRate = 0.25
	}
	ringCap := 4096
	if cfg.BaselineTTL > 0 && cfg.RTTWindow > 0 {
		// 基线窗口至少要能放下基线 TTL 内的样本，粗估按窗口比例放大
		if est := int(float64(ringCap) * float64(cfg.BaselineTTL) / float64(cfg.RTTWindow)); est > ringCap {
			ringCap = est
		}
	}
	if ringCap > 1<<16 {
		ringCap = 1 << 16
	}
	return &Limiter{
		cfg:       cfg,
		baseLimit: cfg.InitialLimit,
		ring:      newRTTRing(ringCap),
		waitRing:  newRTTRing(1024),
		algo:      cfg.Algo,
	}
}

// Lease 一次被放行的请求凭证，必须调用 Release 归还在途名额。
type Lease struct {
	l         *Limiter
	start     time.Time // 获得名额、真正开始下游调用的时刻
	enqueueAt time.Time // 进入限流器的时刻（用于区分本地排队等待与下游延迟）
	prio      Priority
	done      bool
}

// Acquire 尝试获取一个在途名额。
// 立即有名额则返回 lease；否则进入有界优先级队列等待，等待期间高档排在队首。
// 等待超过 cfg.WaitTimeout 或队列已满才拒绝。拒绝只是“停止放行新请求”，
// 对已在途请求没有任何影响（需求 5）。
func (l *Limiter) Acquire(prio Priority) (*Lease, error) {
	now := time.Now()
	l.mu.Lock()

	l.total[prio]++
	l.observeArrival(now, prio == PrioHigh)
	limit := l.effectiveLimit(now)

	if l.admit(prio, limit, now) {
		l.inFlight++
		l.inFlightPrio[prio]++
		l.mu.Unlock()
		return &Lease{l: l, start: now, enqueueAt: now, prio: prio}, nil
	}

	// 立即名额拿不到：配置了等待则进入优先级队列。
	// 队列容量也为高档保留 1/4：队列将满时只拒中低档，高档仍可入队，
	// 防止海量低档请求把等待队列占满、高档连排队机会都没有。
	curInFlight := l.inFlight // 持锁时快照，避免解锁后再读共享字段触发竞态
	if l.cfg.WaitTimeout <= 0 {
		l.rejected[prio]++
		l.rejectQueueFull[prio]++
		l.mu.Unlock()
		return nil, ErrRejected{Name: l.cfg.Name, Prio: prio, Limit: limit, InFlight: curInFlight}
	}
	qLen := l.waitQ.aliveCount()
	highWater := l.cfg.MaxWaiters - l.cfg.MaxWaiters/4
	if qLen >= l.cfg.MaxWaiters || (prio != PrioHigh && qLen >= highWater) {
		l.rejected[prio]++
		l.rejectQueueFull[prio]++
		l.mu.Unlock()
		return nil, ErrRejected{Name: l.cfg.Name, Prio: prio, Limit: limit, InFlight: curInFlight}
	}

	e := &waitEntry{prio: prio, ch: make(chan struct{})}
	l.waitQ.push(e)
	// 高档请求更重要，给予 2.5 倍等待时间：下游卡顿（数百 ms）期间
	// 高优先级等待者不会因普通等待超时而被误杀，低/中档仍用标准超时。
	waitTimeout := l.cfg.WaitTimeout
	if prio == PrioHigh {
		waitTimeout = waitTimeout * 5 / 2
	}
	l.mu.Unlock()

	timer := waitAfterFunc(waitTimeout, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if e.settled() {
			return // 已被正常授予，竞态下忽略
		}
		e.canceled = true
		l.waitQ.cancel(e)
		close(e.ch)
	})

	<-e.ch
	timer.Stop()

	l.mu.Lock()
	defer l.mu.Unlock()
	// granted 与 canceled 互斥：granted=true 是正常授予，canceled=true 是超时
	if e.canceled {
		l.rejected[prio]++
		l.rejectWaitTimeout[prio]++
		return nil, ErrRejected{Name: l.cfg.Name, Prio: prio, Limit: l.effectiveLimit(time.Now()), InFlight: l.inFlight}
	}
	// 正常唤醒：名额已在唤醒方保留，这里登记占用。
	// start 取“真正获得名额”的时刻，确保 RTT 样本只含下游处理延迟，
	// 不含在本地等待队列里排队的时间（后者不是下游排队信号）。
	grantAt := time.Now()
	l.inFlight++
	l.inFlightPrio[prio]++
	return &Lease{l: l, start: grantAt, enqueueAt: now, prio: prio}, nil
}

// signalWaiterLocked 在有名额释放时唤醒队首优先级最高的等待者（调用方持 mu）。
// 返回 true 表示已把一个名额转交给等待者（调用方不应再把名额退回空闲池）。
func (l *Limiter) signalWaiterLocked() bool {
	e := l.waitQ.popReady()
	if e == nil {
		return false
	}
	e.granted = true // 标记为正常授予，与超时互斥
	close(e.ch)
	return true
}

// observeArrival 同时维护总到达率与高档到达率的 EWMA。
// 时间常数 0.15s，尖峰到来时十几毫秒内即可抬升，抢在低档填满槽位前更新预留。
func (l *Limiter) observeArrival(now time.Time, isHigh bool) {
	// 总到达率
	if !l.lastAnyArrival.IsZero() {
		if dt := now.Sub(l.lastAnyArrival).Seconds(); dt > 0 {
			inst := 1.0 / dt
			if l.allRateEWMA == 0 {
				l.allRateEWMA = inst
			} else {
				alpha := 1 - math.Exp(-dt/0.15)
				l.allRateEWMA = l.allRateEWMA*(1-alpha) + inst*alpha
			}
		}
	}
	l.lastAnyArrival = now

	// 高档到达率
	if !isHigh {
		return
	}
	if l.lastArrival.IsZero() {
		l.lastArrival = now
		return
	}
	if dt := now.Sub(l.lastArrival).Seconds(); dt > 0 {
		inst := 1.0 / dt
		if l.highRateEWMA == 0 {
			l.highRateEWMA = inst
		} else {
			alpha := 1 - math.Exp(-dt/0.15)
			l.highRateEWMA = l.highRateEWMA*(1-alpha) + inst*alpha
		}
	}
	l.lastArrival = now
}

// highReservation 当前需要为高档刚性预留的并发名额。
// 核心：按高档的实时流量占比等比例保留槽位，再乘 1.5 突发安全系数，
// 使任意时刻都有空槽等待高档到达，而不是被先到的低档占满。
// 空闲期高档占比低，预留回落到 HighHeadroom 地板，不浪费平时容量。
func (l *Limiter) highReservation(limit int, now time.Time) int {
	need := 0
	if l.allRateEWMA > 0 && l.highRateEWMA > 0 {
		share := l.highRateEWMA / l.allRateEWMA // 高档流量占比
		need = int(math.Ceil(share * float64(limit) * 1.5))
	}
	floor := int(math.Ceil(float64(limit) * l.cfg.HighHeadroom))
	ceil := int(float64(limit) * l.cfg.HighHeadMax)
	if need < floor {
		need = floor
	}
	if need > ceil {
		need = ceil
	}
	if need > limit-1 {
		need = limit - 1 // 至少给其他档留 1 个名额，避免死锁式独占
	}
	return need
}

// hasHighWaiter 判断等待队列里是否有高档请求正在等待（调用方持 mu）。
func (l *Limiter) hasHighWaiter() bool {
	for _, e := range l.waitQ.entries {
		if !e.settled() && e.prio == PrioHigh {
			return true
		}
	}
	return false
}

// admit 分档放行规则（需求 4）：
//   - 高档：只要总在途 < 上限就放行，可使用全部名额（含为其预留的部分）
//   - 中/低档：并发硬上限 = 上限 - 高档预留。预留名额在无高档使用时空闲保留，
//     不借给其他档，保证高档到达时立即有名额可用（这是高档 SLO 的关键）
//   - 一旦有高档在等待队列，中/低档立即停止占用新释放的槽位，全部让给排队的高档
//   - 低档：在中档约束上再叠加 LowHeadroom 比例上限，紧张时最先被拒
func (l *Limiter) admit(prio Priority, limit int, now time.Time) bool {
	if l.inFlight >= limit {
		return false
	}
	// 总量闸门模式：不分档，只看总在途是否触顶
	if l.cfg.TotalOnly {
		return true
	}
	reserved := l.highReservation(limit, now)
	nonHighCap := limit - reserved // 中/低档合计可用名额（不含高档在途）
	nonHighInFlight := l.inFlight - l.inFlightPrio[PrioHigh]
	// 有高档等待时，中/低档只能维持现有在途，不得新增占用（槽位全留给排队高档）
	highWaiting := l.hasHighWaiter()
	switch prio {
	case PrioHigh:
		return true
	case PrioNormal:
		if highWaiting {
			return false
		}
		// 中+低档在途数不得越过 nonHighCap
		return nonHighInFlight < nonHighCap
	case PrioLow:
		if highWaiting {
			return false
		}
		capLow := int(float64(limit) * l.cfg.LowHeadroom)
		if capLow > nonHighCap {
			capLow = nonHighCap
		}
		if capLow < 1 {
			capLow = 1
		}
		// 高负载（系统接近触顶）时进一步压低低档：若高档实际在途尚未达到预留数，
		// 说明预留名额还在等高档，低档只能用到 nonHighCap 的 70%，把缓冲让给高档。
		if l.inFlight >= limit-1 && l.inFlightPrio[PrioHigh] < reserved {
			capLow = capLow * 7 / 10
			if capLow < 1 {
				capLow = 1
			}
		}
		return nonHighInFlight < capLow
	default:
		return l.inFlight < limit
	}
}

// Release 归还名额并反馈结果。
// 超时（OutcomeTimeout）同样带着真实经过时间进入延迟样本（需求 8），
// 过载/超时同时作为强信号触发上限立即减半（需求 6）。
func (ls *Lease) Release(outcome Outcome) {
	l := ls.l
	now := time.Now()
	rtt := now.Sub(ls.start)

	l.mu.Lock()
	defer l.mu.Unlock()

	if ls.done {
		return
	}
	ls.done = true

	// 本地等待排队时长（入队到获得名额）单独记录，不混入下游 RTT 控制信号
	localWait := ls.start.Sub(ls.enqueueAt)
	if localWait > 0 {
		l.waitRing.add(now, localWait, l.inFlight)
	}

	// 先记下完成瞬间的在途数（尚未减 1），梯度算法要用
	inflightAtCompletion := l.inFlight
	l.ring.add(now, rtt, inflightAtCompletion)
	l.completed++

	// 归还在途名额；上限调低时在途请求自然排空，不强行取消（需求 5）
	l.inFlight--
	l.inFlightPrio[ls.prio]--

	// 若有等待者，优先把刚释放的名额转交给队首（高档优先）。
	// 转交时在途数净值不变（-1 已记账，唤醒方 Acquire 会 +1），因此这里直接 return。
	if l.signalWaiterLocked() {
		// 强信号仍要处理，但不影响名额转交
		if outcome == OutcomeOverload || outcome == OutcomeTimeout {
			l.recordStrong(now)
		}
		return
	}

	strong := outcome == OutcomeOverload || outcome == OutcomeTimeout
	if strong {
		l.recordStrong(now)
	}

	if l.completed%int64(l.cfg.UpdateEveryN) == 0 {
		l.adjust(now, strong)
	}
}

// recordStrong 记录一次过载/超时强信号：上限立即减半并进入冷却（需求 6）。
func (l *Limiter) recordStrong(now time.Time) {
	l.baseLimit = clampInt(l.baseLimit/2, l.cfg.MinLimit, l.cfg.MaxLimit)
	l.cooldownUntil = now.Add(strongCooldown)
	l.strongEvents++
	l.probeUntil = time.Time{} // 强信号优先，取消正在进行的探底
}

// effectiveLimit 当前对外生效的上限；探底期间返回临时压低值。
func (l *Limiter) effectiveLimit(now time.Time) int {
	if !l.probeUntil.IsZero() && now.Before(l.probeUntil) {
		return l.probeLimit
	}
	return l.baseLimit
}

// adjust 一次上限重算，调用方持有 mu。
func (l *Limiter) adjust(now time.Time, justStrong bool) {
	// 探底窗口结束：用探底期间采到的最小 RTT 刷新基线（需求 2）
	if !l.probeUntil.IsZero() && !now.Before(l.probeUntil) {
		l.finishProbe(now)
	}

	if l.algo == AlgoFixed {
		return
	}

	// 强信号冷却期：线性 +1 恢复（需求 6）
	if now.Before(l.cooldownUntil) {
		l.baseLimit = clampInt(l.baseLimit+1, l.cfg.MinLimit, l.cfg.MaxLimit)
		return
	}

	// 基线漂移：定期主动压低并发重测（需求 2）。
	// 只在“已建立基线 + 满载 + 无排队”时探底：
	//   - 排队严重时跳过，避免过载窗口内再砍一半；
	//   - 空载时跳过，避免给本就不满的流量制造无谓拒绝（探底棘轮）；
	//   - 基线未建立时跳过，先让正常控制环跑起来。
	if l.cfg.ProbeInterval > 0 && l.probeUntil.IsZero() && l.baseline > 0 &&
		now.Sub(l.lastProbe) >= l.cfg.ProbeInterval &&
		!l.queuePressureHigh(now) && l.saturated(now) {
		l.startProbe(now)
		return
	}
	if !l.probeUntil.IsZero() {
		return // 探底进行中，等窗口结束再恢复正常调节
	}

	// 刷新基线：基线窗口内的最小观测 RTT
	l.refreshBaseline(now)
	if l.baseline <= 0 {
		// 还没有有效基线，温和扩张试探
		l.baseLimit = clampInt(l.baseLimit+maxInt(1, l.baseLimit/10), l.cfg.MinLimit, l.cfg.MaxLimit)
		return
	}

	var target float64
	switch l.algo {
	case AlgoVegas:
		target = l.vegasTarget(now)
	case AlgoGradient:
		target = l.gradientTarget(now)
	default:
		return
	}
	l.applyTarget(target, now)
}

func (l *Limiter) startProbe(now time.Time) {
	probe := int(float64(l.baseLimit) * l.cfg.ProbeFactor)
	l.probeLimit = clampInt(probe, l.cfg.MinLimit, l.baseLimit)
	l.probeStart = now
	// 探底窗口收紧到一个 RTT 窗口（上限 1s），尽快恢复正常控制
	window := l.cfg.RTTWindow
	if maxW := time.Second; window > maxW {
		window = maxW
	}
	l.probeUntil = now.Add(window)
	l.lastProbe = now
}

// queuePressureHigh 当前窗口排队比超过 1.15 即视为有排队压力，探底让位。
// 探底的目的是在“无排队”状态下重测基线，因此只要存在可观测排队就不应探底，
// 避免尖峰刚结束、残余排队尚未消散时把上限误砍一半。
func (l *Limiter) queuePressureHigh(now time.Time) bool {
	if l.baseline <= 0 {
		return false
	}
	_, p50, _, _, n := l.ring.stats(now, l.cfg.RTTWindow)
	if n < l.cfg.MinSamples {
		return false
	}
	return float64(p50)/float64(l.baseline) > 1.15
}

// saturated 当前平均在途贴着上限（>=85%）视为满载，探底只在满载时才有信息量。
func (l *Limiter) saturated(now time.Time) bool {
	_, _, _, avgInFlight, n := l.ring.stats(now, l.cfg.RTTWindow)
	if n < l.cfg.MinSamples {
		return false
	}
	return avgInFlight >= float64(l.baseLimit)*0.85
}

// finishProbe 探底结束：探底期间系统排队最少，其中的最小 RTT 直接作为新基线。
func (l *Limiter) finishProbe(now time.Time) {
	probeWindow := now.Sub(l.probeStart)
	minRTT, _, _, _, n := l.ring.stats(now, probeWindow+50*time.Millisecond)
	if n > 0 && minRTT > 0 {
		l.baseline = minRTT
	}
	l.probeUntil = time.Time{}
}

// refreshBaseline 取基线 TTL 窗口内的最小 RTT 作为无排队基线。
func (l *Limiter) refreshBaseline(now time.Time) {
	minRTT, _, _, _, _ := l.ring.stats(now, l.cfg.BaselineTTL)
	if minRTT > 0 {
		l.baseline = minRTT
	}
}

// applyTarget 对目标上限做指数平滑 + 单次变化率钳制（需求 1：上限变化做平滑）。
func (l *Limiter) applyTarget(target float64, now time.Time) {
	if math.IsNaN(target) || target <= 0 {
		return
	}
	smoothed := float64(l.baseLimit)*(1-l.cfg.SmoothAlpha) + target*l.cfg.SmoothAlpha
	maxStep := float64(l.baseLimit) * l.cfg.MaxChangeRate
	if maxStep < 1 {
		maxStep = 1
	}
	newLimit := math.Round(smoothed)
	if d := newLimit - float64(l.baseLimit); d > maxStep {
		newLimit = float64(l.baseLimit) + maxStep
	} else if d < -maxStep {
		newLimit = float64(l.baseLimit) - maxStep
	}
	l.baseLimit = clampInt(int(newLimit), l.cfg.MinLimit, l.cfg.MaxLimit)
}

// SwitchAlgo 热切换算法（页面上可切换）。切换后保留当前上限，仅重置调节节拍。
func (l *Limiter) SwitchAlgo(algo Algorithm) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if algo == l.algo {
		return
	}
	l.algo = algo
	l.cooldownUntil = time.Time{}
	l.probeUntil = time.Time{}
}

// SetLimit 直接设置基础并发上限（夹在配置上下界内），用于入口层宽松闸门等场景。
func (l *Limiter) SetLimit(n int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.baseLimit = clampInt(n, l.cfg.MinLimit, l.cfg.MaxLimit)
}

// Snapshot 取当前状态快照。
func (l *Limiter) Snapshot() Snapshot {
	now := time.Now()
	l.mu.Lock()
	defer l.mu.Unlock()

	_, p50, _, _, _ := l.ring.stats(now, l.cfg.RTTWindow)
	_, waitP50, waitP99, _, _ := l.waitRing.stats(now, 30*time.Second)
	limit := l.effectiveLimit(now)
	ratio := 0.0
	if l.baseline > 0 {
		ratio = float64(p50) / float64(l.baseline)
	}
	return Snapshot{
		Name:              l.cfg.Name,
		Algo:              l.algo,
		Limit:             limit,
		BaseLimit:         l.baseLimit,
		InFlight:          l.inFlight,
		InFlightPrio:      l.inFlightPrio,
		Baseline:          l.baseline,
		CurrentRTT:        p50,
		QueueRatio:        ratio,
		Rejected:          l.rejected,
		Total:             l.total,
		Completed:         l.completed,
		StrongEvents:      l.strongEvents,
		Probing:           !l.probeUntil.IsZero() && now.Before(l.probeUntil),
		HighReserve:       l.highReservation(limit, now),
		HighRate:          l.highRateEWMA,
		Waiters:           l.waitQ.aliveCount(),
		RejectQueueFull:   l.rejectQueueFull,
		RejectWaitTimeout: l.rejectWaitTimeout,
		WaitTimeP50:       waitP50,
		WaitTimeP99:       waitP99,
	}
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
