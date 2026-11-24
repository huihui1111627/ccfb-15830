package sim

import (
	"math"
	"math/rand"
	"sync"
	"time"
)

// dynWaiter 一个排队等待容量令牌的请求。
// granted 与 canceled 在 mu 保护下裁决，保证“令牌转交”和“取消”只可能有一个生效，
// 消除 select 在两个信号同时就绪时选错分支导致的重复归还。
type dynWaiter struct {
	stopC    <-chan struct{}
	ch       chan struct{} // 拿到令牌时被唤醒（缓冲 1，发送不阻塞）
	granted  bool          // 令牌已转交给本 waiter（mu 保护）
	canceled bool          // 调用方已超时取消（mu 保护）
}

// dynSem 动态容量信号量：容量可以在运行中随时调整（Go 原生带缓冲 channel 不支持改容量）。
// 等待队列有界（queueLimit），超出立即拒绝，模拟真实连接池的有界排队。
//
// 记账模型（mu 保护）：
//   - inflight：已拿到名额、正在执行的请求数（含被 granted 的 waiter）
//   - tokens：当前空闲可立即发放的名额，恒 >= 0
//   - debt：缩容时名额都被在途占用、无法即时回收的“欠账”，恒 >= 0
//
// 不变量：inflight + tokens - debt == capacity。
// 缩容不强行取消在途请求（需求 5），收不回的名额记为 debt；
// 之后请求释放名额时优先偿还 debt，还清前不产生新的空闲名额。
type dynSem struct {
	mu         sync.Mutex
	capacity   int
	tokens     int
	inflight   int
	debt       int
	waiters    []*dynWaiter
	queueLimit int
}

func newDynSem(capacity int) *dynSem {
	return &dynSem{capacity: capacity, tokens: capacity, queueLimit: capacity * 3}
}

// acquire 取一个令牌。
//   - (true, false)：拿到令牌
//   - (false, false)：排队中被调用方超时取消
//   - (false, true)：等待队列已满，调用方应立即按过载拒绝
func (s *dynSem) acquire(stopC <-chan struct{}) (acquired, rejected bool) {
	s.mu.Lock()
	if s.tokens > 0 {
		s.tokens--
		s.inflight++
		s.mu.Unlock()
		return true, false
	}
	s.reapCanceledLocked()
	if len(s.waiters) >= s.queueLimit {
		s.mu.Unlock()
		return false, true
	}
	w := &dynWaiter{stopC: stopC, ch: make(chan struct{}, 1)}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		// 令牌已在 mu 保护下转交给本 waiter，inflight 已计入，直接持有
		return true, false
	case <-stopC:
		s.mu.Lock()
		if w.granted {
			// 令牌已先一步转交给本 waiter（select 恰好选了取消分支）。
			// 本 waiter 不再使用该名额，等价于一次释放：交给其他排队者或退回令牌。
			// 全程在同一把锁内完成，与任何外部 release 互斥，不会重复计数。
			s.returnSlotLocked()
			s.mu.Unlock()
			return false, false
		}
		w.canceled = true
		s.removeWaiterLocked(w)
		s.mu.Unlock()
		return false, false
	}
}

// returnSlotLocked 释放一个持有的名额（调用方持 mu）：
// 优先转交给队首未取消的排队者（inflight 净值不变）；
// 没有排队者时 inflight--，并优先偿还缩容欠账，还清后才退回为空闲 token。
func (s *dynSem) returnSlotLocked() {
	if s.handoffLocked() {
		return
	}
	s.inflight--
	if s.debt > 0 {
		s.debt-- // 偿还欠账，不产生新空闲名额
		return
	}
	s.tokens++
}

// reapCanceledLocked 压缩队列里已取消但还没被移除的 waiter（调用方持 mu）。
func (s *dynSem) reapCanceledLocked() {
	if len(s.waiters) == 0 {
		return
	}
	alive := s.waiters[:0]
	for _, w := range s.waiters {
		if !w.canceled {
			alive = append(alive, w)
		}
	}
	s.waiters = alive
}

func (s *dynSem) removeWaiterLocked(w *dynWaiter) {
	for i, x := range s.waiters {
		if x == w {
			s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
			return
		}
	}
}

// handoffLocked 把一个“已被占用、正在释放”的名额转交给队首未取消的排队者。
// 释放者离开、接手者进入，占用数净值为 0，因此不改动 inflight（调用方持 mu）。
func (s *dynSem) handoffLocked() bool {
	for len(s.waiters) > 0 {
		w := s.waiters[0]
		s.waiters = s.waiters[1:]
		if w.canceled {
			continue
		}
		w.granted = true
		w.ch <- struct{}{} // 缓冲通道，不阻塞
		return true
	}
	return false
}

// handoffNewSlotLocked 把扩容产生的“全新名额”交给队首排队者。
// 这是新增占用，必须 inflight++（与“先 tokens++ 再被 acquire：tokens--、inflight++”等价）。
func (s *dynSem) handoffNewSlotLocked() bool {
	for len(s.waiters) > 0 {
		w := s.waiters[0]
		s.waiters = s.waiters[1:]
		if w.canceled {
			continue
		}
		w.granted = true
		w.ch <- struct{}{}
		s.inflight++
		return true
	}
	return false
}

func (s *dynSem) release() {
	s.mu.Lock()
	// 没有等待者时 tokens++ 会填平缩容欠账（负 token），填平前 acquire 因
	// tokens<=0 拿不到名额；不变量 inflight + tokens == capacity 始终保持。
	s.returnSlotLocked()
	s.mu.Unlock()
}

// setCapacity 运行中调整容量。调小后多余在途请求继续跑完、不强行取消（需求 5）。
// 排队上限随容量联动（容量的 3 倍），已在队列里的请求不受影响。
//   - 扩容：新增名额先转交排队者，其余进 tokens；若此前有欠账先抵消欠账。
//   - 缩容：优先回收空闲 tokens；不足部分记为 debt，等在途释放时偿还。
func (s *dynSem) setCapacity(c int) {
	s.mu.Lock()
	old := s.capacity
	s.capacity = c
	s.queueLimit = c * 3
	if c > old {
		add := c - old
		// 先抵消历史欠账（抵消欠账不产生可发放名额）
		if s.debt > 0 {
			used := add
			if used > s.debt {
				used = s.debt
			}
			s.debt -= used
			add -= used
		}
		// 剩余新增名额先转交排队者（新占用，inflight++），再进 tokens
		for add > 0 && s.handoffNewSlotLocked() {
			add--
		}
		s.tokens += add
	} else if c < old {
		drop := old - c
		for drop > 0 && s.tokens > 0 {
			s.tokens--
			drop--
		}
		s.debt += drop // 收不回的名额记为欠账，inflight 保持不变
	}
	s.mu.Unlock()
}

func isClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

// Downstream 一个被模拟的下游服务（数据库/接口），延迟与容量都随时间变化。
type Downstream struct {
	Name string

	rng *rand.Rand

	sem       *dynSem
	serviceMu sync.Mutex
	baseSvc   time.Duration

	// 容量随时间变化（容量突降/恢复）
	capAt func(rel time.Duration) int

	// 服务时间随时间变化（延迟缓慢漂高）
	svcAt func(rel time.Duration, base time.Duration) time.Duration

	// 周期性卡顿：返回该时刻附加的停顿
	stallAt func(rel time.Duration) time.Duration

	// 下游主动过载：每请求独立概率返回过载（强信号来源之一）
	overloadProb func(rel time.Duration) float64

	// 外部容量覆盖（演示服务用）：>0 时优先于 capAt，<=0 表示不覆盖
	extCapMu sync.Mutex
	extCap   int

	start time.Time
}

// DownConfig 下游构造参数。
type DownConfig struct {
	Name         string
	Seed         int64
	BaseService  time.Duration
	InitialCap   int
	CapAt        func(rel time.Duration) int
	SvcAt        func(rel time.Duration, base time.Duration) time.Duration
	StallAt      func(rel time.Duration) time.Duration
	OverloadProb func(rel time.Duration) float64
}

func NewDownstream(cfg DownConfig) *Downstream {
	d := &Downstream{
		Name:         cfg.Name,
		rng:          rand.New(rand.NewSource(cfg.Seed)),
		sem:          newDynSem(cfg.InitialCap),
		baseSvc:      cfg.BaseService,
		capAt:        cfg.CapAt,
		svcAt:        cfg.SvcAt,
		stallAt:      cfg.StallAt,
		overloadProb: cfg.OverloadProb,
		start:        time.Now(),
	}
	if d.capAt == nil {
		d.capAt = func(time.Duration) int { return cfg.InitialCap }
	}
	if d.svcAt == nil {
		d.svcAt = func(_ time.Duration, base time.Duration) time.Duration { return base }
	}
	if d.stallAt == nil {
		d.stallAt = func(time.Duration) time.Duration { return 0 }
	}
	if d.overloadProb == nil {
		d.overloadProb = func(time.Duration) float64 { return 0 }
	}
	// 把当前容量对齐到 capAt(0)
	d.sem.setCapacity(d.capAt(0))
	return d
}

// CapacityAt 查询某时刻的逻辑容量（报表里的“真实最优并发”基准）。
// 若演示服务设置了外部覆盖，优先返回覆盖值。
func (d *Downstream) CapacityAt(rel time.Duration) int {
	d.extCapMu.Lock()
	ext := d.extCap
	d.extCapMu.Unlock()
	if ext > 0 {
		return ext
	}
	return d.capAt(rel)
}

// SetExternalCapacity 设置外部容量覆盖（演示服务模拟数据库容量突降用）。
// 同时立即把信号量容量调整过去；传入 <=0 清除覆盖。
func (d *Downstream) SetExternalCapacity(c int) {
	d.extCapMu.Lock()
	d.extCap = c
	d.extCapMu.Unlock()
	if c > 0 {
		d.sem.setCapacity(c)
	}
}

// QueueLen 下游当前活跃排队等待容量的请求数（不含已取消待清理的）。
func (d *Downstream) QueueLen() int {
	d.sem.mu.Lock()
	defer d.sem.mu.Unlock()
	active := 0
	for _, w := range d.sem.waiters {
		if !w.canceled {
			active++
		}
	}
	return active
}

// RawWaiters 调试用：waiter 切片原始长度与其中已取消数量。
func (d *Downstream) RawWaiters() (total, canceled int) {
	d.sem.mu.Lock()
	defer d.sem.mu.Unlock()
	for _, w := range d.sem.waiters {
		if w.canceled {
			canceled++
		}
	}
	return len(d.sem.waiters), canceled
}

// InFlight 下游当前占用容量的在途请求数。
func (d *Downstream) InFlight() int {
	d.sem.mu.Lock()
	defer d.sem.mu.Unlock()
	return d.sem.inflight
}

// Call 处理一次下游调用。stopC 关闭表示调用方超时放弃。
// 返回服务延迟（含排队）和结果；超时放弃时延迟照样返回，供上层计入样本（需求 8）。
func (d *Downstream) Call(stopC <-chan struct{}) (time.Duration, CallResult) {
	callStart := time.Now()
	rel := callStart.Sub(d.start)

	// 下游在入口直接拒过载：不占容量，立刻返回强信号
	if p := d.overloadProb(rel); p > 0 {
		d.serviceMu.Lock()
		roll := d.rng.Float64()
		d.serviceMu.Unlock()
		if roll < p {
			return time.Since(callStart), ResultOverload
		}
	}

	// 排队等容量令牌；队列满则下游立即返回过载（强信号），不无限堆积
	acquired, rejected := d.sem.acquire(stopC)
	if rejected {
		return time.Since(callStart), ResultOverload
	}
	if !acquired {
		return time.Since(callStart), ResultTimeout
	}
	defer d.sem.release()

	// 拿到令牌后如果调用方已超时，直接返回（服务时间样本仍已包含排队时间）
	if isClosed(stopC) {
		return time.Since(callStart), ResultTimeout
	}

	rel2 := time.Since(d.start)
	d.serviceMu.Lock()
	svc := d.svcAt(rel2, d.baseSvc)
	// ±15% 服务时间抖动，模拟真实请求差异
	jitter := time.Duration(d.rng.Float64() * 0.3 * float64(svc))
	d.serviceMu.Unlock()
	svc = svc - jitter/2 + jitter
	stall := d.stallAt(rel2)
	total := svc + stall

	timer := time.NewTimer(total)
	defer timer.Stop()
	select {
	case <-timer.C:
		return time.Since(callStart), ResultSuccess
	case <-stopC:
		// 调用方超时：模拟被取消的在途请求，真实经过时间完整保留
		return time.Since(callStart), ResultTimeout
	}
}

// CallResult 下游调用结果。
type CallResult int

const (
	ResultSuccess CallResult = iota
	ResultTimeout
	ResultOverload
)

// CapPoint 容量分段点：从 At 时刻起容量变为 C。
type CapPoint struct {
	At time.Duration
	C  int
}

// piecewiseCap 生成“分段常量”容量函数。
func piecewiseCap(points ...CapPoint) func(time.Duration) int {
	return func(rel time.Duration) int {
		c := points[0].C
		for _, p := range points {
			if rel >= p.At {
				c = p.C
			}
		}
		return c
	}
}

// linearDriftSvc 服务时间从 driftStart 起线性漂高，最大涨幅 maxDrift（相对 base）。
func linearDriftSvc(driftStart time.Duration, maxDrift float64) func(time.Duration, time.Duration) time.Duration {
	return func(rel, base time.Duration) time.Duration {
		if rel <= driftStart {
			return base
		}
		f := math.Min(maxDrift, maxDrift*(float64(rel-driftStart)/float64(driftStart)))
		return time.Duration(float64(base) * (1 + f))
	}
}
