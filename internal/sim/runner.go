package sim

import (
	"math"
	"math/rand"
	"sync"
	"time"

	"temple/internal/limiter"
)

// Mode 一次运行的限流模式。
type Mode string

const (
	ModeAdaptiveVegas    Mode = "adaptive-vegas"
	ModeAdaptiveGradient Mode = "adaptive-gradient"
	ModeFixed            Mode = "fixed" // 对照组：固定并发上限
)

// RunConfig 单次运行参数。
type RunConfig struct {
	Scenario      *Scenario
	Mode          Mode
	Duration      time.Duration
	FixedLimit    int // ModeFixed 时的上限
	InitialLimit  int // 自适应初始上限
	SnapshotEvery time.Duration
	Workers       int // 最大同时在途 goroutine 数（仿真器自身保护）
}

// Runner 一次场景仿真。
type Runner struct {
	cfg RunConfig
	sc  *Scenario

	downstream *Downstream
	entryL     *limiter.Limiter
	clientL    *limiter.Limiter
	metrics    *Metrics

	rng      *rand.Rand
	rngMu    sync.Mutex
	wg       sync.WaitGroup
	sem      chan struct{} // 仿真器自身的 goroutine 闸门
	startAt  time.Time
	stopOnce sync.Once
	stopC    chan struct{}
}

// prioWeights 流量分档比例：低 60% / 中 32% / 高 8%。
// 高档占比小，过载窗口内其需求才可能被预留额度完整承接（需求 4 的数学前提）。
var prioWeights = []float64{0.60, 0.32, 0.08}

// NewRunner 按配置构造仿真器；同场景同种子下两种模式的随机序列完全一致，保证对比公平。
func NewRunner(cfg RunConfig) *Runner {
	sc := cfg.Scenario
	dcfg := DownConfig{
		Name:         sc.Name,
		Seed:         sc.Seed,
		BaseService:  sc.BaseService,
		InitialCap:   sc.InitialCap,
		CapAt:        sc.CapAt,
		SvcAt:        sc.SvcAt,
		StallAt:      sc.StallAt,
		OverloadProb: sc.OverloadProb,
	}
	down := NewDownstream(dcfg)

	base := limiter.DefaultConfig(sc.Name)
	base.InitialLimit = cfg.InitialLimit
	base.MaxLimit = sc.InitialCap * 8
	base.MinLimit = 1

	var entryL, clientL *limiter.Limiter
	if cfg.Mode == ModeFixed {
		// 对照组：两层都是同一个固定上限
		entryL = limiter.New(withFixed(base, cfg.FixedLimit, "entry"))
		clientL = limiter.New(withFixed(base, cfg.FixedLimit, "client"))
	} else {
		algo := limiter.AlgoVegas
		if cfg.Mode == ModeAdaptiveGradient {
			algo = limiter.AlgoGradient
		}
		// 客户端层：自适应算法，紧贴下游真实容量，是真正的并发边界。
		clientCfg := base
		clientCfg.Name = sc.Name + "/client"
		clientCfg.Algo = algo
		clientL = limiter.New(clientCfg)

		// 入口层：宽松固定总量闸门，不做分档（分档保护统一在客户端层）。
		// 取初始容量的 1.5 倍作为粗粒度保护，瓶颈始终在客户端自适应层，
		// 避免入口层的分档规则把高档请求提前拒掉、与客户端层冲突。
		entryCfg := base
		entryCfg.Name = sc.Name + "/entry"
		entryCfg.TotalOnly = true
		entryCfg.WaitTimeout = 0 // 入口层立即拒绝，不排队，等待承接只在客户端层
		// 入口闸门仅作防雪崩的最后兜底，取一个远大于任何场景瞬时并发的值，
		// 使仿真中永不触顶；真正的并发边界与分档保护由客户端层承担。
		entryGate := 10000
		entryCfg.MaxLimit = entryGate
		entryCfg.MinLimit = 1
		entryL = limiter.New(withFixed(entryCfg, entryGate, "entry"))
	}

	return &Runner{
		cfg:        cfg,
		sc:         sc,
		downstream: down,
		entryL:     entryL,
		clientL:    clientL,
		metrics:    newMetrics(int(cfg.Duration / cfg.SnapshotEvery)),
		rng:        rand.New(rand.NewSource(sc.Seed)),
		sem:        make(chan struct{}, cfg.Workers),
		stopC:      make(chan struct{}),
	}
}

func withFixed(cfg limiter.Config, fixed int, suffix string) limiter.Config {
	cfg.Algo = limiter.AlgoFixed
	cfg.InitialLimit = fixed
	cfg.MinLimit = fixed
	cfg.MaxLimit = fixed
	cfg.Name = cfg.Name + "/" + suffix
	cfg.WaitTimeout = 0 // 固定对照组立即拒绝，不使用等待队列，保证对比口径一致
	return cfg
}

// Limiters 暴露限流器（页面/外部切换算法用）。
func (r *Runner) Limiters() (entry, client *limiter.Limiter) { return r.entryL, r.clientL }

// Metrics 暴露指标。
func (r *Runner) Metrics() *Metrics { return r.metrics }

// Downstream 暴露下游。
func (r *Runner) Downstream() *Downstream { return r.downstream }

// Run 执行完整仿真，阻塞到 Duration 结束并等所有在途请求收尾。
func (r *Runner) Run() *Metrics {
	r.startAt = time.Now()
	r.downstream.start = r.startAt

	var bg sync.WaitGroup
	bg.Add(3)
	go r.capacityTicker(&bg)
	go r.snapshotTicker(&bg)
	go r.arrivalLoop(&bg)

	time.Sleep(r.cfg.Duration)
	r.stopOnce.Do(func() { close(r.stopC) })
	bg.Wait()
	r.wg.Wait() // 等在途请求全部收尾（含超时），上限调低不取消在途（需求 5）
	return r.metrics
}

// Stop 提前结束（演示模式由 HTTP handler 控制时保留）。
func (r *Runner) Stop() { r.stopOnce.Do(func() { close(r.stopC) }) }

// Elapsed 自运行起点经过的时间。
func (r *Runner) Elapsed() time.Duration { return time.Since(r.startAt) }

// capacityTicker 定期把分段容量函数应用到动态信号量。
func (r *Runner) capacityTicker(bg *sync.WaitGroup) {
	defer bg.Done()
	t := time.NewTicker(100 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-r.stopC:
			return
		case now := <-t.C:
			rel := now.Sub(r.startAt)
			r.downstream.sem.setCapacity(r.sc.CapAt(rel))
		}
	}
}

// snapshotTicker 记录限流器状态与下游真实容量的对照。
func (r *Runner) snapshotTicker(bg *sync.WaitGroup) {
	defer bg.Done()
	t := time.NewTicker(r.cfg.SnapshotEvery)
	defer t.Stop()
	for {
		select {
		case <-r.stopC:
			return
		case now := <-t.C:
			rel := now.Sub(r.startAt)
			es := r.entryL.Snapshot()
			cs := r.clientL.Snapshot()
			r.metrics.Snapshots = append(r.metrics.Snapshots, LimitSnapshot{
				T:              rel,
				EntryLimit:     es.Limit,
				EntryInFlight:  es.InFlight,
				ClientLimit:    cs.Limit,
				ClientInFlight: cs.InFlight,
				TrueCapacity:   r.sc.CapAt(rel),
				QueueLen:       r.downstream.QueueLen(),
			})
		}
	}
}

// arrivalLoop 非齐次泊松到达过程：每 100ms 重算一次瞬时到达率。
// 到达循环只负责按泊松节奏“投递”请求，每个请求在独立 goroutine 里完成
// 准入（可能在限流器的等待队列中阻塞）与下游调用，因此到达过程严格符合
// 泊松过程，不受准入等待或下游处理速度影响。
func (r *Runner) arrivalLoop(bg *sync.WaitGroup) {
	defer bg.Done()
	slot := 100 * time.Millisecond
	t := time.NewTicker(slot)
	defer t.Stop()
	for {
		select {
		case <-r.stopC:
			return
		case <-t.C:
			rel := time.Since(r.startAt)
			lambda := r.instantRate(rel) // 请求/秒
			// 泊松过程在长度 slot 的区间内到达数服从 Poisson(lambda*slot)
			n := r.poisson(lambda * slot.Seconds())
			for i := 0; i < n; i++ {
				r.spawn(limiter.Priority(r.drawPrio()))
			}
		}
	}
}

// spawn 为一个到达的请求启动独立处理 goroutine。
// sem 是 goroutine 数量的硬保护：等待队列有界（限流器 MaxWaiters），
// 因此同时阻塞的 goroutine 数也有上界，不会无限增长。
func (r *Runner) spawn(prio limiter.Priority) {
	select {
	case r.sem <- struct{}{}:
	case <-r.stopC:
		return
	}
	r.wg.Add(1)
	go func() {
		defer r.wg.Done()
		defer func() { <-r.sem }()
		r.handleOne(prio)
	}()
}

// handleOne 单个请求的完整生命周期：双层准入（含等待）→ 下游调用 → 双层释放。
func (r *Runner) handleOne(prio limiter.Priority) {
	p := int(prio)
	r.metrics.addArrival(p)
	inWindow := r.inOverloadWindow()
	if inWindow {
		r.metrics.winArrival(p)
	}

	// 第一层：服务入口限流（总量闸门，可能短暂等待）
	entryLease, err := r.entryL.Acquire(prio)
	if err != nil {
		r.metrics.addRejEntry(p)
		if inWindow {
			r.metrics.winReject(p)
		}
		return
	}

	// 第二层：客户端限流（自适应 + 分档 + 优先级等待队列）
	clientLease, err := r.clientL.Acquire(prio)
	if err != nil {
		r.metrics.addRejClient(p)
		if inWindow {
			r.metrics.winReject(p)
		}
		entryLease.Release(limiter.OutcomeError) // 入口层不把本地拒绝视为下游故障
		return
	}

	r.process(prio, entryLease, clientLease, inWindow)
}

// instantRate 当前时刻到达率（请求/秒）。平稳期按容量配比，尖峰期乘 SpikeMul。
func (r *Runner) instantRate(rel time.Duration) float64 {
	base := r.sc.BaseLoad * float64(r.sc.InitialCap) / r.sc.BaseService.Seconds()
	if r.sc.SpikeMul > 1 && rel >= r.sc.SpikeStart && rel < r.sc.SpikeEnd {
		return base * r.sc.SpikeMul
	}
	return base
}

// inOverloadWindow 判断当前是否处于尖峰/过载窗口。
func (r *Runner) inOverloadWindow() bool {
	rel := time.Since(r.startAt)
	return r.sc.SpikeMul > 1 && rel >= r.sc.SpikeStart && rel < r.sc.SpikeEnd
}

func (r *Runner) poisson(mean float64) int {
	// Knuth 泊松抽样；mean 较大时走正态近似避免 O(mean) 循环
	r.rngMu.Lock()
	defer r.rngMu.Unlock()
	if mean < 30 {
		l := math.Exp(-mean)
		k, p := 0, 1.0
		for {
			k++
			p *= r.rng.Float64()
			if p <= l {
				return k - 1
			}
		}
	}
	// 正态近似 N(mean, mean)，四舍五入并夹正
	v := mean + r.rng.NormFloat64()*math.Sqrt(mean)
	if v < 0 {
		v = 0
	}
	return int(v + 0.5)
}

func (r *Runner) drawPrio() int {
	r.rngMu.Lock()
	roll := r.rng.Float64()
	r.rngMu.Unlock()
	acc := 0.0
	for p, w := range prioWeights {
		acc += w
		if roll < acc {
			return p
		}
	}
	return 0
}

// process 已获双层名额的请求：调用下游并把结果反馈给两层限流器。
func (r *Runner) process(prio limiter.Priority, entryLease, clientLease *limiter.Lease, inWindow bool) {
	p := int(prio)

	// 带超时的下游调用
	timer := time.NewTimer(r.sc.Timeout)
	defer timer.Stop()
	stopC := make(chan struct{})
	callDone := make(chan callReturn, 1)

	go func() {
		rtt, res := r.downstream.Call(stopC)
		callDone <- callReturn{rtt: rtt, res: res}
	}()

	var cr callReturn
	select {
	case cr = <-callDone:
		close(stopC)
	case <-timer.C:
		close(stopC)
		cr = <-callDone // 等被取消的调用返回，拿到真实经过时间（需求 8）
		cr.timedOut = true
	}

	r.metrics.recordRTT(p, cr.rtt)

	// 结果映射到限流器 Outcome
	outcome := limiter.OutcomeSuccess
	switch {
	case cr.timedOut || cr.res == ResultTimeout:
		outcome = limiter.OutcomeTimeout
		r.metrics.addTimeout(p)
		if inWindow {
			r.metrics.winTimeout(p)
		}
	case cr.res == ResultOverload:
		outcome = limiter.OutcomeOverload
		r.metrics.addOverload(p)
	default:
		r.metrics.addSuccess(p)
		if inWindow {
			r.metrics.winSuccess(p)
		}
	}

	// 双层同时反馈：过载/超时在两层都是强信号（需求 6）
	clientLease.Release(outcome)
	entryLease.Release(outcome)
}

type callReturn struct {
	rtt      time.Duration
	res      CallResult
	timedOut bool
}
