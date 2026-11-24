package demo

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"temple/internal/limiter"
	"temple/internal/sim"
)

// HistoryPoint 监控页面上一个时刻的采样点。
type HistoryPoint struct {
	T            float64 `json:"t"`
	Limit        int     `json:"limit"`
	InFlight     int     `json:"inFlight"`
	Waiters      int     `json:"waiters"`
	BaselineMs   float64 `json:"baselineMs"`
	CurrentRTTMs float64 `json:"currentRttMs"`
	QueueRatio   float64 `json:"queueRatio"`
	Rejected     int64   `json:"rejected"`
	Arrived      int64   `json:"arrived"`
	Success      int64   `json:"success"`
	TrueCapacity int     `json:"trueCapacity"`
}

// EndpointSpec 一个演示接口的静态描述。
type EndpointSpec struct {
	Key         string
	Name        string
	BaseService time.Duration
	Capacity    int
	BaseRate    float64 // 平稳期到达率（请求/秒）
	SpikeMul    float64 // 节庆模式倍数
	PrioWeights []float64
}

// Endpoint 一个运行中的接口：下游 + 入口/客户端两层限流 + 流量发生器 + 历史曲线。
type Endpoint struct {
	Spec EndpointSpec

	downstream *sim.Downstream
	entryL     *limiter.Limiter
	clientL    *limiter.Limiter

	historyMu sync.Mutex
	history   []HistoryPoint

	startAt time.Time
	stopC   chan struct{}
	wg      sync.WaitGroup

	// 简单计数（演示用，统计口径与 bench 一致）
	arrived   [3]int64
	rejected  [3]int64
	succeeded [3]int64

	rngMu sync.Mutex
	rng   *rand.Rand
}

// Service 整个演示服务，管理多个接口。
type Service struct {
	mu        sync.RWMutex
	endpoints []*Endpoint
	festival  atomic.Bool
	historyN  int
}

const historyPoints = 300 // 保留最近 300 个采样点（200ms 一个，共 60s）

// DefaultEndpoints 寺庙场景的四个接口。
func DefaultEndpoints() []EndpointSpec {
	return []EndpointSpec{
		{
			Key: "booking", Name: "法会预约登记（高档为主）",
			BaseService: 35 * time.Millisecond, Capacity: 16,
			BaseRate: 180, SpikeMul: 12,
			PrioWeights: []float64{0.25, 0.35, 0.40},
		},
		{
			Key: "lamp", Name: "点灯祈福（中档为主）",
			BaseService: 30 * time.Millisecond, Capacity: 22,
			BaseRate: 320, SpikeMul: 14,
			PrioWeights: []float64{0.45, 0.45, 0.10},
		},
		{
			Key: "meal", Name: "斋饭订餐（中档为主）",
			BaseService: 45 * time.Millisecond, Capacity: 14,
			BaseRate: 160, SpikeMul: 11,
			PrioWeights: []float64{0.50, 0.42, 0.08},
		},
		{
			Key: "query", Name: "客堂咨询查询（低档为主）",
			BaseService: 20 * time.Millisecond, Capacity: 30,
			BaseRate: 600, SpikeMul: 13,
			PrioWeights: []float64{0.75, 0.20, 0.05},
		},
	}
}

// NewService 构造并启动演示服务。
func NewService(specs []EndpointSpec, seedBase int64) *Service {
	s := &Service{historyN: historyPoints}
	for i, spec := range specs {
		ep := newEndpoint(spec, seedBase+int64(i)*101)
		s.endpoints = append(s.endpoints, ep)
	}
	for _, ep := range s.endpoints {
		ep.start(s)
	}
	return s
}

func newEndpoint(spec EndpointSpec, seed int64) *Endpoint {
	down := sim.NewDownstream(sim.DownConfig{
		Name:        spec.Key,
		Seed:        seed,
		BaseService: spec.BaseService,
		InitialCap:  spec.Capacity,
	})

	base := limiter.DefaultConfig(spec.Key)
	base.InitialLimit = spec.Capacity
	base.MaxLimit = spec.Capacity * 8

	// 客户端层：自适应 + 分档 + 优先级等待队列
	clientCfg := base
	clientCfg.Name = spec.Key + "/client"
	clientL := limiter.New(clientCfg)

	// 入口层：宽松总量闸门，不分档、不等待
	entryCfg := base
	entryCfg.Name = spec.Key + "/entry"
	entryCfg.TotalOnly = true
	entryCfg.WaitTimeout = 0
	gate := 10000
	entryCfg.MaxLimit = gate
	entryCfg.MinLimit = 1
	entryL := limiter.New(withFixedGate(entryCfg, gate))

	return &Endpoint{
		Spec:       spec,
		downstream: down,
		entryL:     entryL,
		clientL:    clientL,
		stopC:      make(chan struct{}),
		rng:        rand.New(rand.NewSource(seed)),
		history:    make([]HistoryPoint, 0, historyPoints),
	}
}

func withFixedGate(cfg limiter.Config, fixed int) limiter.Config {
	cfg.Algo = limiter.AlgoFixed
	cfg.InitialLimit = fixed
	cfg.MinLimit = fixed
	cfg.MaxLimit = fixed
	return cfg
}

func (e *Endpoint) start(svc *Service) {
	e.startAt = time.Now()
	e.wg.Add(3)
	go e.loadLoop(svc)
	go e.sampleLoop()
	go e.capacityDriftLoop(svc)
}

// Stop 停止全部后台 goroutine。
func (s *Service) Stop() {
	for _, ep := range s.endpoints {
		close(ep.stopC)
	}
	for _, ep := range s.endpoints {
		ep.wg.Wait()
	}
}

// SetFestival 开关节庆模式（十几倍流量）。
func (s *Service) SetFestival(on bool) { s.festival.Store(on) }

// Festival 当前是否节庆模式。
func (s *Service) Festival() bool { return s.festival.Load() }

// SwitchAlgo 热切换全部接口客户端层的自适应算法。
func (s *Service) SwitchAlgo(algo limiter.Algorithm) {
	for _, ep := range s.endpoints {
		ep.clientL.SwitchAlgo(algo)
	}
}

// Algo 返回当前算法（取第一个接口即可，全部同步切换）。
func (s *Service) Algo() limiter.Algorithm {
	if len(s.endpoints) == 0 {
		return limiter.AlgoVegas
	}
	return s.endpoints[0].clientL.Snapshot().Algo
}

// timeSince 包一层便于在 http.go 里取相对时间。
func timeSince(t time.Time) time.Duration { return time.Since(t) }
