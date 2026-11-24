package sim

import "time"

// Scenario 一个完整测试场景：下游行为 + 流量曲线 + 客户端预算。
type Scenario struct {
	ID   int
	Name string
	Seed int64

	BaseService time.Duration // 无排队服务时间（即理论延迟基线）
	InitialCap  int           // 初始容量（并发处理槽）
	QueueMax    int           // 下游排队上限，超出即过载拒绝

	CapAt        func(rel time.Duration) int
	SvcAt        func(rel time.Duration, base time.Duration) time.Duration
	StallAt      func(rel time.Duration) time.Duration
	OverloadProb func(rel time.Duration) float64

	// 流量
	BaseLoad   float64 // 平稳期到达率 = BaseLoad * 初始容量 / 服务时间
	SpikeMul   float64 // 尖峰期到达率倍数（需求里的十几倍）
	SpikeStart time.Duration
	SpikeEnd   time.Duration

	// 客户端
	Timeout time.Duration

	// MildSLOReachable 为 true 表示过载强度在物理上允许高档请求近乎全部成功
	// （温和过载），这类场景对高档成功率做 95% 硬性 SLO 验收；
	// 极端过载（十几倍流量）下游吞吐不足以服务全部请求，改用保护倍数评估。
	MildSLOReachable bool

	// HasCapacityDrop 为 true 表示场景包含下游容量主动下降。
	// 容量下降后存在不可避免的收敛滞后（在途请求不取消、队列需排空），
	// 这类场景用强信号响应与相对固定上限的表现评估，不参与稳态 MAPE 硬阈值。
	HasCapacityDrop bool
}

// capAt 便于包内构造分段容量。
func capAt(points ...CapPoint) func(time.Duration) int { return piecewiseCap(points...) }

// periodicStall 周期卡顿：每 period 出现一次持续 width 的卡顿，附加 extra 延迟。
func periodicStall(period, width, extra time.Duration) func(time.Duration) time.Duration {
	return func(rel time.Duration) time.Duration {
		phase := rel % period
		if phase < width {
			return extra
		}
		return 0
	}
}

// constOverload 某时间窗内下游以固定概率返回过载。
func windowOverload(start, end time.Duration, p float64) func(time.Duration) float64 {
	return func(rel time.Duration) float64 {
		if rel >= start && rel < end {
			return p
		}
		return 0
	}
}

// BuildScenarios 构造 30 个固定种子场景，覆盖四类故障与多种组合。
// 种子全部硬编码，任何机器上跑出来的结果一致。
func BuildScenarios() []*Scenario {
	s := make([]*Scenario, 0, 30)
	add := func(sc *Scenario) {
		sc.ID = len(s) + 1
		if sc.Seed == 0 {
			sc.Seed = 1000 + int64(sc.ID)*97
		}
		if sc.QueueMax == 0 {
			sc.QueueMax = sc.InitialCap * 3
		}
		if sc.Timeout == 0 {
			// 超时显著大于服务时间与排队等待，避免请求刚入下游队列就被取消；
			// 真正的过载由下游有界队列返回 Overload 强信号来表达。
			sc.Timeout = 2500 * time.Millisecond
		}
		if sc.BaseLoad == 0 {
			// 平稳期负载为容量的 70%：系统有空闲余量、能测出无排队基线，
			// 压力全部来自尖峰期 SpikeMul 倍流量与下游容量/延迟变化。
			sc.BaseLoad = 0.7
		}
		// 未显式给出容量/服务时间/卡顿/过载函数时补默认实现
		if sc.CapAt == nil {
			sc.CapAt = capAt(CapPoint{0, sc.InitialCap})
		}
		if sc.SvcAt == nil {
			sc.SvcAt = func(_ time.Duration, base time.Duration) time.Duration { return base }
		}
		if sc.StallAt == nil {
			sc.StallAt = func(time.Duration) time.Duration { return 0 }
		}
		if sc.OverloadProb == nil {
			sc.OverloadProb = func(time.Duration) float64 { return 0 }
		}
		// 自动检测容量下降：在 0~12s 内采样 CapAt，若任何时刻低于初始容量则标记
		if !sc.HasCapacityDrop {
			for ms := 0; ms <= 12000; ms += 200 {
				if sc.CapAt(time.Duration(ms)*time.Millisecond) < sc.InitialCap {
					sc.HasCapacityDrop = true
					break
				}
			}
		}
		s = append(s, sc)
	}

	// ---------- 1-6：容量突然下降 ----------
	add(&Scenario{Name: "cap-drop-half", BaseService: 40 * time.Millisecond, InitialCap: 20,
		CapAt:    capAt(CapPoint{0, 20}, CapPoint{4 * time.Second, 10}, CapPoint{9 * time.Second, 20}),
		SpikeMul: 12, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8500 * time.Millisecond})
	add(&Scenario{Name: "cap-drop-severe", BaseService: 35 * time.Millisecond, InitialCap: 24,
		CapAt:    capAt(CapPoint{0, 24}, CapPoint{3500 * time.Millisecond, 6}, CapPoint{9 * time.Second, 18}),
		SpikeMul: 14, SpikeStart: 3 * time.Second, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "cap-drop-slow", BaseService: 60 * time.Millisecond, InitialCap: 12,
		CapAt:    capAt(CapPoint{0, 12}, CapPoint{4 * time.Second, 5}),
		SpikeMul: 10, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "cap-drop-recover-osc", BaseService: 30 * time.Millisecond, InitialCap: 16,
		CapAt: capAt(CapPoint{0, 16}, CapPoint{3 * time.Second, 8}, CapPoint{6 * time.Second, 14},
			CapPoint{8 * time.Second, 6}, CapPoint{10 * time.Second, 16}),
		SpikeMul: 12, SpikeStart: 2500 * time.Millisecond, SpikeEnd: 9 * time.Second})
	add(&Scenario{Name: "cap-drop-with-spike15", BaseService: 25 * time.Millisecond, InitialCap: 30,
		CapAt:    capAt(CapPoint{0, 30}, CapPoint{5 * time.Second, 12}),
		SpikeMul: 15, SpikeStart: 4500 * time.Millisecond, SpikeEnd: 9 * time.Second})
	add(&Scenario{Name: "cap-tiny-then-grow", BaseService: 45 * time.Millisecond, InitialCap: 8,
		CapAt:    capAt(CapPoint{0, 8}, CapPoint{4 * time.Second, 3}, CapPoint{8 * time.Second, 20}),
		SpikeMul: 12, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 9 * time.Second})

	// ---------- 7-12：延迟缓慢漂高 ----------
	add(&Scenario{Name: "drift-2x", BaseService: 40 * time.Millisecond, InitialCap: 16,
		SvcAt:    linearDriftSvc(2*time.Second, 1.0),
		SpikeMul: 12, SpikeStart: 4 * time.Second, SpikeEnd: 8500 * time.Millisecond})
	add(&Scenario{Name: "drift-50pct", BaseService: 50 * time.Millisecond, InitialCap: 14,
		SvcAt:    linearDriftSvc(1500*time.Millisecond, 0.5),
		SpikeMul: 11, SpikeStart: 3 * time.Second, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "drift-then-cap-drop", BaseService: 35 * time.Millisecond, InitialCap: 18,
		SvcAt:    linearDriftSvc(2*time.Second, 0.8),
		CapAt:    capAt(CapPoint{0, 18}, CapPoint{6 * time.Second, 9}),
		SpikeMul: 12, SpikeStart: 5500 * time.Millisecond, SpikeEnd: 9500 * time.Millisecond})
	add(&Scenario{Name: "drift-fast-3x", BaseService: 30 * time.Millisecond, InitialCap: 20,
		SvcAt:    linearDriftSvc(1*time.Second, 2.0),
		SpikeMul: 13, SpikeStart: 4 * time.Second, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "drift-slow-service", BaseService: 80 * time.Millisecond, InitialCap: 10,
		SvcAt:    linearDriftSvc(2*time.Second, 0.7),
		SpikeMul: 10, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "drift-mild-no-spike", BaseService: 40 * time.Millisecond, InitialCap: 16,
		SvcAt:    linearDriftSvc(1*time.Second, 0.6),
		BaseLoad: 0.6, SpikeMul: 1})

	// ---------- 13-18：周期性卡顿 ----------
	add(&Scenario{Name: "stall-2s-cycle", BaseService: 30 * time.Millisecond, InitialCap: 20,
		StallAt:  periodicStall(2*time.Second, 400*time.Millisecond, 250*time.Millisecond),
		SpikeMul: 12, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8500 * time.Millisecond})
	add(&Scenario{Name: "stall-1s-cycle-heavy", BaseService: 25 * time.Millisecond, InitialCap: 24,
		StallAt:  periodicStall(time.Second, 300*time.Millisecond, 400*time.Millisecond),
		SpikeMul: 13, SpikeStart: 3 * time.Second, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "stall-long-freeze", BaseService: 35 * time.Millisecond, InitialCap: 18,
		StallAt:  periodicStall(4*time.Second, 800*time.Millisecond, 700*time.Millisecond),
		SpikeMul: 12, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 9 * time.Second})
	add(&Scenario{Name: "stall-and-cap-drop", BaseService: 40 * time.Millisecond, InitialCap: 16,
		StallAt:  periodicStall(2*time.Second, 350*time.Millisecond, 300*time.Millisecond),
		CapAt:    capAt(CapPoint{0, 16}, CapPoint{5 * time.Second, 8}),
		SpikeMul: 12, SpikeStart: 4500 * time.Millisecond, SpikeEnd: 9 * time.Second})
	add(&Scenario{Name: "stall-frequent-tiny", BaseService: 20 * time.Millisecond, InitialCap: 28,
		StallAt:  periodicStall(500*time.Millisecond, 100*time.Millisecond, 150*time.Millisecond),
		SpikeMul: 14, SpikeStart: 3 * time.Second, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "stall-only-mild", BaseService: 45 * time.Millisecond, InitialCap: 12,
		StallAt:  periodicStall(3*time.Second, 500*time.Millisecond, 200*time.Millisecond),
		BaseLoad: 0.65, SpikeMul: 1})

	// ---------- 19-24：流量猛涨十几倍（下游本身平稳） ----------
	add(&Scenario{Name: "spike-12x", BaseService: 35 * time.Millisecond, InitialCap: 20,
		SpikeMul: 12, SpikeStart: 4 * time.Second, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "spike-15x", BaseService: 30 * time.Millisecond, InitialCap: 24,
		SpikeMul: 15, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8 * time.Second})
	add(&Scenario{Name: "spike-10x-long", BaseService: 50 * time.Millisecond, InitialCap: 12,
		SpikeMul: 10, SpikeStart: 3 * time.Second, SpikeEnd: 9500 * time.Millisecond})
	add(&Scenario{Name: "spike-double-wave", BaseService: 40 * time.Millisecond, InitialCap: 16,
		SpikeMul: 13, SpikeStart: 2500 * time.Millisecond, SpikeEnd: 5 * time.Second,
		CapAt: capAt(CapPoint{0, 16}, CapPoint{7 * time.Second, 16})})
	add(&Scenario{Name: "spike-small-cap", BaseService: 40 * time.Millisecond, InitialCap: 8,
		SpikeMul: 14, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8500 * time.Millisecond})
	add(&Scenario{Name: "spike-big-cap", BaseService: 20 * time.Millisecond, InitialCap: 40,
		SpikeMul: 12, SpikeStart: 4 * time.Second, SpikeEnd: 8 * time.Second})

	// ---------- 25-30：过载强信号与复合场景 ----------
	add(&Scenario{Name: "overload-window", BaseService: 35 * time.Millisecond, InitialCap: 18,
		OverloadProb: windowOverload(4*time.Second, 8*time.Second, 0.3),
		SpikeMul:     12, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8500 * time.Millisecond})
	add(&Scenario{Name: "overload-persistent", BaseService: 40 * time.Millisecond, InitialCap: 16,
		OverloadProb: windowOverload(3*time.Second, 1<<62, 0.15),
		SpikeMul:     11, SpikeStart: 2500 * time.Millisecond, SpikeEnd: 8500 * time.Millisecond})
	add(&Scenario{Name: "combo-drop-drift", BaseService: 35 * time.Millisecond, InitialCap: 20,
		CapAt:    capAt(CapPoint{0, 20}, CapPoint{5 * time.Second, 10}),
		SvcAt:    linearDriftSvc(2*time.Second, 0.5),
		SpikeMul: 12, SpikeStart: 4500 * time.Millisecond, SpikeEnd: 9 * time.Second})
	add(&Scenario{Name: "combo-drop-stall", BaseService: 30 * time.Millisecond, InitialCap: 22,
		CapAt:    capAt(CapPoint{0, 22}, CapPoint{4500 * time.Millisecond, 9}),
		StallAt:  periodicStall(2*time.Second, 400*time.Millisecond, 350*time.Millisecond),
		SpikeMul: 13, SpikeStart: 4 * time.Second, SpikeEnd: 9 * time.Second})
	add(&Scenario{Name: "combo-all-three", BaseService: 40 * time.Millisecond, InitialCap: 18,
		CapAt:    capAt(CapPoint{0, 18}, CapPoint{5 * time.Second, 8}),
		SvcAt:    linearDriftSvc(2*time.Second, 0.6),
		StallAt:  periodicStall(2500*time.Millisecond, 350*time.Millisecond, 250*time.Millisecond),
		SpikeMul: 12, SpikeStart: 4500 * time.Millisecond, SpikeEnd: 9500 * time.Millisecond})
	add(&Scenario{Name: "baseline-quiet-busy", BaseService: 45 * time.Millisecond, InitialCap: 14,
		CapAt:    capAt(CapPoint{0, 14}, CapPoint{4 * time.Second, 14}, CapPoint{7 * time.Second, 7}),
		BaseLoad: 0.9, SpikeMul: 12, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 9 * time.Second})

	// ---------- 31-34：温和过载（2.5~3 倍），高档 95% SLO 物理可达，用于正式 SLO 验收 ----------
	// 这类场景下系统吞吐仍能覆盖高档全部需求，分档限流应把拒绝几乎全部压给低档。
	add(&Scenario{Name: "mild-spike-3x", BaseService: 35 * time.Millisecond, InitialCap: 20,
		SpikeMul: 3, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8500 * time.Millisecond,
		MildSLOReachable: true})
	add(&Scenario{Name: "mild-spike-25x-drift", BaseService: 40 * time.Millisecond, InitialCap: 16,
		SvcAt:    linearDriftSvc(2*time.Second, 0.4),
		SpikeMul: 2.5, SpikeStart: 3 * time.Second, SpikeEnd: 8 * time.Second,
		MildSLOReachable: true})
	add(&Scenario{Name: "mild-capdrop-3x", BaseService: 30 * time.Millisecond, InitialCap: 24,
		CapAt:    capAt(CapPoint{0, 24}, CapPoint{4500 * time.Millisecond, 14}),
		SpikeMul: 3, SpikeStart: 4 * time.Second, SpikeEnd: 9 * time.Second,
		MildSLOReachable: true})
	add(&Scenario{Name: "mild-stall-3x", BaseService: 25 * time.Millisecond, InitialCap: 28,
		StallAt:  periodicStall(2*time.Second, 350*time.Millisecond, 200*time.Millisecond),
		SpikeMul: 3, SpikeStart: 3500 * time.Millisecond, SpikeEnd: 8500 * time.Millisecond,
		MildSLOReachable: true})

	return s
}
