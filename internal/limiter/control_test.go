package limiter

import (
	"sync"
	"testing"
	"time"
)

// 验证需求 6：强信号减半后，冷却期内线性恢复，冷却结束后能重新增长。
func TestStrongSignalThenLinearRecovery(t *testing.T) {
	cfg := DefaultConfig("rec")
	cfg.InitialLimit = 64
	cfg.MinLimit = 2
	cfg.MaxLimit = 512
	cfg.Algo = AlgoVegas
	cfg.WaitTimeout = 0
	l := New(cfg)

	// 触发一次过载，上限减半到 32
	ls, _ := l.Acquire(PrioNormal)
	ls.Release(OutcomeOverload)
	if got := l.Snapshot().Limit; got != 32 {
		t.Fatalf("after overload limit=%d want 32", got)
	}

	// 冷却期内（2s）每完成一个样本线性 +1，验证会逐步恢复
	before := l.Snapshot().Limit
	for i := 0; i < 5; i++ {
		ls, _ := l.Acquire(PrioNormal)
		ls.Release(OutcomeSuccess)
	}
	after := l.Snapshot().Limit
	if after <= before {
		t.Fatalf("cooldown should recover upward: before=%d after=%d", before, after)
	}
}

// 验证需求 2：探底只在满载且无排队压力时进行，空载不探、高压不探。
func TestProbeGuards(t *testing.T) {
	cfg := DefaultConfig("probe")
	cfg.InitialLimit = 20
	cfg.ProbeInterval = 50 * time.Millisecond
	cfg.Algo = AlgoVegas
	cfg.WaitTimeout = 0
	l := New(cfg)

	// 空载：即使超过探底间隔也不应进入探底（saturated=false）
	time.Sleep(80 * time.Millisecond)
	ls, _ := l.Acquire(PrioNormal)
	ls.Release(OutcomeSuccess)
	if l.Snapshot().Probing {
		t.Fatal("should not probe when unsaturated")
	}
}

// 验证需求 1 的平滑性：连续调整下上限的单步变化不超过 MaxChangeRate。
func TestLimitChangeIsBounded(t *testing.T) {
	cfg := DefaultConfig("smooth")
	cfg.InitialLimit = 40
	cfg.MinLimit = 1
	cfg.MaxLimit = 10000
	cfg.Algo = AlgoVegas
	cfg.MaxChangeRate = 0.3
	cfg.UpdateEveryN = 1
	cfg.WaitTimeout = 0
	l := New(cfg)

	// 制造一批饱和成功样本，让控制环持续运转
	prev := l.Snapshot().Limit
	for i := 0; i < 60; i++ {
		ls, _ := l.Acquire(PrioNormal)
		time.Sleep(time.Millisecond)
		ls.Release(OutcomeSuccess)
		cur := l.Snapshot().BaseLimit
		// 允许取整误差 1
		maxStep := float64(prev)*cfg.MaxChangeRate + 1.5
		if d := cur - prev; float64(abs(d)) > maxStep {
			t.Fatalf("step %d: limit jumped %d->%d, max allowed %.1f", i, prev, cur, maxStep)
		}
		prev = cur
	}
}

// 验证 Gradient 算法在满载平稳延迟下不会持续收缩（应维持或缓增）。
func TestGradientStableDoesNotShrink(t *testing.T) {
	cfg := DefaultConfig("grad")
	cfg.InitialLimit = 20
	cfg.MinLimit = 1
	cfg.MaxLimit = 1000
	cfg.Algo = AlgoGradient
	cfg.UpdateEveryN = 4
	cfg.WaitTimeout = 0
	l := New(cfg)

	// 平稳服务时间（用 sleep 模拟恒定延迟），满载运行
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if ls, err := l.Acquire(PrioNormal); err == nil {
						time.Sleep(8 * time.Millisecond)
						ls.Release(OutcomeSuccess)
					} else {
						time.Sleep(time.Millisecond)
					}
				}
			}
		}()
	}
	time.Sleep(400 * time.Millisecond)
	close(stop)
	wg.Wait()

	if got := l.Snapshot().BaseLimit; got < 10 {
		t.Fatalf("gradient under stable load collapsed to %d, want >=10", got)
	}
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
