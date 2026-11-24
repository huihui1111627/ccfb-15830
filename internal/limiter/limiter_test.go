package limiter

import (
	"testing"
	"time"
)

// 验证需求 4：高负载下高档请求优先获得名额，低档先被拒。
func TestPriorityAdmission(t *testing.T) {
	cfg := DefaultConfig("test")
	cfg.InitialLimit = 10
	cfg.MinLimit = 10
	cfg.MaxLimit = 10
	cfg.Algo = AlgoFixed
	cfg.HighHeadroom = 0.3 // 至少预留 3 个给高档
	cfg.WaitTimeout = 0    // 本测试验证立即拒绝语义，不排队
	l := New(cfg)

	// 先用低档把非高档名额占满（10 - 预留3 = 7 个）
	lowLeases := []*Lease{}
	for i := 0; i < 20; i++ {
		ls, err := l.Acquire(PrioLow)
		if err == nil {
			lowLeases = append(lowLeases, ls)
		}
	}
	if len(lowLeases) != 7 {
		t.Fatalf("low admitted=%d want 7 (10 limit - 3 reserved)", len(lowLeases))
	}

	// 此时总在途=7 < 10，高档应仍能拿到预留名额，最多再进 3 个
	highLeases := []*Lease{}
	for i := 0; i < 10; i++ {
		ls, err := l.Acquire(PrioHigh)
		if err == nil {
			highLeases = append(highLeases, ls)
		}
	}
	if len(highLeases) != 3 {
		t.Fatalf("high admitted=%d want 3 (reserved slots)", len(highLeases))
	}

	// 全部 10 个名额占满后，任何新请求都被拒
	if _, err := l.Acquire(PrioHigh); err == nil {
		t.Fatal("high should be rejected when fully saturated")
	}
	if _, err := l.Acquire(PrioNormal); err == nil {
		t.Fatal("normal should be rejected when fully saturated")
	}

	// 释放一个高档，名额应能立刻给新的高档
	highLeases[0].Release(OutcomeSuccess)
	if ls, err := l.Acquire(PrioHigh); err != nil {
		t.Fatalf("high should take freed reserved slot: %v", err)
	} else {
		ls.Release(OutcomeSuccess)
	}
}

// 验证需求 6：过载/超时强信号触发上限立即减半，冷却期线性恢复。
func TestStrongSignalHalvesLimit(t *testing.T) {
	cfg := DefaultConfig("test")
	cfg.InitialLimit = 100
	cfg.MinLimit = 4
	cfg.MaxLimit = 400
	cfg.Algo = AlgoVegas
	l := New(cfg)

	ls, err := l.Acquire(PrioNormal)
	if err != nil {
		t.Fatal(err)
	}
	ls.Release(OutcomeOverload)

	if got := l.Snapshot().Limit; got != 50 {
		t.Fatalf("after overload limit=%d want 50", got)
	}

	// 再一次超时，应再减半到 25
	ls2, _ := l.Acquire(PrioNormal)
	ls2.Release(OutcomeTimeout)
	if got := l.Snapshot().Limit; got != 25 {
		t.Fatalf("after timeout limit=%d want 25", got)
	}
}

// 验证需求 8：超时取消的请求也产生 RTT 样本。
func TestTimeoutRecordedAsSample(t *testing.T) {
	cfg := DefaultConfig("test")
	cfg.InitialLimit = 10
	cfg.Algo = AlgoFixed
	cfg.WaitTimeout = 0
	l := New(cfg)

	before := l.Snapshot().Completed
	ls, _ := l.Acquire(PrioNormal)
	time.Sleep(5 * time.Millisecond)
	ls.Release(OutcomeTimeout)
	after := l.Snapshot()
	if after.Completed != before+1 {
		t.Fatalf("completed=%d want %d", after.Completed, before+1)
	}
	if after.CurrentRTT <= 0 {
		t.Fatal("timeout request did not produce an RTT sample")
	}
}
