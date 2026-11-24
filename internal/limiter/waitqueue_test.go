package limiter

import (
	"sync"
	"testing"
	"time"
)

// 直接验证优先级队列数据结构：高档排最前，同档 FIFO。
func TestPriorityWaitQueueOrdering(t *testing.T) {
	q := &priorityQueue{}
	q.push(&waitEntry{prio: PrioLow, ch: make(chan struct{})}) // tag 顺序
	q.push(&waitEntry{prio: PrioNormal, ch: make(chan struct{})})
	q.push(&waitEntry{prio: PrioHigh, ch: make(chan struct{})})
	q.push(&waitEntry{prio: PrioHigh, ch: make(chan struct{})})
	q.push(&waitEntry{prio: PrioLow, ch: make(chan struct{})})

	want := []Priority{PrioHigh, PrioHigh, PrioLow, PrioNormal, PrioLow}
	for i, w := range want {
		e := q.popReady()
		if e == nil {
			t.Fatalf("step %d: nil entry", i)
		}
		if e.prio != w {
			t.Fatalf("step %d: got prio %d want %d", i, e.prio, w)
		}
	}
	if q.popReady() != nil {
		t.Fatal("queue should be empty")
	}
}

// 验证已终态条目被 popReady 跳过、aliveCount 正确。
func TestWaitQueueDeadSkipping(t *testing.T) {
	q := &priorityQueue{}
	e1 := &waitEntry{prio: PrioLow, ch: make(chan struct{})}
	e2 := &waitEntry{prio: PrioHigh, ch: make(chan struct{})}
	e3 := &waitEntry{prio: PrioNormal, ch: make(chan struct{})}
	q.push(e1)
	q.push(e2)
	q.push(e3)
	if q.aliveCount() != 3 {
		t.Fatalf("alive=%d want 3", q.aliveCount())
	}
	q.cancel(e2) // 高档条目超时
	if q.aliveCount() != 2 {
		t.Fatalf("alive after cancel=%d want 2", q.aliveCount())
	}
	// popReady 跳过已终态的 e2，返回第一个仍在等待的 e1
	if got := q.popReady(); got != e1 {
		t.Fatalf("popReady returned %v, want e1 (settled e2 must be skipped)", got)
	}
	if got := q.popReady(); got != e3 {
		t.Fatalf("popReady returned %v, want e3", got)
	}
	if q.popReady() != nil {
		t.Fatal("queue should be empty after draining alive entries")
	}
}

// 验证物理可达的过载下，高档请求经等待队列后成功率接近 100%。
// 构造：槽位=10，高档到达率低于吞吐能力，低中档制造持续压力。
func TestHighPriorityProtectedUnderReachableOverload(t *testing.T) {
	cfg := DefaultConfig("test")
	cfg.InitialLimit = 10
	cfg.MinLimit = 10
	cfg.MaxLimit = 10
	cfg.Algo = AlgoFixed
	cfg.WaitTimeout = 300 * time.Millisecond
	cfg.MaxWaiters = 500
	l := New(cfg)

	var wg sync.WaitGroup
	var highOK, highFail int64
	var mu sync.Mutex

	// 持续的低中档压力，占满并维持队列
	stop := make(chan struct{})
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					if ls, err := l.Acquire(PrioLow); err == nil {
						time.Sleep(10 * time.Millisecond)
						ls.Release(OutcomeSuccess)
					} else {
						time.Sleep(time.Millisecond)
					}
				}
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)

	// 高档请求：速率不高，应几乎全部经等待队列成功
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ls, err := l.Acquire(PrioHigh); err == nil {
				time.Sleep(5 * time.Millisecond)
				ls.Release(OutcomeSuccess)
				mu.Lock()
				highOK++
				mu.Unlock()
			} else {
				mu.Lock()
				highFail++
				mu.Unlock()
			}
		}()
		time.Sleep(8 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	rate := float64(highOK) / float64(highOK+highFail)
	if rate < 0.95 {
		t.Fatalf("high success rate=%.2f (ok=%d fail=%d), want >=0.95", rate, highOK, highFail)
	}
}
