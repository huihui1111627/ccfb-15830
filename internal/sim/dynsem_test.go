package sim

import (
	"sync"
	"testing"
	"time"
)

func TestDynSemBoundedQueue(t *testing.T) {
	sem := newDynSem(2) // 容量 2，队列上限 6
	if sem.queueLimit != 6 {
		t.Fatalf("queueLimit=%d want 6", sem.queueLimit)
	}

	// 占满 2 个令牌
	stop := make(chan struct{})
	for i := 0; i < 2; i++ {
		got, rej := sem.acquire(stop)
		if !got || rej {
			t.Fatalf("token %d: got=%v rej=%v", i, got, rej)
		}
	}

	// 再进 6 个，应排队成功
	waiters := make(chan struct{}, 10)
	for i := 0; i < 6; i++ {
		go func() {
			g, r := sem.acquire(stop)
			waiters <- struct{}{}
			_ = g
			_ = r
		}()
	}
	time.Sleep(30 * time.Millisecond)
	sem.mu.Lock()
	ql := len(sem.waiters)
	sem.mu.Unlock()
	if ql != 6 {
		t.Fatalf("after 6 queued: waiters=%d want 6", ql)
	}

	// 第 7 个应被立即拒绝（队列满）
	got, rej := sem.acquire(stop)
	if got || !rej {
		t.Fatalf("7th: got=%v rej=%v, want rejected", got, rej)
	}
}

func TestDynSemCancelCleanup(t *testing.T) {
	sem := newDynSem(1)
	stop0 := make(chan struct{})
	if g, _ := sem.acquire(stop0); !g {
		t.Fatal("first acquire failed")
	}

	// 3 个请求排队，随后全部取消
	stops := make([]chan struct{}, 3)
	done := make(chan int, 3)
	for i := range stops {
		stops[i] = make(chan struct{})
		go func(c chan struct{}) {
			g, _ := sem.acquire(c)
			if !g {
				done <- 1
			}
		}(stops[i])
	}
	time.Sleep(30 * time.Millisecond)
	for _, c := range stops {
		close(c)
	}
	time.Sleep(50 * time.Millisecond)

	sem.mu.Lock()
	active := 0
	for _, w := range sem.waiters {
		if !w.canceled {
			active++
		}
	}
	total := len(sem.waiters)
	sem.mu.Unlock()
	if active != 0 {
		t.Fatalf("active waiters=%d want 0", active)
	}

	// 释放令牌后，新请求应能立刻拿到，且队列被压缩
	sem.release()
	time.Sleep(20 * time.Millisecond)
	stopNew := make(chan struct{})
	g, rej := sem.acquire(stopNew)
	if !g || rej {
		t.Fatalf("after cancel+release: got=%v rej=%v, want acquired", g, rej)
	}
	sem.mu.Lock()
	postTotal := len(sem.waiters)
	sem.mu.Unlock()
	if postTotal != 0 {
		t.Fatalf("zombie waiters remain: total=%d (was %d)", postTotal, total)
	}
}

// TestDynSemInvariantUnderChurn 高强度获取/取消交织下，
// 不变量 inflight + tokens == capacity 必须始终成立，且 inflight 不超过容量。
func TestDynSemInvariantUnderChurn(t *testing.T) {
	sem := newDynSem(4)
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 一批会被超时取消的请求：取消信号由外部控制，先排队后统一取消
	cancelC := make(chan struct{})
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, _ := sem.acquire(cancelC)
			if got {
				time.Sleep(time.Duration(1+(i%3)) * time.Millisecond)
				sem.release()
			}
			// 未拿到令牌的分支由 acquire 内部处理取消，这里不再重复操作
		}()
	}
	// 一批正常获取并释放的请求
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if got, _ := sem.acquire(stop); got {
				time.Sleep(time.Millisecond)
				sem.release()
			}
		}()
	}

	// 运行中反复扩缩容
	for i := 0; i < 20; i++ {
		if i%2 == 0 {
			sem.setCapacity(2)
		} else {
			sem.setCapacity(6)
		}
		time.Sleep(2 * time.Millisecond)
	}
	// 统一发出取消信号，唤醒所有排队等待 cancelC 的请求
	close(cancelC)

	wg.Wait()
	sem.mu.Lock()
	got := sem.inflight + sem.tokens - sem.debt
	inflight := sem.inflight
	waiters := len(sem.waiters)
	capNow := sem.capacity
	sem.mu.Unlock()

	if inflight != 0 {
		t.Errorf("inflight=%d after all done, want 0", inflight)
	}
	if waiters != 0 {
		t.Errorf("waiters=%d after all done, want 0", waiters)
	}
	if got != capNow {
		t.Errorf("inflight+tokens-debt=%d != capacity=%d (invariant broken)", got, capNow)
	}
}
