package sim

import (
	"sync"
	"testing"
)

// 最小复现：N 个令牌、N+M 个请求，M 个在排队时被取消，
// 其余全部正常 release，最终 inflight 必须归零、tokens 回到 capacity。
func TestDynSemMinimalCancel(t *testing.T) {
	for trial := 0; trial < 200; trial++ {
		sem := newDynSem(3)
		var wg sync.WaitGroup
		cancelC := make(chan struct{})

		// 占满 3 个并持有
		holders := make(chan struct{}, 3)
		for i := 0; i < 3; i++ {
			g, _ := sem.acquire(cancelC)
			if !g {
				t.Fatal("holder acquire failed")
			}
		}
		// 5 个排队，随后取消
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				g, _ := sem.acquire(cancelC)
				if g {
					sem.release()
				}
			}()
		}
		// 确保 5 个都已入队
		waitUntil := func(cond func() bool) {
			for k := 0; k < 1000 && !cond(); k++ {
			}
		}
		waitUntil(func() bool {
			sem.mu.Lock()
			n := len(sem.waiters)
			sem.mu.Unlock()
			return n == 5
		})
		close(cancelC)
		wg.Wait()

		// 持有者释放
		for i := 0; i < 3; i++ {
			sem.release()
		}
		_ = holders

		sem.mu.Lock()
		inflight, tokens, cap, debt := sem.inflight, sem.tokens, sem.capacity, sem.debt
		waiters := len(sem.waiters)
		sem.mu.Unlock()

		if inflight != 0 || tokens != cap || waiters != 0 || debt != 0 {
			t.Fatalf("trial %d: inflight=%d tokens=%d cap=%d waiters=%d debt=%d",
				trial, inflight, tokens, cap, waiters, debt)
		}
	}
}
