package demo

import (
	"math"
	"sync/atomic"
	"time"

	"temple/internal/limiter"
	"temple/internal/sim"
)

// loadLoop 非齐次泊松流量发生器：节庆模式开启时到达率乘 SpikeMul。
func (e *Endpoint) loadLoop(svc *Service) {
	defer e.wg.Done()
	slot := 100 * time.Millisecond
	t := time.NewTicker(slot)
	defer t.Stop()
	for {
		select {
		case <-e.stopC:
			return
		case <-t.C:
			rate := e.Spec.BaseRate
			if svc.Festival() {
				rate *= e.Spec.SpikeMul
			}
			n := e.poisson(rate * slot.Seconds())
			for i := 0; i < n; i++ {
				go e.handle(limiter.Priority(e.drawPrio()))
			}
		}
	}
}

// handle 单请求生命周期，与 bench 的双层限流 + 下游调用一致。
func (e *Endpoint) handle(prio limiter.Priority) {
	p := int(prio)
	atomic.AddInt64(&e.arrived[p], 1)

	entryLease, err := e.entryL.Acquire(prio)
	if err != nil {
		atomic.AddInt64(&e.rejected[p], 1)
		return
	}
	clientLease, err := e.clientL.Acquire(prio)
	if err != nil {
		atomic.AddInt64(&e.rejected[p], 1)
		entryLease.Release(limiter.OutcomeError)
		return
	}

	const callTimeout = 2500 * time.Millisecond
	timer := time.NewTimer(callTimeout)
	defer timer.Stop()
	stopC := make(chan struct{})
	done := make(chan struct {
		rtt time.Duration
		res sim.CallResult
	}, 1)
	go func() {
		rtt, res := e.downstream.Call(stopC)
		done <- struct {
			rtt time.Duration
			res sim.CallResult
		}{rtt, res}
	}()

	var res sim.CallResult
	select {
	case r := <-done:
		close(stopC)
		res = r.res
	case <-timer.C:
		close(stopC)
		r := <-done
		res = sim.ResultTimeout
		_ = r
	}

	outcome := limiter.OutcomeSuccess
	switch res {
	case sim.ResultTimeout:
		outcome = limiter.OutcomeTimeout
	case sim.ResultOverload:
		outcome = limiter.OutcomeOverload
	default:
		atomic.AddInt64(&e.succeeded[p], 1)
	}
	clientLease.Release(outcome)
	entryLease.Release(outcome)
}

// sampleLoop 每 200ms 记录一个历史点，并把各档计数快照进去。
func (e *Endpoint) sampleLoop() {
	defer e.wg.Done()
	t := time.NewTicker(200 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-e.stopC:
			return
		case now := <-t.C:
			cs := e.clientL.Snapshot()
			rel := now.Sub(e.startAt)
			point := HistoryPoint{
				T:            rel.Seconds(),
				Limit:        cs.Limit,
				InFlight:     cs.InFlight,
				Waiters:      cs.Waiters,
				BaselineMs:   float64(cs.Baseline.Microseconds()) / 1000,
				CurrentRTTMs: float64(cs.CurrentRTT.Microseconds()) / 1000,
				QueueRatio:   cs.QueueRatio,
				Rejected:     sumInt64(e.rejected[:]),
				Arrived:      sumInt64(e.arrived[:]),
				Success:      sumInt64(e.succeeded[:]),
				TrueCapacity: e.downstream.CapacityAt(rel),
			}
			e.historyMu.Lock()
			e.history = append(e.history, point)
			if len(e.history) > cap(e.history) {
				e.history = e.history[len(e.history)-cap(e.history):]
			}
			e.historyMu.Unlock()
		}
	}
}

// capacityDriftLoop 演示容量突降：节庆期间周期性地把容量砍到 40%，模拟数据库变慢。
func (e *Endpoint) capacityDriftLoop(svc *Service) {
	defer e.wg.Done()
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	normal := e.Spec.Capacity
	for {
		select {
		case <-e.stopC:
			return
		case <-t.C:
			rel := time.Since(e.startAt)
			// 节庆开启 3s 后进入一段容量降级，再恢复，循环演示自适应
			target := normal
			if svc.Festival() {
				phase := int(rel.Seconds()) % 16
				if phase >= 4 && phase < 10 {
					target = normal * 4 / 10
				}
			}
			e.downstream.SetExternalCapacity(target)
		}
	}
}

// History 返回历史曲线副本。
func (e *Endpoint) History() []HistoryPoint {
	e.historyMu.Lock()
	defer e.historyMu.Unlock()
	out := make([]HistoryPoint, len(e.history))
	copy(out, e.history)
	return out
}

// Counters 返回各档累计计数。
func (e *Endpoint) Counters() (arrived, rejected, succeeded [3]int64) {
	for i := 0; i < 3; i++ {
		arrived[i] = atomic.LoadInt64(&e.arrived[i])
		rejected[i] = atomic.LoadInt64(&e.rejected[i])
		succeeded[i] = atomic.LoadInt64(&e.succeeded[i])
	}
	return
}

func (e *Endpoint) poisson(mean float64) int {
	e.rngMu.Lock()
	defer e.rngMu.Unlock()
	if mean < 30 {
		l := math.Exp(-mean)
		k, p := 0, 1.0
		for {
			k++
			p *= e.rng.Float64()
			if p <= l {
				return k - 1
			}
		}
	}
	v := mean + e.rng.NormFloat64()*math.Sqrt(mean)
	if v < 0 {
		v = 0
	}
	return int(v + 0.5)
}

func (e *Endpoint) drawPrio() int {
	e.rngMu.Lock()
	defer e.rngMu.Unlock()
	roll := e.rng.Float64()
	acc := 0.0
	for p, w := range e.Spec.PrioWeights {
		acc += w
		if roll < acc {
			return p
		}
	}
	return 0
}

func sumInt64(a []int64) int64 {
	var s int64
	for _, v := range a {
		s += v
	}
	return s
}
