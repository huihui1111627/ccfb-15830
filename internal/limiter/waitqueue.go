package limiter

import "time"

// waitEntry 等待队列中的一个请求。
// 两个终态标志互斥，在 Limiter.mu 保护下裁决：
//   - granted=true：名额已正常授予（被 Release 唤醒），Acquire 应成功
//   - canceled=true：等待超时，Acquire 应拒绝
//
// 不能用同一个布尔位表达两种结局，否则被正常唤醒的请求会被误判为超时。
type waitEntry struct {
	prio     Priority
	ch       chan struct{} // 终态确定时关闭，唤醒等待中的 Acquire
	granted  bool
	canceled bool
}

// settled 是否已进入终态。
func (e *waitEntry) settled() bool { return e.granted || e.canceled }

// priorityQueue 有界优先级等待队列：高档排最前，同档 FIFO。
// 仅在 Limiter.mu 保护下操作，本身不加锁。
type priorityQueue struct {
	entries []*waitEntry
}

func (q *priorityQueue) len() int { return len(q.entries) }

// push 按优先级插入：高档（2）在所有中低档之前；同档追加到末尾。
func (q *priorityQueue) push(e *waitEntry) {
	idx := len(q.entries)
	if e.prio == PrioHigh {
		// 找到第一个非高档的位置，插到它前面
		idx = 0
		for idx < len(q.entries) && q.entries[idx].prio == PrioHigh {
			idx++
		}
	}
	q.entries = append(q.entries, nil)
	copy(q.entries[idx+1:], q.entries[idx:])
	q.entries[idx] = e
}

// popReady 移除并返回队首仍在等待（未进入终态）的 entry；全是终态条目时返回 nil。
func (q *priorityQueue) popReady() *waitEntry {
	for len(q.entries) > 0 {
		e := q.entries[0]
		q.entries = q.entries[1:]
		if !e.settled() {
			return e
		}
	}
	return nil
}

// cancel 标记一个 entry 等待超时（惰性清理，不做切片搬移）。
func (q *priorityQueue) cancel(target *waitEntry) {
	target.canceled = true
}

// aliveCount 统计仍在有效等待的条目数。
func (q *priorityQueue) aliveCount() int {
	n := 0
	for _, e := range q.entries {
		if !e.settled() {
			n++
		}
	}
	return n
}

// waitTimer 抽象计时，便于测试注入；生产用 time.AfterFunc。
var waitAfterFunc = time.AfterFunc
