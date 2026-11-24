package limiter

import "fmt"

// ErrRejected 表示请求因并发上限（含分档额度）被拒绝。
type ErrRejected struct {
	Name     string
	Prio     Priority
	Limit    int
	InFlight int
}

func (e ErrRejected) Error() string {
	return fmt.Sprintf("limiter %q rejected prio=%d request: inFlight=%d limit=%d",
		e.Name, e.Prio, e.InFlight, e.Limit)
}
