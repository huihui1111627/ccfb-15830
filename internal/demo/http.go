package demo

import (
	"encoding/json"
	"net/http"

	"temple/internal/limiter"
)

// EndpointState 单个接口的完整状态 DTO。
type EndpointState struct {
	Key          string  `json:"key"`
	Name         string  `json:"name"`
	Algo         string  `json:"algo"`
	Limit        int     `json:"limit"`
	BaseLimit    int     `json:"baseLimit"`
	InFlight     int     `json:"inFlight"`
	Waiters      int     `json:"waiters"`
	BaselineMs   float64 `json:"baselineMs"`
	CurrentRTTMs float64 `json:"currentRttMs"`
	QueueRatio   float64 `json:"queueRatio"`
	TrueCapacity int     `json:"trueCapacity"`
	Probing      bool    `json:"probing"`
	StrongEvents int64   `json:"strongEvents"`
	HighReserve  int     `json:"highReserve"`
	WaitP50Ms    float64 `json:"waitP50Ms"`
	WaitP99Ms    float64 `json:"waitP99Ms"`

	Arrived   [3]int64 `json:"arrived"`
	Rejected  [3]int64 `json:"rejected"`
	Succeeded [3]int64 `json:"succeeded"`
	RejWaitTO [3]int64 `json:"rejWaitTimeout"`
	RejQFull  [3]int64 `json:"rejQueueFull"`

	History []HistoryPoint `json:"history"`
}

// FullState 整个页面一次拉取的状态。
type FullState struct {
	Festival  bool            `json:"festival"`
	Algo      string          `json:"algo"`
	Endpoints []EndpointState `json:"endpoints"`
}

func (s *Service) snapshot() FullState {
	s.mu.RLock()
	eps := s.endpoints
	s.mu.RUnlock()

	states := make([]EndpointState, 0, len(eps))
	for _, ep := range eps {
		cs := ep.clientL.Snapshot()
		arrived, rejected, succeeded := ep.Counters()
		rel := timeSince(ep.startAt)
		states = append(states, EndpointState{
			Key:          ep.Spec.Key,
			Name:         ep.Spec.Name,
			Algo:         string(cs.Algo),
			Limit:        cs.Limit,
			BaseLimit:    cs.BaseLimit,
			InFlight:     cs.InFlight,
			Waiters:      cs.Waiters,
			BaselineMs:   float64(cs.Baseline.Microseconds()) / 1000,
			CurrentRTTMs: float64(cs.CurrentRTT.Microseconds()) / 1000,
			QueueRatio:   cs.QueueRatio,
			TrueCapacity: ep.downstream.CapacityAt(rel),
			Probing:      cs.Probing,
			StrongEvents: cs.StrongEvents,
			HighReserve:  cs.HighReserve,
			WaitP50Ms:    float64(cs.WaitTimeP50.Microseconds()) / 1000,
			WaitP99Ms:    float64(cs.WaitTimeP99.Microseconds()) / 1000,
			Arrived:      arrived,
			Rejected:     rejected,
			Succeeded:    succeeded,
			RejWaitTO:    cs.RejectWaitTimeout,
			RejQFull:     cs.RejectQueueFull,
			History:      ep.History(),
		})
	}
	return FullState{Festival: s.Festival(), Algo: string(s.Algo()), Endpoints: states}
}

// Handler 返回注册好路由的 http.Handler。
func (s *Service) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("/api/state", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, s.snapshot())
	})

	mux.HandleFunc("/api/festival", func(w http.ResponseWriter, r *http.Request) {
		on := r.URL.Query().Get("on") == "1" || r.URL.Query().Get("on") == "true"
		s.SetFestival(on)
		writeJSON(w, map[string]any{"festival": s.Festival()})
	})

	mux.HandleFunc("/api/algo", func(w http.ResponseWriter, r *http.Request) {
		a := r.URL.Query().Get("algo")
		switch limiter.Algorithm(a) {
		case limiter.AlgoVegas, limiter.AlgoGradient:
			s.SwitchAlgo(limiter.Algorithm(a))
		default:
			http.Error(w, "unknown algo", http.StatusBadRequest)
			return
		}
		writeJSON(w, map[string]any{"algo": s.Algo()})
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(dashboardHTML))
	})

	return mux
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	enc := json.NewEncoder(w)
	_ = enc.Encode(v)
}
