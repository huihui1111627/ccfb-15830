package limiter

import "time"

// Priority 请求重要程度分档，数字越大越重要。
type Priority int

const (
	PrioLow    Priority = 0 // 低：普通祈福浏览等，过载时最先被拒
	PrioNormal Priority = 1 // 中：点灯、斋饭订餐等普通业务
	PrioHigh   Priority = 2 // 高：法会预约、支付等关键业务
)

// Outcome 请求结束后的结果分类，用于反馈给限流器。
type Outcome int

const (
	OutcomeSuccess  Outcome = 0 // 成功
	OutcomeTimeout  Outcome = 1 // 在途超时被取消（仍计入延迟样本）
	OutcomeOverload Outcome = 2 // 下游明确返回过载（强信号）
	OutcomeError    Outcome = 3 // 普通业务错误（不当作过载强信号）
)

// Algorithm 自适应算法类型，两种可热切换。
type Algorithm string

const (
	AlgoVegas    Algorithm = "vegas"    // 最小延迟基线 + 当前/基线排队比
	AlgoGradient Algorithm = "gradient" // 按延迟随在途数变化的梯度伸缩
	AlgoFixed    Algorithm = "fixed"    // 固定上限，对照组
)

// Config 单接口限流器配置。
type Config struct {
	Name          string        // 接口名
	InitialLimit  int           // 初始并发上限
	MinLimit      int           // 上限硬下界
	MaxLimit      int           // 上限硬上界
	HighHeadroom  float64       // 高档预留下限比例（动态预留不低于此值）
	HighHeadMax   float64       // 高档预留上限比例，防止高档流量异常时挤占全部额度
	LowHeadroom   float64       // 低档最多使用的额度比例，如 0.75
	ProbeInterval time.Duration // 定期压低并发重测基线的周期
	ProbeFactor   float64       // 探底时上限临时乘的系数，如 0.5
	MinSamples    int           // 样本不足多少条时只线性扩张、不收缩
	UpdateEveryN  int           // 每收到 N 个完成样本重新算一次上限
	SmoothAlpha   float64       // 上限平滑系数（指数平滑），0~1，越小越平滑
	MaxChangeRate float64       // 单次调整最大变化比例，如 0.25
	RTTWindow     time.Duration // 当前延迟统计窗口
	BaselineTTL   time.Duration // 基线样本最大年龄（窗口）
	Algo          Algorithm     // 初始算法
	TotalOnly     bool          // 为 true 时只做总量闸门、不做分档预留（入口层用）
	WaitTimeout   time.Duration // 拿不到名额时的最长排队等待；0 表示立即拒绝
	MaxWaiters    int           // 等待队列容量，超出立即拒绝
}

// DefaultConfig 返回一套经过标定的默认参数。
func DefaultConfig(name string) Config {
	return Config{
		Name:          name,
		InitialLimit:  16,
		MinLimit:      2,
		MaxLimit:      2048,
		HighHeadroom:  0.25,
		HighHeadMax:   0.90,
		LowHeadroom:   0.70,
		ProbeInterval: 10 * time.Second,
		ProbeFactor:   0.7,
		MinSamples:    8,
		UpdateEveryN:  6,
		SmoothAlpha:   0.5,
		MaxChangeRate: 0.30,
		RTTWindow:     1500 * time.Millisecond,
		BaselineTTL:   30 * time.Second,
		Algo:          AlgoVegas,
		WaitTimeout:   200 * time.Millisecond,
		MaxWaiters:    4096,
	}
}

// Snapshot 限流器当前状态快照，供监控页面/报表读取。
type Snapshot struct {
	Name              string
	Algo              Algorithm
	Limit             int           // 当前对外生效的并发上限（探底期间为临时值）
	BaseLimit         int           // 平滑后的基础上限（非探底时与 Limit 相同）
	InFlight          int           // 实际在途请求数
	InFlightPrio      [3]int        // 各档在途数
	Baseline          time.Duration // 最小往返延迟基线（无排队）
	CurrentRTT        time.Duration // 当前窗口延迟（p50）
	QueueRatio        float64       // 当前延迟 / 基线
	Rejected          [3]int64      // 各档累计被拒请求数
	Total             [3]int64      // 各档累计到达请求数
	Completed         int64         // 已完成样本数
	StrongEvents      int64         // 强信号（过载/超时）触发次数
	Probing           bool          // 是否处于探底重测基线状态
	HighReserve       int           // 当前为高档预留的名额
	HighRate          float64       // 高档到达率 EWMA（请求/秒）
	Waiters           int           // 等待队列中有效请求数
	RejectQueueFull   [3]int64      // 因等待队列满/未配置等待被拒
	RejectWaitTimeout [3]int64      // 因等待超时被拒
	WaitTimeP50       time.Duration // 本地等待队列排队时长 p50
	WaitTimeP99       time.Duration // 本地等待队列排队时长 p99
}
