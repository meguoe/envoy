package xdsserver

import (
	"context"
	"log/slog"
	"time"
)

// RuleChangeNotifier 定义规则变更通知接口。
type RuleChangeNotifier interface {
	NotifyRevision(revision int64)
}

// PushStore 定义推送 worker 所需的存储接口。
type PushStore interface {
	LoadRevision(ctx context.Context) (int64, error)
	Load(ctx context.Context) ([]*ProxyRule, error)
	LogPushPending(ctx context.Context, revision int64) error
	PushStatus(ctx context.Context, revision int64) (string, error)
	MarkPushFailed(ctx context.Context, revision int64, errMsg string) error
}

// RulePushWorker 事件驱动的规则推送 worker，支持通知触发和定时兜底。
type RulePushWorker struct {
	notifyCh chan int64
	store    PushStore
	engine   *Engine
	ticker   *time.Ticker
	ctx      context.Context
	cancel   context.CancelFunc
	done     chan struct{}
}

// NewRulePushWorker 创建推送 worker，tickerInterval 为定时兜底推送间隔。
func NewRulePushWorker(store PushStore, engine *Engine, tickerInterval time.Duration) *RulePushWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &RulePushWorker{
		notifyCh: make(chan int64, 1),
		store:    store,
		engine:   engine,
		ticker:   time.NewTicker(tickerInterval),
		ctx:      ctx,
		cancel:   cancel,
		done:     make(chan struct{}),
	}
}

// NotifyRevision 非阻塞通知 worker 有新 revision 需要推送。
// channel buffer=1，并发事件自动合并为最新 revision，中间 revision 不保证推送。
func (w *RulePushWorker) NotifyRevision(revision int64) {
	select {
	case <-w.notifyCh:
	default:
	}
	select {
	case w.notifyCh <- revision:
	default:
	}
}

// Start 启动推送 worker 的后台运行协程。
func (w *RulePushWorker) Start() {
	go w.run()
}

// Stop 停止推送 worker 并等待协程退出。
func (w *RulePushWorker) Stop() {
	w.cancel()
	w.ticker.Stop()
	<-w.done
}

// run 是 worker 的主循环，监听通知通道和定时器触发推送。
func (w *RulePushWorker) run() {
	defer close(w.done)
	for {
		select {
		case <-w.ctx.Done():
			return
		case <-w.ticker.C:
			w.doPush()
		case <-w.notifyCh:
			w.doPush()
		}
	}
}

// doPush 执行一次规则推送：从数据库加载最新 revision 和规则，构建快照并推送。
func (w *RulePushWorker) doPush() {
	loadCtx, cancelLoad := context.WithTimeout(w.ctx, 5*time.Second)
	dbRev, err := w.store.LoadRevision(loadCtx)
	cancelLoad()
	if err != nil {
		slog.Error("RulePushWorker 加载 revision 失败", "error", err)
		return
	}

	const maxIterations = 100
	for i := 0; i < maxIterations; i++ {
		engineRev := w.engine.KnownRevision()
		if dbRev == engineRev {
			statusCtx, cancelStatus := context.WithTimeout(w.ctx, 5*time.Second)
			status, err := w.store.PushStatus(statusCtx, dbRev)
			cancelStatus()
			if err != nil {
				slog.Error("RulePushWorker 查询 push 状态失败", "error", err)
				return
			}
			if status == "deployed" || status == "" {
				return
			}
			if status != "failed" {
				if status == "pending" {
					slog.Info("RulePushWorker 跳过推送", "revision", dbRev, "status", status)
				}
				return
			}
			slog.Info("RulePushWorker 重试已失败的 revision", "revision", dbRev)
		}

		rulesCtx, cancelRules := context.WithTimeout(w.ctx, 5*time.Second)
		rules, err := w.store.Load(rulesCtx)
		cancelRules()
		if err != nil {
			slog.Error("RulePushWorker 加载规则失败", "error", err)
			return
		}
		pendingCtx, cancelPending := context.WithTimeout(w.ctx, 5*time.Second)
		if err := w.store.LogPushPending(pendingCtx, dbRev); err != nil {
			cancelPending()
			slog.Error("RulePushWorker 记录 push pending 失败", "error", err)
			return
		}
		cancelPending()
		if err := w.engine.ReplaceRulesAndPushWithVersion(rules, dbRev); err != nil {
			slog.Error("RulePushWorker 推送失败", "revision", dbRev, "error", err)
			_ = w.store.MarkPushFailed(w.ctx, dbRev, err.Error())
			return
		}
		slog.Info("RulePushWorker 推送成功", "rules", len(rules), "revision", dbRev)

		reCheckCtx, cancelReCheck := context.WithTimeout(w.ctx, 5*time.Second)
		newRev, err := w.store.LoadRevision(reCheckCtx)
		cancelReCheck()
		if err != nil {
			slog.Error("RulePushWorker re-check 加载 revision 失败", "error", err)
			return
		}
		if newRev <= dbRev {
			return
		}
		dbRev = newRev
	}
}
