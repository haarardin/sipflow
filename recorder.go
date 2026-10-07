package sipflow

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrRecorderClosed = errors.New("sipflow: recorder closed")
	ErrQueueFull      = errors.New("sipflow: recorder queue full")
)

type Sink interface {
	WriteEvent(context.Context, Event) error
	CompleteFlow(context.Context, Flow) error
}

type NopSink struct{}

func (NopSink) WriteEvent(context.Context, Event) error   { return nil }
func (NopSink) CompleteFlow(context.Context, Flow) error  { return nil }

type OverflowPolicy int

const (
	DropNewest OverflowPolicy = iota
	DropOldest
	Block
)

type Option func(*Recorder)

func WithQueueSize(n int) Option {
	return func(r *Recorder) {
		if n > 0 {
			r.queueSize = n
		}
	}
}
func WithOverflowPolicy(p OverflowPolicy) Option { return func(r *Recorder) { r.overflow = p } }
func WithSink(s Sink) Option {
	return func(r *Recorder) {
		if s != nil {
			r.sink = s
		}
	}
}
func WithCaptureRaw(v bool) Option { return func(r *Recorder) { r.captureRaw = v } }
func WithErrorHandler(h func(error)) Option {
	return func(r *Recorder) {
		if h != nil {
			r.onError = h
		}
	}
}
func WithTerminalRetention(d time.Duration) Option {
	return func(r *Recorder) { r.trackerOptions.TerminalRetention = d }
}
func WithInactiveRetention(d time.Duration) Option {
	return func(r *Recorder) { r.trackerOptions.InactiveRetention = d }
}
func WithCleanupInterval(d time.Duration) Option {
	return func(r *Recorder) {
		if d > 0 {
			r.cleanupInterval = d
		}
	}
}

type observation struct {
	ctx context.Context
	d   Direction
	raw []byte
	m   Meta
}

type Recorder struct {
	tracker        *Tracker
	trackerOptions TrackerOptions
	sink           Sink
	queueSize      int
	overflow       OverflowPolicy
	captureRaw      bool
	onError         func(error)
	cleanupInterval time.Duration
	queue           chan observation
	stop, done      chan struct{}
	closed          atomic.Bool
	mu              sync.RWMutex
}

func New(opts ...Option) *Recorder {
	r := &Recorder{
		sink:            NopSink{},
		queueSize:       1024,
		onError:         func(error) {},
		cleanupInterval: time.Minute,
		stop:            make(chan struct{}),
		done:            make(chan struct{}),
	}
	for _, o := range opts {
		o(r)
	}
	r.tracker = NewTrackerWithOptions(r.trackerOptions)
	r.queue = make(chan observation, r.queueSize)
	go r.run()
	return r
}

func (r *Recorder) Observe(ctx context.Context, d Direction, raw []byte, m Meta) error {
	if ctx == nil {
		ctx = context.Background()
	}
	o := observation{ctx: ctx, d: d, raw: append([]byte(nil), raw...), m: m}
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed.Load() {
		return ErrRecorderClosed
	}
	switch r.overflow {
	case Block:
		select {
		case r.queue <- o:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-r.stop:
			return ErrRecorderClosed
		}
	case DropOldest:
		select {
		case r.queue <- o:
			return nil
		default:
		}
		select {
		case <-r.queue:
		default:
		}
		select {
		case r.queue <- o:
			return nil
		default:
			return ErrQueueFull
		}
	default:
		select {
		case r.queue <- o:
			return nil
		default:
			return ErrQueueFull
		}
	}
}

func (r *Recorder) Flow(id string) (Flow, bool) { return r.tracker.Flow(id) }
func (r *Recorder) Flows() []Flow               { return r.tracker.Flows() }
func (r *Recorder) Cleanup(now time.Time) int    { return r.tracker.Cleanup(now) }
func (r *Recorder) Stats() TrackerStats          { return r.tracker.Stats() }

func (r *Recorder) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	r.mu.Lock()
	if !r.closed.Swap(true) {
		close(r.stop)
	}
	r.mu.Unlock()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Recorder) run() {
	defer close(r.done)
	var ticker *time.Ticker
	var cleanup <-chan time.Time
	if (r.trackerOptions.TerminalRetention > 0 || r.trackerOptions.InactiveRetention > 0) && r.cleanupInterval > 0 {
		ticker = time.NewTicker(r.cleanupInterval)
		cleanup = ticker.C
		defer ticker.Stop()
	}
	for {
		select {
		case o := <-r.queue:
			r.process(o)
		case now := <-cleanup:
			r.tracker.Cleanup(now)
		case <-r.stop:
			for {
				select {
				case o := <-r.queue:
					r.process(o)
				default:
					return
				}
			}
		}
	}
}

func (r *Recorder) process(o observation) {
	e, err := Parse(o.raw, o.d, o.m, ParseOptions{CaptureRaw: r.captureRaw})
	if err != nil {
		r.onError(err)
		return
	}
	f, done := r.tracker.Apply(e)
	e = f.Events[len(f.Events)-1]
	if err = r.sink.WriteEvent(o.ctx, e); err != nil {
		r.onError(err)
	}
	if done {
		if err = r.sink.CompleteFlow(o.ctx, f); err != nil {
			r.onError(err)
		}
	}
}
