package fusee

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"
)

// RetryPolicy says how a failing step is retried. Attempts includes the
// first; the delay doubles from Initial up to Max ("exponential", the
// default) or stays Initial ("fixed").
type RetryPolicy struct {
	Attempts int
	Backoff  string
	Initial  time.Duration
	Max      time.Duration
	// Failures of these types (Failure.Type, or the error's type name), or
	// with a cause of one, are not retried.
	NonRetryable []string
	// Retried only while this returns true (attempt: the one that failed).
	RetryIf func(f *Failure, attempt uint32) bool
	// What the step does when its attempts run out on a failure that could
	// be retried: Pause pauses the run (its parent and callers keep
	// waiting) until Run.Resume runs the step again with a fresh set of
	// attempts. Zero takes the engine's Options.OnExhausted, else Fail.
	OnExhausted Exhausted
}

// Exhausted is what a step does when its retries run out.
type Exhausted int

const (
	// ExhaustedDefault takes the engine's Options.OnExhausted.
	ExhaustedDefault Exhausted = iota
	// Fail fails the step.
	Fail
	// Pause pauses the run until Run.Resume.
	Pause
)

// paused is the retry time that means "until resumed".
const paused = ^uint64(0)

// retries reports whether a failed attempt is retried: not a final
// failure, none of NonRetryable, and RetryIf agrees.
func (r RetryPolicy) retries(f *Failure, attempt uint32) bool {
	if f.Final() {
		return false
	}
	for c := f; c != nil; c = c.Cause {
		for _, t := range r.NonRetryable {
			if c.Type == t {
				return false
			}
		}
	}
	return r.RetryIf == nil || r.RetryIf(f, attempt)
}

// DefaultRetry is three attempts, 100 ms doubling up to 10 s.
var DefaultRetry = RetryPolicy{Attempts: 3, Backoff: "exponential", Initial: 100 * time.Millisecond, Max: 10 * time.Second}

func (r RetryPolicy) delay(failed uint32) uint64 {
	initial, cap := ms(r.Initial), ms(r.Max)
	if r.Backoff == "fixed" {
		return min(cap, initial)
	}
	d := float64(initial) * math.Pow(2, float64(failed)-1)
	return min(cap, uint64(d))
}

// StepOptions configure a step.
type StepOptions struct {
	// Nil takes DefaultRetry.
	Retry *RetryPolicy
	// An attempt that takes longer fails with a TimeoutError (its context
	// is cancelled; a late result is ignored).
	Timeout time.Duration
	// An attempt that sends no Heartbeat for this long fails.
	HeartbeatTimeout time.Duration
	// Each attempt first takes a permit of this limit (Engine.Limit).
	Limit string
	// While waiting for a permit, a higher priority goes first; steps of
	// one priority take turns by fairness key, each key FairnessWeight
	// permits per turn (zero: 1). Not journaled.
	Priority       int32
	FairnessKey    string
	FairnessWeight uint32
	// Run at once, without waiting for earlier records to be durable. Only
	// for steps that change nothing outside the process: a crash can make
	// them run again with different inputs.
	ReadOnly bool
	// Run the function on the workflow's goroutine while it holds the
	// engine, instead of on a goroutine of its own: no handoff, but nothing
	// else on this engine runs until it returns, and Timeout cannot cut it
	// short. For fast, CPU-only functions.
	Inline bool
}

func stepOptions(opts []StepOptions) StepOptions {
	if len(opts) > 0 {
		return opts[0]
	}
	return StepOptions{}
}

// stepFn runs one attempt and returns its encoded result.
type stepFn func(ctx context.Context) ([]byte, error)

// attempts runs a step's function (or an async completion's send
// function) under its gate, permit, timeouts and retry policy.
type attempts struct {
	x         *runExec
	seq       uint32
	gate      uint64
	attempt   uint32
	notBefore uint64
	// launch prepares an attempt holding the engine (an await's token).
	launch func() (stepFn, error)
	o      StepOptions
	retry  RetryPolicy
	// done records the final outcome.
	done func(ok bool, data []byte, msg string) error
	name string
	// A step (not an await's send) pauses when its retries run out.
	pausable bool
	// What begin found for the first attempt's gate: -1 unknown, 0 shut
	// (the engine wakes the run when it may open), 1 open.
	known     int
	ticket    uint64
	hasTicket bool
	running   bool
	settled   bool
	// The one pending timer that calls tryRun again (a retry delay or a
	// rate limit's refill), by generation.
	later    *time.Timer
	laterGen uint64
	gen      uint64
	timers   []*time.Timer
	cancel   context.CancelFunc
}

func (a *attempts) cancelLater() {
	if a.later != nil {
		a.later.Stop()
		a.later = nil
	}
}

func (a *attempts) retryAt(at uint64) {
	a.cancelLater()
	a.laterGen++
	g := a.laterGen
	d := time.Duration(0)
	if now := nowMs(); at > now {
		d = time.Duration(at-now) * time.Millisecond
	}
	e := a.x.e
	a.later = time.AfterFunc(d, func() {
		e.post(func() {
			if a.laterGen != g {
				return
			}
			a.later = nil
			a.tryRun()
		})
	})
}

func (a *attempts) release() {
	if a.hasTicket && a.o.Limit != "" {
		x := a.x
		_, _, _ = x.e.n.host(x.shard, x.run, opRelease, a.ticket, 0, 0, str(a.o.Limit), str(""), str(""))
		a.hasTicket = false
	}
}

// fail settles the step's operation with err.
func (a *attempts) fail(err error) {
	x := a.x
	a.settled = true
	if o := x.pending[a.seq]; o != nil {
		delete(x.pending, a.seq)
		x.settle(o, x.note(err))
	}
	x.runReady()
}

func (a *attempts) tryRun() {
	// A retry timer and a wake can both get here: one attempt runs.
	if a.running || a.settled {
		return
	}
	x := a.x
	if x.ended || x.pending[a.seq] == nil {
		x.ungate(a.seq)
		a.cancelLater()
		a.release()
		return
	}
	// A paused step waits for its resume, which wakes the run. What begin
	// found for its gate is stale by then: ask again.
	if a.notBefore == paused {
		a.known = -1
		r, _, err := x.e.n.host(x.shard, x.run, opStepPaused, uint64(a.seq), 0, 0, str(""), str(""), str(""))
		if err != nil {
			a.fail(err)
			return
		}
		if r[0] != 0 {
			x.gate(a.seq, a.tryRun)
			return
		}
		// Resumed: a fresh set of attempts, counted from here.
		a.notBefore = 0
		a.attempt = 1
	}
	// A wake during the retry delay cannot let the attempt run: the delay's
	// timer calls back.
	if a.later != nil && nowMs() < a.notBefore {
		return
	}
	known := a.known
	a.known = -1
	open := known == 1
	if known == -1 {
		g := a.gate
		if a.o.ReadOnly {
			g = 0
		}
		var err error
		if open, err = x.mayRun(g, a.notBefore); err != nil {
			a.fail(err)
			return
		}
	}
	if !open {
		x.gate(a.seq, a.tryRun)
		if a.notBefore > nowMs() {
			a.retryAt(a.notBefore)
		}
		return
	}
	if a.o.Limit != "" {
		if !a.hasTicket {
			r, _, err := x.e.n.host(x.shard, x.run, opLimitTicket, 0, 0, 0, str(""), str(""), str(""))
			if err != nil {
				a.fail(err)
				return
			}
			a.ticket, a.hasTicket = r[0], true
		}
		r, _, err := x.e.n.host(x.shard, x.run, opAcquire, a.ticket, uint64(int64(a.o.Priority)), uint64(a.o.FairnessWeight),
			str(a.o.Limit), str(a.o.FairnessKey), str(""))
		if err != nil {
			a.fail(err)
			return
		}
		if r[0] != 0 {
			// Woken when the permit may be ours (or at the refill).
			x.gate(a.seq, a.tryRun)
			if r[0] == 1 {
				a.retryAt(r[1])
			}
			return
		}
	}
	x.ungate(a.seq)
	a.cancelLater()
	fn, err := a.launch()
	a.running = true
	a.gen++
	gen := a.gen
	x.beats[a.seq] = nowMs()
	if err != nil {
		a.finished(gen, nil, err)
		return
	}
	if a.o.Inline {
		info := &stepInfo{x: x, seq: a.seq, inline: true, a: a, gen: gen}
		x.current = int64(a.seq)
		data, err := call(fn, context.WithValue(x.e.base, stepKey{}, info))
		x.current = -1
		a.finished(gen, data, err)
		return
	}
	ctx, cancel := context.WithCancel(x.e.base)
	a.cancel = cancel
	ctx = context.WithValue(ctx, stepKey{}, &stepInfo{x: x, seq: a.seq, a: a, gen: gen})
	e := x.e
	expire := func(msg string) {
		e.post(func() {
			if a.gen == gen && a.running {
				a.finished(gen, nil, &TimeoutError{Message: msg})
			}
		})
	}
	if t := a.o.Timeout; t > 0 {
		a.timers = append(a.timers, time.AfterFunc(t, func() {
			expire(fmt.Sprintf("step timed out after %d ms", ms(t)))
		}))
	}
	if hb := a.o.HeartbeatTimeout; hb > 0 {
		var check func()
		check = func() {
			e.post(func() {
				if a.gen != gen || !a.running {
					return
				}
				left := int64(x.beats[a.seq]+ms(hb)) - int64(nowMs())
				if left <= 0 {
					a.finished(gen, nil, &TimeoutError{Message: fmt.Sprintf("step sent no heartbeat for %d ms", ms(hb))})
					return
				}
				a.timers = append(a.timers, time.AfterFunc(time.Duration(left)*time.Millisecond, check))
			})
		}
		a.timers = append(a.timers, time.AfterFunc(hb, check))
	}
	go func() {
		data, err := call(fn, ctx)
		e.post(func() { a.finished(gen, data, err) })
	}()
}

func call(fn stepFn, ctx context.Context) (data []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("step panicked: %v", r)
		}
	}()
	return fn(ctx)
}

func (a *attempts) finished(gen uint64, data []byte, err error) {
	if gen != a.gen || !a.running {
		return // a late result of an attempt that timed out
	}
	x := a.x
	a.running = false
	for _, t := range a.timers {
		t.Stop()
	}
	a.timers = nil
	if a.cancel != nil {
		a.cancel()
		a.cancel = nil
	}
	delete(x.beats, a.seq)
	a.release()
	if x.ended || x.pending[a.seq] == nil || a.settled {
		return
	}
	if err == nil {
		a.settled = true
		if e := a.done(true, data, ""); e != nil {
			a.fail(e)
			return
		}
		x.pump()
		return
	}
	f := FailureOf(err)
	msg := string(encodeFailure(err))
	retryable := a.retry.retries(f, a.attempt)
	pause := a.retry.OnExhausted
	if pause == ExhaustedDefault {
		pause = x.e.onExhausted
	}
	if retryable && (int(a.attempt) < a.retry.Attempts || (a.pausable && pause == Pause)) {
		if int(a.attempt) < a.retry.Attempts {
			a.notBefore = nowMs() + a.retry.delay(a.attempt)
		} else {
			// Out of retries: the run waits for a resume.
			a.notBefore = paused
			x.e.emitPaused(x, a.name, f)
		}
		gate, e := x.e.n.stepRetry(x.shard, x.run, a.seq, a.notBefore, msg)
		if e != nil {
			a.fail(e)
			return
		}
		a.gate = gate
		a.attempt++
		x.e.kicked = true
		a.tryRun()
		return
	}
	a.settled = true
	if e := a.done(false, nil, msg); e != nil {
		a.fail(e)
		return
	}
	x.pump()
}

// stepDone records a step's final result (kept here, so its delivery does
// not carry it back) or failure.
func (x *runExec) stepDone(seq uint32, ok bool, data []byte, msg string) error {
	if x.ended {
		return errEnded
	}
	if ok {
		x.held[seq] = data
		return x.e.n.stepDone(x.shard, x.run, seq, true, data)
	}
	return x.e.n.stepDone(x.shard, x.run, seq, false, []byte(msg))
}

// StartStep begins a durable step without waiting for it. The function
// runs once everything the run recorded before is durable (at once with
// ReadOnly), on its own goroutine (or inline, see StepOptions); it never
// runs again once its result is recorded.
func StartStep[T any](c *Ctx, name string, fn func(ctx context.Context) (T, error), opts ...StepOptions) *Future[T] {
	c.enter()
	x := c.x
	o := stepOptions(opts)
	if x.ended {
		return &Future[T]{failedOp(errEnded)}
	}
	s, err := x.e.n.beginStep(x.shard, x.run, name, o.ReadOnly, x.e.inlineOK())
	if err != nil {
		return &Future[T]{failedOp(x.note(err))}
	}
	op := c.newOp(kStep, name, s.seq)
	if !s.replay {
		known := 0
		if s.open {
			known = 1
		}
		retry := DefaultRetry
		if o.Retry != nil {
			retry = *o.Retry
		}
		seq := s.seq
		a := &attempts{
			x: x, seq: seq, gate: s.gate, attempt: s.attempt, notBefore: s.notBefore,
			launch: func() (stepFn, error) {
				return func(ctx context.Context) ([]byte, error) {
					v, err := fn(ctx)
					if err != nil {
						return nil, err
					}
					return encode(v)
				}, nil
			},
			o: o, retry: retry, name: name, known: known, pausable: true,
			done: func(ok bool, data []byte, msg string) error { return x.stepDone(seq, ok, data, msg) },
		}
		a.tryRun()
	}
	return &Future[T]{op}
}

// Step runs fn as a durable step and returns its result: recorded once,
// replayed after a crash.
func Step[T any](c *Ctx, name string, fn func(ctx context.Context) (T, error), opts ...StepOptions) (T, error) {
	return StartStep(c, name, fn, opts...).Get(c)
}

// StartAwaitCompletion begins a step completed from outside: send(token)
// starts work elsewhere, and the step waits until something holding the
// token calls Engine.Complete (or Fail). send is retried under the retry
// policy, each attempt with a new token.
func StartAwaitCompletion[T any](c *Ctx, name string, send func(ctx context.Context, token string) error, opts ...StepOptions) *Future[T] {
	r, _, err := c.host(opBeginAwait, 0, 0, 0, []byte(name))
	if err != nil {
		return &Future[T]{failedOp(err)}
	}
	x := c.x
	seq := uint32(r[0])
	op := c.newOp(kAwait, name, seq)
	if r[1] == 0 {
		o := stepOptions(opts)
		retry := DefaultRetry
		if o.Retry != nil {
			retry = *o.Retry
		}
		a := &attempts{
			x: x, seq: seq, gate: r[2], attempt: uint32(r[3]), notBefore: r[4],
			launch: func() (stepFn, error) {
				_, b, err := x.e.n.host(x.shard, x.run, opToken, uint64(seq), 0, 0, str(""), str(""), str(""))
				if err != nil {
					return nil, err
				}
				token := string(b)
				return func(ctx context.Context) ([]byte, error) { return nil, send(ctx, token) }, nil
			},
			o:     StepOptions{Timeout: o.Timeout, Inline: o.Inline},
			retry: retry, name: name, known: -1,
			done: func(ok bool, _ []byte, msg string) error {
				if ok {
					_, _, err := x.e.n.host(x.shard, x.run, opAwaiting, uint64(seq), 0, 0, str(""), str(""), str(""))
					return err
				}
				return x.stepDone(seq, false, nil, msg)
			},
		}
		a.tryRun()
	}
	return &Future[T]{op}
}

// AwaitCompletion runs StartAwaitCompletion and waits for the value.
func AwaitCompletion[T any](c *Ctx, name string, send func(ctx context.Context, token string) error, opts ...StepOptions) (T, error) {
	return StartAwaitCompletion[T](c, name, send, opts...).Get(c)
}

// ------------------------------------------------------------ inside steps

type stepKey struct{}

type stepInfo struct {
	x      *runExec
	seq    uint32
	inline bool
	once   sync.Once
	key    string
	// The attempt running the function: a stream operation of an attempt
	// that is over (it timed out) is dropped.
	a   *attempts
	gen uint64
}

// live reports whether this attempt still runs (holding the engine).
func (s *stepInfo) live() bool {
	return !s.x.ended && (s.a == nil || (s.a.running && s.a.gen == s.gen))
}

func stepOf(ctx context.Context) *stepInfo {
	s, _ := ctx.Value(stepKey{}).(*stepInfo)
	return s
}

// within runs f holding the engine: directly for an inline step (which
// holds it already), through the loop otherwise.
func (s *stepInfo) within(f func() error) error {
	if s.inline {
		return f()
	}
	return s.x.e.do(f)
}

// Key is a key stable across retries and recovery for the step whose
// function received ctx (distinct for each run of an ID), for idempotent
// calls to other services. Empty outside a step.
func Key(ctx context.Context) string {
	s := stepOf(ctx)
	if s == nil {
		return ""
	}
	s.once.Do(func() {
		_ = s.within(func() error {
			if s.x.ended {
				return errEnded
			}
			_, b, err := s.x.e.n.host(s.x.shard, s.x.run, opKey, uint64(s.seq), 0, 0, str(""), str(""), str(""))
			s.key = string(b)
			return err
		})
	})
	return s.key
}

// Heartbeat reports the running step's progress: coalesced, never waits
// for the disk, and resets its HeartbeatTimeout.
func Heartbeat(ctx context.Context, progress any) {
	s := stepOf(ctx)
	if s == nil {
		return
	}
	data, err := encode(progress)
	if err != nil {
		return
	}
	beat := func() {
		x := s.x
		if x.ended {
			return
		}
		x.beats[s.seq] = nowMs()
		_, _, _ = x.e.n.host(x.shard, x.run, opHeartbeat, uint64(s.seq), 0, 0, bin(data), str(""), str(""))
		x.e.kicked = true
	}
	if s.inline {
		beat()
		return
	}
	s.x.e.post(beat)
}

// Progress is the latest progress the running step reported, also from
// before a crash; ok is false if it reported none.
func Progress[T any](ctx context.Context) (value T, ok bool, err error) {
	s := stepOf(ctx)
	if s == nil {
		return value, false, nil
	}
	var data []byte
	err = s.within(func() error {
		x := s.x
		if x.ended {
			return errEnded
		}
		r, b, err := x.e.n.host(x.shard, x.run, opProgress, uint64(s.seq), 0, 0, str(""), str(""), str(""))
		if r[0] == 1 {
			ok = true
			data = append([]byte(nil), b...)
		}
		return err
	})
	if err != nil || !ok {
		return value, false, err
	}
	value, err = decode[T](data)
	return value, err == nil, err
}
