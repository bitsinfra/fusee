package fusee

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
	"unsafe"
)

// Options configure an engine. The zero value is a local engine whose
// data directory is ./fusee-data.
type Options struct {
	// "local" (the default), "memory", "server" or "replicated".
	Durability string
	// Local: the data directory. Replicated: this member's directory.
	Dir string
	// Server: the server's address (host:port). Replicated: this member's
	// connection options only (?cert=..&key=..&ca=..&token-file=..).
	Address string
	// Replicated: this member's index in Peers.
	ReplicaID int
	// Replicated: every member's address (three or five).
	Peers []string
	// Shards of a new engine (default 1); a server's engine has the
	// server's count.
	Shards int
	// Zero takes each default.
	Retention, ParkAfter, CheckpointInterval time.Duration
	// "fsync", "fdatasync", "fullfsync" or "dsync"; empty for the default.
	Flush        string
	SegmentBytes int64
	// Keep finished runs' journals (for Inspect and ExportJournal).
	KeepHistory    bool
	ArchiveHistory bool
	// AES-256-GCM payload encryption: the first key encrypts, every key
	// decrypts.
	Codec []CodecKey
	// Off: every flush goes to the log thread, so engine calls never wait
	// for the disk. Nil takes the default (on).
	InlineFlush *bool
	// Workflow definitions (Define) to register as the engine opens.
	Workflows []Definition
	// What a step does when its retries run out, unless its RetryPolicy
	// says: Pause pauses the run until Run.Resume. Zero: Fail.
	OnExhausted Exhausted
}

// CodecKey is one AES-256-GCM key (32 bytes) and its ID.
type CodecKey struct {
	ID  string
	Key []byte
}

// Gates a lone workflow may flush inline per tick before the loop looks at
// other work (as the Python SDK).
const inlinePerTick = 32

// An Engine runs workflows. Workflow code runs on goroutines the engine
// schedules one at a time (see Ctx); its methods may be called from any
// goroutine except workflow code and inline steps.
type Engine struct {
	// Limits defined through this engine, by name (Limits); loop only.
	limitSpecs map[string]LimitOptions
	// On("stuck", ...) handlers; loop only.
	stuckHandlers []func(workflowID, message string)
	// On("paused", ...) handlers; loop only.
	pausedHandlers []func(workflowID, message string)
	// Options.OnExhausted.
	onExhausted Exhausted
	n             native
	key           unsafe.Pointer
	reqs          chan func()
	// Notifications from the engine's threads (coalesced).
	wakeCh chan struct{}
	done   chan struct{}
	// Cancelled when the engine closes: the parent of step contexts.
	base       context.Context
	cancelBase context.CancelFunc
	closeErr   error
	// Stream operations step functions queued for the loop (stream.go).
	streams streamQueue

	// Owned by the goroutine holding the engine: the loop, or the
	// workflow task it handed the engine to.
	closing     bool
	failure     error
	types       map[string]map[uint32]workflowFn
	runs        map[runKey]*runExec
	awaiting    map[string][]startEvent
	tokens      map[uint64]pendingReply
	waiters     map[runKey][]chan resultMsg
	nextToken   uint64
	kicked      bool
	timer       *time.Timer
	timerOn     bool
	timerAt     uint64
	inlineLeft  int
	back        chan struct{}
	running     *task
	serveRemote bool
	ticks       uint64
}

type runKey struct {
	shard uint32
	run   uint64
}

type workflowFn func(c *Ctx, input []byte) (any, error)

type startEvent struct {
	shard     uint32
	run       uint64
	version   uint32
	startedMs uint64
	id, wtype string
	input     []byte
}

type reply struct {
	payload []byte
	err     error
}

type pendingReply struct {
	ch    chan reply
	value bool
}

type resultMsg struct {
	status  uint32
	payload []byte
	err     error
}

// Open opens an engine. In server mode it connects to the server (and may
// wait for the previous worker's lease); in replicated mode it waits until
// this member leads.
func Open(opts Options) (*Engine, error) {
	if opts.Durability == "" {
		opts.Durability = "local"
	}
	if v := ABIVersion(); v < abiNeeded {
		return nil, &Error{Code: "Invalid", Message: fmt.Sprintf("libfusee_c speaks C interface version %d; this package needs %d", v, abiNeeded)}
	}
	e := &Engine{
		reqs:        make(chan func(), 1024),
		wakeCh:      make(chan struct{}, 1),
		done:        make(chan struct{}),
		types:       map[string]map[uint32]workflowFn{},
		runs:        map[runKey]*runExec{},
		awaiting:    map[string][]startEvent{},
		tokens:      map[uint64]pendingReply{},
		waiters:     map[runKey][]chan resultMsg{},
		nextToken:   1,
		back:        make(chan struct{}),
		serveRemote: opts.Durability == "server",
		inlineLeft:  inlinePerTick,
		onExhausted: opts.OnExhausted,
	}
	e.base, e.cancelBase = context.WithCancel(context.Background())
	e.timer = time.NewTimer(time.Hour)
	e.timer.Stop()
	e.key = allocKey()
	engines.Store(uintptr(e.key), e)
	n, err := openNative(opts, e.key)
	if err != nil {
		engines.Delete(uintptr(e.key))
		freeKey(e.key)
		e.cancelBase()
		return nil, err
	}
	e.n = n
	e.kicked = true
	go e.loop()
	// Runs recovered before their code is registered wait for it.
	for _, d := range opts.Workflows {
		d.registerOn(e)
	}
	return e, nil
}

// notify is the engine's wake from any thread.
func (e *Engine) notify() {
	select {
	case e.wakeCh <- struct{}{}:
	default:
	}
}

func (e *Engine) loop() {
	defer close(e.done)
	for {
		if !e.kicked {
			select {
			case f := <-e.reqs:
				f()
			case <-e.wakeCh:
			case <-e.timer.C:
				e.timerOn = false
			}
		}
		e.kicked = false
	more:
		for i := 0; i < 1024 && !e.closing; i++ {
			select {
			case f := <-e.reqs:
				f()
			default:
				break more
			}
		}
		if e.closing {
			e.shutdown()
			return
		}
		e.tick()
	}
}

// post runs f on the engine's loop, later; nothing if it closed.
func (e *Engine) post(f func()) {
	select {
	case e.reqs <- f:
	case <-e.done:
	}
}

// do runs f holding the engine and waits for it. Never call it from
// workflow code or an inline step (they hold the engine already).
func (e *Engine) do(f func() error) error {
	errc := make(chan error, 1)
	select {
	case e.reqs <- func() {
		if e.closing {
			errc <- e.closedErr()
			return
		}
		errc <- f()
	}:
	case <-e.done:
		return e.closedErr()
	}
	select {
	case err := <-errc:
		return err
	case <-e.done:
		select {
		case err := <-errc:
			return err
		default:
			return e.closedErr()
		}
	}
}

func (e *Engine) closedErr() error {
	if e.failure != nil {
		return e.failure
	}
	return ErrClosed
}

// request issues a call the engine answers with a token event, then waits
// for the answer.
func (e *Engine) request(ctx context.Context, value bool, issue func(token uint64) error) ([]byte, error) {
	ch := make(chan reply, 1)
	err := e.do(func() error {
		tok := e.nextToken
		e.nextToken++
		e.tokens[tok] = pendingReply{ch, value}
		if err := issue(tok); err != nil {
			delete(e.tokens, tok)
			return err
		}
		e.kicked = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return wait(ctx, ch)
}

func wait(ctx context.Context, ch chan reply) ([]byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case r := <-ch:
		return r.payload, r.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Close flushes and closes the engine. Runs left unfinished resume when an
// engine opens the same directory again.
func (e *Engine) Close() error {
	_ = e.do(func() error {
		e.closing = true
		return nil
	})
	<-e.done
	return e.closeErr
}

func (e *Engine) shutdown() {
	reason := e.closedErr()
	for tok, p := range e.tokens {
		p.ch <- reply{err: reason}
		delete(e.tokens, tok)
	}
	for k, ws := range e.waiters {
		for _, ch := range ws {
			ch <- resultMsg{err: reason}
		}
		delete(e.waiters, k)
	}
	for k, x := range e.runs {
		x.ended = true
		x.reap()
		delete(e.runs, k)
	}
	e.timer.Stop()
	e.cancelBase()
	err := e.n.close()
	if e.failure == nil {
		e.closeErr = err
	}
	engines.Delete(uintptr(e.key))
	freeKey(e.key)
}

func (e *Engine) stopAfterFailure(err error) {
	if e.failure == nil {
		e.failure = err
	}
	e.closing = true
}

// inlineOK spends the tick's budget of inline flushes.
func (e *Engine) inlineOK() bool {
	if e.inlineLeft <= 0 {
		return false
	}
	e.inlineLeft--
	return true
}

func (e *Engine) tick() {
	if e.closing {
		return
	}
	e.ticks++
	e.inlineLeft = inlinePerTick
	e.n.io.owed = 0
	t, err := e.n.tick()
	if err != nil {
		e.stopAfterFailure(err)
		return
	}
	if e.serveRemote {
		for {
			j, ok, err := e.n.nextCall()
			if err != nil {
				e.stopAfterFailure(err)
				return
			}
			if !ok {
				break
			}
			go e.handleRemote(j)
		}
	}
	for i := 0; i+stride <= len(t.events); i += stride {
		e.handle(t.events[i:i+stride], &t)
	}
	now := nowMs()
	var at uint64
	has := false
	if t.next >= 0 {
		at, has = uint64(t.next), true
	} else if len(e.tokens) > 0 || len(e.waiters) > 0 {
		at, has = now+50, true
	}
	switch {
	case !has:
		if e.timerOn {
			e.timer.Stop()
			e.timerOn = false
		}
	case at <= now:
		e.kicked = true
	case !e.timerOn || at < e.timerAt:
		// A timer due no later than needed stays: an early tick only looks.
		e.timer.Reset(time.Duration(at-now) * time.Millisecond)
		e.timerOn = true
		e.timerAt = at
	}
	if len(t.events) > 0 || e.n.owed() {
		e.kicked = true
	}
}

func (e *Engine) handle(ev []uint64, t *tickOut) {
	switch ev[0] {
	case evWake:
		if x := e.runs[runKey{uint32(ev[1]), ev[2]}]; x != nil {
			x.wake()
		}
	case evAck, evReject, evReply, evRefuse:
		p, ok := e.tokens[ev[1]]
		if !ok {
			return
		}
		delete(e.tokens, ev[1])
		var r reply
		switch ev[0] {
		case evReply:
			payload := append([]byte(nil), t.extra(ev[5])...)
			if ev[2] == statusOK {
				r.payload = payload
			} else {
				r.err = &WorkflowError{Cause: parseFailure(payload)}
			}
		case evRefuse:
			r.err = &RejectedError{Reason: payloadText(t.extra(ev[5]))}
		case evReject:
			code := "Rejected"
			if p.value {
				code = "AlreadyExists"
			}
			r.err = &Error{Code: code, Message: "the workflow has already finished, or its ID is in use"}
		}
		p.ch <- r
	case evResult:
		k := runKey{uint32(ev[1]), ev[2]}
		ws := e.waiters[k]
		if len(ws) == 0 {
			return
		}
		delete(e.waiters, k)
		payload := append([]byte(nil), t.extra(ev[5])...)
		for _, ch := range ws {
			ch <- resultMsg{status: uint32(ev[3]), payload: payload}
		}
	case evStart:
		x := ev[5]
		e.onStart(startEvent{
			shard: uint32(ev[1]), run: ev[2], version: uint32(ev[3]), startedMs: ev[4],
			id: string(t.extra(x)), wtype: string(t.extra(x + 1)),
			input: append([]byte(nil), t.extra(x+2)...),
		})
	case evStuck:
		if x := e.runs[runKey{uint32(ev[1]), ev[2]}]; x != nil && len(e.stuckHandlers) > 0 {
			msg := ""
			if int64(ev[5]) >= 0 {
				msg = string(t.extra(ev[5]))
			}
			for _, h := range e.stuckHandlers {
				h(x.id, msg)
			}
		}
		e.onEnded(uint32(ev[1]), ev[2])
	case evEnded:
		e.onEnded(uint32(ev[1]), ev[2])
	}
}

func (e *Engine) onStart(ev startEvent) {
	if !e.instantiate(ev) {
		e.awaiting[ev.wtype] = append(e.awaiting[ev.wtype], ev)
	}
}

func (e *Engine) onEnded(shard uint32, run uint64) {
	k := runKey{shard, run}
	if x := e.runs[k]; x != nil {
		delete(e.runs, k)
		x.ended = true
		x.reap()
	}
}

func (e *Engine) instantiate(ev startEvent) bool {
	fn := e.types[ev.wtype][ev.version]
	if fn == nil {
		return false
	}
	k := runKey{ev.shard, ev.run}
	if e.runs[k] != nil {
		return true
	}
	x := newRunExec(e, ev)
	e.runs[k] = x
	root := x.newTask(func(c *Ctx) (any, error) { return fn(c, ev.input) }, func(v any, err error) {
		e.finished(x, v, err)
	})
	x.ready = append(x.ready, item{t: root})
	x.runReady()
	x.pump()
	return true
}

func (e *Engine) register(name string, version uint32, fn workflowFn) error {
	return e.do(func() error {
		e.registerHolding(name, version, fn)
		return nil
	})
}

// registerHolding registers code, holding the engine: runs of the type that
// were waiting for it start.
func (e *Engine) registerHolding(name string, version uint32, fn workflowFn) {
	m := e.types[name]
	if m == nil {
		m = map[uint32]workflowFn{}
		e.types[name] = m
	}
	m[version] = fn
	waiting := e.awaiting[name]
	delete(e.awaiting, name)
	var keep []startEvent
	for _, ev := range waiting {
		if !e.instantiate(ev) {
			keep = append(keep, ev)
		}
	}
	if len(keep) > 0 {
		e.awaiting[name] = keep
	}
}

// latest is the newest registered version of a type (at least version).
func (e *Engine) latest(wtype string, version uint32) uint32 {
	for v := range e.types[wtype] {
		if v > version {
			version = v
		}
	}
	if version == 0 {
		version = 1
	}
	return version
}

// finished records a run's result when its code returned.
func (e *Engine) finished(x *runExec, v any, err error) {
	if x.ended {
		return
	}
	x.ended = true
	if x.stuck {
		// The engine stopped the run; its Stuck event drops it.
		return
	}
	delete(e.runs, runKey{x.shard, x.run})
	var status uint32
	var data []byte
	if err == nil {
		b, encErr := encode(v)
		if encErr != nil {
			status, data = statusFailed, []byte("result is not JSON: "+encErr.Error())
		} else {
			status, data = statusOK, b
		}
	} else {
		s, msg := outcome(err)
		status, data = s, []byte(msg)
	}
	if ferr := e.n.finish(x.shard, x.run, status, data); ferr != nil {
		e.stopAfterFailure(ferr)
	}
	e.kicked = true
}

// result waits for a run's result, following continue-as-new and reset.
func (e *Engine) result(ctx context.Context, id string) (uint32, []byte, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		var st resultState
		ch := make(chan resultMsg, 1)
		err := e.do(func() error {
			s, err := e.n.result(id)
			if err != nil {
				return err
			}
			st = s
			if s.state == 1 {
				k := runKey{s.shard, s.run}
				e.waiters[k] = append(e.waiters[k], ch)
				e.kicked = true
			}
			return nil
		})
		if err != nil {
			return 0, nil, err
		}
		switch st.state {
		case 0:
			return 0, nil, &Error{Code: "NotFound", Message: fmt.Sprintf("workflow '%s' not found", id)}
		case 2:
			return st.status, st.payload, nil
		}
		select {
		case m := <-ch:
			if m.err != nil {
				return 0, nil, m.err
			}
			if m.status == statusContinued || m.status == statusReset {
				continue
			}
			return m.status, m.payload, nil
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		}
	}
}

func (e *Engine) exists(id string) (bool, error) {
	var found bool
	err := e.do(func() error {
		s, err := e.n.result(id)
		found = s.state != 0
		return err
	})
	return found, err
}

// query asks a run's query handler; the answer waits until the state it
// read is durable.
func (e *Engine) query(ctx context.Context, id, name string, arg any) (json.RawMessage, error) {
	argData, err := json.Marshal(arg)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 4000; i++ {
		var (
			answer []byte
			native bool
			retry  bool
			shard  uint32
			lsn    uint64
		)
		err := e.do(func() error {
			s, run, l, foreign, err := e.n.queryPoint(id)
			if err != nil {
				return err
			}
			shard, lsn = s, l
			x := e.runs[runKey{s, run}]
			slot, replaying, err := e.n.codeState(s, run)
			if err != nil {
				return err
			}
			if x == nil && !foreign {
				if !slot {
					return &Error{Code: "Failed", Message: fmt.Sprintf("workflow '%s' is not running code", id)}
				}
				native = true
				return nil
			}
			if x == nil || (slot && replaying) {
				e.kicked = true
				retry = true
				return nil
			}
			h := x.queries[name]
			if h == nil {
				return &RejectedError{Reason: fmt.Sprintf("no handler for query '%s'", name)}
			}
			answer, err = safeQuery(h, argData)
			return err
		})
		if err != nil {
			return nil, err
		}
		if native {
			p, err := e.request(ctx, true, func(tok uint64) error { return e.n.queryNative(id, name, argData, tok) })
			return raw(p), err
		}
		if retry {
			select {
			case <-time.After(time.Millisecond):
			case <-ctxDone(ctx):
				return nil, ctx.Err()
			}
			continue
		}
		if _, err := e.request(ctx, false, func(tok uint64) error { return e.n.ackAt(shard, lsn, tok) }); err != nil {
			return nil, err
		}
		return raw(answer), nil
	}
	return nil, &Error{Code: "Failed", Message: "the workflow did not finish replaying"}
}

// safeQuery runs a query handler; a panic in it is the query's error.
func safeQuery(h func([]byte) ([]byte, error), arg []byte) (out []byte, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("query handler panicked: %v", r)
		}
	}()
	return h(arg)
}

func ctxDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

// ------------------------------------------------------------ engine API

// Run is a handle to the run with this workflow ID, its result untyped.
func (e *Engine) Run(id string) *Run[json.RawMessage] { return &Run[json.RawMessage]{e: e, ID: id} }

// RunSummary is one run in a listing.
type RunSummary struct {
	ID         string         `json:"id"`
	Type       string         `json:"type"`
	Version    uint32         `json:"version"`
	Status     string         `json:"status"`
	StartedMs  uint64         `json:"startedMs"`
	FinishedMs *uint64        `json:"finishedMs"`
	Error      *string        `json:"error"`
	Shard      uint32         `json:"shard"`
	Run        uint64         `json:"run"`
	Attributes map[string]any `json:"attributes"`
	// The run is a key of an object type (see RegisterObject).
	Object bool `json:"object"`
}

// RunDetail is a run's summary and its normalized journal, and for a key
// of an object type its state and pending calls.
type RunDetail struct {
	RunSummary
	Journal json.RawMessage `json:"journal"`
	// Nil for a run that is not a key of an object type.
	Object *ObjectDetail `json:"object,omitempty"`
}

// ListOptions filter a listing: Where matches the run's type, status, id,
// startedMs and search attributes, with equality or an operator object
// ({"gte": 100}; eq, ne, gt, gte, lt, lte).
type ListOptions struct {
	Limit int
	Where map[string]any
}

// List lists runs.
func (e *Engine) List(ctx context.Context, opts ListOptions) ([]RunSummary, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	var filter []byte
	if opts.Where != nil {
		b, err := json.Marshal(opts.Where)
		if err != nil {
			return nil, err
		}
		filter = b
	}
	var out []byte
	err := e.do(func() error {
		j, err := e.n.list(filter, opts.Limit)
		out = j
		return err
	})
	if err != nil {
		return nil, err
	}
	var runs []RunSummary
	return runs, json.Unmarshal(out, &runs)
}

// LimitOptions size a concurrency and rate limit for steps.
type LimitOptions struct {
	Concurrent uint32
	PerSecond  float64
}

// Limit defines (or resizes) a limit that steps take a permit of
// (StepOptions.Limit).
func (e *Engine) Limit(name string, o LimitOptions) error {
	return e.do(func() error {
		if err := e.n.limit(name, o.Concurrent, o.PerSecond); err != nil {
			return err
		}
		if e.limitSpecs == nil {
			e.limitSpecs = map[string]LimitOptions{}
		}
		e.limitSpecs[name] = o
		return nil
	})
}

// LimitInUse is how many permits of a limit are held right now.
func (e *Engine) LimitInUse(name string) (int, error) {
	var held uint32
	err := e.do(func() error {
		h, _, err := e.n.limitUsage(name)
		held = h
		return err
	})
	return int(held), err
}

// LimitWaiting is how many of this process's steps wait for a permit.
func (e *Engine) LimitWaiting(name string) (int, error) {
	var waiting uint64
	err := e.do(func() error {
		_, w, err := e.n.limitUsage(name)
		waiting = w
		return err
	})
	return int(waiting), err
}

// Complete finishes an async completion with its token; it returns once
// the value is durable. A stale token (an earlier attempt's) is rejected.
func (e *Engine) Complete(ctx context.Context, token string, value any) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = e.request(ctx, false, func(tok uint64) error { return e.n.complete(token, true, data, tok) })
	return err
}

// Fail fails an async completion with its token.
func (e *Engine) Fail(ctx context.Context, token string, message string) error {
	_, err := e.request(ctx, false, func(tok uint64) error { return e.n.complete(token, false, []byte(message), tok) })
	return err
}

// ExportJournal is a run's journal as an export file (.fjnl), or nil.
func (e *Engine) ExportJournal(id string) ([]byte, error) {
	var out []byte
	err := e.do(func() error {
		b, found, err := e.n.exportJournal(id)
		if found {
			out = b
		}
		return err
	})
	return out, err
}

// ShardMetrics are each shard's counters, in shard order (durations in
// microseconds).
func (e *Engine) ShardMetrics() ([]map[string]uint64, error) {
	var out []byte
	err := e.do(func() error {
		b, err := e.n.shardMetrics()
		out = b
		return err
	})
	if err != nil {
		return nil, err
	}
	var m []map[string]uint64
	return m, json.Unmarshal(out, &m)
}

// Stats count the engine's work in this process.
type Stats struct {
	// Calls from Go into the engine library.
	NativeCalls uint64
	// Engine ticks the loop ran.
	Ticks uint64
}

// Stats are the engine's counters now.
func (e *Engine) Stats() (Stats, error) {
	var s Stats
	err := e.do(func() error {
		s = Stats{NativeCalls: *e.n.calls, Ticks: e.ticks}
		return nil
	})
	return s, err
}

// Shards is the engine's shard count.
func (e *Engine) Shards() int { return int(e.n.io.shards) }
