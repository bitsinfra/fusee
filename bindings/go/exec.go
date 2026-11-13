package fusee

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"slices"
)

// The scheduler. A run's workflow code runs on goroutines ("tasks") of
// which exactly one runs at a time, engine-wide: the goroutine holding the
// engine (the loop) hands it to a task and waits until the task blocks on
// a journal operation or returns. Between two deliveries a run's code runs
// until every task is blocked, as on replay, so the code makes the same
// journal operations in the same order live and replaying. Every handoff
// is a channel operation, so whatever one goroutine wrote, the next sees.

type opKind uint8

const (
	kStep opKind = iota
	kAwait
	kSleep
	kWait
	kChild
	kTask
	kGroup
)

// op is a journal operation (or a task, or a group) that code waits for.
type op struct {
	kind     opKind
	name     string
	seq      int64
	done     bool
	err      error
	payload  []byte
	timedOut bool
	value    any
	waiters  []item
}

// item is a task to resume, or a function to call, in a run's ready queue.
type item struct {
	t *task
	f func()
}

type task struct {
	x       *runExec
	ctx     *Ctx
	body    func(*Ctx) (any, error)
	onDone  func(any, error)
	wake    chan struct{}
	started bool
	exited  bool
	kill    bool
}

type gatedStep struct {
	seq   uint32
	retry func()
}

type updateHandler struct {
	validate func([]byte) error
	handle   func(*Ctx, []byte) (any, error)
}

// A step registered to undo work if the workflow fails (Ctx.Compensate).
type compensation struct {
	name string
	fn   func(ctx context.Context) error
}

type runExec struct {
	// Compensations, newest last (Ctx.Compensate); nil until one is registered.
	undo      []compensation
	e         *Engine
	shard     uint32
	run       uint64
	id, wtype string
	version   uint32
	startedMs uint64
	pending   map[uint32]*op
	gated     []gatedStep
	beats     map[uint32]uint64
	queries   map[string]func([]byte) ([]byte, error)
	updates   map[string]*updateHandler
	// A key of an object type: its handlers, looked up when a call is
	// delivered (object.go).
	object map[string]*updateHandler
	// Encoded step results this code recorded and has not had delivered.
	held  map[uint32][]byte
	ready []item
	tasks map[*task]struct{}
	// A scheduler loop is on the stack (deliveries wait for it), and a
	// delivery was asked for meanwhile.
	scheduling bool
	deliver    bool
	ended      bool
	reaped     bool
	// The engine stopped the run on a determinism error.
	stuck bool
	// The inline step running now, or -1.
	current int64
}

func newRunExec(e *Engine, ev startEvent) *runExec {
	return &runExec{
		e: e, shard: ev.shard, run: ev.run, id: ev.id, wtype: ev.wtype,
		version: ev.version, startedMs: ev.startedMs,
		pending: map[uint32]*op{},
		beats:   map[uint32]uint64{},
		queries: map[string]func([]byte) ([]byte, error){},
		updates: map[string]*updateHandler{},
		held:    map[uint32][]byte{},
		tasks:   map[*task]struct{}{},
		current: -1,
	}
}

func (x *runExec) newTask(body func(*Ctx) (any, error), onDone func(any, error)) *task {
	t := &task{x: x, body: body, onDone: onDone, wake: make(chan struct{})}
	t.ctx = &Ctx{x: x, t: t}
	x.tasks[t] = struct{}{}
	return t
}

// note marks a determinism error: the engine stopped the run.
func (x *runExec) note(err error) error {
	var d *DeterminismError
	if errors.As(err, &d) {
		x.stuck = true
	}
	return err
}

// ------------------------------------------------------------- handoffs

// resume runs t until it blocks or returns.
func (x *runExec) resume(t *task) {
	e := x.e
	prev := e.running
	e.running = t
	if !t.started {
		t.started = true
		go t.main()
	} else {
		t.wake <- struct{}{}
	}
	<-e.back
	e.running = prev
}

func (t *task) main() {
	defer func() {
		t.exited = true
		delete(t.x.tasks, t)
		t.x.e.back <- struct{}{}
	}()
	var (
		v   any
		err error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				err = fmt.Errorf("workflow code panicked: %v", r)
			}
		}()
		v, err = t.body(t.ctx)
	}()
	t.onDone(v, err)
}

// park gives the engine back and waits to be resumed. A task of a run that
// ended is killed instead: it exits without returning to its code.
func (t *task) park() {
	t.x.e.back <- struct{}{}
	<-t.wake
	if t.kill {
		runtime.Goexit()
	}
}

// reap ends the goroutines of a run that ended.
func (x *runExec) reap() {
	if x.reaped || x.scheduling {
		return
	}
	x.reaped = true
	x.ready = nil
	for t := range x.tasks {
		if t.started && !t.exited {
			t.kill = true
			x.resume(t)
		}
	}
	clear(x.tasks)
	for _, o := range x.pending {
		o.waiters = nil
	}
}

// ---------------------------------------------------------------- ready

func (x *runExec) settle(o *op, err error) {
	if o.done {
		return
	}
	o.done = true
	o.err = err
	x.ready = append(x.ready, o.waiters...)
	o.waiters = nil
}

func (x *runExec) drain() {
	for len(x.ready) > 0 && !x.ended {
		it := x.ready[0]
		x.ready[0] = item{}
		x.ready = x.ready[1:]
		if it.t != nil {
			if !it.t.exited {
				x.resume(it.t)
			}
		} else {
			it.f()
		}
	}
}

func (x *runExec) afterSchedule() {
	if x.ended {
		x.reap()
	}
}

// runReady runs every runnable task until all are blocked.
func (x *runExec) runReady() {
	if x.scheduling {
		return // the loop on the stack runs them
	}
	x.scheduling = true
	x.drain()
	x.scheduling = false
	if x.deliver {
		x.pump()
		return
	}
	x.afterSchedule()
}

// pump hands the run its deliveries one at a time; between two, its code
// runs until every task is blocked.
func (x *runExec) pump() {
	if x.scheduling {
		// Called from running code: the loop on the stack delivers once
		// every task is blocked.
		x.deliver = true
		return
	}
	x.scheduling = true
	for !x.ended {
		d, err := x.e.n.quiescent(x.shard, x.run)
		if err != nil {
			if x.failAll(err) {
				continue
			}
			break
		}
		if d.kind == dNone {
			break
		}
		x.apply(d)
		x.drain()
	}
	x.scheduling = false
	x.deliver = false
	x.e.kicked = true
	x.afterSchedule()
}

// failAll: the engine refused a delivery, so every pending operation
// fails. True if the code that then ran asked for another delivery.
func (x *runExec) failAll(err error) bool {
	seqs := make([]uint32, 0, len(x.pending))
	for s := range x.pending {
		seqs = append(seqs, s)
	}
	slices.Sort(seqs)
	for _, s := range seqs {
		x.settle(x.pending[s], err)
	}
	clear(x.pending)
	x.deliver = false
	x.drain()
	return x.deliver
}

func (x *runExec) wake() {
	for _, g := range slices.Clone(x.gated) {
		g.retry()
	}
	x.pump()
}

func (x *runExec) gate(seq uint32, retry func()) {
	for i := range x.gated {
		if x.gated[i].seq == seq {
			x.gated[i].retry = retry
			return
		}
	}
	x.gated = append(x.gated, gatedStep{seq, retry})
}

func (x *runExec) ungate(seq uint32) {
	for i := range x.gated {
		if x.gated[i].seq == seq {
			x.gated = slices.Delete(x.gated, i, i+1)
			return
		}
	}
}

// mayRun: whether a step gated on gate may run now. Open by the commit
// position the last tick or gate check reported, without a call.
func (x *runExec) mayRun(gate, notBefore uint64) (bool, error) {
	if x.ended {
		return false, errEnded
	}
	if gate <= x.e.n.commit(x.shard) && (notBefore == 0 || notBefore <= nowMs()) {
		return true, nil
	}
	return x.e.n.stepGate(x.shard, x.run, gate, notBefore, x.e.inlineOK())
}

// ------------------------------------------------------------ deliveries

func (x *runExec) apply(d delivery) {
	switch d.kind {
	case dResolve, dHeld:
		held, kept := x.held[d.seq]
		delete(x.held, d.seq)
		o := x.pending[d.seq]
		if o == nil {
			return
		}
		delete(x.pending, d.seq)
		if d.kind == dHeld {
			if !kept {
				x.settle(o, &Error{Code: "Failed", Message: fmt.Sprintf("the result of step %d was not kept", d.seq)})
				return
			}
			x.resolve(o, d.status, held)
			return
		}
		x.resolve(o, d.status, d.payload)
	case dCancel:
		reason := payloadText(d.payload)
		for _, s := range d.seqs {
			o := x.pending[s]
			delete(x.pending, s)
			x.ungate(s)
			delete(x.held, s)
			if o != nil {
				x.settle(o, &CancelledError{Reason: reason})
			}
		}
	case dUpdate:
		x.startUpdate(d)
	}
}

func (x *runExec) resolve(o *op, status uint32, payload []byte) {
	switch status {
	case statusOK:
		o.payload = payload
		x.settle(o, nil)
	case statusFailed:
		f := parseFailure(payload)
		switch o.kind {
		case kStep, kAwait:
			x.settle(o, &StepError{Step: o.name, Detail: f.Message, Cause: f})
		case kChild:
			x.settle(o, &WorkflowError{Code: "ChildFailed", ID: o.name, Cause: f})
		default:
			x.settle(o, &WorkflowError{Cause: f})
		}
	case statusCancelled:
		x.settle(o, &CancelledError{Reason: payloadText(payload)})
	case statusTimedOut:
		if o.kind == kWait {
			o.timedOut = true
			x.settle(o, nil)
			return
		}
		msg := payloadText(payload)
		if msg == "" {
			msg = "timed out"
		}
		x.settle(o, &TimeoutError{Message: msg})
	default:
		x.settle(o, &TerminatedError{Message: payloadText(payload)})
	}
}

func (x *runExec) startUpdate(d delivery) {
	h := x.updates[d.name]
	if h == nil && x.object != nil {
		h = x.object[d.name]
	}
	ordinal := d.ordinal
	if d.token >= 0 {
		var refusal *string
		if h == nil {
			r := fmt.Sprintf("no handler for update '%s'", d.name)
			refusal = &r
		} else if h.validate != nil {
			if err := safeValidate(h.validate, d.payload); err != nil {
				r := err.Error()
				refusal = &r
			}
		}
		o, err := x.e.n.decideUpdate(x.shard, uint64(d.token), refusal)
		if err != nil || refusal != nil || o < 0 {
			return
		}
		ordinal = uint64(o)
	} else if h == nil {
		msg := fmt.Sprintf("determinism error: the journal delivers update '%s' (id '%s') but the code registered no handler for it", d.name, d.id)
		x.ended = true
		x.stuck = true
		_ = x.e.n.stuck(x.shard, x.run, msg)
		return
	}
	arg := d.payload
	t := x.newTask(func(c *Ctx) (any, error) { return h.handle(c, arg) }, func(v any, err error) {
		if x.ended {
			return
		}
		var status uint32
		var data []byte
		if err == nil {
			b, encErr := encode(v)
			if encErr != nil {
				status, data = statusFailed, []byte("update result is not JSON: "+encErr.Error())
			} else {
				status, data = statusOK, b
			}
		} else {
			s, msg := outcome(err)
			status, data = s, []byte(msg)
		}
		_, _, _ = x.e.n.host(x.shard, x.run, opUpdateDone, ordinal, uint64(status), 0, bin(data), str(""), str(""))
		x.e.kicked = true
	})
	x.ready = append(x.ready, item{t: t})
}

func safeValidate(v func([]byte) error, p []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%v", r)
		}
	}()
	return v(p)
}

// forget drops an operation the code no longer waits for (a race's loser).
func (x *runExec) forget(seq uint32) {
	if _, ok := x.pending[seq]; !ok {
		return
	}
	delete(x.pending, seq)
	x.ungate(seq)
	delete(x.held, seq)
	_, _, _ = x.e.n.host(x.shard, x.run, opForget, uint64(seq), 0, 0, str(""), str(""), str(""))
}
