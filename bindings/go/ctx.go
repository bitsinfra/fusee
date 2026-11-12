package fusee

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"
)

// Ctx is the workflow context, one per workflow task. Its operations are
// journal operations: recorded in the engine's log and replayed after a
// crash. Workflow code must be deterministic:
//
//   - use only Ctx operations and the package's functions on a Ctx
//     (Step, WaitFor, Child, All, Race, Go, ...) to wait, branch on time
//     or randomness, or run code concurrently;
//   - start no goroutines, and use no channels, select, mutexes,
//     time.Now, time.Sleep, timers or math/rand in workflow code (step
//     functions may do anything);
//   - do not let map iteration order decide which journal operations run;
//   - pass a Ctx and the futures it made only to code of the same run.
//
// A Ctx's methods may be called only from the task it belongs to (the
// workflow function, a function started with Go, or an update handler).
type Ctx struct {
	x *runExec
	t *task
}

// enter checks the caller holds the engine as this task.
func (c *Ctx) enter() {
	if c.x.e.running != c.t {
		panic("fusee: a Ctx was used outside its workflow task (start concurrent workflow code with fusee.Go, not a goroutine)")
	}
}

// wait blocks the task until o settles (or the run ends).
func (c *Ctx) wait(o *op) {
	c.enter()
	for !o.done {
		if c.x.ended {
			return
		}
		o.waiters = append(o.waiters, item{t: c.t})
		c.t.park()
	}
}

func (c *Ctx) newOp(kind opKind, name string, seq uint32) *op {
	o := &op{kind: kind, name: name, seq: int64(seq)}
	c.x.pending[seq] = o
	c.x.e.kicked = true
	return o
}

func failedOp(err error) *op { return &op{kind: kTask, seq: -1, done: true, err: err} }

// host runs a journal operation for this run.
func (c *Ctx) host(code uint32, n0, n1, n2 uint64, s ...[]byte) ([5]uint64, []byte, error) {
	c.enter()
	x := c.x
	if x.ended {
		return [5]uint64{}, nil, errEnded
	}
	var a [3][]byte
	copy(a[:], s)
	r, data, err := x.e.n.host(x.shard, x.run, code, n0, n1, n2, bin(a[0]), bin(a[1]), bin(a[2]))
	return r, data, x.note(err)
}

// WorkflowID is the run's workflow ID.
func (c *Ctx) WorkflowID() string { return c.x.id }

// WorkflowType is the run's workflow type.
func (c *Ctx) WorkflowType() string { return c.x.wtype }

// Version is the code version the run started under.
func (c *Ctx) Version() uint32 { return c.x.version }

// RunID is the run's number (a reset or continue-as-new makes a new one).
func (c *Ctx) RunID() uint64 { return c.x.run }

// Future is an operation the code started and may wait for later.
type Future[T any] struct{ o *op }

// Awaitable is a future, for All and Race.
type Awaitable interface{ future() *op }

func (f *Future[T]) future() *op { return f.o }

// Get waits for the operation and returns its result.
func (f *Future[T]) Get(c *Ctx) (T, error) {
	var zero T
	o := f.o
	if !o.done {
		c.wait(o)
	}
	if !o.done {
		return zero, errEnded
	}
	if o.err != nil {
		return zero, o.err
	}
	switch o.kind {
	case kTask, kGroup:
		v, _ := o.value.(T)
		return v, nil
	case kSleep:
		return zero, nil
	}
	if o.timedOut {
		return zero, nil
	}
	return decode[T](o.payload)
}

// Done reports whether the operation finished (without waiting).
func (f *Future[T]) Done() bool { return f.o.done }

// TimedOut reports whether a wait (StartWait) ended by its timeout.
func (f *Future[T]) TimedOut() bool { return f.o.timedOut }

// StartSleep starts a durable timer.
func (c *Ctx) StartSleep(name string, d time.Duration) *Future[struct{}] {
	r, _, err := c.host(opBeginSleep, nowMs()+ms(d), 0, 0, []byte(name))
	if err != nil {
		return &Future[struct{}]{failedOp(err)}
	}
	return &Future[struct{}]{c.newOp(kSleep, name, uint32(r[0]))}
}

// Sleep waits for a durable timer; it survives restarts.
func (c *Ctx) Sleep(name string, d time.Duration) error {
	_, err := c.StartSleep(name, d).Get(c)
	return err
}

// StartWait waits for the next signal name without blocking; on timeout
// (zero: none) the future's value is T's zero value and TimedOut is true.
func StartWait[T any](c *Ctx, name string, timeout time.Duration) *Future[T] {
	var deadline uint64
	if timeout > 0 {
		deadline = nowMs() + ms(timeout)
	}
	r, _, err := c.host(opBeginWait, deadline, 0, 0, []byte(name))
	if err != nil {
		return &Future[T]{failedOp(err)}
	}
	return &Future[T]{c.newOp(kWait, name, uint32(r[0]))}
}

// WaitFor waits for the next signal name; ok is false on timeout.
func WaitFor[T any](c *Ctx, name string, timeout time.Duration) (value T, ok bool, err error) {
	f := StartWait[T](c, name, timeout)
	v, err := f.Get(c)
	return v, err == nil && !f.TimedOut(), err
}

// ChildOptions configure a child workflow.
type ChildOptions struct {
	// What happens to the child when its parent closes: "terminate" (the
	// default), "request-cancel" or "abandon".
	ParentClose string
	Timeout     time.Duration
}

func closeBits(p string) uint64 {
	switch p {
	case "request-cancel":
		return 1 << 4
	case "abandon":
		return 2 << 4
	}
	return 0
}

const detached = 1 << 6

func childArgs(input any, opts []ChildOptions) ([]byte, uint64, uint64, error) {
	var o ChildOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	data, err := encode(input)
	var deadline uint64
	if o.Timeout > 0 {
		deadline = nowMs() + ms(o.Timeout)
	}
	return data, closeBits(o.ParentClose), deadline, err
}

// StartChild starts a child workflow without waiting for its result.
func StartChild[O any](c *Ctx, wtype, id string, input any, opts ...ChildOptions) *Future[O] {
	data, flags, deadline, err := childArgs(input, opts)
	if err != nil {
		return &Future[O]{failedOp(err)}
	}
	r, _, err := c.host(opBeginChild, flags, deadline, 0, []byte(wtype), []byte(id), data)
	if err != nil {
		return &Future[O]{failedOp(err)}
	}
	return &Future[O]{c.newOp(kChild, wtype, uint32(r[0]))}
}

// Child runs a child workflow and returns its result.
func Child[O any](c *Ctx, wtype, id string, input any, opts ...ChildOptions) (O, error) {
	return StartChild[O](c, wtype, id, input, opts...).Get(c)
}

// SpawnChild starts a child without waiting for it. By default it is
// terminated when this workflow closes; ParentClose "abandon" lets it
// outlive it.
func (c *Ctx) SpawnChild(wtype, id string, input any, opts ...ChildOptions) error {
	data, flags, deadline, err := childArgs(input, opts)
	if err != nil {
		return err
	}
	_, _, err = c.host(opBeginChild, flags|detached, deadline, 0, []byte(wtype), []byte(id), data)
	c.x.e.kicked = true
	return err
}

// Compensate registers a step that undoes what the workflow did so far: if
// the workflow fails or is cancelled, every registered compensation runs as a
// step named name, newest first, then the workflow's error is returned.
// Register it after the step it undoes succeeded:
//
//	hold, err := fusee.Step(c, "reserve", reserve)
//	c.Compensate("release", func(ctx context.Context) error { return release(ctx, hold) })
func (c *Ctx) Compensate(name string, fn func(ctx context.Context) error) {
	c.x.undo = append(c.x.undo, compensation{name, fn})
}

// runCompensations runs the registered compensations, newest first. One that
// fails after its retries is reported and the others still run.
func (c *Ctx) runCompensations() {
	for len(c.x.undo) > 0 {
		k := c.x.undo[len(c.x.undo)-1]
		c.x.undo = c.x.undo[:len(c.x.undo)-1]
		_, err := Step(c, k.name, func(ctx context.Context) (struct{}, error) { return struct{}{}, k.fn(ctx) })
		if err != nil {
			fmt.Fprintf(os.Stderr, "fusee: workflow '%s': compensation '%s' failed: %v\n", c.x.id, k.name, err)
		}
	}
}

// SignalWorkflow is Signal under the name every SDK uses.
func (c *Ctx) SignalWorkflow(target, name string, value any) error {
	return c.Signal(target, name, value)
}

// Signal sends a signal to another workflow once this point is durable.
func (c *Ctx) Signal(target, name string, value any) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, _, err = c.host(opSend, 0, 0, 0, []byte(target), []byte(name), data)
	c.x.e.kicked = true
	return err
}

// CancelWorkflow asks another workflow to cancel, once this point is
// durable.
func (c *Ctx) CancelWorkflow(target, reason string) error {
	_, _, err := c.host(opCancelWorkflow, 0, 0, 0, []byte(target), []byte(reason))
	c.x.e.kicked = true
	return err
}

// ContinueOptions configure continue-as-new.
type ContinueOptions struct {
	// The new run's code version (zero: this run's).
	Version uint32
}

// ContinueAsNew ends this run and starts a new run of the same ID with
// input, in one record. It does not return unless recording failed:
// write `return out, ctx.ContinueAsNew(next)`.
func (c *Ctx) ContinueAsNew(input any, opts ...ContinueOptions) error {
	data, err := encode(input)
	if err != nil {
		return err
	}
	var v uint64
	if len(opts) > 0 {
		v = uint64(opts[0].Version)
	}
	if _, _, err := c.host(opContinueAsNew, v, 0, 0, data); err != nil {
		return err
	}
	c.x.e.kicked = true
	// The run ended: wait until its code is dropped.
	never := &op{kind: kTask, seq: -1}
	for !c.x.ended {
		c.wait(never)
	}
	return errEnded
}

// JournalSize is how many journal operations the run made so far.
func (c *Ctx) JournalSize() int {
	r, _, _ := c.host(opJournalSize, 0, 0, 0)
	return int(r[0])
}

// Patched is true for new runs, and for running ones the first time they
// get here after the deploy; false while replaying an older journal.
func (c *Ctx) Patched(id string) bool {
	r, _, _ := c.host(opPatched, 0, 0, 0, []byte(id))
	return r[0] == 1
}

// DeprecatePatch marks a patch whose old branch no running journal needs.
func (c *Ctx) DeprecatePatch(id string) {
	_, _, _ = c.host(opPatched, 1, 0, 0, []byte(id))
}

// SetAttributes records search attributes (a nil value removes a key).
// They are never encrypted.
func (c *Ctx) SetAttributes(attributes map[string]any) error {
	data, err := encode(attributes)
	if err != nil {
		return err
	}
	_, _, err = c.host(opSetAttributes, 0, 0, 0, data)
	return err
}

// Now is the current time, recorded (the same on replay).
func (c *Ctx) Now() time.Time {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], nowMs())
	_, v, err := c.host(opValue, 0, 0, 0, b[:])
	if err != nil || len(v) < 8 {
		return time.Time{}
	}
	return time.UnixMilli(int64(binary.LittleEndian.Uint64(v)))
}

// Random is a number in [0, 1), recorded.
func (c *Ctx) Random() float64 {
	_, v, err := c.host(opValue, 1, 0, 0, randomBytes(8))
	if err != nil || len(v) < 8 {
		return 0
	}
	return float64(binary.LittleEndian.Uint64(v)>>11) / float64(uint64(1)<<53)
}

// UUID is a random UUID, recorded.
func (c *Ctx) UUID() string {
	b := randomBytes(16)
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	_, v, err := c.host(opValue, 2, 0, 0, b)
	if err != nil || len(v) < 16 {
		return ""
	}
	return uuidText(v)
}

// OnQuery registers a read-only query handler (Run.Query). It runs while
// the workflow's code is blocked; the answer is released once the state it
// read is durable.
func OnQuery[A, R any](c *Ctx, name string, fn func(arg A) (R, error)) {
	c.x.queries[name] = func(p []byte) ([]byte, error) {
		a, err := decode[A](p)
		if err != nil {
			return nil, err
		}
		r, err := fn(a)
		if err != nil {
			return nil, err
		}
		return encode(r)
	}
}

// OnUpdate registers an update (Run.Update). validate (may be nil) runs
// first: an error refuses the update and records nothing. handle runs once
// per accepted update, as workflow code (it may run steps).
func OnUpdate[A, R any](c *Ctx, name string, handle func(c *Ctx, arg A) (R, error), validate func(arg A) error) {
	h := &updateHandler{handle: func(c *Ctx, p []byte) (any, error) {
		a, err := decode[A](p)
		if err != nil {
			return nil, err
		}
		return handle(c, a)
	}}
	if validate != nil {
		h.validate = func(p []byte) error {
			a, err := decode[A](p)
			if err != nil {
				return err
			}
			return validate(a)
		}
	}
	c.x.updates[name] = h
}

// Go runs fn as more workflow code of this run, concurrently with the
// caller (it starts when the caller next waits). Its result is the future's.
func Go[T any](c *Ctx, fn func(c *Ctx) (T, error)) *Future[T] {
	c.enter()
	x := c.x
	o := &op{kind: kTask, seq: -1}
	t := x.newTask(func(tc *Ctx) (any, error) { return fn(tc) }, func(v any, err error) {
		o.value = v
		x.settle(o, err)
	})
	x.ready = append(x.ready, item{t: t})
	return &Future[T]{o}
}

// All waits for every future (results in their futures); it returns the
// first error as soon as one fails.
func All(c *Ctx, fs ...Awaitable) error {
	c.enter()
	x := c.x
	g := &op{kind: kGroup, seq: -1}
	left := len(fs)
	if left == 0 {
		return nil
	}
	for _, f := range fs {
		o := f.future()
		member := func() {
			if g.done {
				return
			}
			if o.err != nil {
				x.settle(g, o.err)
				return
			}
			left--
			if left == 0 {
				x.settle(g, nil)
			}
		}
		x.ready = append(x.ready, item{f: func() { whenDone(o, member) }})
	}
	c.wait(g)
	return g.err
}

// Race waits for the first future to finish and returns its index and
// error (its value is in its future); replay picks the same one. The
// losing journal operations are dropped (a dropped wait takes no signal).
func Race(c *Ctx, fs ...Awaitable) (int, error) {
	c.enter()
	x := c.x
	g := &op{kind: kGroup, seq: -1}
	ops := make([]*op, len(fs))
	for i, f := range fs {
		ops[i] = f.future()
	}
	for i, o := range ops {
		member := func() {
			if g.done {
				return
			}
			g.value = i
			x.settle(g, o.err)
			for _, l := range ops {
				if !l.done && l.seq >= 0 && l.kind != kTask && l.kind != kGroup {
					x.forget(uint32(l.seq))
				}
			}
		}
		x.ready = append(x.ready, item{f: func() { whenDone(o, member) }})
	}
	c.wait(g)
	i, _ := g.value.(int)
	return i, g.err
}

// whenDone calls f now if o is done, else when it settles.
func whenDone(o *op, f func()) {
	if o.done {
		f()
		return
	}
	o.waiters = append(o.waiters, item{f: f})
}
