package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"time"
)

// Workflow is a registered workflow type: In is its input, Out its result.
type Workflow[In, Out any] struct {
	e       *Engine
	Name    string
	Version uint32
}

// RegisterOptions configure a registration.
type RegisterOptions struct {
	// The code version (default 1). New runs start under the newest one
	// registered; a run keeps the version it started under.
	Version uint32
}

// Register registers a workflow type on the engine. Runs of the type that
// were waiting for code (after a restart) start replaying now.
func Register[In, Out any](e *Engine, name string, fn func(c *Ctx, in In) (Out, error), opts ...RegisterOptions) *Workflow[In, Out] {
	version := uint32(1)
	if len(opts) > 0 && opts[0].Version > 0 {
		version = opts[0].Version
	}
	_ = e.register(name, version, func(c *Ctx, input []byte) (any, error) {
		in, err := decode[In](input)
		if err != nil {
			return nil, fmt.Errorf("workflow input: %w", err)
		}
		out, err := fn(c, in)
		if err != nil && len(c.x.undo) > 0 && !c.x.stuck {
			// Failed or cancelled: undo what is registered first.
			c.runCompensations()
		}
		return out, err
	})
	return &Workflow[In, Out]{e: e, Name: name, Version: version}
}

// Def is a workflow type apart from any engine: its name, version and
// code. Define one at package level, register it on each engine that runs
// it (Options.Workflows, or Register on the definition), and start runs
// through the handle Register returns.
//
//	var Greet = fusee.Define("greet", func(c *fusee.Ctx, name string) (string, error) { ... })
//	engine, _ := fusee.Open(fusee.Options{Workflows: []fusee.Definition{Greet}})
//	run, _ := Greet.On(engine).Start(ctx, "g-1", "ana")
type Def[In, Out any] struct {
	Name    string
	Version uint32
	fn      func(c *Ctx, in In) (Out, error)
}

// Definition is any workflow definition (Def), for Options.Workflows.
type Definition interface {
	registerOn(e *Engine)
}

// Define is a workflow type apart from any engine (Def).
func Define[In, Out any](name string, fn func(c *Ctx, in In) (Out, error), opts ...RegisterOptions) Def[In, Out] {
	version := uint32(1)
	if len(opts) > 0 && opts[0].Version > 0 {
		version = opts[0].Version
	}
	return Def[In, Out]{Name: name, Version: version, fn: fn}
}

// Register registers the definition on e and returns its handle.
func (d Def[In, Out]) Register(e *Engine) *Workflow[In, Out] {
	return Register(e, d.Name, d.fn, RegisterOptions{Version: d.Version})
}

// On is the handle that starts runs of the definition on e (registered
// there, or run by another engine of the same server).
func (d Def[In, Out]) On(e *Engine) *Workflow[In, Out] {
	return &Workflow[In, Out]{e: e, Name: d.Name, Version: d.Version}
}

func (d Def[In, Out]) registerOn(e *Engine) { d.Register(e) }

// WorkflowOf is a handle to start runs of a type registered elsewhere
// (another process, or another engine through a server).
func WorkflowOf[In, Out any](e *Engine, name string) *Workflow[In, Out] {
	return &Workflow[In, Out]{e: e, Name: name}
}

// Policies for a start whose workflow ID is in use.
const (
	// While a run of the ID is running.
	IfRunningFail              = "fail"
	IfRunningUseExisting       = "use-existing"
	IfRunningTerminateExisting = "terminate-existing"
	// After a run of the ID finished.
	IfFinishedAllow         = "allow"
	IfFinishedAllowIfFailed = "allow-if-failed"
	IfFinishedRefuse        = "refuse"
	IfFinishedUseExisting   = "use-existing"
)

// StartOptions configure a start.
type StartOptions struct {
	// The run's deadline from now (zero: none).
	Timeout time.Duration
	// When a run of the ID is running (default "fail") or finished
	// (default "allow"); see the IfRunning and IfFinished constants.
	IfRunning  string
	IfFinished string
	// The code version (zero: the newest registered).
	Version uint32
	// Update-with-start: the update's ID (repeating it gets the recorded
	// result).
	UpdateID string
}

func idPolicy(running, finished string) (uint32, error) {
	r := map[string]uint32{"": 0, "fail": 0, "use-existing": 1, "terminate-existing": 2}
	f := map[string]uint32{"": 0, "allow": 0, "allow-if-failed": 1, "refuse": 2, "use-existing": 3}
	rv, ok1 := r[running]
	fv, ok2 := f[finished]
	if !ok1 || !ok2 {
		return 0, &Error{Code: "Invalid", Message: fmt.Sprintf("bad ID policy %q / %q", running, finished)}
	}
	return rv | fv<<2, nil
}

func deadline(d time.Duration) uint64 {
	if d <= 0 {
		return 0
	}
	return nowMs() + ms(d)
}

// startWire starts a run from an encoded input (version 0: the newest).
func (e *Engine) startWire(ctx context.Context, wtype string, version uint32, id string, input []byte, o StartOptions) error {
	policy, err := idPolicy(o.IfRunning, o.IfFinished)
	if err != nil {
		return err
	}
	dl := deadline(o.Timeout)
	_, err = e.request(ctx, false, func(tok uint64) error {
		v := o.Version
		if v == 0 {
			v = e.latest(wtype, version)
		}
		_, err := e.n.start(startReq{id: id, wtype: wtype, input: input, version: v, policy: policy, deadline: dl, token: int64(tok)})
		return err
	})
	return err
}

// updateWithStartWire starts the run if needed and delivers an update in
// the same record; returns the handler's encoded result.
func (e *Engine) updateWithStartWire(ctx context.Context, wtype string, version uint32, id string, input []byte, update string, arg []byte, o StartOptions) ([]byte, error) {
	running := o.IfRunning
	if running == "" || running == "fail" {
		running = "use-existing"
	}
	policy, err := idPolicy(running, o.IfFinished)
	if err != nil {
		return nil, err
	}
	dl := deadline(o.Timeout)
	return e.request(ctx, true, func(tok uint64) error {
		v := o.Version
		if v == 0 {
			v = e.latest(wtype, version)
		}
		_, err := e.n.start(startReq{id: id, wtype: wtype, input: input, version: v, policy: policy, deadline: dl, token: -1,
			update: true, updateName: update, updateID: o.UpdateID, updateArg: arg, updateToken: int64(tok)})
		return err
	})
}

// Start starts a run; it returns once the start is durable.
func (w *Workflow[In, Out]) Start(ctx context.Context, id string, in In, opts ...StartOptions) (*Run[Out], error) {
	input, err := encode(in)
	if err != nil {
		return nil, err
	}
	var o StartOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	if err := w.e.startWire(ctx, w.Name, w.Version, id, input, o); err != nil {
		return nil, err
	}
	return &Run[Out]{e: w.e, ID: id}, nil
}

// UpdateWithStart starts the workflow if needed (IfRunning defaults to
// "use-existing") and delivers an update in the same record; it returns
// the run and the update handler's result.
func UpdateWithStart[R, In, Out any](ctx context.Context, w *Workflow[In, Out], id string, in In, update string, arg any, opts ...StartOptions) (*Run[Out], R, error) {
	var zero R
	input, err := encode(in)
	if err != nil {
		return nil, zero, err
	}
	a, err := encode(arg)
	if err != nil {
		return nil, zero, err
	}
	var o StartOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	p, err := w.e.updateWithStartWire(ctx, w.Name, w.Version, id, input, update, a, o)
	if err != nil {
		return nil, zero, err
	}
	r, err := decode[R](p)
	return &Run[Out]{e: w.e, ID: id}, r, err
}

// DebounceOptions configure a debounced start.
type DebounceOptions struct {
	// Starts inside the window (after the latest start) collapse into one
	// run, which begins with the latest start's input. Required.
	Window time.Duration
	// The run begins at most this long after the first start (zero: the
	// default).
	MaxWait time.Duration
	// How long an event ID deduplicates after the key's last event (zero:
	// ten minutes).
	DedupWindow time.Duration
	// The event's ID: a retried call with the same ID joins once.
	ID string
}

// BatchOptions configure a batched event.
type BatchOptions struct {
	// The batch closes at this many events (zero: 100) or MaxWait after
	// its first (zero: one second).
	MaxEvents   int
	MaxWait     time.Duration
	DedupWindow time.Duration
	ID          string
}

const (
	eventUpdate = "fusee.event"
	endedText   = "the workflow ended before the update finished"
)

var workflowFailed = regexp.MustCompile(`^(?:workflow failed: )+`)

// untilTaken sends an event until a coordinator takes it (one that was
// ending refused it); returns the receipt's run ID.
func untilTaken(coordinator string, send func() ([]byte, error)) (string, error) {
	for i := 0; i < 100; i++ {
		p, err := send()
		if err == nil {
			var receipt struct {
				ID string `json:"id"`
			}
			if err := json.Unmarshal(p, &receipt); err != nil {
				return "", err
			}
			return receipt.ID, nil
		}
		var fe *Error
		if !errors.As(err, &fe) || workflowFailed.ReplaceAllString(fe.Message, "") != endedText {
			return "", err
		}
	}
	return "", &Error{Code: "Failed", Message: fmt.Sprintf("coordinator '%s' kept ending before taking the event", coordinator)}
}

func nanOr(d time.Duration) float64 {
	if d <= 0 {
		return math.NaN()
	}
	return float64(ms(d))
}

func queueEvent(kind uint32, workflow, key, eventID string, value any, a, b, c float64) ([4][]byte, error) {
	if _, isBytes := value.([]byte); isBytes {
		what := "batched event"
		if kind == 0 {
			what = "debounced input"
		}
		return [4][]byte{}, &Error{Code: "Invalid", Message: fmt.Sprintf("a %s is JSON, not bytes", what)}
	}
	input, err := json.Marshal(value)
	if err != nil {
		return [4][]byte{}, err
	}
	return queueRequest(kind, workflow, key, eventID, input, a, b, c)
}

func (e *Engine) enqueue(ctx context.Context, q [4][]byte, eventID string) (string, error) {
	wtype, coordinator := string(q[0]), string(q[1])
	return untilTaken(coordinator, func() ([]byte, error) {
		return e.updateWithStartWire(ctx, wtype, 1, coordinator, q[2], eventUpdate, q[3],
			StartOptions{IfRunning: "use-existing", Version: 1, UpdateID: eventID})
	})
}

// Debounce debounces a start under key. It returns once this start is
// durable, with the handle of the run it joined; the run exists once the
// window closes (Run.Started).
func (w *Workflow[In, Out]) Debounce(ctx context.Context, key string, in In, o DebounceOptions) (*Run[Out], error) {
	if o.Window <= 0 {
		return nil, &Error{Code: "Invalid", Message: "debounce needs a window"}
	}
	q, err := queueEvent(0, w.Name, key, o.ID, in, float64(ms(o.Window)), nanOr(o.MaxWait), nanOr(o.DedupWindow))
	if err != nil {
		return nil, err
	}
	id, err := w.e.enqueue(ctx, q, o.ID)
	if err != nil {
		return nil, err
	}
	return &Run[Out]{e: w.e, ID: id}, nil
}

// Batch adds an event to the batch under key: once the batch is full or
// its wait passed, one run starts with all its events, in the order they
// were accepted, as its input (a list). It returns once the event is
// durable, with the handle of that run.
func (w *Workflow[In, Out]) Batch(ctx context.Context, key string, event any, o BatchOptions) (*Run[Out], error) {
	a := math.NaN()
	if o.MaxEvents > 0 {
		a = float64(o.MaxEvents)
	}
	q, err := queueEvent(1, w.Name, key, o.ID, event, a, nanOr(o.MaxWait), nanOr(o.DedupWindow))
	if err != nil {
		return nil, err
	}
	id, err := w.e.enqueue(ctx, q, o.ID)
	if err != nil {
		return nil, err
	}
	return &Run[Out]{e: w.e, ID: id}, nil
}

// Run is a handle to the run of a workflow ID (following continue-as-new
// and reset).
type Run[Out any] struct {
	e  *Engine
	ID string
}

// RunOf is a typed handle to the run of a workflow ID.
func RunOf[Out any](e *Engine, id string) *Run[Out] { return &Run[Out]{e: e, ID: id} }

// Signal sends a signal; it returns once the signal is durable.
func (r *Run[Out]) Signal(ctx context.Context, name string, value any) error {
	data, err := encode(value)
	if err != nil {
		return err
	}
	_, err = r.e.request(ctx, false, func(tok uint64) error { return r.e.n.signal(r.ID, name, data, tok) })
	return err
}

// Result waits for the run's result.
func (r *Run[Out]) Result(ctx context.Context) (Out, error) {
	var zero Out
	status, payload, err := r.e.result(ctx, r.ID)
	if err != nil {
		return zero, err
	}
	if err := settled(status, payload); err != nil {
		return zero, err
	}
	return decode[Out](payload)
}

// Started waits until a run with this ID exists (a debounced or batched
// run starts when its window closes).
func (r *Run[Out]) Started(ctx context.Context) error {
	pause := time.Millisecond
	for {
		ok, err := r.e.exists(r.ID)
		if err != nil || ok {
			return err
		}
		select {
		case <-time.After(pause):
		case <-ctxDone(ctx):
			return ctx.Err()
		}
		pause = min(pause*2, 50*time.Millisecond)
	}
}

// UpdateOptions configure an update.
type UpdateOptions struct {
	// A retried call with the same ID gets the recorded result.
	ID string
}

// Update sends an update and waits for its handler's durable result.
func (r *Run[Out]) Update(ctx context.Context, name string, arg any, opts ...UpdateOptions) (json.RawMessage, error) {
	data, err := encode(arg)
	if err != nil {
		return nil, err
	}
	var uid string
	if len(opts) > 0 {
		uid = opts[0].ID
	}
	p, err := r.e.request(ctx, true, func(tok uint64) error { return r.e.n.update(r.ID, name, uid, data, tok) })
	if err != nil {
		return nil, err
	}
	return raw(p), nil
}

// Query calls a query handler; the answer waits until the state it read
// is durable.
func (r *Run[Out]) Query(ctx context.Context, name string, arg any) (json.RawMessage, error) {
	return r.e.query(ctx, r.ID, name, arg)
}

// Update sends an update to a run and decodes its result.
func Update[R, Out any](ctx context.Context, r *Run[Out], name string, arg any, opts ...UpdateOptions) (R, error) {
	var zero R
	p, err := r.Update(ctx, name, arg, opts...)
	if err != nil {
		return zero, err
	}
	return decode[R](p)
}

// Query calls a run's query handler and decodes its answer.
func Query[R, Out any](ctx context.Context, r *Run[Out], name string, arg any) (R, error) {
	var zero R
	p, err := r.Query(ctx, name, arg)
	if err != nil {
		return zero, err
	}
	return decode[R](p)
}

// Cancel asks the run to cancel: its pending operations fail with a
// CancelledError, and its code may compensate.
func (r *Run[Out]) Cancel(ctx context.Context, reason string) error {
	_, err := r.e.request(ctx, false, func(tok uint64) error { return r.e.n.cancel(r.ID, reason, false, tok) })
	return err
}

// Resume resumes a run that paused because a step ran out of retries
// (RetryPolicy.OnExhausted Pause): the step runs again at once, with a
// fresh set of attempts, on the code this process has now. A
// *RejectedError if nothing is paused.
func (r *Run[Out]) Resume(ctx context.Context) error {
	_, err := r.e.request(ctx, false, func(tok uint64) error { return r.e.n.resume(r.ID, tok) })
	return err
}

// Terminate ends the run at once; children follow their parent-close
// policy.
func (r *Run[Out]) Terminate(ctx context.Context, reason string) error {
	_, err := r.e.request(ctx, false, func(tok uint64) error { return r.e.n.cancel(r.ID, reason, true, tok) })
	return err
}

// ResetOptions configure a reset.
type ResetOptions struct {
	// Signals after the cut are delivered again (default true).
	DropSignals bool
	Version     uint32
}

// Reset starts a new run whose journal is this one's cut before the step
// beforeStep.
func (r *Run[Out]) Reset(ctx context.Context, beforeStep string, opts ...ResetOptions) error {
	var o ResetOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := r.e.request(ctx, false, func(tok uint64) error {
		return r.e.n.reset(r.ID, beforeStep, !o.DropSignals, o.Version, tok)
	})
	return err
}

// Inspect is the run's summary and normalized journal, or nil.
func (r *Run[Out]) Inspect(ctx context.Context) (*RunDetail, error) {
	var out []byte
	var found bool
	err := r.e.do(func() error {
		j, f, err := r.e.n.inspect(r.ID)
		out, found = j, f
		return err
	})
	if err != nil || !found {
		return nil, err
	}
	var d RunDetail
	return &d, json.Unmarshal(out, &d)
}
