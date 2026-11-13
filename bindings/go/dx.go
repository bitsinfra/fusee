package fusee

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// SignalWithStart signals the run id, starting it first with in if it is
// not running. The start and the signal reach the engine in one turn of its
// loop, so the signal is recorded before a new run's code first runs; it
// returns once both are durable.
func SignalWithStart[In, Out any](ctx context.Context, w *Workflow[In, Out], id string, in In, signal string, value any, opts ...StartOptions) (*Run[Out], error) {
	input, err := encode(in)
	if err != nil {
		return nil, err
	}
	data, err := encode(value)
	if err != nil {
		return nil, err
	}
	var o StartOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	running := o.IfRunning
	if running == "" || running == "fail" {
		running = "use-existing"
	}
	policy, err := idPolicy(running, o.IfFinished)
	if err != nil {
		return nil, err
	}
	dl := deadline(o.Timeout)
	e := w.e
	started, signalled := make(chan reply, 1), make(chan reply, 1)
	err = e.do(func() error {
		t1 := e.nextToken
		e.nextToken++
		e.tokens[t1] = pendingReply{started, false}
		v := o.Version
		if v == 0 {
			v = e.latest(w.Name, w.Version)
		}
		if _, err := e.n.start(startReq{id: id, wtype: w.Name, input: input, version: v, policy: policy, deadline: dl, token: int64(t1)}); err != nil {
			delete(e.tokens, t1)
			return err
		}
		t2 := e.nextToken
		e.nextToken++
		e.tokens[t2] = pendingReply{signalled, false}
		if err := e.n.signal(id, signal, data, t2); err != nil {
			delete(e.tokens, t2)
			return err
		}
		e.kicked = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	if _, err := wait(ctx, started); err != nil {
		return nil, err
	}
	if _, err := wait(ctx, signalled); err != nil {
		return nil, err
	}
	return &Run[Out]{e: e, ID: id}, nil
}

// Retry runs a failed run again from its failed step (a reset before it,
// with its signals).
func (r *Run[Out]) Retry(ctx context.Context) error {
	d, err := r.Inspect(ctx)
	if err != nil {
		return err
	}
	var journal []map[string]any
	if err := json.Unmarshal(d.Journal, &journal); err != nil {
		return err
	}
	for i := len(journal) - 1; i >= 0; i-- {
		j := journal[i]
		if j["kind"] == "step" && j["status"] == "failed" {
			name, _ := j["name"].(string)
			return r.Reset(ctx, name)
		}
	}
	return &RejectedError{Reason: fmt.Sprintf("workflow '%s' has no failed step to retry (status %s)", r.ID, d.Status)}
}

// SleepUntil waits for a durable timer that fires at at; it survives
// restarts.
func (c *Ctx) SleepUntil(name string, at time.Time) error {
	r, _, err := c.host(opBeginSleep, uint64(at.UnixMilli()), 0, 0, []byte(name))
	if err != nil {
		return err
	}
	_, err = (&Future[struct{}]{c.newOp(kSleep, name, uint32(r[0]))}).Get(c)
	return err
}

// Metrics are the engine's counters summed over its shards.
func (e *Engine) Metrics() (map[string]uint64, error) {
	shards, err := e.ShardMetrics()
	if err != nil {
		return nil, err
	}
	total := map[string]uint64{}
	for _, m := range shards {
		for k, v := range m {
			total[k] += v
		}
	}
	return total, nil
}

// LimitInfo is a limit and its permits now (Engine.Limits).
type LimitInfo struct {
	Name       string  `json:"name"`
	Concurrent uint32  `json:"concurrent"`
	PerSecond  float64 `json:"perSecond"`
	InUse      int     `json:"inUse"`
	Waiting    int     `json:"waiting"`
}

// Limits lists the limits this engine defined, by name, with their permits
// now.
func (e *Engine) Limits() ([]LimitInfo, error) {
	var specs map[string]LimitOptions
	if err := e.do(func() error {
		specs = make(map[string]LimitOptions, len(e.limitSpecs))
		for k, v := range e.limitSpecs {
			specs[k] = v
		}
		return nil
	}); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(specs))
	for n := range specs {
		names = append(names, n)
	}
	sort.Strings(names)
	out := make([]LimitInfo, 0, len(names))
	for _, n := range names {
		used, err := e.LimitInUse(n)
		if err != nil {
			return nil, err
		}
		waiting, err := e.LimitWaiting(n)
		if err != nil {
			return nil, err
		}
		s := specs[n]
		out = append(out, LimitInfo{Name: n, Concurrent: s.Concurrent, PerSecond: s.PerSecond, InUse: used, Waiting: waiting})
	}
	return out, nil
}

// On calls handler(workflowID, message) on the engine's loop when a run
// sticks (its code no longer matches its journal: "stuck") or when a step
// of this engine runs out of retries and pauses its run ("paused").
func (e *Engine) On(event string, handler func(workflowID, message string)) error {
	if event != "stuck" && event != "paused" {
		return &RejectedError{Reason: fmt.Sprintf("the events are: stuck, paused, not '%s'", event)}
	}
	return e.do(func() error {
		if event == "paused" {
			e.pausedHandlers = append(e.pausedHandlers, handler)
		} else {
			e.stuckHandlers = append(e.stuckHandlers, handler)
		}
		return nil
	})
}

// emitPaused tells the On("paused") handlers that a step of x ran out of
// retries and paused its run (loop only).
func (e *Engine) emitPaused(x *runExec, step string, f *Failure) {
	if len(e.pausedHandlers) == 0 {
		return
	}
	msg := fmt.Sprintf("step '%s' ran out of retries: %s", step, f.Message)
	for _, h := range e.pausedHandlers {
		h(x.id, msg)
	}
}

// Schedules is an engine's schedules (Engine.Schedules).
type Schedules struct{ e *Engine }

// Schedules gives Get and List.
func (e *Engine) Schedules() Schedules { return Schedules{e} }

// Get is a handle to the schedule name.
func (s Schedules) Get(name string) *Schedule { return s.e.ScheduleHandle(name) }

// List is every schedule that exists (its coordinator run has not ended).
func (s Schedules) List(ctx context.Context) ([]*Schedule, error) {
	rows, err := s.e.List(ctx, ListOptions{})
	if err != nil {
		return nil, err
	}
	var out []*Schedule
	for _, r := range rows {
		if name, ok := strings.CutPrefix(r.ID, "fusee.schedule/"); ok && r.FinishedMs == nil {
			out = append(out, s.e.ScheduleHandle(name))
		}
	}
	return out, nil
}

// Serve serves the HTTP API and the web UI at addr ("127.0.0.1:0" for any
// free port) on a thread of its own, answered by this engine; token, if
// given, is required as "Authorization: Bearer <token>" (and needed off
// loopback). Returns the URL it listens on.
func (e *Engine) Serve(addr string, token ...string) (string, error) {
	t := ""
	if len(token) > 0 {
		t = token[0]
	}
	var url string
	err := e.do(func() error {
		u, err := e.n.serve(addr, t)
		url = u
		e.serveRemote = true
		return err
	})
	return url, err
}
