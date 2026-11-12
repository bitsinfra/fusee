package fusee

import (
	"context"
	"encoding/json"
	"math"
	"sync"
	"time"
)

// Client sends calls through a Fusee server to whichever worker holds the
// workflow's shards, in any language. Safe for concurrent use.
type Client struct {
	n      nativeClient
	mu     sync.RWMutex
	closed bool
}

// Connect connects to a server: host:port, unix:/path.sock or
// replicas://..., with options such as ?ca=ca.pem&token-file=token.
func Connect(address string) (*Client, error) {
	n, err := connectNative(address)
	if err != nil {
		return nil, err
	}
	return &Client{n: n}, nil
}

// Call sends a protocol call and waits for the worker's answer.
func (c *Client) Call(ctx context.Context, method string, args any) (json.RawMessage, error) {
	b, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	type answer struct {
		v   []byte
		err error
	}
	ch := make(chan answer, 1)
	go func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.closed {
			ch <- answer{err: ErrClosed}
			return
		}
		v, err := c.n.call(method, b)
		ch <- answer{v, err}
	}()
	select {
	case a := <-ch:
		return a.v, a.err
	case <-ctxDone(ctx):
		return nil, ctx.Err()
	}
}

// Close closes the connection, failing calls in progress.
func (c *Client) Close() error {
	c.n.shutdown()
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.closed {
		c.closed = true
		c.n.free()
	}
	return nil
}

func remoteOptions(o StartOptions) map[string]any {
	var timeout any
	if o.Timeout > 0 {
		timeout = ms(o.Timeout)
	}
	running, finished := o.IfRunning, o.IfFinished
	if running == "" {
		running = "fail"
	}
	if finished == "" {
		finished = "allow"
	}
	return map[string]any{"timeout": timeout, "idPolicy": map[string]any{"running": running, "finished": finished}}
}

func versionOrNil(v uint32) any {
	if v == 0 {
		return nil
	}
	return v
}

// Start starts a run of workflow type wtype.
func (c *Client) Start(ctx context.Context, wtype, id string, input any, opts ...StartOptions) (*RemoteRun, error) {
	var o StartOptions
	if len(opts) > 0 {
		o = opts[0]
	}
	_, err := c.Call(ctx, "start", map[string]any{"type": wtype, "id": id, "input": input, "version": versionOrNil(o.Version), "options": remoteOptions(o)})
	if err != nil {
		return nil, err
	}
	return c.Run(id), nil
}

// UpdateWithStart starts the workflow if needed and delivers an update in
// the same record; returns the handler's result.
func (c *Client) UpdateWithStart(ctx context.Context, wtype, id string, input any, update string, arg any, opts ...StartOptions) (json.RawMessage, error) {
	o := StartOptions{IfRunning: IfRunningUseExisting}
	if len(opts) > 0 {
		o = opts[0]
		if o.IfRunning == "" {
			o.IfRunning = IfRunningUseExisting
		}
	}
	return c.Call(ctx, "updateWithStart", map[string]any{"type": wtype, "id": id, "input": input, "name": update,
		"arg": arg, "updateId": o.UpdateID, "version": versionOrNil(o.Version), "options": remoteOptions(o)})
}

func (c *Client) enqueue(ctx context.Context, q [4][]byte, eventID string) (*RemoteRun, error) {
	args := map[string]any{"type": string(q[0]), "id": string(q[1]), "input": json.RawMessage(q[2]), "name": eventUpdate,
		"arg": json.RawMessage(q[3]), "updateId": eventID, "version": 1,
		"options": map[string]any{"timeout": nil, "idPolicy": map[string]any{"running": "use-existing", "finished": "allow"}}}
	id, err := untilTaken(string(q[1]), func() ([]byte, error) { return c.Call(ctx, "updateWithStart", args) })
	if err != nil {
		return nil, err
	}
	return c.Run(id), nil
}

// Debounce debounces a start of workflow type wtype under key (see
// Workflow.Debounce).
func (c *Client) Debounce(ctx context.Context, wtype, key string, input any, o DebounceOptions) (*RemoteRun, error) {
	if o.Window <= 0 {
		return nil, &Error{Code: "Invalid", Message: "debounce needs a window"}
	}
	q, err := queueEvent(0, wtype, key, o.ID, input, float64(ms(o.Window)), nanOr(o.MaxWait), nanOr(o.DedupWindow))
	if err != nil {
		return nil, err
	}
	return c.enqueue(ctx, q, o.ID)
}

// Batch adds an event to the batch of workflow type wtype under key (see
// Workflow.Batch).
func (c *Client) Batch(ctx context.Context, wtype, key string, event any, o BatchOptions) (*RemoteRun, error) {
	a := math.NaN()
	if o.MaxEvents > 0 {
		a = float64(o.MaxEvents)
	}
	q, err := queueEvent(1, wtype, key, o.ID, event, a, nanOr(o.MaxWait), nanOr(o.DedupWindow))
	if err != nil {
		return nil, err
	}
	return c.enqueue(ctx, q, o.ID)
}

// List lists runs across the server's shards.
func (c *Client) List(ctx context.Context, opts ListOptions) ([]RunSummary, error) {
	if opts.Limit <= 0 {
		opts.Limit = 100
	}
	where := opts.Where
	if where == nil {
		where = map[string]any{}
	}
	p, err := c.Call(ctx, "list", map[string]any{"limit": opts.Limit, "where": where})
	if err != nil {
		return nil, err
	}
	var runs []RunSummary
	return runs, json.Unmarshal(p, &runs)
}

// Complete finishes an async completion.
func (c *Client) Complete(ctx context.Context, token string, value any) error {
	_, err := c.Call(ctx, "complete", map[string]any{"token": token, "value": value})
	return err
}

// Fail fails an async completion.
func (c *Client) Fail(ctx context.Context, token, message string) error {
	_, err := c.Call(ctx, "fail", map[string]any{"token": token, "error": message})
	return err
}

// Run is a handle to a run through the server.
func (c *Client) Run(id string) *RemoteRun { return &RemoteRun{c: c, ID: id} }

// RemoteRun is a run reached through a server.
type RemoteRun struct {
	c  *Client
	ID string
}

// Result waits for the run's result.
func (r *RemoteRun) Result(ctx context.Context) (json.RawMessage, error) {
	return r.c.Call(ctx, "result", map[string]any{"id": r.ID})
}

// Started waits until a run with this ID exists.
func (r *RemoteRun) Started(ctx context.Context) error {
	pause := time.Millisecond
	for {
		runs, err := r.c.List(ctx, ListOptions{Limit: 1, Where: map[string]any{"id": r.ID}})
		if err != nil || len(runs) > 0 {
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

func (r *RemoteRun) Signal(ctx context.Context, name string, value any) error {
	_, err := r.c.Call(ctx, "signal", map[string]any{"id": r.ID, "name": name, "value": value})
	return err
}

func (r *RemoteRun) Query(ctx context.Context, name string, arg any) (json.RawMessage, error) {
	return r.c.Call(ctx, "query", map[string]any{"id": r.ID, "name": name, "arg": arg})
}

func (r *RemoteRun) Update(ctx context.Context, name string, arg any, opts ...UpdateOptions) (json.RawMessage, error) {
	var id string
	if len(opts) > 0 {
		id = opts[0].ID
	}
	return r.c.Call(ctx, "update", map[string]any{"id": r.ID, "name": name, "arg": arg, "updateId": id})
}

func (r *RemoteRun) Cancel(ctx context.Context, reason string) error {
	_, err := r.c.Call(ctx, "cancel", map[string]any{"id": r.ID, "reason": reason})
	return err
}

// Resume resumes a paused run: its step runs again with a fresh set of
// attempts.
func (r *RemoteRun) Resume(ctx context.Context) error {
	_, err := r.c.Call(ctx, "resume", map[string]any{"id": r.ID})
	return err
}

func (r *RemoteRun) Terminate(ctx context.Context, reason string) error {
	_, err := r.c.Call(ctx, "terminate", map[string]any{"id": r.ID, "reason": reason})
	return err
}

func (r *RemoteRun) Reset(ctx context.Context, beforeStep string, reapplySignals bool) error {
	_, err := r.c.Call(ctx, "reset", map[string]any{"id": r.ID, "beforeStep": beforeStep, "reapplySignals": reapplySignals})
	return err
}
