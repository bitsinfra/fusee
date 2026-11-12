package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Keyed objects (docs/features.md#objects-keyed-state): an object type is a
// set of handlers, and each key of the type is one object with its own
// key/value state. A key is a long-lived run with workflow ID
// "<type>/<key>"; each call is an update of it. The engine orders the calls
// (exclusive ones one at a time per key, shared ones at once), keeps the
// state next to the run and commits an exclusive handler's writes with its
// result, and compacts the key's history; this file is the code a key runs.

const (
	// The query every object answers with its committed state.
	stateQuery = "fusee.state"
	// Update IDs of one-way calls start with this: their callers are
	// answered once the call is accepted.
	sendPrefix = "fusee.send/"
	// The handler every object type has that edits its state from outside.
	editHandler = "fusee.state.edit"
)

// StateEdit is an edit of a key's state from outside its handlers,
// applied in this order: ClearAll, Clear, Set (a nil value removes the
// field), Patch (a JSON Patch, RFC 6902, over the state as one object).
// IfVersion refuses the edit unless the state is still at that version
// (RunDetail.Object's version).
type StateEdit struct {
	ClearAll  bool             `json:"clearAll,omitempty"`
	Clear     []string         `json:"clear,omitempty"`
	Set       map[string]any   `json:"set,omitempty"`
	Patch     []map[string]any `json:"patch,omitempty"`
	IfVersion string           `json:"ifVersion,omitempty"`
}

// EditResult is what a state edit answers: the state's new version and
// the fields it changed.
type EditResult struct {
	Version string   `json:"version"`
	Changed []string `json:"changed"`
}

// editStateHandler applies an edit as the exclusive handler running: the
// engine makes the writes.
var editStateHandler = &updateHandler{handle: func(c *Ctx, p []byte) (any, error) {
	x := c.x
	_, data, err := x.e.n.host(x.shard, x.run, opObjectEdit, 0, 0, 0, bin(p), str(""), str(""))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(append([]byte(nil), data...)), nil
}}

func editResult(p json.RawMessage, err error) (*EditResult, error) {
	if err != nil {
		return nil, err
	}
	var r EditResult
	if err := json.Unmarshal(p, &r); err != nil {
		return nil, err
	}
	return &r, nil
}

// Handler is one handler of an object type: Exclusive or Shared.
type Handler struct {
	shared bool
	fn     func(c *ObjectCtx, arg []byte) (any, error)
}

func objectHandler[A, R any](shared bool, fn func(c *ObjectCtx, arg A) (R, error)) Handler {
	return Handler{shared: shared, fn: func(c *ObjectCtx, p []byte) (any, error) {
		a, err := decode[A](p)
		if err != nil {
			return nil, err
		}
		return fn(c, a)
	}}
}

// Exclusive is a handler that runs one call at a time per key, in the order
// the calls reach the key, and may change its state. Its writes take effect
// for everyone else when it returns (with its result, durable together); a
// handler that fails changes nothing.
func Exclusive[A, R any](fn func(c *ObjectCtx, arg A) (R, error)) Handler {
	return objectHandler(false, fn)
}

// Shared is a handler that runs as soon as its call arrives, next to other
// calls (an exclusive one included), and only reads the committed state.
func Shared[A, R any](fn func(c *ObjectCtx, arg A) (R, error)) Handler {
	return objectHandler(true, fn)
}

// Handlers are an object type's handlers by name.
type Handlers map[string]Handler

// ObjectType is a registered object type.
type ObjectType struct {
	e       *Engine
	Name    string
	Version uint32
}

func checkType(name string) error {
	if name == "" || strings.Contains(name, "/") {
		return &RejectedError{Reason: fmt.Sprintf("an object type's name is not empty and has no '/': '%s'", name)}
	}
	return nil
}

// RegisterObject registers an object type and its handlers (register it
// right after the engine opens: a call from workflow code to a type no
// process registered waits until one does). Keys whose runs were waiting for
// this code resume. A new version (RegisterOptions.Version) takes over each
// key at its next compaction.
func RegisterObject(e *Engine, name string, handlers Handlers, opts ...RegisterOptions) (*ObjectType, error) {
	if err := checkType(name); err != nil {
		return nil, err
	}
	version := uint32(1)
	if len(opts) > 0 && opts[0].Version > 0 {
		version = opts[0].Version
	}
	var shared []string
	table := make(map[string]*updateHandler, len(handlers))
	for n, h := range handlers {
		if h.fn == nil {
			return nil, &Error{Code: "Invalid", Message: fmt.Sprintf("handler '%s' of '%s' is not fusee.Exclusive or fusee.Shared", n, name)}
		}
		if h.shared {
			shared = append(shared, n)
		}
		table[n] = h.update()
	}
	table[editHandler] = editStateHandler
	err := e.do(func() error {
		if err := e.n.registerObject(name, shared); err != nil {
			return err
		}
		e.registerHolding(name, version, objectMain(table))
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &ObjectType{e: e, Name: name, Version: version}, nil
}

// update is the handler as the run's update handler.
func (h Handler) update() *updateHandler {
	exclusive := !h.shared
	fn := h.fn
	return &updateHandler{handle: func(c *Ctx, p []byte) (any, error) {
		return fn(&ObjectCtx{Ctx: c, exclusive: exclusive}, p)
	}}
}

// objectMain is an object type's code: its handlers are the run's updates,
// looked up when a call is delivered, and the state query. It never returns
// (the engine compacts the key's run).
func objectMain(handlers map[string]*updateHandler) workflowFn {
	return func(c *Ctx, _ []byte) (any, error) {
		x := c.x
		x.object = handlers
		x.queries[stateQuery] = func([]byte) ([]byte, error) {
			if x.ended {
				return nil, errEnded
			}
			_, data, err := x.e.n.host(x.shard, x.run, opObjectState, 0, 0, 0, str(""), str(""), str(""))
			if err != nil {
				return nil, x.note(err)
			}
			return append([]byte(nil), data...), nil
		}
		never := &op{kind: kTask, seq: -1}
		for !x.ended {
			c.wait(never)
		}
		return nil, errEnded
	}
}

// Key is the object with key key.
func (t *ObjectType) Key(key string) *ObjectRef { return t.e.Object(t.Name, key) }

// ObjectCtx is what an object's handler gets: the workflow context of the
// key's run (every Ctx operation works: steps, sleeps, children, calls to
// other objects), its state and its key. Pass c.Ctx to the package's
// functions (fusee.Step(c.Ctx, ...)).
type ObjectCtx struct {
	*Ctx
	exclusive bool
}

// ObjectKey is the object's key.
func (c *ObjectCtx) ObjectKey() string { return c.x.id[min(len(c.x.wtype)+1, len(c.x.id)):] }

// IsExclusive reports whether the handler is exclusive (it may change state).
func (c *ObjectCtx) IsExclusive() bool { return c.exclusive }

// State is the key's state. In an exclusive handler it includes the
// handler's own writes; in a shared one it is the committed state.
func (c *ObjectCtx) State() *State { return &State{x: c.x, exclusive: c.exclusive} }

// State is a key's state as a handler sees it: JSON values under string
// keys. Reads are synchronous and not journaled (the engine holds the state
// next to the run); writes are an exclusive handler's own until it returns.
// Use it from the handler's workflow code (and code it starts with Go).
type State struct {
	x         *runExec
	exclusive bool
}

func (s *State) host(code uint32, n0 uint64, a, b []byte) ([5]uint64, []byte, error) {
	x := s.x
	if r := x.e.running; r == nil || r.x != x {
		panic("fusee: an object's state was used outside its handler's workflow code")
	}
	if x.ended {
		return [5]uint64{}, nil, errEnded
	}
	r, data, err := x.e.n.host(x.shard, x.run, code, n0, 0, 0, bin(a), bin(b), str(""))
	return r, data, x.note(err)
}

func flag(b bool) uint64 {
	if b {
		return 1
	}
	return 0
}

// Raw is a value's JSON as recorded, or nil when the key holds none.
func (s *State) Raw(key string) (json.RawMessage, error) {
	r, data, err := s.host(opObjectGet, flag(s.exclusive), []byte(key), nil)
	if err != nil || r[0] == 0 {
		return nil, err
	}
	return append(json.RawMessage(nil), data...), nil
}

// Get decodes a value into v (as json.Unmarshal); false when the key holds
// none (v is left as it is).
func (s *State) Get(key string, v any) (bool, error) {
	p, err := s.Raw(key)
	if err != nil || p == nil {
		return false, err
	}
	return true, json.Unmarshal(p, v)
}

// StateGet is a value of the state as a T; false (and T's zero value) when
// the key holds none.
func StateGet[T any](s *State, key string) (T, bool, error) {
	var zero T
	p, err := s.Raw(key)
	if err != nil || p == nil {
		return zero, false, err
	}
	v, err := decode[T](p)
	return v, err == nil, err
}

func (s *State) write(key string, value []byte, set bool) error {
	if !s.exclusive {
		return &RejectedError{Reason: "a shared handler cannot change state"}
	}
	_, _, err := s.host(opObjectSet, flag(set), []byte(key), value)
	return err
}

// Set sets a value (JSON; nil, or anything that encodes as null, clears the
// key). Only an exclusive handler may; the change is committed when it
// returns.
func (s *State) Set(key string, value any) error {
	if _, isBytes := value.([]byte); isBytes {
		return &Error{Code: "Invalid", Message: "state values are JSON, not bytes"}
	}
	p, err := encode(value)
	if err != nil {
		return err
	}
	if string(p) == "null" {
		return s.write(key, nil, false)
	}
	return s.write(key, p, true)
}

// Clear clears a key.
func (s *State) Clear(key string) error { return s.write(key, nil, false) }

// ClearAll clears every key.
func (s *State) ClearAll() error {
	keys, err := s.Keys()
	if err != nil {
		return err
	}
	for _, k := range keys {
		if err := s.Clear(k); err != nil {
			return err
		}
	}
	return nil
}

// Keys are the keys that hold a value, sorted.
func (s *State) Keys() ([]string, error) {
	_, data, err := s.host(opObjectKeys, flag(s.exclusive), nil, nil)
	if err != nil {
		return nil, err
	}
	keys := []string{}
	if len(data) > 0 {
		err = json.Unmarshal(data, &keys)
	}
	return keys, err
}

// ---------------------------------------------------------- from workflow code

// ObjectCall is a key of an object type called from workflow code (or from
// another object's handler): Ctx.Object.
type ObjectCall struct {
	c           *Ctx
	wtype, key  string
	targetError error
}

// Object is key key of object type wtype, called from this workflow code:
// each call is a journal operation, sent through the durable outbox and run
// exactly once, and its result is recorded.
func (c *Ctx) Object(wtype, key string) *ObjectCall {
	return &ObjectCall{c: c, wtype: wtype, key: key, targetError: checkType(wtype)}
}

func (o *ObjectCall) begin(handler string, arg any, oneWay bool) (uint32, error) {
	if o.targetError != nil {
		return 0, o.targetError
	}
	data, err := encode(arg)
	if err != nil {
		return 0, err
	}
	r, _, err := o.c.host(opBeginCall, flag(oneWay), 0, 0, []byte(o.wtype+"/"+o.key), []byte(handler), data)
	o.c.x.e.kicked = true
	return uint32(r[0]), err
}

// StartCall calls a handler without waiting: its future is the handler's
// result.
func StartCall[R any](o *ObjectCall, handler string, arg any) *Future[R] {
	seq, err := o.begin(handler, arg, false)
	if err != nil {
		return &Future[R]{failedOp(err)}
	}
	return &Future[R]{o.c.newOp(kChild, o.wtype+"."+handler, seq)}
}

// Call calls a handler and waits for its result.
func Call[R any](o *ObjectCall, handler string, arg any) (R, error) {
	return StartCall[R](o, handler, arg).Get(o.c)
}

// Call calls a handler and waits for its result, as JSON.
func (o *ObjectCall) Call(handler string, arg any) (json.RawMessage, error) {
	return Call[json.RawMessage](o, handler, arg)
}

// Send calls a handler without waiting for it: the outbox delivers the call
// once this point of the journal is durable, and it runs once.
func (o *ObjectCall) Send(handler string, arg any) error {
	_, err := o.begin(handler, arg, true)
	return err
}

// --------------------------------------------------------- from outside

// CallOptions configure a call from outside workflow code.
type CallOptions struct {
	// An idempotency ID: calls with the same ID run once, and each gets the
	// recorded result (a key remembers them across compaction for an hour,
	// its latest 1,024). Without one a retry is a new call.
	ID string
}

func callID(opts []CallOptions) string {
	if len(opts) > 0 {
		return opts[0].ID
	}
	return ""
}

// Caller is a key that can be called from outside workflow code: an
// ObjectRef, or a RemoteObjectRef through a server.
type Caller interface {
	Call(ctx context.Context, handler string, arg any, opts ...CallOptions) (json.RawMessage, error)
}

// CallRef calls a handler of a key from outside workflow code and decodes
// its result.
func CallRef[R any](ctx context.Context, k Caller, handler string, arg any, opts ...CallOptions) (R, error) {
	var zero R
	p, err := k.Call(ctx, handler, arg, opts...)
	if err != nil {
		return zero, err
	}
	return decode[R](p)
}

// ObjectRef is a key of an object type, called from outside workflow code.
type ObjectRef struct {
	e         *Engine
	Type, Key string
}

// Object is key key of object type wtype.
func (e *Engine) Object(wtype, key string) *ObjectRef { return &ObjectRef{e: e, Type: wtype, Key: key} }

// ID is the key's workflow ID ("<type>/<key>").
func (r *ObjectRef) ID() string { return r.Type + "/" + r.Key }

func (r *ObjectRef) send(ctx context.Context, handler string, arg any, id string) ([]byte, error) {
	if err := checkType(r.Type); err != nil {
		return nil, err
	}
	data, err := encode(arg)
	if err != nil {
		return nil, err
	}
	return r.e.updateWithStartWire(ctx, r.Type, 1, r.ID(), []byte("null"), handler, data,
		StartOptions{IfRunning: IfRunningUseExisting, UpdateID: id})
}

// Call calls a handler and waits for its durable result (as JSON).
func (r *ObjectRef) Call(ctx context.Context, handler string, arg any, opts ...CallOptions) (json.RawMessage, error) {
	p, err := r.send(ctx, handler, arg, callID(opts))
	if err != nil {
		return nil, err
	}
	return raw(p), nil
}

// Send calls a handler without waiting for its result: it returns once the
// call is accepted (durable, and its handler started).
func (r *ObjectRef) Send(ctx context.Context, handler string, arg any, opts ...CallOptions) error {
	_, err := r.send(ctx, handler, arg, sendPrefix+callID(opts))
	return err
}

// EditState edits the state from outside its handlers: an exclusive call,
// so it waits for the key and commits as a handler's writes do.
func (r *ObjectRef) EditState(ctx context.Context, edit StateEdit, opts ...CallOptions) (*EditResult, error) {
	return editResult(r.Call(ctx, editHandler, edit, opts...))
}

// SetState sets one field of the state.
func (r *ObjectRef) SetState(ctx context.Context, field string, value any) (*EditResult, error) {
	return r.EditState(ctx, StateEdit{Set: map[string]any{field: value}})
}

// PatchState applies a JSON Patch (RFC 6902) to the state as one object.
func (r *ObjectRef) PatchState(ctx context.Context, ops []map[string]any) (*EditResult, error) {
	return r.EditState(ctx, StateEdit{Patch: ops})
}

// ClearState clears one field, or (field "") the whole state; the key
// remains.
func (r *ObjectRef) ClearState(ctx context.Context, field string) (*EditResult, error) {
	if field == "" {
		return r.EditState(ctx, StateEdit{ClearAll: true})
	}
	return r.EditState(ctx, StateEdit{Clear: []string{field}})
}

// emptyState is the state of a key never called (or deleted).
var emptyState = json.RawMessage("{}")

func notFound(err error) bool {
	var fe *Error
	return errors.As(err, &fe) && (fe.Code == "NotFound" || strings.Contains(strings.ToLower(fe.Message), "not found"))
}

// State is the committed state, a JSON object ({} for a key never called),
// answered once it is durable.
func (r *ObjectRef) State(ctx context.Context) (json.RawMessage, error) {
	p, err := r.e.query(ctx, r.ID(), stateQuery, nil)
	if notFound(err) {
		return emptyState, nil
	}
	return p, err
}

// Inspect is the key's run: its journal, and in Object its state and
// pending calls (running and waiting); nil for a key never called.
func (r *ObjectRef) Inspect(ctx context.Context) (*RunDetail, error) {
	return r.e.Run(r.ID()).Inspect(ctx)
}

// Delete deletes the key: its run ends and its state with it. The next
// call starts with empty state.
func (r *ObjectRef) Delete(ctx context.Context) error {
	err := r.e.Run(r.ID()).Terminate(ctx, "object deleted")
	if notFound(err) {
		return nil
	}
	return err
}

// Objects are the keys of object type wtype (up to limit per shard; zero:
// 1,000).
func (e *Engine) Objects(ctx context.Context, wtype string, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 1000
	}
	runs, err := e.List(ctx, ListOptions{Limit: limit, Where: map[string]any{"type": wtype}})
	if err != nil {
		return nil, err
	}
	keys := []string{}
	for _, s := range runs {
		if s.Object && s.FinishedMs == nil && strings.HasPrefix(s.ID, wtype+"/") {
			keys = append(keys, s.ID[len(wtype)+1:])
		}
	}
	return keys, nil
}

// ObjectDetail is what Inspect adds for a key of an object type.
type ObjectDetail struct {
	// The committed state, a JSON object.
	State json.RawMessage `json:"state"`
	// Accepted calls whose handlers have not returned.
	Running []PendingCall `json:"running"`
	// Exclusive calls waiting for the key, in arrival order.
	Waiting []PendingCall `json:"waiting"`
}

// PendingCall is a call to a key that has not finished.
type PendingCall struct {
	Handler string `json:"handler"`
	ID      string `json:"id"`
}

// ---------------------------------------------------------- through a server

// Object is key key of object type wtype, called through the server.
func (c *Client) Object(wtype, key string) *RemoteObjectRef {
	return &RemoteObjectRef{c: c, Type: wtype, Key: key}
}

// RemoteObjectRef is a key of an object type reached through a server.
type RemoteObjectRef struct {
	c         *Client
	Type, Key string
}

// ID is the key's workflow ID ("<type>/<key>").
func (r *RemoteObjectRef) ID() string { return r.Type + "/" + r.Key }

// Call calls a handler and waits for its durable result.
func (r *RemoteObjectRef) Call(ctx context.Context, handler string, arg any, opts ...CallOptions) (json.RawMessage, error) {
	return r.c.UpdateWithStart(ctx, r.Type, r.ID(), nil, handler, arg, StartOptions{UpdateID: callID(opts)})
}

// Send calls a handler without waiting for its result: it returns once the
// call is accepted.
func (r *RemoteObjectRef) Send(ctx context.Context, handler string, arg any, opts ...CallOptions) error {
	_, err := r.c.UpdateWithStart(ctx, r.Type, r.ID(), nil, handler, arg, StartOptions{UpdateID: sendPrefix + callID(opts)})
	return err
}

// EditState edits the state from outside its handlers (see
// ObjectRef.EditState).
func (r *RemoteObjectRef) EditState(ctx context.Context, edit StateEdit, opts ...CallOptions) (*EditResult, error) {
	return editResult(r.c.Call(ctx, "editState", map[string]any{"id": r.ID(), "edit": edit, "updateId": callID(opts)}))
}

// SetState sets one field of the state.
func (r *RemoteObjectRef) SetState(ctx context.Context, field string, value any) (*EditResult, error) {
	return r.EditState(ctx, StateEdit{Set: map[string]any{field: value}})
}

// PatchState applies a JSON Patch (RFC 6902) to the state as one object.
func (r *RemoteObjectRef) PatchState(ctx context.Context, ops []map[string]any) (*EditResult, error) {
	return r.EditState(ctx, StateEdit{Patch: ops})
}

// ClearState clears one field, or (field "") the whole state.
func (r *RemoteObjectRef) ClearState(ctx context.Context, field string) (*EditResult, error) {
	if field == "" {
		return r.EditState(ctx, StateEdit{ClearAll: true})
	}
	return r.EditState(ctx, StateEdit{Clear: []string{field}})
}

// State is the committed state ({} for a key never called).
func (r *RemoteObjectRef) State(ctx context.Context) (json.RawMessage, error) {
	p, err := r.c.Call(ctx, "query", map[string]any{"id": r.ID(), "name": stateQuery, "arg": nil})
	if notFound(err) {
		return emptyState, nil
	}
	return p, err
}
