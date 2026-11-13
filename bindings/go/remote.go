package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Server mode: calls a Client sent through the server reach the worker
// holding the shards, here, and are answered with the engine's own API.

type relayedCall struct {
	Relay  string          `json:"relay"`
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}

type remoteArgs struct {
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Version  uint32          `json:"version"`
	Input    json.RawMessage `json:"input"`
	Name     string          `json:"name"`
	Arg      json.RawMessage `json:"arg"`
	Value    json.RawMessage `json:"value"`
	UpdateID string          `json:"updateId"`
	Token    string          `json:"token"`
	Error    string          `json:"error"`
	Reason   string          `json:"reason"`
	Before   string          `json:"beforeStep"`
	Reapply  *bool           `json:"reapplySignals"`
	Limit    int             `json:"limit"`
	Where    map[string]any  `json:"where"`
	Run      *uint64         `json:"run"`
	From     uint64          `json:"from"`
	WaitMs   *uint64         `json:"waitMs"`
	Options  struct {
		Timeout  *float64 `json:"timeout"`
		IDPolicy struct {
			Running  string `json:"running"`
			Finished string `json:"finished"`
		} `json:"idPolicy"`
	} `json:"options"`
}

// The longest a relayed stream read waits for a chunk, as the other SDKs.
const maxRemoteWaitMs = 60_000

func orNull(v json.RawMessage) json.RawMessage {
	if len(v) == 0 {
		return json.RawMessage("null")
	}
	return v
}

func (e *Engine) handleRemote(msg []byte) {
	var c relayedCall
	if json.Unmarshal(msg, &c) != nil {
		return
	}
	relay, err := strconv.ParseUint(c.Relay, 10, 64)
	if err != nil {
		return
	}
	value, err := e.serveCall(c.Method, c.Args)
	var response []byte
	if err != nil {
		msg := err.Error()
		// A step's, child's or workflow's failure goes as its envelope, so
		// the caller gets the same error, cause and all.
		var (
			we *WorkflowError
			se *StepError
			fe *Failure
		)
		switch {
		case errors.As(err, &we) && we.code() == "WorkflowFailed" && we.Cause != nil:
			// A run's (or update's) failure is the one its code recorded.
			msg = string(encodeFailure(we.Cause))
		case errors.As(err, &we) || errors.As(err, &se) || errors.As(err, &fe):
			msg = string(encodeFailure(err))
		}
		response, _ = json.Marshal(map[string]any{"error": msg, "kind": kindOf(err)})
	} else {
		response, _ = json.Marshal(map[string]any{"value": orNull(value)})
	}
	_ = e.do(func() error {
		if err := e.n.replyCall(relay, response); err != nil {
			e.stopAfterFailure(err)
		}
		return nil
	})
}

func (e *Engine) serveCall(method string, args json.RawMessage) (json.RawMessage, error) {
	var a remoteArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, err
	}
	ctx := context.Background()
	run := e.Run(a.ID)
	o := StartOptions{Version: a.Version, IfRunning: a.Options.IDPolicy.Running, IfFinished: a.Options.IDPolicy.Finished}
	if a.Options.Timeout != nil {
		o.Timeout = time.Duration(*a.Options.Timeout) * time.Millisecond
	}
	switch method {
	case "start":
		return nil, e.startWire(ctx, a.Type, 1, a.ID, orNull(a.Input), o)
	case "updateWithStart":
		o.UpdateID = a.UpdateID
		p, err := e.updateWithStartWire(ctx, a.Type, 1, a.ID, orNull(a.Input), a.Name, orNull(a.Arg), o)
		return raw(p), err
	case "signal":
		return nil, run.Signal(ctx, a.Name, orNull(a.Value))
	case "query":
		return run.Query(ctx, a.Name, orNull(a.Arg))
	case "update":
		return run.Update(ctx, a.Name, orNull(a.Arg), UpdateOptions{ID: a.UpdateID})
	case "result":
		return run.Result(ctx)
	case "complete":
		return nil, e.Complete(ctx, a.Token, orNull(a.Value))
	case "fail":
		return nil, e.Fail(ctx, a.Token, a.Error)
	case "cancel":
		return nil, run.Cancel(ctx, a.Reason)
	case "resume":
		return nil, run.Resume(ctx)
	case "terminate":
		return nil, run.Terminate(ctx, a.Reason)
	case "reset":
		reapply := a.Reapply == nil || *a.Reapply
		return nil, run.Reset(ctx, a.Before, ResetOptions{DropSignals: !reapply, Version: a.Version})
	case "signalWithStart":
		w := WorkflowOf[json.RawMessage, json.RawMessage](e, a.Type)
		_, err := SignalWithStart(ctx, w, a.ID, orNull(a.Input), a.Name, orNull(a.Value), o)
		return nil, err
	case "describe":
		d, err := run.Inspect(ctx)
		if err != nil {
			return nil, err
		}
		if d == nil {
			return nil, &Error{Code: "NotFound", Message: "workflow '" + a.ID + "' not found"}
		}
		out, err := json.Marshal(d.RunSummary)
		if err != nil {
			return nil, err
		}
		var m map[string]any
		_ = json.Unmarshal(out, &m)
		m["journal"] = d.Journal
		m["objectDetail"] = d.Object
		return json.Marshal(m)
	case "types":
		type typeInfo struct {
			Name     string   `json:"name"`
			Versions []uint32 `json:"versions"`
			Object   bool     `json:"object"`
		}
		var types []typeInfo
		_ = e.do(func() error {
			for name, vs := range e.types {
				if strings.HasPrefix(name, "fusee.") {
					continue
				}
				t := typeInfo{Name: name, Versions: []uint32{}}
				for v := range vs {
					t.Versions = append(t.Versions, v)
				}
				sort.Slice(t.Versions, func(i, j int) bool { return t.Versions[i] < t.Versions[j] })
				types = append(types, t)
			}
			return nil
		})
		sort.Slice(types, func(i, j int) bool { return types[i].Name < types[j].Name })
		return json.Marshal(types)
	case "info":
		return json.Marshal(map[string]any{"engine": "fusee", "sdk": "go", "shards": e.Shards()})
	case "metrics":
		total, err := e.Metrics()
		if err != nil {
			return nil, err
		}
		shards, err := e.ShardMetrics()
		if err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"total": total, "shards": shards})
	case "limits":
		limits, err := e.Limits()
		if err != nil {
			return nil, err
		}
		return json.Marshal(limits)
	case "list":
		runs, err := e.List(ctx, ListOptions{Limit: a.Limit, Where: a.Where})
		if err != nil {
			return nil, err
		}
		return json.Marshal(runs)
	case "streamRead":
		// One read (a long poll of at most a minute): the reader's JSON.
		wait := uint64(ms(waitDefault))
		if a.WaitMs != nil {
			wait = min(*a.WaitMs, maxRemoteWaitMs)
		}
		run := int64(-1)
		if a.Run != nil {
			run = int64(*a.Run)
		}
		p, err := e.request(ctx, true, func(tok uint64) error { return e.n.streamRead(a.ID, run, a.Name, a.From, wait, tok) })
		if err != nil {
			return nil, err
		}
		return raw(p), nil
	}
	return nil, &Error{Code: "Failed", Message: "unknown remote call '" + method + "'"}
}
