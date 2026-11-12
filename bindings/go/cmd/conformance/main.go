// Conformance runner (Go SDK): the scenarios of conformance/README.md,
// printed as each run's normalized journal for conformance/compare.py.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"slices"
	"sync"
	"time"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

type out map[string]json.RawMessage

var ctx = context.Background()

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// pause: see pause in crates/fusee/examples/conformance.rs.
func pause(e *fusee.Engine, o out) {
	calls := 0
	w := fusee.Register(e, "pause", func(c *fusee.Ctx, _ any) (string, error) {
		return fusee.Step(c, "flaky", func(context.Context) (string, error) {
			calls++
			if calls < 3 {
				return "", fmt.Errorf("try %d", calls)
			}
			return "ok", nil
		}, fusee.StepOptions{Inline: true, Retry: &fusee.RetryPolicy{Attempts: 2, Initial: time.Millisecond,
			Max: time.Millisecond, OnExhausted: fusee.Pause}})
	})
	_, err := w.Start(ctx, "pause", nil)
	must(err)
	until(e, "pause", "paused")
	must(e.Run("pause").Resume(ctx))
	journal(e, o, "pause", "pause")
}

func journal(e *fusee.Engine, o out, name, id string) {
	_, _ = e.Run(id).Result(ctx)
	d, err := e.Run(id).Inspect(ctx)
	must(err)
	if d == nil {
		log.Fatalf("no run %s", id)
	}
	o[name] = d.Journal
}

// until waits for a run to reach one of the states.
func until(e *fusee.Engine, id string, states ...string) {
	for i := 0; i < 2000; i++ {
		if d, _ := e.Run(id).Inspect(ctx); d != nil {
			for _, s := range states {
				if d.Status == s {
					return
				}
			}
		}
		time.Sleep(time.Millisecond)
	}
	log.Fatalf("%s never reached %v", id, states)
}

// The Python scenarios count attempts per workflow ID.
type tries struct {
	mu sync.Mutex
	n  map[string]int
}

func (t *tries) add(id string) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.n[id]++
	return t.n[id]
}

type seqIn struct {
	N int `json:"n"`
}

func main() {
	e, err := fusee.Open(fusee.Options{Durability: "memory", Shards: 2, KeepHistory: true})
	must(err)
	o := out{}
	basics(e, o)
	features(e, o)
	streams(e, o)
	queues(e, o)
	pause(e, o)
	objects(o)
	b, err := json.MarshalIndent(o, "", "  ")
	must(err)
	_, _ = os.Stdout.Write(append(b, '\n'))
	must(e.Close())
}

func basics(e *fusee.Engine, o out) {
	r3 := &fusee.RetryPolicy{Attempts: 3, Initial: time.Millisecond, Max: time.Millisecond}
	t := &tries{n: map[string]int{}}

	seq := fusee.Register(e, "seq", func(c *fusee.Ctx, in seqIn) (map[string]int, error) {
		a, err := fusee.Step(c, "a", func(context.Context) (int, error) { return in.N + 1, nil })
		if err != nil {
			return nil, err
		}
		b, err := fusee.Step(c, "b", func(context.Context) (int, error) { return a * 2, nil })
		if err != nil {
			return nil, err
		}
		return fusee.Step(c, "c", func(context.Context) (map[string]int, error) { return map[string]int{"a": a, "b": b}, nil })
	})
	parallel := fusee.Register(e, "parallel", func(c *fusee.Ctx, n int) (int, error) {
		// Step functions run on their own goroutines unless inline, so two
		// parallel steps record in the order they finish; inline, they run
		// in the order they start, as in the other SDKs.
		in := fusee.StepOptions{Inline: true}
		x := fusee.StartStep(c, "x", func(context.Context) (int, error) { return n, nil }, in)
		y := fusee.StartStep(c, "y", func(context.Context) (int, error) { return n * 10, nil }, in)
		if err := fusee.All(c, x, y); err != nil {
			return 0, err
		}
		xv, _ := x.Get(c)
		yv, _ := y.Get(c)
		return fusee.Step(c, "z", func(context.Context) (int, error) { return xv + yv, nil })
	})
	retry := fusee.Register(e, "retry", func(c *fusee.Ctx, _ any) (string, error) {
		id := c.WorkflowID()
		return fusee.Step(c, "flaky", func(context.Context) (string, error) {
			if n := t.add(id); n < 3 {
				return "", fmt.Errorf("try %d", n)
			}
			return "ok", nil
		}, fusee.StepOptions{Retry: r3})
	})
	readOnly := fusee.Register(e, "read_only", func(c *fusee.Ctx, _ any) (string, error) {
		id := c.WorkflowID()
		v, err := fusee.Step(c, "fetch", func(context.Context) (string, error) {
			if n := t.add(id); n < 2 {
				return "", fmt.Errorf("try %d", n)
			}
			return "data", nil
		}, fusee.StepOptions{Retry: r3, ReadOnly: true})
		if err != nil {
			return "", err
		}
		return fusee.Step(c, "store", func(context.Context) (string, error) { return v + "!", nil })
	})
	signal := fusee.Register(e, "signal", func(c *fusee.Ctx, _ any) (string, error) {
		v, _, err := fusee.WaitFor[string](c, "go", 0)
		if err != nil {
			return "", err
		}
		return fusee.Step(c, "use", func(context.Context) (string, error) { return v + "!", nil })
	})
	fusee.Register(e, "double", func(c *fusee.Ctx, x int) (int, error) {
		return fusee.Step(c, "mul", func(context.Context) (int, error) { return x * 2, nil })
	})
	child := fusee.Register(e, "child", func(c *fusee.Ctx, _ any) (int, error) {
		return fusee.Child[int](c, "double", c.WorkflowID()+"/c", 21)
	})
	sleep := fusee.Register(e, "sleep", func(c *fusee.Ctx, _ any) (string, error) {
		if err := c.Sleep("nap", time.Millisecond); err != nil {
			return "", err
		}
		return fusee.Step(c, "after", func(context.Context) (string, error) { return "woke", nil })
	})
	values := fusee.Register(e, "values", func(c *fusee.Ctx, _ any) (string, error) {
		c.Now()
		c.Random()
		c.UUID()
		return "values", nil
	})
	cancel := fusee.Register(e, "cancel", func(c *fusee.Ctx, _ any) (string, error) {
		v, _, err := fusee.WaitFor[string](c, "never", 0)
		var ce *fusee.CancelledError
		if errors.As(err, &ce) {
			if _, err := fusee.Step(c, "undo", func(context.Context) (string, error) { return ce.Reason, nil }); err != nil {
				return "", err
			}
			return "", fusee.Cancelled("undone: " + ce.Reason)
		}
		return v, err
	})
	fail := fusee.Register(e, "fail", func(c *fusee.Ctx, _ any) (string, error) {
		_, err := fusee.Step(c, "boom", func(context.Context) (string, error) { return "", errors.New("boom") },
			fusee.StepOptions{Retry: &fusee.RetryPolicy{Attempts: 1}})
		var se *fusee.StepError
		if !errors.As(err, &se) {
			return "unexpected", nil
		}
		return fusee.Step(c, "recover", func(context.Context) (string, error) { return "recovered", nil })
	})
	listener := fusee.Register(e, "listener", func(c *fusee.Ctx, _ any) (string, error) {
		v, _, err := fusee.WaitFor[string](c, "ping", 0)
		return v, err
	})
	send := fusee.Register(e, "send", func(c *fusee.Ctx, target string) (string, error) {
		return "sent", c.Signal(target, "ping", "hello")
	})
	timeoutWait := fusee.Register(e, "timeout_wait", func(c *fusee.Ctx, _ any) (string, error) {
		_, ok, err := fusee.WaitFor[string](c, "never", 5*time.Millisecond)
		if err != nil {
			return "", err
		}
		return fusee.Step(c, "after", func(context.Context) (string, error) {
			if ok {
				return "some", nil
			}
			return "none", nil
		})
	})

	_, err := seq.Start(ctx, "seq", seqIn{N: 3})
	must(err)
	_, err = parallel.Start(ctx, "parallel", 5)
	must(err)
	_, err = retry.Start(ctx, "retry", nil)
	must(err)
	_, err = readOnly.Start(ctx, "read_only", nil)
	must(err)
	_, err = signal.Start(ctx, "signal", nil)
	must(err)
	must(e.Run("signal").Signal(ctx, "go", "hi"))
	_, err = child.Start(ctx, "child", nil)
	must(err)
	_, err = sleep.Start(ctx, "sleep", nil)
	must(err)
	_, err = values.Start(ctx, "values", nil)
	must(err)
	_, err = cancel.Start(ctx, "cancel", nil)
	must(err)
	_, err = fail.Start(ctx, "fail", nil)
	must(err)
	_, err = listener.Start(ctx, "listener", nil)
	must(err)
	_, err = send.Start(ctx, "send", "listener")
	must(err)
	_, err = timeoutWait.Start(ctx, "timeout_wait", nil)
	must(err)
	for i := 0; i < 100; i++ {
		if d, _ := e.Run("cancel").Inspect(ctx); d != nil && (d.Status == "suspended" || d.Status == "parked") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	must(e.Run("cancel").Cancel(ctx, "stop"))
	for _, id := range []string{"seq", "parallel", "retry", "read_only", "signal", "child", "child/c", "sleep", "values",
		"cancel", "fail", "listener", "send", "timeout_wait"} {
		journal(e, o, id, id)
	}
}

func features(e *fusee.Engine, o out) {
	updates := fusee.Register(e, "updates", func(c *fusee.Ctx, _ any) (int, error) {
		total := 0
		fusee.OnUpdate(c, "add", func(c *fusee.Ctx, v int) (int, error) {
			d, err := fusee.Step(c, "apply", func(context.Context) (int, error) { return v * 2, nil })
			if err != nil {
				return 0, err
			}
			total += d
			return total, nil
		}, func(v int) error {
			if v < 0 {
				return errors.New("negative")
			}
			return nil
		})
		fusee.OnQuery(c, "total", func(any) (int, error) { return total, nil })
		if _, _, err := fusee.WaitFor[any](c, "done", 0); err != nil {
			return 0, err
		}
		return total, nil
	})
	updateStart := fusee.Register(e, "update_start", func(c *fusee.Ctx, start int) (int, error) {
		n := start
		fusee.OnUpdate(c, "inc", func(c *fusee.Ctx, by int) (int, error) {
			n += by
			return n, nil
		}, nil)
		if _, _, err := fusee.WaitFor[any](c, "stop", 0); err != nil {
			return 0, err
		}
		return n, nil
	})
	heartbeat := fusee.Register(e, "heartbeat", func(c *fusee.Ctx, _ any) (int, error) {
		return fusee.Step(c, "work", func(ctx context.Context) (int, error) {
			fusee.Heartbeat(ctx, 1)
			time.Sleep(30 * time.Millisecond)
			v, _, err := fusee.Progress[int](ctx)
			return v, err
		})
	})
	tokens := make(chan string, 1)
	await := fusee.Register(e, "await", func(c *fusee.Ctx, _ any) (string, error) {
		return fusee.AwaitCompletion[string](c, "approval", func(_ context.Context, token string) error {
			select {
			case tokens <- token:
			default:
			}
			return nil
		})
	})
	continued := fusee.Register(e, "continued", func(c *fusee.Ctx, left int) (int, error) {
		v, err := fusee.Step(c, "work", func(context.Context) (int, error) { return left, nil })
		if err != nil || left == 0 {
			return v, err
		}
		return 0, c.ContinueAsNew(left - 1)
	})
	patched := fusee.Register(e, "patched", func(c *fusee.Ctx, _ any) (string, error) {
		c.DeprecatePatch("old")
		label := "old"
		if c.Patched("new") {
			label = "new"
		}
		return fusee.Step(c, "after", func(context.Context) (string, error) { return label, nil })
	})
	attributes := fusee.Register(e, "attributes", func(c *fusee.Ctx, _ any) (string, error) {
		if err := c.SetAttributes(map[string]any{"customer": "c-1", "amount": 5}); err != nil {
			return "", err
		}
		return "attributes", c.SetAttributes(map[string]any{"stage": "paid"})
	})
	fusee.Register(e, "kid", func(c *fusee.Ctx, _ any) (string, error) {
		v, _, err := fusee.WaitFor[string](c, "go", 0)
		return v, err
	})
	parentClose := fusee.Register(e, "parent_close", func(c *fusee.Ctx, _ any) (string, error) {
		if err := c.SpawnChild("kid", "parent_close/terminate", nil); err != nil {
			return "", err
		}
		if err := c.SpawnChild("kid", "parent_close/abandon", nil, fusee.ChildOptions{ParentClose: "abandon"}); err != nil {
			return "", err
		}
		_, _, err := fusee.WaitFor[any](c, "never", 0)
		return "unreachable", err
	})
	reset := fusee.Register(e, "reset", func(c *fusee.Ctx, _ any) (int, error) {
		a, err := fusee.Step(c, "a", func(context.Context) (int, error) { return 1, nil })
		if err != nil {
			return 0, err
		}
		b, err := fusee.Step(c, "b", func(context.Context) (int, error) { return a + 1, nil })
		if err != nil {
			return 0, err
		}
		return fusee.Step(c, "c", func(context.Context) (int, error) { return b + 1, nil })
	})
	victim := fusee.Register(e, "victim", func(c *fusee.Ctx, _ any) (string, error) {
		_, _, err := fusee.WaitFor[any](c, "never", 0)
		var ce *fusee.CancelledError
		if !errors.As(err, &ce) {
			return "unexpected", err
		}
		if _, err := fusee.Step(c, "undo", func(context.Context) (string, error) { return ce.Reason, nil }); err != nil {
			return "", err
		}
		return "", fusee.Cancelled("undone")
	})
	canceller := fusee.Register(e, "canceller", func(c *fusee.Ctx, _ any) (string, error) {
		return "sent", c.CancelWorkflow("victim", "stop it")
	})
	raceWait := fusee.Register(e, "race_wait", func(c *fusee.Ctx, _ any) (string, error) {
		if _, err := fusee.Race(c, fusee.StartWait[string](c, "x", 0), c.StartSleep("nap", time.Millisecond)); err != nil {
			return "", err
		}
		v, _, err := fusee.WaitFor[string](c, "x", 0)
		return v, err
	})

	_, err := updates.Start(ctx, "updates", nil, fusee.StartOptions{IfRunning: fusee.IfRunningUseExisting})
	must(err)
	up := fusee.RunOf[int](e, "updates")
	a, err := fusee.Update[int](ctx, up, "add", 3, fusee.UpdateOptions{ID: "u1"})
	must(err)
	b, err := fusee.Update[int](ctx, up, "add", 3, fusee.UpdateOptions{ID: "u1"})
	must(err)
	_, rerr := fusee.Update[int](ctx, up, "add", -1)
	total, err := fusee.Query[int](ctx, up, "total", nil)
	must(err)
	if a != 6 || b != 6 || rerr == nil || total != 6 {
		log.Fatalf("updates: %d %d %v %d", a, b, rerr, total)
	}
	must(up.Signal(ctx, "done", nil))
	_, r, err := fusee.UpdateWithStart[int](ctx, updateStart, "update_start", 10, "inc", 5)
	must(err)
	if r != 15 {
		log.Fatalf("update with start: %d", r)
	}
	must(e.Run("update_start").Signal(ctx, "stop", nil))
	_, err = heartbeat.Start(ctx, "heartbeat", nil)
	must(err)
	_, err = await.Start(ctx, "await", nil)
	must(err)
	token := <-tokens
	until(e, "await", "suspended", "parked")
	must(e.Complete(ctx, token, "approved"))
	_, err = continued.Start(ctx, "continued", 2)
	must(err)
	_, err = patched.Start(ctx, "patched", nil)
	must(err)
	_, err = attributes.Start(ctx, "attributes", nil)
	must(err)
	_, err = parentClose.Start(ctx, "parent_close", nil)
	must(err)
	until(e, "parent_close/terminate", "suspended", "parked")
	until(e, "parent_close/abandon", "suspended", "parked")
	must(e.Run("parent_close").Terminate(ctx, "stop"))
	until(e, "parent_close/terminate", "terminated")
	must(e.Run("parent_close/abandon").Signal(ctx, "go", "free"))
	_, err = reset.Start(ctx, "reset", nil)
	must(err)
	_, _ = e.Run("reset").Result(ctx)
	must(e.Run("reset").Reset(ctx, "b"))
	_, err = victim.Start(ctx, "victim", nil)
	must(err)
	until(e, "victim", "suspended", "parked")
	_, err = canceller.Start(ctx, "canceller", nil)
	must(err)
	_, err = raceWait.Start(ctx, "race_wait", nil)
	must(err)
	for i := 0; i < 2000; i++ {
		d, _ := e.Run("race_wait").Inspect(ctx)
		var j []json.RawMessage
		if d != nil && json.Unmarshal(d.Journal, &j) == nil && len(j) >= 6 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	must(e.Run("race_wait").Signal(ctx, "x", "late"))
	for _, id := range []string{"updates", "update_start", "heartbeat", "await", "continued", "patched", "attributes",
		"parent_close", "parent_close/terminate", "parent_close/abandon", "reset", "victim", "canceller", "race_wait"} {
		journal(e, o, id, id)
	}
}

// Streams: a workflow's own stream (appended once, closed by the code), a
// step's stream (JSON and bytes chunks, ended by the step's result), and a
// step whose retry continues after what its failed attempt recorded. The
// reader's view of the workflow's stream is compared too.
func streams(e *fusee.Engine, o out) {
	r3 := &fusee.RetryPolicy{Attempts: 3, Initial: time.Millisecond, Max: time.Millisecond}
	fusee.Register(e, "streams", func(c *fusee.Ctx, _ any) (string, error) {
		p, err := c.Stream("progress")
		if err != nil {
			return "", err
		}
		if err := p.Append("start"); err != nil {
			return "", err
		}
		text, err := fusee.Step(c, "generate", func(ctx context.Context) (string, error) {
			out, err := fusee.Stream(ctx, "tokens")
			if err != nil {
				return "", err
			}
			for _, t := range []string{"a", "b", "c"} {
				if err := out.Append(t); err != nil {
					return "", err
				}
			}
			return "abc", out.Append([]byte{1, 2})
		})
		if err != nil {
			return "", err
		}
		if err := p.Append(map[string]string{"text": text}); err != nil {
			return "", err
		}
		return text, p.Close()
	})
	fusee.Register(e, "stream_retry", func(c *fusee.Ctx, _ any) (int, error) {
		return fusee.Step(c, "generate", func(ctx context.Context) (int, error) {
			out, err := fusee.Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			prev, err := fusee.Recorded[string](out)
			if err != nil {
				return 0, err
			}
			if len(prev) == 0 {
				_ = out.Append("x")
				_ = out.Append("y")
				return 0, errors.New("cut off")
			}
			return len(prev) + 1, out.Append("z")
		}, fusee.StepOptions{Retry: r3})
	})
	for _, id := range []string{"streams", "stream_retry"} {
		_, err := fusee.WorkflowOf[any, any](e, id).Start(ctx, id, nil)
		must(err)
		journal(e, o, id, id)
	}
	r := e.Run("streams").Stream("progress", 0)
	chunks := []json.RawMessage{}
	for c, err := range r.All(ctx) {
		must(err)
		chunks = append(chunks, c.JSON)
	}
	b, err := json.Marshal(map[string]any{"chunks": chunks, "status": r.Status()})
	must(err)
	o["streams/read"] = b
}

type prioIn struct {
	Level  int32  `json:"level"`
	Key    string `json:"key"`
	Weight uint32 `json:"weight"`
}

func queues(e *fusee.Engine, o out) {
	must(e.Limit("conf", fusee.LimitOptions{Concurrent: 1}))
	prio := fusee.Register(e, "prio", func(c *fusee.Ctx, p prioIn) (string, error) {
		a, err := fusee.Step(c, "first", func(context.Context) (int32, error) { return p.Level, nil },
			fusee.StepOptions{Limit: "conf", Priority: p.Level, FairnessKey: p.Key, FairnessWeight: p.Weight})
		if err != nil {
			return "", err
		}
		return fusee.Step(c, "second", func(context.Context) (string, error) { return fmt.Sprintf("%s:%d", p.Key, a), nil },
			fusee.StepOptions{Limit: "conf", Priority: -p.Level, FairnessKey: "other"})
	})
	deb := fusee.Register(e, "deb_target", func(c *fusee.Ctx, v any) (any, error) {
		return fusee.Step(c, "got", func(context.Context) (any, error) { return v, nil })
	})
	bat := fusee.Register(e, "batch_target", func(c *fusee.Ctx, events []any) ([]any, error) {
		return fusee.Step(c, "got", func(context.Context) ([]any, error) { return events, nil })
	})
	var ids []string
	for i := int32(0); i < 4; i++ {
		key := "even"
		if i%2 == 1 {
			key = "odd"
		}
		id := fmt.Sprintf("prio/%d", i)
		_, err := prio.Start(ctx, id, prioIn{Level: i, Key: key, Weight: uint32(i + 1)})
		must(err)
		ids = append(ids, id)
	}
	// Three starts in order inside the window: one run with the latest
	// input; a retried event ID joins once and its input is not used.
	w := fusee.DebounceOptions{Window: 500 * time.Millisecond}
	var runs []*fusee.Run[any]
	for n := 1; n <= 3; n++ {
		opt := w
		opt.ID = fmt.Sprintf("d%d", n)
		r, err := deb.Debounce(ctx, "k", n, opt)
		must(err)
		runs = append(runs, r)
	}
	again := w
	again.ID = "d2"
	r, err := deb.Debounce(ctx, "k", 99, again)
	must(err)
	for _, x := range append(runs, r) {
		if x.ID != runs[0].ID {
			log.Fatal("debounced starts joined different runs")
		}
	}
	must(runs[0].Started(ctx))
	journal(e, o, "debounce/run", runs[0].ID)
	var batch []*fusee.Run[[]any]
	for _, ev := range [][2]string{{"b1", "a"}, {"b2", "b"}, {"b2", "x"}, {"b3", "c"}} {
		r, err := bat.Batch(ctx, "k", ev[1], fusee.BatchOptions{MaxEvents: 3, MaxWait: time.Minute, ID: ev[0]})
		must(err)
		batch = append(batch, r)
	}
	for _, x := range batch {
		if x.ID != batch[0].ID {
			log.Fatal("batched events joined different runs")
		}
	}
	must(batch[0].Started(ctx))
	journal(e, o, "batch/full", batch[0].ID)
	timed, err := bat.Batch(ctx, "t", "z", fusee.BatchOptions{MaxEvents: 100, MaxWait: 20 * time.Millisecond})
	must(err)
	must(timed.Started(ctx))
	journal(e, o, "batch/timed", timed.ID)
	d, err := e.Run("fusee.debounce/deb_target/k").Query(ctx, "describe", nil)
	must(err)
	o["debounce/describe"] = d
	d, err = e.Run("fusee.batch/batch_target/k").Query(ctx, "describe", nil)
	must(err)
	o["batch/describe"] = d
	for _, id := range ids {
		journal(e, o, id, id)
	}
}

// inspected is a run's journal now (a key's run never finishes).
func inspected(e *fusee.Engine, o out, name, id string) {
	d, err := e.Run(id).Inspect(ctx)
	must(err)
	if d == nil {
		log.Fatalf("no run %s", id)
	}
	o[name] = d.Journal
}

// Keyed objects: exclusive calls with a step, a retried call ID, a failed
// call that changes nothing, a shared read, a one-way call, calls from a
// workflow (one awaited, one sent), clearing, and a key compacted after 128
// operations. On an engine that never parks, so a compaction happens only
// where the scenario makes it.
func objects(o out) {
	e, err := fusee.Open(fusee.Options{Durability: "memory", Shards: 2, KeepHistory: true, ParkAfter: time.Hour})
	must(err)
	items := func(s *fusee.State) []string {
		v, _, err := fusee.StateGet[[]string](s, "items")
		must(err)
		return v
	}
	_, err = fusee.RegisterObject(e, "conf_cart", fusee.Handlers{
		"add": fusee.Exclusive(func(c *fusee.ObjectCtx, item string) (int, error) {
			priced, err := fusee.Step(c.Ctx, "price", func(context.Context) (string, error) { return item + "$", nil })
			if err != nil {
				return 0, err
			}
			all := append(items(c.State()), priced)
			must(c.State().Set("items", all))
			must(c.State().Set("last", all[len(all)-1]))
			return len(all), nil
		}),
		"remove": fusee.Exclusive(func(c *fusee.ObjectCtx, item string) (int, error) {
			all := items(c.State())
			must(c.State().Set("touched", true))
			at := slices.Index(all, item)
			if at < 0 {
				return 0, fmt.Errorf("missing %s", item)
			}
			all = slices.Delete(all, at, at+1)
			must(c.State().Set("items", all))
			return len(all), nil
		}),
		"get": fusee.Shared(func(c *fusee.ObjectCtx, _ any) (map[string]any, error) {
			v, err := c.State().Raw("items")
			if err != nil {
				return nil, err
			}
			keys, err := c.State().Keys()
			// No items: null, as in the other SDKs (a nil RawMessage is null).
			return map[string]any{"items": v, "keys": keys}, err
		}),
		"clear": fusee.Exclusive(func(c *fusee.ObjectCtx, _ any) (int, error) {
			must(c.State().ClearAll())
			keys, err := c.State().Keys()
			return len(keys), err
		}),
	})
	must(err)
	_, err = fusee.RegisterObject(e, "conf_tally", fusee.Handlers{
		"add": fusee.Exclusive(func(c *fusee.ObjectCtx, n int64) (int64, error) {
			sum, _, err := fusee.StateGet[int64](c.State(), "sum")
			if err != nil {
				return 0, err
			}
			sum += n
			return sum, c.State().Set("sum", sum)
		}),
	})
	must(err)
	caller := fusee.Register(e, "obj_caller", func(c *fusee.Ctx, key string) (int64, error) {
		n, err := fusee.Call[int64](c.Object("conf_cart", key), "add", "d")
		if err != nil {
			return 0, err
		}
		return n, c.Object("conf_cart", key).Send("add", "e")
	})
	k := e.Object("conf_cart", "k1")
	var calls []json.RawMessage
	call := func(handler string, arg any, id string) {
		v, err := k.Call(ctx, handler, arg, fusee.CallOptions{ID: id})
		if err != nil {
			v = json.RawMessage(`{"error":true}`)
		}
		calls = append(calls, v)
	}
	call("add", "a", "c1")
	call("add", "b", "c2")
	call("add", "b", "c2")
	call("remove", "zz", "c3")
	call("get", nil, "c4")
	must(k.Send(ctx, "add", "c", fusee.CallOptions{ID: "s1"}))
	run, err := caller.Start(ctx, "obj_caller", "k1")
	must(err)
	n, err := run.Result(ctx)
	must(err)
	b, err := json.Marshal(n)
	must(err)
	calls = append(calls, b)
	for i := 0; i < 2000; i++ {
		st, err := k.State(ctx)
		must(err)
		var m struct {
			Items []any `json:"items"`
		}
		if json.Unmarshal(st, &m) == nil && len(m.Items) == 5 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	call("get", nil, "c5")
	call("clear", nil, "c6")
	call("add", "f", "c7")
	call("fusee.state.edit", map[string]any{"set": map[string]any{"owner": "ana"},
		"patch": []map[string]any{{"op": "add", "path": "/items/-", "value": "g"}}}, "c8")
	st, err := k.State(ctx)
	must(err)
	calls = append(calls, st)
	b, err = json.Marshal(calls)
	must(err)
	o["obj/calls"] = b
	inspected(e, o, "obj/cart", k.ID())
	inspected(e, o, "obj/caller", "obj_caller")
	t := e.Object("conf_tally", "t")
	var sums []int64
	for n := int64(1); n <= 130; n++ {
		s, err := fusee.CallRef[int64](ctx, t, "add", n, fusee.CallOptions{ID: fmt.Sprintf("t-%d", n)})
		must(err)
		sums = append(sums, s)
	}
	// A retry from before the compaction gets its recorded result.
	s, err := fusee.CallRef[int64](ctx, t, "add", 5, fusee.CallOptions{ID: "t-5"})
	must(err)
	sums = append(sums, s)
	b, err = json.Marshal([]int64{sums[0], sums[127], sums[129], sums[130]})
	must(err)
	o["obj/sums"] = b
	o["obj/tally_state"], err = t.State(ctx)
	must(err)
	inspected(e, o, "obj/tally", t.ID())
	must(e.Close())
}
