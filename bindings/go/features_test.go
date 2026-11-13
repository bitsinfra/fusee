package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpdatesQueriesAndUpdateWithStart(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	counter := Register(e, "counter", func(c *Ctx, start int) (int, error) {
		n := start
		OnUpdate(c, "add", func(c *Ctx, by int) (int, error) {
			d, err := Step(c, "apply", func(context.Context) (int, error) { return by, nil })
			n += d
			return n, err
		}, func(by int) error {
			if by < 0 {
				return errors.New("negative")
			}
			return nil
		})
		OnQuery(c, "n", func(any) (int, error) { return n, nil })
		_, _, err := WaitFor[any](c, "stop", 0)
		return n, err
	})
	run, first, err := UpdateWithStart[int](ctx, counter, "c-1", 10, "add", 5, StartOptions{UpdateID: "u1"})
	if err != nil || first != 15 {
		t.Fatalf("%d %v", first, err)
	}
	again, err := Update[int](ctx, run, "add", 5, UpdateOptions{ID: "u1"})
	if err != nil || again != 15 {
		t.Fatalf("a repeated update ID: %d %v", again, err)
	}
	_, err = Update[int](ctx, run, "add", -1)
	var rj *RejectedError
	if !errors.As(err, &rj) || rj.Reason != "negative" {
		t.Fatalf("validator: %v", err)
	}
	_, err = Update[int](ctx, run, "nope", 1)
	if !errors.As(err, &rj) {
		t.Fatalf("no handler: %v", err)
	}
	n, err := Query[int](ctx, run, "n", nil)
	if err != nil || n != 15 {
		t.Fatalf("query %d %v", n, err)
	}
	if _, err := run.Query(ctx, "nope", nil); !errors.As(err, &rj) {
		t.Fatalf("no query handler: %v", err)
	}
	if err := run.Signal(ctx, "stop", nil); err != nil {
		t.Fatal(err)
	}
	if v, err := run.Result(ctx); err != nil || v != 15 {
		t.Fatalf("%d %v", v, err)
	}
}

func TestCancelTerminateAndStartPolicies(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var undone atomic.Bool
	wf := Register(e, "waiter", func(c *Ctx, _ any) (string, error) {
		_, _, err := WaitFor[any](c, "never", 0)
		var ce *CancelledError
		if errors.As(err, &ce) {
			_, _ = Step(c, "undo", func(context.Context) (bool, error) { undone.Store(true); return true, nil })
			return "", Cancelled("undone: " + ce.Reason)
		}
		return "", err
	})
	run, err := wf.Start(ctx, "w-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := wf.Start(ctx, "w-1", nil); err == nil {
		t.Fatal("starting a running ID succeeded")
	}
	if _, err := wf.Start(ctx, "w-1", nil, StartOptions{IfRunning: IfRunningUseExisting}); err != nil {
		t.Fatalf("use-existing: %v", err)
	}
	waitStatus(t, e, "w-1", "suspended", "parked")
	if err := run.Cancel(ctx, "stop"); err != nil {
		t.Fatal(err)
	}
	_, err = run.Result(ctx)
	var ce *CancelledError
	if !errors.As(err, &ce) || ce.Reason != "undone: stop" || !undone.Load() {
		t.Fatalf("%v %v", err, undone.Load())
	}
	run2, _ := wf.Start(ctx, "w-2", nil)
	waitStatus(t, e, "w-2", "suspended", "parked")
	if err := run2.Terminate(ctx, "enough"); err != nil {
		t.Fatal(err)
	}
	var te *TerminatedError
	if _, err := run2.Result(ctx); !errors.As(err, &te) {
		t.Fatalf("terminated: %v", err)
	}
	if _, err := e.Run("nope").Result(ctx); err == nil || codeOf(err) != "NotFound" {
		t.Fatalf("not found: %v", err)
	}
}

func TestContinueAsNewPatchesAttributesAndList(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	wf := Register(e, "loop", func(c *Ctx, left int) (int, error) {
		if c.Patched("p1") {
			c.DeprecatePatch("p0")
		}
		if err := c.SetAttributes(map[string]any{"left": left, "kind": "loop"}); err != nil {
			return 0, err
		}
		v, err := Step(c, "work", func(context.Context) (int, error) { return left, nil })
		if err != nil || left == 0 {
			return v, err
		}
		return 0, c.ContinueAsNew(left - 1)
	})
	run, err := wf.Start(ctx, "loop-1", 3)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := run.Result(ctx); err != nil || v != 0 {
		t.Fatalf("%d %v", v, err)
	}
	runs, err := e.List(ctx, ListOptions{Where: map[string]any{"kind": "loop", "status": "ok"}})
	if err != nil || len(runs) != 1 || runs[0].ID != "loop-1" || runs[0].Attributes["left"] != float64(0) {
		t.Fatalf("%+v %v", runs, err)
	}
}

func TestLimitsTimeoutsHeartbeatsAndPanics(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	if err := e.Limit("one", LimitOptions{Concurrent: 1}); err != nil {
		t.Fatal(err)
	}
	var inside, most atomic.Int32
	limited := Register(e, "limited", func(c *Ctx, i int) (int, error) {
		return Step(c, "work", func(context.Context) (int, error) {
			n := inside.Add(1)
			for {
				m := most.Load()
				if n <= m || most.CompareAndSwap(m, n) {
					break
				}
			}
			time.Sleep(2 * time.Millisecond)
			inside.Add(-1)
			return i, nil
		}, StepOptions{Limit: "one", Priority: int32(i), FairnessKey: "k"})
	})
	var runs []*Run[int]
	for i := 0; i < 5; i++ {
		r, err := limited.Start(ctx, "lim-"+string(rune('a'+i)), i)
		if err != nil {
			t.Fatal(err)
		}
		runs = append(runs, r)
	}
	for i, r := range runs {
		if v, err := r.Result(ctx); err != nil || v != i {
			t.Fatalf("%d %v", v, err)
		}
	}
	if most.Load() != 1 {
		t.Fatalf("%d steps held the one permit at once", most.Load())
	}
	one := &RetryPolicy{Attempts: 1}
	slow := Register(e, "slow", func(c *Ctx, _ any) (string, error) {
		_, err := Step(c, "late", func(ctx context.Context) (int, error) {
			<-ctx.Done()
			return 0, ctx.Err()
		}, StepOptions{Timeout: 20 * time.Millisecond, Retry: one})
		if err == nil || !strings.Contains(err.Error(), "timed out after 20 ms") {
			return "", errors.New("no timeout: " + errString(err))
		}
		_, err = Step(c, "silent", func(ctx context.Context) (int, error) {
			Heartbeat(ctx, "started")
			<-ctx.Done()
			return 0, nil
		}, StepOptions{HeartbeatTimeout: 20 * time.Millisecond, Retry: one})
		if err == nil || !strings.Contains(err.Error(), "no heartbeat") {
			return "", errors.New("no heartbeat timeout: " + errString(err))
		}
		_, err = Step(c, "panics", func(context.Context) (int, error) { panic("boom") }, StepOptions{Retry: one})
		if err == nil || !strings.Contains(err.Error(), "boom") {
			return "", errors.New("no panic: " + errString(err))
		}
		key, err := Step(c, "key", func(ctx context.Context) (string, error) { return Key(ctx), nil })
		if err != nil || key == "" {
			return "", errors.New("no key")
		}
		return "ok", nil
	})
	r, err := slow.Start(ctx, "slow-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := r.Result(ctx); err != nil || v != "ok" {
		t.Fatalf("%q %v", v, err)
	}
	broken := Register(e, "broken", func(c *Ctx, _ any) (int, error) { panic("workflow bug") })
	r2, _ := broken.Start(ctx, "broken-1", nil)
	if _, err := r2.Result(ctx); err == nil || !strings.Contains(err.Error(), "workflow bug") {
		t.Fatalf("%v", err)
	}
}

func errString(err error) string {
	if err == nil {
		return "nil"
	}
	return err.Error()
}

func TestCompletionTokensSchedulesQueues(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	tokens := make(chan string, 2)
	approval := Register(e, "approval", func(c *Ctx, _ any) (string, error) {
		return AwaitCompletion[string](c, "decision", func(_ context.Context, token string) error {
			tokens <- token
			return nil
		})
	})
	run, err := approval.Start(ctx, "ap-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	token := <-tokens
	waitStatus(t, e, "ap-1", "suspended", "parked")
	if err := e.Complete(ctx, token, "yes"); err != nil {
		t.Fatal(err)
	}
	if v, err := run.Result(ctx); err != nil || v != "yes" {
		t.Fatalf("%q %v", v, err)
	}
	var rj *RejectedError
	if err := e.Complete(ctx, "not a token", 1); !errors.As(err, &rj) {
		t.Fatalf("a bad token: %v", err)
	}

	ticks := Register(e, "tick", func(c *Ctx, n int) (int, error) { return n, nil })
	s, err := e.Schedule(ctx, "every", ticks.Name, 7, ScheduleOptions{Every: time.Hour, Paused: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Trigger(ctx); err != nil {
		t.Fatal(err)
	}
	desc, err := s.Describe(ctx)
	if err != nil || !strings.Contains(string(desc), "paused") {
		t.Fatalf("%s %v", desc, err)
	}
	if _, err := e.Schedule(ctx, "bad", ticks.Name, nil, ScheduleOptions{Cron: "not cron"}); err == nil {
		t.Fatal("a bad cron was accepted")
	}
	if err := s.Delete(ctx); err != nil {
		t.Fatal(err)
	}

	collect := Register(e, "collect", func(c *Ctx, events []string) ([]string, error) { return events, nil })
	var ids []string
	for _, ev := range []string{"a", "b", "b"} {
		r, err := collect.Batch(ctx, "k", ev, BatchOptions{MaxEvents: 3, MaxWait: 50 * time.Millisecond, ID: ev})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, r.ID)
	}
	if ids[0] != ids[1] || ids[1] != ids[2] {
		t.Fatalf("%v", ids)
	}
	b := RunOf[[]string](e, ids[0])
	if err := b.Started(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := b.Result(ctx); err != nil || strings.Join(v, ",") != "a,b" {
		t.Fatalf("%v %v", v, err)
	}
	echo := Register(e, "echo", func(c *Ctx, v int) (int, error) { return v, nil })
	d1, err := echo.Debounce(ctx, "k", 1, DebounceOptions{Window: 30 * time.Millisecond, ID: "d1"})
	if err != nil {
		t.Fatal(err)
	}
	d2, err := echo.Debounce(ctx, "k", 2, DebounceOptions{Window: 30 * time.Millisecond, ID: "d2"})
	if err != nil || d1.ID != d2.ID {
		t.Fatalf("%v %v %v", d1, d2, err)
	}
	if err := d1.Started(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := d1.Result(ctx); err != nil || v != 2 {
		t.Fatalf("%d %v", v, err)
	}
	if b, err := e.ExportJournal(ids[0]); err != nil || len(b) == 0 {
		t.Fatalf("export: %d %v", len(b), err)
	}
	if m, err := e.ShardMetrics(); err != nil || len(m) != 2 {
		t.Fatalf("metrics %v %v", m, err)
	}
}

func TestRawBytesAndMisuse(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var leaked atomic.Pointer[Ctx]
	wf := Register(e, "bytes", func(c *Ctx, in []byte) ([]byte, error) {
		leaked.Store(c)
		out, err := Step(c, "rev", func(context.Context) ([]byte, error) {
			b := append([]byte(nil), in...)
			for i, j := 0, len(b)-1; i < j; i, j = i+1, j-1 {
				b[i], b[j] = b[j], b[i]
			}
			return b, nil
		})
		return out, err
	})
	run, err := wf.Start(ctx, "b-1", []byte{0, 1, 2, 255})
	if err != nil {
		t.Fatal(err)
	}
	v, err := run.Result(ctx)
	if err != nil || len(v) != 4 || v[0] != 255 || v[3] != 0 {
		t.Fatalf("%v %v", v, err)
	}
	d, _ := run.Inspect(ctx)
	var j []map[string]any
	if json.Unmarshal(d.Journal, &j) != nil || j[0]["kind"] != "start" {
		t.Fatalf("%s", d.Journal)
	}
	defer func() {
		if recover() == nil {
			t.Fatal("using a Ctx outside its task did not panic")
		}
	}()
	leaked.Load().Now()
}
