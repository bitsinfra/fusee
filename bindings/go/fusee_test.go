package fusee

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func memory(t *testing.T) *Engine {
	t.Helper()
	e, err := Open(Options{Durability: "memory", Shards: 2, KeepHistory: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	return e
}

func bg(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type order struct {
	N int `json:"n"`
}

func TestSteps(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	wf := Register(e, "seq", func(c *Ctx, in order) (map[string]int, error) {
		a, err := Step(c, "a", func(context.Context) (int, error) { return in.N + 1, nil })
		if err != nil {
			return nil, err
		}
		b, err := Step(c, "b", func(context.Context) (int, error) { return a * 2, nil })
		if err != nil {
			return nil, err
		}
		return Step(c, "c", func(context.Context) (map[string]int, error) { return map[string]int{"a": a, "b": b}, nil })
	})
	run, err := wf.Start(ctx, "seq-1", order{N: 3})
	if err != nil {
		t.Fatal(err)
	}
	out, err := run.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out["a"] != 4 || out["b"] != 8 {
		t.Fatalf("got %v", out)
	}
	d, err := run.Inspect(ctx)
	if err != nil || d == nil || d.Status != "ok" {
		t.Fatalf("inspect %v %v", d, err)
	}
	if !strings.Contains(string(d.Journal), `"name":"b"`) {
		t.Fatalf("journal %s", d.Journal)
	}
}

func TestRetryAndFailure(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var tries atomic.Int32
	r3 := &RetryPolicy{Attempts: 3, Initial: time.Millisecond, Max: time.Millisecond}
	wf := Register(e, "retry", func(c *Ctx, _ any) (string, error) {
		v, err := Step(c, "flaky", func(context.Context) (string, error) {
			if n := tries.Add(1); n < 3 {
				return "", fmt.Errorf("try %d", n)
			}
			return "ok", nil
		}, StepOptions{Retry: r3})
		if err != nil {
			return "", err
		}
		_, err = Step(c, "boom", func(context.Context) (int, error) { return 0, errors.New("boom") },
			StepOptions{Retry: &RetryPolicy{Attempts: 1}})
		var se *StepError
		if !errors.As(err, &se) || se.Detail != "boom" {
			return "", fmt.Errorf("want a step error, got %v", err)
		}
		return v, nil
	})
	run, err := wf.Start(ctx, "retry-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	out, err := run.Result(ctx)
	if err != nil || out != "ok" || tries.Load() != 3 {
		t.Fatalf("%q %v %d", out, err, tries.Load())
	}
}

func TestSignalsSleepsChildrenRaces(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	Register(e, "double", func(c *Ctx, x int) (int, error) {
		return Step(c, "mul", func(context.Context) (int, error) { return x * 2, nil })
	})
	wf := Register(e, "main", func(c *Ctx, _ any) ([]any, error) {
		if err := c.Sleep("nap", time.Millisecond); err != nil {
			return nil, err
		}
		child, err := Child[int](c, "double", c.WorkflowID()+"/c", 21)
		if err != nil {
			return nil, err
		}
		v, ok, err := WaitFor[string](c, "go", 0)
		if err != nil || !ok {
			return nil, fmt.Errorf("wait: %v %v", ok, err)
		}
		// The signal "x" never comes: the timer wins and the wait is dropped.
		w := StartWait[string](c, "x", 0)
		i, err := Race(c, w, c.StartSleep("short", time.Millisecond))
		if err != nil {
			return nil, err
		}
		_, ok, err = WaitFor[string](c, "late", 5*time.Millisecond)
		if err != nil {
			return nil, err
		}
		fx := StartStep(c, "x", func(context.Context) (int, error) { return 1, nil })
		fy := StartStep(c, "y", func(context.Context) (int, error) { return 2, nil })
		if err := All(c, fx, fy); err != nil {
			return nil, err
		}
		x, _ := fx.Get(c)
		y, _ := fy.Get(c)
		g := Go(c, func(c *Ctx) (int, error) {
			return Step(c, "in-go", func(context.Context) (int, error) { return 7, nil })
		})
		z, err := g.Get(c)
		if err != nil {
			return nil, err
		}
		return []any{child, v, i, ok, x + y, z, c.Random() < 1, len(c.UUID()), c.Now().Year() > 2000}, nil
	})
	run, err := wf.Start(ctx, "main-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Signal(ctx, "go", "hi"); err != nil {
		t.Fatal(err)
	}
	out, err := run.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := `[42 hi 1 false 3 7 true 36 true]`
	if fmt.Sprint(out) != want {
		t.Fatalf("got %v want %s", out, want)
	}
}
