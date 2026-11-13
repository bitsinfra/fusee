package fusee

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// Compensations run as steps, newest first, when the workflow fails or is
// cancelled, and the failure is returned.
func TestCompensationsRunNewestFirst(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var mu sync.Mutex
	var undone []string
	trip := Register(e, "trip", func(c *Ctx, fail bool) (struct{}, error) {
		for _, what := range []string{"flight", "hotel"} {
			what := what
			if _, err := Step(c, "book "+what, func(context.Context) (int, error) { return 1, nil }); err != nil {
				return struct{}{}, err
			}
			c.Compensate("cancel "+what, func(context.Context) error {
				mu.Lock()
				undone = append(undone, what)
				mu.Unlock()
				return nil
			})
		}
		if fail {
			_, err := Step(c, "pay", func(context.Context) (int, error) {
				return 0, errors.New("card declined")
			}, StepOptions{Retry: &RetryPolicy{Attempts: 1}})
			return struct{}{}, err
		}
		_, _, err := WaitFor[any](c, "never", 0)
		return struct{}{}, err
	})
	run, err := trip.Start(ctx, "t-1", true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := run.Result(ctx); err == nil {
		t.Fatal("expected the run to fail")
	}
	if !reflect.DeepEqual(undone, []string{"hotel", "flight"}) {
		t.Fatalf("compensations: %v", undone)
	}
	undone = nil
	run2, err := trip.Start(ctx, "t-2", false)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	if err := run2.Cancel(ctx, "changed plans"); err != nil {
		t.Fatal(err)
	}
	if _, err := run2.Result(ctx); err == nil {
		t.Fatal("expected the run to be cancelled")
	}
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(undone, []string{"hotel", "flight"}) {
		t.Fatalf("compensations after cancel: %v", undone)
	}
}

// SignalWithStart starts once and signals; Retry reruns from the failed
// step; Limits and Metrics answer.
func TestSignalWithStartRetryLimitsMetrics(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	cart := Register(e, "cart", func(c *Ctx, items []string) ([]string, error) {
		for len(items) < 2 {
			v, ok, err := WaitFor[string](c, "add", 0)
			if err != nil {
				return nil, err
			}
			if ok {
				items = append(items, v)
			}
		}
		return items, nil
	})
	run, err := SignalWithStart(ctx, cart, "cart-1", []string{}, "add", "apple")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := SignalWithStart(ctx, cart, "cart-1", []string{}, "add", "pear"); err != nil {
		t.Fatal(err)
	}
	got, err := run.Result(ctx)
	if err != nil || !reflect.DeepEqual(got, []string{"apple", "pear"}) {
		t.Fatalf("%v %v", got, err)
	}
	tries := 0
	flaky := Register(e, "flaky", func(c *Ctx, _ struct{}) (int, error) {
		return Step(c, "call", func(context.Context) (int, error) {
			tries++
			if tries == 1 {
				return 0, errors.New("unavailable")
			}
			return 7, nil
		}, StepOptions{Retry: &RetryPolicy{Attempts: 1}})
	})
	fr, _ := flaky.Start(ctx, "f-1", struct{}{})
	if _, err := fr.Result(ctx); err == nil {
		t.Fatal("expected the first run to fail")
	}
	if err := fr.Retry(ctx); err != nil {
		t.Fatal(err)
	}
	if v, err := fr.Result(ctx); err != nil || v != 7 {
		t.Fatalf("after retry: %v %v", v, err)
	}
	if err := e.Limit("stripe", LimitOptions{Concurrent: 2}); err != nil {
		t.Fatal(err)
	}
	ls, err := e.Limits()
	if err != nil || len(ls) != 1 || ls[0].Name != "stripe" || ls[0].Concurrent != 2 {
		t.Fatalf("limits: %+v %v", ls, err)
	}
	m, err := e.Metrics()
	if err != nil || m["steps_run"] == 0 {
		t.Fatalf("metrics: %v %v", m, err)
	}
	if _, err := e.Schedule(ctx, "nightly", "cart", []string{}, ScheduleOptions{Cron: "0 3 * * *"}); err != nil {
		t.Fatal(err)
	}
	all, err := e.Schedules().List(ctx)
	if err != nil || len(all) != 1 || all[0].Name != "nightly" {
		t.Fatalf("schedules: %v %v", all, err)
	}
	if err := e.On("nope", func(string, string) {}); err == nil {
		t.Fatal("an unknown event is refused")
	}
}
