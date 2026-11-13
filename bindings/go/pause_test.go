package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// A step whose retries run out pauses its run; Resume runs it again with a
// fresh set of attempts, and the parent waits throughout.
func TestPausedStepResumes(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var mu sync.Mutex
	broken := true
	calls := 0
	var pausedMsgs []string
	if err := e.On("paused", func(id, msg string) { pausedMsgs = append(pausedMsgs, id+": "+msg) }); err != nil {
		t.Fatal(err)
	}
	Register(e, "pay", func(c *Ctx, n int) (int, error) {
		v, err := Step(c, "charge", func(context.Context) (int, error) {
			mu.Lock()
			defer mu.Unlock()
			calls++
			if broken {
				return 0, errors.New("payments are down")
			}
			return n * 10, nil
		}, StepOptions{Inline: true, Retry: &RetryPolicy{Attempts: 2, Initial: time.Millisecond, OnExhausted: Pause}})
		return v + 1, err
	})
	order := Register(e, "order", func(c *Ctx, n int) (int, error) {
		return Child[int](c, "pay", "pay-1", n)
	})
	run, err := order.Start(ctx, "order-1", 4)
	if err != nil {
		t.Fatal(err)
	}
	status := func(id string) string {
		d, err := RunOf[int](e, id).Inspect(ctx)
		if err != nil || d == nil {
			return ""
		}
		return d.Status
	}
	deadline := time.Now().Add(10 * time.Second)
	for status("pay-1") != "paused" {
		if time.Now().After(deadline) {
			t.Fatalf("no pause: %s", status("pay-1"))
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	if calls != 2 {
		t.Fatalf("calls while paused: %d", calls)
	}
	broken = false
	mu.Unlock()
	if len(pausedMsgs) != 1 || pausedMsgs[0] != "pay-1: step 'charge' ran out of retries: payments are down" {
		t.Fatalf("paused events: %v", pausedMsgs)
	}
	if err := RunOf[int](e, "pay-1").Resume(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := run.Result(ctx)
	if err != nil || got != 41 {
		t.Fatalf("result %v %v", got, err)
	}
	d, err := RunOf[int](e, "pay-1").Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var journal []map[string]any
	if err := json.Unmarshal(d.Journal, &journal); err != nil {
		t.Fatal(err)
	}
	var kinds []string
	for _, r := range journal {
		kinds = append(kinds, r["kind"].(string))
	}
	if !reflect.DeepEqual(kinds, []string{"start", "attempt", "attempt", "resume", "step", "done"}) {
		t.Fatalf("journal kinds: %v", kinds)
	}
	var rejected *RejectedError
	if err := RunOf[int](e, "pay-1").Resume(ctx); !errors.As(err, &rejected) {
		t.Fatalf("a second resume: %v", err)
	}
}
