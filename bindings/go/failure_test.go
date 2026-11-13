package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// CardDeclined is an application error with its own type.
type CardDeclined struct{ Last4 string }

func (e *CardDeclined) Error() string { return "card declined" }

// A failure travels as the envelope: Fatal stops retries, a custom error's
// type name and a Failure's code and details reach the caller, and a
// failed run returns them under WorkflowFailed.
func TestFailuresTravelAsTheEnvelope(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var tries atomic.Int32
	fast := StepOptions{Retry: &RetryPolicy{Attempts: 3, Initial: time.Millisecond, Max: time.Millisecond}}
	pay := Register(e, "pay", func(c *Ctx, kind string) (int, error) {
		return Step(c, "charge", func(context.Context) (int, error) {
			tries.Add(1)
			switch kind {
			case "fatal":
				return 0, Fatal(&CardDeclined{Last4: "4242"})
			case "coded":
				d, _ := json.Marshal(map[string]string{"last4": "4242"})
				no := false
				return 0, &Failure{Message: "declined", Type: "CardDeclined", Code: "card_declined", Details: d, Retryable: &no}
			default:
				return 0, &CardDeclined{}
			}
		}, fast)
	})

	run, err := pay.Start(ctx, "p-1", "fatal")
	if err != nil {
		t.Fatal(err)
	}
	_, err = run.Result(ctx)
	var we *WorkflowError
	if !errors.As(err, &we) || we.Cause.Type != "StepFailed" || we.Cause.Cause == nil || we.Cause.Cause.Type != "CardDeclined" {
		t.Fatalf("fatal: %#v", err)
	}
	if tries.Load() != 1 {
		t.Fatalf("a fatal failure was tried %d times", tries.Load())
	}

	tries.Store(0)
	run, _ = pay.Start(ctx, "p-2", "coded")
	_, err = run.Result(ctx)
	if !errors.As(err, &we) || we.Cause.Cause.Code != "card_declined" || string(we.Cause.Cause.Details) != `{"last4":"4242"}` {
		t.Fatalf("coded: %#v", err)
	}
	if tries.Load() != 1 {
		t.Fatalf("a non-retryable failure was tried %d times", tries.Load())
	}

	// A plain custom error is retried under the policy, and the step's
	// failure keeps its type.
	tries.Store(0)
	run, _ = pay.Start(ctx, "p-3", "plain")
	_, err = run.Result(ctx)
	if tries.Load() != 3 {
		t.Fatalf("tried %d times", tries.Load())
	}
	if !errors.As(err, &we) || we.Cause.Cause.Type != "CardDeclined" || we.Cause.Cause.Message != "card declined" {
		t.Fatalf("plain: %#v", err)
	}

	// NonRetryable names types not to retry.
	tries.Store(0)
	only := Register(e, "pay-once", func(c *Ctx, _ struct{}) (int, error) {
		return Step(c, "charge", func(context.Context) (int, error) {
			tries.Add(1)
			return 0, &CardDeclined{}
		}, StepOptions{Retry: &RetryPolicy{Attempts: 5, Initial: time.Millisecond, NonRetryable: []string{"CardDeclined"}}})
	})
	run4, _ := only.Start(ctx, "p-4", struct{}{})
	_, err = run4.Result(ctx)
	var se *StepError
	if !errors.As(err, &se) && tries.Load() != 1 {
		t.Fatalf("non-retryable: tried %d times, %v", tries.Load(), err)
	}
	if tries.Load() != 1 {
		t.Fatalf("NonRetryable: tried %d times", tries.Load())
	}
}
