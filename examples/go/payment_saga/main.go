// A payment saga in Go: each step that changes the world registers the step
// that undoes it; if the order fails or is cancelled, the compensations run,
// newest first.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

type Order struct {
	SKU   string `json:"sku"`
	Cents int    `json:"cents"`
}

// short is a short ID made from a step's idempotency key (as a service
// would): FNV-1a.
func short(key string) string {
	h := uint32(0x811c9dc5)
	for _, b := range []byte(key) {
		h = (h ^ uint32(b)) * 0x01000193
	}
	return fmt.Sprintf("%08x", h)
}

var Checkout = fusee.Define("checkout", func(c *fusee.Ctx, o Order) (string, error) {
	reservation, err := fusee.Step(c, "reserve", func(ctx context.Context) (string, error) {
		return "res-" + o.SKU + "-" + short(fusee.Key(ctx)), nil
	})
	if err != nil {
		return "", err
	}
	c.Compensate("release", func(context.Context) error {
		fmt.Println("  released", reservation)
		return nil
	})
	charge, err := fusee.Step(c, "charge", func(ctx context.Context) (string, error) {
		if o.Cents > 1_000_000 {
			return "", fusee.Fatal(errors.New("card declined"))
		}
		return "ch-" + short(fusee.Key(ctx)), nil
	}, fusee.StepOptions{Retry: &fusee.RetryPolicy{Attempts: 5, Initial: 200 * time.Millisecond, Max: time.Minute}})
	if err != nil {
		return "", err
	}
	c.Compensate("refund", func(context.Context) error {
		fmt.Println("  refunded", charge)
		return nil
	})
	shipped, ok, err := fusee.WaitFor[bool](c, "shipped", 24*time.Hour)
	if err != nil {
		return "", err
	}
	if !ok || !shipped {
		return "", fusee.Fatal(errors.New("not shipped in a day"))
	}
	return charge, nil
})

func main() {
	e, err := fusee.Open(fusee.Options{Dir: "./data/payment-saga", Workflows: []fusee.Definition{Checkout}})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	run, err := Checkout.On(e).Start(ctx, "order-123", Order{SKU: "SKU-9", Cents: 4200},
		fusee.StartOptions{IfRunning: fusee.IfRunningUseExisting, IfFinished: fusee.IfFinishedUseExisting})
	if err != nil {
		log.Fatal(err)
	}
	if err := run.Signal(ctx, "shipped", true); err != nil {
		log.Fatal(err)
	}
	charge, err := run.Result(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("charged:", charge)
}
