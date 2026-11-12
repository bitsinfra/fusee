// Human approval in Go: a workflow that asks a person, waits up to three
// days for the answer (durably: a crash meanwhile loses nothing), and acts
// on it.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

type Claim struct {
	By    string `json:"by"`
	Cents int    `json:"cents"`
}

type Decision struct {
	Approved bool   `json:"approved"`
	By       string `json:"by"`
}

var Expense = fusee.Define("expense", func(c *fusee.Ctx, claim Claim) (string, error) {
	if _, err := fusee.Step(c, "ask", func(context.Context) (bool, error) {
		fmt.Printf("  asked finance to approve %d cents for %s\n", claim.Cents, claim.By)
		return true, nil
	}); err != nil {
		return "", err
	}
	answer, ok, err := fusee.WaitFor[Decision](c, "decision", 3*24*time.Hour)
	if err != nil {
		return "", err
	}
	if !ok {
		return "expired: nobody answered in three days", nil
	}
	if !answer.Approved {
		return "rejected by " + answer.By, nil
	}
	paid, err := fusee.Step(c, "pay", func(context.Context) (string, error) {
		return fmt.Sprintf("paid %d cents", claim.Cents), nil
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s (approved by %s)", paid, answer.By), nil
})

func main() {
	e, err := fusee.Open(fusee.Options{Dir: "./data/human-approval", Workflows: []fusee.Definition{Expense}})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	ctx := context.Background()
	run, err := Expense.On(e).Start(ctx, "expense-7", Claim{By: "ana", Cents: 12000},
		fusee.StartOptions{IfRunning: fusee.IfRunningUseExisting, IfFinished: fusee.IfFinishedUseExisting})
	if err != nil {
		log.Fatal(err)
	}
	if err := run.Signal(ctx, "decision", Decision{Approved: true, By: "dana"}); err != nil {
		log.Fatal(err)
	}
	result, err := run.Result(ctx)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("result:", result)
}
