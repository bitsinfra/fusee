// A cart object in Go: one object per user, with its own state; calls to a
// key take turns, and a call sent again with the same ID runs once.
package main

import (
	"context"
	"fmt"
	"log"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

type Item struct {
	SKU   string `json:"sku"`
	Cents int    `json:"cents"`
}

func items(c *fusee.ObjectCtx) ([]Item, error) {
	all, _, err := fusee.StateGet[[]Item](c.State(), "items")
	return all, err
}

func sum(all []Item) int {
	n := 0
	for _, i := range all {
		n += i.Cents
	}
	return n
}

func main() {
	e, err := fusee.Open(fusee.Options{Dir: "./data/cart-object"})
	if err != nil {
		log.Fatal(err)
	}
	defer e.Close()
	_, err = fusee.RegisterObject(e, "cart", fusee.Handlers{
		"add": fusee.Exclusive(func(c *fusee.ObjectCtx, item Item) (int, error) {
			all, err := items(c)
			if err != nil {
				return 0, err
			}
			all = append(all, item)
			return len(all), c.State().Set("items", all)
		}),
		"total": fusee.Shared(func(c *fusee.ObjectCtx, _ any) (int, error) {
			all, err := items(c)
			return sum(all), err
		}),
		"checkout": fusee.Exclusive(func(c *fusee.ObjectCtx, _ any) (string, error) {
			all, err := items(c)
			if err != nil {
				return "", err
			}
			receipt, err := fusee.Step(c.Ctx, "charge", func(context.Context) (string, error) {
				return fmt.Sprintf("charged %d cents for %d items", sum(all), len(all)), nil
			})
			if err != nil {
				return "", err
			}
			return receipt, c.State().Clear("items")
		}),
	})
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	user := e.Object("cart", "user-7")
	tea, milk := Item{SKU: "tea", Cents: 450}, Item{SKU: "milk", Cents: 120}
	if _, err := fusee.CallRef[int](ctx, user, "add", tea, fusee.CallOptions{ID: "r1"}); err != nil {
		log.Fatal(err)
	}
	n, err := fusee.CallRef[int](ctx, user, "add", tea, fusee.CallOptions{ID: "r1"}) // sent again: once
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("items:", n)
	if n, err = fusee.CallRef[int](ctx, user, "add", milk, fusee.CallOptions{ID: "r2"}); err != nil {
		log.Fatal(err)
	}
	fmt.Println("items:", n)
	total, err := fusee.CallRef[int](ctx, user, "total", nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("total:", total)
	receipt, err := fusee.CallRef[string](ctx, user, "checkout", nil, fusee.CallOptions{ID: "c1"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("receipt:", receipt)
}
