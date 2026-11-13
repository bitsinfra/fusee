// Per-step overhead of a sequential no-op chain, closed loop, as
// bindings/node/bench.mjs and bindings/python/bench.py measure it:
//
//	go run ./cmd/bench [memory|local|fsync]
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

func main() {
	mode := "memory"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}
	opts := fusee.Options{Durability: "memory"}
	if mode != "memory" {
		dir := filepath.Join(os.TempDir(), "fusee-go-bench")
		_ = os.RemoveAll(dir)
		opts = fusee.Options{Dir: dir}
		if mode == "fsync" {
			opts.Flush = "fsync"
		}
	}
	e, err := fusee.Open(opts)
	if err != nil {
		log.Fatal(err)
	}
	ctx := context.Background()
	var stamps []time.Time
	var mu sync.Mutex
	stamp := func() {
		mu.Lock()
		stamps = append(stamps, time.Now())
		mu.Unlock()
	}
	chain := func(inline bool) func(c *fusee.Ctx, n int) (int, error) {
		o := fusee.StepOptions{Inline: inline}
		return func(c *fusee.Ctx, n int) (int, error) {
			for i := 0; i < n; i++ {
				if _, err := fusee.Step(c, "noop", func(context.Context) (int, error) { stamp(); return i, nil }, o); err != nil {
					return 0, err
				}
			}
			return n, nil
		}
	}
	wf := fusee.Register(e, "chain", chain(false))
	inl := fusee.Register(e, "chain-inline", chain(true))
	n := 3000
	if mode == "local" {
		n = 300
	}
	for _, w := range []*fusee.Workflow[int, int]{wf, inl} {
		stamps = stamps[:0]
		before, _ := e.Stats()
		r, err := w.Start(ctx, "solo-"+w.Name, n)
		if err != nil {
			log.Fatal(err)
		}
		if _, err := r.Result(ctx); err != nil {
			log.Fatal(err)
		}
		after, _ := e.Stats()
		var gaps []float64
		for i := 1; i < len(stamps); i++ {
			gaps = append(gaps, float64(stamps[i].Sub(stamps[i-1]).Nanoseconds())/1e3)
		}
		sort.Float64s(gaps)
		p := func(q float64) float64 { return gaps[int(float64(len(gaps)-1)*q)] }
		fmt.Printf("go %s %s: step-to-step p50 %.1f µs  p99 %.1f µs  (%d steps; %.1f engine calls and %.2f ticks per step)\n",
			mode, w.Name, p(0.5), p(0.99), n,
			float64(after.NativeCalls-before.NativeCalls)/float64(n), float64(after.Ticks-before.Ticks)/float64(n))
	}
	const W, S = 1000, 20
	t0 := time.Now()
	runs := make([]*fusee.Run[int], W)
	for i := range runs {
		if runs[i], err = wf.Start(ctx, fmt.Sprintf("w%d", i), S); err != nil {
			log.Fatal(err)
		}
	}
	for _, r := range runs {
		if _, err := r.Result(ctx); err != nil {
			log.Fatal(err)
		}
	}
	el := time.Since(t0).Seconds()
	fmt.Printf("  %d durable steps in %.2f s = %.0f steps/s\n", W*S, el, W*S/el)
	if err := e.Close(); err != nil {
		log.Fatal(err)
	}
}
