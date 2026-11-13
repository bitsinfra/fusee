// Companion to conformance/remote.py: `remote worker ADDRESS` serves the
// workflows through a Fusee server; `remote client ADDRESS` drives a
// worker in any language through it (TAG makes its workflow IDs unique).
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"reflect"
	"syscall"
	"time"

	fusee "github.com/bitsinfra/fusee/bindings/go"
)

func main() {
	if len(os.Args) < 3 {
		log.Fatal("usage: remote worker|client ADDRESS [TAG]")
	}
	if os.Args[1] == "worker" {
		worker(os.Args[2])
	} else {
		tag := ""
		if len(os.Args) > 3 {
			tag = os.Args[3]
		}
		client(os.Args[2], tag)
	}
}

func worker(address string) {
	e, err := fusee.Open(fusee.Options{Durability: "server", Address: address})
	if err != nil {
		log.Fatal(err)
	}
	fusee.Register(e, "approval", func(c *fusee.Ctx, initial int) ([]any, error) {
		count := initial
		var token any
		fusee.OnUpdate(c, "add", func(c *fusee.Ctx, n int) (int, error) { count += n; return count, nil }, nil)
		fusee.OnQuery(c, "count", func(any) (int, error) { return count, nil })
		fusee.OnQuery(c, "token", func(any) (any, error) { return token, nil })
		// Inline: the send function sets workflow state.
		approved, err := fusee.AwaitCompletion[bool](c, "decision", func(_ context.Context, t string) error {
			token = t
			return nil
		}, fusee.StepOptions{Inline: true})
		return []any{count, approved}, err
	})
	// Debounced and batched runs, their steps queued for a limit by priority.
	if err := e.Limit("remote", fusee.LimitOptions{Concurrent: 1}); err != nil {
		log.Fatal(err)
	}
	fusee.Register(e, "echo", func(c *fusee.Ctx, v any) (any, error) {
		return fusee.Step(c, "v", func(context.Context) (any, error) { return v, nil },
			fusee.StepOptions{Limit: "remote", Priority: 2, FairnessKey: "t", FairnessWeight: 2})
	})
	fusee.Register(e, "collect", func(c *fusee.Ctx, events []any) ([]any, error) {
		return fusee.Step(c, "all", func(context.Context) ([]any, error) { return events, nil },
			fusee.StepOptions{Limit: "remote", Priority: -1})
	})
	// A step that streams tokens, then bytes.
	fusee.Register(e, "talk", func(c *fusee.Ctx, n int) (int, error) {
		return fusee.Step(c, "generate", func(ctx context.Context) (int, error) {
			out, err := fusee.Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			for i := 0; i < n; i++ {
				if err := out.Append(fmt.Sprintf("t%d", i)); err != nil {
					return 0, err
				}
				time.Sleep(5 * time.Millisecond)
			}
			return n, out.Append([]byte{1, 2})
		})
	})
	// A key of an object type, called by every client.
	if _, err := fusee.RegisterObject(e, "tally", fusee.Handlers{
		"add": fusee.Exclusive(func(c *fusee.ObjectCtx, n int64) (int64, error) {
			sum, _, err := fusee.StateGet[int64](c.State(), "sum")
			if err != nil {
				return 0, err
			}
			sum += n
			return sum, c.State().Set("sum", sum)
		}),
	}); err != nil {
		log.Fatal(err)
	}
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	fmt.Println("READY")
	<-stop
	if err := e.Close(); err != nil {
		log.Fatal(err)
	}
}

func check(ok bool, what string, args ...any) {
	if !ok {
		log.Fatalf("FAIL Go client: "+what, args...)
	}
}

func client(address, tag string) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	c, err := fusee.Connect(address)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	name := "from-go" + tag
	v, err := c.UpdateWithStart(ctx, "approval", name, 4, "add", 3, fusee.StartOptions{UpdateID: "once"})
	check(err == nil && string(v) == "7", "update with start: %s %v", v, err)
	run := c.Run(name)
	v, err = run.Update(ctx, "add", 3, fusee.UpdateOptions{ID: "once"})
	check(err == nil && string(v) == "7", "a repeated update: %s %v", v, err)
	v, err = run.Query(ctx, "count", nil)
	check(err == nil && string(v) == "7", "query: %s %v", v, err)
	var token string
	for i := 0; i < 100 && token == ""; i++ {
		p, err := run.Query(ctx, "token", nil)
		check(err == nil, "token query: %v", err)
		_ = json.Unmarshal(p, &token)
		if token == "" {
			time.Sleep(10 * time.Millisecond)
		}
	}
	check(token != "", "no token")
	check(c.Complete(ctx, token, true) == nil, "complete")
	v, err = run.Result(ctx)
	var result []any
	check(err == nil && json.Unmarshal(v, &result) == nil && reflect.DeepEqual(result, []any{7.0, true}), "result: %s %v", v, err)
	runs, err := c.List(ctx, fusee.ListOptions{Where: map[string]any{"status": "ok"}})
	found := false
	for _, r := range runs {
		found = found || (r.ID == name && r.StartedMs > 0)
	}
	check(err == nil && found, "listing: %v %v", runs, err)
	// Two starts inside the window: one run, the latest input.
	first, err := c.Debounce(ctx, "echo", "d-"+name, 1, fusee.DebounceOptions{Window: 500 * time.Millisecond, ID: "d1"})
	check(err == nil, "debounce: %v", err)
	latest, err := c.Debounce(ctx, "echo", "d-"+name, 2, fusee.DebounceOptions{Window: 500 * time.Millisecond, ID: "d2"})
	check(err == nil && first.ID == latest.ID, "debounce joined %v %v: %v", first, latest, err)
	check(first.Started(ctx) == nil, "debounced run never started")
	v, err = first.Result(ctx)
	check(err == nil && string(v) == "2", "debounced result: %s %v", v, err)
	// A repeated event ID adds nothing; the batch closes on its wait.
	var ids []string
	var events []*fusee.RemoteRun
	for _, ev := range []string{"a", "b", "b"} {
		r, err := c.Batch(ctx, "collect", "b-"+name, ev, fusee.BatchOptions{MaxEvents: 3, MaxWait: 300 * time.Millisecond, ID: ev})
		check(err == nil, "batch: %v", err)
		ids = append(ids, r.ID)
		events = append(events, r)
	}
	check(ids[0] == ids[1] && ids[1] == ids[2], "batched events joined %v", ids)
	check(events[0].Started(ctx) == nil, "batched run never started")
	v, err = events[0].Result(ctx)
	check(err == nil && string(v) == `["a","b"]`, "batch result: %s %v", v, err)
	// A stream read through the server, then again from where it stopped.
	_, err = c.Start(ctx, "talk", "s-"+name, 10)
	check(err == nil, "start talk: %v", err)
	var got []string
	text := func(ch fusee.Chunk) string {
		if ch.IsBytes() {
			return fmt.Sprintf("bytes:%x", ch.Bytes)
		}
		return string(ch.JSON)
	}
	r := c.Run("s-"+name).Stream("tokens", 0)
	for ch, err := range r.All(ctx) {
		check(err == nil, "stream read: %v", err)
		got = append(got, text(ch))
		if len(got) == 4 {
			break
		}
	}
	rest := c.Run("s-"+name).Stream("tokens", r.Offset())
	for ch, err := range rest.All(ctx) {
		check(err == nil, "stream read from %d: %v", r.Offset(), err)
		got = append(got, text(ch))
	}
	var want []string
	for i := 0; i < 10; i++ {
		want = append(want, fmt.Sprintf(`"t%d"`, i))
	}
	want = append(want, "bytes:0102")
	check(reflect.DeepEqual(got, want), "stream chunks: %v", got)
	check(rest.Status() == "ok" && rest.Offset() == 11, "stream end: %q at %d", rest.Status(), rest.Offset())
	// A key of an object type: a call, its retry, a one-way call, the state.
	k := c.Object("tally", name)
	for i := 0; i < 2; i++ {
		v, err := fusee.CallRef[int64](ctx, k, "add", 2, fusee.CallOptions{ID: "o1"})
		check(err == nil && v == 2, "object call %d: %d %v", i, v, err)
	}
	check(k.Send(ctx, "add", 3, fusee.CallOptions{ID: "o2"}) == nil, "object send")
	var st json.RawMessage
	for i := 0; i < 200; i++ {
		st, err = k.State(ctx)
		check(err == nil, "object state: %v", err)
		if string(st) == `{"sum":5}` {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	check(string(st) == `{"sum":5}`, "object state %s", st)
	fmt.Println("PASS Go client")
}
