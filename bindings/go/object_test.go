package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// eventually polls f until it holds.
func eventually(t *testing.T, what string, f func() bool) {
	t.Helper()
	for i := 0; i < 4000; i++ {
		if f() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func stateOf(t *testing.T, k Caller) map[string]any {
	t.Helper()
	var p json.RawMessage
	var err error
	switch r := k.(type) {
	case *ObjectRef:
		p, err = r.State(context.Background())
	case *RemoteObjectRef:
		p, err = r.State(context.Background())
	}
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]any
	if err := json.Unmarshal(p, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// cart registers the cart type of these tests; ran counts its step's runs.
func cart(t *testing.T, e *Engine, ran *atomic.Int32) *ObjectType {
	t.Helper()
	ty, err := RegisterObject(e, "cart", Handlers{
		"add": Exclusive(func(c *ObjectCtx, item string) (int, error) {
			priced, err := Step(c.Ctx, "price", func(context.Context) (string, error) {
				ran.Add(1)
				return item + "$", nil
			})
			if err != nil {
				return 0, err
			}
			items, _, err := StateGet[[]string](c.State(), "items")
			if err != nil {
				return 0, err
			}
			items = append(items, priced)
			return len(items), c.State().Set("items", items)
		}),
		"get": Shared(func(c *ObjectCtx, _ any) ([]string, error) {
			items, _, err := StateGet[[]string](c.State(), "items")
			if items == nil {
				items = []string{}
			}
			return items, err
		}),
		"fail": Exclusive(func(c *ObjectCtx, _ any) (any, error) {
			if err := c.State().Set("items", []string{"lost"}); err != nil {
				return nil, err
			}
			return nil, errors.New("out of stock")
		}),
		"key":   Shared(func(c *ObjectCtx, _ any) (string, error) { return c.ObjectKey(), nil }),
		"write": Shared(func(c *ObjectCtx, _ any) (any, error) { return nil, c.State().Set("x", 1) }),
	})
	if err != nil {
		t.Fatal(err)
	}
	return ty
}

// Calls change state, a failure changes nothing and records its bare
// message, a retry with an ID runs once, and a restart keeps the state and
// the IDs (a one-way call sent before the close runs after it).
func TestObjectStateRetriesAndRestart(t *testing.T) {
	dir := t.TempDir()
	ctx := bg(t)
	var ran atomic.Int32
	e := local(t, dir)
	k := cart(t, e, &ran).Key("u-1")
	for _, c := range []struct {
		item, id string
		want     int
	}{{"apple", "a", 1}, {"pear", "b", 2}, {"pear", "b", 2}} {
		if n, err := CallRef[int](ctx, k, "add", c.item, CallOptions{ID: c.id}); err != nil || n != c.want {
			t.Fatalf("add %s: %d %v", c.item, n, err)
		}
	}
	if ran.Load() != 2 {
		t.Fatalf("the step ran %d times", ran.Load())
	}
	if items, err := CallRef[[]string](ctx, k, "get", nil); err != nil || !reflect.DeepEqual(items, []string{"apple$", "pear$"}) {
		t.Fatalf("get: %v %v", items, err)
	}
	_, err := k.Call(ctx, "fail", nil)
	var fe *WorkflowError
	if !errors.As(err, &fe) || fe.Cause.Message != "out of stock" {
		t.Fatalf("a failed handler: %#v", err)
	}
	if _, err := k.Call(ctx, "write", nil); err == nil || !strings.Contains(err.Error(), "shared handler cannot change state") {
		t.Fatalf("a shared handler's write: %v", err)
	}
	if got := stateOf(t, k); !reflect.DeepEqual(got, map[string]any{"items": []any{"apple$", "pear$"}}) {
		t.Fatalf("state %v", got)
	}
	if key, err := CallRef[string](ctx, k, "key", nil); err != nil || key != "u-1" {
		t.Fatalf("key %q %v", key, err)
	}
	if got := stateOf(t, e.Object("cart", "nobody")); len(got) != 0 {
		t.Fatalf("a key never called: %v", got)
	}
	d, err := k.Inspect(ctx)
	if err != nil || d == nil || d.Object == nil {
		t.Fatalf("inspect %+v %v", d, err)
	}
	var st map[string]any
	if json.Unmarshal(d.Object.State, &st) != nil || len(st["items"].([]any)) != 2 {
		t.Fatalf("inspected state %s", d.Object.State)
	}
	var journal []map[string]any
	if err := json.Unmarshal(d.Journal, &journal); err != nil {
		t.Fatal(err)
	}
	// A failure is recorded as its envelope ({"message": ...}), as every SDK
	// reads it back.
	failed := false
	for _, r := range journal {
		p, _ := r["payload"].(map[string]any)
		failed = failed || (r["kind"] == "update_done" && r["status"] == "failed" && p["message"] == "out of stock")
	}
	if !failed {
		t.Fatalf("the failure is not recorded with its message: %s", d.Journal)
	}
	if err := k.Send(ctx, "add", "fig", CallOptions{ID: "c"}); err != nil {
		t.Fatal(err)
	}
	if keys, err := e.Objects(ctx, "cart", 0); err != nil || !reflect.DeepEqual(keys, []string{"u-1"}) {
		t.Fatalf("objects %v %v", keys, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = local(t, dir)
	defer e.Close()
	k = cart(t, e, &ran).Key("u-1")
	eventually(t, "the send", func() bool {
		items, _ := stateOf(t, k)["items"].([]any)
		return len(items) == 3
	})
	// Three steps ran, or four: a close that came while the send's step was
	// running leaves no result for it, and the step runs again after the
	// restart (steps are at least once).
	before := ran.Load()
	if before != 3 && before != 4 {
		t.Fatalf("the step ran %d times", before)
	}
	if n, err := CallRef[int](ctx, k, "add", "pear", CallOptions{ID: "b"}); err != nil || n != 2 {
		t.Fatalf("a retry after the restart: %d %v", n, err)
	}
	if ran.Load() != before {
		t.Fatalf("a retry after the restart ran the step again (%d times, %d before)", ran.Load(), before)
	}
	if err := k.Delete(ctx); err != nil {
		t.Fatal(err)
	}
	// The next call starts with empty state.
	if n, err := CallRef[int](ctx, k, "add", "kiwi"); err != nil || n != 1 {
		t.Fatalf("a call after the delete: %d %v", n, err)
	}
}

// Exclusive calls take turns in arrival order; a shared call does not wait
// for a running exclusive one, and reads the committed state.
func TestObjectExclusiveOrderAndShared(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var busy, overlaps atomic.Int32
	release := make(chan struct{})
	ty, err := RegisterObject(e, "doc", Handlers{
		"bump": Exclusive(func(c *ObjectCtx, who int) (int, error) {
			if !busy.CompareAndSwap(0, 1) {
				overlaps.Add(1)
			}
			if _, err := Step(c.Ctx, "work", func(context.Context) (int, error) {
				time.Sleep(time.Millisecond)
				return 0, nil
			}); err != nil {
				return 0, err
			}
			seen, _, err := StateGet[[]int](c.State(), "seen")
			if err != nil {
				return 0, err
			}
			seen = append(seen, who)
			busy.Store(0)
			return len(seen), c.State().Set("seen", seen)
		}),
		"hold": Exclusive(func(c *ObjectCtx, _ any) (any, error) {
			if err := c.State().Set("held", true); err != nil {
				return nil, err
			}
			_, err := Step(c.Ctx, "hold", func(context.Context) (any, error) {
				<-release
				return nil, nil
			})
			return nil, err
		}),
		"read": Shared(func(c *ObjectCtx, _ any) (bool, error) {
			held, _, err := StateGet[bool](c.State(), "held")
			return held, err
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	k := ty.Key("d")
	places := make([]int, 30)
	var wg sync.WaitGroup
	for who := range places {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p, err := CallRef[int](ctx, k, "bump", who)
			if err != nil {
				t.Error(err)
			}
			places[who] = p
		}()
	}
	wg.Wait()
	if overlaps.Load() != 0 {
		t.Fatalf("%d exclusive calls overlapped", overlaps.Load())
	}
	var seen []int
	if err := json.Unmarshal(mustState(t, k)["seen"], &seen); err != nil {
		t.Fatal(err)
	}
	for who, place := range places {
		if seen[place-1] != who {
			t.Fatalf("call %d returned place %d but %v", who, place, seen)
		}
	}
	held := make(chan error, 1)
	go func() {
		_, err := k.Call(ctx, "hold", nil)
		held <- err
	}()
	eventually(t, "hold to start", func() bool {
		d, _ := k.Inspect(ctx)
		return d != nil && d.Object != nil && len(d.Object.Running) == 1
	})
	// The exclusive call holds the key; a shared read answers at once, with
	// the committed state.
	if v, err := CallRef[bool](ctx, k, "read", nil); err != nil || v {
		t.Fatalf("a shared read while held: %v %v", v, err)
	}
	next := make(chan int, 1)
	go func() {
		n, err := CallRef[int](ctx, k, "bump", 99)
		if err != nil {
			t.Error(err)
		}
		next <- n
	}()
	eventually(t, "bump to wait", func() bool {
		d, _ := k.Inspect(ctx)
		return d != nil && d.Object != nil && len(d.Object.Waiting) == 1 && d.Object.Waiting[0].Handler == "bump"
	})
	close(release)
	if err := <-held; err != nil {
		t.Fatal(err)
	}
	if n := <-next; n != 31 {
		t.Fatalf("the waiting call's place %d", n)
	}
	if v, err := CallRef[bool](ctx, k, "read", nil); err != nil || !v {
		t.Fatalf("a shared read after: %v %v", v, err)
	}
}

func mustState(t *testing.T, k *ObjectRef) map[string]json.RawMessage {
	t.Helper()
	p, err := k.State(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(p, &m); err != nil {
		t.Fatal(err)
	}
	return m
}

// A workflow and an object's handler call objects exactly once: calls are
// journal operations, and each handler's step ran once.
func TestObjectCallsFromWorkflowsAndObjects(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var ran atomic.Int32
	cart(t, e, &ran)
	if _, err := RegisterObject(e, "order", Handlers{
		"place": Exclusive(func(c *ObjectCtx, items []string) (int, error) {
			n := 0
			for _, i := range items {
				var err error
				if n, err = Call[int](c.Object("cart", c.ObjectKey()), "add", i); err != nil {
					return 0, err
				}
			}
			return n, c.State().Set("placed", n)
		}),
	}); err != nil {
		t.Fatal(err)
	}
	type out struct {
		A      int      `json:"a"`
		Placed int      `json:"placed"`
		Items  []string `json:"items"`
	}
	wf := Register(e, "shopper", func(c *Ctx, key string) (out, error) {
		a, err := Call[int](c.Object("cart", key), "add", "tea")
		if err != nil {
			return out{}, err
		}
		if err := c.Object("cart", key).Send("add", "milk"); err != nil {
			return out{}, err
		}
		placed, err := Call[int](c.Object("order", key), "place", []string{"jam", "bread"})
		if err != nil {
			return out{}, err
		}
		items, err := Call[[]string](c.Object("cart", key), "get", nil)
		return out{a, placed, items}, err
	})
	run, err := wf.Start(ctx, "shop-1", "s1")
	if err != nil {
		t.Fatal(err)
	}
	o, err := run.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(o.Items)
	if o.A != 1 || !reflect.DeepEqual(o.Items, []string{"bread$", "jam$", "milk$", "tea$"}) {
		t.Fatalf("%+v", o)
	}
	if ran.Load() != 4 {
		t.Fatalf("the step ran %d times", ran.Load())
	}
	d, err := run.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var journal []map[string]any
	if err := json.Unmarshal(d.Journal, &journal); err != nil {
		t.Fatal(err)
	}
	calls := 0
	for _, r := range journal {
		if r["kind"] == "call" {
			calls++
		}
	}
	if calls != 4 {
		t.Fatalf("%d call records: %s", calls, d.Journal)
	}
	// A call to a type with a bad name fails in the workflow.
	bad := Register(e, "bad_caller", func(c *Ctx, _ any) (string, error) {
		_, err := c.Object("no/type", "k").Call("x", nil)
		return fmt.Sprint(err), nil
	})
	r2, err := bad.Start(ctx, "bad-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := r2.Result(ctx); err != nil || !strings.Contains(v, "has no '/'") {
		t.Fatalf("%q %v", v, err)
	}
}

// A key's history is compacted: its journal stays short, and its state and
// the results of calls with IDs survive (a retry gets its recorded result).
func TestObjectCompactionKeepsStateAndIDs(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	var ran atomic.Int32
	ty, err := RegisterObject(e, "tally", Handlers{
		"add": Exclusive(func(c *ObjectCtx, n int) (int, error) {
			ran.Add(1)
			sum, _, err := StateGet[int](c.State(), "sum")
			if err != nil {
				return 0, err
			}
			sum += n
			return sum, c.State().Set("sum", sum)
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	k := ty.Key("t")
	for n := 1; n <= 300; n++ {
		if s, err := CallRef[int](ctx, k, "add", n, CallOptions{ID: fmt.Sprintf("id-%d", n)}); err != nil || s != n*(n+1)/2 {
			t.Fatalf("call %d: %d %v", n, s, err)
		}
	}
	d, err := k.Inspect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var journal []json.RawMessage
	if err := json.Unmarshal(d.Journal, &journal); err != nil {
		t.Fatal(err)
	}
	if len(journal) > 2*128+4 {
		t.Fatalf("%d records", len(journal))
	}
	if string(d.Object.State) != `{"sum":45150}` {
		t.Fatalf("state %s", d.Object.State)
	}
	before := ran.Load()
	if s, err := CallRef[int](ctx, k, "add", 5, CallOptions{ID: "id-5"}); err != nil || s != 15 {
		t.Fatalf("a retry from before the compaction: %d %v", s, err)
	}
	if ran.Load() != before {
		t.Fatal("a retry ran again")
	}
}
