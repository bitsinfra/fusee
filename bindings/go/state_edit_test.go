package fusee

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Edits of a key's state from outside its handlers.
func TestStateEdits(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	if _, err := RegisterObject(e, "cart", Handlers{
		"add": Exclusive(func(c *ObjectCtx, item string) (int, error) {
			var items []string
			if _, err := c.State().Get("items", &items); err != nil {
				return 0, err
			}
			items = append(items, item)
			return len(items), c.State().Set("items", items)
		}),
	}); err != nil {
		t.Fatal(err)
	}
	c := e.Object("cart", "u-1")
	if _, err := c.Call(ctx, "add", "apple"); err != nil {
		t.Fatal(err)
	}
	r, err := c.SetState(ctx, "owner", "ana")
	if err != nil || !reflect.DeepEqual(r.Changed, []string{"owner"}) {
		t.Fatalf("set: %v %v", r, err)
	}
	if _, err := c.PatchState(ctx, []map[string]any{{"op": "add", "path": "/items/-", "value": "pear"}}); err != nil {
		t.Fatal(err)
	}
	state := func() map[string]any {
		p, err := c.State(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		_ = json.Unmarshal(p, &m)
		return m
	}
	if got := state(); !reflect.DeepEqual(got, map[string]any{"items": []any{"apple", "pear"}, "owner": "ana"}) {
		t.Fatalf("state: %v", got)
	}
	if _, err := c.EditState(ctx, StateEdit{IfVersion: "0000000000000000", ClearAll: true}); err == nil || !strings.Contains(err.Error(), "changed since version") {
		t.Fatalf("a stale edit: %v", err)
	}
	if _, err := c.ClearState(ctx, ""); err != nil {
		t.Fatal(err)
	}
	if got := state(); len(got) != 0 {
		t.Fatalf("cleared: %v", got)
	}
}
