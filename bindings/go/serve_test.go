package fusee

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"
)

// Greet is a definition apart from any engine.
var Greet = Define("greet-go", func(c *Ctx, name string) (string, error) {
	g, err := Step(c, "compose", func(context.Context) (string, error) { return "hello " + name, nil })
	if err != nil {
		return "", err
	}
	ok, _, err := WaitFor[bool](c, "approve", 0)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", Fatal(errors.New("not approved"))
	}
	return g, nil
})

// A definition registered at Open runs, and the engine serves the HTTP API:
// start, signal, result, describe, types, info and a failure's envelope.
func TestDefinitionsAndTheHTTPAPI(t *testing.T) {
	e, err := Open(Options{Durability: "memory", KeepHistory: true, Workflows: []Definition{Greet}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = e.Close() })
	ctx := bg(t)
	url, err := e.Serve("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	call := func(method, path string, body any) (int, map[string]any) {
		var r io.Reader
		if body != nil {
			b, _ := json.Marshal(body)
			r = bytes.NewReader(b)
		}
		req, _ := http.NewRequestWithContext(ctx, method, url+path, r)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		raw, _ := io.ReadAll(resp.Body)
		if json.Unmarshal(raw, &out) != nil {
			out = map[string]any{"raw": string(raw)}
		}
		return resp.StatusCode, out
	}
	if s, b := call("POST", "/v1/runs", map[string]any{"type": "greet-go", "id": "g-1", "input": "ana"}); s != 200 {
		t.Fatalf("start: %d %v", s, b)
	}
	if s, b := call("POST", "/v1/runs/g-1/signals/approve", true); s != 200 {
		t.Fatalf("signal: %d %v", s, b)
	}
	if s, b := call("GET", "/v1/runs/g-1/result", nil); s != 200 || b["value"] != "hello ana" {
		t.Fatalf("result: %d %v", s, b)
	}
	if s, b := call("GET", "/v1/runs/g-1", nil); s != 200 || b["status"] != "ok" {
		t.Fatalf("describe: %d %v", s, b)
	}
	if s, b := call("GET", "/v1/info", nil); s != 200 || b["sdk"] != "go" {
		t.Fatalf("info: %d %v", s, b)
	}
	// A failed run's result carries the envelope.
	run, err := Greet.On(e).Start(ctx, "g-2", "bo")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Signal(ctx, "approve", false); err != nil {
		t.Fatal(err)
	}
	if _, err := run.Result(ctx); err == nil {
		t.Fatal("expected a failure")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		s, b := call("GET", "/v1/runs/g-2/result", nil)
		if b["status"] == "failed" {
			f, _ := b["error"].(map[string]any)
			if f["message"] != "not approved" || f["retryable"] != false {
				t.Fatalf("failure: %d %v", s, b)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("result: %d %v", s, b)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
