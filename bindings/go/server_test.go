package fusee

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// fuseeServer starts `fusee server` (FUSEE_CLI, or the repository's
// target/release/fusee) on a free port; the test is skipped without it.
func fuseeServer(t *testing.T) string {
	t.Helper()
	cli := os.Getenv("FUSEE_CLI")
	if cli == "" {
		cli, _ = filepath.Abs("../../target/release/fusee")
	}
	if _, err := os.Stat(cli); err != nil {
		t.Skipf("no fusee CLI at %s (cargo build --release -p fusee-cli)", cli)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	cmd := exec.Command(cli, "server", "--dir", t.TempDir(), "--listen", addr, "--shards", "2")
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(os.Interrupt)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(8 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
	})
	for i := 0; i < 200; i++ {
		if c, err := net.DialTimeout("tcp", addr, 100*time.Millisecond); err == nil {
			_ = c.Close()
			return addr
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the server did not listen on %s", addr)
	return ""
}

func TestServerMode(t *testing.T) {
	addr := fuseeServer(t)
	ctx := bg(t)
	e, err := Open(Options{Durability: "server", Address: addr})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	if e.Shards() != 2 {
		t.Fatalf("%d shards", e.Shards())
	}
	approval := Register(e, "approval", func(c *Ctx, initial int) ([]any, error) {
		count := initial
		var token string
		OnUpdate(c, "add", func(c *Ctx, n int) (int, error) { count += n; return count, nil }, nil)
		OnQuery(c, "count", func(any) (int, error) { return count, nil })
		OnQuery(c, "token", func(any) (string, error) { return token, nil })
		v, err := Step(c, "double", func(context.Context) (int, error) { return initial * 2, nil })
		if err != nil {
			return nil, err
		}
		// Inline: the send function writes workflow state, so it runs
		// holding the engine rather than on a goroutine of its own.
		approved, err := AwaitCompletion[bool](c, "decision", func(_ context.Context, t string) error {
			token = t
			return nil
		}, StepOptions{Inline: true})
		return []any{count, v, approved}, err
	})
	// The worker's own API.
	run, err := approval.Start(ctx, "local-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	var token string
	for i := 0; i < 500 && token == ""; i++ {
		token, _ = Query[string](ctx, run, "token", nil)
		time.Sleep(5 * time.Millisecond)
	}
	if err := e.Complete(ctx, token, true); err != nil {
		t.Fatal(err)
	}
	out, err := run.Result(ctx)
	if err != nil || fmt.Sprint(out) != "[1 2 true]" {
		t.Fatalf("%v %v", out, err)
	}
	// A client reaching this worker through the server.
	c, err := Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	v, err := c.UpdateWithStart(ctx, "approval", "remote-1", 4, "add", 3, StartOptions{UpdateID: "once"})
	if err != nil || string(v) != "7" {
		t.Fatalf("%s %v", v, err)
	}
	rr := c.Run("remote-1")
	if v, err := rr.Update(ctx, "add", 3, UpdateOptions{ID: "once"}); err != nil || string(v) != "7" {
		t.Fatalf("a repeated update: %s %v", v, err)
	}
	token = ""
	for i := 0; i < 500 && token == ""; i++ {
		p, _ := rr.Query(ctx, "token", nil)
		_ = json.Unmarshal(p, &token)
		time.Sleep(5 * time.Millisecond)
	}
	if err := c.Complete(ctx, token, true); err != nil {
		t.Fatal(err)
	}
	res, err := rr.Result(ctx)
	if err != nil || string(res) != "[7,8,true]" {
		t.Fatalf("%s %v", res, err)
	}
	runs, err := c.List(ctx, ListOptions{Where: map[string]any{"status": "ok"}})
	if err != nil || len(runs) != 2 {
		t.Fatalf("%+v %v", runs, err)
	}
	if _, err := c.Run("nope").Result(ctx); err == nil || codeOf(err) != "NotFound" {
		t.Fatalf("not found: %v", err)
	}
	// A fresh engine on the same server finds the runs (after the lease
	// moves to it).
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e2, err := Open(Options{Durability: "server", Address: addr})
	if err != nil {
		t.Fatal(err)
	}
	defer e2.Close()
	if v, err := RunOf[[]any](e2, "local-1").Result(context.Background()); err != nil || fmt.Sprint(v) != "[1 2 true]" {
		t.Fatalf("%v %v", v, err)
	}
}
