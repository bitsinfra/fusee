package fusee

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// Invoked workers in the Go SDK: a Fusee server started with --invoke POSTs
// to a net/http endpoint whose handler opens a fresh engine per invocation;
// workflows sleep, take signals and finish with no engine open between
// invocations.

var invokeSecret = []byte("an invocation secret of 32 bytes")

func sign(body []byte, t time.Time) string {
	mac := hmac.New(sha256.New, invokeSecret)
	fmt.Fprintf(mac, "%d.", t.UnixMilli())
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", t.UnixMilli(), hex.EncodeToString(mac.Sum(nil)))
}

func TestInvocationSignatures(t *testing.T) {
	body := []byte(`{"version":1}`)
	now := time.Now()
	if err := VerifySignature(invokeSecret, sign(body, now), body, now); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		header string
		body   []byte
		want   string
	}{
		{sign(body, now), []byte("{}"), "does not match"},
		{sign(body, now.Add(-10*time.Minute)), body, "too old"},
		{"v1=00", body, "no timestamp"},
	} {
		if err := VerifySignature(invokeSecret, c.header, c.body, now); err == nil || !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%q: %v", c.want, err)
		}
	}
}

func TestInvocationRefusesBadRequestsBeforeConnecting(t *testing.T) {
	opened := 0
	h, err := NewInvocationHandler(InvocationOptions{Address: "127.0.0.1:1", Secret: invokeSecret,
		Register: func(*Engine) error { opened++; return nil }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewInvocationHandler(InvocationOptions{Address: "x", Secret: []byte("short"),
		Register: func(*Engine) error { return nil }}); err == nil {
		t.Fatal("a short secret was accepted")
	}
	body := []byte(`{"version":1,"invocation":"00000000000000aa","shards":[0],"budgetMs":1000}`)
	ctx := context.Background()
	if s, _ := h.Invoke(ctx, "", body); s != http.StatusUnauthorized {
		t.Fatalf("unsigned: %d", s)
	}
	if s, _ := h.Invoke(ctx, sign(body, time.Now()), append(body, ' ')); s != http.StatusUnauthorized {
		t.Fatalf("a changed body: %d", s)
	}
	if s, _ := h.Invoke(ctx, sign([]byte("{}"), time.Now()), []byte("{}")); s != http.StatusBadRequest {
		t.Fatalf("no invocation: %d", s)
	}
	if opened != 0 {
		t.Fatalf("%d engines opened", opened)
	}
}

func TestInvokedWorkflowsSleepSignalFinish(t *testing.T) {
	cli := os.Getenv("FUSEE_CLI")
	if cli == "" {
		cli = filepath.Join("..", "..", "target", "release", "fusee")
	}
	if _, err := os.Stat(cli); err != nil {
		t.Skipf("no %s", cli)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "secret"), invokeSecret, 0o600); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := l.Addr().String()
	l.Close()
	var mu sync.Mutex
	opened := 0
	steps := map[string]int{}
	count := func(k string) {
		mu.Lock()
		steps[k]++
		mu.Unlock()
	}
	h, err := NewInvocationHandler(InvocationOptions{Address: address, Secret: invokeSecret,
		Register: func(e *Engine) error {
			mu.Lock()
			opened++
			mu.Unlock()
			Register(e, "sleeper", func(c *Ctx, n int) (int, error) {
				a, err := Step(c, "first", func(context.Context) (int, error) {
					count(c.WorkflowID() + "/first")
					return n + 1, nil
				})
				if err != nil {
					return 0, err
				}
				if err := c.Sleep("nap", 1200*time.Millisecond); err != nil {
					return 0, err
				}
				g, _, err := WaitFor[int](c, "go", 0)
				if err != nil {
					return 0, err
				}
				return Step(c, "second", func(context.Context) (int, error) {
					count(c.WorkflowID() + "/second")
					return a*10 + g, nil
				})
			})
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	hl, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go srv.Serve(hl)
	defer srv.Close()
	server := exec.Command(cli, "server", "--dir", filepath.Join(dir, "server"), "--listen", address, "--shards", "2",
		"--invoke", "http://"+hl.Addr().String()+"/invoke", "--invoke-secret-file", filepath.Join(dir, "secret"))
	server.Stderr = os.Stderr
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = server.Process.Kill()
		_ = server.Wait()
	}()
	var c *Client
	for i := 0; i < 200; i++ {
		if c, err = Connect(address); err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := bg(t)
	ids := []string{"s-0", "s-1", "s-2"}
	for k, id := range ids {
		if _, err := c.Start(ctx, "sleeper", id, k); err != nil {
			t.Fatal(err)
		}
	}
	// The runs sleep with nobody up, then wait for their signals.
	time.Sleep(2500 * time.Millisecond)
	for k, id := range ids {
		if err := c.Run(id).Signal(ctx, "go", k+5); err != nil {
			t.Fatal(err)
		}
		v, err := c.Run(id).Result(ctx)
		if err != nil || string(v) != fmt.Sprint((k+1)*10+k+5) {
			t.Fatalf("%s: %s %v", id, v, err)
		}
	}
	mu.Lock()
	for _, id := range ids {
		if steps[id+"/first"] != 1 || steps[id+"/second"] != 1 {
			t.Fatalf("%s: steps ran %v", id, steps)
		}
	}
	n := opened
	mu.Unlock()
	if n < 2 {
		t.Fatalf("%d engines", n)
	}
	m, err := c.Call(ctx, "invocations", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	var metrics struct {
		Endpoints []struct {
			Failed, TimedOut, Unleased, Invocations int
		} `json:"endpoints"`
	}
	if err := json.Unmarshal(m, &metrics); err != nil || len(metrics.Endpoints) != 1 {
		t.Fatalf("%s %v", m, err)
	}
	e := metrics.Endpoints[0]
	if e.Failed+e.TimedOut+e.Unleased != 0 {
		t.Fatalf("%s", m)
	}
	t.Logf("%d engines for %d invocations", n, e.Invocations)
}
