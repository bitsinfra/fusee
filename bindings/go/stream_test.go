package fusee

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// readAll reads a stream to its end: each chunk as text (JSON, or
// "bytes:<hex>"), and the offsets it came at.
func readAll(t *testing.T, ctx context.Context, r *StreamReader) ([]string, []uint64) {
	t.Helper()
	var got []string
	var at []uint64
	for c, err := range r.All(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		if c.IsBytes() {
			got = append(got, fmt.Sprintf("bytes:%x", c.Bytes))
		} else {
			got = append(got, string(c.JSON))
		}
		at = append(at, c.Offset)
	}
	return got, at
}

func tokens(from, to int) []string {
	var out []string
	for i := from; i < to; i++ {
		out = append(out, fmt.Sprintf(`"t%d"`, i))
	}
	return out
}

// journalKinds counts a run's journal records by kind.
func journalKinds(t *testing.T, e *Engine, id string) map[string]int {
	t.Helper()
	d, err := e.Run(id).Inspect(context.Background())
	if err != nil || d == nil {
		t.Fatalf("inspect %s: %v", id, err)
	}
	var recs []map[string]any
	if err := json.Unmarshal(d.Journal, &recs); err != nil {
		t.Fatal(err)
	}
	n := map[string]int{}
	for _, r := range recs {
		n[r["kind"].(string)]++
	}
	return n
}

// A reader that follows a step's stream while it grows gets every chunk,
// in order, each once, JSON and bytes, and then the end; the run's own
// stream carries workflow code's appends.
func TestStreamOrderAndCompleteness(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	const n = 2000
	release := make(chan struct{})
	gen := Register(e, "gen", func(c *Ctx, _ any) (int, error) {
		p, err := c.Stream("progress")
		if err != nil {
			return 0, err
		}
		if err := p.Append("start"); err != nil {
			return 0, err
		}
		v, err := Step(c, "generate", func(ctx context.Context) (int, error) {
			out, err := Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			if out.RecordedCount() != 0 || out.Closed() {
				return 0, fmt.Errorf("a first attempt found %d chunks", out.RecordedCount())
			}
			for i := 0; i < n; i++ {
				if i == n/2 {
					<-release
				}
				if err := out.Append(fmt.Sprintf("t%d", i)); err != nil {
					return 0, err
				}
			}
			return n, out.Append([]byte{0, 1, 2})
		})
		if err != nil {
			return 0, err
		}
		if err := p.Append(map[string]int{"n": v}); err != nil {
			return 0, err
		}
		return v, p.Close()
	})
	run, err := gen.Start(ctx, "gen-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := run.Stream("tokens", 0)
	// The first half arrives while the step still runs.
	for i := 0; i < n/2; i++ {
		c, ok, err := r.Next(ctx)
		if err != nil || !ok {
			t.Fatalf("chunk %d: %v %v", i, ok, err)
		}
		v, err := ChunkAs[string](c)
		if c.Offset != uint64(i) || err != nil || v != fmt.Sprintf("t%d", i) {
			t.Fatalf("chunk %d: %+v %q %v", i, c, v, err)
		}
	}
	if r.Status() != "" {
		t.Fatalf("ended early: %s", r.Status())
	}
	close(release)
	got, at := readAll(t, ctx, r)
	want := append(tokens(n/2, n), "bytes:000102")
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("%d chunks, last %v", len(got), got[max(0, len(got)-2):])
	}
	for i, o := range at {
		if o != uint64(n/2+i) {
			t.Fatalf("chunk %d at offset %d", i, o)
		}
	}
	if r.Status() != "ok" || !r.End().Closed || r.Offset() != n+1 {
		t.Fatalf("end %+v at %d", r.End(), r.Offset())
	}
	if v, err := run.Result(ctx); err != nil || v != n {
		t.Fatalf("%d %v", v, err)
	}
	p, _ := readAll(t, ctx, run.Stream("progress", 0))
	if !reflect.DeepEqual(p, []string{`"start"`, `{"n":2000}`}) {
		t.Fatalf("%v", p)
	}
	k := journalKinds(t, e, "gen-1")
	if k["chunk"] != n+3 || k["stream_end"] != 2 {
		t.Fatalf("%v", k)
	}
	// A reader of a finished run's stream, and of none.
	if _, _, err := e.Run("nobody").Stream("tokens", 0).Next(ctx); codeOf(err) != "NotFound" {
		t.Fatalf("a missing run: %v", err)
	}
	empty, _ := readAll(t, ctx, run.Stream("nothing", 0))
	if len(empty) != 0 {
		t.Fatalf("%v", empty)
	}
}

// A reader that reconnects with the offset it reached continues exactly
// there; a short wait only bounds each read.
func TestStreamReconnect(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	talk := Register(e, "talk", func(c *Ctx, n int) (int, error) {
		return Step(c, "generate", func(ctx context.Context) (int, error) {
			out, err := Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			for i := 0; i < n; i++ {
				if err := out.Append(fmt.Sprintf("t%d", i)); err != nil {
					return 0, err
				}
				time.Sleep(2 * time.Millisecond)
			}
			return n, nil
		})
	})
	run, err := talk.Start(ctx, "talk-1", 20)
	if err != nil {
		t.Fatal(err)
	}
	r := run.Stream("tokens", 0).Wait(5 * time.Millisecond)
	var got []string
	for c, err := range r.All(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(c.JSON))
		if len(got) == 7 {
			break
		}
	}
	if r.Offset() != 7 || r.RunID() == 0 {
		t.Fatalf("stopped at %d of run %d", r.Offset(), r.RunID())
	}
	rest := run.Stream("tokens", r.Offset()).OfRun(r.RunID())
	more, _ := readAll(t, ctx, rest)
	got = append(got, more...)
	if !reflect.DeepEqual(got, tokens(0, 20)) || rest.Offset() != 20 || rest.Status() != "ok" {
		t.Fatalf("%v at %d (%s)", got, rest.Offset(), rest.Status())
	}
	// A reader past the end ends at once.
	if got, _ := readAll(t, ctx, run.Stream("tokens", 20)); len(got) != 0 {
		t.Fatalf("%v", got)
	}
}

// A step that runs again continues after what its failed attempt
// recorded; workflow code's appends are recorded once, also when its code
// replays after the engine reopened.
func TestStreamRetryAndReplay(t *testing.T) {
	dir := t.TempDir()
	ctx := bg(t)
	var attempts atomic.Int32
	register := func(e *Engine) *Workflow[any, string] {
		return Register(e, "agent", func(c *Ctx, _ any) (string, error) {
			events, err := c.Stream("events")
			if err != nil {
				return "", err
			}
			if err := events.Append("plan"); err != nil {
				return "", err
			}
			text, err := Step(c, "generate", func(ctx context.Context) (string, error) {
				attempts.Add(1)
				out, err := Stream(ctx, "tokens")
				if err != nil {
					return "", err
				}
				prev, err := Recorded[string](out)
				if err != nil {
					return "", err
				}
				if uint64(len(prev)) != out.RecordedCount() {
					return "", fmt.Errorf("%d recorded, %d returned", out.RecordedCount(), len(prev))
				}
				if len(prev) == 0 {
					_ = out.Append("a")
					_ = out.Append("b")
					return "", errors.New("cut off")
				}
				// The step appends to the run's stream as well.
				ev, err := Stream(ctx, "events")
				if err != nil {
					return "", err
				}
				if err := ev.Append("generated"); err != nil {
					return "", err
				}
				return strings.Join(prev, "") + "c", out.Append("c")
			}, StepOptions{Retry: &RetryPolicy{Attempts: 3, Initial: time.Millisecond, Max: time.Millisecond}})
			if err != nil {
				return "", err
			}
			if err := events.Append("waiting"); err != nil {
				return "", err
			}
			if _, _, err := WaitFor[any](c, "go", 0); err != nil {
				return "", err
			}
			if err := events.Append("done"); err != nil {
				return "", err
			}
			return text, events.Close()
		})
	}
	e := local(t, dir)
	if _, err := register(e).Start(ctx, "agent-1", nil); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, e, "agent-1", "suspended", "parked")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = local(t, dir)
	defer e.Close()
	register(e)
	run := RunOf[string](e, "agent-1")
	if err := run.Signal(ctx, "go", nil); err != nil {
		t.Fatal(err)
	}
	if v, err := run.Result(ctx); err != nil || v != "abc" {
		t.Fatalf("%q %v", v, err)
	}
	if n := attempts.Load(); n != 2 {
		t.Fatalf("the step ran %d times", n)
	}
	got, _ := readAll(t, ctx, run.Stream("tokens", 0))
	if !reflect.DeepEqual(got, []string{`"a"`, `"b"`, `"c"`}) {
		t.Fatalf("tokens %v", got)
	}
	ev, _ := readAll(t, ctx, run.Stream("events", 0))
	if !reflect.DeepEqual(ev, []string{`"plan"`, `"generated"`, `"waiting"`, `"done"`}) {
		t.Fatalf("events %v", ev)
	}
	if k := journalKinds(t, e, "agent-1"); k["chunk"] != 7 || k["stream_end"] != 2 {
		t.Fatalf("%v", k)
	}
}

// Workflow code cannot write a stream a step owns; a step function's
// writer needs the step's context.
func TestStreamOwnership(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	if _, err := Stream(ctx, "x"); codeOf(err) != "Invalid" {
		t.Fatalf("outside a step: %v", err)
	}
	wf := Register(e, "owner", func(c *Ctx, _ any) (string, error) {
		if _, err := Step(c, "s", func(ctx context.Context) (int, error) {
			out, err := Stream(ctx, "mine")
			if err != nil {
				return 0, err
			}
			return 0, out.Append(1)
		}); err != nil {
			return "", err
		}
		w, err := c.Stream("mine")
		if err == nil {
			err = w.Append(2)
		}
		if err == nil {
			return "", errors.New("workflow code wrote a step's stream")
		}
		return err.Error(), nil
	})
	run, err := wf.Start(ctx, "owner-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := run.Result(ctx); err != nil || !strings.Contains(v, "closed") && !strings.Contains(v, "written by") {
		t.Fatalf("%q %v", v, err)
	}
}

// The child half of TestStreamCrash: a step streams five tokens, a reader
// in the same process sees them (so they are durable), and the process is
// killed mid-step.
func TestStreamCrashChild(t *testing.T) {
	dir := os.Getenv("FUSEE_STREAM_CRASH_DIR")
	if dir == "" {
		t.Skip("run by TestStreamCrash")
	}
	e := local(t, dir)
	wf := crashWorkflow(e, func(ctx context.Context, out *StreamWriter) error {
		for i := 0; i < 5; i++ {
			if err := out.Append(fmt.Sprintf("t%d", i)); err != nil {
				return err
			}
		}
		select {}
	})
	ctx := context.Background()
	run, err := wf.Start(ctx, "crash-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	r := run.Stream("tokens", 0)
	for i := 0; i < 4; i++ {
		if _, ok, err := r.Next(ctx); !ok || err != nil {
			t.Fatal(ok, err)
		}
	}
	// The reader stopped at offset 4; the parent resumes it there.
	if err := os.WriteFile(filepath.Join(dir, "offset"), []byte(strconv.FormatUint(r.Offset(), 10)), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

func crashWorkflow(e *Engine, body func(ctx context.Context, out *StreamWriter) error) *Workflow[any, int] {
	return Register(e, "crash", func(c *Ctx, _ any) (int, error) {
		return Step(c, "generate", func(ctx context.Context) (int, error) {
			out, err := Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			return int(out.RecordedCount()), body(ctx, out)
		})
	})
}

// A process killed mid-stream: the step runs again after the restart,
// finds the durable chunks and continues after them, and a reader that
// resumes at its offset sees every chunk once.
func TestStreamCrash(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestStreamCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), "FUSEE_STREAM_CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || !strings.Contains(exit.String(), "killed") {
		t.Fatalf("the child was not killed: %v\n%s", err, out)
	}
	b, err := os.ReadFile(filepath.Join(dir, "offset"))
	if err != nil {
		t.Fatal(err)
	}
	offset, _ := strconv.ParseUint(string(b), 10, 64)
	ctx := bg(t)
	e := local(t, dir)
	defer e.Close()
	var recorded []string
	crashWorkflow(e, func(ctx context.Context, out *StreamWriter) error {
		prev, err := Recorded[string](out)
		if err != nil {
			return err
		}
		recorded = prev
		for i := len(prev); i < 10; i++ {
			if err := out.Append(fmt.Sprintf("t%d", i)); err != nil {
				return err
			}
		}
		return nil
	})
	run := RunOf[int](e, "crash-1")
	n, err := run.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// What the child's reader saw was durable, so the retry found it.
	if n < int(offset) || len(recorded) != n {
		t.Fatalf("the retry found %d chunks (%v); the reader had seen %d", n, recorded, offset)
	}
	rest, at := readAll(t, ctx, run.Stream("tokens", offset))
	if !reflect.DeepEqual(rest, tokens(int(offset), 10)) || at[0] != offset {
		t.Fatalf("resumed at %d: %v", offset, rest)
	}
	all, _ := readAll(t, ctx, run.Stream("tokens", 0))
	seen := map[string]bool{}
	for _, c := range all {
		if seen[c] {
			t.Fatalf("%s twice in %v", c, all)
		}
		seen[c] = true
	}
	if !reflect.DeepEqual(all, tokens(0, 10)) {
		t.Fatalf("%v", all)
	}
}

// An attempt that timed out cannot append after the next one started.
func TestStreamStaleAttempt(t *testing.T) {
	e := memory(t)
	ctx := bg(t)
	late := make(chan error, 1)
	var tries atomic.Int32
	wf := Register(e, "slow", func(c *Ctx, _ any) (int, error) {
		return Step(c, "s", func(ctx context.Context) (int, error) {
			out, err := Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			if tries.Add(1) == 1 {
				<-ctx.Done()
				time.Sleep(20 * time.Millisecond)
				_ = out.Append("late")
				time.Sleep(20 * time.Millisecond)
				late <- out.Append("later")
				return 0, nil
			}
			return 2, out.Append("fresh")
		}, StepOptions{Timeout: 30 * time.Millisecond, Retry: &RetryPolicy{Attempts: 2, Initial: time.Millisecond, Max: time.Millisecond}})
	})
	run, err := wf.Start(ctx, "slow-1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if v, err := run.Result(ctx); err != nil || v != 2 {
		t.Fatalf("%d %v", v, err)
	}
	if err := <-late; err == nil {
		t.Fatal("a stale attempt appended")
	}
	got, _ := readAll(t, ctx, run.Stream("tokens", 0))
	if !reflect.DeepEqual(got, []string{`"fresh"`}) {
		t.Fatalf("%v", got)
	}
}

// Readers through a server: a client reads a Go worker's stream (the
// relayed streamRead call), stops, and continues from its offset.
func TestStreamServer(t *testing.T) {
	addr := fuseeServer(t)
	ctx := bg(t)
	e, err := Open(Options{Durability: "server", Address: addr})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	Register(e, "talk", func(c *Ctx, n int) (int, error) {
		return Step(c, "generate", func(ctx context.Context) (int, error) {
			out, err := Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			for i := 0; i < n; i++ {
				if err := out.Append(fmt.Sprintf("t%d", i)); err != nil {
					return 0, err
				}
				time.Sleep(3 * time.Millisecond)
			}
			return n, out.Append([]byte{1, 2})
		})
	})
	c, err := Connect(addr)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, err := c.Start(ctx, "talk", "talk-r", 12); err != nil {
		t.Fatal(err)
	}
	r := c.Run("talk-r").Stream("tokens", 0)
	var got []string
	for ch, err := range r.All(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, string(ch.JSON))
		if len(got) == 5 {
			break
		}
	}
	rest, _ := readAll(t, ctx, c.Run("talk-r").Stream("tokens", r.Offset()))
	got = append(got, rest...)
	if !reflect.DeepEqual(got, append(tokens(0, 12), "bytes:0102")) {
		t.Fatalf("%v", got)
	}
	// Several readers at once (each read is its own relayed call).
	res := make(chan error, 4)
	for i := 0; i < 4; i++ {
		go func() {
			var n uint64
			for ch, err := range c.Run("talk-r").Stream("tokens", 0).All(ctx) {
				if err != nil {
					res <- err
					return
				}
				if ch.Offset != n {
					res <- fmt.Errorf("chunk %d at offset %d", n, ch.Offset)
					return
				}
				n++
			}
			if n != 13 {
				res <- fmt.Errorf("%d chunks", n)
				return
			}
			res <- nil
		}()
	}
	for i := 0; i < 4; i++ {
		if err := <-res; err != nil {
			t.Fatal(err)
		}
	}
	// The worker's own reader, and a missing run through the server.
	mine, _ := readAll(t, ctx, e.Run("talk-r").Stream("tokens", 11))
	if !reflect.DeepEqual(mine, []string{`"t11"`, "bytes:0102"}) {
		t.Fatalf("%v", mine)
	}
	if _, _, err := c.Run("nobody").Stream("tokens", 0).Next(ctx); err == nil {
		t.Fatal("a missing run's stream")
	}
}

// Append cost and reader latency, printed (FUSEE_STREAM_COST=1; without
// the race detector for meaningful numbers): 100,000 small chunks from one
// step in memory mode, then chunks appended every 2 ms to a local engine
// (fsync) and the time from each append to a reader in the same process
// holding it.
func TestStreamCost(t *testing.T) {
	if os.Getenv("FUSEE_STREAM_COST") == "" {
		t.Skip("set FUSEE_STREAM_COST=1")
	}
	ctx := bg(t)
	e := memory(t)
	const n = 100_000
	bulk := Register(e, "bulk", func(c *Ctx, n int) (int64, error) {
		return Step(c, "generate", func(ctx context.Context) (int64, error) {
			out, err := Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			start := time.Now()
			for i := 0; i < n; i++ {
				if err := out.Append(i); err != nil {
					return 0, err
				}
			}
			return time.Since(start).Nanoseconds(), nil
		})
	})
	t0 := time.Now()
	run, err := bulk.Start(ctx, "b-1", n)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := run.Result(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Until the result: every chunk recorded by the loop as well.
	whole := time.Since(t0)
	start := time.Now()
	count := 0
	for c, err := range run.Stream("tokens", 0).All(ctx) {
		if err != nil || c.Offset != uint64(count) {
			t.Fatalf("%d: %+v %v", count, c, err)
		}
		count++
	}
	read := time.Since(start)
	fmt.Printf("go append: %.0f chunks/s (%.0f ns each; %.0f ns each until the result); read back %.0f chunks/s\n",
		float64(n)/(float64(ns)/1e9), float64(ns)/n, float64(whole.Nanoseconds())/n, float64(n)/read.Seconds())
	// fsync, as the Rust measurement (macOS's default, F_FULLFSYNC, takes
	// milliseconds).
	le, err := Open(Options{Dir: t.TempDir(), Flush: "fsync"})
	if err != nil {
		t.Fatal(err)
	}
	defer le.Close()
	const samples = 200
	sent := make([]time.Time, samples)
	paced := Register(le, "paced", func(c *Ctx, n int) (int, error) {
		return Step(c, "generate", func(ctx context.Context) (int, error) {
			out, err := Stream(ctx, "tokens")
			if err != nil {
				return 0, err
			}
			for i := 0; i < n; i++ {
				time.Sleep(2 * time.Millisecond)
				sent[i] = time.Now()
				if err := out.Append(i); err != nil {
					return 0, err
				}
			}
			return n, nil
		})
	})
	prun, err := paced.Start(ctx, "l-1", samples)
	if err != nil {
		t.Fatal(err)
	}
	var lat []time.Duration
	for c, err := range prun.Stream("tokens", 0).All(ctx) {
		if err != nil {
			t.Fatal(err)
		}
		lat = append(lat, time.Since(sent[c.Offset]))
	}
	slices.Sort(lat)
	fmt.Printf("go reader latency (append to reader, local fsync): p50 %d us, p90 %d us, p99 %d us\n",
		lat[len(lat)/2].Microseconds(), lat[len(lat)*9/10].Microseconds(), lat[len(lat)*99/100].Microseconds())
}
