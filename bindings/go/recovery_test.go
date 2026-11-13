package fusee

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func local(t *testing.T, dir string) *Engine {
	t.Helper()
	e, err := Open(Options{Dir: dir, KeepHistory: true})
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// waitStatus polls a run until it has one of the statuses.
func waitStatus(t *testing.T, e *Engine, id string, statuses ...string) *RunDetail {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 2000; i++ {
		if d, _ := e.Run(id).Inspect(ctx); d != nil {
			for _, s := range statuses {
				if d.Status == s {
					return d
				}
			}
		}
		time.Sleep(2 * time.Millisecond)
	}
	d, _ := e.Run(id).Inspect(ctx)
	t.Fatalf("%s never reached %v: %+v", id, statuses, d)
	return nil
}

func payWorkflow(e *Engine, charges *atomic.Int32) *Workflow[int, int] {
	return Register(e, "pay", func(c *Ctx, amount int) (int, error) {
		id, err := Step(c, "charge", func(ctx context.Context) (int, error) {
			charges.Add(1)
			return amount * 10, nil
		})
		if err != nil {
			return 0, err
		}
		bonus, _, err := WaitFor[int](c, "go", 0)
		if err != nil {
			return 0, err
		}
		return Step(c, "settle", func(context.Context) (int, error) { return id + bonus, nil })
	})
}

// A run left unfinished by a clean close resumes in the next engine
// without running its recorded step again.
func TestCloseAndReopen(t *testing.T) {
	dir := t.TempDir()
	ctx := bg(t)
	var charges atomic.Int32
	e := local(t, dir)
	pay := payWorkflow(e, &charges)
	if _, err := pay.Start(ctx, "pay-1", 4); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, e, "pay-1", "suspended", "parked")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := pay.Start(ctx, "pay-2", 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("a closed engine: %v", err)
	}
	e = local(t, dir)
	defer e.Close()
	payWorkflow(e, &charges)
	run := RunOf[int](e, "pay-1")
	if err := run.Signal(ctx, "go", 2); err != nil {
		t.Fatal(err)
	}
	v, err := run.Result(ctx)
	if err != nil || v != 42 {
		t.Fatalf("%d %v", v, err)
	}
	if n := charges.Load(); n != 1 {
		t.Fatalf("the charge ran %d times", n)
	}
}

// The child half of TestCrash: starts a run, waits until its step is
// durable, and kills its own process.
func TestCrashChild(t *testing.T) {
	dir := os.Getenv("FUSEE_CRASH_DIR")
	if dir == "" {
		t.Skip("run by TestCrash")
	}
	var charges atomic.Int32
	e := local(t, dir)
	pay := payWorkflow(e, &charges)
	if _, err := pay.Start(context.Background(), "crash-1", 5); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, e, "crash-1", "suspended", "parked")
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

// A process killed mid-run: the next engine finds the run, replays the
// recorded step and finishes it.
func TestCrash(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), "FUSEE_CRASH_DIR="+dir)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || !strings.Contains(exit.String(), "killed") {
		t.Fatalf("the child was not killed: %v\n%s", err, out)
	}
	ctx := bg(t)
	var charges atomic.Int32
	e := local(t, dir)
	defer e.Close()
	payWorkflow(e, &charges)
	run := RunOf[int](e, "crash-1")
	if err := run.Signal(ctx, "go", 7); err != nil {
		t.Fatal(err)
	}
	v, err := run.Result(ctx)
	if err != nil || v != 57 {
		t.Fatalf("%d %v", v, err)
	}
	if n := charges.Load(); n != 0 {
		t.Fatalf("the recorded charge ran again (%d)", n)
	}
}

// Code that changed under a recorded run stops it with a determinism error
// naming both steps; the run waits as "stuck".
func TestDeterminismError(t *testing.T) {
	dir := t.TempDir()
	ctx := bg(t)
	var charges atomic.Int32
	e := local(t, dir)
	pay := payWorkflow(e, &charges)
	if _, err := pay.Start(ctx, "pay-1", 4); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, e, "pay-1", "suspended", "parked")
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	e = local(t, dir)
	defer e.Close()
	seen := make(chan error, 4)
	Register(e, "pay", func(c *Ctx, amount int) (int, error) {
		v, err := Step(c, "refund", func(context.Context) (int, error) { return amount, nil })
		seen <- err
		return v, err
	})
	d := waitStatus(t, e, "pay-1", "stuck")
	if d.Error == nil || !strings.Contains(*d.Error, "'charge'") || !strings.Contains(*d.Error, "'refund'") {
		t.Fatalf("%+v", d)
	}
	var de *DeterminismError
	if err := <-seen; !errors.As(err, &de) {
		t.Fatalf("the code saw %v", err)
	}
}
