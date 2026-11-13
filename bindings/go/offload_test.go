package fusee

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// Payloads over the payload offload threshold (64 KiB by default) go to the
// shared log's blob area. A 1 MiB step result recorded before a SIGKILL is
// replayed from the log after it, and a 1 MiB signal round trips exactly.

const mib = 1 << 20

func page(seed uint32, n int) string {
	x := seed | 1
	b := make([]byte, n)
	for i := range b {
		x = x*1103515245 + 12345
		b[i] = byte('a' + x%26)
	}
	return string(b)
}

type bigResult struct {
	Body     int  `json:"body"`
	Same     bool `json:"same"`
	Note     int  `json:"note"`
	NoteSame bool `json:"note_same"`
}

func bigWorkflow(e *Engine, fetched, marked *atomic.Int32) *Workflow[int, bigResult] {
	return Register(e, "big", func(c *Ctx, n int) (bigResult, error) {
		body, err := Step(c, "fetch", func(context.Context) (string, error) {
			fetched.Add(1)
			return page(7, n), nil
		})
		if err != nil {
			return bigResult{}, err
		}
		// A step's function starts only once what precedes it is durable,
		// so this one running shows that the 1 MiB result is.
		if _, err := Step(c, "mark", func(context.Context) (int, error) {
			marked.Add(1)
			return 1, nil
		}); err != nil {
			return bigResult{}, err
		}
		note, _, err := WaitFor[string](c, "note", 0)
		if err != nil {
			return bigResult{}, err
		}
		return bigResult{len(body), body == page(7, n), len(note), note == page(9, mib)}, nil
	})
}

// The child records a 1 MiB step result, then dies by SIGKILL.
func TestOffloadCrashChild(t *testing.T) {
	dir := os.Getenv("FUSEE_OFFLOAD_DIR")
	if dir == "" {
		t.Skip("run by TestOffloadSurvivesSigkill")
	}
	var fetched, marked atomic.Int32
	e := local(t, dir)
	big := bigWorkflow(e, &fetched, &marked)
	if _, err := big.Start(context.Background(), "big-1", mib); err != nil {
		t.Fatal(err)
	}
	// Reaching the WaitFor is not enough: code may run ahead of the disk.
	// The "mark" step starting is.
	for i := 0; marked.Load() == 0; i++ {
		if i == 5000 {
			t.Fatal("the step after the 1 MiB result never started")
		}
		time.Sleep(time.Millisecond)
	}
	_ = syscall.Kill(os.Getpid(), syscall.SIGKILL)
	select {}
}

func TestOffloadSurvivesSigkill(t *testing.T) {
	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestOffloadCrashChild$", "-test.count=1")
	cmd.Env = append(os.Environ(), "FUSEE_OFFLOAD_DIR="+dir)
	out, err := cmd.CombinedOutput()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || !strings.Contains(exit.String(), "killed") {
		t.Fatalf("the child was not killed: %v\n%s", err, out)
	}
	if blobs, _ := os.ReadDir(filepath.Join(dir, "wal", "blobs")); len(blobs) == 0 {
		t.Fatal("the 1 MiB step result did not go to the blob area")
	}
	ctx := bg(t)
	var fetched, marked atomic.Int32
	e := local(t, dir)
	defer e.Close()
	bigWorkflow(e, &fetched, &marked)
	run := RunOf[bigResult](e, "big-1")
	if err := run.Signal(ctx, "note", page(9, mib)); err != nil {
		t.Fatal(err)
	}
	v, err := run.Result(ctx)
	want := bigResult{mib, true, mib, true}
	if err != nil || v != want {
		t.Fatalf("%+v %v", v, err)
	}
	if n := fetched.Load(); n != 0 {
		t.Fatalf("the recorded step ran again (%d)", n)
	}
}
