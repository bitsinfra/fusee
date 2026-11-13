package fusee

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
)

// Payloads are encrypted before they become records: nothing on disk holds
// them, and a reopened engine with the key reads them back.
func TestEncryption(t *testing.T) {
	dir := t.TempDir()
	ctx := bg(t)
	key := []CodecKey{{ID: "k1", Key: bytes.Repeat([]byte{3}, 32)}}
	open := func() *Engine {
		e, err := Open(Options{Dir: dir, Flush: "fsync", Codec: key})
		if err != nil {
			t.Fatal(err)
		}
		return e
	}
	e := open()
	secret := Register(e, "secret", func(c *Ctx, card string) (string, error) {
		masked, err := Step(c, "mask", func(context.Context) (string, error) { return card[:4] + "-SECRETRESULT", nil })
		if err != nil {
			return "", err
		}
		note, _, err := WaitFor[string](c, "note", 0)
		return masked + "/" + note, err
	})
	run, err := secret.Start(ctx, "s-1", "4242SECRETCARD")
	if err != nil {
		t.Fatal(err)
	}
	if err := run.Signal(ctx, "note", "SECRETSIGNAL"); err != nil {
		t.Fatal(err)
	}
	if v, err := run.Result(ctx); err != nil || v != "4242-SECRETRESULT/SECRETSIGNAL" {
		t.Fatalf("%q %v", v, err)
	}
	if err := e.Close(); err != nil {
		t.Fatal(err)
	}
	_ = filepath.WalkDir(dir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		b, _ := os.ReadFile(p)
		for _, needle := range []string{"SECRETCARD", "SECRETRESULT", "SECRETSIGNAL"} {
			if bytes.Contains(b, []byte(needle)) {
				t.Errorf("%s holds %s", p, needle)
			}
		}
		return nil
	})
	e = open()
	defer e.Close()
	if v, err := RunOf[string](e, "s-1").Result(ctx); err != nil || v != "4242-SECRETRESULT/SECRETSIGNAL" {
		t.Fatalf("%q %v", v, err)
	}
	if _, err := Open(Options{Durability: "memory", Codec: []CodecKey{{ID: "short", Key: []byte{1}}}}); err == nil {
		t.Fatal("a short key was accepted")
	}
}
