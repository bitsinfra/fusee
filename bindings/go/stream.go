package fusee

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"iter"
	"sync"
	"time"
)

// Durable streams (docs/features.md#streams): chunks a run appends to a
// named stream (a model's tokens, progress events) and readers that follow
// them from an offset.
//
//	answer, err := fusee.Step(c, "generate", func(ctx context.Context) (string, error) {
//		out, err := fusee.Stream(ctx, "tokens")
//		if err != nil {
//			return "", err
//		}
//		prev, err := fusee.Recorded[string](out) // what an earlier attempt recorded
//		if err != nil {
//			return "", err
//		}
//		text := strings.Join(prev, "")
//		for token := range model.Stream(prompt, text) {
//			if err := out.Append(token); err != nil {
//				return "", err
//			}
//			text += token
//		}
//		return text, nil
//	})
//
//	r := engine.Run("q-1").Stream("tokens", lastSeen)
//	for chunk, err := range r.All(ctx) { ... } // r.Offset(): where to reconnect
//
// Appending never waits: from a step's goroutine an append is queued for
// the engine's loop (appends of one step stay in order) and goes to the log
// with whatever flushes next. Readers see a chunk only once it is durable,
// so a crash never takes back a chunk a reader saw. A step that runs again
// keeps the chunks earlier attempts recorded and continues after them
// (Recorded); workflow code that replays appends nothing.

// StreamWriter appends to one of the run's streams. Opened in workflow code
// (Ctx.Stream) it writes the run's stream, which Close or the run's end
// completes; opened in a step's function (Stream), the step's stream, which
// the step's final result completes. A writer opened in a step's function
// belongs to that attempt; one opened in workflow code is used from
// workflow code (and inline steps).
type StreamWriter struct {
	x    *runExec
	name string
	// The step whose function opened it; nil for workflow code.
	step     *stepInfo
	recorded uint64
	closed   bool
	mu       sync.Mutex
	// The first failure of an append queued for the loop, returned by the
	// next Append or Close.
	err error
}

var errAttemptOver = &Error{Code: "Failed", Message: "the step's attempt is over (it timed out or the run ended); its stream operations are dropped"}

// Stream opens the run's stream name from workflow code: its appends are
// recorded once (a replay appends nothing) and Close or the run's end
// completes it. The run's steps may append to it too: a step opens it with
// fusee.Stream(ctx, name).
func (c *Ctx) Stream(name string) (*StreamWriter, error) {
	r, _, err := c.host(opStreamOpen, noSeq, 0, 0, []byte(name))
	if err != nil {
		return nil, err
	}
	return &StreamWriter{x: c.x, name: name, recorded: r[0], closed: r[1] != 0}, nil
}

// Stream opens stream name from a step's function (ctx is the context the
// function received). A stream nothing else opened belongs to the step,
// and the step's final result completes it; one workflow code opened takes
// the step's appends too. In a step that runs again (a retry, or after a
// crash) RecordedCount and Recorded give what its earlier attempts recorded:
// continue after it, so readers see no chunk twice. If an earlier attempt
// completed the stream, Closed is true and appends are dropped.
func Stream(ctx context.Context, name string) (*StreamWriter, error) {
	s := stepOf(ctx)
	if s == nil {
		return nil, &Error{Code: "Invalid", Message: "fusee.Stream needs a step function's context; in workflow code use Ctx.Stream"}
	}
	w := &StreamWriter{x: s.x, name: name, step: s}
	err := s.within(func() error {
		if !s.live() {
			return errAttemptOver
		}
		x := s.x
		r, _, err := x.e.n.host(x.shard, x.run, opStreamOpen, uint64(s.seq), 0, 0, str(name), str(""), str(""))
		w.recorded, w.closed = r[0], r[1] != 0
		return x.note(err)
	})
	if err != nil {
		return nil, err
	}
	return w, nil
}

// Name is the stream's name.
func (w *StreamWriter) Name() string { return w.name }

// RecordedCount is how many chunks the step's earlier attempts recorded
// (after a crash, the durable ones); 0 on a first attempt and in workflow
// code.
func (w *StreamWriter) RecordedCount() uint64 { return w.recorded }

// Closed reports whether the stream was complete when it was opened: an
// earlier attempt of the step completed it (appends are dropped), or its
// writer closed it.
func (w *StreamWriter) Closed() bool { return w.closed }

// Append appends a chunk: []byte as bytes, anything else as JSON. It never
// waits for the disk; from a step's goroutine it does not wait for the
// engine either (a failure is returned by the next Append or Close).
func (w *StreamWriter) Append(v any) error {
	p, err := encode(v)
	if err != nil {
		return err
	}
	s := w.step
	if s == nil {
		return w.direct(opStreamAppend, w.workflowSeq(), p)
	}
	if err := w.failure(); err != nil {
		return err
	}
	if w.closed {
		return nil
	}
	if s.inline {
		if !s.live() {
			return errAttemptOver
		}
		return w.direct(opStreamAppend, uint64(s.seq), p)
	}
	w.x.e.queueAppend(w, p)
	return nil
}

// Close completes the stream: readers end once they have every chunk.
func (w *StreamWriter) Close() error {
	s := w.step
	if s == nil {
		return w.direct(opStreamClose, w.workflowSeq(), nil)
	}
	// After the appends queued before it (the loop runs them in order).
	err := s.within(func() error {
		if !s.live() {
			return errAttemptOver
		}
		return w.direct(opStreamClose, uint64(s.seq), nil)
	})
	if first := w.failure(); first != nil {
		return first
	}
	return err
}

// Recorded decodes the chunks a step's earlier attempts recorded to the
// stream (RecordedCount of them): a []byte or any for a chunk of bytes,
// JSON into anything else. Nil on a first attempt and in workflow code.
func Recorded[T any](w *StreamWriter) ([]T, error) {
	raw, err := w.RecordedRaw()
	if err != nil {
		return nil, err
	}
	out := make([]T, 0, len(raw))
	for _, p := range raw {
		v, err := decode[T](p)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

// RecordedRaw is Recorded as recorded payloads: JSON text, or a zero byte
// and the bytes.
func (w *StreamWriter) RecordedRaw() ([][]byte, error) {
	s := w.step
	if s == nil || w.recorded == 0 {
		return nil, nil
	}
	var out [][]byte
	err := s.within(func() error {
		x := w.x
		if x.ended {
			return errEnded
		}
		_, data, err := x.e.n.host(x.shard, x.run, opStreamRecorded, uint64(s.seq), w.recorded, 0, str(w.name), str(""), str(""))
		if err != nil {
			return err
		}
		out = unframe(data)
		return nil
	})
	return out, err
}

// unframe copies payloads framed as a 4-byte little-endian length and the
// payload each.
func unframe(b []byte) [][]byte {
	var out [][]byte
	for len(b) >= 4 {
		n := int(binary.LittleEndian.Uint32(b))
		if n > len(b)-4 {
			break
		}
		out = append(out, append([]byte(nil), b[4:4+n]...))
		b = b[4+n:]
	}
	return out
}

// workflowSeq is the writer of a workflow-code writer's operation: the
// inline step running now (a step appending to the run's stream), else
// workflow code. The caller must hold the engine as the run's code.
func (w *StreamWriter) workflowSeq() uint64 {
	x := w.x
	if x.current >= 0 {
		return uint64(x.current)
	}
	if t := x.e.running; t == nil || t.x != x {
		panic("fusee: a stream opened in workflow code was used outside the run's code (open it in a step with fusee.Stream(ctx, name))")
	}
	return noSeq
}

// direct runs a stream operation holding the engine.
func (w *StreamWriter) direct(op uint32, seq uint64, p []byte) error {
	x := w.x
	if x.ended {
		return errEnded
	}
	_, _, err := x.e.n.host(x.shard, x.run, op, seq, 0, 0, str(w.name), bin(p), str(""))
	x.e.kicked = true
	return x.note(err)
}

func (w *StreamWriter) failure() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.err
}

func (w *StreamWriter) fail(err error) {
	w.mu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.mu.Unlock()
}

// ----------------------------------------------------- the append queue

// streamQueue holds the appends step goroutines made since the loop last
// took them; one posted flush takes all of them, in order.
type streamQueue struct {
	mu     sync.Mutex
	ops    []streamOp
	spare  []streamOp
	posted bool
}

type streamOp struct {
	w     *StreamWriter
	chunk []byte
}

func (e *Engine) queueAppend(w *StreamWriter, p []byte) {
	q := &e.streams
	q.mu.Lock()
	q.ops = append(q.ops, streamOp{w, p})
	post := !q.posted
	q.posted = true
	q.mu.Unlock()
	if post {
		e.post(e.flushStreams)
	}
}

// flushStreams records the queued appends, holding the engine. An append
// queued before anything else a step's goroutine hands the loop (its
// result, a Close) is recorded before it.
func (e *Engine) flushStreams() {
	q := &e.streams
	q.mu.Lock()
	ops := q.ops
	q.ops, q.spare = q.spare[:0], nil
	q.posted = false
	q.mu.Unlock()
	for i := range ops {
		op := &ops[i]
		s := op.w.step
		if !s.live() {
			op.w.fail(errAttemptOver)
			continue
		}
		if err := op.w.direct(opStreamAppend, uint64(s.seq), op.chunk); err != nil {
			op.w.fail(err)
		}
	}
	clear(ops)
	q.mu.Lock()
	q.spare = ops[:0]
	q.mu.Unlock()
}

// --------------------------------------------------------------- readers

// Chunk is one chunk of a stream as a reader receives it.
type Chunk struct {
	// Its place in the stream: 0, 1, 2, ...
	Offset uint64
	// The chunk's JSON; nil for a chunk of bytes.
	JSON json.RawMessage
	// The chunk's bytes; nil for a JSON chunk.
	Bytes []byte
}

// IsBytes reports whether the chunk was appended as bytes.
func (c Chunk) IsBytes() bool { return c.JSON == nil }

// Value is the chunk as a value: its bytes, or its JSON decoded into maps,
// slices, strings, float64s and bools.
func (c Chunk) Value() (any, error) {
	if c.IsBytes() {
		return c.Bytes, nil
	}
	var v any
	err := json.Unmarshal(c.JSON, &v)
	return v, err
}

// ChunkAs decodes a chunk into a T ([]byte or any for a chunk of bytes).
func ChunkAs[T any](c Chunk) (T, error) {
	if c.IsBytes() {
		p := make([]byte, len(c.Bytes)+1)
		copy(p[1:], c.Bytes)
		return decode[T](p)
	}
	return decode[T](c.JSON)
}

// StreamEnd is how a stream ended.
type StreamEnd struct {
	// "ok" for a stream its writer closed or whose step succeeded; the
	// step's status ("failed", "cancelled") when its step ended otherwise;
	// the run's status when the run ended with the stream open.
	Status string `json:"status"`
	// A writer or step completed it (rather than the run's end).
	Closed bool `json:"closed"`
}

// StreamReader follows a run's stream: Next returns each chunk once it is
// durable, in order, and reports the end once the stream is complete.
// Offset is where a reader that reconnects continues. Not safe for
// concurrent use.
type StreamReader struct {
	read func(ctx context.Context, run int64, from, waitMs uint64) ([]byte, error)
	run  int64
	next uint64
	wait time.Duration
	buf  []Chunk
	end  *StreamEnd
}

// waitDefault bounds one read (a long poll), not the reader.
const waitDefault = 30 * time.Second

func newStreamReader(read func(ctx context.Context, run int64, from, waitMs uint64) ([]byte, error), from uint64) *StreamReader {
	return &StreamReader{read: read, run: -1, next: from, wait: waitDefault}
}

// Stream reads the run's stream name from offset from (0: the first
// chunk), in this process.
func (r *Run[Out]) Stream(name string, from uint64) *StreamReader {
	e, id := r.e, r.ID
	return newStreamReader(func(ctx context.Context, run int64, from, waitMs uint64) ([]byte, error) {
		return e.request(ctx, true, func(tok uint64) error { return e.n.streamRead(id, run, name, from, waitMs, tok) })
	}, from)
}

// Stream reads the run's stream name from offset from, answered by the
// worker holding its shard.
func (r *RemoteRun) Stream(name string, from uint64) *StreamReader {
	c, id := r.c, r.ID
	return newStreamReader(func(ctx context.Context, run int64, from, waitMs uint64) ([]byte, error) {
		var rv any
		if run >= 0 {
			rv = run
		}
		return c.Call(ctx, "streamRead", map[string]any{"id": id, "run": rv, "name": name, "from": from, "waitMs": waitMs})
	}, from)
}

// Wait sets how long one read waits for a new chunk before it asks again
// (default 30 s); it only bounds a request, not the reader.
func (r *StreamReader) Wait(d time.Duration) *StreamReader {
	r.wait = d
	return r
}

// OfRun reads the stream of run (an earlier reader's RunID), not the
// workflow ID's newest run.
func (r *StreamReader) OfRun(run uint64) *StreamReader {
	r.run = int64(run)
	return r
}

// Offset is the offset of the chunk Next returns next.
func (r *StreamReader) Offset() uint64 {
	if len(r.buf) > 0 {
		return r.buf[0].Offset
	}
	return r.next
}

// RunID is the run being read, once the first read found it (else 0).
func (r *StreamReader) RunID() uint64 {
	if r.run < 0 {
		return 0
	}
	return uint64(r.run)
}

// End is how the stream ended, once Next reported the end (else nil).
func (r *StreamReader) End() *StreamEnd {
	if len(r.buf) > 0 {
		return nil
	}
	return r.end
}

// Status is End's status, or "" before the end.
func (r *StreamReader) Status() string {
	if e := r.End(); e != nil {
		return e.Status
	}
	return ""
}

type wireBatch struct {
	Run    uint64            `json:"run"`
	From   uint64            `json:"from"`
	Next   uint64            `json:"next"`
	Chunks []json.RawMessage `json:"chunks"`
	Bytes  []int             `json:"bytes"`
	End    *StreamEnd        `json:"end"`
}

// Next returns the next chunk, waiting until one is durable; ok is false
// once the stream is complete and every chunk was returned.
func (r *StreamReader) Next(ctx context.Context) (c Chunk, ok bool, err error) {
	for {
		if len(r.buf) > 0 {
			c = r.buf[0]
			r.buf[0] = Chunk{}
			r.buf = r.buf[1:]
			return c, true, nil
		}
		if r.end != nil {
			return Chunk{}, false, nil
		}
		if err := ctx.Err(); err != nil {
			return Chunk{}, false, err
		}
		p, err := r.read(ctx, r.run, r.next, ms(r.wait))
		if err != nil {
			return Chunk{}, false, err
		}
		var b wireBatch
		if err := json.Unmarshal(p, &b); err != nil {
			return Chunk{}, false, fmt.Errorf("malformed stream read: %w", err)
		}
		r.run = int64(b.Run)
		bytes := 0
		for i, v := range b.Chunks {
			ch := Chunk{Offset: b.From + uint64(i)}
			if bytes < len(b.Bytes) && b.Bytes[bytes] == i {
				bytes++
				if err := json.Unmarshal(v, &ch.Bytes); err != nil {
					return Chunk{}, false, fmt.Errorf("malformed stream chunk %d: %w", ch.Offset, err)
				}
				if ch.Bytes == nil {
					ch.Bytes = []byte{}
				}
			} else {
				ch.JSON = v
			}
			r.buf = append(r.buf, ch)
		}
		r.next = max(r.next, b.Next)
		r.end = b.End
	}
}

// All iterates over the chunks from the reader's offset until the stream is
// complete; a failed read is the last pair.
func (r *StreamReader) All(ctx context.Context) iter.Seq2[Chunk, error] {
	return func(yield func(Chunk, error) bool) {
		for {
			c, ok, err := r.Next(ctx)
			if err != nil {
				yield(Chunk{}, err)
				return
			}
			if !ok || !yield(c, nil) {
				return
			}
		}
	}
}
