package fusee

// The worker side of invocations over HTTP, suspend to zero
// (docs/server-mode.md, "Invoked workers"): a Fusee server started with
// --invoke URL POSTs to that URL when shards no worker holds have work due;
// each request opens a fresh engine for the invocation's shards, runs them
// until they are idle or the time budget ends, and closes it.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// SignatureHeader carries a request's signature: t=<unix ms>,v1=<hex
// HMAC-SHA256 of "<t>." and the body>.
const SignatureHeader = "fusee-signature"

// InvocationOptions configure an InvocationHandler.
type InvocationOptions struct {
	// Engine options for each invocation's engine (Durability and Address
	// are set by the handler; ParkAfter defaults to 50 ms, so the next
	// invocation's open stays cheap).
	Engine Options
	// The server, with this worker's connection options:
	// host:port?ca=...&token-file=...
	Address string
	// The endpoint's signing secret (the server's --invoke-secret-file); nil
	// accepts unsigned requests (only behind something that authenticates).
	Secret []byte
	// Registers the workflows on each invocation's fresh engine.
	Register func(e *Engine) error
}

// InvocationHandler answers a server's invocations; it is an http.Handler.
type InvocationHandler struct {
	opts InvocationOptions
}

// NewInvocationHandler checks the options.
func NewInvocationHandler(opts InvocationOptions) (*InvocationHandler, error) {
	if opts.Secret != nil && len(opts.Secret) < 16 {
		return nil, errors.New("fusee: an invocation secret needs at least 16 bytes; use 32 random bytes")
	}
	if opts.Address == "" || opts.Register == nil {
		return nil, errors.New("fusee: an invocation handler needs Address and Register")
	}
	return &InvocationHandler{opts: opts}, nil
}

// VerifySignature checks a fusee-signature value against the body, within
// five minutes of now.
func VerifySignature(secret []byte, header string, body []byte, now time.Time) error {
	var t int64 = -1
	var sigs []string
	for _, part := range strings.Split(header, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || v == "" {
			continue
		}
		switch k {
		case "t":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				t = n
			}
		case "v1":
			sigs = append(sigs, v)
		}
	}
	if t < 0 {
		return errors.New("the signature has no timestamp")
	}
	if d := now.UnixMilli() - t; d > 300_000 || d < -300_000 {
		return errors.New("the signature is too old (or the clocks differ by over five minutes)")
	}
	mac := hmac.New(sha256.New, secret)
	fmt.Fprintf(mac, "%d.", t)
	mac.Write(body)
	want := mac.Sum(nil)
	for _, s := range sigs {
		if got, err := hex.DecodeString(s); err == nil && hmac.Equal(got, want) {
			return nil
		}
	}
	return errors.New("the signature does not match")
}

var invocationID = regexp.MustCompile(`^[0-9a-f]{16}$`)

// InvocationResult is a handled invocation, the 200 answer's body.
type InvocationResult struct {
	Invocation string  `json:"invocation"`
	Outcome    string  `json:"outcome,omitempty"`
	Skipped    string  `json:"skipped,omitempty"`
	Steps      uint64  `json:"steps"`
	OpenMs     float64 `json:"openMs"`
	Ms         float64 `json:"ms"`
	Error      string  `json:"error,omitempty"`
}

func errorBody(msg string) []byte {
	b, _ := json.Marshal(map[string]string{"error": msg})
	return b
}

// Invoke verifies a request, runs its invocation and returns the HTTP
// status and JSON body: 200 with a summary, 401, 400 or 503.
func (h *InvocationHandler) Invoke(ctx context.Context, signature string, body []byte) (int, []byte) {
	if h.opts.Secret != nil {
		if signature == "" {
			return http.StatusUnauthorized, errorBody("unsigned request")
		}
		if err := VerifySignature(h.opts.Secret, signature, body, time.Now()); err != nil {
			return http.StatusUnauthorized, errorBody(err.Error())
		}
	}
	var req struct {
		Invocation string `json:"invocation"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return http.StatusBadRequest, errorBody(err.Error())
	}
	id := req.Invocation
	if !invocationID.MatchString(id) || strings.Trim(id, "0") == "" {
		return http.StatusBadRequest, errorBody("invalid invocation ID")
	}
	began := time.Now()
	o := h.opts.Engine
	o.Durability = "server"
	sep := "?"
	if strings.Contains(h.opts.Address, "?") {
		sep = "&"
	}
	o.Address = h.opts.Address + sep + "invocation=" + id
	if o.ParkAfter == 0 {
		o.ParkAfter = 50 * time.Millisecond
	}
	e, err := Open(o)
	if err != nil {
		// The server refused: the invocation is over or its shards are held.
		if strings.Contains(err.Error(), "refused this worker") {
			b, _ := json.Marshal(InvocationResult{Invocation: id, Skipped: err.Error()})
			return http.StatusOK, b
		}
		return http.StatusServiceUnavailable, errorBody(err.Error())
	}
	res := InvocationResult{Invocation: id, Outcome: "running", OpenMs: float64(time.Since(began).Microseconds()) / 1e3}
	if err := h.opts.Register(e); err != nil {
		res.Error = err.Error()
	} else {
		for {
			state, err := e.invocationState()
			if err != nil {
				res.Error = err.Error()
				break
			}
			if state == "" {
				state = "idle"
			}
			res.Outcome = state
			if state != "running" || ctx.Err() != nil {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if m, err := e.ShardMetrics(); err == nil {
		for _, s := range m {
			res.Steps += s["steps_run"]
		}
	}
	if err := e.Close(); err != nil && res.Error == "" {
		res.Error = err.Error()
	}
	res.Ms = float64(time.Since(began).Microseconds()) / 1e3
	b, _ := json.Marshal(res)
	return http.StatusOK, b
}

// ServeHTTP makes the handler an http.Handler (net/http and the frameworks
// built on it).
func (h *InvocationHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	status, out := http.StatusBadRequest, errorBody("")
	if err != nil {
		out = errorBody(err.Error())
	} else {
		status, out = h.Invoke(r.Context(), r.Header.Get(SignatureHeader), body)
	}
	w.Header().Set("content-type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(out)
}

// invocationState is this engine's invocation state ("" when it is not an
// invoked worker).
func (e *Engine) invocationState() (string, error) {
	var state string
	err := e.do(func() error {
		var awaiting []string
		for t, runs := range e.awaiting {
			if len(runs) > 0 {
				awaiting = append(awaiting, t)
			}
		}
		s, err := e.n.invocationState(awaiting)
		state = s
		return err
	})
	return state, err
}
