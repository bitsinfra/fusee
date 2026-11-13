package fusee

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"strings"
)

// Failure is a failure as every SDK records and reads it, the failure
// envelope (docs/errors.md#the-failure-envelope). Return one from a step
// or a workflow to give the failure a type, a code and details that reach
// the caller in any language; Fatal makes one that is never retried.
// A step's or workflow's other errors are recorded as their message and
// type name, with the errors they wrap as causes.
type Failure struct {
	Message   string          `json:"message"`
	Type      string          `json:"type,omitempty"`
	Code      string          `json:"code,omitempty"`
	Details   json.RawMessage `json:"details,omitempty"`
	Retryable *bool           `json:"retryable,omitempty"`
	Stack     string          `json:"stack,omitempty"`
	Cause     *Failure        `json:"cause,omitempty"`
}

func (f *Failure) Error() string { return f.Message }

// Unwrap is the failure's cause.
func (f *Failure) Unwrap() error {
	if f.Cause == nil {
		return nil
	}
	return f.Cause
}

// Final reports whether no retry policy retries the failure.
func (f *Failure) Final() bool {
	for c := f; c != nil; c = c.Cause {
		if c.Retryable != nil && !*c.Retryable {
			return true
		}
	}
	return false
}

// Fatal is err as a failure no retry policy retries: a declined card, an
// invalid request.
func Fatal(err error) *Failure {
	f := FailureOf(err)
	no := false
	f.Retryable = &no
	return f
}

// FailureOf is err as the envelope it is recorded as: a *Failure as it
// is, Fusee's own errors under their codes, and any other error as its
// message, its type name (not for plain errors) and the error it wraps as
// its cause.
func FailureOf(err error) *Failure {
	if err == nil {
		return nil
	}
	var f *Failure
	if errors.As(err, &f) && f == err {
		c := *f
		return &c
	}
	var se *StepError
	if errors.As(err, &se) && se == err {
		return &Failure{Message: se.Error(), Type: "StepFailed", Cause: se.cause()}
	}
	var we *WorkflowError
	if errors.As(err, &we) && we == err {
		return &Failure{Message: we.Error(), Type: we.code(), Cause: we.Cause}
	}
	out := &Failure{Message: err.Error(), Type: typeName(err)}
	if inner := errors.Unwrap(err); inner != nil {
		out.Cause = FailureOf(inner)
	}
	return out
}

// typeName is an error's type name, or "" for Go's plain error types.
func typeName(err error) string {
	t := reflect.TypeOf(err)
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	switch t.PkgPath() + "." + t.Name() {
	case "errors.errorString", "fmt.wrapError", "fmt.wrapErrors", "errors.joinError", ".":
		return ""
	}
	return t.Name()
}

// encodeFailure is err's envelope as recorded.
func encodeFailure(err error) []byte {
	b, e := json.Marshal(FailureOf(err))
	if e != nil {
		b, _ = json.Marshal(&Failure{Message: err.Error()})
	}
	return b
}

// parseFailure reads a recorded failure: an envelope, or (recorded before
// the envelope) its text.
func parseFailure(p []byte) *Failure {
	text := payloadText(p)
	if strings.HasPrefix(text, "{") {
		var f Failure
		if json.Unmarshal([]byte(text), &f) == nil && (f.Message != "" || f.Type != "") {
			return &f
		}
	}
	return &Failure{Message: text}
}

// WorkflowError is a workflow's, a child's or an update's failure, with
// the failure its code returned as Cause.
type WorkflowError struct {
	// "WorkflowFailed" or "ChildFailed".
	Code  string
	ID    string
	Cause *Failure
}

func (e *WorkflowError) code() string {
	if e.Code == "" {
		return "WorkflowFailed"
	}
	return e.Code
}

func (e *WorkflowError) Error() string {
	what := "workflow"
	if e.code() == "ChildFailed" {
		what = "child workflow"
	}
	if e.ID != "" {
		return fmt.Sprintf("%s '%s' failed: %s", what, e.ID, e.Cause.Message)
	}
	return what + " failed: " + e.Cause.Message
}

// Unwrap is the failure the workflow returned.
func (e *WorkflowError) Unwrap() error { return e.Cause }

// Error is a failure the engine reported. Code is one of "Failed",
// "Busy", "NotFound", "AlreadyExists", "UnknownWorkflow", "Io", "Closed",
// "Invalid" or "Panic".
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Message }

// StepError is a step's final failure, after its last attempt. Cause is
// the failure the step's function returned (its type, code and details).
type StepError struct {
	Step   string
	Detail string
	Cause  *Failure
}

func (e *StepError) Error() string { return fmt.Sprintf("step '%s' failed: %s", e.Step, e.Detail) }

// Unwrap is the step's failure.
func (e *StepError) Unwrap() error {
	if e.Cause == nil {
		return nil
	}
	return e.Cause
}

func (e *StepError) cause() *Failure {
	if e.Cause != nil {
		return e.Cause
	}
	return &Failure{Message: e.Detail}
}

// CancelledError is cancellation reaching an operation. A workflow that
// returns one finishes as cancelled.
type CancelledError struct{ Reason string }

func (e *CancelledError) Error() string { return "cancelled: " + e.Reason }

// TimeoutError is a step's, wait's or workflow's timeout. A workflow that
// returns one finishes as timed out.
type TimeoutError struct{ Message string }

func (e *TimeoutError) Error() string {
	if e.Message == "" {
		return "timed out"
	}
	return e.Message
}

// TerminatedError is a run that was terminated (or replaced).
type TerminatedError struct{ Message string }

func (e *TerminatedError) Error() string {
	if e.Message == "" {
		return "terminated"
	}
	return e.Message
}

// DeterminismError is code that made different journal operations than
// its journal records. The run stops (status "stuck") until code that
// matches is deployed.
type DeterminismError struct{ Message string }

func (e *DeterminismError) Error() string { return e.Message }

// RejectedError is an update a validator refused (nothing was recorded),
// a stale completion token, or a query or update with no handler.
type RejectedError struct{ Reason string }

func (e *RejectedError) Error() string { return "rejected: " + e.Reason }

// Cancelled is a CancelledError with this reason.
func Cancelled(reason string) error { return &CancelledError{Reason: reason} }

// ErrClosed is returned by calls on a closed engine.
var ErrClosed = &Error{Code: "Closed", Message: "engine closed"}

var errEnded = &Error{Code: "Failed", Message: "workflow execution has ended"}

var codeNames = map[int32]string{
	1: "Failed", 2: "Busy", 3: "NotFound", 4: "AlreadyExists", 5: "UnknownWorkflow",
	6: "Determinism", 7: "Rejected", 8: "Io", 9: "Closed", 10: "Invalid", 11: "Panic",
}

func codeError(code int32, msg string) error {
	switch code {
	case 6:
		return &DeterminismError{Message: msg}
	case 7:
		return &RejectedError{Reason: strings.TrimPrefix(msg, "rejected: ")}
	}
	name, ok := codeNames[code]
	if !ok {
		name = "Failed"
	}
	return &Error{Code: name, Message: msg}
}

// codeOf is an error's code as the other SDKs name it (for replies to
// server calls).
func codeOf(err error) string {
	var (
		e  *Error
		s  *StepError
		c  *CancelledError
		t  *TimeoutError
		x  *TerminatedError
		d  *DeterminismError
		rj *RejectedError
	)
	switch {
	case errors.As(err, &e):
		return e.Code
	case errors.As(err, &s):
		return "StepFailed"
	case errors.As(err, &c):
		return "Cancelled"
	case errors.As(err, &t):
		return "TimedOut"
	case errors.As(err, &x):
		return "Terminated"
	case errors.As(err, &d):
		return "Determinism"
	case errors.As(err, &rj):
		return "Rejected"
	}
	return "Failed"
}

var camel = regexp.MustCompile(`([a-z])([A-Z])`)

// kindOf is a code in the protocol's snake case ("NotFound": "not_found").
func kindOf(err error) string {
	return strings.ToLower(camel.ReplaceAllString(codeOf(err), "${1}_${2}"))
}

// outcome is how an error ends a run or an update: a status and its text.
func outcome(err error) (uint32, string) {
	var (
		c *CancelledError
		t *TimeoutError
		x *TerminatedError
	)
	switch {
	case errors.As(err, &c):
		return statusCancelled, c.Reason
	case errors.As(err, &t):
		return statusTimedOut, t.Error()
	case errors.As(err, &x):
		return statusTerminated, x.Error()
	}
	return statusFailed, string(encodeFailure(err))
}

// settled is a finished run's result as an error, or nil for success.
func settled(status uint32, payload []byte) error {
	msg := payloadText(payload)
	switch status {
	case statusOK:
		return nil
	case statusCancelled:
		return &CancelledError{Reason: msg}
	case statusTimedOut:
		return &TimeoutError{Message: msg}
	case statusTerminated, statusContinued, statusReset:
		return &TerminatedError{Message: msg}
	}
	return &WorkflowError{Cause: parseFailure(payload)}
}
