package fusee

// The engine is libfusee_c (bindings/c), linked statically from the
// repository's target/release by default. Build tags choose otherwise:
//
//	fusee_dynamic  link libfusee_c.dylib / .so from target/release (rpath set)
//	fusee_system   link -lfusee_c from the system or CGO_LDFLAGS paths

/*
#cgo CFLAGS: -I${SRCDIR}/../c/include
#cgo !fusee_dynamic,!fusee_system LDFLAGS: ${SRCDIR}/../../target/release/libfusee_c.a
#cgo !fusee_dynamic,!fusee_system,darwin LDFLAGS: -liconv -lm
#cgo !fusee_dynamic,!fusee_system,linux LDFLAGS: -lgcc_s -lutil -lrt -lpthread -lm -ldl
#cgo fusee_dynamic LDFLAGS: -L${SRCDIR}/../../target/release -lfusee_c -Wl,-rpath,${SRCDIR}/../../target/release
#cgo fusee_system LDFLAGS: -lfusee_c
#include <stdlib.h>
#include "fusee.h"
*/
import "C"

import (
	"unsafe"
)

const (
	evWake   = C.FUSEE_EV_WAKE
	evAck    = C.FUSEE_EV_ACK
	evReject = C.FUSEE_EV_REJECT
	evStart  = C.FUSEE_EV_START
	evStuck  = C.FUSEE_EV_STUCK
	evEnded  = C.FUSEE_EV_ENDED
	evResult = C.FUSEE_EV_RESULT
	evReply  = C.FUSEE_EV_REPLY
	evRefuse = C.FUSEE_EV_REFUSE
	stride   = C.FUSEE_EVENT_STRIDE
)

const (
	dNone    = C.FUSEE_DELIVERY_NONE
	dResolve = C.FUSEE_DELIVERY_RESOLVE
	dHeld    = C.FUSEE_DELIVERY_HELD
	dCancel  = C.FUSEE_DELIVERY_CANCEL
	dUpdate  = C.FUSEE_DELIVERY_UPDATE
)

const (
	opBeginAwait     = C.FUSEE_OP_BEGIN_AWAIT
	opBeginSleep     = C.FUSEE_OP_BEGIN_SLEEP
	opBeginWait      = C.FUSEE_OP_BEGIN_WAIT
	opBeginChild     = C.FUSEE_OP_BEGIN_CHILD
	opSend           = C.FUSEE_OP_SEND
	opAwaiting       = C.FUSEE_OP_AWAITING
	opHeartbeat      = C.FUSEE_OP_HEARTBEAT
	opPatched        = C.FUSEE_OP_PATCHED
	opSetAttributes  = C.FUSEE_OP_SET_ATTRIBUTES
	opUpdateDone     = C.FUSEE_OP_UPDATE_DONE
	opContinueAsNew  = C.FUSEE_OP_CONTINUE_AS_NEW
	opJournalSize    = C.FUSEE_OP_JOURNAL_SIZE
	opCancelWorkflow = C.FUSEE_OP_CANCEL_WORKFLOW
	opForget         = C.FUSEE_OP_FORGET
	opLimitTicket    = C.FUSEE_OP_LIMIT_TICKET
	opAcquire        = C.FUSEE_OP_ACQUIRE
	opRelease        = C.FUSEE_OP_RELEASE
	opValue          = C.FUSEE_OP_VALUE
	opKey            = C.FUSEE_OP_KEY
	opToken          = C.FUSEE_OP_TOKEN
	opProgress       = C.FUSEE_OP_PROGRESS
	opStreamOpen     = C.FUSEE_OP_STREAM_OPEN
	opStreamAppend   = C.FUSEE_OP_STREAM_APPEND
	opStreamClose    = C.FUSEE_OP_STREAM_CLOSE
	opStreamRecorded = C.FUSEE_OP_STREAM_RECORDED
	opObjectGet      = C.FUSEE_OP_OBJECT_GET
	opObjectSet      = C.FUSEE_OP_OBJECT_SET
	opObjectKeys     = C.FUSEE_OP_OBJECT_KEYS
	opObjectState    = C.FUSEE_OP_OBJECT_STATE
	opBeginCall      = C.FUSEE_OP_BEGIN_CALL
	opStepPaused     = C.FUSEE_OP_STEP_PAUSED
	opObjectEdit     = C.FUSEE_OP_OBJECT_EDIT
	// A stream operation's writer when it is workflow code.
	noSeq = C.FUSEE_NO_SEQ
)

// abiNeeded is the C interface version this package needs (paused steps).
const abiNeeded = 4

// Statuses of steps, runs and replies.
const (
	statusOK         = 0
	statusFailed     = 1
	statusCancelled  = 2
	statusTimedOut   = 3
	statusTerminated = 4
	statusContinued  = 5
	statusReset      = 6
)

// str borrows a Go string for one call (no copy).
func str(s string) C.fusee_str {
	if len(s) == 0 {
		return C.fusee_str{}
	}
	return C.fusee_str{ptr: (*C.uint8_t)(unsafe.Pointer(unsafe.StringData(s))), len: C.size_t(len(s))}
}

// bin borrows Go bytes for one call (no copy).
func bin(b []byte) C.fusee_str {
	if len(b) == 0 {
		return C.fusee_str{}
	}
	return C.fusee_str{ptr: (*C.uint8_t)(unsafe.Pointer(&b[0])), len: C.size_t(len(b))}
}

// view is engine memory, valid until the next call (no copy).
func view(s C.fusee_str) []byte {
	if s.len == 0 || s.ptr == nil {
		return nil
	}
	return unsafe.Slice((*byte)(unsafe.Pointer(s.ptr)), int(s.len))
}

func copied(s C.fusee_str) []byte {
	v := view(s)
	if v == nil {
		return nil
	}
	return append([]byte(nil), v...)
}

func text(s C.fusee_str) string {
	return string(view(s))
}

func takeBuf(b *C.fusee_buf) []byte {
	if b.ptr == nil {
		return nil
	}
	out := C.GoBytes(unsafe.Pointer(b.ptr), C.int(b.len))
	C.fusee_buf_free(b)
	return out
}

// native wraps the engine handle; every method is called by the goroutine
// holding the engine (see engine.go).
type native struct {
	h  *C.fusee_engine
	io *C.fusee_io
	// Calls into the engine (every one goes through fail).
	calls *uint64
}

func (n native) fail(rc C.int32_t) error {
	*n.calls++
	if rc == C.FUSEE_OK {
		return nil
	}
	return codeError(int32(rc), text(C.fusee_engine_error(n.h)))
}

func (n native) owed() bool {
	if n.io.owed != 0 {
		n.io.owed = 0
		return true
	}
	return false
}

// commit is shard's commit position as of the last tick or gate check.
func (n native) commit(shard uint32) uint64 {
	if shard >= uint32(n.io.shards) {
		return 0
	}
	return uint64(unsafe.Slice((*C.uint64_t)(unsafe.Pointer(n.io.commits)), int(n.io.shards))[shard])
}

type stepStart struct {
	seq       uint32
	replay    bool
	gate      uint64
	attempt   uint32
	open      bool
	notBefore uint64
}

func (n native) beginStep(shard uint32, run uint64, name string, readOnly, inline bool) (stepStart, error) {
	var flags C.uint32_t
	if readOnly {
		flags |= C.FUSEE_STEP_READ_ONLY
	}
	if inline {
		flags |= C.FUSEE_STEP_INLINE
	}
	var s C.fusee_step
	if err := n.fail(C.fusee_engine_begin_step(n.h, C.uint32_t(shard), C.uint64_t(run), str(name), flags, &s)); err != nil {
		return stepStart{}, err
	}
	return stepStart{uint32(s.seq), s.replay != 0, uint64(s.gate), uint32(s.attempt), s.open != 0, uint64(s.not_before_ms)}, nil
}

func (n native) stepGate(shard uint32, run, gate, notBefore uint64, inline bool) (bool, error) {
	var open C.uint32_t
	in := C.uint32_t(0)
	if inline {
		in = 1
	}
	err := n.fail(C.fusee_engine_step_gate(n.h, C.uint32_t(shard), C.uint64_t(run), C.uint64_t(gate), C.uint64_t(notBefore), in, &open))
	return open != 0, err
}

func (n native) stepDone(shard uint32, run uint64, seq uint32, ok bool, value []byte) error {
	flags := C.uint32_t(0)
	if ok {
		flags = C.FUSEE_DONE_OK | C.FUSEE_DONE_KEEP
	}
	return n.fail(C.fusee_engine_step_done(n.h, C.uint32_t(shard), C.uint64_t(run), C.uint32_t(seq), flags, bin(value)))
}

func (n native) stepRetry(shard uint32, run uint64, seq uint32, at uint64, msg string) (uint64, error) {
	var gate C.uint64_t
	err := n.fail(C.fusee_engine_step_retry(n.h, C.uint32_t(shard), C.uint64_t(run), C.uint32_t(seq), C.uint64_t(at), str(msg), &gate))
	return uint64(gate), err
}

// host runs a journal operation (FUSEE_OP_*) with up to three numbers and
// three strings: numbers out, and bytes valid until the next call.
func (n native) host(shard uint32, run uint64, op uint32, n0, n1, n2 uint64, s0, s1, s2 C.fusee_str) ([5]uint64, []byte, error) {
	var out [5]C.uint64_t
	var data C.fusee_str
	rc := C.fusee_engine_host(n.h, C.uint32_t(shard), C.uint64_t(run), C.uint32_t(op), C.uint64_t(n0), C.uint64_t(n1), C.uint64_t(n2), s0, s1, s2, &out[0], &data)
	var r [5]uint64
	for i := range out {
		r[i] = uint64(out[i])
	}
	return r, view(data), n.fail(rc)
}

type delivery struct {
	kind    int
	seq     uint32
	status  uint32
	payload []byte
	seqs    []uint32
	ordinal uint64
	token   int64
	name    string
	id      string
}

func (n native) quiescent(shard uint32, run uint64) (delivery, error) {
	var d C.fusee_delivery
	if err := n.fail(C.fusee_engine_quiescent(n.h, C.uint32_t(shard), C.uint64_t(run), &d)); err != nil {
		return delivery{}, err
	}
	out := delivery{kind: int(d.kind), seq: uint32(d.seq), status: uint32(d.status)}
	switch out.kind {
	case dResolve:
		out.payload = copied(d.payload)
	case dCancel:
		out.payload = copied(d.payload)
		if d.n_seqs > 0 {
			out.seqs = append([]uint32(nil), unsafe.Slice((*uint32)(unsafe.Pointer(d.seqs)), int(d.n_seqs))...)
		}
	case dUpdate:
		out.payload = copied(d.payload)
		out.ordinal = uint64(d.ordinal)
		out.token = int64(d.token)
		out.name = text(d.name)
		out.id = text(d.id)
	}
	return out, nil
}

func (n native) finish(shard uint32, run uint64, status uint32, value []byte) error {
	return n.fail(C.fusee_engine_finish(n.h, C.uint32_t(shard), C.uint64_t(run), C.uint32_t(status), bin(value)))
}

func (n native) stuck(shard uint32, run uint64, msg string) error {
	return n.fail(C.fusee_engine_stuck(n.h, C.uint32_t(shard), C.uint64_t(run), str(msg)))
}

func (n native) decideUpdate(shard uint32, token uint64, refusal *string) (int64, error) {
	ok := C.uint32_t(1)
	msg := ""
	if refusal != nil {
		ok, msg = 0, *refusal
	}
	var ordinal C.int64_t
	err := n.fail(C.fusee_engine_decide_update(n.h, C.uint32_t(shard), C.uint64_t(token), ok, str(msg), &ordinal))
	return int64(ordinal), err
}
