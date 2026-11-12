package fusee

/*
#include <stdlib.h>
#include "fusee.h"
extern void fuseeGoNotify(void *user);
static fusee_notify_fn fusee_go_notify(void) { return fuseeGoNotify; }
*/
import "C"

import (
	"fmt"
	"time"
	"unsafe"
)

// ------------------------------------------------------------- lifecycle

func cstr(s string, free *[]unsafe.Pointer) *C.char {
	if s == "" {
		return nil
	}
	p := C.CString(s)
	*free = append(*free, unsafe.Pointer(p))
	return p
}

// openNative opens the engine; user is the notify callback's argument.
func openNative(o Options, user unsafe.Pointer) (native, error) {
	var free []unsafe.Pointer
	defer func() {
		for _, p := range free {
			C.free(p)
		}
	}()
	var c C.fusee_options
	c.size = C.uint32_t(unsafe.Sizeof(c))
	c.durability = cstr(o.Durability, &free)
	c.dir = cstr(o.Dir, &free)
	c.address = cstr(o.Address, &free)
	c.replica_id = -1
	if o.Durability == "replicated" {
		c.replica_id = C.int32_t(o.ReplicaID)
	}
	if len(o.Peers) > 0 {
		p := C.malloc(C.size_t(len(o.Peers)) * C.size_t(unsafe.Sizeof(uintptr(0))))
		free = append(free, p)
		arr := unsafe.Slice((**C.char)(p), len(o.Peers))
		for i, peer := range o.Peers {
			arr[i] = cstr(peer, &free)
		}
		c.peers = &arr[0]
		c.n_peers = C.uint32_t(len(o.Peers))
	}
	c.shards = C.uint32_t(o.Shards)
	if o.KeepHistory {
		c.keep_history = 1
	}
	if o.ArchiveHistory {
		c.archive_history = 1
	}
	c.inline_flush = -1
	if o.InlineFlush != nil {
		c.inline_flush = 0
		if *o.InlineFlush {
			c.inline_flush = 1
		}
	}
	dur := func(d time.Duration) C.int64_t {
		if d <= 0 {
			return -1
		}
		return C.int64_t(d / time.Millisecond)
	}
	c.park_after_ms = dur(o.ParkAfter)
	c.retention_ms = dur(o.Retention)
	c.checkpoint_interval_ms = dur(o.CheckpointInterval)
	c.segment_bytes = -1
	if o.SegmentBytes > 0 {
		c.segment_bytes = C.int64_t(o.SegmentBytes)
	}
	c.flush = cstr(o.Flush, &free)
	if len(o.Codec) > 0 {
		p := C.malloc(C.size_t(len(o.Codec)) * C.size_t(unsafe.Sizeof(C.fusee_key{})))
		free = append(free, p)
		keys := unsafe.Slice((*C.fusee_key)(p), len(o.Codec))
		for i, k := range o.Codec {
			if len(k.Key) != 32 {
				return native{}, &Error{Code: "Invalid", Message: fmt.Sprintf("key %q is not 32 bytes", k.ID)}
			}
			kb := C.CBytes(k.Key)
			free = append(free, kb)
			keys[i] = C.fusee_key{id: cstr(k.ID, &free), key: (*C.uint8_t)(kb)}
		}
		c.codec_keys = &keys[0]
		c.n_codec_keys = C.uint32_t(len(o.Codec))
	}
	c.notify = C.fusee_go_notify()
	c.notify_user = user
	c.notify_fd = -1
	var h *C.fusee_engine
	var buf C.fusee_buf
	rc := C.fusee_engine_open(&c, &h, &buf)
	msg := string(takeBuf(&buf))
	if rc != C.FUSEE_OK {
		return native{}, codeError(int32(rc), msg)
	}
	return native{h: h, io: C.fusee_engine_io(h), calls: new(uint64)}, nil
}

func (n native) close() error {
	var buf C.fusee_buf
	rc := C.fusee_engine_close(n.h, &buf)
	msg := string(takeBuf(&buf))
	if rc != C.FUSEE_OK {
		return codeError(int32(rc), msg)
	}
	return nil
}

type tickOut struct {
	events []uint64
	extras []C.fusee_str
	next   int64
}

// extra is an event's i-th extra, valid until the next tick.
func (t *tickOut) extra(i uint64) []byte {
	if i >= uint64(len(t.extras)) {
		return nil
	}
	return view(t.extras[i])
}

func (n native) tick() (tickOut, error) {
	var t C.fusee_tick
	if err := n.fail(C.fusee_engine_tick(n.h, &t)); err != nil {
		return tickOut{}, err
	}
	out := tickOut{next: int64(t.next_wake_ms)}
	if t.n_events > 0 {
		out.events = unsafe.Slice((*uint64)(unsafe.Pointer(t.events)), int(t.n_events)*stride)
	}
	if t.n_extras > 0 {
		out.extras = unsafe.Slice(t.extras, int(t.n_extras))
	}
	return out, nil
}

// ------------------------------------------------------------- requests

type startReq struct {
	id, wtype   string
	input       []byte
	version     uint32
	policy      uint32
	deadline    uint64
	token       int64
	update      bool
	updateName  string
	updateID    string
	updateArg   []byte
	updateToken int64
}

func (n native) start(r startReq) (bool, error) {
	var q C.fusee_start
	q.id = str(r.id)
	q.wtype = str(r.wtype)
	q.input = bin(r.input)
	q.version = C.uint32_t(r.version)
	q.policy = C.uint32_t(r.policy)
	q.deadline_ms = C.uint64_t(r.deadline)
	q.token = C.int64_t(r.token)
	q.update_token = -1
	if r.update {
		q.has_update = 1
		q.update_name = str(r.updateName)
		q.update_id = str(r.updateID)
		q.update_arg = bin(r.updateArg)
		q.update_token = C.int64_t(r.updateToken)
	}
	var existed C.uint32_t
	err := n.fail(C.fusee_engine_start(n.h, q, &existed))
	return existed != 0, err
}

func (n native) signal(id, name string, value []byte, token uint64) error {
	return n.fail(C.fusee_engine_signal(n.h, str(id), str(name), bin(value), C.uint64_t(token)))
}

func (n native) cancel(id, reason string, terminate bool, token uint64) error {
	t := C.uint32_t(0)
	if terminate {
		t = 1
	}
	return n.fail(C.fusee_engine_cancel(n.h, str(id), str(reason), t, C.uint64_t(token)))
}

func (n native) resume(id string, token uint64) error {
	return n.fail(C.fusee_engine_resume(n.h, str(id), C.uint64_t(token)))
}

func (n native) update(id, name, updateID string, arg []byte, token uint64) error {
	return n.fail(C.fusee_engine_update(n.h, str(id), str(name), str(updateID), bin(arg), C.uint64_t(token)))
}

func (n native) complete(completion string, ok bool, value []byte, token uint64) error {
	o := C.uint32_t(0)
	if ok {
		o = 1
	}
	return n.fail(C.fusee_engine_complete(n.h, str(completion), o, bin(value), C.uint64_t(token)))
}

func (n native) reset(id, before string, reapply bool, version uint32, token uint64) error {
	r := C.uint32_t(0)
	if reapply {
		r = 1
	}
	return n.fail(C.fusee_engine_reset(n.h, str(id), str(before), r, C.uint32_t(version), C.uint64_t(token)))
}

func (n native) ackAt(shard uint32, lsn, token uint64) error {
	return n.fail(C.fusee_engine_ack_at(n.h, C.uint32_t(shard), C.uint64_t(lsn), C.uint64_t(token)))
}

func (n native) queryNative(id, name string, arg []byte, token uint64) error {
	return n.fail(C.fusee_engine_query_native(n.h, str(id), str(name), bin(arg), C.uint64_t(token)))
}

// streamRead reads stream name of id from offset from (of run when it is
// not negative); the answer, JSON, is the reply for token.
func (n native) streamRead(id string, run int64, name string, from, waitMs, token uint64) error {
	return n.fail(C.fusee_engine_stream_read(n.h, str(id), C.int64_t(run), str(name), C.uint64_t(from), C.uint64_t(waitMs), C.uint64_t(token)))
}

// ---------------------------------------------------------------- reads

type resultState struct {
	state   int // 0 not found, 1 running, 2 done
	shard   uint32
	run     uint64
	status  uint32
	payload []byte
}

func (n native) result(id string) (resultState, error) {
	var r C.fusee_result
	if err := n.fail(C.fusee_engine_result(n.h, str(id), &r)); err != nil {
		return resultState{}, err
	}
	return resultState{int(r.state), uint32(r.shard), uint64(r.run), uint32(r.status), copied(r.payload)}, nil
}

func (n native) inspect(id string) ([]byte, bool, error) {
	var j C.fusee_str
	var found C.uint32_t
	err := n.fail(C.fusee_engine_inspect(n.h, str(id), &j, &found))
	return copied(j), found != 0, err
}

func (n native) list(filter []byte, limit int) ([]byte, error) {
	var j C.fusee_str
	err := n.fail(C.fusee_engine_list(n.h, bin(filter), C.uint32_t(limit), &j))
	return copied(j), err
}

func (n native) exportJournal(id string) ([]byte, bool, error) {
	var j C.fusee_str
	var found C.uint32_t
	err := n.fail(C.fusee_engine_export_journal(n.h, str(id), &j, &found))
	return copied(j), found != 0, err
}

func (n native) shardMetrics() ([]byte, error) {
	var j C.fusee_str
	err := n.fail(C.fusee_engine_shard_metrics(n.h, &j))
	return copied(j), err
}

func (n native) queryPoint(id string) (shard uint32, run, lsn uint64, foreign bool, err error) {
	var s, f C.uint32_t
	var r, l C.uint64_t
	err = n.fail(C.fusee_engine_query_point(n.h, str(id), &s, &r, &l, &f))
	return uint32(s), uint64(r), uint64(l), f != 0, err
}

// codeState: whether the run has a code slot, and whether it is replaying.
func (n native) codeState(shard uint32, run uint64) (slot, replaying bool, err error) {
	var s C.uint32_t
	err = n.fail(C.fusee_engine_code_state(n.h, C.uint32_t(shard), C.uint64_t(run), &s))
	return s&1 != 0, s&2 != 0, err
}

func (n native) limit(name string, concurrent uint32, perSecond float64) error {
	return n.fail(C.fusee_engine_limit(n.h, str(name), C.uint32_t(concurrent), C.double(perSecond)))
}

// registerObject marks name as an object type whose shared handlers are
// those named (every other one is exclusive).
func (n native) registerObject(name string, shared []string) error {
	var arr *C.fusee_str
	if len(shared) > 0 {
		// The names live in C memory: C memory must not hold Go pointers.
		p := C.malloc(C.size_t(len(shared)) * C.size_t(unsafe.Sizeof(C.fusee_str{})))
		defer C.free(p)
		strs := unsafe.Slice((*C.fusee_str)(p), len(shared))
		for i, s := range shared {
			b := C.CBytes([]byte(s))
			defer C.free(b)
			strs[i] = C.fusee_str{ptr: (*C.uint8_t)(b), len: C.size_t(len(s))}
		}
		arr = &strs[0]
	}
	return n.fail(C.fusee_engine_register_object(n.h, str(name), arr, C.uint32_t(len(shared))))
}

func (n native) limitUsage(name string) (uint32, uint64, error) {
	var held C.uint32_t
	var waiting C.uint64_t
	err := n.fail(C.fusee_engine_limit_usage(n.h, str(name), &held, &waiting))
	return uint32(held), uint64(waiting), err
}

func (n native) nextCall() ([]byte, bool, error) {
	var j C.fusee_str
	var found C.uint32_t
	err := n.fail(C.fusee_engine_next_call(n.h, &j, &found))
	return copied(j), found != 0, err
}

// invocationState is an invoked worker's state ("running", "idle",
// "expired", "lost"), or "" for any other engine; awaiting are the types the
// engine has runs of and no code for.
func (n native) invocationState(awaiting []string) (string, error) {
	var arr *C.fusee_str
	if len(awaiting) > 0 {
		p := C.malloc(C.size_t(len(awaiting)) * C.size_t(unsafe.Sizeof(C.fusee_str{})))
		defer C.free(p)
		strs := unsafe.Slice((*C.fusee_str)(p), len(awaiting))
		for i, s := range awaiting {
			b := C.CBytes([]byte(s))
			defer C.free(b)
			strs[i] = C.fusee_str{ptr: (*C.uint8_t)(b), len: C.size_t(len(s))}
		}
		arr = &strs[0]
	}
	var st C.fusee_str
	err := n.fail(C.fusee_engine_invocation_state(n.h, arr, C.uint32_t(len(awaiting)), &st))
	return string(copied(st)), err
}

func (n native) serve(addr, token string) (string, error) {
	var bound C.fusee_str
	err := n.fail(C.fusee_engine_serve(n.h, str(addr), str(token), &bound))
	return string(copied(bound)), err
}

func (n native) replyCall(relay uint64, response []byte) error {
	return n.fail(C.fusee_engine_reply_call(n.h, C.uint64_t(relay), bin(response)))
}

// ------------------------------------------------------- free functions

// queueRequest builds a debounced start (kind 0) or batched event (1) as
// fusee::queue::request does: coordinator type, ID, start input, update
// argument. NaN numbers take the defaults.
func queueRequest(kind uint32, workflow, key, eventID string, input []byte, a, b, c float64) ([4][]byte, error) {
	var out [4]C.fusee_buf
	var buf C.fusee_buf
	rc := C.fusee_queue_request(C.uint32_t(kind), str(workflow), str(key), str(eventID), bin(input), C.double(a), C.double(b), C.double(c), &out[0], &buf)
	msg := string(takeBuf(&buf))
	var r [4][]byte
	for i := range out {
		r[i] = takeBuf(&out[i])
	}
	if rc != C.FUSEE_OK {
		return r, codeError(int32(rc), msg)
	}
	return r, nil
}

func validateSchedule(spec []byte) error {
	var buf C.fusee_buf
	rc := C.fusee_validate_schedule(bin(spec), &buf)
	msg := string(takeBuf(&buf))
	if rc != C.FUSEE_OK {
		return &Error{Code: "Invalid", Message: msg}
	}
	return nil
}

// ---------------------------------------------------------------- client

type nativeClient struct{ c *C.fusee_client }

func connectNative(address string) (nativeClient, error) {
	var c *C.fusee_client
	var buf C.fusee_buf
	rc := C.fusee_client_connect(str(address), &c, &buf)
	msg := string(takeBuf(&buf))
	if rc != C.FUSEE_OK {
		return nativeClient{}, codeError(int32(rc), msg)
	}
	return nativeClient{c}, nil
}

func (c nativeClient) call(method string, args []byte) ([]byte, error) {
	var buf C.fusee_buf
	rc := C.fusee_client_call(c.c, str(method), bin(args), &buf)
	out := takeBuf(&buf)
	if rc != C.FUSEE_OK {
		return nil, codeError(int32(rc), string(out))
	}
	return out, nil
}

func (c nativeClient) shutdown() { C.fusee_client_close(c.c) }
func (c nativeClient) free()     { C.fusee_client_free(c.c) }

func allocKey() unsafe.Pointer { return C.malloc(1) }
func freeKey(p unsafe.Pointer) { C.free(p) }

// ABIVersion is the C interface version of the linked library.
func ABIVersion() uint32 { return uint32(C.fusee_abi_version()) }

// LibraryVersion is the linked engine library's version.
func LibraryVersion() string { return C.GoString(C.fusee_version()) }
