//! Native-driven turns.
//!
//! A libuv async handle owned by this module runs the SDK's turns back to
//! back inside one libuv callback. Each turn enters JavaScript through
//! `napi_make_callback` at callback-scope depth zero, so when it returns
//! Node has run every `process.nextTick` callback and microtask the turn
//! queued: the same quiescence a macrotask per turn gives, without a trip
//! through the event loop. The SDK asks for another turn by setting
//! `IO_KICK` in the shared array while `IO_DRIVING` is set; a wake from
//! another thread (a flush finished) sends the handle.
//!
//! libuv is resolved at run time, so the module still loads where the host
//! has no libuv (Bun); there the SDK keeps its macrotask path.

use napi::sys;
use std::ffi::{c_int, c_void};
use std::ptr;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

pub const IO_DRIVING: usize = 11;
pub const IO_KICK: usize = 12;
/// What the turn running now was asked for: the `IO_KICK` value before the
/// driver cleared it (0 or 1 a full turn, 2 a turn only for deliveries).
pub const IO_TURN: usize = 15;
/// Turns one libuv callback may run before it yields to the event loop.
const MAX_TURNS: u32 = 1024;
const MAX_TIME: Duration = Duration::from_millis(2);
const UV_ASYNC: c_int = 1;

type AsyncCb = unsafe extern "C" fn(*mut c_void);
type CloseCb = unsafe extern "C" fn(*mut c_void);

struct Uv {
    async_init: unsafe extern "C" fn(*mut c_void, *mut c_void, AsyncCb) -> c_int,
    async_send: unsafe extern "C" fn(*mut c_void) -> c_int,
    close: unsafe extern "C" fn(*mut c_void, Option<CloseCb>),
    refer: unsafe extern "C" fn(*mut c_void),
    unref: unsafe extern "C" fn(*mut c_void),
    handle_size: unsafe extern "C" fn(c_int) -> usize,
}

/// The process's function `name` (NUL-terminated) as a `T`.
///
/// # Safety
/// `T` is a function pointer type matching that function's signature.
unsafe fn function<T>(name: &[u8]) -> Option<T> {
    // SAFETY: `name` is NUL-terminated; RTLD_DEFAULT searches the process.
    let p = unsafe { libc::dlsym(libc::RTLD_DEFAULT, name.as_ptr().cast()) };
    // SAFETY: the caller names the pointer's type.
    (!p.is_null()).then(|| unsafe { std::mem::transmute_copy::<*mut c_void, T>(&p) })
}

fn uv() -> Option<&'static Uv> {
    static UV: OnceLock<Option<Uv>> = OnceLock::new();
    UV.get_or_init(|| {
        // SAFETY: each is libuv's function of that name, with the signature
        // libuv documents.
        unsafe {
            Some(Uv {
                async_init: function(b"uv_async_init\0")?,
                async_send: function(b"uv_async_send\0")?,
                close: function(b"uv_close\0")?,
                refer: function(b"uv_ref\0")?,
                unref: function(b"uv_unref\0")?,
                handle_size: function(b"uv_handle_size\0")?,
            })
        }
    })
    .as_ref()
}

/// The handle, for senders on any thread; `None` once closed.
#[derive(Clone)]
pub struct Sender(Arc<Mutex<Option<Handle>>>);

#[derive(Clone, Copy)]
struct Handle(*mut c_void);
// SAFETY: uv_async_send may be called from any thread while the handle is
// open; the mutex keeps sends from racing its close.
unsafe impl Send for Handle {}

impl Sender {
    /// Asks for a drive soon (coalesced by libuv). False once closed.
    pub fn send(&self) -> bool {
        let Ok(h) = self.0.lock() else { return false };
        match *h {
            Some(Handle(p)) => {
                if let Some(uv) = uv() {
                    // SAFETY: the handle is open while it is in the slot.
                    unsafe { (uv.async_send)(p) };
                }
                true
            }
            None => false,
        }
    }
}

struct State {
    env: sys::napi_env,
    turn: sys::napi_ref,
    ctx: sys::napi_async_context,
    io: *mut f64,
    slot: Arc<Mutex<Option<Handle>>>,
    /// The handle's memory (uv_async_t), first field `void *data`.
    mem: *mut u64,
    words: usize,
}

/// Owns the handle; dropping it closes the handle.
pub struct Driver {
    state: *mut State,
    pub sender: Sender,
}

impl Driver {
    /// Starts driving `turn` (a JavaScript function) from a libuv handle on
    /// the environment's loop. None where libuv is not available.
    ///
    /// # Safety
    /// `io` points into the shared array for the life of the driver, and
    /// this runs on the JavaScript thread of `env`.
    pub unsafe fn start(env: sys::napi_env, turn: sys::napi_value, io: *mut f64) -> Option<Self> {
        let uv = uv()?;
        let mut lp: *mut sys::uv_loop_s = ptr::null_mut();
        // SAFETY (whole block): plain N-API and libuv calls on the JS thread.
        unsafe {
            if sys::napi_get_uv_event_loop(env, &mut lp) != sys::Status::napi_ok || lp.is_null() {
                return None;
            }
            let size = (uv.handle_size)(UV_ASYNC);
            if size == 0 {
                return None;
            }
            let words = size.div_ceil(8);
            let mem = Box::into_raw(vec![0u64; words].into_boxed_slice()).cast::<u64>();
            let mut turn_ref = ptr::null_mut();
            sys::napi_create_reference(env, turn, 1, &mut turn_ref);
            let mut name = ptr::null_mut();
            sys::napi_create_string_utf8(env, c"fusee.turn".as_ptr(), 10, &mut name);
            let mut ctx = ptr::null_mut();
            sys::napi_async_init(env, ptr::null_mut(), name, &mut ctx);
            let slot = Arc::new(Mutex::new(None));
            let state = Box::into_raw(Box::new(State {
                env,
                turn: turn_ref,
                ctx,
                io,
                slot: slot.clone(),
                mem,
                words,
            }));
            if (uv.async_init)(lp.cast(), mem.cast(), drive) != 0 {
                drop(Box::from_raw(state));
                drop(Box::from_raw(ptr::slice_from_raw_parts_mut(mem, words)));
                return None;
            }
            // `data` is the handle's first field.
            *mem.cast::<*mut State>() = state;
            // The SDK keeps the process alive while it waits for a turn.
            (uv.unref)(mem.cast());
            *slot.lock().ok()? = Some(Handle(mem.cast()));
            sys::napi_add_env_cleanup_hook(env, Some(cleanup), state.cast());
            Some(Self {
                state,
                sender: Sender(slot),
            })
        }
    }
}

impl Drop for Driver {
    fn drop(&mut self) {
        // The environment's cleanup hook may have closed the handle already
        // (its close callback frees the state): the slot, which outlives
        // the state, says. Only the JS thread closes, so this cannot race.
        let open = self.sender.0.lock().map(|h| h.is_some()).unwrap_or(false);
        if !open {
            return;
        }
        // SAFETY: the handle is open, so the state is alive; this runs on
        // the JS thread.
        unsafe {
            let st = &*self.state;
            sys::napi_remove_env_cleanup_hook(st.env, Some(cleanup), self.state.cast());
            close(self.state);
        }
    }
}

unsafe extern "C" fn cleanup(arg: *mut c_void) {
    // SAFETY: registered with the state pointer; the environment is going
    // away, so nothing else closes the handle.
    unsafe { close(arg.cast()) };
}

/// Takes the handle out of the senders' slot and closes it; the close
/// callback frees everything.
unsafe fn close(state: *mut State) {
    // SAFETY: called on the JS thread with a live state.
    unsafe {
        let st = &*state;
        let handle = st.slot.lock().ok().and_then(|mut h| h.take());
        if let (Some(Handle(h)), Some(uv)) = (handle, uv()) {
            (uv.close)(h, Some(closed));
        }
    }
}

unsafe extern "C" fn closed(handle: *mut c_void) {
    // SAFETY: libuv is done with the handle; its data is the state.
    unsafe {
        let state = *handle.cast::<*mut State>();
        let st = Box::from_raw(state);
        sys::napi_async_destroy(st.env, st.ctx);
        sys::napi_delete_reference(st.env, st.turn);
        drop(Box::from_raw(ptr::slice_from_raw_parts_mut(
            st.mem, st.words,
        )));
    }
}

/// The handle's callback: turns until the SDK stops asking or the budget
/// runs out (then the next loop iteration continues).
unsafe extern "C" fn drive(handle: *mut c_void) {
    // SAFETY: libuv calls this on the loop (JS) thread with the handle
    // whose data is the state; the shared array outlives the driver.
    unsafe {
        let st = &*(*handle.cast::<*mut State>());
        let env = st.env;
        let io = std::slice::from_raw_parts_mut(st.io, IO_TURN + 1);
        if let Some(uv) = uv() {
            // Held while a turn was owed from the last callback (below).
            (uv.unref)(handle);
        }
        let mut scope = ptr::null_mut();
        if sys::napi_open_handle_scope(env, &mut scope) != sys::Status::napi_ok {
            return;
        }
        let mut func = ptr::null_mut();
        let mut recv = ptr::null_mut();
        sys::napi_get_reference_value(env, st.turn, &mut func);
        sys::napi_get_global(env, &mut recv);
        let began = Instant::now();
        for n in 0.. {
            io[IO_TURN] = io[IO_KICK];
            io[IO_KICK] = 0.0;
            io[IO_DRIVING] = 1.0;
            let mut result = ptr::null_mut();
            let status =
                sys::napi_make_callback(env, st.ctx, recv, func, 0, ptr::null(), &mut result);
            io[IO_DRIVING] = 0.0;
            if status == sys::Status::napi_pending_exception {
                let mut e = ptr::null_mut();
                sys::napi_get_and_clear_last_exception(env, &mut e);
                sys::napi_fatal_exception(env, e);
                break;
            }
            if status != sys::Status::napi_ok || io[IO_KICK] == 0.0 {
                break;
            }
            if n + 1 >= MAX_TURNS || began.elapsed() >= MAX_TIME {
                // Let timers and I/O run; carry on in the next iteration,
                // holding the process open until then.
                if let Some(uv) = uv() {
                    (uv.refer)(handle);
                    (uv.async_send)(handle);
                }
                break;
            }
        }
        sys::napi_close_handle_scope(env, scope);
    }
}
