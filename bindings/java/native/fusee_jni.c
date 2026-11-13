/*
 * fusee_jni.c: the JNI layer of the Java SDK over libfusee_c (fusee.h).
 *
 * One function per method of dev.fusee.Native, each a thin call into the C
 * interface: byte arrays are copied into call-length buffers (on the stack
 * when small) and borrowed as fusee_str, numbers the C call returns through
 * pointers go to the caller's long array, and a failed call throws the
 * exception Native.fail builds from its code and message. Linked with the
 * static libfusee_c.a, so the one shared library carries the engine.
 *
 * Threads: Java calls on one engine never overlap (the SDK hands the engine
 * between threads with a synchronizing handoff), so the per-engine stash of
 * the last tick and delivery needs no lock. The notify callback runs on the
 * engine's threads: each attaches to the JVM once (as a daemon) and is
 * detached by a thread-local destructor when it exits.
 */
#include <jni.h>
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "dev_fusee_Native.h"
#include "fusee.h"

static JavaVM *jvm;
static jclass native_class;
static jmethodID fail_method;
static jmethodID notify_method;
static pthread_key_t detach_key;

/* The hot calls take their byte arguments from the engine's input buffer
 * and leave their outputs in its output buffer (both direct ByteBuffers the
 * Java side registered), so they make no JNI calls back into the JVM: on
 * macOS/arm64 each such call (an array access) costs about 70 ns. */
#define OUT_DATA 128    /* output buffer: numbers in 16 slots, then data */
#define OUT_LEN_SLOT 15 /* the data's length; -1 none, -2 returned as an array instead */

typedef struct {
    fusee_engine *e;
    fusee_tick tick;
    fusee_delivery d;
    uint8_t *in;
    size_t in_cap;
    uint8_t *out;
    size_t out_cap;
} handle;

#define H(h) ((handle *)(intptr_t)(h))

/* ------------------------------------------------------------ arguments */

#define STACK_BYTES 512

typedef struct {
    fusee_str s;
    jbyte *heap;
    jbyte stack[STACK_BYTES];
} arg;

static void throw_oom(JNIEnv *env) {
    jclass c = (*env)->FindClass(env, "java/lang/OutOfMemoryError");
    if (c) (*env)->ThrowNew(env, c, "fusee_jni: out of memory");
}

/* Copies a byte array for the call; 0 (with an exception) on failure. */
static int arg_in(JNIEnv *env, jbyteArray a, arg *x) {
    x->heap = NULL;
    x->s.ptr = NULL;
    x->s.len = 0;
    if (a == NULL) return 1;
    jsize n = (*env)->GetArrayLength(env, a);
    if (n <= 0) return 1;
    jbyte *p = x->stack;
    if (n > STACK_BYTES) {
        p = malloc((size_t)n);
        if (!p) {
            throw_oom(env);
            return 0;
        }
        x->heap = p;
    }
    (*env)->GetByteArrayRegion(env, a, 0, n, p);
    x->s.ptr = (const uint8_t *)p;
    x->s.len = (size_t)n;
    return 1;
}

static void arg_free(arg *x) {
    free(x->heap);
    x->heap = NULL;
}

/* A NUL-terminated copy of a byte array (NULL for null); *ok 0 on failure. */
static char *cstring(JNIEnv *env, jbyteArray a, int *ok) {
    if (a == NULL) return NULL;
    jsize n = (*env)->GetArrayLength(env, a);
    char *p = malloc((size_t)n + 1);
    if (!p) {
        throw_oom(env);
        *ok = 0;
        return NULL;
    }
    (*env)->GetByteArrayRegion(env, a, 0, n, (jbyte *)p);
    p[n] = 0;
    return p;
}

/* -------------------------------------------------------------- outputs */

static jbyteArray bytes_out(JNIEnv *env, const uint8_t *p, size_t n) {
    if (n == 0 || p == NULL) return NULL;
    jbyteArray a = (*env)->NewByteArray(env, (jsize)n);
    if (a) (*env)->SetByteArrayRegion(env, a, 0, (jsize)n, (const jbyte *)p);
    return a;
}

static jbyteArray str_out(JNIEnv *env, fusee_str s) { return bytes_out(env, s.ptr, s.len); }

/* A caller-owned buffer as a byte array (an empty one for an empty buffer), freed. */
static jbyteArray buf_out(JNIEnv *env, fusee_buf *b) {
    jbyteArray a = (*env)->NewByteArray(env, (jsize)b->len);
    if (a && b->len) (*env)->SetByteArrayRegion(env, a, 0, (jsize)b->len, (const jbyte *)b->ptr);
    fusee_buf_free(b);
    return a;
}

static void longs_out(JNIEnv *env, jlongArray out, const uint64_t *v, jsize n) {
    if (out == NULL) return;
    jsize len = (*env)->GetArrayLength(env, out);
    if (n > len) n = len;
    (*env)->SetLongArrayRegion(env, out, 0, n, (const jlong *)v);
}

static void slot(handle *h, int i, uint64_t v) { memcpy(h->out + 8 * (size_t)i, &v, 8); }

/* in[off, off+len) as a borrowed fusee_str; 0 (with an exception) if out of bounds. */
static int in_str(JNIEnv *env, handle *h, size_t off, jint len, fusee_str *s) {
    s->ptr = NULL;
    s->len = 0;
    if (len <= 0) return 1;
    if (!h->in || off + (size_t)len > h->in_cap) {
        jclass c = (*env)->FindClass(env, "java/lang/IndexOutOfBoundsException");
        if (c) (*env)->ThrowNew(env, c, "fusee_jni: an argument past the input buffer");
        return 0;
    }
    s->ptr = h->in + off;
    s->len = (size_t)len;
    return 1;
}

/* Bytes into the output buffer's data region (length in slot 15), or, when
 * they do not fit, a new array (slot 15 = -2). */
static jbyteArray data_out(JNIEnv *env, handle *h, const uint8_t *p, size_t n) {
    if (n == 0 || p == NULL) {
        slot(h, OUT_LEN_SLOT, (uint64_t)-1);
        return NULL;
    }
    if (n <= h->out_cap - OUT_DATA) {
        memcpy(h->out + OUT_DATA, p, n);
        slot(h, OUT_LEN_SLOT, n);
        return NULL;
    }
    slot(h, OUT_LEN_SLOT, (uint64_t)-2);
    return bytes_out(env, p, n);
}

/* ---------------------------------------------------------------- errors */

static void throw_code(JNIEnv *env, int code, const uint8_t *msg, size_t len) {
    if ((*env)->ExceptionCheck(env)) return;
    jbyteArray m = (*env)->NewByteArray(env, (jsize)len);
    if (!m) return;
    if (len) (*env)->SetByteArrayRegion(env, m, 0, (jsize)len, (const jbyte *)msg);
    jobject ex = (*env)->CallStaticObjectMethod(env, native_class, fail_method, (jint)code, m);
    if (ex && !(*env)->ExceptionCheck(env)) (*env)->Throw(env, (jthrowable)ex);
}

/* 1 if rc is FUSEE_OK, else throws the engine's last error and returns 0. */
static int check(JNIEnv *env, handle *h, int32_t rc) {
    if (rc == FUSEE_OK) return 1;
    fusee_str m = fusee_engine_error(h->e);
    throw_code(env, rc, m.ptr, m.len);
    return 0;
}

static int check_buf(JNIEnv *env, int32_t rc, fusee_buf *b) {
    if (rc == FUSEE_OK) {
        fusee_buf_free(b);
        return 1;
    }
    throw_code(env, rc, b->ptr, b->len);
    fusee_buf_free(b);
    return 0;
}

/* ---------------------------------------------------------------- notify */

static void detach_thread(void *unused) {
    (void)unused;
    if (jvm) (*jvm)->DetachCurrentThread(jvm);
}

static void on_notify(void *user) {
    JNIEnv *env = NULL;
    jint r = (*jvm)->GetEnv(jvm, (void **)&env, JNI_VERSION_1_8);
    if (r == JNI_EDETACHED) {
        JavaVMAttachArgs args = {JNI_VERSION_1_8, (char *)"fusee-native", NULL};
        if ((*jvm)->AttachCurrentThreadAsDaemon(jvm, (void **)&env, &args) != JNI_OK) return;
        pthread_setspecific(detach_key, (void *)1);
    } else if (r != JNI_OK) {
        return;
    }
    (*env)->CallStaticVoidMethod(env, native_class, notify_method, (jlong)(intptr_t)user);
    if ((*env)->ExceptionCheck(env)) (*env)->ExceptionClear(env);
}

JNIEXPORT jint JNICALL JNI_OnLoad(JavaVM *vm, void *reserved) {
    (void)reserved;
    jvm = vm;
    JNIEnv *env;
    if ((*vm)->GetEnv(vm, (void **)&env, JNI_VERSION_1_8) != JNI_OK) return JNI_ERR;
    jclass c = (*env)->FindClass(env, "dev/fusee/Native");
    if (!c) return JNI_ERR;
    native_class = (*env)->NewGlobalRef(env, c);
    fail_method = (*env)->GetStaticMethodID(env, c, "fail", "(I[B)Ljava/lang/RuntimeException;");
    notify_method = (*env)->GetStaticMethodID(env, c, "notify", "(J)V");
    if (!native_class || !fail_method || !notify_method) return JNI_ERR;
    if (pthread_key_create(&detach_key, detach_thread) != 0) return JNI_ERR;
    return JNI_VERSION_1_8;
}

/* --------------------------------------------------------------- library */

JNIEXPORT jint JNICALL Java_dev_fusee_Native_abiVersion(JNIEnv *env, jclass c) {
    (void)env;
    (void)c;
    return (jint)fusee_abi_version();
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_version(JNIEnv *env, jclass c) {
    (void)c;
    const char *v = fusee_version();
    return bytes_out(env, (const uint8_t *)v, strlen(v));
}

/* ------------------------------------------------------------- lifecycle */

JNIEXPORT jlong JNICALL Java_dev_fusee_Native_open(JNIEnv *env, jclass c, jbyteArray durability, jbyteArray dir,
                                                   jbyteArray address, jint replica_id, jobjectArray peers,
                                                   jint shards, jboolean keep_history, jboolean archive_history,
                                                   jint inline_flush, jlong park_after_ms, jlong retention_ms,
                                                   jlong checkpoint_ms, jlong segment_bytes, jbyteArray flush,
                                                   jobjectArray key_ids, jobjectArray keys, jlong notify_key) {
    (void)c;
    int ok = 1;
    fusee_options o;
    memset(&o, 0, sizeof o);
    o.size = sizeof o;
    char *s_durability = cstring(env, durability, &ok);
    char *s_dir = cstring(env, dir, &ok);
    char *s_address = cstring(env, address, &ok);
    char *s_flush = cstring(env, flush, &ok);
    jsize n_peers = peers ? (*env)->GetArrayLength(env, peers) : 0;
    jsize n_keys = keys ? (*env)->GetArrayLength(env, keys) : 0;
    char **peer_list = n_peers ? calloc((size_t)n_peers, sizeof(char *)) : NULL;
    char **id_list = n_keys ? calloc((size_t)n_keys, sizeof(char *)) : NULL;
    uint8_t *key_bytes = n_keys ? calloc((size_t)n_keys, 32) : NULL;
    fusee_key *key_list = n_keys ? calloc((size_t)n_keys, sizeof(fusee_key)) : NULL;
    if ((n_peers && !peer_list) || (n_keys && (!id_list || !key_bytes || !key_list))) {
        throw_oom(env);
        ok = 0;
    }
    for (jsize i = 0; ok && i < n_peers; i++) {
        jbyteArray p = (jbyteArray)(*env)->GetObjectArrayElement(env, peers, i);
        peer_list[i] = cstring(env, p, &ok);
        (*env)->DeleteLocalRef(env, p);
    }
    for (jsize i = 0; ok && i < n_keys; i++) {
        jbyteArray id = (jbyteArray)(*env)->GetObjectArrayElement(env, key_ids, i);
        jbyteArray k = (jbyteArray)(*env)->GetObjectArrayElement(env, keys, i);
        id_list[i] = cstring(env, id, &ok);
        if (ok && (k == NULL || (*env)->GetArrayLength(env, k) != 32)) {
            const char *m = "an encryption key is not 32 bytes";
            throw_code(env, FUSEE_INVALID, (const uint8_t *)m, strlen(m));
            ok = 0;
        }
        if (ok) (*env)->GetByteArrayRegion(env, k, 0, 32, (jbyte *)(key_bytes + 32 * i));
        key_list[i].id = id_list[i];
        key_list[i].key = key_bytes + 32 * i;
        (*env)->DeleteLocalRef(env, id);
        (*env)->DeleteLocalRef(env, k);
    }
    fusee_engine *e = NULL;
    if (ok) {
        o.durability = s_durability;
        o.dir = s_dir;
        o.address = s_address;
        o.replica_id = replica_id;
        o.n_peers = (uint32_t)n_peers;
        o.peers = (const char *const *)peer_list;
        o.shards = (uint32_t)shards;
        o.keep_history = keep_history ? 1 : 0;
        o.archive_history = archive_history ? 1 : 0;
        o.inline_flush = inline_flush;
        o.park_after_ms = park_after_ms;
        o.retention_ms = retention_ms;
        o.checkpoint_interval_ms = checkpoint_ms;
        o.segment_bytes = segment_bytes;
        o.flush = s_flush;
        o.n_codec_keys = (uint32_t)n_keys;
        o.codec_keys = key_list;
        o.notify = on_notify;
        o.notify_user = (void *)(intptr_t)notify_key;
        o.notify_fd = -1;
        fusee_buf err = {0};
        int32_t rc = fusee_engine_open(&o, &e, &err);
        if (!check_buf(env, rc, &err)) e = NULL;
    }
    free(s_durability);
    free(s_dir);
    free(s_address);
    free(s_flush);
    for (jsize i = 0; peer_list && i < n_peers; i++) free(peer_list[i]);
    for (jsize i = 0; id_list && i < n_keys; i++) free(id_list[i]);
    free(peer_list);
    free(id_list);
    if (key_bytes) memset(key_bytes, 0, 32 * (size_t)n_keys);
    free(key_bytes);
    free(key_list);
    if (!e) return 0;
    handle *h = calloc(1, sizeof(handle));
    if (!h) {
        fusee_buf err = {0};
        fusee_engine_close(e, &err);
        fusee_buf_free(&err);
        throw_oom(env);
        return 0;
    }
    h->e = e;
    return (jlong)(intptr_t)h;
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_close(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    handle *h = H(hh);
    if (!h) return;
    fusee_buf err = {0};
    int32_t rc = fusee_engine_close(h->e, &err);
    free(h);
    check_buf(env, rc, &err);
}

JNIEXPORT jobject JNICALL Java_dev_fusee_Native_io(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    fusee_io *io = fusee_engine_io(H(hh)->e);
    return (*env)->NewDirectByteBuffer(env, io, (jlong)sizeof(fusee_io));
}

JNIEXPORT jobject JNICALL Java_dev_fusee_Native_commits(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    fusee_io *io = fusee_engine_io(H(hh)->e);
    return (*env)->NewDirectByteBuffer(env, (void *)io->commits, (jlong)io->shards * 8);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_buffers(JNIEnv *env, jclass c, jlong hh, jobject in, jobject out) {
    (void)c;
    handle *h = H(hh);
    h->in = (*env)->GetDirectBufferAddress(env, in);
    h->in_cap = h->in ? (size_t)(*env)->GetDirectBufferCapacity(env, in) : 0;
    h->out = (*env)->GetDirectBufferAddress(env, out);
    h->out_cap = h->out ? (size_t)(*env)->GetDirectBufferCapacity(env, out) : 0;
}

/* --------------------------------------------------------------- driving */

JNIEXPORT jint JNICALL Java_dev_fusee_Native_tick(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    handle *h = H(hh);
    if (!check(env, h, fusee_engine_tick(h->e, &h->tick))) return 0;
    size_t bytes = h->tick.n_events * FUSEE_EVENT_STRIDE * 8;
    slot(h, 0, (uint64_t)h->tick.next_wake_ms);
    if (bytes <= h->out_cap - OUT_DATA) {
        if (bytes) memcpy(h->out + OUT_DATA, h->tick.events, bytes);
        slot(h, 1, 1);
    } else {
        slot(h, 1, 0); /* fetch them with tickEvents */
    }
    return (jint)h->tick.n_events;
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_tickEvents(JNIEnv *env, jclass c, jlong hh, jlongArray events) {
    (void)c;
    handle *h = H(hh);
    jsize rows = (jsize)(h->tick.n_events * FUSEE_EVENT_STRIDE);
    jsize len = (*env)->GetArrayLength(env, events);
    if (rows > len) rows = len;
    if (rows > 0) (*env)->SetLongArrayRegion(env, events, 0, rows, (const jlong *)h->tick.events);
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_tickExtra(JNIEnv *env, jclass c, jlong hh, jint i) {
    (void)c;
    handle *h = H(hh);
    if (i < 0 || (size_t)i >= h->tick.n_extras) return data_out(env, h, NULL, 0);
    return data_out(env, h, h->tick.extras[i].ptr, h->tick.extras[i].len);
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_nextCall(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    handle *h = H(hh);
    fusee_str j = {0};
    uint32_t found = 0;
    if (!check(env, h, fusee_engine_next_call(h->e, &j, &found)) || !found) return NULL;
    return str_out(env, j);
}

/* An invoked worker's state, or an empty array for any other engine. */
JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_invocationState(JNIEnv *env, jclass c, jlong hh,
                                                                  jobjectArray awaiting) {
    (void)c;
    handle *h = H(hh);
    jsize n = awaiting ? (*env)->GetArrayLength(env, awaiting) : 0;
    arg *names = n ? calloc((size_t)n, sizeof(arg)) : NULL;
    fusee_str *strs = n ? calloc((size_t)n, sizeof(fusee_str)) : NULL;
    if (n && (!names || !strs)) {
        throw_oom(env);
        free(names);
        free(strs);
        return NULL;
    }
    jsize got = 0;
    int ok = 1;
    for (; ok && got < n; got++) {
        jbyteArray s = (jbyteArray)(*env)->GetObjectArrayElement(env, awaiting, got);
        ok = arg_in(env, s, &names[got]);
        if (s) (*env)->DeleteLocalRef(env, s);
        if (ok) strs[got] = names[got].s;
    }
    jbyteArray out = NULL;
    fusee_str st = {0};
    if (ok && check(env, h, fusee_engine_invocation_state(h->e, strs, (uint32_t)n, &st))) {
        out = (*env)->NewByteArray(env, (jsize)st.len);
        if (out && st.len) (*env)->SetByteArrayRegion(env, out, 0, (jsize)st.len, (const jbyte *)st.ptr);
    }
    for (jsize i = 0; i < got; i++) arg_free(&names[i]);
    free(names);
    free(strs);
    return out;
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_replyCall(JNIEnv *env, jclass c, jlong hh, jlong relay,
                                                       jbyteArray json) {
    (void)c;
    handle *h = H(hh);
    arg a;
    if (!arg_in(env, json, &a)) return;
    check(env, h, fusee_engine_reply_call(h->e, (uint64_t)relay, a.s));
    arg_free(&a);
}

/* -------------------------------------------------------------- requests */

JNIEXPORT jboolean JNICALL Java_dev_fusee_Native_start(JNIEnv *env, jclass c, jlong hh, jbyteArray id,
                                                       jbyteArray wtype, jbyteArray input, jint version,
                                                       jint policy, jlong deadline, jlong token,
                                                       jboolean has_update, jbyteArray update_name,
                                                       jbyteArray update_id, jbyteArray update_arg,
                                                       jlong update_token) {
    (void)c;
    handle *h = H(hh);
    arg a0, a1, a2, a3, a4, a5;
    jboolean existed_out = JNI_FALSE;
    int ok = arg_in(env, id, &a0);
    ok = ok && arg_in(env, wtype, &a1);
    ok = ok && arg_in(env, input, &a2);
    ok = ok && arg_in(env, update_name, &a3);
    ok = ok && arg_in(env, update_id, &a4);
    ok = ok && arg_in(env, update_arg, &a5);
    if (ok) {
        fusee_start r;
        memset(&r, 0, sizeof r);
        r.id = a0.s;
        r.wtype = a1.s;
        r.input = a2.s;
        r.version = (uint32_t)version;
        r.policy = (uint32_t)policy;
        r.deadline_ms = (uint64_t)deadline;
        r.token = token;
        r.update_token = -1;
        if (has_update) {
            r.has_update = 1;
            r.update_name = a3.s;
            r.update_id = a4.s;
            r.update_arg = a5.s;
            r.update_token = update_token;
        }
        uint32_t existed = 0;
        if (check(env, h, fusee_engine_start(h->e, r, &existed))) existed_out = existed ? JNI_TRUE : JNI_FALSE;
    }
    arg_free(&a0);
    arg_free(&a1);
    arg_free(&a2);
    arg_free(&a3);
    arg_free(&a4);
    arg_free(&a5);
    return existed_out;
}

/* Up to three byte-array arguments, released together. */
typedef struct {
    arg a[3];
    int ok;
} args3;

static void args_in(JNIEnv *env, args3 *x, jbyteArray s0, jbyteArray s1, jbyteArray s2) {
    x->a[0].heap = x->a[1].heap = x->a[2].heap = NULL;
    x->ok = arg_in(env, s0, &x->a[0]) && arg_in(env, s1, &x->a[1]) && arg_in(env, s2, &x->a[2]);
}

static void args_free(args3 *x) {
    arg_free(&x->a[0]);
    arg_free(&x->a[1]);
    arg_free(&x->a[2]);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_signal(JNIEnv *env, jclass c, jlong hh, jbyteArray id, jbyteArray name,
                                                    jbyteArray payload, jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    args_in(env, &x, id, name, payload);
    if (x.ok) check(env, h, fusee_engine_signal(h->e, x.a[0].s, x.a[1].s, x.a[2].s, (uint64_t)token));
    args_free(&x);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_cancel(JNIEnv *env, jclass c, jlong hh, jbyteArray id,
                                                    jbyteArray reason, jboolean terminate, jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    args_in(env, &x, id, reason, NULL);
    if (x.ok) check(env, h, fusee_engine_cancel(h->e, x.a[0].s, x.a[1].s, terminate ? 1 : 0, (uint64_t)token));
    args_free(&x);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_resume(JNIEnv *env, jclass c, jlong hh, jbyteArray id, jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    args_in(env, &x, id, NULL, NULL);
    if (x.ok) check(env, h, fusee_engine_resume(h->e, x.a[0].s, (uint64_t)token));
    args_free(&x);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_update(JNIEnv *env, jclass c, jlong hh, jbyteArray id, jbyteArray name,
                                                    jbyteArray update_id, jbyteArray arg_bytes, jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    arg a3;
    args_in(env, &x, id, name, update_id);
    if (x.ok && arg_in(env, arg_bytes, &a3)) {
        check(env, h, fusee_engine_update(h->e, x.a[0].s, x.a[1].s, x.a[2].s, a3.s, (uint64_t)token));
        arg_free(&a3);
    }
    args_free(&x);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_complete(JNIEnv *env, jclass c, jlong hh, jbyteArray completion,
                                                      jboolean ok, jbyteArray payload, jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    args_in(env, &x, completion, payload, NULL);
    if (x.ok) check(env, h, fusee_engine_complete(h->e, x.a[0].s, ok ? 1 : 0, x.a[1].s, (uint64_t)token));
    args_free(&x);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_reset(JNIEnv *env, jclass c, jlong hh, jbyteArray id,
                                                   jbyteArray before_step, jboolean reapply, jint version,
                                                   jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    args_in(env, &x, id, before_step, NULL);
    if (x.ok) {
        check(env, h,
              fusee_engine_reset(h->e, x.a[0].s, x.a[1].s, reapply ? 1 : 0, (uint32_t)version, (uint64_t)token));
    }
    args_free(&x);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_ackAt(JNIEnv *env, jclass c, jlong hh, jint shard, jlong lsn,
                                                   jlong token) {
    (void)c;
    handle *h = H(hh);
    check(env, h, fusee_engine_ack_at(h->e, (uint32_t)shard, (uint64_t)lsn, (uint64_t)token));
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_queryNative(JNIEnv *env, jclass c, jlong hh, jbyteArray id,
                                                         jbyteArray name, jbyteArray arg_bytes, jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    args_in(env, &x, id, name, arg_bytes);
    if (x.ok) check(env, h, fusee_engine_query_native(h->e, x.a[0].s, x.a[1].s, x.a[2].s, (uint64_t)token));
    args_free(&x);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_streamRead(JNIEnv *env, jclass c, jlong hh, jbyteArray id, jlong run,
                                                        jbyteArray name, jlong from, jlong wait_ms, jlong token) {
    (void)c;
    handle *h = H(hh);
    args3 x;
    args_in(env, &x, id, name, NULL);
    if (x.ok) {
        check(env, h,
              fusee_engine_stream_read(h->e, x.a[0].s, (int64_t)run, x.a[1].s, (uint64_t)from, (uint64_t)wait_ms,
                                       (uint64_t)token));
    }
    args_free(&x);
}

/* ----------------------------------------------------------------- reads */

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_result(JNIEnv *env, jclass c, jlong hh, jbyteArray id,
                                                          jlongArray out) {
    (void)c;
    handle *h = H(hh);
    arg a;
    jbyteArray payload = NULL;
    if (!arg_in(env, id, &a)) return NULL;
    fusee_result r;
    memset(&r, 0, sizeof r);
    if (check(env, h, fusee_engine_result(h->e, a.s, &r))) {
        uint64_t v[4] = {r.state, r.shard, r.run, r.status};
        longs_out(env, out, v, 4);
        payload = str_out(env, r.payload);
    }
    arg_free(&a);
    return payload;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_inspect(JNIEnv *env, jclass c, jlong hh, jbyteArray id) {
    (void)c;
    handle *h = H(hh);
    arg a;
    jbyteArray json = NULL;
    if (!arg_in(env, id, &a)) return NULL;
    fusee_str j = {0};
    uint32_t found = 0;
    if (check(env, h, fusee_engine_inspect(h->e, a.s, &j, &found)) && found) json = str_out(env, j);
    arg_free(&a);
    return json;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_list(JNIEnv *env, jclass c, jlong hh, jbyteArray filter,
                                                        jint limit) {
    (void)c;
    handle *h = H(hh);
    arg a;
    jbyteArray json = NULL;
    if (!arg_in(env, filter, &a)) return NULL;
    fusee_str j = {0};
    if (check(env, h, fusee_engine_list(h->e, a.s, (uint32_t)limit, &j))) json = str_out(env, j);
    arg_free(&a);
    return json;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_exportJournal(JNIEnv *env, jclass c, jlong hh, jbyteArray id) {
    (void)c;
    handle *h = H(hh);
    arg a;
    jbyteArray file = NULL;
    if (!arg_in(env, id, &a)) return NULL;
    fusee_str j = {0};
    uint32_t found = 0;
    if (check(env, h, fusee_engine_export_journal(h->e, a.s, &j, &found)) && found) file = str_out(env, j);
    arg_free(&a);
    return file;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_shardMetrics(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    handle *h = H(hh);
    fusee_str j = {0};
    if (!check(env, h, fusee_engine_shard_metrics(h->e, &j))) return NULL;
    return str_out(env, j);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_queryPoint(JNIEnv *env, jclass c, jlong hh, jbyteArray id,
                                                        jlongArray out) {
    (void)c;
    handle *h = H(hh);
    arg a;
    if (!arg_in(env, id, &a)) return;
    uint32_t shard = 0, foreign = 0;
    uint64_t run = 0, lsn = 0;
    if (check(env, h, fusee_engine_query_point(h->e, a.s, &shard, &run, &lsn, &foreign))) {
        uint64_t v[4] = {shard, run, lsn, foreign};
        longs_out(env, out, v, 4);
    }
    arg_free(&a);
}

JNIEXPORT jint JNICALL Java_dev_fusee_Native_codeState(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run) {
    (void)c;
    handle *h = H(hh);
    uint32_t state = 0;
    if (!check(env, h, fusee_engine_code_state(h->e, (uint32_t)shard, (uint64_t)run, &state))) return 0;
    return (jint)state;
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_limit(JNIEnv *env, jclass c, jlong hh, jbyteArray name,
                                                   jint concurrent, jdouble per_second) {
    (void)c;
    handle *h = H(hh);
    arg a;
    if (!arg_in(env, name, &a)) return;
    check(env, h, fusee_engine_limit(h->e, a.s, (uint32_t)concurrent, per_second));
    arg_free(&a);
}

/* Marks `name` as an object type whose handlers in `shared` are shared. */
JNIEXPORT void JNICALL Java_dev_fusee_Native_registerObject(JNIEnv *env, jclass c, jlong hh, jbyteArray name,
                                                            jobjectArray shared) {
    (void)c;
    handle *h = H(hh);
    jsize n = shared ? (*env)->GetArrayLength(env, shared) : 0;
    arg a;
    if (!arg_in(env, name, &a)) return;
    arg *names = n ? calloc((size_t)n, sizeof(arg)) : NULL;
    fusee_str *strs = n ? calloc((size_t)n, sizeof(fusee_str)) : NULL;
    if (n && (!names || !strs)) {
        throw_oom(env);
        free(names);
        free(strs);
        arg_free(&a);
        return;
    }
    jsize got = 0;
    int ok = 1;
    for (; ok && got < n; got++) {
        jbyteArray s = (jbyteArray)(*env)->GetObjectArrayElement(env, shared, got);
        ok = arg_in(env, s, &names[got]);
        if (s) (*env)->DeleteLocalRef(env, s);
        if (ok) strs[got] = names[got].s;
    }
    if (ok) check(env, h, fusee_engine_register_object(h->e, a.s, strs, (uint32_t)n));
    for (jsize i = 0; i < got; i++) arg_free(&names[i]);
    free(names);
    free(strs);
    arg_free(&a);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_limitUsage(JNIEnv *env, jclass c, jlong hh, jbyteArray name,
                                                        jlongArray out) {
    (void)c;
    handle *h = H(hh);
    arg a;
    if (!arg_in(env, name, &a)) return;
    uint32_t held = 0;
    uint64_t waiting = 0;
    if (check(env, h, fusee_engine_limit_usage(h->e, a.s, &held, &waiting))) {
        uint64_t v[2] = {held, waiting};
        longs_out(env, out, v, 2);
    }
    arg_free(&a);
}

/* ---------------------------------------------------------- workflow code */

JNIEXPORT void JNICALL Java_dev_fusee_Native_beginStep(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run,
                                                       jint name_len, jint flags) {
    (void)c;
    handle *h = H(hh);
    fusee_str name;
    if (!in_str(env, h, 0, name_len, &name)) return;
    fusee_step st;
    memset(&st, 0, sizeof st);
    if (check(env, h, fusee_engine_begin_step(h->e, (uint32_t)shard, (uint64_t)run, name, (uint32_t)flags, &st))) {
        slot(h, 0, st.seq);
        slot(h, 1, st.replay);
        slot(h, 2, st.gate);
        slot(h, 3, st.attempt);
        slot(h, 4, st.open);
        slot(h, 5, st.not_before_ms);
    }
}

JNIEXPORT jboolean JNICALL Java_dev_fusee_Native_stepGate(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run,
                                                          jlong gate, jlong not_before, jboolean inline_flush) {
    (void)c;
    handle *h = H(hh);
    uint32_t open = 0;
    if (!check(env, h,
               fusee_engine_step_gate(h->e, (uint32_t)shard, (uint64_t)run, (uint64_t)gate, (uint64_t)not_before,
                                      inline_flush ? 1 : 0, &open))) {
        return JNI_FALSE;
    }
    return open ? JNI_TRUE : JNI_FALSE;
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_stepDone(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run,
                                                      jint seq, jint flags, jint len) {
    (void)c;
    handle *h = H(hh);
    fusee_str payload;
    if (!in_str(env, h, 0, len, &payload)) return;
    check(env, h,
          fusee_engine_step_done(h->e, (uint32_t)shard, (uint64_t)run, (uint32_t)seq, (uint32_t)flags, payload));
}

JNIEXPORT jlong JNICALL Java_dev_fusee_Native_stepRetry(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run,
                                                        jint seq, jlong retry_at, jint len) {
    (void)c;
    handle *h = H(hh);
    fusee_str error;
    if (!in_str(env, h, 0, len, &error)) return 0;
    uint64_t gate = 0;
    check(env, h,
          fusee_engine_step_retry(h->e, (uint32_t)shard, (uint64_t)run, (uint32_t)seq, (uint64_t)retry_at, error,
                                  &gate));
    return (jlong)gate;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_host(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run,
                                                        jint op, jlong n0, jlong n1, jlong n2, jint l0, jint l1,
                                                        jint l2) {
    (void)c;
    handle *h = H(hh);
    size_t o1 = (size_t)(l0 > 0 ? l0 : 0), o2 = o1 + (size_t)(l1 > 0 ? l1 : 0);
    fusee_str s0, s1, s2;
    if (!in_str(env, h, 0, l0, &s0) || !in_str(env, h, o1, l1, &s1) || !in_str(env, h, o2, l2, &s2)) return NULL;
    uint64_t result[5] = {0, 0, 0, 0, 0};
    fusee_str data = {0};
    int32_t rc = fusee_engine_host(h->e, (uint32_t)shard, (uint64_t)run, (uint32_t)op, (uint64_t)n0, (uint64_t)n1,
                                   (uint64_t)n2, s0, s1, s2, result, &data);
    for (int i = 0; i < 5; i++) slot(h, i, result[i]);
    if (!check(env, h, rc)) return NULL;
    return data_out(env, h, data.ptr, data.len);
}

JNIEXPORT jint JNICALL Java_dev_fusee_Native_quiescent(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run) {
    (void)c;
    handle *h = H(hh);
    fusee_delivery *d = &h->d;
    memset(d, 0, sizeof *d);
    if (!check(env, h, fusee_engine_quiescent(h->e, (uint32_t)shard, (uint64_t)run, d))) return 0;
    if (d->kind != FUSEE_DELIVERY_NONE) {
        slot(h, 0, d->seq);
        slot(h, 1, d->status);
        slot(h, 2, d->n_seqs);
        slot(h, 3, d->ordinal);
        slot(h, 4, (uint64_t)d->token);
        slot(h, 5, d->payload.len);
        slot(h, 6, d->name.len);
        slot(h, 7, d->id.len);
        size_t n = d->payload.len + d->name.len + d->id.len;
        if (n == 0) {
            slot(h, OUT_LEN_SLOT, (uint64_t)-1);
        } else if (n <= h->out_cap - OUT_DATA) {
            uint8_t *at = h->out + OUT_DATA;
            if (d->payload.len) memcpy(at, d->payload.ptr, d->payload.len);
            if (d->name.len) memcpy(at + d->payload.len, d->name.ptr, d->name.len);
            if (d->id.len) memcpy(at + d->payload.len + d->name.len, d->id.ptr, d->id.len);
            slot(h, OUT_LEN_SLOT, n);
        } else {
            slot(h, OUT_LEN_SLOT, (uint64_t)-2); /* fetch them with deliveryBytes */
        }
    }
    return (jint)d->kind;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_deliveryBytes(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    fusee_delivery *d = &H(hh)->d;
    size_t n = d->payload.len + d->name.len + d->id.len;
    if (n == 0) return NULL;
    jbyteArray a = (*env)->NewByteArray(env, (jsize)n);
    if (!a) return NULL;
    jsize at = 0;
    if (d->payload.len) {
        (*env)->SetByteArrayRegion(env, a, at, (jsize)d->payload.len, (const jbyte *)d->payload.ptr);
        at += (jsize)d->payload.len;
    }
    if (d->name.len) {
        (*env)->SetByteArrayRegion(env, a, at, (jsize)d->name.len, (const jbyte *)d->name.ptr);
        at += (jsize)d->name.len;
    }
    if (d->id.len) (*env)->SetByteArrayRegion(env, a, at, (jsize)d->id.len, (const jbyte *)d->id.ptr);
    return a;
}

JNIEXPORT jintArray JNICALL Java_dev_fusee_Native_deliverySeqs(JNIEnv *env, jclass c, jlong hh) {
    (void)c;
    fusee_delivery *d = &H(hh)->d;
    jintArray a = (*env)->NewIntArray(env, (jsize)d->n_seqs);
    if (a && d->n_seqs) (*env)->SetIntArrayRegion(env, a, 0, (jsize)d->n_seqs, (const jint *)d->seqs);
    return a;
}

JNIEXPORT jlong JNICALL Java_dev_fusee_Native_decideUpdate(JNIEnv *env, jclass c, jlong hh, jint shard, jlong token,
                                                           jboolean ok, jbyteArray message) {
    (void)c;
    handle *h = H(hh);
    arg a;
    if (!arg_in(env, message, &a)) return -1;
    int64_t ordinal = -1;
    if (!check(env, h,
               fusee_engine_decide_update(h->e, (uint32_t)shard, (uint64_t)token, ok ? 1 : 0, a.s, &ordinal))) {
        ordinal = -1;
    }
    arg_free(&a);
    return (jlong)ordinal;
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_finish(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run,
                                                    jint status, jint len) {
    (void)c;
    handle *h = H(hh);
    fusee_str payload;
    if (!in_str(env, h, 0, len, &payload)) return;
    check(env, h, fusee_engine_finish(h->e, (uint32_t)shard, (uint64_t)run, (uint32_t)status, payload));
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_stuck(JNIEnv *env, jclass c, jlong hh, jint shard, jlong run,
                                                   jbyteArray message) {
    (void)c;
    handle *h = H(hh);
    arg a;
    if (!arg_in(env, message, &a)) return;
    check(env, h, fusee_engine_stuck(h->e, (uint32_t)shard, (uint64_t)run, a.s));
    arg_free(&a);
}

/* -------------------------------------------------------- free functions */

JNIEXPORT jobjectArray JNICALL Java_dev_fusee_Native_queueRequest(JNIEnv *env, jclass c, jint kind,
                                                                  jbyteArray workflow, jbyteArray key,
                                                                  jbyteArray event_id, jbyteArray input, jdouble a,
                                                                  jdouble b, jdouble cc) {
    (void)c;
    args3 x;
    arg a3;
    jobjectArray result = NULL;
    args_in(env, &x, workflow, key, event_id);
    if (x.ok && arg_in(env, input, &a3)) {
        fusee_buf out[4];
        memset(out, 0, sizeof out);
        fusee_buf err = {0};
        int32_t rc = fusee_queue_request((uint32_t)kind, x.a[0].s, x.a[1].s, x.a[2].s, a3.s, a, b, cc, out, &err);
        if (check_buf(env, rc, &err)) {
            jclass bytes = (*env)->FindClass(env, "[B");
            result = bytes ? (*env)->NewObjectArray(env, 4, bytes, NULL) : NULL;
            for (int i = 0; result && i < 4; i++) {
                jbyteArray v = buf_out(env, &out[i]);
                (*env)->SetObjectArrayElement(env, result, i, v);
                (*env)->DeleteLocalRef(env, v);
            }
        }
        for (int i = 0; i < 4; i++) fusee_buf_free(&out[i]);
        arg_free(&a3);
    }
    args_free(&x);
    return result;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_validateSchedule(JNIEnv *env, jclass c, jbyteArray spec) {
    (void)c;
    arg a;
    if (!arg_in(env, spec, &a)) return NULL;
    fusee_buf problem = {0};
    int32_t rc = fusee_validate_schedule(a.s, &problem);
    arg_free(&a);
    if (rc == FUSEE_OK) {
        fusee_buf_free(&problem);
        return NULL;
    }
    return buf_out(env, &problem);
}

/* ---------------------------------------------------------------- client */

JNIEXPORT jlong JNICALL Java_dev_fusee_Native_clientConnect(JNIEnv *env, jclass c, jbyteArray address) {
    (void)c;
    arg a;
    if (!arg_in(env, address, &a)) return 0;
    fusee_client *cl = NULL;
    fusee_buf err = {0};
    int32_t rc = fusee_client_connect(a.s, &cl, &err);
    arg_free(&a);
    if (!check_buf(env, rc, &err)) return 0;
    return (jlong)(intptr_t)cl;
}

JNIEXPORT jbyteArray JNICALL Java_dev_fusee_Native_clientCall(JNIEnv *env, jclass c, jlong cl, jbyteArray method,
                                                              jbyteArray args_bytes) {
    (void)c;
    args3 x;
    jbyteArray out = NULL;
    args_in(env, &x, method, args_bytes, NULL);
    if (x.ok) {
        fusee_buf result = {0};
        int32_t rc = fusee_client_call((const fusee_client *)(intptr_t)cl, x.a[0].s, x.a[1].s, &result);
        if (rc == FUSEE_OK) {
            out = buf_out(env, &result);
        } else {
            throw_code(env, rc, result.ptr, result.len);
            fusee_buf_free(&result);
        }
    }
    args_free(&x);
    return out;
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_clientClose(JNIEnv *env, jclass c, jlong cl) {
    (void)env;
    (void)c;
    fusee_client_close((const fusee_client *)(intptr_t)cl);
}

JNIEXPORT void JNICALL Java_dev_fusee_Native_clientFree(JNIEnv *env, jclass c, jlong cl) {
    (void)env;
    (void)c;
    fusee_client_free((fusee_client *)(intptr_t)cl);
}
