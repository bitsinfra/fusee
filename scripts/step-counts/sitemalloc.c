// Like countmalloc, plus: while sitemalloc_on(1), every allocation's call site
// (the first frames outside the allocator and libc, by symbol) is tallied;
// sitemalloc_dump(path) writes "count  frame <- frame <- frame" lines.
#include <dlfcn.h>
#include <execinfo.h>
#include <pthread.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
static _Atomic unsigned long long allocs;
static _Atomic int on;
static __thread int inside;
#define SLOTS 4096
static struct { char key[600]; unsigned long long n; } table[SLOTS];
static pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;
static unsigned hash(const char *s) { unsigned h = 2166136261u; while (*s) { h ^= (unsigned char)*s++; h *= 16777619u; } return h; }
static int skip(const char *n) { return !n || strstr(n, "sitemalloc") || strstr(n, "m_malloc") || strstr(n, "m_calloc") || strstr(n, "m_realloc") || strstr(n, "m_posix") || !strncmp(n, "_malloc", 7) || strstr(n, "__rdl_") || strstr(n, "__rust_alloc") || strstr(n, "alloc::alloc") || strstr(n, "raw_vec") || strstr(n, "RawVec") || strstr(n, "_PyObject_Malloc") || strstr(n, "PyMem_") || strstr(n, "_PyMem") || strstr(n, "pymalloc"); }
static void note(void) {
    atomic_fetch_add_explicit(&allocs, 1, memory_order_relaxed);
    if (!atomic_load_explicit(&on, memory_order_relaxed) || inside) return;
    inside = 1;
    void *fr[24]; int n = backtrace(fr, 24);
    char key[600]; key[0] = 0; int used = 0;
    for (int i = 1; i < n && used < 3; i++) {
        Dl_info d; if (!dladdr(fr[i], &d) || !d.dli_sname) continue;
        if (skip(d.dli_sname)) continue;
        const char *lib = d.dli_fname ? strrchr(d.dli_fname, '/') : 0;
        char part[200]; snprintf(part, sizeof part, "%s%.150s[%s]", used ? " <- " : "", d.dli_sname, lib ? lib + 1 : "?");
        strncat(key, part, sizeof key - strlen(key) - 1); used++;
    }
    unsigned h = hash(key) % SLOTS;
    pthread_mutex_lock(&mu);
    for (int k = 0; k < SLOTS; k++) { unsigned s = (h + k) % SLOTS; if (!table[s].key[0]) { strcpy(table[s].key, key); table[s].n = 1; break; } if (!strcmp(table[s].key, key)) { table[s].n++; break; } }
    pthread_mutex_unlock(&mu);
    inside = 0;
}
static void *m_malloc(size_t s) { note(); return malloc(s); }
static void *m_calloc(size_t a, size_t b) { note(); return calloc(a, b); }
static int m_posix_memalign(void **p, size_t a, size_t s) { note(); return posix_memalign(p, a, s); }
static void *m_aligned_alloc(size_t a, size_t s) { note(); return aligned_alloc(a, s); }
static void *m_realloc(void *p, size_t s) { return realloc(p, s); }
__attribute__((used)) static const struct { const void *r, *o; } interposers[] __attribute__((section("__DATA,__interpose"))) = {
    {(const void *)m_malloc, (const void *)malloc}, {(const void *)m_calloc, (const void *)calloc},
    {(const void *)m_posix_memalign, (const void *)posix_memalign}, {(const void *)m_aligned_alloc, (const void *)aligned_alloc},
    {(const void *)m_realloc, (const void *)realloc},
};
unsigned long long countmalloc_total(void) { return allocs; }
void sitemalloc_on(int v) { atomic_store(&on, v); }
void sitemalloc_dump(const char *path) {
    FILE *o = fopen(path, "w"); if (!o) return;
    for (int s = 0; s < SLOTS; s++) if (table[s].key[0]) fprintf(o, "%llu\t%s\n", table[s].n, table[s].key);
    fclose(o);
}
