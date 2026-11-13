// Counts allocations in the whole process (DYLD_INSERT_LIBRARIES): malloc,
// calloc, posix_memalign, aligned_alloc and realloc. At exit writes
// "<allocs> <reallocs>" to $COUNTMALLOC_OUT; countmalloc_total() reads it live.
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
static _Atomic unsigned long long allocs, reallocs;
static void *m_malloc(size_t s) { atomic_fetch_add_explicit(&allocs, 1, memory_order_relaxed); return malloc(s); }
static void *m_calloc(size_t a, size_t b) { atomic_fetch_add_explicit(&allocs, 1, memory_order_relaxed); return calloc(a, b); }
static int m_posix_memalign(void **p, size_t a, size_t s) { atomic_fetch_add_explicit(&allocs, 1, memory_order_relaxed); return posix_memalign(p, a, s); }
static void *m_aligned_alloc(size_t a, size_t s) { atomic_fetch_add_explicit(&allocs, 1, memory_order_relaxed); return aligned_alloc(a, s); }
static void *m_realloc(void *p, size_t s) { atomic_fetch_add_explicit(&reallocs, 1, memory_order_relaxed); return realloc(p, s); }
__attribute__((used)) static const struct { const void *r, *o; } interposers[] __attribute__((section("__DATA,__interpose"))) = {
    {(const void *)m_malloc, (const void *)malloc},
    {(const void *)m_calloc, (const void *)calloc},
    {(const void *)m_posix_memalign, (const void *)posix_memalign},
    {(const void *)m_aligned_alloc, (const void *)aligned_alloc},
    {(const void *)m_realloc, (const void *)realloc},
};
unsigned long long countmalloc_total(void) { return allocs; }
unsigned long long countmalloc_reallocs(void) { return reallocs; }
__attribute__((destructor)) static void report(void) {
    const char *f = getenv("COUNTMALLOC_OUT");
    if (!f) return;
    FILE *o = fopen(f, "w");
    if (o) { fprintf(o, "%llu %llu\n", (unsigned long long)allocs, (unsigned long long)reallocs); fclose(o); }
}
