/* Test module reproducing the glibc arena-lock crash deadlock. */

#define _GNU_SOURCE
#include "valkeymodule.h"

#include <stdint.h>
#include <string.h>
#include <stdlib.h>

/* This reproduction depends on glibc's inline chunk-header layout. */
#if defined(__has_include)
#if __has_include(<features.h>)
#include <features.h>
#endif
#endif

#define ARENALOCK_GLIBC 0
#if defined(__GLIBC__) && !defined(__APPLE__)
#undef ARENALOCK_GLIBC
#define ARENALOCK_GLIBC 1
#endif

#define UNUSED(V) ((void)V)

/* ARENALOCK.CRASH
 * Corrupt a chunk header and realloc to abort with the arena lock held. */
static int cmd_crash(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(ctx);
    UNUSED(argv);
    UNUSED(argc);

#if !ARENALOCK_GLIBC
    ValkeyModule_Log(ctx, "warning",
                     "arenalock: skipping, reproduction requires glibc (not macOS/musl)");
    ValkeyModule_ReplyWithError(ctx, "ERR arenalock.crash requires glibc (skipped on macOS/Alpine)");
    return VALKEYMODULE_ERR;
#else
    size_t n = 64;
    unsigned char *p = (unsigned char *)malloc(n);
    if (p == NULL) {
        ValkeyModule_ReplyWithError(ctx, "ERR malloc failed");
        return VALKEYMODULE_ERR;
    }
    memset(p, 'A', n);

    /* glibc stores the chunk size one machine word before the user pointer.
     * Low 3 bits are flags (PREV_INUSE/IS_MMAPPED/NON_MAIN_ARENA); keep
     * IS_MMAPPED clear so the chunk is treated as arena-backed. */
    size_t *size_word = (size_t *)(p - sizeof(size_t));
    ValkeyModule_Log(ctx, "warning", "arenalock: ptr=%p size_word@%p before=0x%zx",
        (void *)p, (void *)size_word, *size_word);

    /* Preserve the low flag bits and only inflate the size field itself, so the
     * chunk still looks arena-backed but with a size far larger than av->system_mem. */
    size_t orig = *size_word;
    size_t flags = orig & 0x7;
    *size_word = (size_t)0x4141414141414000ULL | flags;
    ValkeyModule_Log(ctx, "warning", "arenalock: size_word after=0x%zx (flags preserved=0x%zx), calling realloc",
                     *size_word, flags);

    /* realloc aborts while holding the lock. */
    void *q = realloc(p, 128);
    ValkeyModule_Log(ctx, "warning", "arenalock: realloc returned %p (NO abort)", q);
    (void)q;

    /* Unreachable on a successful reproduction. */
    ValkeyModule_ReplyWithError(ctx, "ERR realloc did not abort");
    return VALKEYMODULE_ERR;
#endif /* ARENALOCK_GLIBC */
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    UNUSED(argv);
    UNUSED(argc);

    if (ValkeyModule_Init(ctx, "arenalock", 1, VALKEYMODULE_APIVER_1) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    if (ValkeyModule_CreateCommand(ctx, "arenalock.crash", cmd_crash, "", 0, 0, 0) == VALKEYMODULE_ERR)
        return VALKEYMODULE_ERR;

    return VALKEYMODULE_OK;
}
