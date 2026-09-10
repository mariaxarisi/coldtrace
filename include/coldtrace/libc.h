/*
 * Copyright (C) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MIT
 */

#ifndef COLDTRACE_LIBC_H
#define COLDTRACE_LIBC_H

#include <stddef.h>
#include <sys/types.h>

#ifdef __cplusplus
extern "C" {
#endif

#include <dice/interpose.h>
#include <vsync/atomic.h>

#define COLDTRACE_REAL_(NAME)                                                  \
    ({                                                                         \
        static vatomicptr_t cached_;                                           \
        void *p_ = vatomicptr_read_acq(&cached_);                              \
        if (p_ == NULL) {                                                      \
            p_ = real_sym((NAME), NULL);                                       \
            vatomicptr_write_rel(&cached_, p_);                                \
        }                                                                      \
        p_;                                                                    \
    })

/* --- (dice/events/pthread.h) --- */

static inline int
coldtrace_pthread_create(pthread_t *thread, const pthread_attr_t *attr,
                         void *(*start_routine)(void *), void *arg)
{
    typedef int (*fn_t)(pthread_t *, const pthread_attr_t *, void *(*)(void *),
                        void *);
    fn_t fn = (fn_t)COLDTRACE_REAL_("pthread_create");
    return fn(thread, attr, start_routine, arg);
}

static inline int
coldtrace_pthread_join(pthread_t thread, void **retval)
{
    typedef int (*fn_t)(pthread_t, void **);
    fn_t fn = (fn_t)COLDTRACE_REAL_("pthread_join");
    return fn(thread, retval);
}

static inline void
coldtrace_pthread_exit(void *retval)
{
    typedef void (*fn_t)(void *);
    fn_t fn = (fn_t)COLDTRACE_REAL_("pthread_exit");
    fn(retval);
}

static inline int
coldtrace_pthread_mutex_lock(pthread_mutex_t *mutex)
{
    typedef int (*fn_t)(pthread_mutex_t *);
    fn_t fn = (fn_t)COLDTRACE_REAL_("pthread_mutex_lock");
    return fn(mutex);
}

static inline int
coldtrace_pthread_mutex_unlock(pthread_mutex_t *mutex)
{
    typedef int (*fn_t)(pthread_mutex_t *);
    fn_t fn = (fn_t)COLDTRACE_REAL_("pthread_mutex_unlock");
    return fn(mutex);
}

static inline int
coldtrace_pthread_cond_wait(pthread_cond_t *cond, pthread_mutex_t *mutex)
{
    typedef int (*fn_t)(pthread_cond_t *, pthread_mutex_t *);
    fn_t fn = (fn_t)COLDTRACE_REAL_("pthread_cond_wait");
    return fn(cond, mutex);
}

static inline int
coldtrace_pthread_cond_signal(pthread_cond_t *cond)
{
    typedef int (*fn_t)(pthread_cond_t *);
    fn_t fn = (fn_t)COLDTRACE_REAL_("pthread_cond_signal");
    return fn(cond);
}

/* --- (dice/events/malloc.h) --- */

static inline void *
coldtrace_malloc(size_t size)
{
    typedef void *(*fn_t)(size_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("malloc");
    return fn(size);
}

static inline void *
coldtrace_calloc(size_t number, size_t size)
{
    typedef void *(*fn_t)(size_t, size_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("calloc");
    return fn(number, size);
}

static inline void *
coldtrace_realloc(void *ptr, size_t size)
{
    typedef void *(*fn_t)(void *, size_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("realloc");
    return fn(ptr, size);
}

static inline void
coldtrace_free(void *ptr)
{
    typedef void (*fn_t)(void *);
    fn_t fn = (fn_t)COLDTRACE_REAL_("free");
    fn(ptr);
}

/* --- (dice/events/memcpy.h) --- */

static inline void *
coldtrace_memcpy(void *dest, const void *src, size_t n)
{
    typedef void *(*fn_t)(void *, const void *, size_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("memcpy");
    return fn(dest, src, n);
}

static inline void *
coldtrace_memmove(void *dest, const void *src, size_t n)
{
    typedef void *(*fn_t)(void *, const void *, size_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("memmove");
    return fn(dest, src, n);
}

static inline void *
coldtrace_memset(void *ptr, int value, size_t n)
{
    typedef void *(*fn_t)(void *, int, size_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("memset");
    return fn(ptr, value, n);
}

/* --- (dice/events/mman.h) --- */

static inline void *
coldtrace_mmap(void *addr, size_t length, int prot, int flags, int fd,
               off_t offset)
{
    typedef void *(*fn_t)(void *, size_t, int, int, int, off_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("mmap");
    return fn(addr, length, prot, flags, fd, offset);
}

static inline int
coldtrace_munmap(void *addr, size_t length)
{
    typedef int (*fn_t)(void *, size_t);
    fn_t fn = (fn_t)COLDTRACE_REAL_("munmap");
    return fn(addr, length);
}

#ifdef __cplusplus
}
#endif

#endif /* COLDTRACE_LIBC_H */
