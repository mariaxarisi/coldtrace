#include <coldtrace/config.h>
#include <coldtrace/libc.h>
#include <coldtrace/version.h>
#include <coldtrace/writer.h>
#include <dice/compiler.h>
#include <dice/log.h>
#include <dice/mempool.h>
#include <dice/self.h>
#include <dice/types.h>
#include <errno.h>
#include <fcntl.h>
#include <lz4.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <unistd.h>

#define LZ4_ACCELERATION       1
#define FORMAT_EXPANSION_SPACE 20
#define NBUFFERS               2
#define FILE_PERMISSIONS                                                       \
    (S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)

enum slot_state {
    SLOT_EMPTY = 0,
    SLOT_FULL,
};

struct trace_buf {
    uint64_t *data;
    char *comp_buf;
    uint64_t offset;
    uint32_t enumerator;
    enum slot_state state;
};

struct writer_worker {
    struct trace_buf bufs[NBUFFERS];
    pthread_t thread;
    pthread_mutex_t lock;
    pthread_cond_t filled;
    pthread_cond_t drained;
    uint64_t tid;
    uint64_t size;
    bool stop;
    bool failed;
    unsigned current;
};

struct writer_impl {
    bool initd;
    bool failed;
    struct writer_worker *worker;
    uint64_t *buffer;
    uint64_t size;
    uint64_t offset;
    uint64_t tid;
    uint32_t enumerator;
    metadata_t *md;
};

// Ensure the size of implementation matches the public size.
STATIC_ASSERT(sizeof(struct writer_impl) == sizeof(struct coldtrace_writer),
              "incorrect writer_impl size");

static bool
write_all_(int fd, const void *buf, size_t count)
{
    const char *p    = (const char *)buf;
    size_t remaining = count;
    while (remaining > 0) {
        ssize_t w = write(fd, p, remaining);
        if (w == -1) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        p += (size_t)w;
        remaining -= (size_t)w;
    }
    return true;
}

// Compress buf->data[0, offset) and write it to its fragment file as
// [uint32_t raw_size][lz4 payload]. Runs on the worker thread.
static void
write_buffer_(struct writer_worker *w, struct trace_buf *buf)
{
    size_t raw = buf->offset;

    int comp =
        LZ4_compress_fast((const char *)buf->data, buf->comp_buf, (int)raw,
                          LZ4_compressBound((int)w->size), LZ4_ACCELERATION);
    if (comp <= 0) {
        log_warn("worker: LZ4 compression failed");
        w->failed = true;
        return;
    }

    const char *pattern = coldtrace_get_file_pattern();
    char file_name[strlen(pattern) + FORMAT_EXPANSION_SPACE];
    sprintf(file_name, pattern, w->tid, buf->enumerator);

    int fd = open(file_name, O_WRONLY | O_CREAT | O_TRUNC, FILE_PERMISSIONS);
    if (fd == -1) {
        log_warn("worker open: %s", strerror(errno));
        w->failed = true;
        return;
    }

    uint32_t raw_size = (uint32_t)raw;
    if (!write_all_(fd, &raw_size, sizeof(raw_size)) ||
        !write_all_(fd, buf->comp_buf, (size_t)comp)) {
        log_warn("worker write: %s", strerror(errno));
        close(fd);
        w->failed = true;
        return;
    }

    close(fd);
}

// Worker loop: drain FULL buffers to disk, flipping each back to EMPTY, until
// told to stop and no buffers remain FULL.
static void *
worker_main_(void *arg)
{
    struct writer_worker *w = (struct writer_worker *)arg;

    coldtrace_pthread_mutex_lock(&w->lock);
    for (;;) {
        struct trace_buf *full = NULL;
        for (unsigned i = 0; i < NBUFFERS; i++) {
            if (w->bufs[i].state == SLOT_FULL) {
                full = &w->bufs[i];
                break;
            }
        }

        if (full == NULL) {
            if (w->stop) {
                break;
            }
            coldtrace_pthread_cond_wait(&w->filled, &w->lock);
            continue;
        }

        coldtrace_pthread_mutex_unlock(&w->lock);
        write_buffer_(w, full);
        coldtrace_pthread_mutex_lock(&w->lock);

        full->state = SLOT_EMPTY;
        coldtrace_pthread_cond_signal(&w->drained);
    }
    coldtrace_pthread_mutex_unlock(&w->lock);
    return NULL;
}

// Hand the current buffer to the worker and take the other one, blocking until
// it is EMPTY.
static struct trace_buf *
handoff_and_acquire_(struct writer_worker *w, uint64_t offset)
{
    unsigned cur  = w->current;
    unsigned next = (cur + 1) % NBUFFERS;

    coldtrace_pthread_mutex_lock(&w->lock);

    if (w->failed) {
        coldtrace_pthread_mutex_unlock(&w->lock);
        return NULL;
    }

    // Publish the filled buffer to the worker.
    w->bufs[cur].offset = offset;
    w->bufs[cur].state  = SLOT_FULL;
    coldtrace_pthread_cond_signal(&w->filled);

    // Wait for the next buffer to be free.
    while (w->bufs[next].state != SLOT_EMPTY && !w->failed) {
        coldtrace_pthread_cond_wait(&w->drained, &w->lock);
    }

    if (w->failed) {
        coldtrace_pthread_mutex_unlock(&w->lock);
        return NULL;
    }

    w->current = next;
    coldtrace_pthread_mutex_unlock(&w->lock);
    return &w->bufs[next];
}

// Allocate one trace buffer + its compression scratch.
static bool
alloc_buf_(struct trace_buf *buf, uint64_t size)
{
    buf->data =
        coldtrace_mmap(NULL, size, PROT_READ | PROT_WRITE,
                       MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
    if (buf->data == MAP_FAILED) {
        buf->data = NULL;
        return false;
    }
    buf->comp_buf = coldtrace_malloc(LZ4_compressBound((int)size));
    if (buf->comp_buf == NULL) {
        coldtrace_munmap(buf->data, size);
        buf->data = NULL;
        return false;
    }
    buf->offset     = 0;
    buf->enumerator = 0;
    buf->state      = SLOT_EMPTY;
    return true;
}

static void
free_buf_(struct trace_buf *buf, uint64_t size)
{
    if (buf->comp_buf != NULL) {
        coldtrace_free(buf->comp_buf);
        buf->comp_buf = NULL;
    }
    if (buf->data != NULL) {
        coldtrace_munmap(buf->data, size);
        buf->data = NULL;
    }
}

static struct writer_worker *
worker_create_(uint64_t tid, uint64_t size)
{
    struct writer_worker *w = coldtrace_calloc(1, sizeof(*w));
    if (w == NULL) {
        return NULL;
    }
    w->tid  = tid;
    w->size = size;

    unsigned allocated = 0;
    for (; allocated < NBUFFERS; allocated++) {
        if (!alloc_buf_(&w->bufs[allocated], size)) {
            break;
        }
    }

    if (allocated < NBUFFERS) {
        goto destroy_bufs;
    }
    if (pthread_mutex_init(&w->lock, NULL) != 0) {
        goto destroy_bufs;
    }
    if (pthread_cond_init(&w->filled, NULL) != 0) {
        goto destroy_mutex;
    }
    if (pthread_cond_init(&w->drained, NULL) != 0) {
        goto destroy_filled;
    }
    if (coldtrace_pthread_create(&w->thread, NULL, worker_main_, w) != 0) {
        goto destroy_drained;
    }
    return w;

destroy_drained:
    pthread_cond_destroy(&w->drained);
destroy_filled:
    pthread_cond_destroy(&w->filled);
destroy_mutex:
    pthread_mutex_destroy(&w->lock);
destroy_bufs:
    for (unsigned i = 0; i < allocated; i++) {
        free_buf_(&w->bufs[i], size);
    }
    coldtrace_free(w);
    return NULL;
}

// Signal shutdown, join the worker (which finishes draining every FULL
// buffer), then release all buffers and sync primitives.
static void
worker_destroy_(struct writer_worker *w)
{
    coldtrace_pthread_mutex_lock(&w->lock);
    w->stop = true;
    coldtrace_pthread_cond_signal(&w->filled);
    coldtrace_pthread_mutex_unlock(&w->lock);

    coldtrace_pthread_join(w->thread, NULL);

    pthread_cond_destroy(&w->drained);
    pthread_cond_destroy(&w->filled);
    pthread_mutex_destroy(&w->lock);
    for (unsigned i = 0; i < NBUFFERS; i++) {
        free_buf_(&w->bufs[i], w->size);
    }
    coldtrace_free(w);
}

// Stamp the version header at the front of a fresh fragment and position
// offset just past it
static void
begin_fragment_(struct writer_impl *impl)
{
    struct version_header *header = (struct version_header *)impl->buffer;
    *header                       = current_version_header;
    impl->offset                  = sizeof(struct version_header);
}

static bool
ensure_buffer_(struct writer_impl *impl)
{
    if (!impl->initd) {
        log_warn("Writer not initialized (at %s:%d)", __FILE__, __LINE__);
        impl->buffer = NULL;
        return false;
    }
    if (impl->failed) {
        impl->buffer = NULL;
        return false;
    }
    if (impl->buffer) {
        return true;
    }

    impl->size       = coldtrace_get_trace_size();
    impl->enumerator = 0;

    if (coldtrace_writes_disabled()) {
        impl->buffer = mempool_alloc(impl->size);
        if (impl->buffer == NULL) {
            return false;
        }
        begin_fragment_(impl);
        return true;
    }

    // Create the worker and fill its first buffer.
    impl->worker = worker_create_(impl->tid, impl->size);
    if (impl->worker == NULL) {
        log_warn("ensure_buffer: could not start worker");
        impl->buffer = NULL;
        impl->failed = true;
        return false;
    }

    impl->worker->current = 0;
    impl->buffer          = impl->worker->bufs[0].data;
    begin_fragment_(impl);
    return true;
}

static void
new_trace_(struct writer_impl *impl)
{
    if (coldtrace_writes_disabled()) {
        coldtrace_writer_close(impl->buffer, impl->offset, impl->md);

        size_t trace_size = coldtrace_get_trace_size();
        if (impl->size != trace_size) {
            impl->size = trace_size;
            mempool_free(impl->buffer);
            impl->buffer = mempool_alloc(impl->size);
            if (impl->buffer == NULL) {
                impl->failed = true;
                return;
            }
        }
        begin_fragment_(impl);
        return;
    }

    coldtrace_writer_close(impl->buffer, impl->offset, impl->md);

    struct writer_worker *w        = impl->worker;
    w->bufs[w->current].enumerator = impl->enumerator;
    impl->enumerator = (impl->enumerator + 1) % coldtrace_get_max();

    struct trace_buf *next = handoff_and_acquire_(w, impl->offset);
    if (next == NULL) {
        impl->buffer = NULL;
        impl->failed = true;
        return;
    }

    impl->buffer = next->data;
    begin_fragment_(impl);
}

DICE_HIDE bool
coldtrace_writer_new_trace(struct coldtrace_writer *ct, size_t size)
{
    struct writer_impl *impl = (struct writer_impl *)ct;
    size_t sz                = coldtrace_get_trace_size();
    size_t trace_size        = impl->size;
    if (unlikely(sz < trace_size)) {
        trace_size = sz;
    }

    return (impl->offset + size) > trace_size;
}


DICE_HIDE void *
coldtrace_writer_reserve(struct coldtrace_writer *ct, size_t size)
{
    struct writer_impl *impl = (struct writer_impl *)ct;
    if (!ensure_buffer_(impl)) {
        return NULL;
    }

    if (coldtrace_writer_new_trace(ct, size)) {
        new_trace_(impl);
        if (impl->buffer == NULL) {
            return NULL;
        }
    }

    char *ptr = (char *)(impl->buffer) + impl->offset;
    impl->offset += size;
    return ptr;
}

DICE_HIDE void
coldtrace_writer_init(struct coldtrace_writer *ct, metadata_t *md)
{
    struct writer_impl *impl = (struct writer_impl *)ct;
    if (md == NULL) {
        log_warn("No metadata provided (at %s:%d)", __FILE__, __LINE__);
        impl->initd  = false;
        impl->buffer = NULL;
        return;
    }
    impl->initd      = true;
    impl->failed     = false;
    impl->tid        = self_id(md);
    impl->worker     = NULL;
    impl->buffer     = NULL;
    impl->offset     = 0;
    impl->enumerator = 0;
    impl->size       = 0;
    impl->md         = md;
}

DICE_HIDE void
coldtrace_writer_fini(struct coldtrace_writer *ct)
{
    struct writer_impl *impl = (struct writer_impl *)ct;
    if (!impl->initd) {
        return;
    }

    // Nothing was ever traced on this thread
    if (impl->buffer == NULL) {
        return;
    }

    if (coldtrace_writes_disabled()) {
        coldtrace_writer_close(impl->buffer, impl->offset, impl->md);
        mempool_free(impl->buffer);
        impl->buffer = NULL;
        return;
    }

    coldtrace_writer_close(impl->buffer, impl->offset, impl->md);

    struct writer_worker *w = impl->worker;
    if (!impl->failed) {
        coldtrace_pthread_mutex_lock(&w->lock);
        if (!w->failed) {
            w->bufs[w->current].enumerator = impl->enumerator;
            w->bufs[w->current].offset     = impl->offset;
            w->bufs[w->current].state      = SLOT_FULL;
            coldtrace_pthread_cond_signal(&w->filled);
        }
        coldtrace_pthread_mutex_unlock(&w->lock);
    }

    worker_destroy_(w);
    impl->worker = NULL;
    impl->buffer = NULL;
}


__attribute__((weak)) void
coldtrace_writer_close(void *page, size_t size, metadata_t *md)
{
}
