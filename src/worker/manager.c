/*
 * Copyright (C) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MIT
 *
 * Manager thread: owns the control socket and the channel registry.
 * Starts one consumer per OPEN, reaps consumers when they finish, and on
 * EOF (traced process gone) wakes every consumer still waiting, then
 * returns once all of them have drained their channels.
 */

#include "manager.h"

#include "common.h"
#include "consumer.h"

#include <coldtrace/channel.h>
#include <coldtrace/worker.h>
#include <errno.h>
#include <lz4.h>
#include <poll.h>
#include <semaphore.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

enum recv_result {
    RECV_OK,        // got a well-formed message
    RECV_EOF,       // traced process closed its end
    RECV_ERROR,     // socket error
    RECV_MALFORMED, // wrong size or truncated message
};

// All live channels. Only the manager thread touches the list.
struct registry {
    struct channel *head;
    size_t active;
    int done_fd; // eventfd consumers write to when they exit
};

static enum recv_result
recv_msg_(int socket, struct worker_open_msg *msg, int *fd)
{
    struct iovec iov = {
        .iov_base = msg,
        .iov_len  = sizeof(*msg),
    };
    union {
        char buf[CMSG_SPACE(sizeof(int))];
        struct cmsghdr align;
    } ctrl = {.buf = {0}};

    struct msghdr mh = {
        .msg_iov        = &iov,
        .msg_iovlen     = 1,
        .msg_control    = ctrl.buf,
        .msg_controllen = sizeof(ctrl.buf),
    };

    ssize_t r;
    do {
        r = recvmsg(socket, &mh, MSG_CMSG_CLOEXEC);
    } while (r == -1 && errno == EINTR);

    if (r == 0) {
        return RECV_EOF;
    }
    if (r == -1) {
        log_warn("recvmsg: %s", strerror(errno));
        return RECV_ERROR;
    }

    *fd               = -1;
    struct cmsghdr *c = CMSG_FIRSTHDR(&mh);
    if (c != NULL && c->cmsg_level == SOL_SOCKET &&
        c->cmsg_type == SCM_RIGHTS && c->cmsg_len == CMSG_LEN(sizeof(int))) {
        memcpy(fd, CMSG_DATA(c), sizeof(int));
    }

    if (r != (ssize_t)sizeof(*msg) ||
        (mh.msg_flags & (MSG_TRUNC | MSG_CTRUNC))) {
        log_warn("malformed message (%zd bytes, flags 0x%x), skipping", r,
                 mh.msg_flags);
        if (*fd != -1) {
            close(*fd);
            *fd = -1;
        }
        return RECV_MALFORMED;
    }
    return RECV_OK;
}

// Tell the producer this channel will never be served.
static void
reject_channel_(uint64_t tid, int fd)
{
    uint32_t one = 1;
    off_t off    = (off_t)offsetof(struct channel_hdr, rejected);
    if (pwrite(fd, &one, sizeof(one), off) != (ssize_t)sizeof(one)) {
        log_fatal("manager: cannot reject channel of tid %lu: %s", tid,
                  strerror(errno));
    }
    close(fd);
}

static void
free_channel_(struct channel *ch)
{
    if (ch->region != NULL) {
        munmap(ch->region, ch->region_size);
    }
    free(ch->comp_buf);
    free(ch);
}

// Handle OPEN: map the channel and start its consumer.
static void
open_channel_(struct registry *reg, const struct worker_config *cfg,
              uint64_t tid, int fd)
{
    void *region       = MAP_FAILED;
    size_t size        = 0;
    struct channel *ch = NULL;

    if (fd == -1) {
        log_warn("manager: OPEN tid=%lu without an fd", tid);
        return;
    }

    for (struct channel *it = reg->head; it != NULL; it = it->next) {
        if (it->tid == tid) {
            log_warn("manager: second channel for tid %lu", tid);
            break;
        }
    }

    struct stat st;
    if (fstat(fd, &st) == -1) {
        log_warn("manager: fstat(tid %lu): %s", tid, strerror(errno));
        goto fail;
    }
    size            = (size_t)st.st_size;
    size_t expected = channel_region_size(cfg->trace_size);
    if (size != expected) {
        log_warn("manager: OPEN tid=%lu region is %zu bytes, expected %zu", tid,
                 size, expected);
        goto fail;
    }

    region = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (region == MAP_FAILED) {
        log_warn("manager: mmap(tid %lu): %s", tid, strerror(errno));
        goto fail;
    }

    ch = calloc(1, sizeof(*ch));
    if (ch == NULL) {
        log_warn("manager: out of memory for tid %lu", tid);
        goto fail;
    }
    ch->tid         = tid;
    ch->region      = region;
    ch->region_size = size;
    ch->cfg         = cfg;
    ch->done_fd     = reg->done_fd;
    atomic_init(&ch->stop, false);
    atomic_init(&ch->exited, false);

    ch->comp_buf =
        malloc(FRAG_HDR_SIZE + (size_t)LZ4_compressBound((int)cfg->trace_size));
    if (ch->comp_buf == NULL) {
        log_warn("manager: out of memory for tid %lu", tid);
        goto fail;
    }

    int err = pthread_create(&ch->thread, NULL, consumer_thread_, ch);
    if (err != 0) {
        log_warn("manager: cannot start consumer for tid %lu: %s", tid,
                 strerror(err));
        goto fail;
    }

    close(fd);

    ch->next  = reg->head;
    reg->head = ch;
    reg->active++;
    log_debug("manager: tid %lu: consumer started (%zu active)", tid,
              reg->active);
    return;

fail:
    if (ch != NULL) {
        free_channel_(ch);
    } else if (region != MAP_FAILED) {
        munmap(region, size);
    }
    reject_channel_(tid, fd);
}

// Join and free every consumer that has finished
static void
reap_(struct registry *reg)
{
    uint64_t count;
    if (read(reg->done_fd, &count, sizeof(count)) == -1 && errno != EAGAIN) {
        log_warn("manager: read eventfd: %s", strerror(errno));
    }

    struct channel **link = &reg->head;
    while (*link != NULL) {
        struct channel *ch = *link;
        if (!atomic_load(&ch->exited)) {
            link = &ch->next;
            continue;
        }
        pthread_join(ch->thread, NULL);
        *link = ch->next;
        reg->active--;
        free_channel_(ch);
    }
}

// Wake every consumer once. Each one first drains the slots that were really
// handed over, then finds an empty slot and exits.
static void
wake_all_(struct registry *reg)
{
    for (struct channel *ch = reg->head; ch != NULL; ch = ch->next) {
        atomic_store(&ch->stop, true);
        struct channel_hdr *hdr = ch->region;
        sem_post(&hdr->filled);
    }
}

void *
manager_thread_(void *arg)
{
    const struct worker_config *cfg = arg;
    struct registry reg             = {.head = NULL, .active = 0};

    reg.done_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (reg.done_fd == -1) {
        log_warn("manager: eventfd: %s", strerror(errno));
        return NULL;
    }

    struct pollfd fds[2] = {
        {.fd = WORKER_CTRL_FD, .events = POLLIN},
        {.fd = reg.done_fd, .events = POLLIN},
    };
    bool producer_gone = false;

    // Run until the producer is gone and every channel is drained
    while (!producer_gone || reg.active > 0) {
        if (poll(fds, 2, -1) == -1) {
            if (errno == EINTR) {
                continue;
            }
            log_warn("manager: poll: %s", strerror(errno));
            break;
        }

        if (fds[1].revents & POLLIN) {
            reap_(&reg);
        }

        if (!producer_gone && (fds[0].revents & (POLLIN | POLLHUP | POLLERR))) {
            struct worker_open_msg msg;
            int fd               = -1;
            enum recv_result res = recv_msg_(WORKER_CTRL_FD, &msg, &fd);

            if (res == RECV_EOF || res == RECV_ERROR) {
                log_debug(
                    "manager: traced process is gone, draining %zu "
                    "channel(s)",
                    reg.active);
                producer_gone = true;
                fds[0].fd     = -1;
                wake_all_(&reg);
            } else if (res == RECV_OK) {
                if (msg.type == WORKER_MSG_OPEN) {
                    open_channel_(&reg, cfg, msg.tid, fd);
                } else {
                    log_warn("manager: unknown message type %u", msg.type);
                    if (fd != -1) {
                        reject_channel_(msg.tid, fd);
                    }
                }
            }
        }
    }

    close(reg.done_fd);
    return NULL;
}
