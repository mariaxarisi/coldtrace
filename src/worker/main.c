/*
 * Copyright (C) 2026 Huawei Technologies Co., Ltd.
 * SPDX-License-Identifier: MIT
 */

#include "common.h"
#include "manager.h"

#include <coldtrace/channel.h>
#include <coldtrace/worker.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>

#define DECIMAL_BASE 10

int
main(int argc, char **argv)
{
    if (argc != WORKER_ARGC) {
        log_warn("expected %d args, got %d", WORKER_ARGC - 1, argc - 1);
        return 2;
    }

    struct worker_config cfg = {
        .pattern    = argv[WORKER_ARG_PATTERN],
        .trace_size = strtoull(argv[WORKER_ARG_SIZE], NULL, DECIMAL_BASE),
    };

    if (!channel_valid_trace_size(cfg.trace_size)) {
        log_warn("invalid trace size %lu", cfg.trace_size);
        return 2;
    }

    pthread_t manager;
    int err = pthread_create(&manager, NULL, manager_thread_, &cfg);
    if (err != 0) {
        log_warn("cannot start manager: %s", strerror(err));
        return 1;
    }

    pthread_join(manager, NULL);

    return 0;
}
