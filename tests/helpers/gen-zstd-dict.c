/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * gen-zstd-dict — small helper that trains a ZSTD dictionary from
 * samples piped on stdin. Used by integration tests to generate
 * dicts on demand without baking static fixtures into the repo.
 *
 * Why a separate binary (not a server-side test command):
 *   The integration tests treat valkey-server as the SUT. Generating
 *   training dictionaries via a server-side test command (e.g.,
 *   `DEBUG COMPRESSION TRAIN-FROM-BYTES`) would let bugs in the
 *   server's training plumbing mask themselves — both the test
 *   helper and the production training path would share code, so a
 *   bug appearing in either would also appear in the test fixture.
 *   This binary lives outside src/ and uses only ZDICT directly, so
 *   any divergence between this and the production training path
 *   surfaces as a test failure rather than being hidden.
 *
 * Usage:
 *   gen-zstd-dict <output_path>
 *
 * Stdin format (binary, repeated until EOF):
 *   [4-byte big-endian length][N sample bytes]
 *   ...
 *
 * Stdout: nothing.
 * Stderr: diagnostics on error.
 * Exit:   0 success, 1 usage, 2 input error, 3 zstd error, 4 IO error.
 */

#include <errno.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "zdict.h"

/* Conservative defaults for testing dicts. The output cap is a
 * compromise: large enough to produce a good dictionary (zstd
 * recommends ~100 KiB for production) but small enough to keep
 * test-time training under a second for the typical sample set. */
#define DEFAULT_DICT_SIZE (16 * 1024)
#define MAX_SAMPLES 100000
#define MAX_TOTAL_BYTES (256 * 1024 * 1024)

static int read_exact(unsigned char *buf, size_t n) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = read(STDIN_FILENO, buf + got, n - got);
        if (r == 0) return -1; /* EOF mid-frame is an error here */
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        got += (size_t)r;
    }
    return 0;
}

/* Returns 0 on success, 1 on clean EOF (caller should stop), -1 on partial read. */
static int read_u32_or_eof(uint32_t *out) {
    unsigned char b[4];
    size_t got = 0;
    while (got < 4) {
        ssize_t r = read(STDIN_FILENO, b + got, 4 - got);
        if (r == 0) {
            if (got == 0) return 1; /* clean EOF */
            return -1;
        }
        if (r < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        got += (size_t)r;
    }
    *out = ((uint32_t)b[0] << 24) | ((uint32_t)b[1] << 16) | ((uint32_t)b[2] << 8) | b[3];
    return 0;
}

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "usage: %s <output_path>\n", argv[0]);
        return 1;
    }
    const char *out_path = argv[1];

    /* Read all samples into a flat buffer + parallel sizes array. */
    size_t buf_cap = 4096, buf_used = 0;
    unsigned char *buffer = malloc(buf_cap);
    if (!buffer) {
        fprintf(stderr, "out of memory\n");
        return 2;
    }

    size_t sizes_cap = 64;
    size_t n_samples = 0;
    size_t *sizes = malloc(sizes_cap * sizeof(size_t));
    if (!sizes) {
        fprintf(stderr, "out of memory\n");
        free(buffer);
        return 2;
    }

    while (1) {
        uint32_t len;
        int rc = read_u32_or_eof(&len);
        if (rc == 1) break; /* clean EOF */
        if (rc < 0) {
            fprintf(stderr, "incomplete length header (after %zu samples)\n", n_samples);
            free(buffer);
            free(sizes);
            return 2;
        }
        if (len == 0) {
            fprintf(stderr, "empty sample is not allowed (sample index %zu)\n", n_samples);
            free(buffer);
            free(sizes);
            return 2;
        }
        if (buf_used + len > MAX_TOTAL_BYTES) {
            fprintf(stderr, "samples exceed safety cap %d MiB\n",
                    MAX_TOTAL_BYTES / (1024 * 1024));
            free(buffer);
            free(sizes);
            return 2;
        }
        if (buf_used + len > buf_cap) {
            while (buf_used + len > buf_cap) buf_cap *= 2;
            unsigned char *nb = realloc(buffer, buf_cap);
            if (!nb) {
                fprintf(stderr, "out of memory growing buffer to %zu\n", buf_cap);
                free(buffer);
                free(sizes);
                return 2;
            }
            buffer = nb;
        }
        if (read_exact(buffer + buf_used, len) < 0) {
            fprintf(stderr, "incomplete sample body (n_samples=%zu, expected %u bytes)\n",
                    n_samples, len);
            free(buffer);
            free(sizes);
            return 2;
        }
        if (n_samples >= sizes_cap) {
            sizes_cap *= 2;
            size_t *ns = realloc(sizes, sizes_cap * sizeof(size_t));
            if (!ns) {
                fprintf(stderr, "out of memory growing sizes\n");
                free(buffer);
                free(sizes);
                return 2;
            }
            sizes = ns;
        }
        sizes[n_samples++] = len;
        buf_used += len;
        if (n_samples >= MAX_SAMPLES) {
            fprintf(stderr, "too many samples (cap %d)\n", MAX_SAMPLES);
            free(buffer);
            free(sizes);
            return 2;
        }
    }

    if (n_samples == 0) {
        fprintf(stderr, "no samples on stdin\n");
        free(buffer);
        free(sizes);
        return 2;
    }

    /* Train. */
    unsigned char *dict_buf = malloc(DEFAULT_DICT_SIZE);
    if (!dict_buf) {
        fprintf(stderr, "out of memory allocating dict output\n");
        free(buffer);
        free(sizes);
        return 2;
    }
    size_t dict_size = ZDICT_trainFromBuffer(dict_buf, DEFAULT_DICT_SIZE,
                                              buffer, sizes, (unsigned)n_samples);
    free(buffer);
    free(sizes);
    if (ZDICT_isError(dict_size)) {
        fprintf(stderr, "ZDICT_trainFromBuffer failed: %s (n_samples=%zu, total_bytes=%zu)\n",
                ZDICT_getErrorName(dict_size), n_samples, buf_used);
        free(dict_buf);
        return 3;
    }

    /* Write to output. */
    FILE *f = fopen(out_path, "wb");
    if (!f) {
        fprintf(stderr, "fopen %s: %s\n", out_path, strerror(errno));
        free(dict_buf);
        return 4;
    }
    size_t written = fwrite(dict_buf, 1, dict_size, f);
    int close_rc = fclose(f);
    free(dict_buf);
    if (written != dict_size) {
        fprintf(stderr, "short write: got %zu of %zu\n", written, dict_size);
        return 4;
    }
    if (close_rc != 0) {
        fprintf(stderr, "fclose %s: %s\n", out_path, strerror(errno));
        return 4;
    }
    return 0;
}
