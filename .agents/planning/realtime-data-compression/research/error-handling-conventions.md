# Research — Error-handling conventions in Valkey's `src/`

_Scope: derive Valkey's de-facto error-handling convention from the
existing source so the compression feature follows the same patterns
consistently. There is no documented convention in `CONTRIBUTING.md`
or `DEVELOPMENT_GUIDE.md`; what follows is reverse-engineered from
the code._

## 1. Why this document exists

Two places in `compression_header.c` raised the question: when do we
`return NULL`, when do we `serverAssert`, when do we log? Different
choices give materially different production behavior:

- `serverAssert` aborts the process — appropriate for "this can never
  happen unless we have a bug or in-memory corruption."
- `return NULL` is silent — appropriate for "this can legitimately
  happen on bad input; let the caller decide what to do."
- Logging via `serverLog(LL_WARNING, ...)` is operator-visible —
  appropriate for "the operator should know about this, but the
  server stays up."

The convention that follows is what the Valkey codebase already
practices, distilled from grepping `src/`. We adopt it for the
compression feature so our error paths match the rest of the server.

## 2. The convention (decision table)

| Source of the error | Pattern | Logging? | Examples in `src/` |
|---|---|---|---|
| **Programmer error / contract violation / unreachable code** | `serverAssert(invariant)` or `serverPanic("description")` | Aborts with stack trace; no separate log call needed | `serverAssert(o->refcount == 1)`, `serverAssert(o->type == OBJ_STRING)`, `default: serverPanic("Unknown set encoding type")` |
| **External untrusted input** detected at this layer (RDB load, RESP parse, network, module input) | Return `NULL` / `-1` from the helper; **caller** invokes a corruption-reporting macro (`rdbReportCorruptRDB`, `rdbReportReadError`) or sets a client error | Caller logs via the corruption-reporting path | `rdbLoadLzfStringObject` returns NULL on bad LZF; loader emits `rdbReportCorruptRDB("Invalid LZF compressed string")` |
| **Resource failure** (allocator OOM, large allocation refused) | `try*`-prefixed function returns NULL; **callee** logs `serverLog(LL_WARNING, "...failed allocating %llu bytes", ...)` | Callee logs at `LL_WARNING` (or `LL_VERBOSE` in restore context) | `tryCreateRawStringObject`, `rdbLoadLzfStringObject`'s `ztrymalloc`/`sdstrynewlen` failure paths |
| **Recoverable runtime condition** (queue full, no active dict yet, drift detected) | Return code + counter increment; optional log at `LL_NOTICE` or `LL_WARNING` | Counter is the primary signal; log only on first occurrence per minute, or on threshold | (no representative example in current `src/`; new pattern for compression's queue back-pressure work in S2.4) |
| **Trusted internal input** (factory or helper called from a single internal path that controls the inputs) | **No validation** — trust the caller | None | `createObject(int type, void *val)` doesn't check `val != NULL`; `freeStringObject(robj *o)` doesn't check `o != NULL`; `lookupKey*` doesn't validate the db pointer |

Header-decode helpers, parsers, factories that take a buffer follow
the rule: validate the **bytes inside** the buffer (corruption-prone),
do not second-guess the buffer's existence (programmer error).

## 3. Evidence — usage counts in `src/`

Grep across `src/*.c` and `src/*.h` (run on `unstable` at commit
`21773fbe4`):

| Pattern | Sites |
|---|---|
| `serverAssert(...)` | 653 |
| `serverPanic(...)` | 231 |
| `return NULL` | 493 |
| `return -1` | 256 |

Both error patterns are common — they are NOT mutually exclusive.
They serve different categories. The decision is which category an
error falls into, not which mechanism is "preferred."

## 4. Concrete examples

### 4.1 Factory that doesn't validate (trusts the caller)

`src/object.c`:

```c
robj *createObject(int type, void *val) {
    return createUnembeddedObjectWithKeyAndExpire(type, val, NULL, EXPIRY_NONE);
}

void freeStringObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_RAW) {
        sdsfree(objectGetVal(o));
    }
}
```

No `o != NULL` check. No `val != NULL` check. The caller is internal
and the contract is "you pass a valid robj/val." This is the dominant
factory-style pattern in `src/`.

### 4.2 Loader that distinguishes corruption from bug

`src/rdb.c`, `rdbLoadLzfStringObject`:

```c
if ((c = ztrymalloc(clen)) == NULL) {
    serverLog(isRestoreContext() ? LL_VERBOSE : LL_WARNING,
              "rdbLoadLzfStringObject failed allocating %llu bytes",
              (unsigned long long)clen);
    goto err;
}
...
if (lzf_decompress(c, clen, val, len) != len) {
    rdbReportCorruptRDB("Invalid LZF compressed string");
    goto err;
}
```

Two different kinds of failure handled differently:
- Allocator failure → callee logs (`LL_WARNING` or `LL_VERBOSE`
  depending on context), returns NULL via `goto err`.
- Decompression returning the wrong size on bytes that came from
  disk → corruption; calls the corruption-reporting macro, returns
  NULL via `goto err`.

The function never asserts. Both failures are categorized as
external-input issues, not bugs.

### 4.3 Switch over a fixed enum: assert on `default`

`src/object.c`:

```c
void freeListObject(robj *o) {
    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklistRelease(objectGetVal(o));
    } else if (o->encoding == OBJ_ENCODING_LISTPACK) {
        lpFree(objectGetVal(o));
    } else {
        serverPanic("Unknown list encoding type");
    }
}

void freeSetObject(robj *o) {
    switch (o->encoding) {
    case OBJ_ENCODING_HASHTABLE: hashtableRelease((hashtable *)objectGetVal(o)); break;
    case OBJ_ENCODING_INTSET:
    case OBJ_ENCODING_LISTPACK: zfree(objectGetVal(o)); break;
    default: serverPanic("Unknown set encoding type");
    }
}
```

The encoding values are an enum-like fixed set; reaching the default
is a bug. `serverPanic` with a descriptive message is the convention.

### 4.4 Internal invariants asserted

Sample of `serverAssert` call sites from `src/object.c`, `src/db.c`,
`src/t_string.c`, `src/server.c`:

```c
serverAssert(o->refcount == 1);
serverAssert(o->type != OBJ_STRING);
serverAssert(o->type == OBJ_STRING);
serverAssert(ql->len != 0);
serverAssert(hashtableSize(ht) != 0);
serverAssert(zslGetLength(zsl) != 0);
serverAssert(lfu_freq <= UINT8_MAX);
serverAssert(val->refcount != OBJ_SHARED_REFCOUNT);
serverAssert(!(flags & LOOKUP_WRITE));
```

Every one is a contract assertion: the caller (or the data structure
state) is expected to satisfy the invariant; reaching the assert
means a bug elsewhere.

## 5. Application to the compression feature

The convention applies to our hot path as follows:

| Compression site | Category | Choice |
|---|---|---|
| `compressionHeaderDecode(src, out)` — `out == NULL` | Programmer error (caller bug) | `serverAssert(out != NULL)` |
| `compressionHeaderDecode` — unknown `alg_magic` | External corruption (RDB on disk; future module-imported buffers) | `return -1` silently; caller decides |
| `createCompressedObject(type, buffer, len)` — `buffer == NULL` | Programmer error (no caller passes NULL) | `serverAssert(buffer != NULL)` |
| `createCompressedObject` — `len < HEADER_SIZE` | Programmer error (caller allocates `HEADER + compressed_len`) | `serverAssert(len >= HEADER_SIZE)` |
| `createCompressedObject` — bad `alg_magic` | External corruption (defensible from RDB load) | `return NULL`; caller (RDB loader) wraps with `rdbReportCorruptRDB` |
| `createCompressedObject` — `len != HEADER + h.compressed_len` | Programmer error (caller wrote the header to match its own allocation) | `serverAssert(len == HEADER + h.compressed_len)` |
| `freeCompressedObject(o)` — header decode fails | Programmer error / memory corruption (header was validated at install) | `serverAssert(rc == 0)` |
| Compression worker `ZSTD_compress_usingCDict` returns error | Recoverable runtime condition | Set `job->err`; main thread increments `compression_errors_total`; `serverLog(LL_WARNING, ...)` rate-limited (R6.1 in detailed-design.md) |
| Decompression `ZSTD_decompress_usingDDict` failure on read | External corruption (in-memory data damaged) | Log `LL_WARNING`; increment `compression_errors_total`; return `-ERR compressed value corrupt` to client (R6.2) |
| Training `ZDICT_trainFromBuffer` failure | Recoverable runtime condition | `serverLog(LL_WARNING, ...)`; abort training; do not promote (R6.3) |
| Dict cap reached | Recoverable runtime condition | `serverLog(LL_WARNING, ...)`; set `compression_dict_cap_reached = 1`; refuse promotion (R6.4) |
| RDB compressed value with missing dict AUX | External corruption | `rdbReportCorruptRDB(...)`; reject RDB (R6.5) |

The detailed-design.md §6 already enumerates compression-specific
error categories; this document complements it with the underlying
convention so future contributors don't make the case-by-case
decisions our PR #8 review surfaced.

## 6. Things this convention does NOT prescribe

- **Assertion expense.** `serverAssert` is always-on (not behind
  `NDEBUG` in Valkey). Both pre-condition asserts and invariant
  asserts compile into release builds. This is the existing project
  policy.
- **Recovery from `serverAssert`.** None. The process aborts with a
  stack trace. If a path could plausibly recover, it's not an
  assertion site — re-categorize as recoverable runtime condition
  and use a return code.
- **What to log on the recoverable path.** Volume control is the
  caller's job. Compression's worker-error path (R6.1) is rate-
  limited to first-error-per-minute. RDB corruption reports go
  through `rdbReportCorruptRDB` which is rate-limited internally.

## References

- `src/object.c` — factory and free functions, `serverPanic` on
  unknown-encoding default branches.
- `src/rdb.c` — `rdbReportCorruptRDB`, `rdbReportReadError`,
  `rdbLoadLzfStringObject`, `rdbGenericLoadStringObject`.
- `src/server.c` — error-reporting macros and helpers.
- `DEVELOPMENT_GUIDE.md` — does not currently cover error handling;
  this document fills that gap for the compression feature's reviewers
  and contributors.
