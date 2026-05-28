/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * A thread-safe queue, protected by a mutex.
 *
 * Supports:
 *   - Adding an item to the end of the mutexQueue
 *   - Adding a list of items (fifo) to the end of the mutexQueue
 *   - Insertion of a priority item at the beginning of the mutexQueue (but after existing priority items)
 *   - Removing an item from the beginning of the mutexQueue
 *   - Removing ALL items as (as new fifo) from the mutexQueue
 *   - Synchronous waiting on the mutexQueue for new items
 *
 * Priority Use Case:
 * The priority feature provides a 2-level priority system (priority vs normal) without
 * requiring separate fifos with their own mutexes and condition variables. This simplifies
 * synchronization when you need to occasionally process urgent items ahead of routine work.
 *
 * Example: In a background worker thread processing file operations, you might want to
 * prioritize critical shutdown tasks or urgent fsync operations over routine lazy-free jobs.
 * Priority items are processed in FIFO order, followed by normal items in FIFO order.
 *
 * Implementation: Uses two internal FIFOs (priority_fifo and normal_fifo). Items are always
 * popped from priority_fifo first.
 *
 * The caller is responsible for memory management for items in the mutexQueue.
 */

#ifndef __MUTEXQUEUE_H
#define __MUTEXQUEUE_H

#include <stdbool.h>
#include "fifo.h"

/* The mutexQueue is an opaque structure.  */
typedef struct mutexQueue mutexQueue;

/* Create an empty mutexQueue. */
mutexQueue *mutexQueueCreate(void);

/* Release an empty mutexQueue. */
void mutexQueueRelease(mutexQueue *theQueue);

/* Number of items in the mutexQueue. */
unsigned long mutexQueueLength(mutexQueue *theQueue);

/* Insert a priority item at the beginning of the mutexQueue (but after existing priority items). */
void mutexQueuePushPriority(mutexQueue *theQueue, void *value);

/* Insert an item at the end of the mutexQueue. */
void mutexQueueAdd(mutexQueue *theQueue, void *value);

/* Insert multiple items (from a fifo) to the end of the mutexQueue. */
void mutexQueueAddMultiple(mutexQueue *theQueue, fifo *valueFifo);

/* Wake every thread parked in mutexQueuePopWakable(blocking=true)
 * without enqueuing anything. Each woken consumer re-checks the queue;
 * if still empty, mutexQueuePopWakable returns NULL.
 *
 * Use case: QSBR-style grace barriers. The caller wants idle consumers
 * to exit their cond_wait so they can advance per-thread state (e.g.
 * a quiescent generation counter) without consuming a real job.
 *
 * Implementation: pthread_cond_broadcast under the mutex. Any consumer
 * parked in pthread_cond_wait exits; consumers already in the critical
 * section are unaffected.
 *
 * The standard mutexQueuePop / mutexQueuePopAll APIs are NOT affected
 * by this — they re-check the queue under the mutex and re-park on
 * spurious wake, so they still never return NULL when called with
 * blocking=true. Only callers that opt in via the *Wakable* variant
 * see the NULL return. This keeps existing callers (bio, etc.)
 * unchanged when wake-all is added for new use cases.
 *
 * NOT a shutdown signal on its own — callers that want shutdown
 * semantics combine wake-all with their own atomic flag, or use the
 * established sentinel-in-queue pattern (push N sentinels into the
 * mutexQueue so blocked consumers pop them through the standard path).
 */
void mutexQueueWakeAll(mutexQueue *theQueue);

/* Retrieves the first item off the mutexQueue (or NULL if mutexQueue is empty).
 *
 * If 'blocking' is true and the queue is empty, parks the caller on
 * the queue's condition variable until an item is added. Returns the
 * item. NEVER returns NULL when blocking=true — spurious wakes
 * (including those caused by mutexQueueWakeAll on the same queue) are
 * absorbed internally by re-parking. Callers that want to observe
 * wake-all events must use mutexQueuePopWakable instead. */
void *mutexQueuePop(mutexQueue *theQueue, bool blocking);

/* Retrieves all items from the mutexQueue as a fifo (or NULL if the mutexQueue is empty).
 * Same contract as mutexQueuePop above: never returns NULL when
 * blocking=true; spurious wakes are absorbed by re-parking. */
fifo *mutexQueuePopAll(mutexQueue *theQueue, bool blocking);

/* Wake-all-aware variant of mutexQueuePop.
 *
 * Same as mutexQueuePop, except the blocking wait does NOT re-park on
 * a spurious wake — it returns NULL once. This is the variant callers
 * use when they want to be notified by mutexQueueWakeAll (e.g. to run
 * a per-loop-iteration housekeeping step like advancing a QSBR
 * generation counter) without an item being present.
 *
 * Returns NULL if (a) queue was empty and blocking=false, or (b)
 * blocking=true and the cond var fired without an item (wake-all or
 * spurious POSIX wake-up; caller must distinguish via its own state).
 * Returns a non-NULL value pointer when an item was successfully
 * popped under the mutex. */
void *mutexQueuePopWakable(mutexQueue *theQueue, bool blocking);

#endif
