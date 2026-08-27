# Bugfix Requirements Document

## Introduction

Sudden network slowdowns on target Soroban RPC networks cause the internal asynchronous queue ingestion pipelines to grow unboundedly. Because `src/queue/pipeline.py` uses only a semaphore-based concurrency ceiling with no buffer capacity enforcement, and `src/queue/` has no drop-tail policy whatsoever, any sustained slowdown causes the in-flight task set and associated data to swell without limit, eventually exhausting process memory and crashing the service.

The fix refactors the queue ingestion pipeline inside `src/queue/backpressure.py` to introduce a structured drop-tail threshold policy: once the buffer queue reaches 90% of its configured capacity, non-essential historical tracing metrics packets are automatically dropped, preserving headroom for the primary live price channels under load.

## Bug Analysis

### Current Behavior (Defect)

**User Story:** As a system operator, I want to identify exactly how the ingestion pipeline fails under load, so that I can verify the defect is reproducible and the fix is complete.

#### Acceptance Criteria

1.1 WHEN the Soroban RPC network experiences a slowdown and inbound messages arrive faster than they are consumed THEN the system allows the ingestion queue item count to increase without any enforced upper bound, with no maximum capacity limit applied

1.2 WHEN the ingestion queue item count is at or above 90% of its configured maximum capacity and a METRIC priority packet arrives THEN the system accepts and enqueues the packet, increments queue length, and does not increment the dropped-packets counter

1.3 IF the ingestion queue item count equals its configured maximum capacity AND a METRIC priority packet arrives THEN the system blocks the calling coroutine until queue space becomes available, with no timeout or rejection applied

1.4 IF the ingestion queue item count equals its configured maximum capacity AND a METRIC priority packet arrives AND no consumer drains the queue within the observation window THEN the system terminates the process due to resource exhaustion without returning control to the caller

### Expected Behavior (Correct)

**User Story:** As a system operator, I want the ingestion pipeline to enforce a bounded queue with priority-based drop-tail shedding, so that live price channels remain clear and the service does not crash during Soroban RPC network slowdowns.

#### Acceptance Criteria

2.1 WHERE the ingestion pipeline is running, WHEN the queue item count reaches its configured maximum capacity THEN the system SHALL enforce the limit such that the queue item count never exceeds `max_capacity` (configurable integer in range [100, 100 000], default 1 000)

2.2 WHEN the ingestion queue saturation (queue item count / max_capacity) is greater than or equal to 0.90 AND a METRIC priority packet arrives THEN the system SHALL synchronously discard the packet, increment `dropped_packets` by exactly 1, and return to the caller without enqueuing the packet and without suspending the caller

2.3 WHEN the ingestion queue saturation is greater than or equal to 0.90 AND a LIVE_PRICE priority packet arrives THEN the system SHALL enqueue the packet and return without dropping it

2.4 IF the ingestion queue item count equals `max_capacity` AND a LIVE_PRICE priority packet arrives THEN the system SHALL suspend the caller until a slot is available and then enqueue the packet, ensuring no live price packet is lost due to a full queue

### Unchanged Behavior (Regression Prevention)

**User Story:** As a system operator, I want to ensure the drop-tail policy introduces no regressions for normal-load scenarios, so that packets are not incorrectly dropped when the queue is healthy.

#### Acceptance Criteria

3.1 WHILE the ingestion queue saturation is strictly less than 0.90, WHEN any packet arrives (METRIC or LIVE_PRICE priority) THEN the system SHALL CONTINUE TO enqueue the packet and the `dropped_packets` counter SHALL remain unchanged

3.2 WHILE the ingestion queue saturation is strictly less than 0.90, WHEN a METRIC priority packet arrives THEN the system SHALL CONTINUE TO enqueue the packet without incrementing `dropped_packets`

3.3 WHILE the ingestion queue saturation is strictly less than 0.90, WHEN a LIVE_PRICE priority packet arrives THEN the system SHALL CONTINUE TO enqueue the packet and return to the caller in O(1) time without introducing any artificial delay

3.4 WHEN a LIVE_PRICE priority packet is dequeued THEN the system SHALL CONTINUE TO return packets in FIFO order within the LIVE_PRICE channel, such that the n-th enqueued LIVE_PRICE packet is the n-th dequeued LIVE_PRICE packet
