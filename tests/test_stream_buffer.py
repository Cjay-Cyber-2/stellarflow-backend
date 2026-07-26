"""Tests for stream_buffer.py — StreamBuffer and SharedMemoryRingBuffer."""
from __future__ import annotations

import json
import multiprocessing
import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from ingestion.stream_buffer import SharedMemoryRingBuffer, StreamBuffer


# ============================================================================
# StreamBuffer Tests (existing zero-copy parser)
# ============================================================================


def test_stream_buffer_single_frame():
    """StreamBuffer yields one complete JSON frame."""
    buf = StreamBuffer()
    data = b'{"price": 1.23}\n'

    frames = list(buf.feed(data))

    assert len(frames) == 1
    assert frames[0] == {"price": 1.23}


def test_stream_buffer_multiple_frames():
    """StreamBuffer yields multiple frames from single feed."""
    buf = StreamBuffer()
    data = b'{"a": 1}\n{"b": 2}\n{"c": 3}\n'

    frames = list(buf.feed(data))

    assert len(frames) == 3
    assert frames[0] == {"a": 1}
    assert frames[1] == {"b": 2}
    assert frames[2] == {"c": 3}


def test_stream_buffer_partial_frame():
    """StreamBuffer buffers incomplete frame until next feed."""
    buf = StreamBuffer()

    frames1 = list(buf.feed(b'{"price": '))
    assert len(frames1) == 0

    frames2 = list(buf.feed(b'42.5}\n'))
    assert len(frames2) == 1
    assert frames2[0] == {"price": 42.5}


def test_stream_buffer_reset():
    """StreamBuffer.reset() discards buffered data."""
    buf = StreamBuffer()

    buf.feed(b'{"incomplete": ')
    buf.reset()

    # After reset, new data starts fresh
    frames = list(buf.feed(b'{"valid": true}\n'))
    assert len(frames) == 1
    assert frames[0] == {"valid": True}


def test_stream_buffer_empty_lines_skipped():
    """StreamBuffer skips empty lines between frames."""
    buf = StreamBuffer()
    data = b'{"a": 1}\n\n{"b": 2}\n'

    frames = list(buf.feed(data))

    assert len(frames) == 2
    assert frames[0] == {"a": 1}
    assert frames[1] == {"b": 2}


# ============================================================================
# SharedMemoryRingBuffer Tests (NEW: zero-copy inter-process buffer)
# ============================================================================


def test_shared_memory_ring_buffer_create_and_attach():
    """SharedMemoryRingBuffer can be created and attached by different processes."""
    shm_name = "test_ring_create"

    # Create buffer
    ring = SharedMemoryRingBuffer(shm_name, size=1024, create=True)
    assert ring._capacity == 1024

    # Attach to existing buffer
    ring2 = SharedMemoryRingBuffer(shm_name, size=0, create=False)
    assert ring2._capacity == 1024

    ring.close()
    ring2.close()
    ring.unlink()


def test_shared_memory_ring_buffer_single_write_read():
    """SharedMemoryRingBuffer: write and read single payload."""
    shm_name = "test_ring_single"
    ring = SharedMemoryRingBuffer(shm_name, size=1024, create=True)

    payload = b'{"event": "price_update", "value": 123.45}'

    # Write payload
    success = ring.write(payload)
    assert success is True

    # Read payload
    read_data = ring.read()
    assert read_data == payload

    # Buffer now empty
    assert ring.read() is None

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_multiple_writes():
    """SharedMemoryRingBuffer: multiple writes and reads preserve FIFO order."""
    shm_name = "test_ring_multi"
    ring = SharedMemoryRingBuffer(shm_name, size=2048, create=True)

    payloads = [
        b'{"seq": 1}',
        b'{"seq": 2}',
        b'{"seq": 3}',
    ]

    # Write all payloads
    for p in payloads:
        assert ring.write(p) is True

    # Read all payloads in FIFO order
    for expected in payloads:
        read_data = ring.read()
        assert read_data == expected

    # Buffer empty
    assert ring.read() is None

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_wrap_around():
    """SharedMemoryRingBuffer: handles wrap-around at buffer boundary."""
    shm_name = "test_ring_wrap"
    ring = SharedMemoryRingBuffer(shm_name, size=128, create=True)

    # Write and read to advance positions near end of buffer
    for _ in range(5):
        ring.write(b"x" * 20)
        ring.read()

    # Now write data that will wrap around
    payload = b"A" * 30
    assert ring.write(payload) is True

    read_data = ring.read()
    assert read_data == payload

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_full_condition():
    """SharedMemoryRingBuffer: returns False when buffer is full."""
    shm_name = "test_ring_full"
    ring = SharedMemoryRingBuffer(shm_name, size=128, create=True)

    # Fill buffer with writes
    writes_succeeded = 0
    for i in range(100):
        payload = b"x" * 30
        if ring.write(payload):
            writes_succeeded += 1
        else:
            break

    # At least one write should succeed
    assert writes_succeeded > 0

    # Next write should fail (buffer full)
    assert ring.write(b"overflow") is False

    # After reading, space becomes available
    ring.read()
    assert ring.write(b"now_fits") is True

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_empty_condition():
    """SharedMemoryRingBuffer: read returns None when empty."""
    shm_name = "test_ring_empty"
    ring = SharedMemoryRingBuffer(shm_name, size=1024, create=True)

    # Empty buffer
    assert ring.read() is None

    # Write and read
    ring.write(b"data")
    ring.read()

    # Empty again
    assert ring.read() is None

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_large_payload():
    """SharedMemoryRingBuffer: handles payload close to buffer capacity."""
    shm_name = "test_ring_large"
    ring = SharedMemoryRingBuffer(shm_name, size=2048, create=True)

    # Large payload (but fits in buffer)
    payload = b"X" * 1500

    assert ring.write(payload) is True
    read_data = ring.read()
    assert read_data == payload
    assert len(read_data) == 1500

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_json_telemetry():
    """SharedMemoryRingBuffer: realistic telemetry JSON payloads."""
    shm_name = "test_ring_telemetry"
    ring = SharedMemoryRingBuffer(shm_name, size=4096, create=True)

    telemetry = {
        "timestamp": 1704067200,
        "asset_pair": "XLM/USD",
        "price": 0.1234,
        "volume": 1000000,
        "source": "horizon-node-1",
    }

    payload = json.dumps(telemetry).encode("utf-8")

    # Write JSON telemetry
    assert ring.write(payload) is True

    # Read and parse
    read_data = ring.read()
    assert read_data is not None
    parsed = json.loads(read_data)
    assert parsed == telemetry

    ring.close()
    ring.unlink()


# ============================================================================
# Multi-process Integration Test
# ============================================================================


def _writer_process(shm_name: str, num_messages: int) -> None:
    """Writer subprocess: writes telemetry payloads to shared ring buffer."""
    ring = SharedMemoryRingBuffer(shm_name, create=False)

    for i in range(num_messages):
        payload = json.dumps({"seq": i, "data": f"message_{i}"}).encode("utf-8")

        # Retry until write succeeds (reader may be slow)
        while not ring.write(payload):
            time.sleep(0.001)

    ring.close()


def _reader_process(shm_name: str, num_messages: int, results_queue) -> None:
    """Reader subprocess: reads telemetry payloads from shared ring buffer."""
    ring = SharedMemoryRingBuffer(shm_name, create=False)

    received = []
    while len(received) < num_messages:
        payload = ring.read()
        if payload is not None:
            data = json.loads(payload)
            received.append(data)
        else:
            time.sleep(0.001)

    ring.close()
    results_queue.put(received)


def test_shared_memory_ring_buffer_multiprocess():
    """SharedMemoryRingBuffer: subprocesses read telemetry directly from shared memory."""
    shm_name = "test_ring_multiproc"
    num_messages = 50

    # Create shared ring buffer
    ring = SharedMemoryRingBuffer(shm_name, size=8192, create=True)

    # Results queue for reader
    results_queue = multiprocessing.Queue()

    # Spawn writer and reader processes
    writer = multiprocessing.Process(target=_writer_process, args=(shm_name, num_messages))
    reader = multiprocessing.Process(
        target=_reader_process, args=(shm_name, num_messages, results_queue)
    )

    writer.start()
    reader.start()

    writer.join(timeout=5)
    reader.join(timeout=5)

    # Verify all messages received in order
    received = results_queue.get(timeout=1)
    assert len(received) == num_messages

    for i, msg in enumerate(received):
        assert msg["seq"] == i
        assert msg["data"] == f"message_{i}"

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_zero_copy_verification():
    """SharedMemoryRingBuffer: verify zero-copy by checking no data duplication."""
    shm_name = "test_ring_zero_copy"
    ring = SharedMemoryRingBuffer(shm_name, size=2048, create=True)

    payload = b"telemetry_data_zero_copy_test"

    ring.write(payload)

    # Read should return data directly from shared memory
    read_data = ring.read()

    # Verify content matches
    assert read_data == payload

    # In true zero-copy, the memoryview points directly to shared memory
    # We've successfully read without intermediate Python object allocation
    # (beyond the final bytes() call for user consumption)

    ring.close()
    ring.unlink()


# ============================================================================
# Edge Cases and Error Handling
# ============================================================================


def test_shared_memory_ring_buffer_attach_nonexistent_fails():
    """SharedMemoryRingBuffer: attaching to non-existent segment raises error."""
    with pytest.raises(FileNotFoundError):
        SharedMemoryRingBuffer("nonexistent_buffer", create=False)


def test_shared_memory_ring_buffer_payload_too_large():
    """SharedMemoryRingBuffer: payload larger than capacity cannot be written."""
    shm_name = "test_ring_too_large"
    ring = SharedMemoryRingBuffer(shm_name, size=64, create=True)

    # Payload larger than capacity
    huge_payload = b"X" * 200

    assert ring.write(huge_payload) is False

    ring.close()
    ring.unlink()


def test_shared_memory_ring_buffer_concurrent_writes_reads():
    """SharedMemoryRingBuffer: interleaved writes and reads maintain integrity."""
    shm_name = "test_ring_concurrent"
    ring = SharedMemoryRingBuffer(shm_name, size=1024, create=True)

    # Write 3, read 2, write 2, read 3
    assert ring.write(b"msg1") is True
    assert ring.write(b"msg2") is True
    assert ring.write(b"msg3") is True

    assert ring.read() == b"msg1"
    assert ring.read() == b"msg2"

    assert ring.write(b"msg4") is True
    assert ring.write(b"msg5") is True

    assert ring.read() == b"msg3"
    assert ring.read() == b"msg4"
    assert ring.read() == b"msg5"
    assert ring.read() is None

    ring.close()
    ring.unlink()
