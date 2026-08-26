#!/usr/bin/env python3
"""Verification script for SharedMemoryRingBuffer implementation.

Run this script to verify that the shared memory ring buffer works correctly
without needing pytest installed.
"""
import json
import multiprocessing
import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))

from ingestion.stream_buffer import SharedMemoryRingBuffer


def test_basic_write_read():
    """Test basic write and read operations."""
    print("Test 1: Basic write/read...", end=" ")
    shm_name = "verify_basic"
    ring = SharedMemoryRingBuffer(shm_name, size=1024, create=True)

    payload = b'{"event": "price_update", "value": 123.45}'
    assert ring.write(payload) is True, "Write failed"

    read_data = ring.read()
    assert read_data == payload, f"Data mismatch: {read_data} != {payload}"

    assert ring.read() is None, "Buffer should be empty"

    ring.close()
    ring.unlink()
    print("✓ PASS")


def test_multiple_messages():
    """Test multiple writes in FIFO order."""
    print("Test 2: Multiple messages FIFO...", end=" ")
    shm_name = "verify_multi"
    ring = SharedMemoryRingBuffer(shm_name, size=2048, create=True)

    messages = [b'{"seq": 1}', b'{"seq": 2}', b'{"seq": 3}']

    for msg in messages:
        assert ring.write(msg) is True, f"Write failed for {msg}"

    for expected in messages:
        read_data = ring.read()
        assert read_data == expected, f"Data mismatch: {read_data} != {expected}"

    assert ring.read() is None, "Buffer should be empty"

    ring.close()
    ring.unlink()
    print("✓ PASS")


def test_buffer_full():
    """Test buffer full condition."""
    print("Test 3: Buffer full handling...", end=" ")
    shm_name = "verify_full"
    ring = SharedMemoryRingBuffer(shm_name, size=128, create=True)

    # Fill buffer
    writes_succeeded = 0
    for i in range(100):
        if ring.write(b"x" * 30):
            writes_succeeded += 1
        else:
            break

    assert writes_succeeded > 0, "Should have at least one successful write"
    assert ring.write(b"overflow") is False, "Should fail when full"

    # Read one to make space
    ring.read()
    assert ring.write(b"now_fits") is True, "Should succeed after freeing space"

    ring.close()
    ring.unlink()
    print("✓ PASS")


def test_json_telemetry():
    """Test realistic JSON telemetry payloads."""
    print("Test 4: JSON telemetry...", end=" ")
    shm_name = "verify_telemetry"
    ring = SharedMemoryRingBuffer(shm_name, size=4096, create=True)

    telemetry = {
        "timestamp": 1704067200,
        "asset_pair": "XLM/USD",
        "price": 0.1234,
        "volume": 1000000,
        "source": "horizon-node-1",
    }

    payload = json.dumps(telemetry).encode("utf-8")
    assert ring.write(payload) is True, "Write failed"

    read_data = ring.read()
    assert read_data is not None, "Read returned None"

    parsed = json.loads(read_data)
    assert parsed == telemetry, f"Data mismatch: {parsed} != {telemetry}"

    ring.close()
    ring.unlink()
    print("✓ PASS")


def writer_process(shm_name, num_messages):
    """Writer subprocess for multiprocess test."""
    ring = SharedMemoryRingBuffer(shm_name, create=False)

    for i in range(num_messages):
        payload = json.dumps({"seq": i, "data": f"message_{i}"}).encode("utf-8")
        while not ring.write(payload):
            time.sleep(0.001)

    ring.close()


def reader_process(shm_name, num_messages, results_queue):
    """Reader subprocess for multiprocess test."""
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


def test_multiprocess():
    """Test cross-process communication via shared memory."""
    print("Test 5: Multiprocess communication...", end=" ")
    shm_name = "verify_multiproc"
    num_messages = 50

    ring = SharedMemoryRingBuffer(shm_name, size=8192, create=True)

    results_queue = multiprocessing.Queue()

    writer = multiprocessing.Process(target=writer_process, args=(shm_name, num_messages))
    reader = multiprocessing.Process(
        target=reader_process, args=(shm_name, num_messages, results_queue)
    )

    writer.start()
    reader.start()

    writer.join(timeout=5)
    reader.join(timeout=5)

    received = results_queue.get(timeout=1)
    assert len(received) == num_messages, f"Expected {num_messages}, got {len(received)}"

    for i, msg in enumerate(received):
        assert msg["seq"] == i, f"Message out of order: expected seq {i}, got {msg['seq']}"

    ring.close()
    ring.unlink()
    print("✓ PASS")


def main():
    """Run all verification tests."""
    print("\n" + "=" * 60)
    print("SharedMemoryRingBuffer Verification Tests")
    print("=" * 60 + "\n")

    try:
        test_basic_write_read()
        test_multiple_messages()
        test_buffer_full()
        test_json_telemetry()
        test_multiprocess()

        print("\n" + "=" * 60)
        print("✓ All tests PASSED")
        print("=" * 60 + "\n")
        print("Subprocesses can now read telemetry payloads directly")
        print("from shared memory locations without copying overhead.")
        return 0

    except AssertionError as e:
        print(f"\n✗ FAILED: {e}")
        return 1
    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
