# Shared Memory Ring Buffer Implementation

## Overview

The `SharedMemoryRingBuffer` class provides a zero-copy inter-process communication mechanism for high-throughput telemetry ingestion pipelines. By using `multiprocessing.shared_memory`, data is shared directly between processes without copying across process boundaries, eliminating CPU and memory overhead.

## Problem Solved

**Before**: Ingestion pipelines copying data across process boundaries created unnecessary CPU/memory overhead during high-volume market volatility.

**After**: Subprocesses read telemetry payloads directly from shared memory locations, achieving true zero-copy inter-process data transfer.

## Architecture

### Memory Layout

```
[0:16]       Metadata (16 bytes)
  [0:4]      write_pos (uint32)
  [4:8]      read_pos (uint32)
  [8:12]     capacity (uint32)
  [12:16]    reserved
[16:16+size] Ring buffer data area
```

### Ring Buffer Protocol

Each payload is prefixed with a 4-byte length header:
```
[length: 4 bytes][payload: N bytes]
```

The ring buffer wraps around at capacity boundaries, splitting writes/reads when necessary.

## Usage

### Creating a Ring Buffer (Producer Process)

```python
from ingestion.stream_buffer import SharedMemoryRingBuffer

# Create shared memory ring buffer
ring = SharedMemoryRingBuffer(
    name="telemetry_buffer",
    size=1024*1024,  # 1MB data area
    create=True
)

# Write telemetry payload
import json
telemetry = {
    "timestamp": 1704067200,
    "asset_pair": "XLM/USD",
    "price": 0.1234,
    "volume": 1000000,
    "source": "horizon-node-1"
}

payload = json.dumps(telemetry).encode("utf-8")
success = ring.write(payload)

# Clean up (creator only)
ring.close()
ring.unlink()  # Destroy shared memory segment
```

### Attaching to Ring Buffer (Consumer Process)

```python
from ingestion.stream_buffer import SharedMemoryRingBuffer

# Attach to existing shared memory
ring = SharedMemoryRingBuffer(
    name="telemetry_buffer",
    create=False  # Attach to existing segment
)

# Read telemetry payload (zero-copy)
payload = ring.read()
if payload is not None:
    telemetry = json.loads(payload)
    process_telemetry(telemetry)

# Clean up
ring.close()  # Don't call unlink() in consumer!
```

## API Reference

### SharedMemoryRingBuffer(name, size, create)

**Parameters:**
- `name` (str): Unique identifier for the shared memory segment
- `size` (int): Size of data area in bytes (default: 1MB)
- `create` (bool): If True, create new segment; if False, attach to existing

### Methods

#### `write(data: bytes) -> bool`

Write a binary payload to the ring buffer.

**Returns:** `True` if write succeeded, `False` if buffer is full

**Note:** Payload is prefixed with 4-byte length header automatically.

#### `read() -> bytes | None`

Read one payload from the ring buffer.

**Returns:** Binary payload, or `None` if buffer is empty

**Note:** Zero-copy read directly from shared memory.

#### `close()`

Close the shared memory handle. Safe for all processes.

#### `unlink()`

Destroy the shared memory segment. **Only call from the creating process.**

## Performance Characteristics

### Zero-Copy Benefits

- **No serialization overhead**: Raw bytes transferred directly
- **No memory duplication**: Single copy in shared memory
- **Reduced GC pressure**: Minimal Python object allocation
- **Lower CPU usage**: No memcpy() between processes

### Capacity Planning

Buffer size should accommodate burst traffic:

```python
# Example: 100 messages/sec, 1KB average size, 10 sec burst
size = 100 * 1024 * 10  # 1MB buffer
```

### Thread Safety

**Single writer, single reader**: Lock-free implementation

**Multiple writers/readers**: Requires external synchronization

## Testing

### Run Verification Script

```bash
python verify_shared_memory.py
```

This runs comprehensive tests without requiring pytest:
- Basic write/read operations
- FIFO ordering
- Buffer full/empty conditions
- JSON telemetry payloads
- Multi-process communication

### Run Pytest Suite

```bash
pytest tests/test_stream_buffer.py -k test_shared_memory_ring_buffer -v
```

## Integration Example

### Multi-Process Telemetry Pipeline

```python
import multiprocessing
from ingestion.stream_buffer import SharedMemoryRingBuffer

def ingestion_worker(shm_name):
    """Worker process: writes telemetry to shared buffer"""
    ring = SharedMemoryRingBuffer(shm_name, create=False)
    
    while True:
        telemetry = fetch_market_data()
        payload = json.dumps(telemetry).encode("utf-8")
        
        # Retry until space available
        while not ring.write(payload):
            time.sleep(0.001)
    
    ring.close()

def processing_worker(shm_name):
    """Worker process: reads telemetry from shared buffer"""
    ring = SharedMemoryRingBuffer(shm_name, create=False)
    
    while True:
        payload = ring.read()
        if payload:
            telemetry = json.loads(payload)
            analyze_and_store(telemetry)
        else:
            time.sleep(0.001)
    
    ring.close()

# Main process
if __name__ == "__main__":
    shm_name = "market_telemetry"
    
    # Create shared memory
    ring = SharedMemoryRingBuffer(shm_name, size=10*1024*1024, create=True)
    ring.close()
    
    # Spawn workers
    workers = [
        multiprocessing.Process(target=ingestion_worker, args=(shm_name,)),
        multiprocessing.Process(target=processing_worker, args=(shm_name,)),
    ]
    
    for w in workers:
        w.start()
    
    for w in workers:
        w.join()
    
    # Cleanup
    ring = SharedMemoryRingBuffer(shm_name, create=False)
    ring.unlink()
```

## Monitoring

### Buffer Health Metrics

Monitor these indicators for optimal performance:

- **Write failures**: Indicates buffer full, increase size or reader speed
- **Read None**: Indicates buffer empty, normal during idle periods
- **Latency**: Time between write and read should be minimal

### Debug Tips

```python
# Check buffer positions
write_pos, read_pos = ring._get_positions()
print(f"Write: {write_pos}, Read: {read_pos}")

# Check available space
available = ring._available_space(write_pos, read_pos)
print(f"Available: {available} / {ring._capacity} bytes")
```

## Acceptance Criteria ✓

- [x] Built shared memory ring buffers using `multiprocessing.shared_memory`
- [x] Implemented in `src/ingestion/stream_buffer.py`
- [x] Subprocesses read telemetry payloads directly from shared memory
- [x] Created comprehensive test suite in `tests/test_stream_buffer.py`
- [x] All tests passing: `pytest tests/test_stream_buffer.py -k test_shared_memory_ring_buffer`

## References

- Python documentation: [`multiprocessing.shared_memory`](https://docs.python.org/3/library/multiprocessing.shared_memory.html)
- Ring buffer algorithm: Circular buffer with write/read pointers
- Zero-copy I/O: Direct memory access without intermediate copies
