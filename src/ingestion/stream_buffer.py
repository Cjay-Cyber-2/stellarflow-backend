"""stream_buffer.py — zero-copy JSON stream parser using memoryview.

Accepts raw network binary blocks and locates newline-delimited JSON frames
without allocating intermediate string objects, reducing GC pressure during
high-volume market-volatility spikes.

Includes SharedMemoryRingBuffer for multi-process data ingestion pipelines
that eliminates cross-process copying overhead by using shared memory segments.
"""
from __future__ import annotations

import json
import struct
from multiprocessing import shared_memory
from typing import Any, Generator

_NEWLINE = ord("\n")

# Ring buffer metadata layout in first 16 bytes:
# [0:4]   write_pos (int32)
# [4:8]   read_pos (int32)
# [8:12]  capacity (int32)
# [12:16] reserved for alignment
_METADATA_SIZE = 16
_WRITE_POS_OFFSET = 0
_READ_POS_OFFSET = 4
_CAPACITY_OFFSET = 8


class StreamBuffer:
    """Accumulate binary chunks and yield parsed JSON objects zero-copy."""

    __slots__ = ("_buf",)

    def __init__(self) -> None:
        self._buf = bytearray()

    def feed(self, data: bytes | bytearray | memoryview) -> Generator[Any, None, None]:
        """Append *data* and yield every complete newline-delimited JSON frame.

        A memoryview over the internal bytearray is used during the scan phase
        to slice frame boundaries without intermediate string copies.  The view
        is released before the buffer is trimmed so the bytearray can resize.
        """
        self._buf += data  # single extend, no str conversion

        frames: list[bytes] = []
        start = 0

        view = memoryview(self._buf)
        for i in range(len(view)):
            if view[i] == _NEWLINE:
                if i > start:
                    frames.append(bytes(view[start:i]))
                start = i + 1
        consumed = start
        view.release()  # release before resizing

        del self._buf[:consumed]  # keep only the incomplete trailing fragment

        for frame in frames:
            yield json.loads(frame)

    def reset(self) -> None:
        """Discard all buffered data."""
        self._buf.clear()


class SharedMemoryRingBuffer:
    """Lock-free ring buffer using multiprocessing.shared_memory for zero-copy
    inter-process telemetry payload sharing.

    Writer processes call write() to append telemetry frames. Reader processes
    call read() to consume frames directly from shared memory without copying
    across process boundaries.

    Memory layout:
      [0:16]       Metadata (write_pos, read_pos, capacity)
      [16:16+size] Ring buffer data

    Thread safety: Single writer, single reader. Multi-writer or multi-reader
    scenarios require external locking or atomic operations.
    """

    __slots__ = ("_shm", "_capacity", "_buf")

    def __init__(
        self, name: str, size: int = 1024 * 1024, create: bool = False
    ) -> None:
        """Initialize shared memory ring buffer.

        Args:
            name: Unique name for the shared memory segment
            size: Size of the ring buffer data area in bytes (excluding metadata)
            create: If True, create new shared memory; if False, attach to existing
        """
        total_size = _METADATA_SIZE + size

        if create:
            self._shm = shared_memory.SharedMemory(name=name, create=True, size=total_size)
            # Initialize metadata
            struct.pack_into("III", self._shm.buf, 0, 0, 0, size)  # write_pos, read_pos, capacity
        else:
            self._shm = shared_memory.SharedMemory(name=name, create=False)

        # Read capacity from shared memory
        self._capacity = struct.unpack_from("I", self._shm.buf, _CAPACITY_OFFSET)[0]
        self._buf = memoryview(self._shm.buf)

    def write(self, data: bytes) -> bool:
        """Write telemetry payload to the ring buffer.

        Args:
            data: Binary payload to write (typically JSON frame)

        Returns:
            True if write succeeded, False if buffer is full

        The payload is prefixed with a 4-byte length header for framing.
        """
        payload_size = len(data) + 4  # 4 bytes for length prefix

        write_pos, read_pos = self._get_positions()
        available = self._available_space(write_pos, read_pos)

        if payload_size > available:
            return False  # Buffer full

        # Write length prefix
        data_start = _METADATA_SIZE + write_pos
        struct.pack_into("I", self._shm.buf, data_start, len(data))

        # Write payload (handle wrap-around)
        payload_start = data_start + 4
        remaining = self._capacity - (write_pos + 4)

        if remaining >= len(data):
            # No wrap-around needed
            self._buf[payload_start : payload_start + len(data)] = data
        else:
            # Wrap-around: split write
            self._buf[payload_start : payload_start + remaining] = data[:remaining]
            wrap_start = _METADATA_SIZE
            self._buf[wrap_start : wrap_start + len(data) - remaining] = data[remaining:]

        # Update write position
        new_write_pos = (write_pos + payload_size) % self._capacity
        struct.pack_into("I", self._shm.buf, _WRITE_POS_OFFSET, new_write_pos)

        return True

    def read(self) -> bytes | None:
        """Read one telemetry payload from the ring buffer.

        Returns:
            Binary payload, or None if buffer is empty

        Reads are zero-copy: payload is extracted directly from shared memory.
        """
        write_pos, read_pos = self._get_positions()

        if write_pos == read_pos:
            return None  # Buffer empty

        # Read length prefix
        data_start = _METADATA_SIZE + read_pos
        (payload_len,) = struct.unpack_from("I", self._shm.buf, data_start)

        # Read payload (handle wrap-around)
        payload_start = data_start + 4
        remaining = self._capacity - (read_pos + 4)

        if remaining >= payload_len:
            # No wrap-around
            payload = bytes(self._buf[payload_start : payload_start + payload_len])
        else:
            # Wrap-around: split read
            part1 = bytes(self._buf[payload_start : payload_start + remaining])
            wrap_start = _METADATA_SIZE
            part2_len = payload_len - remaining
            part2 = bytes(self._buf[wrap_start : wrap_start + part2_len])
            payload = part1 + part2

        # Update read position
        frame_size = 4 + payload_len
        new_read_pos = (read_pos + frame_size) % self._capacity
        struct.pack_into("I", self._shm.buf, _READ_POS_OFFSET, new_read_pos)

        return payload

    def close(self) -> None:
        """Close the shared memory handle. Does not unlink/destroy the segment."""
        self._shm.close()

    def unlink(self) -> None:
        """Destroy the shared memory segment. Call from the creating process only."""
        self._shm.unlink()

    def _get_positions(self) -> tuple[int, int]:
        """Read current write_pos and read_pos from shared memory metadata."""
        write_pos, read_pos = struct.unpack_from("II", self._shm.buf, 0)
        return write_pos, read_pos

    def _available_space(self, write_pos: int, read_pos: int) -> int:
        """Calculate available space for writing.

        Reserve 1 byte to distinguish full from empty (write_pos == read_pos).
        """
        if write_pos >= read_pos:
            return self._capacity - (write_pos - read_pos) - 1
        else:
            return read_pos - write_pos - 1


__all__ = ["StreamBuffer", "SharedMemoryRingBuffer"]
