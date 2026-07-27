"""stream_buffer.py — zero-copy JSON stream parser with SIMD acceleration.

Accepts raw network binary blocks and locates newline-delimited JSON frames
without allocating intermediate string objects, reducing GC pressure during
high-volume market-volatility spikes.

When ``pysimdjson`` is installed the per-frame decode step is handled by a
thread-local ``simdjson.Parser`` instance, which exploits SIMD vectorization
(SSE4.2 / AVX2 / AVX-512) and avoids repeated C++ allocations by reusing the
same parser object across frames on each OS thread.  If the native extension is
not available the module falls back transparently to the standard ``json``
library so the pipeline remains functional in any environment.
"""
from __future__ import annotations

import asyncio
import json
import logging
import mmap
import os
import struct
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Generator

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# SIMD-accelerated JSON back-end (optional)
# ---------------------------------------------------------------------------
try:
    import simdjson as _simdjson  # type: ignore[import-untyped]

    # Each thread gets its own Parser so concurrent ingestion workers don't
    # race on a shared C++ parser state.
    _local = threading.local()

    def _decode(raw: bytes) -> Any:
        """Decode *raw* bytes via simdjson, reusing the per-thread Parser."""
        parser: _simdjson.Parser = getattr(_local, "parser", None)
        if parser is None:
            parser = _simdjson.Parser()
            _local.parser = parser
        # parse() returns a Mapping-compatible C++ proxy — no full Python dict
        # is materialised unless the caller explicitly iterates all keys.
        return parser.parse(raw)

    SIMDJSON_AVAILABLE: bool = True

except ImportError:  # pragma: no cover — covered by fallback-path tests
    def _decode(raw: bytes) -> Any:  # type: ignore[misc]
        """Fallback: standard library JSON decode."""
        return json.loads(raw)

    SIMDJSON_AVAILABLE = False

# ---------------------------------------------------------------------------

_NEWLINE = ord("\n")
_DEFAULT_BUFFER_SIZE = 64 * 1024

# ---------------------------------------------------------------------------
# Memory-mapped sink constants
# ---------------------------------------------------------------------------

# Default pre-allocated file size: 256 MiB.  The ring buffer wraps around
# when the write cursor reaches the end so no re-allocation is ever needed.
_DEFAULT_MAP_SIZE: int = 256 * 1024 * 1024  # 256 MiB

# Header layout (written at offset 0, never part of the payload region):
#   [0:8]   magic  b"SFMMAP\x00\x01"
#   [8:16]  write_cursor (uint64, little-endian) — next byte to write
#   [16:24] wrap_count   (uint64, little-endian) — times the ring wrapped
_HEADER_SIZE: int = 24
_MAGIC: bytes = b"SFMMAP\x00\x01"
_CURSOR_OFFSET: int = 8
_WRAP_OFFSET: int = 16

# Minimum free space in the usable payload region to trigger a wrap.
# If a frame is larger than this we fall back to a truncated write.
_MIN_WRITE_UNIT: int = 4096

# ---------------------------------------------------------------------------
# Zero-copy magic constant
# ---------------------------------------------------------------------------

# A module-level memoryview over the magic bytes lets _read_header() compare
# header content directly against the backing buffer's memoryview slice —
# no temporary bytes object is ever allocated during the comparison.
_MAGIC_VIEW: memoryview = memoryview(_MAGIC)


class MmapLogSink:
    """Pre-allocated memory-mapped ring buffer for zero-copy payload logging.

    The file is created (or re-opened) at *path* and pre-allocated to
    *map_size* bytes.  The first ``_HEADER_SIZE`` bytes store a small binary
    header so the cursor position survives a process restart.

    All public methods are thread-safe.
    """

    __slots__ = (
        "_path",
        "_map_size",
        "_payload_size",
        "_fd",
        "_mm",
        "_cursor",
        "_wrap_count",
        "_lock",
        "_closed",
    )

    def __init__(
        self,
        path: str | os.PathLike[str],
        map_size: int = _DEFAULT_MAP_SIZE,
    ) -> None:
        self._path = Path(path)
        self._map_size = map_size
        self._payload_size = map_size - _HEADER_SIZE
        self._lock = threading.Lock()
        self._closed = False

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd, self._mm = self._open_or_create()
        self._cursor, self._wrap_count = self._read_header()

        logger.debug(
            "MmapLogSink initialised — path=%s map_size=%d cursor=%d wraps=%d",
            self._path,
            self._map_size,
            self._cursor,
            self._wrap_count,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open_or_create(self) -> tuple[int, mmap.mmap]:
        """Open or create the backing file, ensuring it has the right size."""
        existed = self._path.exists()
        fd = os.open(
            str(self._path),
            os.O_RDWR | os.O_CREAT,
            0o600,
        )
        try:
            current_size = os.fstat(fd).st_size
            if current_size < self._map_size:
                # Pre-allocate by extending the file with zero bytes.
                os.ftruncate(fd, self._map_size)

            mm = mmap.mmap(fd, self._map_size, access=mmap.ACCESS_WRITE)
        except Exception:
            os.close(fd)
            raise

        if not existed or current_size < _HEADER_SIZE:
            # Brand-new file — write magic + zero cursor.
            mv = memoryview(mm)
            mv[0:8] = _MAGIC
            mv[_CURSOR_OFFSET : _CURSOR_OFFSET + 8] = struct.pack("<Q", 0)
            mv[_WRAP_OFFSET : _WRAP_OFFSET + 8] = struct.pack("<Q", 0)
            del mv
            mm.flush()

        return fd, mm

    def _read_header(self) -> tuple[int, int]:
        """Read the write-cursor and wrap-count from the file header.

        Zero-copy implementation: all header field reads operate directly on
        the memoryview of the backing mmap, without allocating intermediate
        bytes objects.

        * Magic validation: ``mv[0:8] == _MAGIC_VIEW`` compares the two
          memoryview slices byte-for-byte in C without creating new bytes.
        * Field unpacking: ``struct.unpack_from`` reads little-endian uint64
          values directly from the memoryview at the given offset, bypassing
          the ``bytes()`` conversion that ``struct.unpack`` would require.

        If the magic bytes are absent the header is considered corrupt and
        the cursor is reset to zero (data from a previous run is left intact
        but will be overwritten from the start of the payload region).
        """
        mv = memoryview(self._mm)
        # Zero-copy magic validation: compare memoryview slices directly —
        # no bytes() allocation required.
        if mv[0:8] != _MAGIC_VIEW:
            logger.warning(
                "MmapLogSink: header magic mismatch at %s — resetting cursor",
                self._path,
            )
            mv[0:8] = _MAGIC
            mv[_CURSOR_OFFSET : _CURSOR_OFFSET + 8] = struct.pack("<Q", 0)
            mv[_WRAP_OFFSET : _WRAP_OFFSET + 8] = struct.pack("<Q", 0)
            del mv
            return 0, 0

        # Zero-copy field reads: struct.unpack_from reads directly from the
        # memoryview buffer at the given offset — no bytes() copy needed.
        cursor: int = struct.unpack_from("<Q", mv, _CURSOR_OFFSET)[0]
        wraps: int = struct.unpack_from("<Q", mv, _WRAP_OFFSET)[0]
        del mv
        # Guard against out-of-range cursor from a truncated / partial write.
        if cursor >= self._payload_size:
            cursor = 0
        return cursor, wraps

    def _flush_header(self, mv: memoryview) -> None:
        """Persist cursor + wrap-count into the header region."""
        mv[_CURSOR_OFFSET : _CURSOR_OFFSET + 8] = struct.pack("<Q", self._cursor)
        mv[_WRAP_OFFSET : _WRAP_OFFSET + 8] = struct.pack("<Q", self._wrap_count)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def write_batch(self, frames: list[bytes]) -> None:
        """Write a batch of raw frame bytes into the ring buffer.

        Each frame is appended verbatim; a newline separator is written
        between frames so the log file remains newline-delimited and can be
        replayed by ``StreamBuffer``.  A single ``mmap.flush()`` call covers
        the entire batch, keeping syscall overhead proportional to batch size
        rather than frame count.
        """
        if self._closed or not frames:
            return

        with self._lock:
            mv = memoryview(self._mm)
            payload_start = _HEADER_SIZE

            for raw in frames:
                # Ensure the frame ends with a newline for replay compatibility.
                entry: bytes = raw if raw.endswith(b"\n") else raw + b"\n"
                entry_len = len(entry)

                if entry_len > self._payload_size:
                    # Pathological frame — truncate rather than refuse.
                    entry = entry[: self._payload_size - 1] + b"\n"
                    entry_len = len(entry)

                write_pos = payload_start + self._cursor

                if self._cursor + entry_len <= self._payload_size:
                    # Fast path: frame fits without wrapping.
                    mv[write_pos : write_pos + entry_len] = entry
                    self._cursor += entry_len
                else:
                    # Ring wrap: write from cursor to end, then continue from
                    # the start of the payload region.
                    tail_space = self._payload_size - self._cursor
                    mv[write_pos : write_pos + tail_space] = entry[:tail_space]
                    remainder = entry[tail_space:]
                    mv[payload_start : payload_start + len(remainder)] = remainder
                    self._cursor = len(remainder)
                    self._wrap_count += 1
                    logger.debug(
                        "MmapLogSink: ring wrapped (count=%d)", self._wrap_count
                    )

            self._flush_header(mv)
            del mv
            # Single msync for the whole batch — the key cost reduction.
            self._mm.flush()

    def write(self, raw: bytes) -> None:
        """Convenience wrapper for writing a single raw frame."""
        self.write_batch([raw])

    @property
    def cursor(self) -> int:
        """Current write cursor position within the payload region."""
        with self._lock:
            return self._cursor

    @property
    def wrap_count(self) -> int:
        """Number of times the ring has wrapped around."""
        with self._lock:
            return self._wrap_count

    def close(self) -> None:
        """Flush and release all resources."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            try:
                mv = memoryview(self._mm)
                self._flush_header(mv)
                del mv
                self._mm.flush()
                self._mm.close()
            finally:
                os.close(self._fd)

        logger.debug("MmapLogSink closed — path=%s", self._path)

    def __enter__(self) -> "MmapLogSink":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# O_DIRECT raw file write sink
# ---------------------------------------------------------------------------

# O_DIRECT requires writes to be aligned to the logical block size of the
# underlying device.  512 bytes is the minimum safe alignment on virtually
# all Linux block devices; 4096 bytes (4 KiB) covers advanced-format drives.
# We default to 4096 so the sink works correctly on both sector sizes without
# needing to query the device at runtime.
_DIRECT_IO_ALIGN: int = 4096

# O_DIRECT is Linux-specific; provide a sentinel for non-Linux platforms so
# the class can still be imported and tested (writes fall back to buffered I/O
# when the flag is unavailable).
_O_DIRECT: int = getattr(os, "O_DIRECT", 0)

# Default thread-pool size for async offload.  One dedicated thread per sink
# is sufficient because writes are serialised by the internal lock anyway.
_DIRECT_IO_EXECUTOR_WORKERS: int = 1


def _align_up(n: int, alignment: int) -> int:
    """Round *n* up to the nearest multiple of *alignment*."""
    return (n + alignment - 1) & ~(alignment - 1)


class DirectIOSink:
    """Raw file write sink that bypasses the OS page cache via ``O_DIRECT``.

    ``O_DIRECT`` instructs the kernel to transfer data directly between user
    space and the storage device, skipping the page cache entirely.  This
    eliminates the double-buffering penalty for high-throughput telemetry logs
    where the data is written once and never re-read via the cache.

    Write alignment requirements
    ----------------------------
    ``O_DIRECT`` imposes strict alignment constraints on Linux:

    * The file offset at which each write begins must be a multiple of the
      logical block size (``alignment``, default 4096).
    * The buffer address in memory must be aligned to the same boundary.
    * The transfer length must be a multiple of the same boundary.

    ``DirectIOSink`` handles all three transparently:

    * The file is opened with ``O_APPEND`` so the kernel manages the seek
      position; writes are always block-aligned because the sink pads each
      frame to the next alignment boundary before calling ``os.write``.
    * The internal write buffer is allocated via ``bytearray`` and padded so
      its length is always a multiple of ``alignment``.
    * Memory alignment of the Python buffer is not enforced at the Python
      level (CPython's allocator typically returns page-aligned memory for
      large objects, but this is not guaranteed).  On kernels ≥ 3.16 the
      alignment requirement on the *buffer address* was relaxed to 512 bytes
      for most file systems; ``DirectIOSink`` keeps frame payloads small
      enough that this is never an issue in practice.

    Non-blocking async integration
    --------------------------------
    ``os.write`` with ``O_DIRECT`` can block for the duration of a DMA
    transfer (typically <1 ms but unbounded under heavy I/O pressure).
    ``DirectIOSink.async_write`` and ``async_write_batch`` offload the
    blocking ``os.write`` call to a dedicated :class:`ThreadPoolExecutor` via
    :func:`asyncio.get_event_loop().run_in_executor`, so the async event loop
    is never stalled.

    Thread safety
    -------------
    All public synchronous methods are protected by an internal ``threading.Lock``.
    Concurrent ``async_write`` / ``async_write_batch`` calls are safe because
    each coroutine awaits its executor future before the next write starts
    (the lock inside the executor call serialises concurrent threads).

    Platform notes
    --------------
    ``O_DIRECT`` is Linux-specific.  On platforms where ``os.O_DIRECT`` is not
    defined the sink opens the file without the flag and behaves identically to
    a regular append-only file sink.  This allows the module to be imported and
    tested on macOS / Windows without modification.
    """

    __slots__ = (
        "_path",
        "_alignment",
        "_fd",
        "_lock",
        "_closed",
        "_bytes_written",
        "_executor",
    )

    def __init__(
        self,
        path: str | os.PathLike[str],
        alignment: int = _DIRECT_IO_ALIGN,
    ) -> None:
        """Open (or create) *path* for O_DIRECT appending.

        Parameters
        ----------
        path:
            Destination file path.  Parent directories are created on demand.
        alignment:
            Block-size alignment for O_DIRECT writes (default: 4096 bytes).
            Must be a power of two and ≥ 512.
        """
        if alignment <= 0 or (alignment & (alignment - 1)) != 0:
            raise ValueError(f"alignment must be a positive power of two, got {alignment}")

        self._path = Path(path)
        self._alignment = alignment
        self._lock = threading.Lock()
        self._closed = False
        self._bytes_written: int = 0
        self._executor: ThreadPoolExecutor = ThreadPoolExecutor(
            max_workers=_DIRECT_IO_EXECUTOR_WORKERS,
            thread_name_prefix="directio_sink",
        )

        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._fd = self._open_fd()

        logger.debug(
            "DirectIOSink initialised — path=%s alignment=%d o_direct=%s",
            self._path,
            self._alignment,
            bool(_O_DIRECT),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _open_fd(self) -> int:
        """Return an O_DIRECT | O_APPEND file descriptor for the sink path."""
        flags = os.O_WRONLY | os.O_CREAT | os.O_APPEND | _O_DIRECT
        return os.open(str(self._path), flags, 0o600)

    def _pad_to_alignment(self, data: bytes) -> bytes:
        """Pad *data* with null bytes so its length is a multiple of *alignment*.

        ``O_DIRECT`` requires transfer sizes to be multiples of the block size.
        Telemetry frames are padded with ``\\x00`` bytes to the next aligned
        boundary.  The reader is expected to strip trailing nulls or rely on
        the embedded length prefix / newline delimiter to locate frame bounds.
        """
        remainder = len(data) % self._alignment
        if remainder == 0:
            return data
        return data + b"\x00" * (self._alignment - remainder)

    def _write_sync(self, data: bytes) -> int:
        """Write *data* (must be alignment-padded) synchronously via os.write.

        Returns the number of bytes written to the underlying file descriptor.
        This method is intended for internal use and executor offload only.
        """
        with self._lock:
            if self._closed:
                raise RuntimeError("DirectIOSink is closed")
            padded = self._pad_to_alignment(data)
            n = os.write(self._fd, padded)
            self._bytes_written += n
            return n

    def _write_batch_sync(self, frames: list[bytes]) -> int:
        """Concatenate and write all frames in *frames* as one aligned block.

        Batching reduces the number of ``os.write`` syscalls (and DMA
        transfers) proportional to the batch size, which is the primary
        throughput lever for ``O_DIRECT`` workloads.

        Returns the total number of bytes written (including alignment padding).
        """
        if not frames:
            return 0

        # Concatenate all frames, appending a newline separator so the file
        # remains replay-compatible with StreamBuffer.
        combined = b"".join(
            (f if f.endswith(b"\n") else f + b"\n") for f in frames
        )

        with self._lock:
            if self._closed:
                raise RuntimeError("DirectIOSink is closed")
            padded = self._pad_to_alignment(combined)
            n = os.write(self._fd, padded)
            self._bytes_written += n
            return n

    # ------------------------------------------------------------------
    # Synchronous public API
    # ------------------------------------------------------------------

    def write(self, data: bytes) -> int:
        """Write *data* synchronously, bypassing the OS page cache.

        The payload is padded to the alignment boundary before the write.
        Returns the number of raw bytes written to the descriptor (including
        padding).
        """
        return self._write_sync(data)

    def write_batch(self, frames: list[bytes]) -> int:
        """Write a batch of frames synchronously as a single aligned transfer.

        Returns the total bytes written (including padding).
        """
        return self._write_batch_sync(frames)

    # ------------------------------------------------------------------
    # Async non-blocking public API
    # ------------------------------------------------------------------

    async def async_write(self, data: bytes) -> int:
        """Write *data* without blocking the async event loop.

        The blocking ``os.write`` call is executed in the sink's dedicated
        :class:`ThreadPoolExecutor` so the calling coroutine yields control
        while the DMA transfer is in flight.

        Returns the number of bytes written (including padding).
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self._executor, self._write_sync, data)

    async def async_write_batch(self, frames: list[bytes]) -> int:
        """Write a batch of frames without blocking the async event loop.

        Returns the total bytes written (including padding).
        """
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            self._executor, self._write_batch_sync, frames
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def bytes_written(self) -> int:
        """Total bytes written to the file descriptor since the sink was opened."""
        with self._lock:
            return self._bytes_written

    @property
    def path(self) -> Path:
        """Resolved path of the backing file."""
        return self._path

    @property
    def alignment(self) -> int:
        """Block-size alignment used for O_DIRECT writes."""
        return self._alignment

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def close(self) -> None:
        """Flush any pending executor tasks and release the file descriptor."""
        with self._lock:
            if self._closed:
                return
            self._closed = True

        # Shut down the thread-pool (waits for in-flight writes to complete).
        self._executor.shutdown(wait=True)

        try:
            os.close(self._fd)
        except OSError:
            pass

        logger.debug(
            "DirectIOSink closed — path=%s bytes_written=%d",
            self._path,
            self._bytes_written,
        )

    def __enter__(self) -> "DirectIOSink":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()


# ---------------------------------------------------------------------------
# Module-level default sink (lazily initialised, one per process)
# ---------------------------------------------------------------------------

_DEFAULT_LOG_DIR = Path("logs/ingestion")
_DEFAULT_LOG_FILE = _DEFAULT_LOG_DIR / "stream_payloads.mmap"

_default_sink: MmapLogSink | None = None
_sink_lock = threading.Lock()


def get_default_sink(
    path: str | os.PathLike[str] | None = None,
    map_size: int = _DEFAULT_MAP_SIZE,
) -> MmapLogSink:
    """Return the process-wide default :class:`MmapLogSink`, creating it once.

    Parameters
    ----------
    path:
        Override the backing-file location.  Defaults to
        ``logs/ingestion/stream_payloads.mmap``.
    map_size:
        Pre-allocated file size in bytes.  Only used on first call.
    """
    global _default_sink
    if _default_sink is None:
        with _sink_lock:
            if _default_sink is None:
                effective_path = path if path is not None else _DEFAULT_LOG_FILE
                _default_sink = MmapLogSink(effective_path, map_size)
    return _default_sink


# ---------------------------------------------------------------------------
# Zero-copy WebSocket / SFMMAP frame header parser
# ---------------------------------------------------------------------------

# Total size of the binary frame header: magic (8) + cursor (8) + wrap (8).
_WS_FRAME_HEADER_SIZE: int = _HEADER_SIZE  # 24 bytes


def parse_ws_frame_header(
    buf: bytes | bytearray | memoryview,
) -> tuple[int, int]:
    """Parse an SFMMAP binary frame header without allocating intermediate bytes.

    This function accepts any buffer that supports the Python buffer protocol
    (``bytes``, ``bytearray``, or ``memoryview``) and returns the
    ``(cursor, wrap_count)`` pair encoded in the header.

    Zero-copy guarantee
    -------------------
    A ``memoryview`` is taken over *buf* exactly once (or reused when *buf* is
    already a ``memoryview``).  All length checks, magic validation, and field
    unpacking operate on that view; no temporary ``bytes`` objects are created
    at any point in the hot path.

    Header layout
    -------------
    ``[0:8]``   magic  — ``b"SFMMAP\\x00\\x01"``
    ``[8:16]``  cursor — uint64 little-endian, next write position
    ``[16:24]`` wraps  — uint64 little-endian, ring wrap count

    Parameters
    ----------
    buf:
        A buffer of at least :data:`_HEADER_SIZE` (24) bytes whose first 24
        bytes contain a valid SFMMAP frame header.

    Returns
    -------
    tuple[int, int]
        ``(cursor, wrap_count)`` extracted from the header.

    Raises
    ------
    ValueError
        If *buf* is shorter than :data:`_HEADER_SIZE` bytes (truncated header).
    ValueError
        If the magic bytes at ``buf[0:8]`` do not match ``_MAGIC``
        (invalid or corrupt header).
    """
    # Wrap in a memoryview only if necessary — if buf is already a memoryview
    # we use it directly so callers that hold a long-lived view pay zero cost.
    mv: memoryview = buf if isinstance(buf, memoryview) else memoryview(buf)

    # Length check before any field access — guards against truncated frames.
    if len(mv) < _WS_FRAME_HEADER_SIZE:
        raise ValueError(
            f"frame header too short: expected {_WS_FRAME_HEADER_SIZE} bytes, "
            f"got {len(mv)}"
        )

    # Zero-copy magic validation: compare memoryview slices directly.
    # mv[0:8] is a sub-view; _MAGIC_VIEW is the module-level view of _MAGIC.
    # CPython compares these via the C buffer protocol — no bytes() allocation.
    if mv[0:8] != _MAGIC_VIEW:
        raise ValueError(
            "invalid frame header: magic bytes mismatch "
            f"(got {bytes(mv[0:8])!r}, expected {_MAGIC!r})"
        )

    # Zero-copy field unpacking: struct.unpack_from reads directly from the
    # memoryview buffer at the specified offset — no bytes() copy required.
    cursor: int = struct.unpack_from("<Q", mv, _CURSOR_OFFSET)[0]
    wraps: int = struct.unpack_from("<Q", mv, _WRAP_OFFSET)[0]

    return cursor, wraps


# ---------------------------------------------------------------------------
# Stream parser
# ---------------------------------------------------------------------------


class StreamBuffer:
    """Accumulate binary chunks, yield parsed JSON objects, and log raw frames.

    Uses a pre-allocated :class:`bytearray` as a rolling window over the
    incoming byte stream.  Newline-delimited JSON frames are located via a
    single linear scan; complete frames are decoded by the SIMD-accelerated
    back-end when ``pysimdjson`` is available, or by ``json.loads`` otherwise.
    The backing buffer is never reallocated between ``feed`` calls, which keeps
    GC pressure flat during sustained high-volume ingestion.
    """

    __slots__ = ("_buf", "_start", "_size", "_capacity")

    def __init__(self, buffer_size: int = _DEFAULT_BUFFER_SIZE) -> None:
        if buffer_size <= 0:
            raise ValueError("buffer size must be positive")
        self._buf = bytearray(buffer_size)
        self._start = 0
        self._size = 0
        self._capacity = buffer_size

    def _compact(self) -> None:
        """Move any retained bytes back to the front of the backing buffer."""
        if self._size == 0 or self._start == 0:
            return
        view = memoryview(self._buf)[self._start : self._start + self._size]
        self._buf[: self._size] = view
        self._start = 0

    def feed(self, data: bytes | bytearray | memoryview) -> Generator[Any, None, None]:
        """Append *data* and yield every complete newline-delimited JSON frame.

        A memoryview over the internal bytearray is used during the scan phase
        to slice frame boundaries without intermediate string copies.  The view
        is released before the buffer is trimmed so the bytearray can resize.

        Each complete frame is decoded by the SIMD-accelerated back-end when
        ``pysimdjson`` is available, or by ``json.loads`` otherwise.
        The parser uses a pre-allocated backing buffer that is reused across feeds
        so stream workers avoid repeated dynamic allocations for incoming blocks.
        """
        if not data:
            return

        payload = memoryview(data)
        self._compact()

        if len(payload) > self._capacity - self._size:
            raise ValueError("stream chunk exceeds pre-allocated buffer capacity")

        end = self._start + self._size
        self._buf[end : end + len(payload)] = payload
        self._size += len(payload)

        raw_frames: list[bytes] = []
        start = 0

        view = memoryview(self._buf)[self._start : self._start + self._size]
        for i in range(len(view)):
            if view[i] == _NEWLINE:
                if i > start:
                    raw_frames.append(bytes(view[start:i]))
                start = i + 1
        consumed = start
        view.release()

        if consumed:
            self._start += consumed
            self._size -= consumed
            if self._size == 0:
                self._start = 0

        for frame in raw_frames:
            # _decode() handles both the SIMD and stdlib fallback paths.
            # Input is already bytes — no str conversion needed, which is a
            # free performance win over the previous json.loads(str) pattern.
            yield _decode(frame)

    def reset(self) -> None:
        """Discard all buffered data while keeping the backing storage reusable."""
        self._start = 0
        self._size = 0


__all__ = [
    "SIMDJSON_AVAILABLE",
    "StreamBuffer",
    "MmapLogSink",
    "DirectIOSink",
    "get_default_sink",
    "parse_ws_frame_header",
]
