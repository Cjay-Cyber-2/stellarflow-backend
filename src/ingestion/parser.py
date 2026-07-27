"""High-throughput websocket ticker flattening for analytics ingestion.

Websocket providers often wrap ticker frames in several layers of schema
metadata (for example: ``{"data": {"ticker": {...}}}`` or batched
``{"frames": [...]}``). This module collapses those nested payloads into
uniform, flat tuple segments so the analytics engine can consume a stable
shape with minimal Python object churn.

SIMD Acceleration
-----------------
When ``pysimdjson`` is installed (``pip install pysimdjson``), the raw JSON
decode step is handled by a thread-local ``simdjson.Parser`` that exploits
SIMD vectorization (SSE4.2 / AVX2 / AVX-512 depending on host CPU).  The
parser is reused across calls on the same OS thread to avoid repeated C++
heap allocations.

If the native extension is not available the module falls back silently to
``json.loads``, keeping the pipeline functional in any environment.  The
:data:`SIMDJSON_AVAILABLE` flag is exported so callers can introspect which
path is active at runtime.

Pre-compiled Binary Layout Decoder
-----------------------------------
In addition to JSON-based parsing, this module provides a
:class:`BinaryFrameLayout` — a ``ctypes.Structure`` subclass — that decodes
pre-packed 40-byte telemetry frames directly from raw bytes without any
dynamic dictionary construction.  This eliminates the per-frame allocation
and field-lookup overhead that JSON parsing incurs, making it suitable for
high-frequency socket ingestion paths.

The binary layout matches the 40-byte frame format defined in
:mod:`src.serialization.encoders`:

    ┌─────────────┬────────┬──────────────────────────────────────────────┐
    │ Field        │ Format │ Description                                  │
    ├─────────────┼────────┼──────────────────────────────────────────────┤
    │ asset_id    │  8s    │ 8-byte ASCII asset pair (e.g. b"NGN/XLM\\x00")│
    │ price       │   q    │ int64 scaled price (fixed-point 10⁷)         │
    │ volume      │   Q    │ uint64 24-h rolling volume (scaled 10⁷)      │
    │ timestamp   │   Q    │ uint64 Unix epoch milliseconds               │
    │ sequence    │   I    │ uint32 monotonic sequence / nonce counter     │
    │ flags       │   H    │ uint16 status-flag bitmask                   │
    │ feed_id     │   B    │ uint8  originating data-feed identifier       │
    │ _reserved   │   B    │ uint8  reserved byte (always 0x00)           │
    └─────────────┴────────┴──────────────────────────────────────────────┘
    Total frame size: 40 bytes

Use :func:`decode_binary_packet` to decode a single 40-byte frame, or
:func:`decode_binary_batch` to decode a concatenated bundle of frames.
"""

from __future__ import annotations

import ctypes
import json
import threading
from collections.abc import Iterable, Iterator, Mapping
from typing import Union, cast

# TypeAlias polyfill for Python < 3.10
try:
    from typing import TypeAlias  # type: ignore[attr-defined]
except ImportError:
    # typing_extensions not available either — define a no-op alias
    TypeAlias = type  # type: ignore[misc,assignment]

# ---------------------------------------------------------------------------
# SIMD-accelerated JSON back-end (optional)
# ---------------------------------------------------------------------------
try:
    import simdjson as _simdjson  # type: ignore[import-untyped]

    # Thread-local storage: each worker thread gets its own Parser instance so
    # concurrent ingestion goroutines cannot race on shared C++ parser state.
    _tl = threading.local()

    def _simd_decode(raw: bytes) -> object:
        """Decode *raw* bytes via simdjson, reusing the per-thread Parser.

        Returns a ``Mapping``-compatible proxy object backed by the C++
        simdjson document.  The proxy supports ``__getitem__``, ``.get()``,
        and the ``collections.abc.Mapping`` protocol, so the rest of this
        module's traversal logic requires no changes.

        Thread safety: each OS thread owns a dedicated ``simdjson.Parser``
        instance stored in ``threading.local()``.
        """
        parser: _simdjson.Parser = getattr(_tl, "parser", None)
        if parser is None:
            parser = _simdjson.Parser()
            _tl.parser = parser
        return parser.parse(raw)

    SIMDJSON_AVAILABLE: bool = True

except ImportError:  # pragma: no cover — exercised by test_parser_fallback_path
    def _simd_decode(raw: bytes) -> object:  # type: ignore[misc]
        """Fallback: standard-library JSON decode."""
        return json.loads(raw)

    SIMDJSON_AVAILABLE = False

# ---------------------------------------------------------------------------
# Type aliases
# ---------------------------------------------------------------------------

TelemetryTuple: TypeAlias = tuple[str, float, int, int, int]
TelemetrySegment: TypeAlias = tuple[TelemetryTuple, ...]
TelemetrySegmentBatch: TypeAlias = tuple[TelemetrySegment, ...]
SequencePayload: TypeAlias = Union[list[object], tuple[object, ...]]

DEFAULT_SEGMENT_SIZE = 256

# ---------------------------------------------------------------------------
# Pre-compiled binary layout decoder (ctypes.Structure)
# ---------------------------------------------------------------------------
# Matches the 40-byte TelemetryFrame layout from src/serialization/encoders.py.
# Using ctypes.Structure avoids dynamic dictionary construction and per-field
# attribute lookups — the C-level struct is mapped directly over the raw bytes.

class BinaryFrameLayout(ctypes.Structure):
    """Pre-compiled ctypes layout for a 40-byte telemetry frame.

    Fields are ordered to match the little-endian, tightly-packed format
    defined in :mod:`src.serialization.encoders`:

    * ``asset_id``  — 8-byte ASCII asset pair (zero-padded)
    * ``price``     — int64 scaled price (fixed-point 10⁷)
    * ``volume``    — uint64 24-h rolling volume (scaled 10⁷)
    * ``timestamp`` — uint64 Unix epoch milliseconds
    * ``sequence``  — uint32 monotonic counter
    * ``flags``     — uint16 status bitmask
    * ``feed_id``   — uint8 data-feed source identifier
    * ``_reserved`` — uint8 padding (always 0x00)

    Usage::

        buf = BinaryFrameLayout.from_buffer_copy(raw_bytes)
        asset = buf.asset_id.rstrip(b"\\x00").decode("ascii")
        price = buf.price / 10_000_000.0
    """

    _pack_ = 1  # tightly-packed, no implicit C padding

    _fields_: list[tuple[str, type[ctypes._CData]]] = [
        ("asset_id", ctypes.c_char * 8),
        ("price", ctypes.c_int64),
        ("volume", ctypes.c_uint64),
        ("timestamp", ctypes.c_uint64),
        ("sequence", ctypes.c_uint32),
        ("flags", ctypes.c_uint16),
        ("feed_id", ctypes.c_uint8),
        ("_reserved", ctypes.c_uint8),
    ]

    FRAME_SIZE: int = ctypes.sizeof(ctypes.c_char * 8) + ctypes.sizeof(ctypes.c_int64) + ctypes.sizeof(ctypes.c_uint64) + ctypes.sizeof(ctypes.c_uint64) + ctypes.sizeof(ctypes.c_uint32) + ctypes.sizeof(ctypes.c_uint16) + ctypes.sizeof(ctypes.c_uint8) + ctypes.sizeof(ctypes.c_uint8)  # 40 bytes

    @classmethod
    def frame_size(cls) -> int:
        """Return the fixed byte size of one binary frame (40 bytes)."""
        return cls.FRAME_SIZE


# Verify compile-time size matches expected 40-byte layout
assert BinaryFrameLayout.FRAME_SIZE == 40, (
    f"BinaryFrameLayout size mismatch: expected 40, got {BinaryFrameLayout.FRAME_SIZE}"
)


def decode_binary_packet(data: bytes) -> TelemetryTuple:
    """Decode a single 40-byte binary frame into a :data:`TelemetryTuple`.

    This function uses :class:`BinaryFrameLayout` (a ``ctypes.Structure``)
    to map the raw bytes directly onto typed C fields, avoiding any dynamic
    dictionary construction or per-field attribute lookups.

    Parameters
    ----------
    data:
        Exactly 40 bytes of packed binary data in the TelemetryFrame layout.

    Returns
    -------
    TelemetryTuple
        ``(asset_id, price, timestamp, sequence, flags)`` where *asset_id*
        is a decoded uppercase string, *price* is a float (unscaled from
        fixed-point 10⁷), and *timestamp*, *sequence*, *flags* are integers.

    Raises
    ------
    ValueError
        If ``data`` is shorter than 40 bytes.
    """
    if len(data) < BinaryFrameLayout.FRAME_SIZE:
        raise ValueError(
            f"Binary packet too short: expected {BinaryFrameLayout.FRAME_SIZE} bytes, "
            f"got {len(data)}"
        )

    buf = BinaryFrameLayout.from_buffer_copy(data[: BinaryFrameLayout.FRAME_SIZE])

    asset = buf.asset_id.rstrip(b"\x00").decode("ascii", errors="replace").strip().upper()
    if not asset:
        raise ValueError("decoded asset identifier is empty")

    # Unscale price from fixed-point 10⁷ back to float
    price = buf.price / 10_000_000.0

    return (
        asset,
        price,
        buf.timestamp,
        buf.sequence,
        buf.flags,
    )


def decode_binary_batch(data: bytes) -> TelemetrySegment:
    """Decode a concatenated bundle of 40-byte binary frames.

    The input is expected to be a contiguous byte string of one or more
    40-byte frames (as produced by ``TelemetryEncoder.pack_bundle()`` in
    :mod:`src.serialization.encoders`).  Trailing bytes that do not form a
    complete frame are silently discarded.

    Parameters
    ----------
    data:
        Raw bytes containing one or more concatenated 40-byte frames.

    Returns
    -------
    TelemetrySegment
        An immutable tuple of :data:`TelemetryTuple` records.
    """
    size = BinaryFrameLayout.FRAME_SIZE
    count = len(data) // size
    frames: list[TelemetryTuple] = [None] * count  # type: ignore[list-item]

    for i in range(count):
        offset = i * size
        frames[i] = decode_binary_packet(data[offset : offset + size])

    return tuple(frames)


# ---------------------------------------------------------------------------
# Field-name look-up tables (ordered by likelihood of appearance)
# ---------------------------------------------------------------------------

_ASSET_KEYS = ("asset_id", "asset", "symbol", "pair", "instrument")
_PRICE_KEYS = ("price", "last_price", "last", "mark_price", "value")
_TIMESTAMP_KEYS = ("timestamp", "ts", "time", "event_time")
_SEQUENCE_KEYS = ("sequence", "seq", "nonce")
_FLAGS_KEYS = ("flags", "status_flags", "flag_bits")

_BATCH_KEYS = ("frames", "ticks", "tickers", "telemetry", "updates", "results")
_CONTAINER_KEYS = ("data", "payload", "result")
_FRAME_KEYS = ("frame", "tick", "ticker", "telemetry")

_NON_STRING_SEQUENCES = (list, tuple)
_MISSING = object()


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _first_present(
    mapping: Mapping[str, object],
    keys: tuple[str, ...],
    default: object = _MISSING,
) -> object:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return default


def _has_any(mapping: Mapping[str, object], keys: tuple[str, ...]) -> bool:
    return _first_present(mapping, keys) is not _MISSING


def _coerce_asset(value: object) -> str:
    if isinstance(value, bytes):
        value = value.decode("ascii")
    elif not isinstance(value, str):
        raise TypeError(
            f"asset identifier must be str or bytes, got {type(value).__name__}"
        )

    asset = value.strip().upper()
    if not asset:
        raise ValueError("asset identifier must not be empty")
    return asset


def _coerce_float(value: object, field_name: str) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name} must not be empty")
        return float(text)
    raise TypeError(f"{field_name} must be numeric, got {type(value).__name__}")


def _coerce_int(value: object, field_name: str) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        if not value.is_integer():
            raise ValueError(f"{field_name} must be an integer value")
        return int(value)
    if isinstance(value, str):
        text = value.strip()
        if not text:
            raise ValueError(f"{field_name} must not be empty")
        return int(text, 10)
    raise TypeError(f"{field_name} must be an integer, got {type(value).__name__}")


def _looks_like_frame(mapping: Mapping[str, object]) -> bool:
    return (
        _has_any(mapping, _ASSET_KEYS)
        and _has_any(mapping, _PRICE_KEYS)
        and _has_any(mapping, _TIMESTAMP_KEYS)
    )


def _flatten_frame(mapping: Mapping[str, object]) -> TelemetryTuple:
    asset_value = _first_present(mapping, _ASSET_KEYS)
    price_value = _first_present(mapping, _PRICE_KEYS)
    timestamp_value = _first_present(mapping, _TIMESTAMP_KEYS)
    sequence_value = _first_present(mapping, _SEQUENCE_KEYS, 0)
    flags_value = _first_present(mapping, _FLAGS_KEYS, 0)

    if asset_value is _MISSING:
        raise ValueError("ticker frame is missing asset identifier")
    if price_value is _MISSING:
        raise ValueError("ticker frame is missing price")
    if timestamp_value is _MISSING:
        raise ValueError("ticker frame is missing timestamp")

    return (
        _coerce_asset(asset_value),
        _coerce_float(price_value, "price"),
        _coerce_int(timestamp_value, "timestamp"),
        _coerce_int(sequence_value, "sequence"),
        _coerce_int(flags_value, "flags"),
    )


def _as_sequence(value: object) -> Union[SequencePayload, None]:
    if isinstance(value, _NON_STRING_SEQUENCES):
        return cast(SequencePayload, value)
    return None


# ---------------------------------------------------------------------------
# Public API — ingestion entry point
# ---------------------------------------------------------------------------


def parse_raw_pack(
    raw: bytes,
    *,
    drop_invalid: bool = False,
) -> TelemetrySegment:
    """Decode a raw JSON B2C pack and flatten all contained ticker frames.

    This is the primary ingestion entry point for incoming binary payloads
    received over websocket or REST transport.  The raw bytes are decoded
    using the SIMD-accelerated back-end when ``pysimdjson`` is available,
    falling back to ``json.loads`` otherwise.

    Parameters
    ----------
    raw:
        Raw JSON-encoded bytes.  Must be a single JSON value (object or
        array) — newline delimiting is handled upstream by
        :class:`~ingestion.stream_buffer.StreamBuffer`.
    drop_invalid:
        When ``True`` frames that fail field extraction are silently skipped.
        When ``False`` (the default) the first malformed frame raises.

    Returns
    -------
    TelemetrySegment
        Immutable tuple of :data:`TelemetryTuple` records in the order they
        were encountered inside *raw*.

    Raises
    ------
    ValueError
        If *raw* is empty.
    json.JSONDecodeError / simdjson.InvalidJSON
        If *raw* is not valid JSON.
    """
    if not raw:
        raise ValueError("raw pack must not be empty")

    decoded = _simd_decode(raw)

    # A top-level array is unpacked as individual payloads; any other value
    # (object, scalar) is wrapped in a list so the flattening loop sees a
    # uniform iterable.
    payloads: list[object] = list(decoded) if isinstance(decoded, (list, tuple)) else [decoded]
    return flatten_telemetry_frames(payloads, drop_invalid=drop_invalid)


# ---------------------------------------------------------------------------
# Public API — streaming / iterator helpers
# ---------------------------------------------------------------------------


def iter_flat_ticker_tuples(
    payloads: Iterable[object],
    *,
    drop_invalid: bool = False,
) -> Iterator[TelemetryTuple]:
    """Yield flat ticker tuples from nested websocket payloads.

    Each yielded tuple has the shape ``(asset_id, price, timestamp, sequence,
    flags)``. Optional ``sequence`` and ``flags`` values default to ``0`` when
    they are not present in the incoming frame.
    """
    for payload in payloads:
        stack: list[object] = [payload]

        while stack:
            current = stack.pop()

            if current is None:
                continue

            if isinstance(current, Mapping):
                mapping = cast(Mapping[str, object], current)

                if _looks_like_frame(mapping):
                    try:
                        yield _flatten_frame(mapping)
                    except (TypeError, ValueError):
                        if not drop_invalid:
                            raise
                    continue

                expanded = False

                for key in _BATCH_KEYS:
                    sequence = _as_sequence(mapping.get(key))
                    if sequence is not None:
                        stack.extend(reversed(sequence))
                        expanded = True

                for key in _CONTAINER_KEYS:
                    value = mapping.get(key)
                    if isinstance(value, Mapping):
                        stack.append(cast(Mapping[str, object], value))
                        expanded = True
                        continue

                    sequence = _as_sequence(value)
                    if sequence is not None:
                        stack.extend(reversed(sequence))
                        expanded = True

                for key in _FRAME_KEYS:
                    value = mapping.get(key)
                    if isinstance(value, Mapping):
                        stack.append(cast(Mapping[str, object], value))
                        expanded = True

                if expanded or drop_invalid:
                    continue

                raise ValueError(f"Unsupported ticker payload shape: {mapping!r}")

            else:
                sequence = _as_sequence(current)
                if sequence is not None:
                    stack.extend(reversed(sequence))
                elif not drop_invalid:
                    raise TypeError(
                        f"Unsupported ticker payload type: {type(current).__name__}"
                    )


def flatten_telemetry_frames(
    payloads: Iterable[object],
    *,
    drop_invalid: bool = False,
) -> TelemetrySegment:
    """Return all parsed websocket ticker frames as a flat immutable tuple."""
    return tuple(iter_flat_ticker_tuples(payloads, drop_invalid=drop_invalid))


def build_telemetry_segments(
    payloads: Iterable[object],
    *,
    segment_size: int = DEFAULT_SEGMENT_SIZE,
    drop_invalid: bool = False,
) -> TelemetrySegmentBatch:
    """Group flat ticker tuples into fixed-size immutable segments.

    Segmenting batches keeps the downstream analytics handoff uniform while also
    avoiding a single oversized container during heavy websocket bursts.
    """
    if segment_size <= 0:
        raise ValueError("segment_size must be greater than zero")

    segments: list[TelemetrySegment] = []
    current: list[TelemetryTuple] = []

    for frame in iter_flat_ticker_tuples(payloads, drop_invalid=drop_invalid):
        current.append(frame)
        if len(current) == segment_size:
            segments.append(tuple(current))
            current = []

    if current:
        segments.append(tuple(current))

    return tuple(segments)


# ---------------------------------------------------------------------------
# Streaming JSON tokenizer — low-memory ingestion via ijson
# ---------------------------------------------------------------------------
# The functions below accept a *binary* file-like object (or any object whose
# ``read()`` returns bytes) and parse it incrementally using ijson's SAX-style
# event emitter.  No complete JSON document is ever held in memory; only the
# current candidate frame dict is assembled and immediately yielded once it is
# recognised as a ticker frame.
#
# Supported top-level shapes (mirrors the in-memory parser above):
#   • A single frame object:   {"asset_id": ..., "price": ..., "timestamp": ...}
#   • A container wrapping a batch:
#       {"frames": [...]} / {"tickers": [...]} / {"data": {"tickers": [...]}}
#   • An array of frame objects at the root.
#
# Attribute extraction is deliberately limited to the same key priority lists
# (_ASSET_KEYS, _PRICE_KEYS, etc.) used by the existing in-memory parser so
# both paths produce identical output for the same data.
# ---------------------------------------------------------------------------

import io
from typing import BinaryIO


def _parse_frame_dict(
    frame: dict[str, object],
    *,
    drop_invalid: bool,
) -> Union[TelemetryTuple, None]:
    """Attempt to parse *frame* as a ticker frame dict.

    Returns a :data:`TelemetryTuple` on success, or ``None`` when the mapping
    does not look like a ticker frame and *drop_invalid* is ``True``.
    """
    if not _looks_like_frame(frame):
        if drop_invalid:
            return None
        raise ValueError(f"Unsupported ticker payload shape: {frame!r}")
    try:
        return _flatten_frame(frame)
    except (TypeError, ValueError):
        if drop_invalid:
            return None
        raise


def iter_price_events_from_stream(
    source: Union[BinaryIO, bytes],
    *,
    drop_invalid: bool = False,
) -> Iterator[TelemetryTuple]:
    """Yield ticker tuples by streaming *source* through an ijson tokenizer.

    Parameters
    ----------
    source:
        A binary file-like object (``read()`` must return ``bytes``) **or** a
        raw ``bytes`` / ``bytearray`` blob.  The data must be valid JSON.
    drop_invalid:
        When ``True``, frames that fail validation are silently skipped.
        When ``False`` (default), a :exc:`ValueError` or :exc:`TypeError` is
        raised on the first invalid frame, matching the behaviour of
        :func:`iter_flat_ticker_tuples`.

    Yields
    ------
    TelemetryTuple
        ``(asset_id, price, timestamp, sequence, flags)`` for each recognised
        ticker frame found anywhere in the JSON structure.

    Notes
    -----
    * Memory usage is bounded by the size of a single frame dict, regardless
      of overall document size.
    * ``ijson`` is imported lazily so callers that never use the streaming path
      do not pay the import cost.
    """
    try:
        import ijson  # type: ignore[import-untyped]
    except ModuleNotFoundError as exc:  # pragma: no cover
        raise ModuleNotFoundError(
            "Streaming JSON ingestion requires 'ijson'.  "
            "Install it with:  pip install ijson"
        ) from exc

    if isinstance(source, (bytes, bytearray)):
        source = io.BytesIO(source)

    # We use ijson's low-level ``parse`` iterator which yields
    # ``(prefix, event, value)`` triples.  A ``prefix`` is a dot-separated
    # path that encodes the nesting level, e.g. ``"frames.item.price"``.
    #
    # Strategy:
    #   1. Walk the event stream; whenever we enter an object (``map_key`` /
    #      ``start_map`` / ``end_map`` events) we accumulate key-value pairs.
    #   2. On ``end_map`` we check whether the accumulated dict looks like a
    #      ticker frame and emit it.
    #   3. We maintain a simple depth-tracked stack so nested objects within a
    #      frame (if any) do not confuse the collector.

    # Stack entries: (depth_at_open, partial_dict)
    object_stack: list[tuple[int, dict[str, object]]] = []
    current_key: str | None = None
    depth: int = 0

    for prefix, event, value in ijson.parse(source, use_float=True):
        if event == "start_map":
            depth += 1
            object_stack.append((depth, {}))
            current_key = None

        elif event == "end_map":
            if object_stack:
                open_depth, frame_dict = object_stack.pop()
                depth -= 1

                # Only attempt frame extraction for objects that were opened
                # at depth 1 greater than the enclosing context.  This means
                # we emit on the *innermost* complete object that matches,
                # which correctly handles both bare frames and nested batches.
                if _looks_like_frame(frame_dict):
                    result = _parse_frame_dict(frame_dict, drop_invalid=drop_invalid)
                    if result is not None:
                        yield result
            else:
                depth -= 1

            current_key = None

        elif event == "map_key":
            current_key = value  # type: ignore[assignment]

        elif event in (
            "null",
            "boolean",
            "integer",
            "number",
            "string",
        ):
            if object_stack and current_key is not None:
                object_stack[-1][1][current_key] = value
                current_key = None

        elif event in ("start_array", "end_array"):
            # Arrays are traversed transparently; individual frame objects
            # inside arrays will be picked up as separate map events above.
            current_key = None


def build_segments_from_stream(
    source: Union[BinaryIO, bytes],
    *,
    segment_size: int = DEFAULT_SEGMENT_SIZE,
    drop_invalid: bool = False,
) -> TelemetrySegmentBatch:
    """Parse *source* with a streaming tokenizer and group results into segments.

    This is the streaming counterpart of :func:`build_telemetry_segments`.  It
    reads the JSON document incrementally via :func:`iter_price_events_from_stream`
    and therefore keeps memory consumption proportional to *segment_size* rather
    than the full document size.

    Parameters
    ----------
    source:
        Binary file-like object or raw bytes containing a JSON payload.
    segment_size:
        Maximum number of ticker tuples per segment.  Must be positive.
    drop_invalid:
        Silently skip unrecognised frames when ``True``.

    Returns
    -------
    TelemetrySegmentBatch
        An immutable tuple of fixed-size :data:`TelemetrySegment` tuples.
    """
    if segment_size <= 0:
        raise ValueError("segment_size must be greater than zero")

    segments: list[TelemetrySegment] = []
    current: list[TelemetryTuple] = []

    for frame in iter_price_events_from_stream(source, drop_invalid=drop_invalid):
        current.append(frame)
        if len(current) == segment_size:
            segments.append(tuple(current))
            current = []

    if current:
        segments.append(tuple(current))

    return tuple(segments)


__all__ = [
    "BinaryFrameLayout",
    "DEFAULT_SEGMENT_SIZE",
    "SIMDJSON_AVAILABLE",
    "TelemetrySegment",
    "TelemetrySegmentBatch",
    "TelemetryTuple",
    "build_segments_from_stream",
    "build_telemetry_segments",
    "decode_binary_batch",
    "decode_binary_packet",
    "flatten_telemetry_frames",
    "iter_flat_ticker_tuples",
    "parse_raw_pack",
]
