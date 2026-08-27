# Design Document — Partial Protobuf Decoding

## Overview

This feature adds a set of lightweight wire-format decoding functions to
`src/serialization/encoders.py` that extract individual fields from raw
protobuf-encoded byte strings without instantiating any compiled message
class. The hot path is entirely pure-Python, relying only on `bytes`,
`memoryview`, and `struct` — the same built-ins already used in the
surrounding encoder code.

### Motivation

StellarFlow's telemetry pipeline emits high-frequency `TelemetryFrame`
protobuf messages. Downstream consumers (price-feed processors, index
calculators, backpressure monitors) often need only one or two fields
(e.g., `price`, `timestamp`) but currently pay the full cost of
`ParseFromString` on a compiled message class. Profiling shows that
repeated full deserialization is the dominant allocation source on the hot
path. Partial decoding bypasses the message object entirely, reading
directly from wire bytes.

### Scope

- **In scope**: `_decode_varint`, `proto_extract_fields`,
  `proto_extract_fields_from_slice`, and the five single-field extractors
  (`proto_get_price`, `proto_get_asset_id`, `proto_get_timestamp`,
  `proto_get_sequence`, `proto_get_feed_id`) — all added to
  `src/serialization/encoders.py`.
- **Out of scope**: Writing protobuf, repeated/nested message support,
  ZigZag-decoded signed integers (sint32/sint64), or changes to
  `proto_broker.py`.

---

## Architecture

The new functions sit inside `src/serialization/encoders.py` as a
self-contained decoding layer beneath the existing struct-pack encoders.
They have no run-time dependency on `stellarflow_channels_pb2` or any
`google.protobuf` symbol.

```
┌─────────────────────────────────────────────────────────────────┐
│  src/serialization/encoders.py                                  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ Existing: TelemetryEncoder, StructPackEncoder,            │  │
│  │           pack_frame / unpack_frame (struct-based)        │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │ NEW: Partial Protobuf Decoding Layer                      │  │
│  │                                                           │  │
│  │  _decode_varint(data, pos) → (value, new_pos)            │  │
│  │                                                           │  │
│  │  proto_extract_fields(data, field_numbers)               │  │
│  │    → dict[int, int | bytes]                              │  │
│  │                                                           │  │
│  │  proto_extract_fields_from_slice(data, offset, length,   │  │
│  │    field_numbers) → dict[int, int | bytes]               │  │
│  │                                                           │  │
│  │  proto_get_price(data)     → int | None                  │  │
│  │  proto_get_asset_id(data)  → bytes | None                │  │
│  │  proto_get_timestamp(data) → int | None                  │  │
│  │  proto_get_sequence(data)  → int | None                  │  │
│  │  proto_get_feed_id(data)   → int | None                  │  │
│  └───────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
         ▲                                       ▲
         │  used by                              │  used by
  proto_broker.py                        tests/test_encoders.py
  (full deserialization, optional)       (round-trip verification
                                          via stellarflow_channels_pb2)
```

The decoding layer does **not** import from `proto_broker.py` or
`stellarflow_channels_pb2`. The test suite imports `stellarflow_channels_pb2`
to construct reference payloads and verify correctness.

---

## Components and Interfaces

### `_decode_varint(data: bytes, pos: int) -> tuple[int, int]`

Internal helper. Reads a protobuf varint from `data` starting at `pos`.
Returns `(value, new_pos)` where `new_pos` is the index of the first byte
after the varint.

- Reads up to 10 bytes (the maximum for a 64-bit varint in protobuf).
- Raises `ValueError` if the buffer is exhausted before a terminator byte
  (MSB == 0) is found.

```python
def _decode_varint(data: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= len(data):
            raise ValueError(
                f"Truncated varint at byte offset {pos}"
            )
        b = data[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not (b & 0x80):
            return result, pos
        shift += 7
        if shift >= 70:
            raise ValueError(
                f"Varint exceeds 64 bits at byte offset {pos}"
            )
```

---

### `proto_extract_fields(data: bytes, field_numbers: set[int]) -> dict[int, int | bytes]`

Core public API. Scans `data` from byte 0, parsing each field tag and
skipping fields whose number is not in `field_numbers`. Returns when the
buffer is exhausted or all requested fields have been found.

**Wire type dispatch:**

| Wire type | Protobuf types | Action |
|-----------|----------------|--------|
| 0 (varint) | int32, int64, uint32, uint64, bool, enum | decode varint → `int` |
| 1 (64-bit) | fixed64, sfixed64, double | read 8 bytes LE → `int` |
| 2 (length-delimited) | string, bytes, embedded messages, packed repeated | read length varint, return raw `bytes` slice |
| 5 (32-bit) | fixed32, sfixed32, float | read 4 bytes LE → `int` |
| 3, 4 | deprecated groups | raise `ValueError` |

**Error handling:**
- Empty `data` → return `{}` immediately.
- Buffer exhausted mid-tag or mid-field → raise `ValueError(f"... at byte offset {pos}")`.
- Unsupported wire type (3 or 4) → raise `ValueError`.

**Early exit optimisation**: if `len(result) == len(field_numbers)` before
the buffer is exhausted, the scan loop stops.

---

### `proto_extract_fields_from_slice(data: bytes, offset: int, length: int, field_numbers: set[int]) -> dict[int, int | bytes]`

Slice variant. Validates `offset` and `length`, then wraps `data` in a
`memoryview` to obtain a zero-copy view of the sub-range, and delegates to
`proto_extract_fields`.

```
if offset < 0 or offset + length > len(data):
    raise ValueError(...)
view = memoryview(data)[offset : offset + length]
return proto_extract_fields(bytes(view), field_numbers)
```

Using `memoryview` avoids a buffer copy for large payloads on the hot
path. The inner call receives `bytes(view)` because `_decode_varint` and
the length-delimited field slicer use `bytes` indexing. For very large
slices a future optimisation could accept `memoryview` throughout, but
this is deferred until profiling indicates it is necessary.

---

### Single-field extractors

Each is a thin wrapper around `proto_extract_fields`:

```python
def proto_get_price(data: bytes) -> int | None:
    result = proto_extract_fields(data, {2})
    return result.get(2)  # type: int | None

def proto_get_asset_id(data: bytes) -> bytes | None:
    result = proto_extract_fields(data, {1})
    return result.get(1)  # type: bytes | None

def proto_get_timestamp(data: bytes) -> int | None:
    result = proto_extract_fields(data, {4})
    return result.get(4)

def proto_get_sequence(data: bytes) -> int | None:
    result = proto_extract_fields(data, {5})
    return result.get(5)

def proto_get_feed_id(data: bytes) -> int | None:
    result = proto_extract_fields(data, {7})
    return result.get(7)
```

These are intentionally trivial. The early-exit optimisation in
`proto_extract_fields` means the scan loop stops as soon as field 2 (or
whichever single field is requested) is found, so the per-call cost is
proportional to the number of preceding bytes in the message, not the
total message length.

---

## Data Models

### Protobuf wire format recap

Every field in a protobuf message is encoded as:

```
[ tag varint ] [ value bytes ]
```

where `tag = (field_number << 3) | wire_type`.

For `TelemetryFrame` the field-to-wire-type mapping is:

| Field | Field # | Proto type | Wire type |
|-------|---------|------------|-----------|
| asset_id | 1 | bytes | 2 (length-delimited) |
| price | 2 | int64 | 0 (varint) |
| volume | 3 | int64 | 0 (varint) |
| timestamp | 4 | uint64 | 0 (varint) |
| sequence | 5 | uint32 | 0 (varint) |
| flags | 6 | uint32 | 0 (varint) |
| feed_id | 7 | uint32 | 0 (varint) |

For `Heartbeat`:

| Field | Field # | Proto type | Wire type |
|-------|---------|------------|-----------|
| service | 1 | string | 2 |
| timestamp | 2 | uint64 | 0 |
| alive | 3 | bool | 0 |
| status | 4 | string | 2 |

For `MessageEnvelope`:

| Field | Field # | Proto type | Wire type |
|-------|---------|------------|-----------|
| channel | 1 | string | 2 |
| content_type | 2 | string | 2 |
| payload | 3 | bytes | 2 |
| timestamp | 4 | uint64 | 0 |
| source | 5 | string | 2 |

### Return value types

`proto_extract_fields` returns `dict[int, int | bytes]`:

- Wire type 0 and fixed-width types → `int` (always unsigned; caller is
  responsible for interpreting signed fields like `int64 price`).
- Wire type 2 → `bytes` (raw content, not decoded as UTF-8 even for
  `string` proto fields, because the partial decoder is message-type agnostic).

### `__all__` additions

The following names will be appended to `__all__` in `encoders.py`:

```python
"proto_extract_fields",
"proto_extract_fields_from_slice",
"proto_get_price",
"proto_get_asset_id",
"proto_get_timestamp",
"proto_get_sequence",
"proto_get_feed_id",
```

`_decode_varint` is intentionally excluded (module-private).

---

## Correctness Properties

*A property is a characteristic or behavior that should hold true across
all valid executions of a system — essentially, a formal statement about
what the system should do. Properties serve as the bridge between
human-readable specifications and machine-verifiable correctness
guarantees.*

### Property 1: TelemetryFrame full field round-trip

*For any* valid `TelemetryFrame` (arbitrary `asset_id`, `price`, `volume`,
`timestamp`, `sequence`, `flags`, `feed_id` values), encoding the frame
with `stellarflow_channels_pb2.TelemetryFrame` and then calling
`proto_extract_fields(data, {1, 2, 3, 4, 5, 6, 7})` SHALL return a dict
whose values match every corresponding attribute of the fully-deserialized
pb2 message.

This property also covers the individual extractor functions: for any
valid encoding, `proto_get_price(data)` equals `pb2_msg.price`,
`proto_get_asset_id(data)` equals `pb2_msg.asset_id`, etc.

**Validates: Requirements 1.2, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 5.1, 5.2, 5.3**

---

### Property 2: Multi-message-type round-trip

*For any* valid `Heartbeat` message (arbitrary `service`, `timestamp`,
`alive`, `status`), encoding with pb2 and then calling
`proto_extract_fields(data, {1, 2, 3, 4})` SHALL return values that match
every attribute of the fully-deserialized pb2 `Heartbeat`.

The same property holds for `PriceUpdate` with field numbers `{1, 2, 3,
4, 5, 6, 7}` and for `MessageEnvelope` with field numbers `{1, 2, 3, 4,
5}`.

This validates that the decoder is message-type agnostic — the same
function works correctly regardless of which schema produced the bytes.

**Validates: Requirements 3.1, 3.2, 3.3, 3.4, 5.4**

---

### Property 3: Slice equivalence

*For any* protobuf-encoded `bytes` buffer `data`, non-negative integer
`offset`, and non-negative integer `length` such that
`offset + length <= len(data)`, calling
`proto_extract_fields_from_slice(data, offset, length, field_numbers)`
SHALL return the same result as
`proto_extract_fields(data[offset:offset+length], field_numbers)` for any
`field_numbers`.

**Validates: Requirements 4.1, 4.2**

---

## Error Handling

| Condition | Behaviour |
|-----------|-----------|
| `data = b""` | Return `{}` immediately |
| Buffer truncated mid-varint | `ValueError(f"Truncated varint at byte offset {pos}")` |
| Buffer truncated mid-field value | `ValueError(f"Truncated field {field_num} (wire type {wt}) at byte offset {pos}: need {n} bytes, {available} available")` |
| Unsupported wire type (3 or 4) | `ValueError(f"Unsupported wire type {wt} for field {field_num} at byte offset {pos}")` |
| Varint exceeds 64 bits | `ValueError(f"Varint exceeds 64 bits at byte offset {pos}")` |
| `offset < 0` in slice variant | `ValueError(f"offset must be non-negative, got {offset}")` |
| `offset + length > len(data)` in slice variant | `ValueError(f"Slice [{offset}:{offset+length}] out of range for buffer of length {len(data)}")` |

All `ValueError` messages include the byte offset of the error to aid
debugging of corrupt payloads.

---

## Testing Strategy

### Framework

- **pytest** (already used in the project) for test execution.
- **hypothesis** for property-based testing. It is not currently in
  `requirements.txt` and must be added: `hypothesis>=6.0.0`.
- Reference encodings are produced using `stellarflow_channels_pb2`
  (compiled from `proto/stellarflow_channels.proto`) — available via the
  existing `proto_broker` import chain in tests only.

### Test file

`tests/test_encoders.py` (new file). Run with:

```
pytest tests/test_encoders.py -k test_partial_protobuf_decoding
```

### Test organisation

#### Property-based tests (hypothesis)

Each property below is implemented as a single `@given`-decorated test
function with a minimum of 100 examples (`settings(max_examples=100)`).

**`test_partial_protobuf_decoding_telemetry_frame_round_trip`**  
Generators: `st.integers` for numeric fields, `st.binary(min_size=1, max_size=8)` for `asset_id`.  
Assertion: `proto_extract_fields(encoded, {1,2,3,4,5,6,7})` values equal pb2 message attributes, and all five `proto_get_*` helpers return matching values.  
*Implements Property 1.*

**`test_partial_protobuf_decoding_heartbeat_round_trip`**  
Generators: `st.text()` for string fields, `st.integers(min_value=0)` for `timestamp`, `st.booleans()` for `alive`.  
Assertion: `proto_extract_fields(encoded, {1,2,3,4})` values match pb2 `Heartbeat` attributes.  
*Implements Property 2 (Heartbeat variant).*

**`test_partial_protobuf_decoding_slice_equivalence`**  
Generators: encode a random `TelemetryFrame` to bytes, generate a random valid `(offset, length)` sub-range.  
Assertion: `proto_extract_fields_from_slice(data_with_prefix, offset, length, {2})` equals `proto_extract_fields(data[offset:offset+length], {2})`.  
*Implements Property 3.*

#### Example-based tests

**`test_partial_protobuf_decoding_absent_field_omitted`**  
Encode a `Heartbeat` (no `price` field), request `{2}` (which is `timestamp` in Heartbeat — present), and `{99}` (non-existent). Assert `99` is absent from the result.

**`test_partial_protobuf_decoding_empty_buffer`**  
Call `proto_extract_fields(b"", {1, 2})`. Assert result `== {}`.

**`test_partial_protobuf_decoding_malformed_raises`**  
Pass `b"\x0a"` (a length-delimited tag with no length byte following) and assert `ValueError` is raised.

**`test_partial_protobuf_decoding_truncated_value_raises`**  
Encode a `TelemetryFrame`, truncate the last field's value bytes, assert `ValueError`.

**`test_partial_protobuf_decoding_slice_invalid_range`**  
Call `proto_extract_fields_from_slice(b"abc", -1, 2, {1})` → `ValueError`.  
Call `proto_extract_fields_from_slice(b"abc", 2, 5, {1})` → `ValueError`.

**`test_partial_protobuf_decoding_slice_matches_direct`**  
Prepend 10 zero bytes before a known `TelemetryFrame` encoding. Assert that `proto_extract_fields_from_slice(prefixed, 10, len(encoded), {2})` equals `proto_extract_fields(encoded, {2})`.

### Unit tests (no hypothesis)

- Test each `proto_get_*` function with a concrete known encoding and
  confirm the returned value matches the expected integer or bytes.
- Test `proto_get_price` on a buffer that lacks field 2; assert `None`.

### Test tagging convention

Each property-based test carries a comment identifying the design property
it verifies:

```python
# Feature: partial-protobuf-decoding, Property 1: TelemetryFrame full field round-trip
```

### Adding hypothesis to the project

Add to `requirements.txt`:

```
hypothesis>=6.0.0
```

No additional pytest plugin is required; `hypothesis` integrates directly
with `pytest` via its `@given` decorator.
