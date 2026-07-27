# Implementation Plan: Partial Protobuf Decoding

## Overview

Add lightweight protobuf wire-format decoding functions to `src/serialization/encoders.py` and a property-based test suite in `tests/test_encoders.py`. The implementation is pure-Python, depends only on built-ins already present in the module, and requires no compiled message classes on the hot path.

## Tasks

- [ ] 1. Add `hypothesis` dependency and prepare the module
  - [ ] 1.1 Add `hypothesis>=6.0.0` to `requirements.txt`
    - Append the line `hypothesis>=6.0.0` to `requirements.txt`
    - _Requirements: 7.4_

  - [ ] 1.2 Extend `__all__` in `src/serialization/encoders.py`
    - Append `"proto_extract_fields"`, `"proto_extract_fields_from_slice"`, `"proto_get_price"`, `"proto_get_asset_id"`, `"proto_get_timestamp"`, `"proto_get_sequence"`, `"proto_get_feed_id"` to the existing `__all__` list
    - Do **not** add `_decode_varint` (module-private by convention)
    - _Requirements: 6.3_

- [ ] 2. Implement `_decode_varint` internal helper
  - [ ] 2.1 Write `_decode_varint(data: bytes, pos: int) -> tuple[int, int]` in `src/serialization/encoders.py`
    - Place it in the new "Partial Protobuf Decoding Layer" section, after the existing `StructPackEncoder` block
    - Reads up to 10 bytes; accumulates 7-bit groups with left-shifts
    - Raises `ValueError("Truncated varint at byte offset {pos}")` when the buffer is exhausted before a terminator byte
    - Raises `ValueError("Varint exceeds 64 bits at byte offset {pos}")` when `shift >= 70`
    - Returns `(value, new_pos)` on success
    - _Requirements: 6.2, 1.9_

- [ ] 3. Implement `proto_extract_fields` core scanner
  - [ ] 3.1 Write `proto_extract_fields(data: bytes, field_numbers: set[int]) -> dict[int, int | bytes]` in `src/serialization/encoders.py`
    - Return `{}` immediately when `data` is empty (Requirement 1.8)
    - Parse each field tag with `_decode_varint`; extract `field_num = tag >> 3` and `wt = tag & 0x07`
    - Wire type dispatch:
      - 0 (varint) → call `_decode_varint`; store `int`
      - 1 (64-bit LE) → read 8 bytes with `struct.unpack_from("<Q", ...)`; store `int`
      - 2 (length-delimited) → read length varint, then slice `bytes`; store `bytes`
      - 5 (32-bit LE) → read 4 bytes with `struct.unpack_from("<I", ...)`; store `int`
      - 3 or 4 → raise `ValueError(f"Unsupported wire type {wt} for field {field_num} at byte offset {pos}")`
    - Only store the decoded value when `field_num in field_numbers`; skip otherwise (still advance `pos` correctly)
    - Early-exit: break out of the scan loop when `len(result) == len(field_numbers)`
    - All truncation errors must include the byte offset in the message
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8, 1.9, 6.1_

  - [ ]* 3.2 Write property test `test_partial_protobuf_decoding_telemetry_frame_round_trip`
    - **Property 1: TelemetryFrame full field round-trip**
    - **Validates: Requirements 1.2, 1.4, 1.5, 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 5.1, 5.2, 5.3**
    - File: `tests/test_encoders.py`
    - Use `@given` with `st.integers` for numeric fields and `st.binary(min_size=1, max_size=8)` for `asset_id`
    - Encode with `stellarflow_channels_pb2.TelemetryFrame`, call `proto_extract_fields(data, {1,2,3,4,5,6,7})`
    - Assert all seven extracted values match the pb2 message attributes
    - Also assert all five `proto_get_*` helpers return values matching pb2 attributes
    - Use `settings(max_examples=100)`
    - Add comment: `# Feature: partial-protobuf-decoding, Property 1: TelemetryFrame full field round-trip`

- [ ] 4. Implement `proto_extract_fields_from_slice`
  - [ ] 4.1 Write `proto_extract_fields_from_slice(data: bytes, offset: int, length: int, field_numbers: set[int]) -> dict[int, int | bytes]` in `src/serialization/encoders.py`
    - Validate: raise `ValueError(f"offset must be non-negative, got {offset}")` if `offset < 0`
    - Validate: raise `ValueError(f"Slice [{offset}:{offset+length}] out of range for buffer of length {len(data)}")` if `offset + length > len(data)`
    - Build a `memoryview(data)[offset : offset + length]` for zero-copy slicing
    - Delegate to `proto_extract_fields(bytes(view), field_numbers)` and return its result
    - _Requirements: 4.1, 4.2, 4.3, 4.4_

  - [ ]* 4.2 Write property test `test_partial_protobuf_decoding_slice_equivalence`
    - **Property 3: Slice equivalence**
    - **Validates: Requirements 4.1, 4.2**
    - File: `tests/test_encoders.py`
    - Generate a random `TelemetryFrame`, encode it, prepend/append random padding bytes
    - Generate a valid `(offset, length)` sub-range covering the encoded frame bytes
    - Assert `proto_extract_fields_from_slice(padded, offset, length, {2})` equals `proto_extract_fields(encoded, {2})`
    - Use `settings(max_examples=100)`
    - Add comment: `# Feature: partial-protobuf-decoding, Property 3: Slice equivalence`

- [ ] 5. Implement single-field extractor wrappers
  - [ ] 5.1 Write `proto_get_price`, `proto_get_asset_id`, `proto_get_timestamp`, `proto_get_sequence`, `proto_get_feed_id` in `src/serialization/encoders.py`
    - Each is a thin one-liner delegating to `proto_extract_fields` with a singleton field-number set
    - Return types: `int | None` for numeric fields; `bytes | None` for `proto_get_asset_id`
    - Return `None` (via `.get(...)`) when the field is absent — do not raise
    - Field number mapping: `proto_get_asset_id → {1}`, `proto_get_price → {2}`, `proto_get_timestamp → {4}`, `proto_get_sequence → {5}`, `proto_get_feed_id → {7}`
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

  - [ ]* 5.2 Write property test `test_partial_protobuf_decoding_heartbeat_round_trip`
    - **Property 2: Multi-message-type round-trip (Heartbeat variant)**
    - **Validates: Requirements 3.1, 3.2, 3.3, 3.4, 5.4**
    - File: `tests/test_encoders.py`
    - Use `@given` with `st.text()` for `service`/`status`, `st.integers(min_value=0)` for `timestamp`, `st.booleans()` for `alive`
    - Encode with `stellarflow_channels_pb2.Heartbeat`, call `proto_extract_fields(data, {1,2,3,4})`
    - Assert extracted values match pb2 `Heartbeat` attributes
    - Use `settings(max_examples=100)`
    - Add comment: `# Feature: partial-protobuf-decoding, Property 2: Multi-message-type round-trip`

- [ ] 6. Write example-based and edge-case tests
  - [ ] 6.1 Write `test_partial_protobuf_decoding_empty_buffer` in `tests/test_encoders.py`
    - Call `proto_extract_fields(b"", {1, 2})` and assert result equals `{}`
    - _Requirements: 1.8, 7.5_

  - [ ] 6.2 Write `test_partial_protobuf_decoding_absent_field_omitted` in `tests/test_encoders.py`
    - Encode a `Heartbeat`, request field `{99}` (non-existent), assert `99` is absent from the result dict
    - Also request `{2}` (timestamp — present) and assert it is present with the correct value
    - _Requirements: 1.3, 7.5_

  - [ ] 6.3 Write `test_partial_protobuf_decoding_malformed_raises` in `tests/test_encoders.py`
    - Pass `b"\x0a"` (length-delimited tag with no length byte) and assert `ValueError` is raised
    - _Requirements: 1.9, 7.5_

  - [ ] 6.4 Write `test_partial_protobuf_decoding_truncated_value_raises` in `tests/test_encoders.py`
    - Encode a `TelemetryFrame`, drop the last byte, assert `ValueError` is raised
    - _Requirements: 1.9, 7.5_

  - [ ] 6.5 Write `test_partial_protobuf_decoding_slice_invalid_range` in `tests/test_encoders.py`
    - Assert `ValueError` for `proto_extract_fields_from_slice(b"abc", -1, 2, {1})`
    - Assert `ValueError` for `proto_extract_fields_from_slice(b"abc", 2, 5, {1})`
    - _Requirements: 4.3, 7.6_

  - [ ] 6.6 Write `test_partial_protobuf_decoding_slice_matches_direct` in `tests/test_encoders.py`
    - Prepend 10 zero bytes to a known `TelemetryFrame` encoding
    - Assert `proto_extract_fields_from_slice(prefixed, 10, len(encoded), {2})` equals `proto_extract_fields(encoded, {2})`
    - _Requirements: 4.2, 7.6_

  - [ ] 6.7 Write `test_partial_protobuf_decoding_single_field_extractors` in `tests/test_encoders.py`
    - Build one concrete `TelemetryFrame` pb2 message with known field values
    - Assert each `proto_get_*` helper returns the expected value
    - Assert `proto_get_price` returns `None` on a buffer that lacks field 2 (e.g., a `Heartbeat` encoding)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 7.3_

- [ ] 7. Checkpoint — Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
  - Run: `pytest tests/test_encoders.py -k test_partial_protobuf_decoding`

## Notes

- Tasks marked with `*` are optional and can be skipped for a faster MVP
- The design is pure-Python; no `stellarflow_channels_pb2` import is permitted at module level in `encoders.py`
- `_decode_varint` is module-private and intentionally excluded from `__all__`
- Wire types 3 and 4 are deprecated protobuf group types and must raise `ValueError`
- All error messages must include the byte offset to aid corruption debugging
- Property tests use `stellarflow_channels_pb2` only inside the test file, never in the implementation module

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1", "1.2"] },
    { "id": 1, "tasks": ["2.1"] },
    { "id": 2, "tasks": ["3.1"] },
    { "id": 3, "tasks": ["4.1", "5.1"] },
    { "id": 4, "tasks": ["3.2", "4.2", "5.2", "6.1", "6.2", "6.3", "6.4", "6.5", "6.6", "6.7"] }
  ]
}
```
