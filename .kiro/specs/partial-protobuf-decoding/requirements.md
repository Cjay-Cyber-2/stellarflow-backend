# Requirements Document

## Introduction

StellarFlow telemetry pipelines transmit high-frequency binary packets encoded as Protocol Buffer messages. Full protobuf deserialization introduces unnecessary CPU overhead when a consumer only needs one or two fields from a large message (e.g., extracting `price` from a `TelemetryFrame` or `timestamp` from a `Heartbeat`). This feature introduces lightweight partial-decoding functions directly in `src/serialization/encoders.py` that can extract individual fields from raw protobuf wire bytes without deserializing the entire message structure. The implementation relies on manual protobuf wire-format parsing (varint, fixed-width, and length-delimited field tags) so that no compiled message class instantiation is required for the hot path.

## Glossary

- **PartialDecoder**: The set of functions added to `src/serialization/encoders.py` that implement field-level protobuf wire-format extraction.
- **Wire_Format**: The binary encoding defined by the Protocol Buffer specification: each field is encoded as a tag (field number + wire type) followed by the field value.
- **Tag**: A varint-encoded value equal to `(field_number << 3) | wire_type` that precedes each field value in a protobuf binary payload.
- **Varint**: A variable-length integer encoding used by protobuf for wire types 0 (int32, int64, uint32, uint64, sint32, sint64, bool, enum).
- **Wire_Type**: A 3-bit discriminant embedded in each Tag indicating how many bytes the following value occupies (0 = varint, 1 = 64-bit, 2 = length-delimited, 5 = 32-bit).
- **Field_Number**: The integer identifier assigned to a field in a `.proto` schema (e.g., `price = 2` in `TelemetryFrame`).
- **TelemetryFrame**: The protobuf message `stellarflow.channels.TelemetryFrame` with field numbers asset_id=1, price=2, volume=3, timestamp=4, sequence=5, flags=6, feed_id=7.
- **PriceUpdate**: The protobuf message `stellarflow.channels.PriceUpdate` with the same field layout as `TelemetryFrame` but with `asset_id` as `string` (wire type 2) instead of `bytes`.
- **Heartbeat**: The protobuf message `stellarflow.channels.Heartbeat` with fields service=1 (string), timestamp=2 (uint64), alive=3 (bool), status=4 (string).
- **MessageEnvelope**: The protobuf message `stellarflow.channels.MessageEnvelope` with fields channel=1 (string), content_type=2 (string), payload=3 (bytes), timestamp=4 (uint64), source=5 (string).
- **TelemetryBundle**: The protobuf message `stellarflow.channels.TelemetryBundle` with fields frames=1 (repeated TelemetryFrame) and bundle_sequence=2 (uint64).
- **Slice**: A contiguous sub-range of bytes from a protobuf-encoded payload, possibly starting mid-stream rather than at byte 0.
- **Selector**: A sequence of one or more `Field_Number` integers passed by the caller to specify which fields to extract.

---

## Requirements

### Requirement 1: Core Partial Decoding API

**User Story:** As a telemetry consumer, I want to extract one or more named fields from a raw protobuf payload without deserializing the full message, so that CPU time is not spent parsing fields I do not need.

#### Acceptance Criteria

1. THE PartialDecoder SHALL expose a function `proto_extract_fields(data: bytes, field_numbers: set[int]) -> dict[int, int | bytes]` in `src/serialization/encoders.py` that accepts a raw protobuf-encoded `bytes` buffer and a set of integer field numbers to extract.
2. WHEN `proto_extract_fields` is called with a valid protobuf buffer, THE PartialDecoder SHALL return a `dict` mapping each requested `Field_Number` to its decoded value, skipping all unrequested fields.
3. WHEN a requested `Field_Number` is absent from the buffer, THE PartialDecoder SHALL omit that field number from the returned dict rather than inserting a default or `None`.
4. WHEN `proto_extract_fields` encounters a varint-encoded field (Wire_Type 0), THE PartialDecoder SHALL decode the value as an unsigned 64-bit integer.
5. WHEN `proto_extract_fields` encounters a length-delimited field (Wire_Type 2), THE PartialDecoder SHALL return the raw `bytes` content of that field without further decoding.
6. WHEN `proto_extract_fields` encounters a 64-bit fixed-width field (Wire_Type 1), THE PartialDecoder SHALL decode the value as a little-endian unsigned 64-bit integer.
7. WHEN `proto_extract_fields` encounters a 32-bit fixed-width field (Wire_Type 5), THE PartialDecoder SHALL decode the value as a little-endian unsigned 32-bit integer.
8. IF `data` is an empty `bytes` object, THEN THE PartialDecoder SHALL return an empty `dict`.
9. IF `data` contains a malformed Tag or truncated field value, THEN THE PartialDecoder SHALL raise a `ValueError` with a descriptive message identifying the byte offset of the error.

---

### Requirement 2: TelemetryFrame Field Extractors

**User Story:** As a price-feed processor, I want single-field extraction functions for each `TelemetryFrame` field, so that per-field hot paths avoid the overhead of building a full result dict.

#### Acceptance Criteria

1. THE PartialDecoder SHALL expose `proto_get_price(data: bytes) -> int | None` that returns the `price` field (field number 2) from a `TelemetryFrame`-encoded buffer, or `None` if absent.
2. THE PartialDecoder SHALL expose `proto_get_asset_id(data: bytes) -> bytes | None` that returns the `asset_id` field (field number 1) as raw bytes, or `None` if absent.
3. THE PartialDecoder SHALL expose `proto_get_timestamp(data: bytes) -> int | None` that returns the `timestamp` field (field number 4) as an unsigned integer, or `None` if absent.
4. THE PartialDecoder SHALL expose `proto_get_sequence(data: bytes) -> int | None` that returns the `sequence` field (field number 5) as an unsigned integer, or `None` if absent.
5. THE PartialDecoder SHALL expose `proto_get_feed_id(data: bytes) -> int | None` that returns the `feed_id` field (field number 7) as an unsigned integer, or `None` if absent.
6. WHEN any single-field extractor is called with a valid buffer that contains the target field, THE PartialDecoder SHALL return the correct decoded value without raising an exception.
7. WHEN any single-field extractor is called with a buffer that does not contain the target field, THE PartialDecoder SHALL return `None` without raising an exception.

---

### Requirement 3: Multi-Message Type Support

**User Story:** As a message bus subscriber, I want partial decoding to work across all StellarFlow protobuf message types, so that I can apply field extraction to `Heartbeat`, `MessageEnvelope`, and `PriceUpdate` payloads in addition to `TelemetryFrame`.

#### Acceptance Criteria

1. WHEN `proto_extract_fields` is called with a `Heartbeat`-encoded buffer and `field_numbers={2}`, THE PartialDecoder SHALL return `{2: <timestamp_value>}` where the value matches the encoded `timestamp` field.
2. WHEN `proto_extract_fields` is called with a `MessageEnvelope`-encoded buffer and `field_numbers={1, 4}`, THE PartialDecoder SHALL return a dict containing `channel` bytes (field 1) and `timestamp` integer (field 4).
3. WHEN `proto_extract_fields` is called with a `PriceUpdate`-encoded buffer and `field_numbers={1, 2}`, THE PartialDecoder SHALL return `asset_id` bytes (field 1) and `price` integer (field 2).
4. THE PartialDecoder SHALL not require knowledge of the specific message type — the same `proto_extract_fields` function SHALL operate on any valid protobuf-encoded bytes from any message type defined in `stellarflow_channels.proto`.

---

### Requirement 4: Partial Slice Decoding

**User Story:** As a network buffer consumer, I want to extract fields from a byte slice that begins at a non-zero offset, so that I can process sub-ranges of larger buffers without copying data.

#### Acceptance Criteria

1. THE PartialDecoder SHALL expose `proto_extract_fields_from_slice(data: bytes, offset: int, length: int, field_numbers: set[int]) -> dict[int, int | bytes]` that decodes only the `length` bytes starting at `offset` within `data`.
2. WHEN `proto_extract_fields_from_slice` is called with a valid offset and length, THE PartialDecoder SHALL return the same result as calling `proto_extract_fields` on `data[offset:offset+length]`.
3. IF `offset` is negative or `offset + length` exceeds `len(data)`, THEN THE PartialDecoder SHALL raise a `ValueError` describing the invalid range.
4. WHERE performance is critical, THE PartialDecoder SHALL implement `proto_extract_fields_from_slice` using a `memoryview` to avoid copying the underlying buffer.

---

### Requirement 5: Round-Trip Consistency

**User Story:** As a quality assurance engineer, I want to verify that partial decoding returns the same field values as full deserialization, so that I can trust the lightweight decoder produces correct results.

#### Acceptance Criteria

1. FOR ALL valid `TelemetryFrame` protobuf payloads, the value returned by `proto_get_price(data)` SHALL equal the `price` attribute of the fully-deserialized `TelemetryFrame` protobuf message produced by `ProtoBroker.deserialize(data, TelemetryFrame_pb2)`.
2. FOR ALL valid `TelemetryFrame` protobuf payloads, the value returned by `proto_get_asset_id(data)` SHALL equal the `asset_id` attribute of the fully-deserialized message.
3. FOR ALL valid `TelemetryFrame` protobuf payloads, `proto_extract_fields(data, {1, 2, 3, 4, 5, 6, 7})` SHALL return field values that match every attribute of the fully-deserialized message.
4. FOR ALL valid `Heartbeat` protobuf payloads, `proto_extract_fields(data, {1, 2, 3, 4})` SHALL return field values that match every attribute of the fully-deserialized `Heartbeat` message.

---

### Requirement 6: No Full Deserialization Dependency

**User Story:** As a performance engineer, I want partial decoding functions to avoid instantiating compiled protobuf message objects, so that the decoder operates with minimal per-call allocation overhead.

#### Acceptance Criteria

1. THE PartialDecoder SHALL implement `proto_extract_fields` using only Python built-ins (`bytes`, `int`, `memoryview`, `struct`) and SHALL NOT call `ParseFromString`, `FromString`, or any method on a compiled `google.protobuf.message.Message` subclass inside the extraction loop.
2. THE PartialDecoder SHALL implement all varint decoding using an inline loop or a dedicated `_decode_varint(data: bytes, pos: int) -> tuple[int, int]` helper that returns `(value, new_position)`.
3. THE PartialDecoder functions SHALL be importable from `src/serialization/encoders.py` without importing `stellarflow_channels_pb2` or `proto_broker` at module level.

---

### Requirement 7: Test Coverage

**User Story:** As a developer, I want a test suite that validates the partial decoding functions, so that regressions are caught automatically and the acceptance criteria are verified.

#### Acceptance Criteria

1. THE test suite SHALL be located at `tests/test_encoders.py` and SHALL contain at least one test function named `test_partial_protobuf_decoding` (or a class containing methods prefixed `test_partial_protobuf_decoding`).
2. WHEN `pytest tests/test_encoders.py -k test_partial_protobuf_decoding` is executed in the project root, THE test suite SHALL pass with zero failures.
3. THE test suite SHALL use `stellarflow_channels_pb2` to construct reference protobuf-encoded payloads and SHALL verify that partial-decoding results match full-deserialization results (round-trip consistency, Requirement 5).
4. THE test suite SHALL include at least one property-based test using `hypothesis` that generates arbitrary valid field values for `TelemetryFrame` and asserts that `proto_extract_fields` returns values consistent with full deserialization (round-trip property).
5. THE test suite SHALL include tests for the empty-buffer case (Requirement 1 AC8), the absent-field case (Requirement 1 AC3), and the malformed-data case (Requirement 1 AC9).
6. THE test suite SHALL include at least one test for `proto_extract_fields_from_slice` verifying that slice decoding matches direct buffer decoding (Requirement 4 AC2).
