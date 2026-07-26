"""Tests for src.serialization.encoders — msgpack binary serializer (Issue #628)."""
import json
import unittest

from src.serialization.encoders import (
    TelemetryFrame,
    RingBufferMetric,
    BackpressureMetric,
    FLAG_LIVE,
    FLAG_STALE,
    FLAG_ANOMALY,
    FLAG_SYNTHETIC,
    FLAG_HALTED,
    FRAME_SIZE,
    TelemetryEncoder,
    StructPackEncoder,
    msgpack_encode,
    msgpack_decode,
    msgpack_encode_telemetry_frame,
    msgpack_decode_telemetry_frame,
    msgpack_encode_ring_buffer_metric,
    msgpack_decode_ring_buffer_metric,
    msgpack_encode_backpressure_metric,
    msgpack_decode_backpressure_metric,
)


class TestMsgpackEncoding(unittest.TestCase):
    """Verify msgpack encode/decode round-trips and size reductions vs JSON."""

    # ---- helpers ----

    def _sample_frame(self, **overrides) -> TelemetryFrame:
        defaults = dict(
            asset_id=b"NGN/XLM",
            price=15_000_000,
            volume=8_200_000,
            timestamp=1_700_000_000_000,
            sequence=42,
            flags=FLAG_LIVE | FLAG_ANOMALY,
            feed_id=3,
        )
        defaults.update(overrides)
        return TelemetryFrame(**defaults)

    def _sample_ring_buffer(self, **overrides) -> RingBufferMetric:
        defaults = dict(
            size=128,
            capacity=1024,
            utilization=7_500_000,
            total_enqueued=9_999_999,
            total_dequeued=9_999_871,
            enqueue_failures=128,
            dequeue_failures=0,
            avg_latency_us=1_500_000,
            peak_latency_us=12_000_000,
            batches_processed=4_000,
        )
        defaults.update(overrides)
        return RingBufferMetric(**defaults)

    def _sample_backpressure(self, **overrides) -> BackpressureMetric:
        defaults = dict(
            queue_length=512,
            max_capacity=2048,
            saturation=3_330_000,
            dropped_packets=7,
            slowed_ingestions=42,
            avg_processing_us=2_000_000,
        )
        defaults.update(overrides)
        return BackpressureMetric(**defaults)

    # ---- generic msgpack round-trip ----

    def test_msgpack_roundtrip_basic_types(self) -> None:
        for obj in [
            {"a": 1, "b": [2, 3]},
            [1, 2, 3],
            "hello",
            42,
            3.14,
            True,
            None,
            b"\x00\x01\x02",
        ]:
            with self.subTest(obj=obj):
                self.assertEqual(msgpack_decode(msgpack_encode(obj)), obj)

    # ---- TelemetryFrame msgpack round-trip ----

    def test_telemetry_frame_roundtrip(self) -> None:
        frame = self._sample_frame()
        encoded = msgpack_encode_telemetry_frame(frame)
        decoded = msgpack_decode_telemetry_frame(encoded)
        self.assertEqual(decoded, frame)

    # ---- RingBufferMetric msgpack round-trip ----

    def test_ring_buffer_metric_roundtrip(self) -> None:
        metric = self._sample_ring_buffer()
        encoded = msgpack_encode_ring_buffer_metric(metric)
        decoded = msgpack_decode_ring_buffer_metric(encoded)
        self.assertEqual(decoded, metric)

    # ---- BackpressureMetric msgpack round-trip ----

    def test_backpressure_metric_roundtrip(self) -> None:
        metric = self._sample_backpressure()
        encoded = msgpack_encode_backpressure_metric(metric)
        decoded = msgpack_decode_backpressure_metric(encoded)
        self.assertEqual(decoded, metric)

    # ---- Payload size reduction vs JSON ----

    def test_msgpack_encoding(self) -> None:
        """msgpack payloads must be >= 35 % smaller than JSON string equivalents."""
        frames = [
            self._sample_frame(
                asset_id=b"NGN/XLM",
                price=15_000_000,
                volume=8_200_000,
                timestamp=1_700_000_000_000,
                sequence=i,
                flags=FLAG_LIVE,
                feed_id=1,
            )
            for i in range(10)
        ]

        for label, frames_subset in [("single_frame", frames[:1]), ("bundle_10", frames)]:
            # Build a JSON-comparable dict representation
            json_dicts = [
                {
                    "asset_id": f.asset_id.decode("ascii"),
                    "price": f.price,
                    "volume": f.volume,
                    "timestamp": f.timestamp,
                    "sequence": f.sequence,
                    "flags": f.flags,
                    "feed_id": f.feed_id,
                }
                for f in frames_subset
            ]
            json_payload = json.dumps(
                json_dicts if len(json_dicts) > 1 else json_dicts[0],
                separators=(",", ":"),
            ).encode("utf-8")
            json_size = len(json_payload)

            # Msgpack payload — encode each frame individually, concatenate
            msgpack_payload = b"".join(
                msgpack_encode_telemetry_frame(f) for f in frames_subset
            )
            msgpack_size = len(msgpack_payload)

            reduction_pct = (1 - msgpack_size / json_size) * 100
            self.assertGreaterEqual(
                reduction_pct,
                35,
                f"{label}: msgpack ({msgpack_size} B) not >= 35% smaller "
                f"than JSON ({json_size} B) — only {reduction_pct:.1f}%",
            )

    # ---- RingBufferMetric size reduction ----

    def test_ring_buffer_metric_size_reduction(self) -> None:
        metric = self._sample_ring_buffer()
        msgpack_data = msgpack_encode_ring_buffer_metric(metric)
        json_data = json.dumps(
            {
                "size": metric.size,
                "capacity": metric.capacity,
                "utilization": metric.utilization,
                "total_enqueued": metric.total_enqueued,
                "total_dequeued": metric.total_dequeued,
                "enqueue_failures": metric.enqueue_failures,
                "dequeue_failures": metric.dequeue_failures,
                "avg_latency_us": metric.avg_latency_us,
                "peak_latency_us": metric.peak_latency_us,
                "batches_processed": metric.batches_processed,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        reduction_pct = (1 - len(msgpack_data) / len(json_data)) * 100
        self.assertGreaterEqual(reduction_pct, 35)

    # ---- BackpressureMetric size reduction ----

    def test_backpressure_metric_size_reduction(self) -> None:
        metric = self._sample_backpressure()
        msgpack_data = msgpack_encode_backpressure_metric(metric)
        json_data = json.dumps(
            {
                "queue_length": metric.queue_length,
                "max_capacity": metric.max_capacity,
                "saturation": metric.saturation,
                "dropped_packets": metric.dropped_packets,
                "slowed_ingestions": metric.slowed_ingestions,
                "avg_processing_us": metric.avg_processing_us,
            },
            separators=(",", ":"),
        ).encode("utf-8")
        reduction_pct = (1 - len(msgpack_data) / len(json_data)) * 100
        self.assertGreaterEqual(reduction_pct, 35)


class TestStructPackEncoder(unittest.TestCase):
    """Issue #613 — struct-pack IPC encoder smoke tests."""

    def test_encode_telemetry_frame(self) -> None:
        enc = StructPackEncoder(channel_id=1)
        frame = TelemetryFrame(
            asset_id=b"NGN/XLM",
            price=15_000_000,
            volume=0,
            timestamp=1_700_000_000_000,
            sequence=1,
            flags=FLAG_LIVE,
            feed_id=3,
        )
        buf = enc.encode_telemetry_frame(frame)
        self.assertEqual(len(buf), FRAME_SIZE + 24)  # 40 + 24-byte header

    def test_decode_header(self) -> None:
        enc = StructPackEncoder(channel_id=2)
        frame = TelemetryFrame(b"USD/XLM", 1000, 2000, 1000000, 1, FLAG_STALE, 1)
        buf = enc.encode_telemetry_frame(frame)
        ptype, version, plen, seq, ts = StructPackEncoder.decode_header(buf)
        self.assertEqual(ptype, 0x01)  # IPC_TYPE_TELEMETRY_FRAME
        self.assertEqual(version, 1)
        self.assertEqual(plen, FRAME_SIZE)

    def test_scale(self) -> None:
        self.assertEqual(StructPackEncoder.scale(1.5), 15_000_000)
        self.assertEqual(StructPackEncoder.scale(0.0), 0)


class TestTelemetryEncoderLegacy(unittest.TestCase):
    """Smoke tests for the existing struct-based packer."""

    def test_pack_unpack_roundtrip(self) -> None:
        frame = TelemetryFrame(
            asset_id=b"EUR/USD",
            price=1_234_567,
            volume=9_876_543,
            timestamp=1_700_000_001_000,
            sequence=7,
            flags=FLAG_SYNTHETIC | FLAG_HALTED,
            feed_id=9,
        )
        packed = TelemetryEncoder.pack(frame)
        self.assertEqual(len(packed), FRAME_SIZE)
        unpacked = TelemetryEncoder.unpack(packed)
        self.assertEqual(unpacked, frame)

    def test_frame_size_constant(self) -> None:
        self.assertEqual(FRAME_SIZE, 40)
