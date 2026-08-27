/**
 * Re-export backpressure primitives from the canonical implementation.
 *
 * The authoritative source lives in `src/flow_control/backpressure.ts`.
 * This shim keeps existing imports in `src/queue/**` and `src/services/**`
 * working without moving or duplicating code.
 */
export {
  PacketPriority,
  AsyncBoundedQueue,
  BackpressureManager,
} from "../flow_control/backpressure";

export type {
  IngestionPacket,
  BackpressureMetrics,
  BackpressureConfig,
} from "../flow_control/backpressure";
