/**
 * Unit tests for GovernanceTimelockService
 *
 * Tests cover:
 *  - Contract event indexing (ProposalQueued, TimelockActionExecuted)
 *  - Execution-ready notification detection and dispatch
 *  - Idempotency of event upserts
 *  - Edge cases: missing proposalId, unknown event names, missing expiry
 */
import {
  describe,
  it,
  expect,
  beforeEach,
  afterEach,
  jest,
} from "@jest/globals";

// ---------------------------------------------------------------------------
// Mock Prisma before importing service
// ---------------------------------------------------------------------------
const mockPrismaTimelockEventUpsert = jest.fn<() => Promise<any>>();
const mockPrismaTimelockEventFindFirst = jest.fn<() => Promise<any>>();
const mockPrismaGovernanceProposalUpsert = jest.fn<() => Promise<any>>();
const mockPrismaGovernanceProposalUpdate = jest.fn<() => Promise<any>>();
const mockPrismaExecuteRaw = jest.fn<() => Promise<any>>();
const mockPrismaQueryRaw = jest.fn<() => Promise<any>>();

jest.mock("../src/lib/prisma", () => ({
  __esModule: true,
  default: {
    timelockEvent: {
      upsert: mockPrismaTimelockEventUpsert,
      findFirst: mockPrismaTimelockEventFindFirst,
    },
    governanceProposal: {
      upsert: mockPrismaGovernanceProposalUpsert,
      update: mockPrismaGovernanceProposalUpdate,
    },
    $executeRaw: mockPrismaExecuteRaw,
    $queryRaw: mockPrismaQueryRaw,
  },
}));

// ---------------------------------------------------------------------------
// Mock stellarProvider
// ---------------------------------------------------------------------------
const mockGetEvents = jest.fn<() => Promise<any>>();
const mockGetServer = jest.fn();

jest.mock("../src/lib/stellarProvider", () => ({
  __esModule: true,
  default: {
    getRpcServer: () => ({ getEvents: mockGetEvents }),
    getServer: mockGetServer,
    reportFailure: jest.fn(),
  },
}));

// ---------------------------------------------------------------------------
// Mock StellarService
// ---------------------------------------------------------------------------
const mockExecuteGovernanceProposal = jest.fn<() => Promise<string>>();

jest.mock("../src/services/stellarService", () => ({
  __esModule: true,
  StellarService: jest.fn(() => ({
    executeGovernanceProposal: mockExecuteGovernanceProposal,
  })),
}));

// ---------------------------------------------------------------------------
// Mock NotificationService
// ---------------------------------------------------------------------------
const mockSendGovernanceTimelockReadyAlert = jest.fn<() => Promise<boolean>>();

jest.mock("../src/services/notificationService", () => ({
  __esModule: true,
  notificationService: {
    sendGovernanceTimelockReadyAlert: mockSendGovernanceTimelockReadyAlert,
  },
  AlertType: { GOVERNANCE_TIMELOCK_READY: "governance_timelock_ready" },
  AlertSeverity: { HIGH: "high" },
}));

// ---------------------------------------------------------------------------
// Mock logger
// ---------------------------------------------------------------------------
jest.mock("../src/utils/logger", () => ({
  logger: {
    info: jest.fn(),
    warn: jest.fn(),
    error: jest.fn(),
    debug: jest.fn(),
  },
}));

// ---------------------------------------------------------------------------
// Import subject under test
// ---------------------------------------------------------------------------
import { GovernanceTimelockService } from "../src/services/governanceTimelockService";

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/** Encode a string to base64 ScVal-like representation. */
function encodeSymbol(s: string): string {
  // Real Soroban RPC returns XDR-encoded ScVal; our decoder falls back to the
  // raw string when XDR parsing fails, so passing the plain string is fine for
  // testing the extraction logic.
  return s;
}

/** Build a minimal mock Soroban event. */
function makeEvent(
  eventName: string,
  proposalId: string,
  opts: {
    txHash?: string;
    ledger?: number;
    ledgerClosedAt?: string;
    expiresAt?: number;
  } = {},
) {
  return {
    type: "contract",
    ledger: opts.ledger ?? 1000,
    ledgerClosedAt: opts.ledgerClosedAt ?? new Date().toISOString(),
    txHash: opts.txHash ?? "abc123",
    contractId: "CONTRACT_A",
    topic: [encodeSymbol(eventName), encodeSymbol(proposalId)],
    value: opts.expiresAt
      ? JSON.stringify({ expiresAt: opts.expiresAt })
      : null,
  };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("GovernanceTimelockService", () => {
  let service: GovernanceTimelockService;

  beforeEach(() => {
    jest.clearAllMocks();

    // Default: no prior indexed events
    mockPrismaTimelockEventFindFirst.mockResolvedValue(null);

    // Default: RPC returns no events
    mockGetEvents.mockResolvedValue({ events: [] });

    // Default: DB writes succeed
    mockPrismaTimelockEventUpsert.mockResolvedValue({});
    mockPrismaGovernanceProposalUpsert.mockResolvedValue({});
    mockPrismaGovernanceProposalUpdate.mockResolvedValue({});
    mockPrismaExecuteRaw.mockResolvedValue(1);
    mockPrismaQueryRaw.mockResolvedValue([]);

    // Default: notification sends succeed
    mockSendGovernanceTimelockReadyAlert.mockResolvedValue(true);

    service = new GovernanceTimelockService(
      60_000,
      // stellarService injected via mock
    );
  });

  afterEach(() => {
    service.stop();
  });

  // -------------------------------------------------------------------------
  // Event indexing
  // -------------------------------------------------------------------------

  describe("indexContractEvents", () => {
    it("ignores events with unrecognised event names", async () => {
      mockGetEvents.mockResolvedValue({
        events: [makeEvent("OtherEvent", "prop-1")],
      });

      await service.indexContractEvents("CONTRACT_A");

      expect(mockPrismaTimelockEventUpsert).not.toHaveBeenCalled();
      expect(mockPrismaGovernanceProposalUpsert).not.toHaveBeenCalled();
    });

    it("ignores events when proposalId cannot be extracted", async () => {
      const badEvent = {
        ...makeEvent("ProposalQueued", ""),
        topic: [encodeSymbol("ProposalQueued")], // no second element
        value: null,
      };
      mockGetEvents.mockResolvedValue({ events: [badEvent] });

      await service.indexContractEvents("CONTRACT_A");

      expect(mockPrismaTimelockEventUpsert).not.toHaveBeenCalled();
    });

    it("upserts a TimelockEvent row for a ProposalQueued event", async () => {
      const event = makeEvent("ProposalQueued", "prop-42", {
        txHash: "tx001",
        ledger: 1050,
        expiresAt: Math.floor(Date.now() / 1000) + 3600,
      });
      mockGetEvents.mockResolvedValue({ events: [event] });

      await service.indexContractEvents("CONTRACT_A");

      expect(mockPrismaTimelockEventUpsert).toHaveBeenCalledTimes(1);
      const upsertCall = mockPrismaTimelockEventUpsert.mock.calls[0]![0] as any;
      expect(upsertCall.create.eventType).toBe("ProposalQueued");
      expect(upsertCall.create.proposalId).toBe("prop-42");
      expect(upsertCall.create.ledgerSeq).toBe(1050);
      expect(upsertCall.create.txHash).toBe("tx001");
    });

    it("upserts a GovernanceProposal row for a ProposalQueued event", async () => {
      const futureEpochSec = Math.floor(Date.now() / 1000) + 7200;
      const event = makeEvent("ProposalQueued", "prop-99", {
        txHash: "tx002",
        ledger: 2000,
        expiresAt: futureEpochSec,
      });
      mockGetEvents.mockResolvedValue({ events: [event] });

      await service.indexContractEvents("CONTRACT_A");

      expect(mockPrismaGovernanceProposalUpsert).toHaveBeenCalledTimes(1);
      const call = mockPrismaGovernanceProposalUpsert.mock.calls[0]![0] as any;
      expect(call.create.proposalId).toBe("prop-99");
      expect(call.create.status).toBe("Queued");
      // expiresAt should be approximately futureEpochSec * 1000 ms from epoch
      const expiresAtMs = (call.create.expiresAt as Date).getTime();
      expect(expiresAtMs).toBeGreaterThan(Date.now());
    });

    it("marks GovernanceProposal Executed for a TimelockActionExecuted event", async () => {
      const event = makeEvent("TimelockActionExecuted", "prop-77", {
        txHash: "tx003",
        ledger: 3000,
      });
      mockGetEvents.mockResolvedValue({ events: [event] });

      await service.indexContractEvents("CONTRACT_A");

      expect(mockPrismaGovernanceProposalUpdate).toHaveBeenCalledTimes(1);
      const call = mockPrismaGovernanceProposalUpdate.mock.calls[0]![0] as any;
      expect(call.where.proposalId).toBe("prop-77");
      expect(call.data.status).toBe("Executed");
    });

    it("handles both event types in a single batch", async () => {
      mockGetEvents.mockResolvedValue({
        events: [
          makeEvent("ProposalQueued", "p1", {
            txHash: "tx100",
            ledger: 100,
          }),
          makeEvent("TimelockActionExecuted", "p2", {
            txHash: "tx101",
            ledger: 101,
          }),
        ],
      });

      await service.indexContractEvents("CONTRACT_A");

      expect(mockPrismaTimelockEventUpsert).toHaveBeenCalledTimes(2);
      expect(mockPrismaGovernanceProposalUpsert).toHaveBeenCalledTimes(1);
      expect(mockPrismaGovernanceProposalUpdate).toHaveBeenCalledTimes(1);
    });

    it("does not crash if getEvents RPC call fails", async () => {
      mockGetEvents.mockRejectedValue(new Error("RPC error"));

      await expect(
        service.indexContractEvents("CONTRACT_A"),
      ).resolves.not.toThrow();
    });

    it("is idempotent: upsert is called again on a duplicate event without throwing", async () => {
      const event = makeEvent("ProposalQueued", "prop-idem", {
        txHash: "txIdem",
        ledger: 500,
      });
      mockGetEvents.mockResolvedValue({ events: [event] });

      await service.indexContractEvents("CONTRACT_A");
      await service.indexContractEvents("CONTRACT_A");

      // Both calls go through; the DB upsert handles the uniqueness constraint
      expect(mockPrismaTimelockEventUpsert).toHaveBeenCalledTimes(2);
    });

    it("advances lastIndexedLedger cursor", async () => {
      mockGetEvents.mockResolvedValue({
        events: [
          makeEvent("ProposalQueued", "p-cursor", {
            txHash: "txCursor",
            ledger: 9999,
          }),
        ],
      });

      await service.indexContractEvents("CONTRACT_A");

      // On the second call, startLedger should be 9999
      await service.indexContractEvents("CONTRACT_A");

      const secondCallArg = mockGetEvents.mock.calls[1]![0] as any;
      expect(secondCallArg.startLedger).toBe(9999);
    });
  });

  // -------------------------------------------------------------------------
  // Notification logic
  // -------------------------------------------------------------------------

  describe("notifyReadyProposals", () => {
    it("does nothing when no proposals are ready", async () => {
      mockPrismaQueryRaw.mockResolvedValue([]);

      await service.notifyReadyProposals();

      expect(mockSendGovernanceTimelockReadyAlert).not.toHaveBeenCalled();
      expect(mockPrismaExecuteRaw).not.toHaveBeenCalled();
    });

    it("fires a notification for each expired un-notified proposal", async () => {
      const expired = new Date(Date.now() - 60_000);
      mockPrismaQueryRaw.mockResolvedValue([
        {
          id: 1,
          proposalId: "prop-ready-1",
          contractId: "CONTRACT_A",
          expiresAt: expired,
          notificationCount: 0,
        },
        {
          id: 2,
          proposalId: "prop-ready-2",
          contractId: "CONTRACT_A",
          expiresAt: expired,
          notificationCount: 0,
        },
      ]);

      await service.notifyReadyProposals();

      expect(mockSendGovernanceTimelockReadyAlert).toHaveBeenCalledTimes(2);
      expect(mockSendGovernanceTimelockReadyAlert).toHaveBeenCalledWith(
        expect.objectContaining({ proposalId: "prop-ready-1" }),
      );
      expect(mockSendGovernanceTimelockReadyAlert).toHaveBeenCalledWith(
        expect.objectContaining({ proposalId: "prop-ready-2" }),
      );
    });

    it("increments notificationCount via $executeRaw after alerting", async () => {
      const expired = new Date(Date.now() - 30_000);
      mockPrismaQueryRaw.mockResolvedValue([
        {
          id: 7,
          proposalId: "prop-incr",
          contractId: "CONTRACT_A",
          expiresAt: expired,
          notificationCount: 1,
        },
      ]);

      await service.notifyReadyProposals();

      expect(mockPrismaExecuteRaw).toHaveBeenCalledTimes(1);
    });

    it("still increments other proposals if one notification fails", async () => {
      const expired = new Date(Date.now() - 60_000);
      mockPrismaQueryRaw.mockResolvedValue([
        {
          id: 10,
          proposalId: "prop-fail",
          contractId: "CONTRACT_A",
          expiresAt: expired,
          notificationCount: 0,
        },
        {
          id: 11,
          proposalId: "prop-ok",
          contractId: "CONTRACT_A",
          expiresAt: expired,
          notificationCount: 0,
        },
      ]);

      // First notification throws, second succeeds
      mockSendGovernanceTimelockReadyAlert
        .mockRejectedValueOnce(new Error("webhook down"))
        .mockResolvedValueOnce(true);

      await service.notifyReadyProposals();

      // Only the successful alert's DB update is called
      expect(mockPrismaExecuteRaw).toHaveBeenCalledTimes(1);
    });

    it("passes correct proposal details to the notification service", async () => {
      const expired = new Date(Date.now() - 5_000);
      mockPrismaQueryRaw.mockResolvedValue([
        {
          id: 20,
          proposalId: "prop-details",
          contractId: "CTRCT_XYZ",
          expiresAt: expired,
          notificationCount: 0,
        },
      ]);

      await service.notifyReadyProposals();

      expect(mockSendGovernanceTimelockReadyAlert).toHaveBeenCalledWith({
        proposalId: "prop-details",
        contractId: "CTRCT_XYZ",
        expiresAt: expired,
      });
    });
  });

  // -------------------------------------------------------------------------
  // start() / stop()
  // -------------------------------------------------------------------------

  describe("lifecycle", () => {
    it("resumes lastIndexedLedger from DB on start", async () => {
      mockPrismaTimelockEventFindFirst.mockResolvedValue({ ledgerSeq: 5050 });
      // Prevent any actual polling side-effects
      mockGetEvents.mockResolvedValue({ events: [] });
      mockPrismaQueryRaw.mockResolvedValue([]);

      const svc = new GovernanceTimelockService(99_999_999);
      await svc.start();
      svc.stop();

      const callArg = mockGetEvents.mock.calls[0]![0] as any;
      expect(callArg.startLedger).toBe(5050);
    });

    it("does not start twice", async () => {
      mockGetEvents.mockResolvedValue({ events: [] });
      mockPrismaQueryRaw.mockResolvedValue([]);

      const svc = new GovernanceTimelockService(99_999_999);
      await svc.start();
      await svc.start(); // second call is a no-op

      // tick() is only called once (from the first start())
      expect(mockGetEvents).toHaveBeenCalledTimes(1);

      svc.stop();
    });

    it("stop() clears the polling timer", () => {
      const svc = new GovernanceTimelockService(99_999_999);
      // Access private field through `any` cast for testing
      (svc as any).running = true;
      (svc as any).timer = setInterval(() => {}, 999_999);

      svc.stop();

      expect((svc as any).running).toBe(false);
      expect((svc as any).timer).toBeNull();
    });
  });

  // -------------------------------------------------------------------------
  // ExpiresAt extraction edge cases
  // -------------------------------------------------------------------------

  describe("expiresAt extraction", () => {
    it("falls back to 24h default when event value has no expiry field", async () => {
      const event = {
        ...makeEvent("ProposalQueued", "prop-no-exp", {
          txHash: "txNoExp",
          ledger: 300,
        }),
        value: null,
      };
      mockGetEvents.mockResolvedValue({ events: [event] });

      await service.indexContractEvents("CONTRACT_A");

      const call = mockPrismaGovernanceProposalUpsert.mock.calls[0]![0] as any;
      const expiresAt = call.create.expiresAt as Date;
      const diff = expiresAt.getTime() - Date.now();
      // Should be approximately 24h (allow 10s tolerance for test execution)
      expect(diff).toBeGreaterThan(24 * 60 * 60 * 1000 - 10_000);
      expect(diff).toBeLessThan(24 * 60 * 60 * 1000 + 10_000);
    });

    it("parses Unix epoch seconds from a numeric expiry field", async () => {
      const futureEpochSec = Math.floor(Date.now() / 1000) + 7200; // +2h
      const event = makeEvent("ProposalQueued", "prop-epoch", {
        txHash: "txEpoch",
        ledger: 400,
        expiresAt: futureEpochSec,
      });
      mockGetEvents.mockResolvedValue({ events: [event] });

      await service.indexContractEvents("CONTRACT_A");

      const call = mockPrismaGovernanceProposalUpsert.mock.calls[0]![0] as any;
      const expiresAt = call.create.expiresAt as Date;
      // Allow ±5s tolerance
      expect(
        Math.abs(expiresAt.getTime() - futureEpochSec * 1000),
      ).toBeLessThan(5000);
    });
  });
});
