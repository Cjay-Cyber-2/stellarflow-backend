import prisma from "../lib/prisma";
import { xdr } from "@stellar/stellar-sdk";

export interface SorobanOrderFilledEvent {
  txHash: string;
  ledger: number;
  index: number;
  topic: unknown;
  value: unknown;
}

export interface OrderFilledData {
  orderId: string;
  fillAmount: string;
}

function decodeScVal(value: unknown): unknown {
  if (typeof value !== "string") return value;
  try {
    const scVal = xdr.ScVal.fromXDR(value, "base64") as any;
    const type = scVal.switch().name;
    if (type === "scvSymbol" || type === "scvString")
      return scVal.value().toString();
    if (
      type === "scvU64" ||
      type === "scvI64" ||
      type === "scvU32" ||
      type === "scvI32"
    ) {
      return scVal.value().toString();
    }
    if (type === "scvMap") {
      return Object.fromEntries(
        (scVal.map() ?? []).map((entry: any) => [
          String(decodeScVal(entry.key())),
          decodeScVal(entry.val()),
        ]),
      );
    }
  } catch {
    return value;
  }
  return value;
}

function topicNames(topic: unknown): string[] {
  if (typeof topic === "string") return [String(decodeScVal(topic))];
  if (!Array.isArray(topic)) return [];
  return topic
    .map((item) => decodeScVal(item))
    .filter((item): item is string => typeof item === "string");
}

export function parseOrderFilledEvent(
  event: SorobanOrderFilledEvent,
): OrderFilledData | null {
  if (!topicNames(event.topic).some((topic) => topic === "OrderFilled")) {
    return null;
  }

  const value = decodeScVal(event.value) as Record<string, unknown> | null;
  if (!value || typeof value.orderId !== "string") return null;

  const fillAmount = value.fillAmount ?? value.amount;
  if (typeof fillAmount !== "string" && typeof fillAmount !== "number") {
    return null;
  }

  const parsed = Number(fillAmount);
  if (!Number.isFinite(parsed) || parsed <= 0) return null;
  return { orderId: value.orderId, fillAmount: String(fillAmount) };
}

export async function verifyOrderFilledEvent(
  event: SorobanOrderFilledEvent,
): Promise<boolean> {
  const fill = parseOrderFilledEvent(event);
  if (!fill) return false;

  await prisma.$transaction(async (transaction) => {
    const indexed = await transaction.orderFilledEvent.findUnique({
      where: {
        txHash_eventIndex: { txHash: event.txHash, eventIndex: event.index },
      },
    });
    if (indexed) return;

    const order = await transaction.openOrder.findUnique({
      where: { orderId: fill.orderId },
    });
    if (!order) {
      await transaction.orderFilledEvent.create({
        data: {
          orderId: fill.orderId,
          fillAmount: fill.fillAmount,
          txHash: event.txHash,
          ledgerSeq: event.ledger,
          eventIndex: event.index,
          payload: event as object,
        },
      });
      return;
    }

    const filledAmount = order.filledAmount.add(fill.fillAmount);
    const status = filledAmount.gte(order.totalAmount)
      ? "filled"
      : "partially_filled";

    await transaction.orderFilledEvent.create({
      data: {
        orderId: fill.orderId,
        fillAmount: fill.fillAmount,
        txHash: event.txHash,
        ledgerSeq: event.ledger,
        eventIndex: event.index,
        payload: event as object,
      },
    });
    await transaction.openOrder.update({
      where: { orderId: fill.orderId },
      data: { filledAmount, status },
    });
  });

  return true;
}
