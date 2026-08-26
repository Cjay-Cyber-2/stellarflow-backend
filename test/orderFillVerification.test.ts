import { parseOrderFilledEvent } from "../src/services/orderFillVerificationService";

const event = {
  txHash: "tx-1",
  ledger: 42,
  index: 3,
  topic: ["OrderFilled"],
  value: { orderId: "order-1", fillAmount: "2.5" },
};

if (
  !parseOrderFilledEvent(event) ||
  parseOrderFilledEvent(event)?.fillAmount !== "2.5"
) {
  throw new Error("OrderFilled events should be parsed");
}
if (parseOrderFilledEvent({ ...event, topic: ["OtherEvent"] }) !== null) {
  throw new Error("Non-fill events should be ignored");
}

console.log("order fill verification parsing passed");
