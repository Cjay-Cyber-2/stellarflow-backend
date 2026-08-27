import assert from "node:assert/strict";
import {
  evaluatePriceMovement,
  PRICE_CHANGE_THRESHOLD,
} from "../src/services/priceProtection";

function run() {
  const safeMove = evaluatePriceMovement({
    currentRate: 111,
    baselineRate: 100,
    currency: "NGN",
  });
  assert.equal(safeMove.isAnomalous, false);
  assert.equal(safeMove.changePercent, 11);

  const edgeMove = evaluatePriceMovement({
    currentRate: 115,
    baselineRate: 100,
    currency: "KES",
  });
  assert.equal(edgeMove.isAnomalous, false);
  assert.equal(edgeMove.changePercent, PRICE_CHANGE_THRESHOLD * 100);

  const flaggedMove = evaluatePriceMovement({
    currentRate: 116,
    baselineRate: 100,
    currency: "GHS",
  });
  assert.equal(flaggedMove.isAnomalous, true);
  assert.match(flaggedMove.reason ?? "", /manual review threshold/i);

  const downwardMove = evaluatePriceMovement({
    currentRate: 84,
    baselineRate: 100,
    currency: "NGN",
  });
  assert.equal(downwardMove.isAnomalous, true);
  assert.equal(downwardMove.changePercent, 16);
}

try {
  run();
} catch (error) {
  console.error(error);
  process.exitCode = 1;
}
