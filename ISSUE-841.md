# Issue #841: Yield Vault TVL Deposit Cap

## Technical Specification

Pre-check total vault TVL before allowing transaction simulation calls for new deposits.

## Deliverables

- Read the current vault TVL from the Redis cache during deposit route execution.
- Reject simulation requests with an explicit HTTP 400 error when the deposit exceeds the strategy TVL cap.
- Re-sync the TVL cache on every deposit and withdrawal event.

## Acceptance Criteria

- Deposit simulations over the strategy TVL cap return HTTP 400 and do not call transaction simulation.
- Deposit simulations within the cap continue normally.
- Deposit and withdrawal events refresh the cached vault TVL.
