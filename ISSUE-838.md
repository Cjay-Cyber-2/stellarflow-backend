# Issue #838: Governance Proposal Spam Filter & Deposit Verification Guard

## Scope

Validate proposal submission rules before indexing new proposals in public API endpoints.

## Deliverables

- Verify the proposer's `veFLOW` balance against the minimum proposal creation threshold.
- Check proposal deposit event confirmation on-chain.
- Mark unverified or spam proposals as hidden in API query responses.

## Acceptance Criteria

- Proposals from accounts below the minimum `veFLOW` threshold are not indexed.
- Proposals without a confirmed on-chain deposit event are not indexed.
- Unverified or spam proposals are hidden from public API query responses.
