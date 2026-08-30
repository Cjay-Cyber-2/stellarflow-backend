## Description

This change implements a Redis-backed encrypted session storage engine for authenticated user sessions. The goal is to protect sensitive session data in memory while keeping fast revocation and TTL enforcement in place for security-sensitive workflows.

The implementation adds AES-256 encryption for session payloads before they are stored in Redis, ensures session entries expire automatically based on inactivity/TTL rules, and enforces immediate revocation during logout or security invalidation. The auth middleware now validates the Redis-backed session state before accepting a request as authenticated, closing the gap where a valid JWT could remain active after the session had already been revoked.

This work is aligned with the project’s existing JWT architecture, while moving the active session authority into Redis for faster invalidation and safer in-memory storage.

## Type of Change

- [ ] Bug fix
- [x] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing

- [x] Tested locally
- [x] Added unit tests
- [ ] Tested on Stellar Testnet (for wallet/contract changes)

## Screenshots (if applicable)

## Related Issues

Closes #914

### Implementation plan and execution summary

1. Reviewed the existing auth/session flow across the JWT utilities, middleware, and route handlers to map the actual security boundary.
2. Added encrypted storage for session payloads before Redis persistence using AES-256 GCM.
3. Implemented session key generation, TTL assignment, and Redis-backed session validation logic in the JWT utility layer.
4. Updated middleware checks so authenticated requests are rejected when the Redis-backed session is expired, missing, or revoked.
5. Updated login/logout flows to create encrypted Redis sessions and revoke them immediately on logout.
6. Added focused regression tests covering encryption/decryption, session persistence, TTL behavior, and immediate revocation.

### Tests carried out

- `npx jest test/auth.jest.test.ts --runInBand --silent`

Result:

- Test Suites: 1 passed, 1 total
- Tests: 8 passed, 8 total
