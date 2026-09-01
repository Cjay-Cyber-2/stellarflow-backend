## Description

This change adds a durable webhook retry worker to StellarFlow using Celery and RabbitMQ. The worker ensures that transient webhook delivery errors are retried with bounded exponential backoff while preserving endpoint health state across process restarts, and it moves permanent or exhausted failures to a terminal DLQ.

The implementation adds a retry policy with capped exponential delay and jitter, endpoint health tracking that disables endpoints after 24 hours of continuous failures, and task registration for the queue and dead-letter flow. It also adds a Node-side RabbitMQ publisher so webhook retry messages can be enqueued with the same routing and durability conventions as the worker.

## Type of Change

- [ ] Bug fix
- [x] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing

- [x] Tested locally
- [x] Added unit tests
- [ ] Tested on Stellar Testnet (for wallet/contract changes)
- [ ] Live RabbitMQ queue publish probe completed (blocked by Docker Desktop image pull environment issue)

## Screenshots (if applicable)

## Related Issues

Addresses the automated webhook retry worker task.

### Implementation plan and execution summary

1. Added a durable retry message model and validation in `app/services/webhook_retry.py`.
2. Implemented a bounded exponential backoff policy with jitter and a 24-hour endpoint disable window.
3. Added Celery task routing and queue configuration for `webhook.retry` and `webhook.dead` in `app/celery_app.py`.
4. Wired the worker task flow in `app/tasks.py` to deliver webhooks, retry transient failures, and move terminal failures to the DLQ.
5. Added a Node RabbitMQ publisher in `src/services/webhookRetryPublisher.ts` to publish retry events into the same exchange/queue contract.
6. Added focused regression tests covering backoff behavior, message validation, and endpoint failure tracking.

### Tests carried out

- `python -m pytest tests/test_webhook_retry.py -q`

### Verification completed

- Retry policy regression tests passed.
- Delivery message validation passed.
- Continuous-failure disable logic passed.
- Success reset behavior passed.

### Environment note

A live Docker/RabbitMQ publish probe was attempted, but Docker Desktop stalled during the RabbitMQ image pull, so the live queue verification is currently environment-limited rather than code-limited. The implementation and task-registration checks themselves are passing.
