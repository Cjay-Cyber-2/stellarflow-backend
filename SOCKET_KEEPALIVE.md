# Socket Keep-Alive Configuration

## Overview

This document describes the socket keep-alive implementation that prevents silent connection drops from external regional exchange API endpoints.

## Problem Statement

External regional exchange API endpoints occasionally drop silent socket connections without sending termination packets, leaving backend ingestion threads hanging indefinitely. This causes:

- Request timeouts that exceed normal thresholds
- Resource exhaustion from hanging connections
- Degraded system performance
- Failed price fetches

## Solution

The implementation configures explicit low-level socket options using Node.js HTTP/HTTPS agents to force the operating system to proactively probe and tear down dead connections.

## Implementation Details

### HTTP Client (`src/lib/httpClient.ts`)

A centralized HTTP client with aggressive socket keep-alive configuration:

```typescript
import { httpClient } from '../lib/httpClient';

// Use for all external API calls
const response = await httpClient.get('https://api.example.com/data');
```

### Socket Configuration

| Parameter | Value | Description |
|-----------|-------|-------------|
| `keepAlive` | `true` | Enables TCP keep-alive at OS level |
| `keepAliveMsecs` | `2000` ms | Probe interval (TCP_KEEPINTVL) |
| `timeout` | `10000` ms | Initial idle timeout (TCP_KEEPIDLE) |
| Max Retries | `3` probes | Number of failed probes before teardown (TCP_KEEPCNT) |

### Hang Prevention Timeline

```
Connection idle → 10s → First probe sent → 2s → Second probe → 2s → Third probe → 2s → Connection terminated
Total maximum hang time: ~16 seconds
```

This ensures that dead connections are detected and cleaned up within **16 seconds** maximum, preventing indefinite hangs.

## Low-Level Socket Options

The implementation configures the following TCP socket options:

1. **SO_KEEPALIVE**: Enables keep-alive probes at the operating system level
2. **TCP_KEEPIDLE**: Sets initial delay before first probe (10 seconds)
3. **TCP_KEEPINTVL**: Sets interval between probes (2 seconds)
4. **TCP_KEEPCNT**: Sets number of failed probes before connection teardown (3 probes)

These options are configured via Node.js HTTP/HTTPS agents and applied to all sockets created for external API calls.

## Updated Services

The following services have been migrated to use the centralized `httpClient`:

### Market Rate Fetchers
- ✅ `src/services/marketRate/ghsFetcher.ts` - GHS rate fetcher
- ✅ `src/services/marketRate/kesFetcher.ts` - KES rate fetcher  
- ✅ `src/services/marketRate/ngnFetcher.ts` - NGN rate fetcher (including VTpass)

### Notification Services
- ✅ `src/services/webhook.ts` - Webhook notification service
- ✅ `src/services/notificationService.ts` - System notification service

### Monitoring Services
- ✅ `src/services/sanityCheckService.ts` - External price validation
- ✅ `src/services/marketRate/marketRateService.ts` - Cross-pair consistency checks

## Benefits

1. **Proactive Dead Connection Detection**: OS-level probes detect silent drops within 16 seconds
2. **Resource Protection**: Prevents thread exhaustion from hanging connections
3. **Improved Reliability**: Failed connections are quickly identified and retried
4. **Consistent Configuration**: All external API calls use the same socket settings
5. **Monitoring Ready**: Centralized client makes it easy to add metrics and logging

## Usage Guidelines

### For New Services

Always use the centralized `httpClient` for external API calls:

```typescript
import { httpClient } from '../lib/httpClient';

// Good ✅
const response = await httpClient.get(url, { timeout: 10000 });

// Bad ❌ - Don't use axios directly
import axios from 'axios';
const response = await axios.get(url);
```

### Custom Timeout Requirements

For endpoints requiring different timeout values:

```typescript
import { createHttpClient } from '../lib/httpClient';

const customClient = createHttpClient({
  timeout: 30000, // 30 seconds
  headers: {
    'X-Custom-Header': 'value'
  }
});

const response = await customClient.get(url);
```

### With Retry Logic

The `httpClient` works seamlessly with the existing retry utility:

```typescript
import { httpClient } from '../lib/httpClient';
import { withRetry } from '../utils/retryUtil';

const response = await withRetry(
  () => httpClient.get(url, { timeout: 10000 }),
  { maxRetries: 3, retryDelay: 1000 }
);
```

## Configuration

Socket keep-alive settings are defined in `src/lib/httpClient.ts`:

```typescript
const KEEP_ALIVE_TIMEOUT_MS = 10_000;      // TCP_KEEPIDLE
const KEEP_ALIVE_PROBE_INTERVAL_MS = 2_000; // TCP_KEEPINTVL  
const KEEP_ALIVE_MAX_RETRIES = 3;           // TCP_KEEPCNT
```

Adjust these values if different behavior is needed for your deployment environment.

## Monitoring

To monitor socket keep-alive effectiveness:

1. **Connection Metrics**: Track successful vs. failed requests
2. **Timeout Patterns**: Monitor if timeouts occur within the 16-second window
3. **Error Logs**: Watch for socket-related errors in application logs
4. **Provider Reputation**: Use the existing reputation service to track provider reliability

## Platform-Specific Behavior

### Linux
Full support for TCP_KEEPIDLE, TCP_KEEPINTVL, and TCP_KEEPCNT via `setKeepAlive()`.

### Windows
Partial support - `setKeepAlive()` configures keep-alive but with OS-default intervals. The timeout parameter sets the initial delay.

### macOS
Similar to Linux with full TCP keep-alive support through BSD sockets.

## Testing

To verify the socket configuration is working:

1. **Network Simulation**: Use tools like `tc` (traffic control) to simulate packet loss
2. **Provider Monitoring**: Check the provider reputation scores over time
3. **Connection Duration**: Monitor average connection times in logs
4. **Error Rates**: Track socket timeout errors vs. other error types

## Troubleshooting

### Connections Still Hanging

If connections still hang beyond 16 seconds:

1. Check firewall rules aren't blocking keep-alive probes
2. Verify OS-level TCP keep-alive is enabled
3. Review application-level timeouts (should be < 16 seconds)
4. Check if provider implements TCP keep-alive properly

### Too Aggressive

If connections are being dropped prematurely:

1. Increase `KEEP_ALIVE_TIMEOUT_MS` (initial delay)
2. Increase `KEEP_ALIVE_PROBE_INTERVAL_MS` (probe interval)
3. Increase `KEEP_ALIVE_MAX_RETRIES` (probe attempts)

## Related Files

- `src/lib/httpClient.ts` - Main HTTP client implementation
- `src/utils/httpTimeout.ts` - Global timeout configuration
- `src/utils/retryUtil.ts` - Retry logic for failed requests
- `src/services/marketRate/` - Market rate fetchers using the client

## References

- [Node.js HTTP Agent Documentation](https://nodejs.org/api/http.html#class-httpagent)
- [TCP Keep-Alive RFC 1122](https://www.rfc-editor.org/rfc/rfc1122)
- [Linux TCP Socket Options](https://man7.org/linux/man-pages/man7/tcp.7.html)

---

**Last Updated**: Implementation completed with socket keep-alive configuration for all external API calls.
