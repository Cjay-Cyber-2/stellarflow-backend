# Backpressure Queue Testing Guide

This guide provides step-by-step instructions to verify the backpressure queue implementation for issue #340.

## Overview

The implementation includes:
- **AsyncBoundedQueue**: A TypeScript async bounded queue similar to Python's asyncio.Queue
- **BackpressureManager**: Manages queue with configurable backpressure rules
- **Integration with MarketRateService**: Applied to rate fetching ingestion loops

## Prerequisites

- Node.js 18+ installed
- Project dependencies installed (`npm install`)
- TypeScript compiler available

---

## Step 1: Run the Test Suite

Execute the comprehensive test suite to verify all functionality:

```bash
npm test -- test/backpressure.test.ts
```

### Expected Results

All tests should pass:
- ✅ Basic queue operations (put, get, tryPut, tryGet)
- ✅ Async operations with waiting producers/consumers
- ✅ Queue closure behavior
- ✅ Backpressure configuration
- ✅ Enqueue with backpressure rules
- ✅ Dequeue operations
- ✅ Metrics tracking (queue length, saturation, dropped packets, slowed ingestions)
- ✅ High load scenario handling
- ✅ Recovery from backpressure

---

## Step 2: Manual Testing - Basic Queue Operations

Create a test file `manual-test-queue.ts`:

```typescript
import { AsyncBoundedQueue } from './src/queue/backpressure';

async function testBasicQueue() {
  console.log('=== Testing Basic Queue Operations ===');
  
  const queue = new AsyncBoundedQueue<number>(5);
  
  // Test tryPut
  console.log('Testing tryPut...');
  console.log('Queue size before:', queue.size());
  queue.tryPut(1);
  queue.tryPut(2);
  queue.tryPut(3);
  console.log('Queue size after adding 3 items:', queue.size());
  
  // Test tryGet
  console.log('\nTesting tryGet...');
  console.log('Dequeued:', queue.tryGet());
  console.log('Dequeued:', queue.tryGet());
  console.log('Queue size after removing 2 items:', queue.size());
  
  // Test full queue
  console.log('\nTesting full queue...');
  queue.tryPut(4);
  queue.tryPut(5);
  console.log('Queue size:', queue.size());
  console.log('Is full?', queue.isFull());
  console.log('Try to add to full queue:', queue.tryPut(6)); // Should return false
  
  console.log('\n✅ Basic queue operations test completed');
}

testBasicQueue().catch(console.error);
```

Run the manual test:
```bash
npx ts-node manual-test-queue.ts
```

---

## Step 3: Manual Testing - Backpressure Manager

Create a test file `manual-test-backpressure.ts`:

```typescript
import { BackpressureManager, PacketPriority } from './src/queue/backpressure';

async function testBackpressureManager() {
  console.log('=== Testing Backpressure Manager ===');
  
  const manager = new BackpressureManager({
    maxCapacity: 10,
    dropThreshold: 0.8,  // Drop metrics at 80%
    slowDownThreshold: 0.6,  // Slow down at 60%
    slowDownDelay: 100,  // 100ms delay
  });
  
  // Test normal enqueue
  console.log('\n1. Testing normal enqueue...');
  await manager.enqueue({
    priority: PacketPriority.STANDARD,
    data: { test: 'normal' },
    timestamp: Date.now(),
  });
  console.log('Queue length:', manager.getQueueLength());
  console.log('Metrics:', manager.getMetrics());
  
  // Test slow down (fill to 70%)
  console.log('\n2. Testing slow down (filling to 70%)...');
  for (let i = 0; i < 6; i++) {
    await manager.enqueue({
      priority: PacketPriority.STANDARD,
      data: { test: i },
      timestamp: Date.now(),
    });
  }
  console.log('Queue length:', manager.getQueueLength());
  console.log('Saturation:', manager.getMetrics().saturation);
  console.log('Slowed down ingestions:', manager.getMetrics().slowedDownIngestions);
  
  // Test drop metric packets (fill to 90%)
  console.log('\n3. Testing metric packet drop (filling to 90%)...');
  for (let i = 0; i < 3; i++) {
    await manager.enqueue({
      priority: PacketPriority.STANDARD,
      data: { test: i },
      timestamp: Date.now(),
    });
  }
  console.log('Queue length:', manager.getQueueLength());
  
  const metricResult = await manager.enqueue({
    priority: PacketPriority.METRIC,
    data: { test: 'metric' },
    timestamp: Date.now(),
  });
  console.log('Metric packet enqueued?', metricResult); // Should be false
  console.log('Dropped packets:', manager.getMetrics().droppedPackets);
  
  // Test dequeue
  console.log('\n4. Testing dequeue...');
  const dequeued = await manager.dequeue();
  console.log('Dequeued packet:', dequeued);
  console.log('Queue length after dequeue:', manager.getQueueLength());
  
  console.log('\n✅ Backpressure manager test completed');
  console.log('\nFinal metrics:', manager.getMetrics());
}

testBackpressureManager().catch(console.error);
```

Run the manual test:
```bash
npx ts-node manual-test-backpressure.ts
```

---

## Step 4: Integration Testing with MarketRateService

Create a test file `manual-test-integration.ts`:

```typescript
import { MarketRateService } from './src/services/marketRate/marketRateService';

async function testMarketRateIntegration() {
  console.log('=== Testing MarketRateService Integration ===');
  
  const service = new MarketRateService();
  
  // Get initial backpressure metrics
  console.log('\n1. Initial backpressure metrics:');
  console.log(service.getBackpressureMetrics());
  
  // Fetch a rate (this will use backpressure)
  console.log('\n2. Fetching NGN rate...');
  const result = await service.getRate('NGN');
  console.log('Fetch result:', result.success ? 'Success' : 'Failed');
  
  // Check metrics after fetch
  console.log('\n3. Backpressure metrics after fetch:');
  console.log(service.getBackpressureMetrics());
  
  // Fetch multiple rates to test backpressure under load
  console.log('\n4. Fetching multiple rates to test backpressure...');
  const currencies = ['NGN', 'KES', 'GHS'];
  for (let i = 0; i < 5; i++) {
    for (const currency of currencies) {
      await service.getRate(currency);
    }
    console.log(`Batch ${i + 1} completed. Queue length:`, service.getBackpressureMetrics().queueLength);
  }
  
  // Final metrics
  console.log('\n5. Final backpressure metrics:');
  console.log(service.getBackpressureMetrics());
  
  console.log('\n✅ Integration test completed');
}

testMarketRateIntegration().catch(console.error);
```

Run the integration test:
```bash
npx ts-node manual-test-integration.ts
```

---

## Step 5: High Load Simulation Test

Create a test file `manual-test-load.ts`:

```typescript
import { BackpressureManager, PacketPriority } from './src/queue/backpressure';

async function testHighLoad() {
  console.log('=== Testing High Load Scenario ===');
  
  const manager = new BackpressureManager({
    maxCapacity: 100,
    dropThreshold: 0.9,
    slowDownThreshold: 0.7,
    slowDownDelay: 10,
  });
  
  console.log('Enqueueing 500 packets rapidly...');
  const startTime = Date.now();
  
  const promises = [];
  for (let i = 0; i < 500; i++) {
    const packet = {
      priority: i % 3 === 0 ? PacketPriority.METRIC : PacketPriority.STANDARD,
      data: { index: i },
      timestamp: Date.now(),
    };
    promises.push(manager.enqueue(packet));
  }
  
  const results = await Promise.all(promises);
  const elapsed = Date.now() - startTime;
  
  const successCount = results.filter(r => r).length;
  const failCount = results.filter(r => !r).length;
  
  console.log('\n=== Results ===');
  console.log('Total packets:', 500);
  console.log('Successfully enqueued:', successCount);
  console.log('Dropped:', failCount);
  console.log('Time elapsed:', elapsed, 'ms');
  
  const metrics = manager.getMetrics();
  console.log('\n=== Metrics ===');
  console.log('Queue length:', metrics.queueLength);
  console.log('Saturation:', (metrics.saturation * 100).toFixed(2) + '%');
  console.log('Dropped packets:', metrics.droppedPackets);
  console.log('Slowed down ingestions:', metrics.slowedDownIngestions);
  console.log('Average processing time:', metrics.averageProcessingTime.toFixed(2), 'ms');
  
  // Process all packets
  console.log('\nProcessing all packets...');
  while (manager.getQueueLength() > 0) {
    await manager.dequeue();
  }
  
  console.log('Final queue length:', manager.getQueueLength());
  console.log('\n✅ High load test completed');
}

testHighLoad().catch(console.error);
```

Run the high load test:
```bash
npx ts-node manual-test-load.ts
```

---

## Step 6: Verify Backpressure Behavior

### Expected Behavior Verification

1. **Slow Down Threshold (70%)**:
   - When queue reaches 70% capacity, ingestion should slow down
   - Check console logs for "Slowing down ingestion" messages
   - Verify `slowedDownIngestions` metric increases

2. **Drop Threshold (90%)**:
   - When queue reaches 90% capacity, METRIC priority packets should be dropped
   - Verify `droppedPackets` metric increases
   - STANDARD and CRITICAL packets should still be accepted

3. **Queue Capacity (1000 default)**:
   - Queue should never exceed max capacity
   - Critical packets wait for space when full
   - Non-critical packets are dropped when full

4. **Metrics Accuracy**:
   - `queueLength` should match actual queue size
   - `saturation` should be queueLength / maxCapacity
   - `droppedPackets` should count actual drops
   - `slowedDownIngestions` should count actual slowdowns

---

## Step 7: Production Readiness Check

Verify the implementation meets production requirements:

```bash
# Check TypeScript compilation
npm run build

# Run linting
npm run lint

# Run all tests
npm test

# Check for any compilation errors in the backpressure module
npx tsc --noEmit src/queue/backpressure.ts
```

---

## Step 8: API Endpoint Verification (Optional)

If you want to add an API endpoint to monitor backpressure metrics:

Add to your routes (e.g., `src/routes/status.ts`):

```typescript
import { marketRateService } from '../services/marketRate/marketRateService';

router.get('/backpressure', (req, res) => {
  const metrics = marketRateService.getBackpressureMetrics();
  res.json({
    success: true,
    data: metrics,
  });
});
```

Test the endpoint:
```bash
curl http://localhost:3000/api/v1/status/backpressure
```

Expected response:
```json
{
  "success": true,
  "data": {
    "queueLength": 5,
    "maxCapacity": 1000,
    "saturation": 0.005,
    "droppedPackets": 0,
    "slowedDownIngestions": 0,
    "averageProcessingTime": 2.5
  }
}
```

---

## Troubleshooting

### Issue: Tests timeout
**Solution**: Increase timeout in jest.config.ts or use `--testTimeout=30000`

### Issue: Queue doesn't slow down
**Solution**: Verify `slowDownThreshold` is set correctly (0-1 range) and queue is filling above threshold

### Issue: Packets not being dropped
**Solution**: Verify `dropThreshold` is set correctly and packets have METRIC priority

### Issue: Integration test fails
**Solution**: Ensure MarketRateService is properly initialized and backpressure manager is instantiated

---

## Success Criteria

You have successfully completed the assignment if:

✅ All unit tests pass (test/backpressure.test.ts)
✅ Manual tests demonstrate correct queue behavior
✅ Backpressure slows down ingestion at 70% capacity
✅ Backpressure drops metric packets at 90% capacity
✅ Queue never exceeds max capacity (1000)
✅ Metrics accurately track queue state
✅ Integration with MarketRateService works correctly
✅ TypeScript compiles without errors
✅ No memory leaks or performance degradation under load

---

## Summary

The backpressure queue implementation provides:

1. **Bounded execution buffer queue** (AsyncBoundedQueue) with max capacity 1000
2. **Backpressure rules** that slow down ingestion at 70% capacity
3. **Drop-tail strategy** that drops metric packets at 90% capacity
4. **Priority-based handling** where critical packets wait for space
5. **Comprehensive metrics** for monitoring queue health
6. **Integration** with MarketRateService ingestion loops

This implementation prevents queue overflow and out-of-memory errors during network lag on external validator endpoints, as specified in issue #340.
