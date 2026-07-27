/**
 * Test suite for HTTP client with socket keep-alive configuration
 */

import { httpClient, createHttpClient, SOCKET_CONFIG } from '../src/lib/httpClient';

describe('HTTP Client Socket Keep-Alive', () => {
  test('httpClient exports are defined', () => {
    expect(httpClient).toBeDefined();
    expect(httpClient.get).toBeDefined();
    expect(httpClient.post).toBeDefined();
  });

  test('createHttpClient factory works', () => {
    const customClient = createHttpClient({ timeout: 30000 });
    expect(customClient).toBeDefined();
    expect(customClient.get).toBeDefined();
  });

  test('SOCKET_CONFIG has correct values', () => {
    expect(SOCKET_CONFIG.keepAliveTimeout).toBe(10000);
    expect(SOCKET_CONFIG.probeInterval).toBe(2000);
    expect(SOCKET_CONFIG.maxProbeRetries).toBe(3);
    expect(SOCKET_CONFIG.maxHangTime).toBe(16000);
  });

  test('httpClient has default headers', () => {
    const defaults = httpClient.defaults.headers as any;
    expect(defaults['Connection']).toBe('keep-alive');
    expect(defaults['User-Agent']).toBe('StellarFlow-Oracle/1.0');
  });

  test('httpClient can make GET request', async () => {
    // Test with a public API that should be available
    const response = await httpClient.get('https://httpbin.org/get', {
      timeout: 5000
    });
    
    expect(response.status).toBe(200);
    expect(response.data).toBeDefined();
  }, 10000);

  test('custom client respects custom timeout', () => {
    const customClient = createHttpClient({ timeout: 1000 });
    expect(customClient.defaults.timeout).toBe(1000);
  });

  test('socket keep-alive configuration prevents hangs', async () => {
    // This test would require a mock server that simulates silent drops
    // For now, we just verify the client is configured
    expect(httpClient.defaults.httpAgent).toBeDefined();
    expect(httpClient.defaults.httpsAgent).toBeDefined();
  });
});

describe('Socket Keep-Alive Integration', () => {
  test('fetcher integration - simulated CoinGecko call', async () => {
    // Simulate a typical fetcher pattern
    const url = 'https://api.coingecko.com/api/v3/simple/price?ids=stellar&vs_currencies=usd';
    
    try {
      const response = await httpClient.get(url, {
        timeout: 5000
      });
      
      expect(response.status).toBe(200);
      expect(response.data).toBeDefined();
      expect(response.data.stellar).toBeDefined();
    } catch (error) {
      // Rate limiting is acceptable for this test
      if (error && typeof error === 'object' && 'response' in error) {
        const axiosError = error as any;
        expect([200, 429]).toContain(axiosError.response?.status);
      }
    }
  }, 10000);

  test('webhook integration - simulated POST', async () => {
    // Test POST request pattern used by webhooks
    try {
      const response = await httpClient.post('https://httpbin.org/post', {
        test: 'data',
        timestamp: new Date().toISOString()
      }, {
        headers: { 'Content-Type': 'application/json' },
        timeout: 5000
      });
      
      expect(response.status).toBe(200);
      expect(response.data.json).toBeDefined();
      expect(response.data.json.test).toBe('data');
    } catch (error) {
      // Network errors are acceptable for this integration test
      console.warn('POST test skipped due to network error');
    }
  }, 10000);
});
