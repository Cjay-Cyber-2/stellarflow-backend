# Testing Setup Guide

## Python Environment Setup

The RPC load balancer is implemented in Python and requires Python 3.9+ to run tests.

### Step 1: Install Python

**Windows:**
```powershell
# Option 1: Download from python.org
# Visit https://www.python.org/downloads/ and install Python 3.9+

# Option 2: Using winget (Windows 11)
winget install Python.Python.3.11

# Option 3: Using Chocolatey
choco install python
```

**Verify installation:**
```bash
python --version
# Should output: Python 3.9.x or higher
```

### Step 2: Install Dependencies

```bash
# Install project dependencies
pip install -r requirements.txt

# Install test dependencies
pip install pytest pytest-asyncio
```

### Step 3: Run Tests

#### Run All RPC Load Balancer Tests
```bash
pytest tests/test_nonce_tracker.py -k test_rpc_load_balancer -v
```

#### Run Specific Test
```bash
# Main acceptance criteria test
pytest tests/test_nonce_tracker.py::test_rpc_load_balancer -v

# Async behavior verification test
pytest tests/test_nonce_tracker.py::test_rpc_load_balancer_async_behavior -v
```

#### Run All Nonce Tracker Tests
```bash
pytest tests/test_nonce_tracker.py -v
```

#### Run with Coverage
```bash
pip install pytest-cov
pytest tests/test_nonce_tracker.py --cov=src.network.nonce_tracker --cov-report=html
```

## Expected Test Output

### Successful Test Run

```
tests/test_nonce_tracker.py::test_rpc_load_balancer PASSED                   [ 90%]
tests/test_nonce_tracker.py::test_rpc_load_balancer_async_behavior PASSED    [100%]

============================== 2 passed in 1.23s ===============================
```

### Test Details

#### Test 1: `test_rpc_load_balancer`
**Validates:**
- ✅ Unhealthy nodes detected within 100ms
- ✅ Round-robin distribution across healthy endpoints
- ✅ Unhealthy nodes bypassed automatically
- ✅ Non-blocking endpoint selection (<10ms for 100 calls)
- ✅ Failure detection speed

**Duration:** ~0.8s

#### Test 2: `test_rpc_load_balancer_async_behavior`
**Validates:**
- ✅ Health checks don't block transaction routing
- ✅ 100 endpoint selections complete in <100ms
- ✅ Background monitoring runs independently

**Duration:** ~0.6s

## Troubleshooting

### Issue: `pytest: command not found`

**Solution:**
```bash
# Ensure pip bin directory is in PATH
python -m pip install --upgrade pip
python -m pip install pytest pytest-asyncio

# Use python -m pytest instead
python -m pytest tests/test_nonce_tracker.py -k test_rpc_load_balancer -v
```

### Issue: `ModuleNotFoundError: No module named 'aiohttp'`

**Solution:**
```bash
pip install -r requirements.txt
```

### Issue: `ModuleNotFoundError: No module named 'pytest'`

**Solution:**
```bash
pip install pytest pytest-asyncio
```

### Issue: Tests timeout or hang

**Possible Causes:**
- Network connectivity issues preventing async operations
- Event loop conflicts

**Solution:**
```bash
# Run with increased timeout
pytest tests/test_nonce_tracker.py::test_rpc_load_balancer -v --timeout=30

# Run with verbose async debugging
pytest tests/test_nonce_tracker.py::test_rpc_load_balancer -v -s
```

### Issue: Mock-related errors

**Solution:**
```bash
# Ensure unittest.mock is available (Python 3.3+)
python --version  # Should be 3.9+

# Install backport if needed (Python < 3.3)
pip install mock
```

## CI/CD Integration

### GitHub Actions

```yaml
name: Python Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    
    steps:
    - uses: actions/checkout@v3
    
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.11'
    
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        pip install pytest pytest-asyncio pytest-cov
    
    - name: Run RPC Load Balancer Tests
      run: |
        pytest tests/test_nonce_tracker.py::test_rpc_load_balancer -v
        pytest tests/test_nonce_tracker.py::test_rpc_load_balancer_async_behavior -v
    
    - name: Run All Tests
      run: |
        pytest tests/ --cov=src --cov-report=xml
    
    - name: Upload Coverage
      uses: codecov/codecov-action@v3
```

### GitLab CI

```yaml
test:
  image: python:3.11
  
  before_script:
    - pip install -r requirements.txt
    - pip install pytest pytest-asyncio pytest-cov
  
  script:
    - pytest tests/test_nonce_tracker.py -k test_rpc_load_balancer -v --cov=src
  
  coverage: '/(?i)total.*? (100(?:\.0+)?\%|[1-9]?\d(?:\.\d+)?\%)$/'
```

## Manual Verification

If automated tests cannot run, you can manually verify the implementation:

### 1. Code Review Checklist

```
✅ Async health checks using aiohttp (non-blocking)
✅ Parallel ping operations via asyncio.gather()
✅ Round-robin endpoint selection with _current_index
✅ Sub-100ms timeout (ping_timeout_sec=0.1)
✅ Background monitoring thread with dedicated event loop
✅ Non-blocking get_active_endpoint() method (O(1) lookup)
✅ Automatic unhealthy node bypass
✅ Graceful startup/shutdown with cleanup
```

### 2. Integration Test

```python
# Create test script: test_integration.py
import time
from src.network.nonce_tracker import RPCNodeFailoverSupervisor

# Initialize supervisor
supervisor = RPCNodeFailoverSupervisor(
    endpoints=[
        "https://horizon.stellar.org",
        "https://horizon-testnet.stellar.org"
    ],
    ping_timeout_sec=0.1
)

# Start monitoring
supervisor.start()
time.sleep(0.5)  # Allow first health check

# Test non-blocking endpoint retrieval
start = time.monotonic()
for _ in range(100):
    endpoint = supervisor.get_active_endpoint()
    print(f"Selected: {endpoint}")
elapsed = (time.monotonic() - start) * 1000

print(f"\n✅ 100 endpoint selections completed in {elapsed:.2f}ms")
assert elapsed < 100, "Selection should be non-blocking"

# Cleanup
supervisor.stop()
```

**Run:**
```bash
python test_integration.py
```

**Expected Output:**
```
Selected: https://horizon.stellar.org
Selected: https://horizon-testnet.stellar.org
...
✅ 100 endpoint selections completed in 8.43ms
```

## Dependencies

### Runtime
- Python 3.9+
- aiohttp >= 3.9.0 (async HTTP client)

### Testing
- pytest >= 7.0.0
- pytest-asyncio >= 0.21.0 (for async test support)

### Optional
- pytest-cov (coverage reporting)
- pytest-timeout (test timeout enforcement)
- pytest-xdist (parallel test execution)

## Quick Start

```bash
# 1. Install Python dependencies
pip install -r requirements.txt
pip install pytest pytest-asyncio

# 2. Run the acceptance test
pytest tests/test_nonce_tracker.py::test_rpc_load_balancer -v

# 3. Verify output shows PASSED
# tests/test_nonce_tracker.py::test_rpc_load_balancer PASSED

# 4. Check implementation logs
grep "RPCNodeFailoverSupervisor" logs/*.log
```

## Performance Benchmarks

Run performance tests:
```bash
pytest tests/test_nonce_tracker.py::test_rpc_load_balancer -v --benchmark-only
```

**Expected Benchmarks:**
- Endpoint selection: <1ms per call
- Health check cycle: <100ms for all nodes
- Failure detection: <100ms from node failure
- No blocking during health checks

## Success Criteria

All tests should pass with:
- ✅ No syntax errors
- ✅ No runtime exceptions
- ✅ All assertions pass
- ✅ Performance criteria met (<100ms failure detection, non-blocking)
- ✅ Code coverage >90% for RPCNodeFailoverSupervisor

## Support

If tests fail or issues arise:
1. Check Python version: `python --version` (should be 3.9+)
2. Verify dependencies: `pip list | grep -E "aiohttp|pytest"`
3. Review logs for error details
4. Check network connectivity to RPC endpoints
5. Consult `RPC_LOAD_BALANCER_IMPLEMENTATION.md` for troubleshooting
