# Quick Guide: Running Circuit Breaker Tests

## Prerequisites

You need Python 3.8+ installed on your system to run the circuit breaker tests.

### Check if Python is Installed

Try one of these commands:
```bash
python --version
python3 --version
py --version
```

If none of these work, you need to install Python.

### Install Python (if needed)

**Windows:**
1. Download Python from https://www.python.org/downloads/
2. Run the installer
3. ✅ **IMPORTANT**: Check "Add Python to PATH" during installation
4. Restart your terminal after installation

**Alternative for Windows:**
```bash
winget install Python.Python.3.12
```

## Install Dependencies

Once Python is installed, run:

```bash
pip install pytest pytest-asyncio
```

Or if you have `pip3`:
```bash
pip3 install pytest pytest-asyncio
```

## Run the Tests

### Run All Circuit Breaker Tests
```bash
pytest tests/test_backpressure.py -v
```

### Run Only State Machine Tests (Acceptance Criteria)
```bash
pytest tests/test_backpressure.py -k test_circuit_breaker_states -v
```

### Run Specific Test Classes
```bash
# Test state transitions
pytest tests/test_backpressure.py::TestCircuitBreakerStates -v

# Test registry functionality  
pytest tests/test_backpressure.py::TestCircuitBreakerRegistry -v

# Test integration scenarios
pytest tests/test_backpressure.py::TestCircuitBreakerIntegration -v
```

### Run a Single Specific Test
```bash
pytest tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_closed_to_open_by_consecutive_failures -v
```

## Expected Output

When tests pass, you should see:
```
tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_initial_state PASSED
tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_closed_to_open_by_consecutive_failures PASSED
tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_closed_to_open_by_error_rate PASSED
tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_open_rejects_requests PASSED
tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_open_to_half_open_after_timeout PASSED
tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_half_open_to_closed_on_success PASSED
tests/test_backpressure.py::TestCircuitBreakerStates::test_circuit_breaker_half_open_to_open_on_failure PASSED
...

===================== X passed in Y.YY seconds =====================
```

## Troubleshooting

### "pytest is not recognized"
**Solution**: Install pytest first:
```bash
pip install pytest pytest-asyncio
```

### "Python was not found"
**Solution**: Install Python and ensure it's added to PATH. After installation, restart your terminal.

### "ModuleNotFoundError: No module named 'src'"
**Solution**: Make sure you're running pytest from the project root directory (`stellarflow-backend/`):
```bash
cd c:\Users\Nana Abdul\Documents\stellarflow-backend
pytest tests/test_backpressure.py -v
```

### Tests Timeout
**Solution**: Some tests use `time.sleep()` for state transitions. If tests timeout, increase the timeout:
```bash
pytest tests/test_backpressure.py --timeout=30 -v
```

### Import Errors
**Solution**: The `conftest.py` file should automatically configure the Python path. If you still get import errors, you can manually add the src directory:
```bash
set PYTHONPATH=%PYTHONPATH%;c:\Users\Nana Abdul\Documents\stellarflow-backend\src
pytest tests/test_backpressure.py -v
```

## Verification Checklist

After running the tests, verify:

- ✅ All tests pass (green checkmarks)
- ✅ State transitions work: CLOSED → OPEN → HALF_OPEN → CLOSED
- ✅ Circuit breaker rejects requests instantly when OPEN
- ✅ Error thresholds trigger state changes correctly
- ✅ Metrics are tracked accurately
- ✅ Thread-safe concurrent operations work
- ✅ Registry manages multiple circuit breakers

## Next Steps After Tests Pass

1. **Integrate with RPC clients**: Wrap RPC calls with circuit breaker pattern
2. **Configure thresholds**: Adjust failure thresholds based on your service SLAs
3. **Add monitoring**: Set up alerts for circuit state changes
4. **Deploy**: Roll out gradually with monitoring

## Additional Resources

- Implementation details: `CIRCUIT_BREAKER_IMPLEMENTATION.md`
- Source code: `src/queue/backpressure.py`
- Test file: `tests/test_backpressure.py`
- Backpressure guide: `BACKPRESSURE_TESTING_GUIDE.md`

---

**Quick Command Reference:**

```bash
# Install dependencies
pip install pytest pytest-asyncio

# Run acceptance criteria tests
pytest tests/test_backpressure.py -k test_circuit_breaker_states -v

# Run all circuit breaker tests
pytest tests/test_backpressure.py -v

# Run with detailed output
pytest tests/test_backpressure.py -vv --tb=short
```
