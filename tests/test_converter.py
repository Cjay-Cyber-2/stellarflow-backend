import pytest
from src.analytics.converter import (
    decode_soroban_i128,
    decode_soroban_u128,
    encode_soroban_i128,
    encode_soroban_u128,
    soroban_i128_to_decimal,
    soroban_u128_to_decimal,
    decimal_to_soroban_i128,
    decimal_to_soroban_u128,
)


# ---------------------------------------------------------------------------
# Issue #636 — Fixed-Point Decimal Truncation Guard for Soroban Event Payloads
# ---------------------------------------------------------------------------


def test_soroban_event_truncation_roundtrip_signed():
    """Signed 128-bit values round-trip through encode/decode without loss."""
    test_values = [
        0,
        1,
        -1,
        12345678901234567890,
        -98765432109876543210,
        2 ** 127 - 1,
        -(2 ** 127),
    ]

    for value in test_values:
        lo, hi = encode_soroban_i128(value)
        decoded = decode_soroban_i128(lo, hi)
        assert decoded == value, f"Round-trip failed for {value}: got {decoded}"


def test_soroban_event_truncation_roundtrip_unsigned():
    """Unsigned 128-bit values round-trip through encode/decode without loss."""
    test_values = [
        0,
        1,
        12345678901234567890,
        2 ** 128 - 1,
    ]

    for value in test_values:
        lo, hi = encode_soroban_u128(value)
        decoded = decode_soroban_u128(lo, hi)
        assert decoded == value, f"Round-trip failed for {value}: got {decoded}"


def test_soroban_i128_out_of_range():
    """Values outside signed 128-bit range raise ValueError."""
    with pytest.raises(ValueError):
        encode_soroban_i128(2 ** 127)
    with pytest.raises(ValueError):
        encode_soroban_i128(-(2 ** 127) - 1)


def test_soroban_u128_out_of_range():
    """Values outside unsigned 128-bit range raise ValueError."""
    with pytest.raises(ValueError):
        encode_soroban_u128(-1)
    with pytest.raises(ValueError):
        encode_soroban_u128(2 ** 128)


def test_soroban_i128_to_decimal_precision():
    """i128 values convert to Decimal with exact precision."""
    from decimal import Decimal

    value_lo = 123456789
    value_hi = 0
    precision = 7
    expected = Decimal("12.3456789")

    result = soroban_i128_to_decimal(value_lo, value_hi, precision)
    assert result == expected


def test_soroban_u128_to_decimal_precision():
    """u128 values convert to Decimal with exact precision."""
    from decimal import Decimal

    value_lo = 123456789
    value_hi = 0
    precision = 7
    expected = Decimal("12.3456789")

    result = soroban_u128_to_decimal(value_lo, value_hi, precision)
    assert result == expected


def test_decimal_to_soroban_i128_roundtrip():
    """Decimal -> i128 -> Decimal preserves the original value."""
    from decimal import Decimal

    value = Decimal("12345.6789")
    precision = 7

    lo, hi = decimal_to_soroban_i128(value, precision)
    result = soroban_i128_to_decimal(lo, hi, precision)
    assert result == value


def test_decimal_to_soroban_u128_roundtrip():
    """Decimal -> u128 -> Decimal preserves the original value."""
    from decimal import Decimal
    value = Decimal("12345.6789")
    precision = 7

    lo, hi = decimal_to_soroban_u128(value, precision)
    result = soroban_u128_to_decimal(lo, hi, precision)
    assert result == value


def test_soroban_128_balance_no_rounding_loss():
    """Event payload balances decode without fractional rounding loss."""
    # Simulate a balance of 1_234_567_890.1234567 (SCALE_7)
    from decimal import Decimal

    balance_int = 1_234_567_890_123_456_7
    lo, hi = encode_soroban_u128(balance_int)
    decoded_raw = decode_soroban_u128(lo, hi)
    assert decoded_raw == balance_int

    decimal_value = soroban_u128_to_decimal(lo, hi, precision=7)
    assert decimal_value == Decimal("1234567890.1234567")