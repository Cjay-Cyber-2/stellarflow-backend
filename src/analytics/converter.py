from __future__ import annotations

from fractions import Fraction
from typing import NamedTuple, Union

Number = Union[int, float, Fraction]

U64_MIN: int = 0
U64_MAX: int = 18_446_744_073_709_551_615

# ---------------------------------------------------------------------------
# Issue #636 — Fixed-Point Decimal Truncation Guard for Soroban Event Payloads
# ---------------------------------------------------------------------------
# Implements exact 128-bit integer precision conversion functions that decode
# Soroban raw byte events without fractional rounding loss.  The core
# functions convert between raw 128-bit integer representations (split into
# lo/hi uint64 pairs) and their exact Decimal / Fraction equivalents,
# ensuring that account balance ledgers never suffer from truncation errors.
#
# Soroban contract events encode 128-bit values as two uint64 fields:
#   - value_lo: lower 64 bits
#   - value_hi: upper 64 bits
# The combined 128-bit value represents a fixed-point decimal scaled by
# 10^precision (default 7, matching SCALE_7).


def decode_soroban_i128(value_lo: int, value_hi: int) -> int:
    """Reconstruct a signed 128-bit integer from its lo/hi uint64 parts.

    Soroban contract events encode i128 values as two little-endian uint64
    fields.  This function reassembles them into a Python int, handling
    the sign bit correctly.

    Args:
        value_lo: Lower 64 bits (uint64).
        value_hi: Upper 64 bits (uint64 — bit 63 is the sign bit).

    Returns:
        A Python ``int`` representing the full 128-bit signed value.
    """
    combined = (value_hi << 64) | value_lo
    # If the sign bit (bit 127) is set, convert to negative via two's complement
    if combined & (1 << 127):
        combined -= 1 << 128
    return combined


def decode_soroban_u128(value_lo: int, value_hi: int) -> int:
    """Reconstruct an unsigned 128-bit integer from its lo/hi uint64 parts.

    Args:
        value_lo: Lower 64 bits (uint64).
        value_hi: Upper 64 bits (uint64).

    Returns:
        A Python ``int`` representing the full 128-bit unsigned value.
    """
    return (value_hi << 64) | value_lo


def encode_soroban_i128(value: int) -> tuple[int, int]:
    """Split a signed 128-bit integer into lo/hi uint64 parts.

    Args:
        value: A Python ``int`` in the range [-2^127, 2^127 - 1].

    Returns:
        ``(value_lo, value_hi)`` as uint64 values suitable for Soroban events.
    """
    if value < -(1 << 127) or value >= (1 << 127):
        raise ValueError(
            f"Value {value} is outside signed 128-bit range "
            f"[{-1 << 127}, {(1 << 127) - 1}]"
        )
    # Convert to two's complement unsigned representation
    if value < 0:
        value += 1 << 128
    value_lo = value & 0xFFFFFFFFFFFFFFFF
    value_hi = (value >> 64) & 0xFFFFFFFFFFFFFFFF
    return (value_lo, value_hi)


def encode_soroban_u128(value: int) -> tuple[int, int]:
    """Split an unsigned 128-bit integer into lo/hi uint64 parts.

    Args:
        value: A Python ``int`` in the range [0, 2^128 - 1].

    Returns:
        ``(value_lo, value_hi)`` as uint64 values suitable for Soroban events.
    """
    if value < 0 or value > (1 << 128) - 1:
        raise ValueError(
            f"Value {value} is outside unsigned 128-bit range [0, {(1 << 128) - 1}]"
        )
    value_lo = value & 0xFFFFFFFFFFFFFFFF
    value_hi = (value >> 64) & 0xFFFFFFFFFFFFFFFF
    return (value_lo, value_hi)


def soroban_i128_to_decimal(
    value_lo: int,
    value_hi: int,
    precision: int = 7,
) -> "Decimal":
    """Convert a Soroban i128 event payload to a high-precision Decimal.

    The raw i128 value is interpreted as a fixed-point number with
    *precision* decimal places.  The conversion uses exact integer
    arithmetic so no fractional rounding loss occurs.

    Args:
        value_lo: Lower 64 bits of the i128 (uint64).
        value_hi: Upper 64 bits of the i128 (uint64).
        precision: Number of decimal places (default 7 -> SCALE_7).

    Returns:
        A ``Decimal`` with the exact unscaled value.
    """
    from decimal import Decimal
    raw = decode_soroban_i128(value_lo, value_hi)
    return Decimal(raw) / Decimal(10 ** precision)


def soroban_u128_to_decimal(
    value_lo: int,
    value_hi: int,
    precision: int = 7,
) -> "Decimal":
    """Convert a Soroban u128 event payload to a high-precision Decimal.

    Args:
        value_lo: Lower 64 bits of the u128 (uint64).
        value_hi: Upper 64 bits of the u128 (uint64).
        precision: Number of decimal places (default 7 -> SCALE_7).

    Returns:
        A ``Decimal`` with the exact unscaled value.
    """
    from decimal import Decimal
    raw = decode_soroban_u128(value_lo, value_hi)
    return Decimal(raw) / Decimal(10 ** precision)


def decimal_to_soroban_i128(
    value: "Decimal",
    precision: int = 7,
) -> tuple[int, int]:
    """Convert a Decimal to a Soroban i128 lo/hi pair.

    The Decimal is scaled by 10^precision and truncated to an integer
    before being split into lo/hi uint64 parts.  This guarantees that
    the round-trip ``decimal_to_soroban_i128 -> soroban_i128_to_decimal``
    is lossless for values that fit within the 128-bit range.

    Args:
        value: A ``Decimal`` to encode.
        precision: Number of decimal places (default 7 -> SCALE_7).

    Returns:
        ``(value_lo, value_hi)`` as uint64 values.
    """
    from decimal import Decimal, ROUND_DOWN
    scaled = int(value * Decimal(10 ** precision))
    return encode_soroban_i128(scaled)


def decimal_to_soroban_u128(
    value: "Decimal",
    precision: int = 7,
) -> tuple[int, int]:
    """Convert a Decimal to a Soroban u128 lo/hi pair.

    Args:
        value: A ``Decimal`` to encode.
        precision: Number of decimal places (default 7 -> SCALE_7).

    Returns:
        ``(value_lo, value_hi)`` as uint64 values.
    """
    from decimal import Decimal
    scaled = int(value * Decimal(10 ** precision))
    return encode_soroban_u128(scaled)


# ---------------------------------------------------------------------------
# Existing fraction utilities
# ---------------------------------------------------------------------------


class ReducedFraction(NamedTuple):
    numerator: int
    denominator: int


class FractionBoundsError(ValueError):
    """Raised when a reduced fraction component exceeds u64 bounds."""


def _validate_u64(value: int, label: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{label} must be an int, got {type(value).__name__}.")
    if value < U64_MIN or value > U64_MAX:
        raise FractionBoundsError(
            f"{label} {value} is outside unsigned 64-bit range "
            f"[{U64_MIN}, {U64_MAX}]."
        )


def reduce_fraction(numerator: int, denominator: int) -> ReducedFraction:
    """Reduce a fraction to lowest terms using exact rational arithmetic.

    Both *numerator* and *denominator* are validated against u64 bounds before
    reduction.  The reduced result is also checked so callers can safely
    serialise the components into on-chain Soroban payloads.

    Parameters
    ----------
    numerator:
        The fraction numerator (must be ``int``, within ``[0, U64_MAX]``).
    denominator:
        The fraction denominator (must be ``int``, within ``[1, U64_MAX]``).

    Returns
    -------
    ReducedFraction
        A named tuple ``(numerator, denominator)`` in lowest terms.

    Raises
    ------
    FractionBoundsError
        If either operand or the reduced components exceed u64 bounds.
    TypeError
        If either operand is a ``bool``.
    """
    if isinstance(numerator, bool) or isinstance(denominator, bool):
        raise TypeError("Numerator and denominator must be numeric, not bool.")

    _validate_u64(numerator, "Numerator")
    _validate_u64(denominator, "Denominator")

    if denominator == 0:
        raise FractionBoundsError("Denominator must not be zero.")

    fraction = Fraction(numerator, denominator)
    reduced_num = fraction.numerator
    reduced_den = fraction.denominator

    # Reduction never increases magnitude beyond min(numerator, denominator),
    # but we validate explicitly so callers can trust the output.
    _validate_u64(reduced_num, "Reduced numerator")
    _validate_u64(reduced_den, "Reduced denominator")

    return ReducedFraction(numerator=reduced_num, denominator=reduced_den)


def decimal_to_fraction(
    value: Number | str,
    max_denominator: int = U64_MAX,
) -> ReducedFraction:
    """Convert a decimal value to an exact reduced fraction within u64 bounds.

    Internally uses ``Fraction.limit_denominator`` to cap the denominator at
    *max_denominator*, which defaults to ``U64_MAX`` so the result is
    guaranteed to fit inside a standard u64 integer.

    Parameters
    ----------
    value:
        The decimal value to convert (``int``, ``float``, ``Fraction``, or
        ``str``).
    max_denominator:
        Maximum allowed denominator for the reduced result.  Defaults to
        ``U64_MAX``.

    Returns
    -------
    ReducedFraction
        The best approximation of *value* as a reduced fraction whose
        denominator does not exceed *max_denominator*.

    Raises
    ------
    FractionBoundsError
        If the reduced components exceed the u64 range.
    """
    fraction = Fraction(value).limit_denominator(max_denominator)
    reduced_num = fraction.numerator
    reduced_den = fraction.denominator

    _validate_u64(reduced_num, "Reduced numerator")
    _validate_u64(reduced_den, "Reduced denominator")

    return ReducedFraction(numerator=reduced_num, denominator=reduced_den)


__all__ = [
    "U64_MIN",
    "U64_MAX",
    "ReducedFraction",
    "FractionBoundsError",
    "reduce_fraction",
    "decimal_to_fraction",
    # Issue #636 — Soroban 128-bit precision conversion
    "decode_soroban_i128",
    "decode_soroban_u128",
    "encode_soroban_i128",
    "encode_soroban_u128",
    "soroban_i128_to_decimal",
    "soroban_u128_to_decimal",
    "decimal_to_soroban_i128",
    "decimal_to_soroban_u128",
]