import logging
import re

logger = logging.getLogger(__name__)

# Compiled once at module load — O(1) amortised across all subsequent calls.
_HEX_RE = re.compile(r'^[0-9A-F]+$')


def normalize_hex(raw: str) -> str:
    """Sanitize and format an inbound hex string to clean uppercase with no prefix.

    Handles all common forms arriving from telemetry sources:
    - Leading / trailing whitespace
    - Optional ``0x`` or ``0X`` prefix
    - Mixed-case characters (``DeAdBeEf`` → ``DEADBEEF``)

    Rejects strings that are empty after sanitization, contain non-hex
    characters, or carry an odd character count (which cannot represent a
    whole number of bytes and indicates a malformed payload).

    Args:
        raw: Inbound hex string from a telemetry record.

    Returns:
        Uppercase hex string, no prefix, guaranteed non-empty and even-length.

    Raises:
        TypeError:  If *raw* is not a string.
        ValueError: If *raw* is empty, contains non-hex characters, or has an
                    odd character count after normalization.

    Time : O(n) where n is len(raw).
    Space: O(n) for the uppercased copy.
    """
    if not isinstance(raw, str):
        raise TypeError(f"Expected str, got {type(raw).__name__}.")

    value = raw.strip()

    if not value:
        raise ValueError("Hex string must not be empty or whitespace-only.")

    # Strip the optional 0x / 0X prefix before character validation.
    if value[:2] in ("0x", "0X"):
        value = value[2:]

    if not value:
        raise ValueError("Hex string contains only a prefix and no hex data.")

    value = value.upper()

    if not _HEX_RE.match(value):
        raise ValueError(f"Hex string contains invalid characters: {value!r}")

    if len(value) % 2 != 0:
        raise ValueError(
            f"Hex string has odd length ({len(value)}); cannot decode to whole bytes."
        )

    return value


def validate_hex(raw: str) -> bool:
    """Return True if *raw* is a well-formed hex string, False otherwise.

    Never raises; all error paths are suppressed. Use normalize_hex() when
    you need the cleaned value or a descriptive error message.

    Time : O(n).
    Space: O(n) for the internal normalization copy.
    """
    try:
        normalize_hex(raw)
        return True
    except (TypeError, ValueError):
        return False


def hex_to_bytes(raw: str) -> bytes:
    """Normalize *raw* and decode it to bytes.

    Combines normalization and conversion in one step so callers at the
    ingestion boundary do not need to call normalize_hex() separately.

    Args:
        raw: Inbound hex string (prefix optional, any casing, any whitespace).

    Returns:
        Decoded bytes.

    Raises:
        TypeError:  Propagated from normalize_hex() for non-string input.
        ValueError: Propagated from normalize_hex() for malformed input.

    Time : O(n).
    Space: O(n) for the intermediate normalized string and the resulting bytes.
    """
    return bytes.fromhex(normalize_hex(raw))


def bytes_to_hex(data: bytes) -> str:
    """Encode *data* to an uppercase hex string with no prefix.

    Args:
        data: Raw bytes to encode.

    Returns:
        Uppercase hex string (e.g. ``"DEADBEEF"``).

    Raises:
        TypeError: If *data* is not bytes or bytearray.

    Time : O(n) where n is len(data).
    Space: O(n) for the hex string.
    """
    if not isinstance(data, (bytes, bytearray)):
        raise TypeError(f"Expected bytes or bytearray, got {type(data).__name__}.")

    return data.hex().upper()
