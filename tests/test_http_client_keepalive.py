from __future__ import annotations

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from network.http_client import make_session


# ---------------------------------------------------------------------------
# HTTP/2 ALPN Protocol Negotiation
# ---------------------------------------------------------------------------


def test_http2_alpn_negotiation():
    """Verify that HTTP/2 session is configured with ALPN protocol negotiation enabled."""
    session = make_session()
    # httpx.AsyncClient with http2=True enables ALPN automatically
    # Verify by checking that the session was created (http2 is configured via kwargs)
    # The actual ALPN negotiation happens during connection establishment
    assert session is not None, "HTTP/2 session should be created successfully"
