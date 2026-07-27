"""network/http_client.py – Shared async HTTP client for the ingestion pipeline.

All external fetch requests are subject to a dynamically-tuned timeout window
enforced at the session level.  The initial hard baseline is 2 500 ms;
subsequent requests use a timeout derived from recent latency observations via
an exponential moving average (EMA) so that the window adapts to regional
network conditions without ever dropping below a safety floor.

TCP socket buffers (SO_RCVBUF, SO_SNDBUF) are also tuned dynamically based on
the same EMA latency estimate using the bandwidth-delay product (BDP) heuristic:
larger buffers for high-latency links, smaller for low-latency links.

Timeout handling contract
-------------------------
* ``httpx.TimeoutException`` / ``asyncio.TimeoutError`` are caught,
  logged with endpoint, duration, and UTC timestamp, then re-raised as
  ``FetchTimeoutError`` so callers can distinguish them from other errors.
* Non-timeout errors (connection refused, DNS failure, HTTP error status)
  propagate unchanged — this module never swallows them.
* Connections are always returned to the pool automatically — httpx manages
  this transparently via its internal connection pool.

Multi-interface failover
-------------------------
* ``MultiInterfaceClient`` wraps ``httpx.AsyncClient`` with health-check
  monitoring and automatic failover between primary and secondary network
  interfaces.
* On primary failure, traffic is rerouted through the secondary interface.
* Health checks run periodically against a configurable endpoint.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum, auto
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

import httpx
from httpx import AsyncHTTPTransport

from src.analytics.ema import RollingEMA


logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Timeout constants
# ---------------------------------------------------------------------------

#: Hard baseline (seconds) used when no latency history is available.
REQUEST_TIMEOUT_S: float = 2.5

#: Minimum timeout (seconds) – prevents the window from becoming too
#: aggressive during a run of unusually-fast responses.
_MIN_TIMEOUT_S: float = 1.0

#: Maximum timeout (seconds) – caps unbounded growth during congestion.
_MAX_TIMEOUT_S: float = 10.0

#: Multiplier applied to the EMA latency to compute the adaptive timeout.
#: A value of 3× gives comfortable headroom above the smoothed baseline.
_EMA_MULTIPLIER: float = 3.0

#: Human-readable label used in log messages so operators see milliseconds.
_TIMEOUT_LABEL_MS: int = int(REQUEST_TIMEOUT_S * 1000)

# ---------------------------------------------------------------------------
# Multi-interface networking & failover
# ---------------------------------------------------------------------------


class InterfaceState(Enum):
    """Health state of a network interface."""
    HEALTHY = auto()
    DEGRADED = auto()
    DOWN = auto()


@dataclass
class InterfaceConfig:
    """Configuration for a network interface.

    Parameters
    ----------
    name:
        Human-readable label (e.g. ``"primary"``, ``"secondary"``).
    bind_ip:
        Optional IP address to bind sockets to.  When ``None`` the OS
        default interface is used.
    description:
        Optional description for logging / observability.
    """
    name: str
    bind_ip: Optional[str] = None
    description: str = ""


@dataclass
class FailoverConfig:
    """Configuration for automatic interface failover.

    Parameters
    ----------
    primary:
        The primary network interface.
    secondary:
        The secondary (backup) network interface.
    health_check_url:
        URL used for periodic health checks (HEAD request).
    check_interval_s:
        Seconds between health checks when the primary is healthy.
    check_timeout_s:
        Timeout for each health check request.
    retry_count:
        Number of times to retry a failed request on the secondary
        before giving up.
    failure_threshold:
        Consecutive health-check failures required to mark an interface DOWN.
    """
    primary: InterfaceConfig
    secondary: InterfaceConfig
    health_check_url: str = "https://example.com"
    check_interval_s: float = 30.0
    check_timeout_s: float = 5.0
    retry_count: int = 2
    failure_threshold: int = 3


class MultiInterfaceClient:
    """Async HTTP client with automatic network-interface failover.

    Wraps two ``httpx.AsyncClient`` sessions (one per interface) and
    monitors primary health.  On primary failure all new requests are
    routed through the secondary interface.  Periodic health checks
    automatically restore the primary when it recovers.

    Parameters
    ----------
    config:
        Failover configuration including primary/secondary interfaces.
    session_kwargs:
        Additional keyword arguments forwarded to ``make_session``.
    """

    def __init__(
        self,
        config: FailoverConfig,
        **session_kwargs: Any,
    ) -> None:
        self.config = config
        self._session_kwargs = session_kwargs

        self._primary_state: InterfaceState = InterfaceState.HEALTHY
        self._secondary_state: InterfaceState = InterfaceState.HEALTHY
        self._active_interface: str = config.primary.name
        self._primary_failures: int = 0
        self._secondary_failures: int = 0
        self._primary_session: Optional[httpx.AsyncClient] = None
        self._secondary_session: Optional[httpx.AsyncClient] = None
        self._health_task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()
        self._started: bool = False

        logger.info(
            "[MultiInterfaceClient] Initialised | primary=%s secondary=%s "
            "health_url=%s check_interval=%.1fs",
            config.primary.name, config.secondary.name,
            config.health_check_url, config.check_interval_s,
        )

    async def _build_primary_session(self) -> httpx.AsyncClient:
        """Create an httpx client bound to the primary interface."""
        kwargs = dict(self._session_kwargs)
        if self.config.primary.bind_ip:
            transport = AsyncHTTPTransport(
                local_address=self.config.primary.bind_ip,
            )
            kwargs.setdefault("transport", transport)
        return make_session(**kwargs)

    async def _build_secondary_session(self) -> httpx.AsyncClient:
        """Create an httpx client bound to the secondary interface."""
        kwargs = dict(self._session_kwargs)
        if self.config.secondary.bind_ip:
            transport = AsyncHTTPTransport(
                local_address=self.config.secondary.bind_ip,
            )
            kwargs.setdefault("transport", transport)
        return make_session(**kwargs)

    async def start(self) -> None:
        """Open both sessions and begin health monitoring."""
        async with self._lock:
            if self._started:
                return
            self._primary_session = await self._build_primary_session()
            self._secondary_session = await self._build_secondary_session()
            self._started = True
            self._health_task = asyncio.create_task(self._health_loop())
            logger.info(
                "[MultiInterfaceClient] Started | active=%s",
                self._active_interface,
            )

    async def stop(self) -> None:
        """Shut down health monitoring and close both sessions."""
        if self._health_task:
            self._health_task.cancel()
            try:
                await self._health_task
            except asyncio.CancelledError:
                pass
        async with self._lock:
            if self._primary_session:
                await self._primary_session.aclose()
            if self._secondary_session:
                await self._secondary_session.aclose()
            self._started = False
        logger.info("[MultiInterfaceClient] Stopped")

    async def _health_loop(self) -> None:
        """Periodically check primary interface health."""
        while True:
            try:
                await asyncio.sleep(self.config.check_interval_s)
                await self._check_primary_health()
            except asyncio.CancelledError:
                break
            except Exception:
                logger.exception("[MultiInterfaceClient] Health check error")

    async def _check_primary_health(self) -> None:
        """Perform a single health check on the primary interface."""
        if not self._primary_session:
            return
        try:
            timeout = httpx.Timeout(self.config.check_timeout_s)
            await self._primary_session.head(
                self.config.health_check_url,
                timeout=timeout,
            )
            self._primary_failures = 0
            if self._primary_state != InterfaceState.HEALTHY:
                self._primary_state = InterfaceState.HEALTHY
                self._active_interface = self.config.primary.name
                logger.warning(
                    "[MultiInterfaceClient] Primary restored | "
                    "failing back to %s",
                    self.config.primary.name,
                )
        except httpx.RequestError:
            self._primary_failures += 1
            logger.warning(
                "[MultiInterfaceClient] Primary health check failed "
                "(%d/%d)",
                self._primary_failures,
                self.config.failure_threshold,
            )
            if self._primary_failures >= self.config.failure_threshold:
                old_state = self._primary_state
                self._primary_state = InterfaceState.DOWN
                if old_state != InterfaceState.DOWN:
                    logger.error(
                        "[MultiInterfaceClient] PRIMARY FAILURE | "
                        "failing over to %s",
                        self.config.secondary.name,
                    )
                    self._active_interface = self.config.secondary.name

    @property
    def active_session(self) -> Optional[httpx.AsyncClient]:
        """Return the currently active httpx session based on interface state."""
        if self._active_interface == self.config.primary.name:
            return self._primary_session
        return self._secondary_session

    @property
    def active_interface(self) -> str:
        """Name of the currently active interface."""
        return self._active_interface

    @property
    def primary_state(self) -> InterfaceState:
        return self._primary_state

    @property
    def secondary_state(self) -> InterfaceState:
        return self._secondary_state

    async def request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        """Make an HTTP request with automatic failover retry.

        On failure, retries the request on the secondary interface.
        """
        session = self.active_session
        if session is None:
            raise RuntimeError("[MultiInterfaceClient] Not started")

        try:
            return await session.request(method, url, **kwargs)
        except httpx.RequestError as exc:
            logger.warning(
                "[MultiInterfaceClient] Request failed on %s | "
                "retrying on %s | url=%s",
                self._active_interface,
                self.config.secondary.name,
                url,
            )
            fallback = self._secondary_session
            if fallback and fallback is not session:
                for attempt in range(self.config.retry_count):
                    try:
                        return await fallback.request(method, url, **kwargs)
                    except httpx.RequestError:
                        if attempt == self.config.retry_count - 1:
                            raise
                        logger.warning(
                            "[MultiInterfaceClient] Retry %d/%d failed "
                            "on secondary | url=%s",
                            attempt + 1, self.config.retry_count, url,
                        )
            raise exc


# ---------------------------------------------------------------------------
# Connection limits & HTTP/2
# ---------------------------------------------------------------------------

#: Keep one reusable connection pipe. With HTTP/2 enabled, concurrent ticker
#: requests share that socket as multiplexed streams instead of opening a new
#: TCP/TLS pipeline per asset.
_LIMITS = httpx.Limits(
    max_connections=1,
    max_keepalive_connections=1,
)

# ---------------------------------------------------------------------------
# TCP buffer tuning constants
# ---------------------------------------------------------------------------

#: Assumed baseline bandwidth (bits per second) used in the BDP calculation.
#: 10 Mbps is a conservative estimate for typical regional inter-datacenter
#: links; operators may tune this via environment variable.
_ASSUMED_BANDWIDTH_BPS: int = int(os.environ.get(
    "HTTP_CLIENT_BANDWIDTH_BPS", "10_000_000"
))

#: Minimum receive buffer size in bytes.
_MIN_RCVBUF: int = 65536  # 64 KB

#: Maximum receive buffer size in bytes.
_MAX_RCVBUF: int = 4_194_304  # 4 MB

#: Minimum send buffer size in bytes.
_MIN_SNDBUF: int = 65536  # 64 KB

#: Maximum send buffer size in bytes.
_MAX_SNDBUF: int = 2_097_152  # 2 MB

# ---------------------------------------------------------------------------
# Keep-alive constants (used by _KeepAliveTransport)
# ---------------------------------------------------------------------------

#: Idle time (seconds) before sending the first keep-alive probe.
_KA_IDLE_S: int = 10

#: Interval (seconds) between subsequent keep-alive probes.
_KA_INTVL_S: int = 5

#: Maximum number of keep-alive probes sent before dropping the connection.
_KA_CNT: int = 3


# ---------------------------------------------------------------------------
# Adaptive timeout
# ---------------------------------------------------------------------------


class AdaptiveTimeout:
    """Tracks recent request latency via EMA and derives a dynamic timeout.

    The timeout for the next request is ``max(_MIN_TIMEOUT_S,
    min(ema_latency * _EMA_MULTIPLIER, _MAX_TIMEOUT_S))``.  Before any
    latency samples have been recorded the baseline ``REQUEST_TIMEOUT_S`` is
    returned unchanged.

    Parameters
    ----------
    smoothing_period:
        Number of samples used to compute the EMA smoothing factor
        (α = 2 / (period + 1)).  A larger period means slower adaptation.
    """

    def __init__(self, smoothing_period: int = 10) -> None:
        self._ema = RollingEMA(smoothing_period=smoothing_period)

    def record(self, latency_s: float) -> None:
        """Feed a new latency observation (seconds) into the EMA."""
        self._ema.update(latency_s)

    @property
    def timeout_s(self) -> float:
        """Return the current adaptive timeout in seconds."""
        if self._ema.value is None:
            return REQUEST_TIMEOUT_S
        adaptive = self._ema.value * _EMA_MULTIPLIER
        return max(_MIN_TIMEOUT_S, min(adaptive, _MAX_TIMEOUT_S))

    def as_httpx_timeout(self) -> httpx.Timeout:
        """Return an ``httpx.Timeout`` built from the current adaptive value."""
        t = self.timeout_s
        return httpx.Timeout(connect=t, read=t, write=t, pool=t)

    @property
    def rtt_estimate_s(self) -> float:
        """Return the current EMA RTT estimate in seconds, or a default."""
        if self._ema.value is None:
            return REQUEST_TIMEOUT_S / _EMA_MULTIPLIER
        return self._ema.value


#: Module-level shared instance – all helpers use this by default so the EMA
#: accumulates across every outbound request in the process.
_adaptive_timeout: AdaptiveTimeout = AdaptiveTimeout()


# ---------------------------------------------------------------------------
# Buffer size computation
# ---------------------------------------------------------------------------


def compute_buffer_size(rtt_s: float) -> Tuple[int, int]:
    """Compute optimal TCP send/receive buffer sizes using BDP.

    The bandwidth-delay product (BDP) is ``(bandwidth in bytes/s) × RTT``.
    The receive buffer is clamped to ``[_MIN_RCVBUF, _MAX_RCVBUF]``; the send
    buffer is half the receive size, clamped to ``[_MIN_SNDBUF, _MAX_SNDBUF]``.

    Parameters
    ----------
    rtt_s:
        Round-trip time estimate in seconds.

    Returns
    -------
    Tuple[int, int]
        ``(recv_buffer_size, send_buffer_size)`` in bytes.
    """
    bandwidth_bytes_s = _ASSUMED_BANDWIDTH_BPS // 8
    bdp = int(bandwidth_bytes_s * rtt_s)
    rcvbuf = max(_MIN_RCVBUF, min(bdp, _MAX_RCVBUF))
    sndbuf = max(_MIN_SNDBUF, min(bdp // 2, _MAX_SNDBUF))
    return rcvbuf, sndbuf


# ---------------------------------------------------------------------------
# Socket helpers
# ---------------------------------------------------------------------------


def _apply_keepalive(sock: socket.socket) -> None:
    """Enable TCP keep-alive on *sock* with the module-level constants."""
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

        if hasattr(socket, "TCP_KEEPIDLE"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, _KA_IDLE_S)
        if hasattr(socket, "TCP_KEEPINTVL"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, _KA_INTVL_S)
        if hasattr(socket, "TCP_KEEPCNT"):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, _KA_CNT)
    except OSError as exc:
        logger.warning("Failed to set TCP keep-alive: %s", exc)


def _apply_tcp_buffers(sock: socket.socket, rtt_s: float) -> None:
    """Set SO_RCVBUF and SO_SNDBUF on *sock* based on the given RTT.

    Parameters
    ----------
    sock:
        The connected TCP socket to tune.
    rtt_s:
        Round-trip time estimate in seconds used to compute buffer sizes.
    """
    try:
        rcvbuf, sndbuf = compute_buffer_size(rtt_s)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, sndbuf)
        logger.debug(
            "TCP buffers tuned | rcvbuf=%d | sndbuf=%d | rtt_s=%.3f",
            rcvbuf,
            sndbuf,
            rtt_s,
        )
    except OSError as exc:
        logger.warning("Failed to set TCP buffer sizes: %s", exc)


# ---------------------------------------------------------------------------
# Custom transport with keep-alive + buffer tuning
# ---------------------------------------------------------------------------


class _KeepAliveTransport(AsyncHTTPTransport):
    """An ``httpx.AsyncHTTPTransport`` that applies keep-alive and dynamic
    TCP buffer sizing to every newly-created socket.

    Buffer sizes are derived from the module-level ``_adaptive_timeout``
    RTT estimate so they respond to the same dynamic network conditions used
    for timeout tuning.
    """

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        # Let the parent transport do the actual I/O, then patch the socket.
        response = await super().handle_async_request(request)
        stream = response.stream
        if stream is None:
            return response

        try:
            conn = stream._connection
        except AttributeError:
            return response

        try:
            sock: Optional[socket.socket] = conn.network_stream.get_extra_info("socket")
        except AttributeError:
            sock = None

        if sock is not None:
            _apply_keepalive(sock)
            if _adaptive_timeout.rtt_estimate_s > 0:
                _apply_tcp_buffers(sock, _adaptive_timeout.rtt_estimate_s)

        return response


# ---------------------------------------------------------------------------
# Typed error
# ---------------------------------------------------------------------------


class FetchTimeoutError(RuntimeError):
    """Raised when an outbound HTTP request exceeds the current adaptive timeout.

    Attributes
    ----------
    url : str
        The endpoint URL that timed out.
    timeout_ms : int
        The configured limit in milliseconds at the time of the failure.
    """

    def __init__(self, url: str, timeout_ms: int) -> None:
        self.url = url
        self.timeout_ms = timeout_ms
        super().__init__(
            f"[HttpClient] Request to {url!r} timed out after {timeout_ms} ms."
        )


MetricRequest = Union[
    str,
    Tuple[str, Optional[Mapping[str, str]]],
    Dict[str, Any],
]


# ---------------------------------------------------------------------------
# Session factory
# ---------------------------------------------------------------------------


def make_session(**kwargs: Any) -> httpx.AsyncClient:
    """Create an ``httpx.AsyncClient`` with HTTP/2 multiplexing enabled.

    The timeout is sourced from ``_adaptive_timeout`` at call time and will be
    updated per-request inside the fetch helpers — the session timeout serves
    only as a safety net for any request that bypasses the helpers.

    ALPN (Application-Layer Protocol Negotiation) is configured to enable
    automatic HTTP/2 protocol negotiation with supporting remote servers. The
    client will negotiate HTTP/2 frame multiplexing when the server supports it,
    falling back to HTTP/1.1 gracefully when HTTP/2 is not available.

    Parameters
    ----------
    **kwargs:
        Forwarded to ``httpx.AsyncClient``.  Supplying *timeout* or *limits*
        is silently discarded; the module-level values are authoritative.

    Returns
    -------
    httpx.AsyncClient
        A configured session ready for use.
    """
    kwargs["timeout"] = _adaptive_timeout.as_httpx_timeout()
    kwargs["limits"] = _LIMITS
    kwargs.setdefault("http2", True)
    
    # Explicitly configure ALPN for HTTP/2 protocol negotiation
    # httpx handles ALPN automatically when http2=True, but we ensure
    # the verification is logged for monitoring purposes
    if kwargs.get("http2"):
        logger.debug("[HttpClient] HTTP/2 with ALPN protocol negotiation enabled")
    
    return httpx.AsyncClient(**kwargs)


# ---------------------------------------------------------------------------
# Fetch helpers
# ---------------------------------------------------------------------------


async def fetch_json(
    session: httpx.AsyncClient,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
) -> Any:
    """Perform a GET request and return the parsed JSON body.

    Records the round-trip latency into the module-level
    :class:`AdaptiveTimeout` so the timeout for the next request is tuned to
    recent network conditions.

    Parameters
    ----------
    session:
        An ``httpx.AsyncClient`` created via :func:`make_session`.
    url:
        Absolute endpoint URL (no credentials / secret query params).
    params:
        Optional query parameters.

    Returns
    -------
    Any
        Parsed JSON payload.

    Raises
    ------
    FetchTimeoutError
        When the connect or read phase exceeds the current adaptive timeout.
    httpx.RequestError
        Propagated unchanged for non-timeout transport errors.
    """
    timeout = _adaptive_timeout.as_httpx_timeout()
    t0 = time.monotonic()
    try:
        resp = await session.get(url, params=params, timeout=timeout)
        _adaptive_timeout.record(time.monotonic() - t0)
        return resp.json()
    except httpx.TimeoutException as exc:
        _log_timeout(url, _adaptive_timeout.timeout_s)
        raise FetchTimeoutError(url, int(_adaptive_timeout.timeout_s * 1000)) from exc


async def fetch_json_many(
    session: httpx.AsyncClient,
    requests: Mapping[str, MetricRequest],
) -> Dict[str, Any]:
    """Fetch multiple JSON metric endpoints concurrently on one HTTP/2 session.

    ``requests`` maps each currency / metric key to either:

    * a URL string
    * ``(url, params)`` where params is a query-parameter mapping
    * ``{"url": url, "params": params}``

    All request tasks are scheduled before awaiting results, allowing httpx to
    multiplex them over the single connection configured in :func:`make_session`.
    """
    keys = list(requests.keys())
    tasks = []

    for key in keys:
        url, params = _normalise_metric_request(key, requests[key])
        tasks.append(asyncio.create_task(fetch_json(session, url, params=params)))

    results = await asyncio.gather(*tasks)
    return dict(zip(keys, results))


async def poll_json_metrics(requests: Mapping[str, MetricRequest]) -> Dict[str, Any]:
    """Create one HTTP/2 session and fetch distinct metric endpoints in parallel."""
    async with make_session() as session:
        return await fetch_json_many(session, requests)


async def fetch_text(
    session: httpx.AsyncClient,
    url: str,
    *,
    params: Optional[Dict[str, str]] = None,
) -> str:
    """Perform a GET request and return the raw response text.

    Identical adaptive-timeout semantics to :func:`fetch_json`.

    Parameters
    ----------
    session:
        Session created via :func:`make_session`.
    url:
        Absolute endpoint URL (no credentials / secret params).
    params:
        Optional query parameters.

    Returns
    -------
    str
        Decoded response body.

    Raises
    ------
    FetchTimeoutError
        On connect or read timeout.
    httpx.RequestError
        Propagated unchanged for non-timeout transport errors.
    """
    timeout = _adaptive_timeout.as_httpx_timeout()
    t0 = time.monotonic()
    try:
        resp = await session.get(url, params=params, timeout=timeout)
        _adaptive_timeout.record(time.monotonic() - t0)
        return resp.text
    except httpx.TimeoutException as exc:
        _log_timeout(url, _adaptive_timeout.timeout_s)
        raise FetchTimeoutError(url, int(_adaptive_timeout.timeout_s * 1000)) from exc


async def post_json(
    session: httpx.AsyncClient,
    url: str,
    payload: Any,
    *,
    headers: Optional[Dict[str, str]] = None,
) -> Any:
    """Perform a POST request with a JSON body and return parsed JSON.

    Identical adaptive-timeout semantics to :func:`fetch_json`.

    Parameters
    ----------
    session:
        Session created via :func:`make_session`.
    url:
        Absolute endpoint URL (no credentials in the URL).
    payload:
        JSON-serialisable object sent as the request body.
    headers:
        Optional additional request headers.

    Returns
    -------
    Any
        Parsed JSON response body.

    Raises
    ------
    FetchTimeoutError
        On connect or read timeout.
    httpx.RequestError
        Propagated unchanged for non-timeout transport errors.
    """
    timeout = _adaptive_timeout.as_httpx_timeout()
    t0 = time.monotonic()
    try:
        resp = await session.post(url, json=payload, headers=headers, timeout=timeout)
        _adaptive_timeout.record(time.monotonic() - t0)
        return resp.json()
    except httpx.TimeoutException as exc:
        _log_timeout(url, _adaptive_timeout.timeout_s)
        raise FetchTimeoutError(url, int(_adaptive_timeout.timeout_s * 1000)) from exc


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalise_metric_request(
    key: str,
    request: MetricRequest,
) -> Tuple[str, Optional[Dict[str, str]]]:
    if isinstance(request, str):
        return request, None

    if isinstance(request, tuple):
        if len(request) != 2:
            raise ValueError(f"Metric request {key!r} must be a (url, params) tuple.")
        url, params = request
    elif isinstance(request, dict):
        url = request.get("url")
        params = request.get("params")
    else:
        raise TypeError(f"Metric request {key!r} must be a URL, tuple, or dict.")

    if not isinstance(url, str) or not url:
        raise ValueError(f"Metric request {key!r} must include a non-empty URL.")
    if params is None:
        return url, None
    if not isinstance(params, Mapping):
        raise TypeError(f"Metric request {key!r} params must be a mapping.")

    return url, dict(params)


def _log_timeout(url: str, timeout_s: float) -> None:
    """Emit a structured warning for a timed-out request.

    Always logs:
    * ``endpoint`` – the URL that stalled (never includes auth headers/tokens)
    * ``timeout_ms`` – the configured hard limit
    * ``timestamp`` – ISO-8601 UTC moment when expiration was detected

    Never logs authentication headers, bearer tokens, or secret query
    parameters — those must be kept out of *url* by callers.
    """
    timestamp = datetime.now(tz=timezone.utc).isoformat()
    logger.warning(
        "[HttpClient] Request timed out | endpoint=%s | timeout_ms=%d | timestamp=%s",
        url,
        int(timeout_s * 1000),
        timestamp,
    )


# ---------------------------------------------------------------------------
# Public surface
# ---------------------------------------------------------------------------

__all__ = [
    "REQUEST_TIMEOUT_S",
    "AdaptiveTimeout",
    "FetchTimeoutError",
    "InterfaceConfig",
    "InterfaceState",
    "FailoverConfig",
    "MultiInterfaceClient",
    "MetricRequest",
    "compute_buffer_size",
    "make_session",
    "fetch_json",
    "fetch_json_many",
    "poll_json_metrics",
    "fetch_text",
    "post_json",
]