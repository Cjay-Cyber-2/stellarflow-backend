"""Pytest configuration for the end-to-end integration suite.

Provides the shared :class:`SystemUnderTest`, the :class:`MetricsCollector`
and the session-wide :class:`ReleaseReport`.  On session finish it serialises
the report to ``reports/release-readiness.{json,md,xml}`` for deployment
pipelines.
"""

from __future__ import annotations

import platform
import sys
from pathlib import Path

import pytest

from report import LayerResult, LayerStatus, ReleaseReport  # type: ignore


def _env_info() -> dict:
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "implementation": platform.python_implementation(),
    }


@pytest.fixture(scope="session")
def report(request) -> ReleaseReport:
    r = ReleaseReport(backend_version="1.0.0", environment=_env_info())
    request.config.e2e_report_obj = r
    yield r


@pytest.fixture
def metrics():
    """Yields a fresh MetricsCollector (test controls start/stop)."""
    from harness import MetricsCollector

    return MetricsCollector()


@pytest.fixture
def sut(tmp_path):
    """Wires all five layers into an in-process system-under-test."""
    from harness import SystemUnderTest

    system = SystemUnderTest(Path(tmp_path) / "sut")
    yield system
    system.shutdown()


@pytest.fixture
def layer_report(report, request):
    """Returns a LayerResult registered into the session report.

    The layer status is derived from the test outcome (set on the node by
    :func:`pytest_runtest_makereport`).
    """
    marker = request.node.get_closest_marker("e2e_layer")
    name = marker.kwargs.get("name") if marker else request.node.name
    result = LayerResult(name=name)
    yield result
    rep = getattr(request.node, "rep_call", None)
    if rep is not None and rep.failed:
        result.status = LayerStatus.FAILED
        result.errors.append(str(rep.longrepr) if rep.longrepr else "test failed")
    report.add_layer(result)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    if rep.when == "call":
        setattr(item, "rep_call", rep)


def pytest_configure(config):
    config.addinivalue_line("markers", "e2e_layer(name): marks a layer E2E test")


def pytest_sessionfinish(session, exitstatus):
    """Write the aggregated release-readiness report."""
    report_obj = getattr(session.config, "e2e_report_obj", None)
    if report_obj is None:
        return
    out_dir = Path("reports")
    written = report_obj.write(out_dir)
    print("\n=== StellarFlow release readiness report ===")
    print(f"GATE : {report_obj.gate}")
    print(f"JSON : {written['json']}")
    print(f"MD   : {written['markdown']}")
    print(f"JUnit: {written['junit']}")
