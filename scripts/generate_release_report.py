#!/usr/bin/env python3
"""Release-readiness report CLI for the StellarFlow backend.

This is the entry point deployment pipelines use to verify a build before
promoting it.  It can:

* ``run``     — execute the end-to-end integration suite and emit the
               release-readiness report (JSON + Markdown + JUnit XML) under
               ``reports/``.  Exit code is non-zero when the release gate is
               ``FAIL``.
* ``show``    — print a human summary of an existing report JSON.
* ``check``   — fail (exit 1) if the latest report's gate is not PASS.

Examples
--------
    python scripts/generate_release_report.py run
    python scripts/generate_release_report.py show --input reports/release-readiness.json
    python scripts/generate_release_report.py check
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_REPORT = ROOT / "reports" / "release-readiness.json"
E2E_DIR = ROOT / "tests" / "integration" / "e2e"


def _run_suite() -> int:
    print("Running end-to-end integration suite (pytest)...")
    proc = subprocess.run(
        [sys.executable, "-m", "pytest", str(E2E_DIR), "-q", "-p", "no:cacheprovider"],
        cwd=ROOT,
    )
    return proc.returncode


def _load_report(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"report not found at {path}; run 'run' first")
    return json.loads(path.read_text("utf-8"))


def cmd_run(args: argparse.Namespace) -> int:
    rc = _run_suite()
    report = _load_report(DEFAULT_REPORT)
    _print_summary(report)
    # The suite may report a soft pass (rc==0) but a FAIL gate (robustness).
    if report.get("gate") != "PASS":
        return 1
    return rc


def cmd_show(args: argparse.Namespace) -> int:
    path = Path(args.input) if args.input else DEFAULT_REPORT
    report = _load_report(path)
    _print_summary(report)
    return 0


def cmd_check(args: argparse.Namespace) -> int:
    report = _load_report(DEFAULT_REPORT)
    if report.get("gate") != "PASS":
        print(f"RELEASE GATE: FAIL — blocking promotion", file=sys.stderr)
        return 1
    print("RELEASE GATE: PASS")
    return 0


def _print_summary(report: dict) -> None:
    s = report.get("summary", {})
    print("=" * 60)
    print("StellarFlow Backend — Release Readiness")
    print("=" * 60)
    print(f"  Gate        : {report.get('gate')}")
    print(f"  Version     : {report.get('backend_version')}")
    print(
        f"  Layers      : {s.get('passed')} passed / {s.get('failed')} failed "
        f"/ {s.get('skipped')} skipped (of {s.get('total_layers')})"
    )
    rob = report.get("robustness", {})
    print("  Robustness  :")
    print(f"    db lock contention : {rob.get('db_lock_contention')}")
    print(f"    memory leak (B)    : {rob.get('memory_leak_bytes')}")
    print(f"    unhandled except. : {rob.get('unhandled_exceptions')}")
    print("=" * 60)


def main() -> int:
    parser = argparse.ArgumentParser(description="StellarFlow release-readiness report tool")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("run", help="run the E2E suite and emit the report")
    sub.add_parser("show", help="print a summary of an existing report")

    show = sub.choices["show"]
    show.add_argument("--input", help="path to a report JSON (default: reports/release-readiness.json)")

    sub.add_parser("check", help="exit non-zero unless the latest gate is PASS")

    args = parser.parse_args()
    if args.command == "run":
        return cmd_run(args)
    if args.command == "show":
        return cmd_show(args)
    if args.command == "check":
        return cmd_check(args)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
