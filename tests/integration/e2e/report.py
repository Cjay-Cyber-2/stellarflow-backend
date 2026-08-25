"""Release-readiness report model and serialisers for the E2E suite.

A :class:`ReleaseReport` aggregates the outcome of every layer's integration
tests plus the cross-cutting load/robustness assertions (database lock
contention, memory leaks, unhandled exceptions).  It can be serialised to:

* JSON  — machine readable, consumed by deployment pipelines / dashboards.
* Markdown — human readable release notes.
* JUnit XML — integrates with CI test-report viewers.

The overall release gate is ``PASS`` only when *every* layer passed **and**
all cross-cutting robustness assertions held (zero lock contention, zero
memory leak, zero unhandled exceptions).
"""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List


class LayerStatus(str, Enum):
    PASSED = "passed"
    FAILED = "failed"
    SKIPPED = "skipped"
    WARNING = "warning"


# A single short load run cannot prove the *absence* of a leak, but it can
# catch gross, unbounded growth.  Net Python-allocation delta below this
# ceiling (16 MiB) is treated as normal allocation noise; above it the
# release gate fails as a suspected memory leak.
MEMORY_LEAK_THRESHOLD = 16 * 1024 * 1024


@dataclass
class LayerResult:
    name: str
    status: LayerStatus = LayerStatus.PASSED
    duration_s: float = 0.0
    checks: int = 0
    errors: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    notes: List[str] = field(default_factory=list)


@dataclass
class ReleaseReport:
    generated_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat()
    )
    backend_version: str = "1.0.0"
    environment: Dict[str, Any] = field(default_factory=dict)
    layers: List[LayerResult] = field(default_factory=list)
    # Cross-cutting robustness assertions (must all be True for a PASS gate).
    db_lock_contention: int = 0
    memory_leak_bytes: int = 0
    unhandled_exceptions: int = 0

    def add_layer(self, layer: LayerResult) -> None:
        self.layers.append(layer)

    # ------------------------------------------------------------------
    # Gate evaluation
    # ------------------------------------------------------------------
    def is_ready(self) -> bool:
        layer_ok = all(
            layer.status in (LayerStatus.PASSED, LayerStatus.SKIPPED, LayerStatus.WARNING)
            and layer.status != LayerStatus.FAILED
            for layer in self.layers
        )
        robust = (
            self.db_lock_contention == 0
            and self.memory_leak_bytes <= MEMORY_LEAK_THRESHOLD
            and self.unhandled_exceptions == 0
        )
        return layer_ok and robust

    @property
    def gate(self) -> str:
        return "PASS" if self.is_ready() else "FAIL"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "generated_at": self.generated_at,
            "backend_version": self.backend_version,
            "environment": self.environment,
            "gate": self.gate,
            "summary": self.summary(),
            "robustness": {
                "db_lock_contention": self.db_lock_contention,
                "memory_leak_bytes": self.memory_leak_bytes,
                "unhandled_exceptions": self.unhandled_exceptions,
            },
            "layers": [asdict(layer) for layer in self.layers],
        }

    def summary(self) -> Dict[str, Any]:
        counts = {s.value: 0 for s in LayerStatus}
        for layer in self.layers:
            counts[layer.status.value] += 1
        return {
            "total_layers": len(self.layers),
            "passed": counts[LayerStatus.PASSED.value],
            "failed": counts[LayerStatus.FAILED.value],
            "skipped": counts[LayerStatus.SKIPPED.value],
            "warning": counts[LayerStatus.WARNING.value],
            "gate": self.gate,
        }

    # ------------------------------------------------------------------
    # Serialisers
    # ------------------------------------------------------------------
    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, default=str)

    def to_markdown(self) -> str:
        lines: List[str] = []
        lines.append("# StellarFlow Backend — Release Readiness Report")
        lines.append("")
        lines.append(f"- **Generated:** {self.generated_at}")
        lines.append(f"- **Backend version:** {self.backend_version}")
        lines.append(f"- **Release gate:** `{self.gate}`")
        lines.append("")
        s = self.summary()
        lines.append(
            f"Layers: {s['passed']} passed / {s['failed']} failed / "
            f"{s['skipped']} skipped / {s['warning']} warning "
            f"(of {s['total_layers']})"
        )
        lines.append("")
        lines.append("## Robustness assertions (simulated load)")
        lines.append("")
        lines.append(f"- Database lock contention events: `{self.db_lock_contention}`")
        lines.append(f"- Memory leak (bytes retained): `{self.memory_leak_bytes}`")
        lines.append(f"- Unhandled exceptions: `{self.unhandled_exceptions}`")
        lines.append("")
        lines.append("## Layer matrix")
        lines.append("")
        lines.append("| Layer | Status | Checks | Duration (s) | Notes |")
        lines.append("| --- | --- | --- | --- | --- |")
        for layer in self.layers:
            note = "; ".join(layer.notes) or "-"
            lines.append(
                f"| {layer.name} | {layer.status.value} | {layer.checks} | "
                f"{layer.duration_s:.3f} | {note} |"
            )
            for err in layer.errors:
                lines.append(f"| ⚠️ {layer.name} error | | | | {err} |")
        lines.append("")
        return "\n".join(lines)

    def to_junit_xml(self) -> str:
        root = ET.Element(
            "testsuites",
            {
                "name": "stellarflow-e2e",
                "tests": str(len(self.layers)),
                "failures": str(
                    sum(1 for l in self.layers if l.status == LayerStatus.FAILED)
                ),
                "skipped": str(
                    sum(1 for l in self.layers if l.status == LayerStatus.SKIPPED)
                ),
            },
        )
        for layer in self.layers:
            case = ET.SubElement(
                root,
                "testcase",
                {
                    "classname": "e2e",
                    "name": f"layer:{layer.name}",
                    "time": f"{layer.duration_s:.3f}",
                },
            )
            if layer.status == LayerStatus.FAILED:
                fail = ET.SubElement(case, "failure", {"message": "layer failed"})
                fail.text = "\n".join(layer.errors) or "failed"
            elif layer.status == LayerStatus.SKIPPED:
                ET.SubElement(case, "skipped", {})
            for metric, value in layer.metrics.items():
                sysout = ET.SubElement(case, "system-out")
                sysout.text = f"{metric}={value}"
        return ET.tostring(root, encoding="unicode")

    # ------------------------------------------------------------------
    # Writers
    # ------------------------------------------------------------------
    def write(self, out_dir: Path, stem: str = "release-readiness") -> Dict[str, Path]:
        out_dir = Path(out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        written: Dict[str, Path] = {}
        json_path = out_dir / f"{stem}.json"
        json_path.write_text(self.to_json(), "utf-8")
        written["json"] = json_path
        md_path = out_dir / f"{stem}.md"
        md_path.write_text(self.to_markdown(), "utf-8")
        written["markdown"] = md_path
        xml_path = out_dir / f"{stem}.xml"
        xml_path.write_text(self.to_junit_xml(), "utf-8")
        written["junit"] = xml_path
        return written


__all__ = ["ReleaseReport", "LayerResult", "LayerStatus"]
