"""Shared worker state and command builders."""

from __future__ import annotations

import os
import shlex
import sys
from dataclasses import dataclass, replace
from typing import Literal

from tui.config import AnnotateConfig, ExportConfig, IngestConfig, PROJECT_ROOT
from tui.parsers import ProgressUpdate


StepName = Literal["ingest", "annotate", "export"]
StepStatus = Literal["idle", "running", "done", "failed"]


@dataclass(frozen=True)
class StepState:
    step: StepName
    status: StepStatus = "idle"
    label: str = "Idle"
    current: int | None = None
    total: int | None = None
    return_code: int | None = None

    def with_changes(self, **changes: object) -> "StepState":
        return replace(self, **changes)


@dataclass(frozen=True)
class WorkerResult:
    step: StepName
    command: list[str]
    return_code: int

    @property
    def ok(self) -> bool:
        return self.return_code == 0


def _python(python_executable: str | None = None) -> str:
    return (
        python_executable
        or os.environ.get("CLINGEN_PIPELINE_PYTHON")
        or sys.executable
    )


def build_ingest_cmd(
    config: IngestConfig,
    python_executable: str | None = None,
) -> list[str]:
    return [
        _python(python_executable),
        "-u",
        str(PROJECT_ROOT / "parallel_ingest_cohort.py"),
        *config.to_args(),
    ]


def build_annotate_cmd(
    config: AnnotateConfig,
    python_executable: str | None = None,
) -> list[str]:
    return [
        _python(python_executable),
        "-u",
        str(PROJECT_ROOT / "annotate_cohort.py"),
        *config.to_args(),
    ]


def build_export_cmd(
    config: ExportConfig,
    python_executable: str | None = None,
) -> list[str]:
    return [
        _python(python_executable),
        "-u",
        str(PROJECT_ROOT / "browser" / "data-pipeline" / "cohort_export.py"),
        *config.to_args(),
    ]


def as_shell_command(command: list[str]) -> str:
    return shlex.join(command)


__all__ = [
    "ProgressUpdate",
    "StepName",
    "StepState",
    "StepStatus",
    "WorkerResult",
    "as_shell_command",
    "build_annotate_cmd",
    "build_export_cmd",
    "build_ingest_cmd",
]
