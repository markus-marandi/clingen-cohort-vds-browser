"""Progress parsers for pipeline subprocess output."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal


RE_BRACKET = re.compile(r"\[\s*([\d,]+)\s*/\s*([\d,]+)\]")
RE_COMPLETED = re.compile(r"Completed:\s+([\d,]+)/([\d,]+)")
RE_INDEXED = re.compile(r"indexed\s+([\d,]+)/([\d,]+)")
RE_STAGE = re.compile(r"^(?:STARTING|COMPLETED):\s+(.+)$")
RE_SUCCESS = re.compile(
    r"PIPELINE COMPLETED SUCCESSFULLY|done:\s+[\d,]+ variants indexed",
    re.IGNORECASE,
)
RE_ERROR = re.compile(r"^(?:Error:|Traceback \(most recent call last\))")


ProgressKind = Literal["progress", "stage", "success", "error"]


@dataclass(frozen=True)
class ProgressUpdate:
    kind: ProgressKind
    current: int | None = None
    total: int | None = None
    label: str | None = None

    @property
    def fraction(self) -> float | None:
        if self.current is None or self.total in (None, 0):
            return None
        return self.current / self.total


def _to_int(value: str) -> int:
    return int(value.replace(",", ""))


def _progress_from_match(match: re.Match[str]) -> ProgressUpdate:
    current = _to_int(match.group(1))
    total = _to_int(match.group(2))
    return ProgressUpdate(
        kind="progress",
        current=current,
        total=total,
        label=f"{current:,}/{total:,}",
    )


def parse_line(line: str) -> ProgressUpdate | None:
    """Parse one output line from a pipeline process."""
    clean = line.strip()

    for pattern in (RE_BRACKET, RE_COMPLETED, RE_INDEXED):
        match = pattern.search(clean)
        if match:
            return _progress_from_match(match)

    match = RE_STAGE.search(clean)
    if match:
        return ProgressUpdate(kind="stage", label=match.group(1).strip())

    if RE_SUCCESS.search(clean):
        return ProgressUpdate(kind="success", label=clean)

    if RE_ERROR.search(clean):
        return ProgressUpdate(kind="error", label=clean)

    return None


def _self_test() -> None:
    assert parse_line("[  1,234/5,000] ok").current == 1234
    assert parse_line("Completed:        1234/5000 (24.7%)").total == 5000
    assert parse_line("  indexed 5,000/150,000 (200 docs/s)").current == 5000
    assert parse_line("STARTING: VDS Combination").label == "VDS Combination"
    assert parse_line("PIPELINE COMPLETED SUCCESSFULLY").kind == "success"
    assert parse_line("Traceback (most recent call last):").kind == "error"


if __name__ == "__main__":
    _self_test()
    print("parser self-test passed")
