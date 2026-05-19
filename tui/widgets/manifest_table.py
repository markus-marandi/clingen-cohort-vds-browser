"""Manifest table widget for ingest history."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

from rich.text import Text
from textual.widgets import DataTable


STATUS_STYLES = {
    "completed": "green",
    "in_progress": "yellow",
    "failed": "red",
}


class ManifestTable(DataTable):
    """DataTable backed by ingest_manifest.json."""

    def __init__(self, **kwargs: object) -> None:
        super().__init__(id="manifest-table", **kwargs)
        self._columns_added = False

    def _ensure_columns(self) -> None:
        if self._columns_added:
            return
        self.add_columns(
            "Run ID",
            "Status",
            "Samples",
            "GVCFs",
            "Started",
            "Completed",
            "Duration",
        )
        self.cursor_type = "row"
        self.zebra_stripes = True
        self._columns_added = True

    def refresh_manifest(self, path: str | Path) -> None:
        self._ensure_columns()
        self.clear()

        manifest_path = Path(path)
        if not manifest_path.exists():
            return

        with manifest_path.open() as handle:
            data = json.load(handle)

        for run in data.get("runs", []):
            self._add_run(run)

    def _add_run(self, run: dict[str, Any]) -> None:
        run_id = str(run.get("run_id", ""))
        status = str(run.get("status", ""))
        style = STATUS_STYLES.get(status, "white")
        status_cell = Text(status.replace("_", " "), style=style)
        gvcfs = run.get("gvcfs") or []
        samples = run.get("n_samples")
        self.add_row(
            run_id,
            status_cell,
            "-" if samples is None else f"{samples:,}",
            f"{len(gvcfs):,}",
            _short_time(run.get("started_at")),
            _short_time(run.get("completed_at")),
            _duration(run.get("started_at"), run.get("completed_at")),
            key=run_id,
        )


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _short_time(value: str | None) -> str:
    parsed = _parse_time(value)
    if parsed is None:
        return "-"
    return parsed.strftime("%Y-%m-%d %H:%M")


def _duration(start: str | None, end: str | None) -> str:
    start_dt = _parse_time(start)
    end_dt = _parse_time(end)
    if start_dt is None:
        return "-"
    if end_dt is None:
        end_dt = datetime.now()
    seconds = int((end_dt - start_dt).total_seconds())
    hours, rem = divmod(max(seconds, 0), 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"
