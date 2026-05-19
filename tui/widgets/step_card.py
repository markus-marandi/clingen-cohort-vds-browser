"""Dashboard status cards."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.widget import Widget
from textual.widgets import Label, ProgressBar, RichLog

from tui.workers.base_worker import StepState, StepStatus


class StatusBadge(Label):
    """Small status label with a CSS class matching the state."""

    STATUSES: tuple[StepStatus, ...] = ("idle", "running", "done", "failed")

    def __init__(self, status: StepStatus = "idle", **kwargs: object) -> None:
        super().__init__(status.upper(), classes=f"status-badge {status}", **kwargs)
        self.status = status

    def update_status(self, status: StepStatus) -> None:
        for css_class in self.STATUSES:
            self.remove_class(css_class)
        self.add_class(status)
        self.status = status
        self.update(status.upper())


class StepCard(Widget):
    """Compact dashboard card for one pipeline step."""

    def __init__(self, step: str, title: str, **kwargs: object) -> None:
        super().__init__(id=f"{step}-card", classes="step-card", **kwargs)
        self.step = step
        self.title = title
        self._lines: list[str] = []

    def compose(self) -> ComposeResult:
        with Vertical():
            with Horizontal(classes="step-card-header"):
                yield Label(self.title, classes="step-card-title")
                yield StatusBadge(id=f"{self.step}-badge")
            yield ProgressBar(
                total=None,
                show_eta=False,
                id=f"{self.step}-card-progress",
                classes="compact-progress",
            )
            yield Label("Idle", id=f"{self.step}-summary", classes="step-summary")
            yield RichLog(
                max_lines=20,
                wrap=True,
                auto_scroll=True,
                id=f"{self.step}-mini-log",
                classes="mini-log",
            )

    def set_state(self, state: StepState) -> None:
        try:
            self.query_one(f"#{self.step}-badge", StatusBadge).update_status(state.status)
            progress = self.query_one(f"#{self.step}-card-progress", ProgressBar)
            if state.status == "done" and state.total is None:
                progress.update(total=1, progress=1)
            else:
                progress.update(total=state.total, progress=state.current or 0)
            self.query_one(f"#{self.step}-summary", Label).update(state.label)
        except NoMatches:
            return

    def append_log_line(self, text: str) -> None:
        self._lines.append(text)
        self._lines = self._lines[-20:]
        try:
            log = self.query_one(f"#{self.step}-mini-log", RichLog)
        except NoMatches:
            return
        log.clear()
        for line in self._lines:
            log.write(line)
