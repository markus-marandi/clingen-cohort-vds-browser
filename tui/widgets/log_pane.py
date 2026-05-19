"""Log display widget for streamed subprocess output."""

from __future__ import annotations

from rich.text import Text
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.widget import Widget
from textual.widgets import Button, RichLog


class LogPane(Widget):
    """RichLog plus follow and clear controls."""

    def __init__(self, step: str, **kwargs: object) -> None:
        super().__init__(id=f"{step}-log-pane", classes="log-pane", **kwargs)
        self.step = step
        self.follow = True

    def compose(self) -> ComposeResult:
        with Vertical():
            with Horizontal(classes="log-toolbar"):
                yield Button("Follow", id=f"{self.step}-toggle-follow", variant="primary")
                yield Button("Clear", id=f"{self.step}-clear-log")
            yield RichLog(
                wrap=False,
                highlight=False,
                markup=False,
                auto_scroll=True,
                id=f"{self.step}-rich-log",
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == f"{self.step}-toggle-follow":
            self.follow = not self.follow
            event.button.label = "Follow" if self.follow else "Locked"
            self.query_one(f"#{self.step}-rich-log", RichLog).auto_scroll = self.follow
            event.stop()
        elif event.button.id == f"{self.step}-clear-log":
            self.clear()
            event.stop()

    def append_line(self, text: str) -> None:
        log = self.query_one(f"#{self.step}-rich-log", RichLog)
        log.write(Text.from_ansi(text), scroll_end=self.follow)

    def clear(self) -> None:
        self.query_one(f"#{self.step}-rich-log", RichLog).clear()
