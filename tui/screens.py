"""Modal screens used by the TUI."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static


class ConfirmLaunchScreen(ModalScreen[bool]):
    """Confirm a pipeline launch and show the exact command."""

    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("n", "cancel", "Cancel"),
        ("y", "confirm", "Launch"),
    ]

    def __init__(self, step: str, command: str) -> None:
        super().__init__()
        self.step = step
        self.command = command

    def compose(self) -> ComposeResult:
        with Vertical(id="confirm-dialog"):
            yield Label(f"Launch {self.step}?", classes="dialog-title")
            yield Static(self.command, id="launch-command")
            with Horizontal(classes="dialog-actions"):
                yield Button("Cancel", id="cancel-launch")
                yield Button("Launch", id="confirm-launch", variant="success")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "confirm-launch":
            self.dismiss(True)
        elif event.button.id == "cancel-launch":
            self.dismiss(False)

    def action_confirm(self) -> None:
        self.dismiss(True)

    def action_cancel(self) -> None:
        self.dismiss(False)
