"""Run the pipeline dashboard with ``python -m tui``."""

from __future__ import annotations

import sys


def main() -> int:
    """Start the Textual application."""
    try:
        from tui.app import PipelineApp
    except ModuleNotFoundError as exc:
        if exc.name in {"textual", "rich"}:
            sys.stderr.write(
                "The TUI requires Textual. Install it in the pipeline Python "
                "environment with: python -m pip install textual\n"
            )
            return 1
        raise

    PipelineApp().run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
