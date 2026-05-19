"""Project-root entry point for the pipeline TUI."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path


def main() -> int:
    """Load the package entry point even though this file is also named tui.py."""
    package_dir = Path(__file__).with_name("tui")
    package = types.ModuleType("tui")
    package.__path__ = [str(package_dir)]  # type: ignore[attr-defined]
    sys.modules["tui"] = package
    return importlib.import_module("tui.__main__").main()


if __name__ == "__main__":
    raise SystemExit(main())
