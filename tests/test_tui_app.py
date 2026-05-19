import asyncio
import os
import sys


def test_pipeline_app_mounts_headless() -> None:
    os.environ.setdefault("CLINGEN_PIPELINE_PYTHON", sys.executable)

    from tui.app import PipelineApp

    async def run_app() -> None:
        app = PipelineApp()
        async with app.run_test(size=(120, 40)) as pilot:
            await pilot.pause()
            assert app.query_one("#main-tabs")
            assert app.query_one("#manifest-table")

    asyncio.run(run_app())
