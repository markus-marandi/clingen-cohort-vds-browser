"""Textual pipeline management dashboard."""

from __future__ import annotations

import json
import os
import subprocess
import threading
from dataclasses import replace
from pathlib import Path

from textual import work
from textual.app import App, ComposeResult
from textual.containers import Horizontal, Vertical
from textual.css.query import NoMatches
from textual.reactive import reactive
from textual.widgets import Button, Footer, Header, Label, ProgressBar, TabPane, TabbedContent

from ingest_manifest import get_in_progress_run, get_latest_completed_vds, load_manifest
from tui.config import (
    AnnotateConfig,
    ExportConfig,
    IngestConfig,
    PipelineConfig,
    PROJECT_ROOT,
)
from tui.parsers import ProgressUpdate, parse_line
from tui.screens import ConfirmLaunchScreen
from tui.widgets import ConfigForm, LogPane, ManifestTable, StepCard
from tui.workers import (
    StepName,
    StepState,
    as_shell_command,
    build_annotate_cmd,
    build_export_cmd,
    build_ingest_cmd,
)


STEP_TITLES = {
    "ingest": "Ingest",
    "annotate": "Annotate",
    "export": "Export",
}


class DashboardTab(TabPane):
    """Overview tab with step cards and manifest history."""

    def __init__(self) -> None:
        super().__init__("Dashboard", id="dashboard")

    def compose(self) -> ComposeResult:
        with Vertical(id="dashboard-tab"):
            yield Label("", id="resume-banner", classes="resume-banner hidden")
            with Horizontal(id="dashboard-cards"):
                yield StepCard("ingest", "Ingest")
                yield StepCard("annotate", "Annotate")
                yield StepCard("export", "Export")
            yield Label("Manifest history", classes="section-title")
            yield ManifestTable()


class PipelineStepTab(TabPane):
    """Config, progress, launcher, and logs for one step."""

    def __init__(
        self,
        step: StepName,
        config: IngestConfig | AnnotateConfig | ExportConfig,
    ) -> None:
        super().__init__(STEP_TITLES[step], id=step)
        self.step = step
        self.config = config

    def compose(self) -> ComposeResult:
        with Vertical(classes="step-tab"):
            yield ConfigForm(self.step, self.config)
            with Horizontal(classes="step-actions"):
                yield Button("Launch", id=f"{self.step}-launch", variant="primary")
                yield Label("Idle", id=f"{self.step}-progress-label", classes="progress-label")
            yield ProgressBar(
                total=None,
                show_eta=False,
                id=f"{self.step}-progress",
                classes="step-progress",
            )
            yield LogPane(self.step)


class PipelineApp(App[None]):
    """Root Textual app for launching and monitoring pipeline steps."""

    CSS_PATH = "styles/app.tcss"
    TITLE = "Cohort VDS Pipeline"
    BINDINGS = [
        ("b", "show_tab('dashboard')", "Dashboard"),
        ("1", "show_tab('ingest')", "Ingest"),
        ("2", "show_tab('annotate')", "Annotate"),
        ("3", "show_tab('export')", "Export"),
        ("ctrl+c", "terminate_active", "Terminate"),
        ("q", "quit", "Quit"),
    ]

    ingest_state: StepState = reactive(StepState("ingest"))
    annotate_state: StepState = reactive(StepState("annotate"))
    export_state: StepState = reactive(StepState("export"))

    def __init__(self) -> None:
        super().__init__()
        try:
            self.config = PipelineConfig.load()
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            self.config = PipelineConfig.defaults()
        self.active_processes: dict[StepName, subprocess.Popen[str]] = {}
        self._process_lock = threading.Lock()
        self._save_timer = None

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with TabbedContent(initial="dashboard", id="main-tabs"):
            yield DashboardTab()
            yield PipelineStepTab("ingest", self.config.ingest)
            yield PipelineStepTab("annotate", self.config.annotate)
            yield PipelineStepTab("export", self.config.export)
        yield Footer()

    def on_mount(self) -> None:
        self.refresh_manifest()
        self.set_interval(5.0, self.refresh_manifest)
        self._sync_step_state(self.ingest_state)
        self._sync_step_state(self.annotate_state)
        self._sync_step_state(self.export_state)

    def watch_ingest_state(self, state: StepState) -> None:
        self._sync_step_state(state)

    def watch_annotate_state(self, state: StepState) -> None:
        self._sync_step_state(state)

    def watch_export_state(self, state: StepState) -> None:
        self._sync_step_state(state)

    def action_show_tab(self, tab_id: str) -> None:
        self.query_one("#main-tabs", TabbedContent).active = tab_id

    def action_terminate_active(self) -> None:
        with self._process_lock:
            running = [
                (step, proc)
                for step, proc in self.active_processes.items()
                if proc.poll() is None
            ]
        for step, proc in running:
            proc.terminate()
            self._set_step_state(step, label="Terminating...")
            thread = threading.Thread(
                target=self._kill_if_needed,
                args=(step, proc),
                daemon=True,
            )
            thread.start()

    def _kill_if_needed(self, step: StepName, proc: subprocess.Popen[str]) -> None:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            self.call_from_thread(
                self._append_step_line,
                step,
                "Process did not exit after SIGTERM; sent SIGKILL.",
            )

    def on_button_pressed(self, event: Button.Pressed) -> None:
        button_id = event.button.id or ""
        if button_id.endswith("-launch"):
            step = button_id.removesuffix("-launch")
            if step in STEP_TITLES:
                self._launch_step(step)  # type: ignore[arg-type]

    def on_config_form_changed(self, event: ConfigForm.Changed) -> None:
        event.stop()
        self._schedule_config_save()

    def _schedule_config_save(self) -> None:
        if self._save_timer is not None:
            self._save_timer.stop()
        self._save_timer = self.set_timer(0.5, self._save_config_from_forms)

    def _save_config_from_forms(self) -> None:
        for step in ("ingest", "annotate", "export"):
            try:
                self._update_config_from_form(step)
            except (NoMatches, ValueError):
                return
        self.config.save()

    def _update_config_from_form(self, step: StepName) -> None:
        form = self.query_one(f"#{step}-form", ConfigForm)
        value = form.to_config()
        setattr(self.config, step, value)

    def _launch_step(self, step: StepName) -> None:
        try:
            self._update_config_from_form(step)
        except ValueError as exc:
            self._append_step_line(step, f"Invalid config: {exc}")
            return

        if step == "ingest":
            config = self.config.ingest
            command = build_ingest_cmd(config)
        elif step == "annotate":
            config = self.config.annotate
            command = build_annotate_cmd(config)
        else:
            config = self.config.export
            command = build_export_cmd(config)

        self.config.save()
        screen = ConfirmLaunchScreen(step, as_shell_command(command))

        def _confirmed(result: bool | None) -> None:
            if result:
                self._start_worker(step, config)

        self.push_screen(screen, callback=_confirmed)

    def _start_worker(
        self,
        step: StepName,
        config: IngestConfig | AnnotateConfig | ExportConfig,
    ) -> None:
        self._clear_step_log(step)
        self._set_step_state(
            step,
            status="running",
            label="Starting...",
            current=0,
            total=None,
            return_code=None,
        )
        if step == "ingest":
            self.run_ingest(config)  # type: ignore[arg-type]
        elif step == "annotate":
            self.run_annotate(config)  # type: ignore[arg-type]
        else:
            self.run_export(config)  # type: ignore[arg-type]

    @work(thread=True, exclusive=True)
    def run_ingest(self, config: IngestConfig) -> None:
        self._run_process("ingest", build_ingest_cmd(config))

    @work(thread=True, exclusive=True)
    def run_annotate(self, config: AnnotateConfig) -> None:
        self._run_process("annotate", build_annotate_cmd(config))

    @work(thread=True, exclusive=True)
    def run_export(self, config: ExportConfig) -> None:
        self._run_process("export", build_export_cmd(config))

    def _run_process(self, step: StepName, command: list[str]) -> None:
        env = {**os.environ, "PYTHONUNBUFFERED": "1"}
        return_code = 127
        try:
            proc = subprocess.Popen(
                command,
                cwd=str(PROJECT_ROOT),
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
                env=env,
            )
        except OSError as exc:
            self.call_from_thread(self._append_step_line, step, f"Launch failed: {exc}")
            self.call_from_thread(self._on_step_done, step, return_code)
            return

        with self._process_lock:
            self.active_processes[step] = proc

        try:
            assert proc.stdout is not None
            for raw_line in proc.stdout:
                line = raw_line.rstrip()
                self.call_from_thread(self._append_step_line, step, line)
                update = parse_line(line)
                if update is not None:
                    self.call_from_thread(self._apply_progress_update, step, update)
            return_code = proc.wait()
        finally:
            with self._process_lock:
                self.active_processes.pop(step, None)

        self.call_from_thread(self._on_step_done, step, return_code)

    def _append_step_line(self, step: StepName, line: str) -> None:
        try:
            self.query_one(f"#{step}-log-pane", LogPane).append_line(line)
        except NoMatches:
            pass
        try:
            self.query_one(f"#{step}-card", StepCard).append_log_line(line)
        except NoMatches:
            pass

    def _clear_step_log(self, step: StepName) -> None:
        try:
            self.query_one(f"#{step}-log-pane", LogPane).clear()
        except NoMatches:
            pass

    def _apply_progress_update(self, step: StepName, update: ProgressUpdate) -> None:
        if update.kind == "progress":
            self._set_step_state(
                step,
                current=update.current,
                total=update.total,
                label=update.label or "Running...",
            )
        elif update.kind == "stage":
            self._set_step_state(step, label=update.label or "Running...")
        elif update.kind == "success":
            self._set_step_state(step, label=update.label or "Completed")
        elif update.kind == "error":
            self._set_step_state(step, label=update.label or "Error")

    def _on_step_done(self, step: StepName, return_code: int) -> None:
        state = self._get_step_state(step)
        status = "done" if return_code == 0 else "failed"
        label = "Done" if return_code == 0 else f"Failed ({return_code})"
        current = state.current
        total = state.total
        if return_code == 0 and total is not None:
            current = total
        self._set_step_state(
            step,
            status=status,
            label=label,
            current=current,
            total=total,
            return_code=return_code,
        )
        self.refresh_manifest()

    def _get_step_state(self, step: StepName) -> StepState:
        return getattr(self, f"{step}_state")

    def _set_step_state(self, step: StepName, **changes: object) -> None:
        state = self._get_step_state(step)
        setattr(self, f"{step}_state", replace(state, **changes))

    def _sync_step_state(self, state: StepState) -> None:
        step = state.step
        try:
            self.query_one(f"#{step}-card", StepCard).set_state(state)
        except NoMatches:
            pass

        try:
            progress = self.query_one(f"#{step}-progress", ProgressBar)
            if state.status == "done" and state.total is None:
                progress.update(total=1, progress=1)
            else:
                progress.update(total=state.total, progress=state.current or 0)
            self.query_one(f"#{step}-progress-label", Label).update(state.label)
            self.query_one(f"#{step}-launch", Button).disabled = state.status == "running"
        except NoMatches:
            pass

    def refresh_manifest(self) -> None:
        manifest_path = self.config.ingest.manifest_path
        try:
            self.query_one(ManifestTable).refresh_manifest(manifest_path)
        except (NoMatches, OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

        manifest = self._load_manifest_safe(manifest_path)
        self._sync_resume_banner(manifest)
        self._sync_derived_paths(manifest)

    def _load_manifest_safe(self, manifest_path: str):
        try:
            return load_manifest(manifest_path)
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            return None

    def _sync_resume_banner(self, manifest) -> None:
        try:
            banner = self.query_one("#resume-banner", Label)
        except NoMatches:
            return
        in_progress = get_in_progress_run(manifest) if manifest else None
        if in_progress:
            banner.update(
                f"Resumable ingest run: {in_progress.run_id} "
                f"({len(in_progress.gvcfs):,} GVCFs)"
            )
            banner.remove_class("hidden")
        else:
            banner.add_class("hidden")

    def _sync_derived_paths(self, manifest) -> None:
        latest_vds = get_latest_completed_vds(manifest) if manifest else None
        if latest_vds and not self.config.annotate.vds_path:
            self.config.annotate.vds_path = latest_vds
            try:
                self.query_one("#annotate-form", ConfigForm).update_field(
                    "vds_path",
                    latest_vds,
                )
            except NoMatches:
                pass
        if self.config.annotate.output_mt and not self.config.export.mt_path:
            self.config.export.mt_path = self.config.annotate.output_mt
            try:
                self.query_one("#export-form", ConfigForm).update_field(
                    "mt_path",
                    self.config.export.mt_path,
                )
            except NoMatches:
                pass
