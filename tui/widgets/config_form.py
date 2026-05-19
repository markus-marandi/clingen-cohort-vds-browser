"""Configuration forms for pipeline steps."""

from __future__ import annotations

import os
from typing import Iterator

from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.message import Message
from textual.widget import Widget
from textual.widgets import Checkbox, Input, Label

from tui.config import AnnotateConfig, ExportConfig, IngestConfig


class PathInput(Input):
    """Path input that marks missing paths with a CSS class."""

    def __init__(self, *args: object, allow_empty: bool = False, **kwargs: object) -> None:
        self.allow_empty = allow_empty
        super().__init__(*args, **kwargs)

    def watch_value(self, value: str) -> None:
        self.validate_path()

    def validate_path(self) -> bool:
        value = self.value.strip()
        valid = (self.allow_empty and value == "") or os.path.exists(value)
        self.set_class(not valid, "missing")
        return valid


class IntInput(Input):
    """Integer input with optional inclusive bounds."""

    def __init__(
        self,
        *args: object,
        minimum: int = 1,
        maximum: int | None = None,
        **kwargs: object,
    ) -> None:
        self.minimum = minimum
        self.maximum = maximum
        kwargs.setdefault("restrict", r"[0-9]*")
        super().__init__(*args, **kwargs)

    def watch_value(self, value: str) -> None:
        self.validate_int()

    def validate_int(self) -> bool:
        try:
            value = int(self.value)
        except ValueError:
            self.set_class(True, "invalid")
            return False
        valid = value >= self.minimum and (
            self.maximum is None or value <= self.maximum
        )
        self.set_class(not valid, "invalid")
        return valid

    def to_int(self) -> int:
        if not self.validate_int():
            raise ValueError(f"invalid integer: {self.value!r}")
        return int(self.value)


class ConfigForm(Widget):
    """Step-specific form that exposes typed config and CLI args."""

    class Changed(Message):
        def __init__(self, form: "ConfigForm") -> None:
            super().__init__()
            self.form = form

    def __init__(
        self,
        step: str,
        config: IngestConfig | AnnotateConfig | ExportConfig,
        **kwargs: object,
    ) -> None:
        super().__init__(id=f"{step}-form", classes="config-form", **kwargs)
        self.step = step
        self.config = config

    def compose(self) -> ComposeResult:
        with Vertical(classes="config-form-fields"):
            if self.step == "ingest":
                yield from self._compose_ingest(self.config)
            elif self.step == "annotate":
                yield from self._compose_annotate(self.config)
            elif self.step == "export":
                yield from self._compose_export(self.config)

    def _compose_ingest(self, config: IngestConfig) -> Iterator[Widget]:
        yield from self._path("Raw GVCF dir", "raw_gvcf_dir", config.raw_gvcf_dir)
        yield from self._path(
            "Filtered GVCF dir",
            "filtered_gvcf_dir",
            config.filtered_gvcf_dir,
            allow_empty=True,
        )
        yield from self._path(
            "Output VDS dir",
            "output_vds_dir",
            config.output_vds_dir,
            allow_empty=True,
        )
        yield from self._path("Temp base", "temp_base", config.temp_base, allow_empty=True)
        yield from self._path(
            "Manifest path",
            "manifest_path",
            config.manifest_path,
            allow_empty=True,
        )
        yield from self._int("CPU cores", "n_cores", config.n_cores, minimum=1)
        yield from self._int("Memory GB", "memory_gb", config.memory_gb, minimum=1)

    def _compose_annotate(self, config: AnnotateConfig) -> Iterator[Widget]:
        yield from self._path("Input VDS", "vds_path", config.vds_path, allow_empty=True)
        yield from self._path("Output MT", "output_mt", config.output_mt, allow_empty=True)
        yield from self._path("VEP config", "vep_config", config.vep_config)
        yield from self._path("gnomAD HT", "gnomad_ht", config.gnomad_ht, allow_empty=True)
        yield from self._path(
            "Metadata CSV",
            "metadata_path",
            config.metadata_path,
            allow_empty=True,
        )
        yield from self._int("CPU cores", "n_cores", config.n_cores, minimum=1)
        yield from self._int("Memory GB", "memory_gb", config.memory_gb, minimum=1)
        yield from self._checkbox("Overwrite MT", "overwrite", config.overwrite)

    def _compose_export(self, config: ExportConfig) -> Iterator[Widget]:
        yield from self._path("Annotated MT", "mt_path", config.mt_path, allow_empty=True)
        yield from self._text("Elasticsearch URL", "es_url", config.es_url)
        yield from self._text("Index", "index", config.index)
        yield from self._int("Batch size", "batch_size", config.batch_size, minimum=1)

    def _field_id(self, name: str) -> str:
        return f"{self.step}-{name.replace('_', '-')}"

    def _field(self, label: str, widget: Widget) -> Iterator[Widget]:
        with Horizontal(classes="form-row"):
            yield Label(label, classes="field-label")
            yield widget

    def _path(
        self,
        label: str,
        name: str,
        value: str,
        allow_empty: bool = False,
    ) -> Iterator[Widget]:
        yield from self._field(
            label,
            PathInput(
                value=value,
                id=self._field_id(name),
                allow_empty=allow_empty,
                classes="path-input",
            ),
        )

    def _text(self, label: str, name: str, value: str) -> Iterator[Widget]:
        yield from self._field(label, Input(value=value, id=self._field_id(name)))

    def _int(
        self,
        label: str,
        name: str,
        value: int,
        minimum: int = 1,
        maximum: int | None = None,
    ) -> Iterator[Widget]:
        yield from self._field(
            label,
            IntInput(
                value=str(value),
                id=self._field_id(name),
                minimum=minimum,
                maximum=maximum,
                classes="int-input",
            ),
        )

    def _checkbox(self, label: str, name: str, value: bool) -> Iterator[Widget]:
        yield from self._field(
            label,
            Checkbox(value=value, id=self._field_id(name)),
        )

    def on_input_changed(self, event: Input.Changed) -> None:
        self.post_message(self.Changed(self))

    def on_checkbox_changed(self, event: Checkbox.Changed) -> None:
        self.post_message(self.Changed(self))

    def _input(self, name: str, cls: type[Input] = Input) -> Input:
        return self.query_one(f"#{self._field_id(name)}", cls)

    def _checked(self, name: str) -> bool:
        return self.query_one(f"#{self._field_id(name)}", Checkbox).value

    def to_config(self) -> IngestConfig | AnnotateConfig | ExportConfig:
        if self.step == "ingest":
            return IngestConfig(
                raw_gvcf_dir=self._input("raw_gvcf_dir").value.strip(),
                filtered_gvcf_dir=self._input("filtered_gvcf_dir").value.strip(),
                output_vds_dir=self._input("output_vds_dir").value.strip(),
                temp_base=self._input("temp_base").value.strip(),
                manifest_path=self._input("manifest_path").value.strip(),
                n_cores=self._input("n_cores", IntInput).to_int(),
                memory_gb=self._input("memory_gb", IntInput).to_int(),
            )
        if self.step == "annotate":
            return AnnotateConfig(
                vds_path=self._input("vds_path").value.strip(),
                output_mt=self._input("output_mt").value.strip(),
                vep_config=self._input("vep_config").value.strip(),
                gnomad_ht=self._input("gnomad_ht").value.strip(),
                metadata_path=self._input("metadata_path").value.strip(),
                n_cores=self._input("n_cores", IntInput).to_int(),
                memory_gb=self._input("memory_gb", IntInput).to_int(),
                overwrite=self._checked("overwrite"),
            )
        return ExportConfig(
            mt_path=self._input("mt_path").value.strip(),
            es_url=self._input("es_url").value.strip(),
            index=self._input("index").value.strip(),
            batch_size=self._input("batch_size", IntInput).to_int(),
        )

    def to_args(self) -> list[str]:
        return self.to_config().to_args()

    def update_field(self, name: str, value: str) -> None:
        self._input(name).value = value
