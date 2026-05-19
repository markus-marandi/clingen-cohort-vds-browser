"""Configuration dataclasses and JSON persistence for the TUI."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Any, Type, TypeVar


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_CONFIG_PATH = PROJECT_ROOT / ".tui_config.json"


def _default_path(env_name: str, *candidates: str) -> str:
    """Return an env override, first existing path, or first candidate."""
    override = os.environ.get(env_name)
    if override is not None:
        return override
    for candidate in candidates:
        if candidate and Path(candidate).exists():
            return candidate
    return candidates[0] if candidates else ""


DEFAULT_RAW_GVCF_DIR = _default_path(
    "CLINGEN_RAW_GVCF_DIR",
    "/mnt/sdb/data/raw_gvcfs/andmebaas_test_valim",
    "/mnt/sdb/data/raw_gvcfs",
    "/mnt/sdb/gvcf_ustina/andmebaas_test_valim",
)
DEFAULT_FILTERED_GVCF_DIR = _default_path(
    "CLINGEN_FILTERED_GVCF_DIR",
    "/mnt/sdb/data/filtered_gvcfs/andmebaas_test_valim_filtered",
    "/mnt/sdb/data/filtered_gvcfs",
    "/mnt/sdb/gvcf_ustina/andmebaas_test_valim_filtered",
)
DEFAULT_OUTPUT_VDS_DIR = _default_path(
    "CLINGEN_OUTPUT_VDS_DIR",
    "/mnt/sdb/data/vds",
    "/mnt/sdb/gvcf_ustina",
)
DEFAULT_TEMP_BASE = _default_path(
    "CLINGEN_TEMP_BASE",
    "/mnt/sdb/data/tmp/combiner_temp",
    "/mnt/sdb/data/tmp",
    "/mnt/sdb/gvcf_ustina/temp",
)
DEFAULT_MANIFEST_PATH = _default_path(
    "CLINGEN_MANIFEST_PATH",
    "/mnt/sdb/data/logs/ingest_manifest.json",
    "/mnt/sdb/gvcf_ustina/ingest_manifest.json",
    str(PROJECT_ROOT / "ingest_manifest.json"),
)
DEFAULT_VDS_PATH = _default_path(
    "CLINGEN_VDS_PATH",
    "/mnt/sdb/data/vds/cohort_2026-03-11_run001.vds",
    "/mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds",
)
DEFAULT_OUTPUT_MT = _default_path(
    "CLINGEN_OUTPUT_MT",
    "/mnt/sdb/data/mt/cohort_annotated.mt",
    "/mnt/sdb/gvcf_ustina/cohort_annotated.mt",
    str(PROJECT_ROOT / "cohort_annotated.mt"),
)
DEFAULT_GNOMAD_HT = _default_path(
    "CLINGEN_GNOMAD_HT",
    "/mnt/sdb/reference/gnomad/gnomad.exomes.r2.1.1.sites.ht",
    str(PROJECT_ROOT / "Plugins" / "gnomad.exomes.r2.1.1.sites.ht"),
)


def _append_arg(args: list[str], flag: str, value: str | int | None) -> None:
    if value is None:
        return
    if isinstance(value, str) and value == "":
        return
    args.extend([flag, str(value)])


@dataclass
class IngestConfig:
    raw_gvcf_dir: str = DEFAULT_RAW_GVCF_DIR
    filtered_gvcf_dir: str = DEFAULT_FILTERED_GVCF_DIR
    output_vds_dir: str = DEFAULT_OUTPUT_VDS_DIR
    temp_base: str = DEFAULT_TEMP_BASE
    manifest_path: str = DEFAULT_MANIFEST_PATH
    n_cores: int = 16
    memory_gb: int = 64

    def to_args(self) -> list[str]:
        args: list[str] = []
        _append_arg(args, "--raw-gvcf-dir", self.raw_gvcf_dir)
        _append_arg(args, "--filtered-gvcf-dir", self.filtered_gvcf_dir)
        _append_arg(args, "--output-vds-dir", self.output_vds_dir)
        _append_arg(args, "--temp-base", self.temp_base)
        _append_arg(args, "--manifest-path", self.manifest_path)
        _append_arg(args, "--n-cores", self.n_cores)
        _append_arg(args, "--memory-gb", self.memory_gb)
        return args


@dataclass
class AnnotateConfig:
    vds_path: str = DEFAULT_VDS_PATH
    output_mt: str = DEFAULT_OUTPUT_MT
    vep_config: str = str(PROJECT_ROOT / "vep_settings.json")
    gnomad_ht: str = DEFAULT_GNOMAD_HT
    metadata_path: str = ""
    n_cores: int = 16
    memory_gb: int = 64
    overwrite: bool = False

    def to_args(self) -> list[str]:
        args: list[str] = []
        _append_arg(args, "--vds-path", self.vds_path)
        _append_arg(args, "--output-mt", self.output_mt)
        _append_arg(args, "--vep-config", self.vep_config)
        _append_arg(args, "--gnomad-ht", self.gnomad_ht)
        _append_arg(args, "--metadata-path", self.metadata_path)
        _append_arg(args, "--n-cores", self.n_cores)
        _append_arg(args, "--memory-gb", self.memory_gb)
        if self.overwrite:
            args.append("--overwrite")
        return args


@dataclass
class ExportConfig:
    mt_path: str = DEFAULT_OUTPUT_MT
    es_url: str = "http://localhost:9200"
    index: str = "cohort_variants"
    batch_size: int = 5000

    def to_args(self) -> list[str]:
        args: list[str] = []
        _append_arg(args, "--mt-path", self.mt_path)
        _append_arg(args, "--es-url", self.es_url)
        _append_arg(args, "--index", self.index)
        _append_arg(args, "--batch-size", self.batch_size)
        return args


T = TypeVar("T")


def _dataclass_from_mapping(cls: Type[T], data: dict[str, Any] | None) -> T:
    data = data or {}
    allowed = {field.name for field in fields(cls)}
    values = {key: value for key, value in data.items() if key in allowed}
    return cls(**values)


@dataclass
class PipelineConfig:
    ingest: IngestConfig
    annotate: AnnotateConfig
    export: ExportConfig

    @classmethod
    def defaults(cls) -> "PipelineConfig":
        return cls(
            ingest=IngestConfig(),
            annotate=AnnotateConfig(),
            export=ExportConfig(),
        )

    @classmethod
    def load(cls, path: str | Path = DEFAULT_CONFIG_PATH) -> "PipelineConfig":
        config_path = Path(path)
        if not config_path.exists():
            return cls.defaults()

        with config_path.open() as handle:
            raw = json.load(handle)

        return cls(
            ingest=_dataclass_from_mapping(IngestConfig, raw.get("ingest")),
            annotate=_dataclass_from_mapping(AnnotateConfig, raw.get("annotate")),
            export=_dataclass_from_mapping(ExportConfig, raw.get("export")),
        )

    def save(self, path: str | Path = DEFAULT_CONFIG_PATH) -> None:
        config_path = Path(path)
        config_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = config_path.with_suffix(config_path.suffix + ".tmp")
        with tmp_path.open("w") as handle:
            json.dump(asdict(self), handle, indent=2)
            handle.write("\n")
        tmp_path.replace(config_path)
