from pathlib import Path

from tui.config import (
    DEFAULT_FILTERED_GVCF_DIR,
    DEFAULT_MANIFEST_PATH,
    DEFAULT_OUTPUT_MT,
    DEFAULT_OUTPUT_VDS_DIR,
    DEFAULT_RAW_GVCF_DIR,
    DEFAULT_TEMP_BASE,
    IngestConfig,
    PipelineConfig,
)


def test_ingest_args_skip_empty_paths() -> None:
    config = IngestConfig(
        raw_gvcf_dir="",
        filtered_gvcf_dir="",
        output_vds_dir="",
        temp_base="",
        manifest_path="/tmp/manifest.json",
        n_cores=8,
        memory_gb=32,
    )
    assert config.to_args() == [
        "--manifest-path",
        "/tmp/manifest.json",
        "--n-cores",
        "8",
        "--memory-gb",
        "32",
    ]


def test_pipeline_defaults_use_vm_paths() -> None:
    config = PipelineConfig.defaults()
    assert config.ingest.raw_gvcf_dir == DEFAULT_RAW_GVCF_DIR
    assert config.ingest.filtered_gvcf_dir == DEFAULT_FILTERED_GVCF_DIR
    assert config.ingest.output_vds_dir == DEFAULT_OUTPUT_VDS_DIR
    assert config.ingest.temp_base == DEFAULT_TEMP_BASE
    assert config.ingest.manifest_path == DEFAULT_MANIFEST_PATH
    assert config.annotate.output_mt == DEFAULT_OUTPUT_MT
    assert config.export.mt_path == DEFAULT_OUTPUT_MT


def test_pipeline_config_round_trip(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    config = PipelineConfig.defaults()
    config.ingest.n_cores = 24
    config.annotate.metadata_path = "/tmp/metadata.csv"
    config.export.index = "cohort_test"
    config.save(path)

    loaded = PipelineConfig.load(path)
    assert loaded.ingest.n_cores == 24
    assert loaded.annotate.metadata_path == "/tmp/metadata.csv"
    assert loaded.export.index == "cohort_test"
