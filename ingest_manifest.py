"""Run manifest for incremental VDS ingest.

tracks which gvcfs are in each vds version and the status of each combiner run,
allowing interrupted pipelines to resume without starting from scratch.
"""

import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional


MANIFEST_VERSION = 1


@dataclass
class RunRecord:
    """Single combiner run entry."""

    run_id: str
    vds_path: str
    temp_path: str
    status: str  # 'in_progress' | 'completed' | 'failed'
    started_at: str
    gvcfs: list[str]
    completed_at: Optional[str] = None
    n_samples: Optional[int] = None


@dataclass
class Manifest:
    """Top-level manifest structure."""

    version: int
    runs: list[RunRecord] = field(default_factory=list)


def load_manifest(path: str) -> Manifest:
    """Load manifest from JSON, returning an empty one if the file does not exist.

    args:
        path (str): path to the manifest JSON file.

    returns:
        Manifest: loaded or freshly initialised manifest.
    """
    if not os.path.exists(path):
        return Manifest(version=MANIFEST_VERSION)

    with open(path) as f:
        data = json.load(f)

    runs = [RunRecord(**r) for r in data.get('runs', [])]
    return Manifest(version=data.get('version', MANIFEST_VERSION), runs=runs)


def save_manifest(manifest: Manifest, path: str) -> None:
    """Atomically write manifest to JSON.

    args:
        manifest (Manifest): manifest to persist.
        path (str): destination file path.
    """
    tmp_path = path + '.tmp'
    data = {
        'version': manifest.version,
        'runs': [asdict(r) for r in manifest.runs],
    }
    with open(tmp_path, 'w') as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, path)


def get_ingested_gvcfs(manifest: Manifest) -> set[str]:
    """Return filenames already committed to a completed VDS run.

    args:
        manifest (Manifest): current manifest.

    returns:
        set[str]: gvcf basenames (not full paths) in any completed run.
    """
    ingested: set[str] = set()
    for run in manifest.runs:
        if run.status == 'completed':
            ingested.update(run.gvcfs)
    return ingested


def get_latest_completed_vds(manifest: Manifest) -> Optional[str]:
    """Return the VDS path of the most recent completed run.

    args:
        manifest (Manifest): current manifest.

    returns:
        str | None: absolute path of the latest completed VDS, or None.
    """
    for run in reversed(manifest.runs):
        if run.status == 'completed':
            return run.vds_path
    return None


def get_in_progress_run(manifest: Manifest) -> Optional[RunRecord]:
    """Return the most recent in-progress run, or None.

    args:
        manifest (Manifest): current manifest.

    returns:
        RunRecord | None: resumable run if one exists.
    """
    for run in reversed(manifest.runs):
        if run.status == 'in_progress':
            return run
    return None


def next_run_id(manifest: Manifest, date_str: str) -> str:
    """Generate the next sequential run ID for the given date.

    args:
        manifest (Manifest): current manifest.
        date_str (str): date in YYYY-MM-DD format.

    returns:
        str: run ID like '2026-03-11_run002'.
    """
    prefix = f'{date_str}_run'
    max_n = 0
    for run in manifest.runs:
        if run.run_id.startswith(prefix):
            try:
                n = int(run.run_id[len(prefix):])
                max_n = max(max_n, n)
            except ValueError:
                pass
    return f'{date_str}_run{max_n + 1:03d}'


def record_run_start(
    manifest: Manifest,
    run_id: str,
    vds_path: str,
    temp_path: str,
    gvcfs: list[str],
) -> RunRecord:
    """Append a new in-progress run record to the manifest.

    args:
        manifest (Manifest): manifest to update in place.
        run_id (str): unique run identifier.
        vds_path (str): destination VDS path.
        temp_path (str): combiner temp/checkpoint directory.
        gvcfs (list[str]): gvcf basenames being ingested in this run.

    returns:
        RunRecord: the newly created record.
    """
    record = RunRecord(
        run_id=run_id,
        vds_path=vds_path,
        temp_path=temp_path,
        status='in_progress',
        started_at=datetime.now().isoformat(),
        gvcfs=gvcfs,
    )
    manifest.runs.append(record)
    return record


def record_run_complete(manifest: Manifest, run_id: str, n_samples: int) -> None:
    """Mark a run as completed.

    args:
        manifest (Manifest): manifest to update in place.
        run_id (str): run to mark complete.
        n_samples (int): total sample count in the resulting VDS.

    raises:
        ValueError: when run_id is not found.
    """
    for run in manifest.runs:
        if run.run_id == run_id:
            run.status = 'completed'
            run.completed_at = datetime.now().isoformat()
            run.n_samples = n_samples
            return
    raise ValueError(f'run not found in manifest: {run_id}')


def record_run_failed(manifest: Manifest, run_id: str) -> None:
    """Mark a run as failed.

    args:
        manifest (Manifest): manifest to update in place.
        run_id (str): run to mark failed.

    raises:
        ValueError: when run_id is not found.
    """
    for run in manifest.runs:
        if run.run_id == run_id:
            run.status = 'failed'
            run.completed_at = datetime.now().isoformat()
            return
    raise ValueError(f'run not found in manifest: {run_id}')
