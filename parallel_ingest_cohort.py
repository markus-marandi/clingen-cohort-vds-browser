"""Parallel GVCF preprocessing and incremental VDS ingestion pipeline.

run with --help for all options. paths default to the original development layout;
override every path via flags so the script is portable across environments.

example:
    python parallel_ingest_cohort.py \\
        --raw-gvcf-dir /data/gvcfs/raw \\
        --filtered-gvcf-dir /data/gvcfs/filtered \\
        --output-vds-dir /data/vds \\
        --temp-base /scratch/combiner_temp \\
        --manifest-path /data/ingest_manifest.json \\
        --n-cores 32 \\
        --memory-gb 128
"""

import argparse
import hail as hl
import os
import subprocess
import shutil
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import sys

from ingest_manifest import (
    load_manifest,
    save_manifest,
    get_ingested_gvcfs,
    get_latest_completed_vds,
    get_in_progress_run,
    next_run_id,
    record_run_start,
    record_run_complete,
    record_run_failed,
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='parallel gvcf preprocessing and incremental hail vds ingestion'
    )
    parser.add_argument(
        '--raw-gvcf-dir',
        default='/mnt/sdb/gvcf_ustina/andmebaas_test_valim/',
        help='directory containing raw .gvcf.gz files',
    )
    parser.add_argument(
        '--filtered-gvcf-dir',
        default='/mnt/sdb/gvcf_ustina/temp/andmebaas_test_valim_filtered/',
        help='destination for bcftools-filtered gvcf.gz files',
    )
    parser.add_argument(
        '--output-vds-dir',
        default='/mnt/sdb/gvcf_ustina/',
        help='directory where versioned VDS outputs are written',
    )
    parser.add_argument(
        '--temp-base',
        default='/mnt/sdb/tmp/combiner_temp',
        help='base directory for hail combiner temp/checkpoint data',
    )
    parser.add_argument(
        '--manifest-path',
        default='/mnt/sdb/gvcf_ustina/ingest_manifest.json',
        help='path to the JSON run manifest',
    )
    parser.add_argument(
        '--n-cores',
        type=int,
        default=16,
        help='total CPU cores available on this machine (default: 16)',
    )
    parser.add_argument(
        '--memory-gb',
        type=int,
        default=64,
        help='total RAM in GB (default: 64)',
    )
    return parser.parse_args()


class TimeTracker:
    def __init__(self):
        self.stage_times = {}
        self.overall_start = datetime.now()

    def start_stage(self, stage_name):
        self.stage_times[stage_name] = {'start': datetime.now()}
        print(f"\n{'='*70}")
        print(f"STARTING: {stage_name}")
        print(f"Time: {self.stage_times[stage_name]['start'].strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"{'='*70}\n")

    def end_stage(self, stage_name):
        if stage_name in self.stage_times:
            self.stage_times[stage_name]['end'] = datetime.now()
            duration = self.stage_times[stage_name]['end'] - self.stage_times[stage_name]['start']
            self.stage_times[stage_name]['duration'] = duration
            print(f"\n{'='*70}")
            print(f"COMPLETED: {stage_name}")
            print(f"Duration: {self._format_duration(duration)}")
            print(f"Total elapsed: {self._format_duration(datetime.now() - self.overall_start)}")
            print(f"{'='*70}\n")

    def predict_remaining(self, completed, total, stage_name):
        if stage_name in self.stage_times and completed > 0:
            elapsed = datetime.now() - self.stage_times[stage_name]['start']
            avg_per_item = elapsed / completed
            remaining_items = total - completed
            estimated_remaining = avg_per_item * remaining_items
            estimated_completion = datetime.now() + estimated_remaining
            rate = completed / elapsed.total_seconds() if elapsed.total_seconds() > 0 else 0

            print(f"\n{'─'*70}")
            print(f"PROGRESS UPDATE - {stage_name}")
            print(f"{'─'*70}")
            print(f"Completed:        {completed:,}/{total:,} ({100*completed/total:.1f}%)")
            print(f"Rate:             {rate*60:.1f} files/minute")
            print(f"Elapsed:          {self._format_duration(elapsed)}")
            print(f"Avg per file:     {self._format_duration(avg_per_item)}")
            print(f"Est. remaining:   {self._format_duration(estimated_remaining)}")
            print(f"Est. completion:  {estimated_completion.strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'─'*70}\n")

    def _format_duration(self, td):
        total_seconds = int(td.total_seconds())
        hours, remainder = divmod(total_seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        elif minutes > 0:
            return f"{minutes}m {seconds}s"
        else:
            return f"{seconds}s"

    def print_summary(self):
        total_duration = datetime.now() - self.overall_start
        print("\n" + "="*70)
        print("PIPELINE SUMMARY")
        print("="*70)
        print(f"Started:  {self.overall_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total:    {self._format_duration(total_duration)}")
        print("-"*70)
        for stage, times in self.stage_times.items():
            if 'duration' in times:
                pct = (times['duration'].total_seconds() / total_duration.total_seconds()) * 100
                print(f"{stage:.<50} {self._format_duration(times['duration']):>10} ({pct:>5.1f}%)")
        print("="*70 + "\n")


def process_single_gvcf(args):
    """Process a single GVCF file.

    returns:
        tuple: (filename, success, error_msg, processing_time)
    """
    name, raw_dir, filtered_dir, mapping_path, regions_str = args

    start_time = time.time()
    in_path = os.path.join(raw_dir, name)
    out_path = os.path.join(filtered_dir, name)

    if os.path.exists(out_path) and os.path.exists(out_path + '.tbi'):
        return (name, True, 'Already processed', time.time() - start_time)

    cmd = (
        f'bcftools annotate --rename-chrs {mapping_path} {in_path} 2>/dev/null | '
        f'bcftools view -r {regions_str} -O z -o {out_path} 2>/dev/null'
    )

    try:
        subprocess.run(
            ['bash', '-o', 'pipefail', '-c', cmd],
            check=True,
            capture_output=True,
            text=True,
        )
        subprocess.run(
            ['tabix', '-p', 'vcf', out_path],
            check=True,
            capture_output=True,
        )

        processing_time = time.time() - start_time

        if os.path.exists(out_path) and os.path.exists(out_path + '.tbi'):
            return (name, True, None, processing_time)
        else:
            return (name, False, 'output files not created', processing_time)

    except subprocess.CalledProcessError as e:
        processing_time = time.time() - start_time
        if os.path.exists(out_path):
            try:
                os.remove(out_path)
            except Exception:
                pass
        error_msg = e.stderr if hasattr(e, 'stderr') and e.stderr else str(e)
        return (name, False, error_msg[:200], processing_time)


def prefilter_gvcfs_parallel(
    raw_dir: str,
    filtered_dir: str,
    max_workers: int,
    tracker: TimeTracker,
) -> int:
    """Run parallel bcftools preprocessing on all GVCF files.

    args:
        raw_dir (str): directory containing raw gvcf.gz files.
        filtered_dir (str): destination for filtered/indexed outputs.
        max_workers (int): number of parallel bcftools processes.
        tracker (TimeTracker): shared progress tracker.

    returns:
        int: number of successfully preprocessed files.

    raises:
        ValueError: when raw_dir and filtered_dir resolve to the same path.
        RuntimeError: when required tools are missing.
    """
    if os.path.abspath(raw_dir) == os.path.abspath(filtered_dir):
        raise ValueError('filtered directory must differ from raw directory')

    os.makedirs(filtered_dir, exist_ok=True)

    missing_tools = [tool for tool in ('bcftools', 'tabix') if shutil.which(tool) is None]
    if missing_tools:
        raise RuntimeError(f"missing required tools: {', '.join(missing_tools)}")

    gvcf_names = [n for n in sorted(os.listdir(raw_dir)) if n.endswith('.gvcf.gz')]
    if not gvcf_names:
        print('no GVCF files found.')
        return 0

    total_files = len(gvcf_names)
    print(f'Found {total_files:,} GVCF files to preprocess')

    mapping_path = os.path.join(filtered_dir, 'chr_mapping.txt')
    with open(mapping_path, 'w') as f:
        for i in range(1, 23):
            f.write(f'chr{i} {i}\n')
        f.write('chrX X\n')
        f.write('chrY Y\n')
        f.write('chrM MT\n')

    allowed_contigs = [str(i) for i in range(1, 23)] + ['X', 'Y']
    regions_str = ','.join(allowed_contigs)

    args_list = [
        (name, raw_dir, filtered_dir, mapping_path, regions_str)
        for name in gvcf_names
    ]

    stage_label = f'Preprocessing {total_files:,} GVCFs ({max_workers} parallel workers)'
    tracker.start_stage(stage_label)

    completed = 0
    successful = 0
    skipped = 0
    failed = []
    processing_times = []

    # report every 1% or at least every 10 files
    report_interval = max(10, total_files // 100)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_gvcf, args): args[0] for args in args_list}

        for future in as_completed(futures):
            name, success, error, proc_time = future.result()
            completed += 1

            if success:
                successful += 1
                if error == 'Already processed':
                    skipped += 1
                    status = '○'
                else:
                    status = '✓'
                    processing_times.append(proc_time)
            else:
                failed.append((name, error))
                status = '✗'

            if completed % 10 == 0 or not success:
                print(
                    f'[{completed:>6,}/{total_files:,}] {status} {name}'
                    + (f' - {error[:50]}' if not success else '')
                )

            if completed % report_interval == 0:
                tracker.predict_remaining(completed, total_files, stage_label)

    tracker.end_stage(stage_label)

    avg_time = sum(processing_times) / len(processing_times) if processing_times else 0

    print(f'PREPROCESSING SUMMARY')
    print(f'Total files:      {total_files:,}')
    print(f'Successful:       {successful:,} ({100*successful/total_files:.1f}%)')
    print(f'  - New:          {successful - skipped:,}')
    print(f'  - Skipped:      {skipped:,}')
    print(f'Failed:           {len(failed):,}')
    if processing_times:
        print(f'Avg time/file:    {avg_time:.2f}s')

    if failed:
        print(f'Failed files (showing first 20):')
        for fname, err in failed[:20]:
            print(f'  - {fname}: {err[:100]}')
        if len(failed) > 20:
            print(f'  ... and {len(failed) - 20} more')
        print()

    return successful


def _build_spark_conf(spark_cores: int, spark_driver_memory: str, run_temp: str) -> dict[str, str]:
    """Build Spark configuration tuned for large single-machine cohorts.

    args:
        spark_cores (int): number of cores allocated to spark.
        spark_driver_memory (str): jvm heap string, e.g. '51g'.
        run_temp (str): per-run local scratch directory.

    returns:
        dict[str, str]: spark conf key-value pairs.
    """
    return {
        'spark.driver.memory': spark_driver_memory,
        'spark.executor.memory': spark_driver_memory,
        'spark.driver.maxResultSize': '0',
        'spark.local.dir': run_temp,
        # coarsen partitioning to avoid excessive shuffle overhead on large single-machine cohorts
        'spark.sql.shuffle.partitions': str(spark_cores * 2),
        'spark.sql.files.openCostInBytes': '1099511627776',
        'spark.sql.files.maxPartitionBytes': '1099511627776',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.kryo.registrator': 'is.hail.kryo.HailKryoRegistrator',
    }


def main(args: argparse.Namespace) -> None:
    """Run the full ingest pipeline using parsed CLI arguments.

    args:
        args (argparse.Namespace): parsed flags from _parse_args().
    """
    n_cores = args.n_cores
    memory_gb = args.memory_gb
    max_parallel_bcftools = max(1, int(n_cores * 0.75))
    spark_cores = max(2, n_cores - max_parallel_bcftools)
    spark_driver_memory = f'{int(memory_gb * 0.8)}g'

    raw_gvcf_dir = args.raw_gvcf_dir
    filtered_gvcf_dir = args.filtered_gvcf_dir
    output_vds_dir = args.output_vds_dir
    temp_base = args.temp_base
    manifest_path = args.manifest_path

    os.makedirs(temp_base, exist_ok=True)
    os.makedirs(filtered_gvcf_dir, exist_ok=True)

    tracker = TimeTracker()

    print(f"""
RESOURCE ALLOCATION
Total Cores:     {n_cores}
Total Memory:    {memory_gb}GB

Bcftools:        {max_parallel_bcftools} parallel workers
Spark:           {spark_cores} cores, {spark_driver_memory} memory
""")
    print(f'HAIL VDS COMBINER PIPELINE')
    print(f'Started: {tracker.overall_start.strftime("%Y-%m-%d %H:%M:%S")}')

    written_count = prefilter_gvcfs_parallel(
        raw_gvcf_dir,
        filtered_gvcf_dir,
        max_parallel_bcftools,
        tracker,
    )

    if written_count == 0:
        raise RuntimeError(
            'no filtered GVCFs were written. check bcftools/tabix output and input paths.'
        )

    manifest = load_manifest(manifest_path)
    ingested = get_ingested_gvcfs(manifest)

    all_filtered = sorted(
        f for f in os.listdir(filtered_gvcf_dir) if f.endswith('.gvcf.gz')
    )
    new_gvcf_names = [n for n in all_filtered if n not in ingested]

    in_progress = get_in_progress_run(manifest)

    if in_progress:
        print(
            f'\nResuming in-progress run: {in_progress.run_id} '
            f'({len(in_progress.gvcfs):,} GVCFs)'
        )
        run_id = in_progress.run_id
        run_vds_path = in_progress.vds_path
        run_temp = in_progress.temp_path
        run_gvcf_names = in_progress.gvcfs
    elif not new_gvcf_names:
        print('\nAll GVCFs are already in a completed VDS run. Nothing to do.')
        tracker.print_summary()
        return
    else:
        today = datetime.now().strftime('%Y-%m-%d')
        run_id = next_run_id(manifest, today)
        run_vds_path = os.path.join(output_vds_dir, f'cohort_{run_id}.vds')
        run_temp = os.path.join(temp_base, f'run_{run_id}')
        run_gvcf_names = new_gvcf_names

        os.makedirs(run_temp, exist_ok=True)
        record_run_start(manifest, run_id, run_vds_path, run_temp, run_gvcf_names)
        save_manifest(manifest, manifest_path)
        print(f'\nStarting new run: {run_id} ({len(run_gvcf_names):,} new GVCFs)')

    previous_vds = get_latest_completed_vds(manifest) if not in_progress else None

    print(f'Output VDS:       {run_vds_path}')
    print(f'Temp directory:   {run_temp}')
    if previous_vds:
        print(f'Appending to:     {previous_vds}')

    tracker.start_stage('Hail Initialization')

    spark_conf = _build_spark_conf(spark_cores, spark_driver_memory, run_temp)

    hl.init(
        master=f'local[{spark_cores}]',
        tmp_dir=run_temp,
        spark_conf=spark_conf,
        quiet=False,
        log='/tmp/hail.log',
    )

    hl.default_reference('GRCh37')
    ref = hl.get_reference('GRCh37')

    tracker.end_stage('Hail Initialization')

    tracker.start_stage('VDS Combination')

    run_gvcf_paths = [os.path.join(filtered_gvcf_dir, n) for n in run_gvcf_names]

    standard_contigs = [str(i) for i in range(1, 23)] + ['X', 'Y']
    intervals = [
        hl.Interval(
            hl.Locus(c, 1, reference_genome='GRCh37'),
            hl.Locus(c, ref.contig_length(c), reference_genome='GRCh37'),
            includes_end=True,
        )
        for c in standard_contigs
    ]

    print(f'Creating VDS combiner for {len(run_gvcf_paths):,} GVCFs...')
    print(f'Output:  {run_vds_path}')
    print(f'Temp:    {run_temp}\n')

    combiner = hl.vds.new_combiner(
        output_path=run_vds_path,
        temp_path=run_temp,
        gvcf_paths=run_gvcf_paths,
        # pass previous VDS to merge new samples into the existing cohort
        vds_paths=[previous_vds] if previous_vds else [],
        reference_genome='GRCh37',
        intervals=intervals,
        # lower branch_factor reduces peak memory per merge pass for large cohorts
        branch_factor=50,
        # larger target_records = fewer partitions = less shuffle overhead
        target_records=30000,
    )

    print('Running VDS combiner...')
    combiner.run()

    tracker.end_stage('VDS Combination')

    tracker.start_stage('Verification')

    n_samples = 0
    if os.path.exists(run_vds_path):
        vds_size = subprocess.check_output(['du', '-sh', run_vds_path]).decode().split()[0]
        print(f'VDS written to: {run_vds_path}')
        print(f'VDS size: {vds_size}')

        try:
            vds = hl.vds.read_vds(run_vds_path)
            n_samples = vds.n_samples()
            print(f'VDS contains {n_samples:,} samples')
        except Exception as e:
            print(f'Warning: could not validate VDS: {e}')
    else:
        raise RuntimeError(f'VDS not found at {run_vds_path} after combiner completed')

    tracker.end_stage('Verification')

    record_run_complete(manifest, run_id, n_samples)
    save_manifest(manifest, manifest_path)
    print(f'Manifest updated: {manifest_path}')

    tracker.print_summary()
    print(f'PIPELINE COMPLETED SUCCESSFULLY')


if __name__ == '__main__':
    _args = _parse_args()

    # read any existing in-progress run id before main() modifies the manifest,
    # so we can mark it failed on crash
    _manifest = load_manifest(_args.manifest_path)
    _current_run_id = None
    _in_prog = get_in_progress_run(_manifest)
    if _in_prog:
        _current_run_id = _in_prog.run_id

    try:
        main(_args)

    except KeyboardInterrupt:
        print('\nWarning: pipeline interrupted by user')
        if _current_run_id:
            try:
                m = load_manifest(_args.manifest_path)
                record_run_failed(m, _current_run_id)
                save_manifest(m, _args.manifest_path)
            except Exception:
                pass
        sys.exit(1)

    except Exception as e:
        print(f'\nError: pipeline failed:')
        print(f'  {type(e).__name__}: {e}')
        import traceback
        traceback.print_exc()
        if _current_run_id:
            try:
                m = load_manifest(_args.manifest_path)
                record_run_failed(m, _current_run_id)
                save_manifest(m, _args.manifest_path)
            except Exception:
                pass
        sys.exit(1)
