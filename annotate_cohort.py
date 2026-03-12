"""Annotate a cohort VDS with VEP, CADD, ClinVar, gnomAD, and cohort frequencies.

All variant-level annotations are written to a Hail MatrixTable that then feeds
cohort_export.py for Elasticsearch indexing.

Usage:
    python annotate_cohort.py \\
        [--vds-path PATH]       # default: cohort_2026-03-11_run001.vds
        [--output-mt PATH]      # default: cohort_annotated.mt
        [--vep-config PATH]     # default: vep_settings.json in project root
        [--gnomad-ht PATH]      # default: Plugins/gnomad.exomes.r2.1.1.sites.ht
        [--metadata-path PATH]  # optional: CSV with sample-level annotations
        [--n-cores N]           # default: 16
        [--memory-gb N]         # default: 64
        [--overwrite]           # overwrite existing output MT

Annotation layers (in order):
    1. hl.variant_qc()          -> cohort AC / AN / AF / hom_count
    2. hl.vep()                 -> consequence, IMPACT, SYMBOL, HGVSg/c/p,
                                   transcript_id, CADD_PHRED, ClinVar_CLNSIG
                                   (all driven by vep_settings.json)
    3. gnomAD HT join           -> gnomad_af, gnomad_nonfin (AF_nfe)
    4. metadata CSV join        -> sex, age, care_site, panel, HPO_ID,
                                   HPO_termin, health_status, test, instrument
                                   (step skipped when --metadata-path not given)
"""

from __future__ import annotations

import argparse
import os
import sys

import hail as hl

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from ingest_manifest import get_latest_completed_vds, load_manifest


# ── default paths ─────────────────────────────────────────────────────────────

MANIFEST_PATH   = '/mnt/sdb/gvcf_ustina/ingest_manifest.json'
PROJECT_ROOT    = '/mnt/sdb/gvcf_ustina/testing/clingen-cohort-vds-browser'

DEFAULT_VDS_PATH    = '/mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds'
DEFAULT_OUTPUT_MT   = '/mnt/sdb/gvcf_ustina/cohort_annotated.mt'
DEFAULT_VEP_CONFIG  = os.path.join(PROJECT_ROOT, 'vep_settings.json')
DEFAULT_GNOMAD_HT   = os.path.join(PROJECT_ROOT, 'Plugins', 'gnomad.exomes.r2.1.1.sites.ht')
GNOMAD_VCF_PATH     = os.path.join(PROJECT_ROOT, 'Plugins', 'gnomad.exomes.r2.1.1.sites.vcf.bgz')

HAIL_JAR  = '/mnt/sdb/markus_files/hail/backend/hail-all-spark.jar'
TMP_DIR   = '/mnt/sdb/tmp/annotate_temp'


# ── argument parsing ───────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description='annotate cohort VDS with VEP, gnomAD, and cohort frequencies'
    )
    p.add_argument(
        '--vds-path',
        help='path to input VDS (default: latest completed run from manifest)',
    )
    p.add_argument('--output-mt', default=DEFAULT_OUTPUT_MT,
                   help=f'output annotated MT path (default: {DEFAULT_OUTPUT_MT})')
    p.add_argument('--vep-config', default=DEFAULT_VEP_CONFIG,
                   help=f'Hail VEP config JSON (default: vep_settings.json)')
    p.add_argument(
        '--gnomad-ht', default=DEFAULT_GNOMAD_HT,
        help=(
            'path to pre-converted gnomAD Hail Table. '
            'if it does not exist, the local VCF is imported and written here '
            f'(one-time cost). VCF expected at: {GNOMAD_VCF_PATH}'
        ),
    )
    p.add_argument(
        '--metadata-path', default=None,
        help=(
            'optional CSV with sample-level clinical annotations '
            '(columns: sample_id, sex, age, care_site, panel, '
            'HPO_ID, HPO_termin, health_status, test, instrument). '
            'skip this flag to omit sample-level annotations.'
        ),
    )
    p.add_argument('--n-cores', type=int, default=16,
                   help='Spark local cores (default: 16)')
    p.add_argument('--memory-gb', type=int, default=64,
                   help='total RAM available in GB (default: 64)')
    p.add_argument('--overwrite', action='store_true',
                   help='overwrite an existing output MT')
    return p.parse_args()


# ── Spark configuration ────────────────────────────────────────────────────────

def _build_spark_conf(n_cores: int, memory_gb: int) -> dict[str, str]:
    driver_mem = f'{int(memory_gb * 0.8)}g'
    return {
        'spark.driver.memory': driver_mem,
        'spark.executor.memory': driver_mem,
        'spark.driver.maxResultSize': '0',
        'spark.local.dir': TMP_DIR,
        'spark.sql.shuffle.partitions': str(n_cores * 2),
        'spark.sql.files.openCostInBytes': '1099511627776',
        'spark.sql.files.maxPartitionBytes': '1099511627776',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.kryo.registrator': 'is.hail.kryo.HailKryoRegistrator',
        'spark.jars': HAIL_JAR,
        'spark.driver.extraClassPath': HAIL_JAR,
        'spark.executor.extraClassPath': './hail-all-spark.jar',
    }


# ── gnomAD helper ─────────────────────────────────────────────────────────────

def _load_gnomad_ht(gnomad_ht_path: str) -> hl.Table:
    """Return a gnomAD HT keyed by (locus, alleles) with AF and AF_nfe columns.

    On first call the local gnomAD VCF is imported, stripped to the two needed
    INFO fields, and written as a Hail Table so future calls are fast.
    """
    if os.path.exists(gnomad_ht_path):
        print(f'Loading gnomAD HT: {gnomad_ht_path}')
        return hl.read_table(gnomad_ht_path)

    if not os.path.exists(GNOMAD_VCF_PATH):
        raise FileNotFoundError(
            f'gnomAD VCF not found at {GNOMAD_VCF_PATH} '
            f'and no pre-built HT at {gnomad_ht_path}. '
            'Provide --gnomad-ht pointing to an existing HT.'
        )

    print(f'gnomAD HT not found — importing from VCF (one-time, may take ~30 min):')
    print(f'  {GNOMAD_VCF_PATH}')

    # import_vcf returns a MatrixTable; .rows() gives the site-level HT
    gnomad_mt = hl.import_vcf(
        GNOMAD_VCF_PATH,
        reference_genome='GRCh37',
        force_bgz=True,
        skip_invalid_loci=True,
    )
    gnomad_ht = gnomad_mt.rows()

    # Number=A fields → one value per alt allele; index [0] for bi-allelic sites
    gnomad_ht = gnomad_ht.select(
        AF=gnomad_ht.info.AF[0],
        AF_nfe=gnomad_ht.info.AF_nfe[0],
    )
    gnomad_ht.write(gnomad_ht_path)
    print(f'gnomAD HT written to: {gnomad_ht_path}')
    return hl.read_table(gnomad_ht_path)


# ── main annotation logic ──────────────────────────────────────────────────────

def annotate(
    vds_path: str,
    output_mt_path: str,
    vep_config: str,
    gnomad_ht_path: str,
    metadata_path: str | None,
    overwrite: bool,
) -> None:
    """Run the full annotation pipeline and write the annotated MT.

    args:
        vds_path (str): input Hail VDS path.
        output_mt_path (str): destination for the annotated MatrixTable.
        vep_config (str): path to the Hail VEP config JSON.
        gnomad_ht_path (str): path to (or desired path for) gnomAD Hail Table.
        metadata_path (str | None): CSV with sample-level clinical metadata,
            or None to skip sample-level annotations.
        overwrite (bool): whether to overwrite an existing output MT.
    """
    if not os.path.exists(vds_path):
        raise FileNotFoundError(f'VDS not found: {vds_path}')
    if not os.path.exists(vep_config):
        raise FileNotFoundError(f'VEP config not found: {vep_config}')

    # ── 1. densify ────────────────────────────────────────────────────────────
    print(f'Reading VDS: {vds_path}')
    vds = hl.vds.read_vds(vds_path)

    print('Densifying VDS to MatrixTable...')
    mt = hl.vds.to_dense_mt(vds)

    # to_dense_mt() produces LGT (local genotype indexed into LA local alleles).
    # variant_qc requires a standard GT field indexed into the global alleles array.
    mt = mt.annotate_entries(GT=hl.vds.lgt_to_gt(mt.LGT, mt.LA))

    # ── 2. cohort frequencies ─────────────────────────────────────────────────
    print('Computing cohort frequencies (variant_qc)...')
    mt = hl.variant_qc(mt)

    # ── 3. VEP + CADD + ClinVar ───────────────────────────────────────────────
    # vep_settings.json drives: consequence, IMPACT, SYMBOL, HGVSg/c/p,
    # transcript_id, CADD_PHRED, ClinVar_CLNSIG, ClinVar_CLNREVSTAT
    print('Running VEP annotation (this is the slowest step)...')
    mt = hl.vep(mt, config=vep_config)

    # ── 4. gnomAD join ────────────────────────────────────────────────────────
    gnomad_ht = _load_gnomad_ht(gnomad_ht_path)

    print('Joining gnomAD allele frequencies...')
    mt = mt.annotate_rows(
        gnomad_af=gnomad_ht[mt.row_key].AF,
        gnomad_nonfin=gnomad_ht[mt.row_key].AF_nfe,
    )

    # ── 5. clinical metadata (optional) ───────────────────────────────────────
    if metadata_path:
        if not os.path.exists(metadata_path):
            raise FileNotFoundError(f'metadata file not found: {metadata_path}')
        print(f'Loading clinical metadata: {metadata_path}')
        # expected columns: sample_id, sex, age, care_site, panel,
        #                   HPO_ID, HPO_termin, health_status, test, instrument
        meta_ht = hl.import_table(
            metadata_path,
            delimiter=',',
            key='sample_id',
            missing='',
        )
        mt = mt.annotate_cols(**meta_ht[mt.s])
        print(f'Sample-level annotations joined from: {metadata_path}')
    else:
        print('--metadata-path not provided — skipping sample-level annotations.')

    # ── 6. write ──────────────────────────────────────────────────────────────
    print(f'Writing annotated MT to: {output_mt_path}')
    mt.write(output_mt_path, overwrite=overwrite)

    n_variants, n_samples = mt.count()
    print(f'\nDone: {n_variants:,} variants × {n_samples:,} samples → {output_mt_path}')


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == '__main__':
    args = _parse_args()

    vds_path = args.vds_path
    if not vds_path:
        manifest = load_manifest(MANIFEST_PATH)
        vds_path = get_latest_completed_vds(manifest)
        if not vds_path:
            print('No completed VDS run found in manifest. Run the ingest pipeline first.')
            sys.exit(1)
        print(f'Using latest completed VDS from manifest: {vds_path}')

    os.makedirs(TMP_DIR, exist_ok=True)

    spark_conf = _build_spark_conf(args.n_cores, args.memory_gb)

    hl.init(
        master=f'local[{args.n_cores}]',
        tmp_dir=TMP_DIR,
        spark_conf=spark_conf,
        quiet=False,
        log='/tmp/hail_annotate.log',
    )
    hl.default_reference('GRCh37')

    annotate(
        vds_path=vds_path,
        output_mt_path=args.output_mt,
        vep_config=args.vep_config,
        gnomad_ht_path=args.gnomad_ht,
        metadata_path=args.metadata_path,
        overwrite=args.overwrite,
    )
