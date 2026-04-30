"""Export cohort variant annotations to Elasticsearch for the gnomAD browser.

Two modes:
  --mt-path   read a pre-annotated Hail MatrixTable (from annotate_cohort.py).
              exports all 35 schema fields including VEP, CADD, ClinVar, gnomAD.
  --vds-path  fallback: read a raw VDS and compute basic variant stats only.
              exports: chrom, pos, ref, alt, ac, an, af, n_hom, filters.

usage:
    # full annotation (preferred):
    python cohort_export.py --mt-path /mnt/sdb/gvcf_ustina/cohort_annotated.mt

    # basic stats only (before annotation pipeline has been run):
    python cohort_export.py --vds-path /mnt/sdb/gvcf_ustina/cohort.vds

    # other options:
    python cohort_export.py --mt-path PATH [--es-url URL] [--index NAME] [--batch-size N]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.request

import hail as hl

# project-local manifest helpers
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..'))
from ingest_manifest import get_latest_completed_vds, load_manifest


MANIFEST_PATH = '/mnt/sdb/gvcf_ustina/ingest_manifest.json'
DEFAULT_ES_URL = 'http://localhost:9200'
DEFAULT_INDEX = 'cohort_variants'
DEFAULT_BATCH_SIZE = 5000

# cohort_variants ES index mapping — covers both annotated-MT and basic VDS exports.
# documents from the VDS-only path will leave annotation fields as null.
INDEX_MAPPING = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0,
    },
    'mappings': {
        'properties': {
            # ── core variant identity ──────────────────────────────────────
            'variant_id':   {'type': 'keyword'},
            'chrom':        {'type': 'keyword'},
            'pos':          {'type': 'integer'},
            'ref':          {'type': 'keyword'},
            'alt':          {'type': 'keyword'},
            'rsids':        {'type': 'keyword'},
            'filters':      {'type': 'keyword'},

            # ── cohort frequencies (annotated MT: new names) ───────────────
            'ac_total':     {'type': 'integer'},
            'an_total':     {'type': 'integer'},
            'af_total':     {'type': 'float'},
            'hom_count':    {'type': 'integer'},

            # ── cohort frequencies (VDS-only fallback: legacy names) ───────
            'ac':           {'type': 'integer'},
            'an':           {'type': 'integer'},
            'af':           {'type': 'float'},
            'n_hom':        {'type': 'integer'},

            # ── VEP ──────────────────────────────────────────────────────
            'gene_symbol':  {'type': 'keyword'},
            'consequence':  {'type': 'keyword'},
            'impact':       {'type': 'keyword'},
            'gdna':         {'type': 'keyword'},
            'cdna':         {'type': 'keyword'},
            'p_nomen':      {'type': 'keyword'},
            'transcript':   {'type': 'keyword'},

            # ── CADD ─────────────────────────────────────────────────────
            'cadd_score':   {'type': 'float'},

            # ── ClinVar ──────────────────────────────────────────────────
            'clinvar_sig':          {'type': 'keyword'},
            'clinvar_clnrevstat':   {'type': 'keyword'},

            # ── gnomAD ───────────────────────────────────────────────────
            'gnomad_af':      {'type': 'float'},
            'gnomad_nonfin':  {'type': 'float'},

            # ── sample-level (populated after metadata join) ──────────────
            # note: these are cohort-aggregated counts, not per-sample docs
        }
    },
}


# ── elasticsearch helpers ──────────────────────────────────────────────────────

def _es_request(
    url: str,
    method: str = 'GET',
    body: dict | None = None,
) -> dict:
    data = json.dumps(body).encode() if body else None
    headers = {'Content-Type': 'application/json'}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        raise RuntimeError(f'ES request failed [{e.code}]: {e.read().decode()}') from e


def _ensure_index(es_url: str, index: str) -> None:
    """Create the index with mapping if it does not already exist.

    args:
        es_url (str): elasticsearch base URL.
        index (str): index name to create.
    """
    try:
        _es_request(f'{es_url}/{index}', method='GET')
        print(f'index "{index}" already exists')
    except RuntimeError:
        _es_request(f'{es_url}/{index}', method='PUT', body=INDEX_MAPPING)
        print(f'created index "{index}"')


def _bulk_index(es_url: str, index: str, docs: list[dict]) -> int:
    """Send a batch of documents using the ES bulk API.

    args:
        es_url (str): elasticsearch base URL.
        index (str): target index.
        docs (list[dict]): documents to index.

    returns:
        int: number of successfully indexed documents.
    """
    lines = []
    for doc in docs:
        action = {'index': {'_index': index, '_id': doc['variant_id']}}
        lines.append(json.dumps(action))
        lines.append(json.dumps(doc))

    body = '\n'.join(lines) + '\n'
    data = body.encode()
    headers = {'Content-Type': 'application/x-ndjson'}
    req = urllib.request.Request(
        f'{es_url}/_bulk',
        data=data,
        headers=headers,
        method='POST',
    )
    with urllib.request.urlopen(req, timeout=120) as resp:
        result = json.loads(resp.read())

    if result.get('errors'):
        failed = sum(
            1 for item in result['items']
            if item.get('index', {}).get('error')
        )
        print(f'  warning: {failed}/{len(docs)} docs failed in batch')
        return len(docs) - failed

    return len(docs)


# ── export logic ──────────────────────────────────────────────────────────────

def _format_variant_id(chrom: str, pos: int, ref: str, alt: str) -> str:
    return f'{chrom}-{pos}-{ref}-{alt}'


def _run_bulk_export(
    ht: hl.Table,
    es_url: str,
    index: str,
    batch_size: int,
    total: int,
    row_to_doc,
) -> None:
    """Shared bulk-indexing loop used by both export functions.

    args:
        ht (hl.Table): already-selected HT ready for .collect().
        es_url (str): elasticsearch base URL.
        index (str): target index name.
        batch_size (int): documents per bulk request.
        total (int): total variant count (for progress reporting only).
        row_to_doc (callable): maps a collected Hail row struct to a dict.
    """
    _ensure_index(es_url, index)

    rows = ht.collect()
    batch: list[dict] = []
    indexed = 0
    t_start = time.time()

    for row in rows:
        batch.append(row_to_doc(row))

        if len(batch) >= batch_size:
            indexed += _bulk_index(es_url, index, batch)
            elapsed = time.time() - t_start
            rate = indexed / elapsed if elapsed > 0 else 0
            print(f'  indexed {indexed:,}/{total:,} ({rate:.0f} docs/s)')
            batch = []

    if batch:
        indexed += _bulk_index(es_url, index, batch)

    elapsed = time.time() - t_start
    print(f'\ndone: {indexed:,} variants indexed in {elapsed:.1f}s')


def export_mt_to_es(
    mt_path: str,
    es_url: str,
    index: str,
    batch_size: int,
) -> None:
    """Read an annotated MatrixTable and index all schema fields to Elasticsearch.

    Expects the MT to have been produced by annotate_cohort.py (i.e. it has
    mt.variant_qc, mt.vep, mt.gnomad_af, mt.gnomad_nonfin row fields).

    args:
        mt_path (str): path to the annotated Hail MatrixTable.
        es_url (str): elasticsearch base URL.
        index (str): target index name.
        batch_size (int): number of documents per bulk request.

    raises:
        FileNotFoundError: when mt_path does not exist.
    """
    if not os.path.exists(mt_path):
        raise FileNotFoundError(f'annotated MT not found: {mt_path}')

    print(f'Reading annotated MT: {mt_path}')
    mt = hl.read_matrix_table(mt_path)

    ht = mt.rows()
    # check if filters field exists in the mt
    has_filters = 'filters' in ht.row
    ht = ht.select(
        chrom=ht.locus.contig,
        pos=ht.locus.position,
        ref=ht.alleles[0],
        alt=ht.alleles[1],
        filters=ht.filters if has_filters else hl.empty_set(hl.tstr),
        # cohort frequencies
        ac_total=ht.variant_qc.AC[1],
        an_total=ht.variant_qc.AN,
        af_total=ht.variant_qc.AF[1],
        hom_count=ht.variant_qc.homozygote_count[1],
        # VEP (flat struct from --pick; may be missing for non-coding variants)
        gene_symbol=ht.vep.SYMBOL,
        consequence=ht.vep.Consequence,
        impact=ht.vep.IMPACT,
        gdna=ht.vep.HGVSg,
        cdna=ht.vep.HGVSc,
        p_nomen=ht.vep.HGVSp,
        transcript=ht.vep.transcript_id,
        cadd_score=ht.vep.CADD_PHRED,
        clinvar_sig=ht.vep.ClinVar_CLNSIG,
        clinvar_clnrevstat=ht.vep.ClinVar_CLNREVSTAT,
        # gnomAD
        gnomad_af=ht.gnomad_af,
        gnomad_nonfin=ht.gnomad_nonfin,
    )

    total = ht.count()
    print(f'Exporting {total:,} variants to index "{index}" at {es_url}')

    def _row_to_doc(row) -> dict:
        chrom, pos, ref, alt = row.chrom, row.pos, row.ref, row.alt
        return {
            'variant_id':           _format_variant_id(chrom, pos, ref, alt),
            'chrom':                chrom,
            'pos':                  pos,
            'ref':                  ref,
            'alt':                  alt,
            'filters':              list(row.filters) if row.filters else [],
            'ac_total':             row.ac_total,
            'an_total':             row.an_total,
            'af_total':             float(row.af_total) if row.af_total is not None else None,
            'hom_count':            row.hom_count,
            'gene_symbol':          row.gene_symbol,
            'consequence':          row.consequence,
            'impact':               row.impact,
            'gdna':                 row.gdna,
            'cdna':                 row.cdna,
            'p_nomen':              row.p_nomen,
            'transcript':           row.transcript,
            'cadd_score':           float(row.cadd_score) if row.cadd_score is not None else None,
            'clinvar_sig':          row.clinvar_sig,
            'clinvar_clnrevstat':   row.clinvar_clnrevstat,
            'gnomad_af':            float(row.gnomad_af) if row.gnomad_af is not None else None,
            'gnomad_nonfin':        float(row.gnomad_nonfin) if row.gnomad_nonfin is not None else None,
        }

    _run_bulk_export(ht, es_url, index, batch_size, total, _row_to_doc)


def export_vds_to_es(
    vds_path: str,
    es_url: str,
    index: str,
    batch_size: int,
) -> None:
    """Read a VDS, compute basic variant stats, and index to Elasticsearch.

    Exports: chrom, pos, ref, alt, ac, an, af, n_hom, filters only.
    Use export_mt_to_es() after running annotate_cohort.py for full annotation.

    args:
        vds_path (str): path to the Hail VDS.
        es_url (str): elasticsearch base URL.
        index (str): target index name.
        batch_size (int): number of documents per bulk request.

    raises:
        FileNotFoundError: when the VDS does not exist.
    """
    if not os.path.exists(vds_path):
        raise FileNotFoundError(f'VDS not found: {vds_path}')

    print(f'Reading VDS: {vds_path}')
    vds = hl.vds.read_vds(vds_path)

    mt = hl.vds.to_dense_mt(vds)
    # vds uses lgt (local genotype); convert to standard gt for variant_qc
    mt = mt.annotate_entries(GT=hl.vds.lgt_to_gt(mt.LGT, mt.LA))
    mt = hl.variant_qc(mt)

    ht = mt.rows()
    # vds does not include filters field by default
    has_filters = 'filters' in ht.row
    ht = ht.select(
        chrom=ht.locus.contig,
        pos=ht.locus.position,
        ref=ht.alleles[0],
        alt=ht.alleles[1],
        ac=ht.variant_qc.AC[1],
        an=ht.variant_qc.AN,
        af=ht.variant_qc.AF[1],
        n_hom=ht.variant_qc.homozygote_count[1],
        filters=ht.filters if has_filters else hl.empty_set(hl.tstr),
    )

    total = ht.count()
    print(f'Exporting {total:,} variants to index "{index}" at {es_url}')

    def _row_to_doc(row) -> dict:
        chrom, pos, ref, alt = row.chrom, row.pos, row.ref, row.alt
        return {
            'variant_id': _format_variant_id(chrom, pos, ref, alt),
            'chrom':   chrom,
            'pos':     pos,
            'ref':     ref,
            'alt':     alt,
            'rsids':   [],
            'ac':      row.ac,
            'an':      row.an,
            'af':      float(row.af) if row.af is not None else None,
            'n_hom':   row.n_hom,
            'filters': list(row.filters) if row.filters else [],
        }

    _run_bulk_export(ht, es_url, index, batch_size, total, _row_to_doc)


# ── entry point ───────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='export cohort variants to Elasticsearch'
    )
    parser.add_argument(
        '--mt-path',
        help='path to annotated MT from annotate_cohort.py (preferred)',
    )
    parser.add_argument(
        '--vds-path',
        help='path to VDS for basic stats-only export (fallback when MT not available)',
    )
    parser.add_argument(
        '--es-url',
        default=DEFAULT_ES_URL,
        help=f'elasticsearch URL (default: {DEFAULT_ES_URL})',
    )
    parser.add_argument(
        '--index',
        default=DEFAULT_INDEX,
        help=f'elasticsearch index name (default: {DEFAULT_INDEX})',
    )
    parser.add_argument(
        '--batch-size',
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f'bulk index batch size (default: {DEFAULT_BATCH_SIZE})',
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = _parse_args()

    hl.init(master='local[4]', quiet=True)
    hl.default_reference('GRCh37')

    if args.mt_path:
        export_mt_to_es(
            mt_path=args.mt_path,
            es_url=args.es_url,
            index=args.index,
            batch_size=args.batch_size,
        )
    else:
        vds_path = args.vds_path
        if not vds_path:
            manifest = load_manifest(MANIFEST_PATH)
            vds_path = get_latest_completed_vds(manifest)
            if not vds_path:
                print('provide --mt-path or --vds-path, or run the ingest pipeline first.')
                sys.exit(1)
            print(f'using latest completed VDS from manifest: {vds_path}')

        export_vds_to_es(
            vds_path=vds_path,
            es_url=args.es_url,
            index=args.index,
            batch_size=args.batch_size,
        )
