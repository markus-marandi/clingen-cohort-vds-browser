"""Export cohort VDS variant stats to Elasticsearch for the gnomAD browser.

reads the latest completed VDS from the ingest manifest, computes per-variant
allele counts/frequencies, and bulk-indexes them into a local Elasticsearch
instance using the cohort_variants index.

usage:
    python cohort_export.py [--vds-path PATH] [--es-url URL] [--index NAME] [--batch-size N]

the elasticsearch index schema is intentionally simpler than gnomAD's native
schema so the cohort graphql-api queries can stay lightweight.
"""

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

# cohort_variants ES index mapping
INDEX_MAPPING = {
    'settings': {
        'number_of_shards': 1,
        'number_of_replicas': 0,
    },
    'mappings': {
        'properties': {
            'variant_id': {'type': 'keyword'},
            'chrom': {'type': 'keyword'},
            'pos': {'type': 'integer'},
            'ref': {'type': 'keyword'},
            'alt': {'type': 'keyword'},
            'rsids': {'type': 'keyword'},
            'ac': {'type': 'integer'},
            'an': {'type': 'integer'},
            'af': {'type': 'float'},
            'n_hom': {'type': 'integer'},
            'filters': {'type': 'keyword'},
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


def export_vds_to_es(
    vds_path: str,
    es_url: str,
    index: str,
    batch_size: int,
) -> None:
    """Read a VDS, compute variant stats, and index to Elasticsearch.

    args:
        vds_path (str): path to the Hail VDS.
        es_url (str): elasticsearch base URL.
        index (str): target index name.
        batch_size (int): number of documents per bulk request.

    raises:
        RuntimeError: when the VDS does not exist or ES is unreachable.
    """
    if not os.path.exists(vds_path):
        raise RuntimeError(f'VDS not found: {vds_path}')

    print(f'Reading VDS: {vds_path}')
    vds = hl.vds.read_vds(vds_path)

    # densify and compute site-level stats
    mt = hl.vds.to_dense_mt(vds)
    mt = hl.variant_qc(mt)

    # select only what we need before collecting
    ht = mt.rows()
    ht = ht.select(
        chrom=ht.locus.contig,
        pos=ht.locus.position,
        ref=ht.alleles[0],
        alt=ht.alleles[1],
        ac=ht.variant_qc.AC[1],
        an=ht.variant_qc.AN,
        af=ht.variant_qc.AF[1],
        n_hom=ht.variant_qc.homozygote_count[1],
        filters=ht.filters,
    )

    total = ht.count()
    print(f'Exporting {total:,} variants to index "{index}" at {es_url}')

    _ensure_index(es_url, index)

    # collect in local python - fine for 2000-exome cohort scale
    rows = ht.collect()

    batch: list[dict] = []
    indexed = 0
    t_start = time.time()

    for row in rows:
        chrom = row.chrom
        pos = row.pos
        ref = row.ref
        alt = row.alt

        doc = {
            'variant_id': _format_variant_id(chrom, pos, ref, alt),
            'chrom': chrom,
            'pos': pos,
            'ref': ref,
            'alt': alt,
            'rsids': [],
            'ac': row.ac,
            'an': row.an,
            'af': float(row.af) if row.af is not None else None,
            'n_hom': row.n_hom,
            'filters': list(row.filters) if row.filters else [],
        }
        batch.append(doc)

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


# ── entry point ───────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description='export cohort VDS variant stats to Elasticsearch'
    )
    parser.add_argument(
        '--vds-path',
        help='path to VDS (defaults to latest completed run in manifest)',
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

    vds_path = args.vds_path
    if not vds_path:
        manifest = load_manifest(MANIFEST_PATH)
        vds_path = get_latest_completed_vds(manifest)
        if not vds_path:
            print('no completed VDS run found in manifest. run the ingest pipeline first.')
            sys.exit(1)
        print(f'using latest completed VDS from manifest: {vds_path}')

    hl.init(master='local[4]', quiet=True)
    hl.default_reference('GRCh37')

    export_vds_to_es(
        vds_path=vds_path,
        es_url=args.es_url,
        index=args.index,
        batch_size=args.batch_size,
    )
