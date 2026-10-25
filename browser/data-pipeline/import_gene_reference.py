#!/usr/bin/env python3
"""Import the GRCh37 gene reference used by the cohort browser.

Source: GENCODE release 19 (Ensembl 74) comprehensive gene annotation,
https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_19/gencode.v19.annotation.gtf.gz
Reference build: GRCh37 / hg19.

This imports only the small, public gene-reference annotation needed for gene
pages and the region gene track. It does not import variant, coverage, ClinVar, or
patient data. The importer is intentionally standard-library-only so it can run
from the pipeline environment without Hail.

Example:
  python data-pipeline/import_gene_reference.py \
    --es-url http://localhost:9200 \
    --index genes_grch37

The command is idempotent only in the sense that it refuses to touch an existing
index. Delete/recreate is deliberately not supported.
"""

import argparse
import gzip
import json
import os
import re
import tempfile
import urllib.error
import urllib.request

GENCODE_RELEASE = "19"
GENCODE_URL = (
    "https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_19/"
    "gencode.v19.annotation.gtf.gz"
)
REFERENCE_BUILD = "GRCh37"

CHROMOSOME_NUMBERS = {str(i): i for i in range(1, 23)}
CHROMOSOME_NUMBERS.update({"X": 23, "Y": 24, "M": 25})

ATTRIBUTE_RE = re.compile(r'(\S+)\s+"([^"]*)"')
EXON_FEATURES = {"exon", "CDS", "UTR"}


def normalize_chrom(contig):
    chrom = contig[3:] if contig.startswith("chr") else contig
    if chrom == "MT":
        chrom = "M"
    return chrom


def x_position(chrom, pos):
    return CHROMOSOME_NUMBERS[chrom] * 1_000_000_000 + pos


def parse_attributes(raw):
    return dict(ATTRIBUTE_RE.findall(raw))


def split_version(identifier):
    if "." not in identifier:
        return identifier, ""
    return identifier.rsplit(".", 1)


def add_unique(items, item, key):
    if not any(key(existing) == key(item) for existing in items):
        items.append(item)


def download(url, destination):
    print(f"downloading {url}")
    urllib.request.urlretrieve(url, destination)


def read_gtf(path):
    genes = {}
    transcripts = {}

    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.startswith("#"):
                continue

            fields = line.rstrip("\n").split("\t")
            if len(fields) != 9:
                continue

            contig, _source, feature, start, stop, _score, strand, _frame, raw_attributes = fields
            if feature not in {"gene", "transcript"} | EXON_FEATURES:
                continue

            attributes = parse_attributes(raw_attributes)
            gene_id, gene_version = split_version(attributes.get("gene_id", ""))
            if not gene_id or "_PAR_Y" in gene_version:
                continue

            chrom = normalize_chrom(contig)
            if chrom not in CHROMOSOME_NUMBERS:
                continue

            start = int(start)
            stop = int(stop)

            if feature == "gene":
                genes[gene_id] = {
                    "gene_id": gene_id,
                    "gene_version": gene_version or GENCODE_RELEASE,
                    "symbol": attributes.get("gene_name", gene_id),
                    "gencode_symbol": attributes.get("gene_name", gene_id),
                    "name": attributes.get("gene_name", gene_id),
                    "chrom": chrom,
                    "start": start,
                    "stop": stop,
                    "strand": strand,
                    "xstart": x_position(chrom, start),
                    "xstop": x_position(chrom, stop),
                    "exons": [],
                    "transcripts": [],
                    "canonical_transcript_id": None,
                    "mane_select_transcript": None,
                    "hgnc_id": None,
                    "ncbi_id": None,
                    "omim_id": None,
                    "flags": [],
                    "reference_genome": REFERENCE_BUILD,
                }
                continue

            gene = genes.get(gene_id)
            if gene is None:
                continue

            transcript_id, transcript_version = split_version(
                attributes.get("transcript_id", "")
            )

            if feature == "transcript":
                transcript = {
                    "transcript_id": transcript_id,
                    "transcript_version": transcript_version or GENCODE_RELEASE,
                    "chrom": chrom,
                    "start": start,
                    "stop": stop,
                    "strand": strand,
                    "exons": [],
                    "gtex_tissue_expression": [],
                    "reference_genome": REFERENCE_BUILD,
                }
                add_unique(
                    gene["transcripts"],
                    transcript,
                    lambda value: value["transcript_id"],
                )
                if "tag" in attributes and attributes["tag"] in {
                    "basic",
                    "Ensembl_canonical",
                }:
                    gene["canonical_transcript_id"] = transcript_id
                continue

            if not transcript_id:
                continue

            exon = {
                "feature_type": feature,
                "start": start,
                "stop": stop,
            }
            add_unique(
                gene["exons"],
                exon,
                lambda value: (value["feature_type"], value["start"], value["stop"]),
            )

            transcript = next(
                (
                    value
                    for value in gene["transcripts"]
                    if value["transcript_id"] == transcript_id
                ),
                None,
            )
            if transcript is not None:
                add_unique(
                    transcript["exons"],
                    exon,
                    lambda value: (
                        value["feature_type"],
                        value["start"],
                        value["stop"],
                    ),
                )

    for gene in genes.values():
        gene["exons"].sort(key=lambda value: (value["start"], value["stop"]))
        for transcript in gene["transcripts"]:
            transcript["exons"].sort(key=lambda value: (value["start"], value["stop"]))

        if gene["canonical_transcript_id"] is None and gene["transcripts"]:
            gene["canonical_transcript_id"] = max(
                gene["transcripts"],
                key=lambda value: value["stop"] - value["start"],
            )["transcript_id"]

    return genes



def es_request(method, url, body=None, content_type="application/json"):
    data = None
    headers = {"Content-Type": content_type}
    if body is not None:
        data = body if isinstance(body, bytes) else body.encode("utf-8")
    request = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=120) as response:
        return response.read()


def create_index(es_url, index):
    mapping = {
        "settings": {"number_of_shards": 1, "number_of_replicas": 0},
        "mappings": {
            "dynamic": False,
            "_meta": {
                "source": "GENCODE",
                "release": GENCODE_RELEASE,
                "reference_build": REFERENCE_BUILD,
                "source_url": GENCODE_URL,
            },
            "properties": {
                "gene_id": {"type": "keyword"},
                "symbol_upper_case": {"type": "keyword"},
                "search_terms": {"type": "keyword"},
                "xstart": {"type": "long"},
                "xstop": {"type": "long"},
                "value": {
                    "dynamic": False,
                    "properties": {
                        "gene_id": {"type": "keyword"},
                        "symbol_upper_case": {"type": "keyword"},
                        "search_terms": {"type": "keyword"},
                        "xstart": {"type": "long"},
                        "xstop": {"type": "long"},
                    },
                },
            },
        },
    }

    try:
        es_request("PUT", f"{es_url}/{index}", json.dumps(mapping))
    except urllib.error.HTTPError as error:
        if error.code == 400 and b"resource_already_exists_exception" in error.read():
            raise SystemExit(
                f"index {index} already exists; refusing to delete or recreate it"
            ) from error
        raise


def update_index_mapping(es_url, index):
    mapping = {
        "properties": {
            "gene_id": {"type": "keyword"},
            "symbol_upper_case": {"type": "keyword"},
            "search_terms": {"type": "keyword"},
            "xstart": {"type": "long"},
            "xstop": {"type": "long"},
        }
    }
    es_request("PUT", f"{es_url}/{index}/_mapping", json.dumps(mapping))


def bulk_index(es_url, index, genes, batch_size):
    actions = []
    for gene in genes:
        value = dict(gene)
        value["symbol_upper_case"] = value["symbol"].upper()
        value["search_terms"] = sorted(
            {
                value["symbol"].upper(),
                value["gencode_symbol"].upper(),
                value["gene_id"].upper(),
            }
        )
        actions.append(
            json.dumps({"index": {"_index": index, "_id": gene["gene_id"]}})
        )
        actions.append(
            json.dumps(
                {
                    "gene_id": gene["gene_id"],
                    "symbol_upper_case": value["symbol_upper_case"],
                    "search_terms": value["search_terms"],
                    "xstart": gene["xstart"],
                    "xstop": gene["xstop"],
                    "value": value,
                }
            )
        )
        if len(actions) >= batch_size * 2:
            es_request(
                "POST",
                f"{es_url}/_bulk",
                "\n".join(actions) + "\n",
                "application/x-ndjson",
            )
            actions = []

    if actions:
        es_request(
            "POST",
            f"{es_url}/_bulk",
            "\n".join(actions) + "\n",
            "application/x-ndjson",
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--index", default="genes_grch37")
    parser.add_argument("--gtf-path", default=None)
    parser.add_argument("--gtf-url", default=GENCODE_URL)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--update-existing",
        action="store_true",
        help="add the root search fields to an existing index and overwrite documents",
    )
    args = parser.parse_args()

    gtf_path = args.gtf_path
    if gtf_path is None:
        gtf_path = os.path.join(tempfile.gettempdir(), "gencode.v19.annotation.gtf.gz")
        if not os.path.exists(gtf_path):
            download(args.gtf_url, gtf_path)

    print(f"reading {gtf_path}")
    genes = read_gtf(gtf_path)
    print(f"parsed {len(genes)} genes")

    if args.update_existing:
        update_index_mapping(args.es_url, args.index)
    else:
        create_index(args.es_url, args.index)
    bulk_index(args.es_url, args.index, genes.values(), args.batch_size)
    es_request("POST", f"{args.es_url}/{args.index}/_refresh")
    print(f"indexed {len(genes)} genes into {args.index}")


if __name__ == "__main__":
    main()
