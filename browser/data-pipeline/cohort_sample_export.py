#!/usr/bin/env python3
"""Export internal pilot sample metadata and real MT carrier calls without reannotation.

TSV values (including prenatal, None, Unknown and whole exome) are preserved.
--dry-run reads metadata only, without Hail or Elasticsearch. Patient data must
stay on the internal host. This script never deletes an index or changes the MT.
"""

import argparse
import csv
import json
import re
import urllib.error
import urllib.request
from collections import Counter
from pathlib import Path


VARIANT_ID = re.compile(r"(?:[1-9]|1[0-9]|2[0-2]|X|Y)-[1-9][0-9]*-[ACGTN]+-[ACGTN]+$")
SAMPLE_FIELDS = (
    "sex_assigned", "date_of_birth", "date_seq", "run_id", "care_site",
    "material", "health_status", "test", "instrument", "ref_genome",
)


def read_tsv(path, required):
    with Path(path).open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if not set(required).issubset(reader.fieldnames or []):
            raise ValueError(f"Required columns absent in {Path(path).name}")
        for row in reader:
            yield {key: (value or "").strip() for key, value in row.items() if key}


def load_samples(sample_data, procedure, panels, in_report):
    samples = {}
    stats = Counter()
    for row in read_tsv(sample_data, ("sample_id",) + SAMPLE_FIELDS[:7]):
        sid = row["sample_id"]
        if not sid or sid in samples:
            raise ValueError("Sample metadata needs one nonempty unique sample_id per row")
        samples[sid] = {
            "sample_id": sid, **{field: row.get(field) or None for field in SAMPLE_FIELDS},
            "panels": [], "reported_variants": [], "report_status": "Not supplied",
            "omitted_report_entries": 0,
        }

    seen = set()
    for row in read_tsv(procedure, ("sample_id", "test", "instrument", "ref_genome")):
        sid = row["sample_id"]
        if sid not in samples:
            stats["orphan_procedure_rows"] += 1
            continue
        if sid in seen:
            raise ValueError("Pilot supports one procedure per sample")
        seen.add(sid)
        if row["ref_genome"] not in ("hg19", "GRCh37"):
            raise ValueError("Pilot supports GRCh37/hg19 procedures only")
        samples[sid].update({key: row[key] or None for key in ("test", "instrument", "ref_genome")})
    if seen != set(samples):
        raise ValueError("Procedure data does not cover every selected sample")

    for row in read_tsv(panels, ("sample_id", "panel")):
        sample = samples.get(row["sample_id"])
        if sample is None:
            stats["orphan_panel_rows"] += 1
        elif row["panel"] and row["panel"] not in sample["panels"]:
            sample["panels"].append(row["panel"])

    report_tokens = {sid: set() for sid in samples}
    for row in read_tsv(in_report, ("sample_id", "variant_id")):
        sample = samples.get(row["sample_id"])
        if sample is None:
            stats["orphan_report_rows"] += 1
            continue
        value = row["variant_id"]
        if value in ("None", "Unknown"):
            report_tokens[sample["sample_id"]].add(value)
        elif value:
            report_tokens[sample["sample_id"]].add("Reported")
            if VARIANT_ID.fullmatch(value):
                if value not in sample["reported_variants"]:
                    sample["reported_variants"].append(value)
            else:
                sample["omitted_report_entries"] += 1
                stats["omitted_report_entries"] += 1

    for sid, sample in samples.items():
        tokens = report_tokens[sid]
        sample["report_status"] = next(iter(tokens)) if len(tokens) == 1 else (
            "Mixed" if tokens else "Not supplied"
        )
        sample["panels"].sort()
        sample["reported_variants"].sort()
        stats["samples"] += 1
        stats["report_" + sample["report_status"]] += 1
    return samples, stats


def report_flag(sample, variant_id):
    if variant_id in sample["reported_variants"]:
        return True
    if sample["report_status"] in ("None", "Reported") and not sample["omitted_report_entries"]:
        return False
    return None


def carrier_documents(rows, samples, stats):
    """Consume streamed, globally indexed allele calls. Never infer carriers from AF."""
    for row in rows:
        sample = samples.get(row["sample_id"])
        if sample is None:
            continue
        # The flattened VEP annotation is row-level, not per-ALT. Reuse it only
        # for single-ALT rows; do not copy another ALT's annotation.
        single_alt = len(row["alleles"]) == 2
        for alt_index in sorted(set(row["called_alleles"]) - {0}):
            variant_id = f'{row["chrom"]}-{row["pos"]}-{row["alleles"][0]}-{row["alleles"][alt_index]}'
            depths = row.get("ad")
            depth_index = alt_index
            if depths is None:
                depths = row.get("lad")
                local_alleles = row.get("local_alleles") or []
                depth_index = local_alleles.index(alt_index) if alt_index in local_alleles else -1
            fraction = None
            if depths and all(value is not None for value in depths):
                denominator = sum(depths)
                if denominator > 0 and 0 <= depth_index < len(depths):
                    fraction = depths[depth_index] / denominator
            stats["carrier_documents"] += 1
            yield {
                "sample_id": row["sample_id"], "variant_id": variant_id,
                "chrom": row["chrom"], "pos": row["pos"],
                "ref": row["alleles"][0], "alt": row["alleles"][alt_index],
                "gene_symbol": row.get("gene_symbol") if single_alt else None,
                "consequence": row.get("consequence") if single_alt else None,
                "genotype": row["genotype"], "depth": row.get("depth"), "gq": row.get("gq"),
                "var_pct": fraction, "in_report": report_flag(sample, variant_id),
            }


def mt_entries(mt_path, sample_ids, cores):
    # Import lazily so metadata dry runs/tests work without Hail or Java.
    import hail as hl

    hl.init(master=f"local[{cores}]", log="/tmp/cohort_sample_export.hail.log")
    mt = hl.read_matrix_table(mt_path)
    if mt.locus.dtype.reference_genome.name != "GRCh37":
        raise ValueError("The MatrixTable must use GRCh37")
    available = set(mt.s.collect())
    if not set(sample_ids).issubset(available):
        raise ValueError("Some selected metadata samples are absent from the MatrixTable")
    mt = mt.filter_cols(hl.literal(set(sample_ids)).contains(mt.s))
    fields = set(mt.entry.dtype.fields)
    if "GT" in fields:
        gt = mt.GT
    elif {"LGT", "LA"}.issubset(fields):
        gt = hl.vds.lgt_to_gt(mt.LGT, mt.LA)
    else:
        raise ValueError("MatrixTable has neither GT nor LGT/LA carrier calls")
    mt = mt.annotate_entries(_carrier_gt=gt)
    mt = mt.filter_entries(hl.is_defined(mt._carrier_gt) & mt._carrier_gt.is_non_ref())
    ht = mt.entries()
    row_fields = set(ht.row.dtype.fields)
    has_vep = "vep" in row_fields
    vep_fields = set(ht.vep.dtype.fields) if has_vep else set()
    if not {"SYMBOL", "Consequence"}.issubset(vep_fields):
        raise ValueError("Expected the existing annotated MT with flattened vep.SYMBOL and vep.Consequence")
    ht = ht.key_by()
    ht = ht.select(
        sample_id=ht.s, chrom=ht.locus.contig, pos=ht.locus.position, alleles=ht.alleles,
        genotype=hl.str(ht._carrier_gt),
        called_alleles=hl.range(ht._carrier_gt.ploidy).map(lambda i: ht._carrier_gt[i]),
        depth=ht.DP if "DP" in fields else hl.missing(hl.tint32),
        gq=ht.GQ if "GQ" in fields else hl.missing(hl.tint32),
        ad=ht.AD if "AD" in fields else hl.missing(hl.tarray(hl.tint32)),
        lad=ht.LAD if "LAD" in fields else hl.missing(hl.tarray(hl.tint32)),
        local_alleles=ht.LA if "LA" in fields else hl.missing(hl.tarray(hl.tint32)),
        gene_symbol=ht.vep.SYMBOL if "SYMBOL" in vep_fields else hl.missing(hl.tstr),
        consequence=ht.vep.Consequence if "Consequence" in vep_fields else hl.missing(hl.tstr),
    )
    # No collection of all entries on the driver; use the existing Spark backend.
    for row in ht.to_spark().toLocalIterator():
        yield row.asDict(recursive=True)


def request(method, url, body=None, content_type="application/json"):
    data = body.encode() if isinstance(body, str) else body
    req = urllib.request.Request(url, data=data, method=method, headers={"Content-Type": content_type})
    with urllib.request.urlopen(req, timeout=120) as response:
        return json.load(response)


def create_index(es_url, index, kind, allow_existing=False):
    properties = {field: {"type": "keyword"} for field in (SAMPLE_FIELDS + (
        "sample_id", "variant_id", "chrom", "ref", "alt", "gene_symbol", "consequence",
        "genotype", "panels", "reported_variants", "report_status",
    ))}
    properties.update({field: {"type": "integer"} for field in (
        "pos", "depth", "gq", "omitted_report_entries",
    )})
    properties.update(var_pct={"type": "float"}, in_report={"type": "boolean"})
    try:
        request("PUT", f"{es_url}/{index}", json.dumps({
            "settings": {"number_of_shards": 1, "number_of_replicas": 0},
            "mappings": {"dynamic": "strict", "_meta": {
                "profile": "internal-pilot", "kind": kind, "reference_genome": "GRCh37",
            }, "properties": properties},
        }))
    except urllib.error.HTTPError as error:
        message = error.read()
        if error.code == 400 and b"resource_already_exists_exception" in message:
            if allow_existing:
                return
            raise ValueError("Index exists; use new pilot index names or explicit --allow-existing") from error
        raise


def bulk_write(es_url, index, documents, sample_index=False, batch_size=1000):
    count = 0
    lines = []

    def flush():
        result = request("POST", f"{es_url}/_bulk", "\n".join(lines) + "\n", "application/x-ndjson")
        items = result.get("items", [])
        if len(items) != len(lines) // 2 or result.get("errors") or any(
            item.get("index", {}).get("status", 500) >= 300 for item in items
        ):
            # Avoid printing response bodies containing patient identifiers.
            raise RuntimeError("Bulk indexing failed; export is incomplete")

    for document in documents:
        document_id = document["sample_id"] if sample_index else json.dumps(
            [document["sample_id"], document["variant_id"]], separators=(",", ":")
        )
        lines.extend((json.dumps({"index": {"_index": index, "_id": document_id}}), json.dumps(document)))
        count += 1
        if len(lines) >= batch_size * 2:
            flush()
            lines = []
    if lines:
        flush()
    request("POST", f"{es_url}/{index}/_refresh")
    return count


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample-data", required=True)
    parser.add_argument("--procedure", required=True)
    parser.add_argument("--panels", required=True)
    parser.add_argument("--in-report", required=True)
    parser.add_argument("--mt-path")
    parser.add_argument("--es-url", default="http://localhost:9200")
    parser.add_argument("--samples-index", default="cohort_samples")
    parser.add_argument("--carriers-index", default="cohort_sample_variants")
    parser.add_argument("--cores", type=int, default=4)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-existing", action="store_true", help="Upsert only; does not remove old documents")
    args = parser.parse_args()
    if not args.dry_run and not args.mt_path:
        parser.error("--mt-path is required unless --dry-run is used")
    samples, stats = load_samples(args.sample_data, args.procedure, args.panels, args.in_report)
    if args.dry_run:
        print(json.dumps(dict(stats), sort_keys=True))
        return
    rows = mt_entries(args.mt_path, samples, args.cores)
    # Trigger MT/schema/sample checks before writing any sample documents.
    first = next(rows, None)
    from itertools import chain
    create_index(args.es_url, args.samples_index, "samples", args.allow_existing)
    create_index(args.es_url, args.carriers_index, "carriers", args.allow_existing)
    stats["indexed_samples"] = bulk_write(args.es_url, args.samples_index, samples.values(), sample_index=True)
    entries = chain([first], rows) if first is not None else []
    stats["indexed_carriers"] = bulk_write(args.es_url, args.carriers_index, carrier_documents(entries, samples, stats))
    print(json.dumps(dict(stats), sort_keys=True))


if __name__ == "__main__":
    main()
