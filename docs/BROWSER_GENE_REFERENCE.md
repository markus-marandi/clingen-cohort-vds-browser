# Gene reference for cohort pages

Cohort gene pages and the region gene track need a small, public gene
annotation index. This is reference data only; it is not patient data and it is not
the full gnomAD variant dataset.

## Source and release

| Item | Value |
| --- | --- |
| Source | GENCODE comprehensive gene annotation |
| Release | GENCODE release 19 (Ensembl 74) |
| Reference build | GRCh37 / hg19 |
| URL | `https://ftp.ebi.ac.uk/pub/databases/gencode/Gencode_human/release_19/gencode.v19.annotation.gtf.gz` |
| Index | `genes_grch37` |
| Importer | `data-pipeline/import_gene_reference.py` |

The importer reads gene, transcript, exon, CDS, and UTR records from the GTF.
It writes `value` documents used by the existing GraphQL gene and region
resolvers, plus root `symbol_upper_case`, `search_terms`, `xstart`, and `xstop`
fields used by gene search and region overlap queries.

The index `_meta` records the source, release, reference build, and source URL.

## Reproducible import

From the browser checkout:

```bash
python data-pipeline/import_gene_reference.py \
  --es-url http://localhost:9200 \
  --index genes_grch37
```

To use an already-downloaded GTF:

```bash
python data-pipeline/import_gene_reference.py \
  --es-url http://localhost:9200 \
  --index genes_grch37 \
  --gtf-path /path/to/gencode.v19.annotation.gtf.gz
```

The importer creates the index if missing and refuses to delete or recreate an
existing index. For an index created before the root search fields were added, the
same command can add the missing mapping and overwrite the documents:

```bash
python data-pipeline/import_gene_reference.py \
  --es-url http://localhost:9200 \
  --index genes_grch37 \
  --gtf-path /path/to/gencode.v19.annotation.gtf.gz \
  --update-existing
```

## What is not imported

Coverage, ClinVar, short-tandem-repeat, constraint, and gnomAD variant indices
are optional upstream features and are not created for cohort pages. Cohort region
and gene pages skip or degrade those optional sections; the cohort variant table and
CSV export use `cohort_variants` only.
