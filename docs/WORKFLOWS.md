# Search workflows

Use the browser reached through the SSH tunnel in the [README](../README.md).
Select **Cohort**. These are structured searches; natural-language and HPO searches
are not available. All coordinates are GRCh37/hg19.

## 1. Variant → samples

Search the exact `chrom-pos-ref-alt` ID, for example `1-69270-A-G`, or open
`/variant/1-69270-A-G?dataset=cohort`. Read **Carrier samples**. Each row gives the
sample ID, genotype, depth, genotype quality, alternate-allele fraction, panels
and report membership. Click a sample ID to open its details.

`/samples` lists the selected samples. Sample details show raw metadata, panels,
reported variants and paged non-reference calls. Follow a called-variant link to
return to the variant page. Next/Previous displays 100 records at a time.

The corrected pilot exports one aggregate record per ALT. The server agent
reports all 155,346 distinct carrier IDs now have a matching aggregate record.
A carrier call and its annotation are separate records; never substitute another
ALT's variant page.

## 2. Gene → samples

Search an official gene symbol, e.g. `OR4F5`, or open
`/gene/OR4F5?dataset=cohort`. Read **Carrier samples** and follow sample links.
The table contains sample/variant pairs: one sample may have several rows.
Its record count is not a count of distinct people. Gene assignments depend on
available annotations. On rows with several alternate alleles, gene and
consequence are deliberately unavailable because the MT does not identify which
ALT owns its flattened annotation. Those calls remain accessible by variant ID
and on sample pages, but are absent from gene carrier results.

## 3. HPO → samples

Not implemented. This pilot has no imported HPO assignments, HPO resolver or
HPO sample-search page. A term with no result must not be interpreted as no
matching samples. HPO is separate follow-up work.

## 4. BRCA1 + ClinVar

Open `/filter`. Enter `BRCA1` in **Gene(s)**, choose **Pathogenic** under
**ClinVar significance**, then **Search**. Open variant links for details and
carrier samples. Run a separate **Likely pathogenic** query if required.

The current filter matches the selected ClinVar text as a case-sensitive
substring. Combined classifications may match. Review the actual labels and
review status; this is not a complete clinical classification policy. Missing
ClinVar values are excluded by a ClinVar filter, not classified as benign.

## 5. Multiple genes + significance or REVEL

In `/filter`, enter `APOB, LDLR, LDLRAP1, PCSK9` in **Gene(s)**.
Choose a ClinVar significance, or leave it at **Any** and set **REVEL ≥** to
`0.9`. If both are set they are combined with AND. Clear filters from a previous
query when changing questions.

The REVEL control is inclusive. For strictly `>0.9`, disregard rows exactly equal
to `0.9`; do not label the inclusive query as strict. A REVEL threshold and a
ClinVar classification answer different questions.

- **AC:** number of called copies of that ALT allele.
- **AN:** total called allele copies used for its frequency.
- **AF:** AC / AN; multiply by 100 for allele frequency in percent.
- **Hom:** number of homozygous ALT calls.
- **Mean AF / Max AF:** summaries of variant AF within each gene.

These do not calculate the percentage of samples carrying any qualifying
variant. That requires distinct-sample counting and an explicit denominator,
which this pilot does not provide. Missing annotation fields can exclude real
variants from a filtered result.

## 6. Consequence / impact + gene / region

For gene-level missense variants, enter a gene and choose `missense variant`
(the stored term is `missense_variant`). For possible LoF variants, query specific
terms separately, such as `stop_gained`, `frameshift_variant`,
`splice_acceptor_variant` and `splice_donor_variant`. There is no combined LoF
classification or multi-select consequence control.

For a region, supply **Chrom**, **Start** and **Stop**, optionally with
**Impact = HIGH**. Start and stop are inclusive, 1-based GRCh37 coordinates.
Example: Chrom `1`, Start `69000`, Stop `70000`. Translate an HGVS_g interval
into these fields on the correct assembly; the form does not parse HGVS text.
HIGH impact is a broad annotation category, not a verified LoF classification.

Consequence filtering currently matches the entire stored string. A row whose
value contains several terms joined with `&` can be missed by a single-term
query. The combined-filter page returns at most 10,000 variants and does not
provide complete paging beyond that limit. Narrow the query; its displayed
result count is the returned count. Sample/carrier tables use separate cursor
paging and are not subject to that offset limit.

## Metadata and exports

DOB/date strings are displayed as supplied, including `prenatal`. Panels retain
`Unknown` and `whole exome` separately. Report status `None` means no listed
report variant; `Unknown` means unavailable report membership. Neither is a
clinical diagnosis. Malformed report IDs are counted and omitted from links.

Where a gene/region variant table exposes CSV export, it exports that variant
table. The dedicated filter and carrier/sample tables currently have no CSV
export button. Do not treat a gene-page export as an export of a separate filter
query. Keep sample-linked downloads on the internal system.
