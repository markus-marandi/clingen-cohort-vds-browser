# Cohort variant browser

Browse internal GRCh37/hg19 variants and the samples carrying them. The current
pilot contains 20 samples. HPO search is not available yet.

## Start and open the pilot

Open the internal browser address supplied by your administrator. Remote login
accounts, hostnames and network addresses are kept outside this repository.

On the internal server, from the project checkout:

```sh
scripts/internal-pilot.sh doctor
scripts/internal-pilot.sh start
scripts/internal-pilot.sh status
```

These commands start the prepared pilot and its existing dependencies. They do
not import, delete or reindex data. The pilot is separate from the existing
browser service; starting it does not replace that service. See
[operations](docs/OPERATIONS.md) for local ports, logs and setup details.

## Find answers

| Question | What to do in the browser |
|---|---|
| **1. Which samples carry a variant?** (`variant_id`) | Search a variant such as `1-69270-A-G`. Open it → **Carrier samples** → click a sample ID for metadata, panels, reports and called variants. |
| **2. Which samples have a variant in a gene?** (`gene_symbol`) | Search a gene, e.g. `OR4F5`. Open its gene page → **Carrier samples**. A sample can appear for several variants. |
| **3. Which samples have an HPO ID or term?** | **Not implemented in this pilot.** No HPO sample-search page is available. |
| **4. Which ClinVar pathogenic variants occur in BRCA1?** | Open **Filters** (`/filter`). Enter `BRCA1`, select ClinVar **Pathogenic**, then **Search**. Repeat with **Likely pathogenic** if wanted; check the returned ClinVar labels. |
| **5. How frequent are variants in the hypercholesterolaemia genes?** | In **Filters**, enter `APOB, LDLR, LDLRAP1, PCSK9`. Select ClinVar significance **or** enter REVEL `0.9`. Read each variant’s **AF** and the per-gene summary. REVEL means **≥0.9**; exclude exactly `0.9` for a strict `>0.9` result. |
| **6. Which missense/LoF variants occur in a gene or region?** | In **Filters**, combine a gene with **Consequence**, e.g. `missense_variant`. For a region, enter **Chrom / Start / Stop** and optionally **Impact = HIGH**. HGVS interval text is not accepted. |

AF is the proportion of called chromosomes carrying an allele, **not the
percentage of samples**. HIGH impact is not a definitive LoF classification.
See [workflow details](docs/WORKFLOWS.md) for exact matching and limits.

Further setup, pipeline commands and reference material are under
[docs/](docs/README.md). Historical tasks and notes are under
[docs/archive/](docs/archive/README.md).
