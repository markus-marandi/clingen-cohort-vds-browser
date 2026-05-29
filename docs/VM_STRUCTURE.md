# VM Directory Structure

Canonical layout for `/mnt/sdb` on the internal analysis VM. All script paths and config files
should reference these locations. Do not put code inside data directories or reference data inside
the git repo.

Production serving uses a second public/browser VM. That VM should not mirror this full layout: it
should contain only the browser stack, public Elasticsearch storage, and sanitized variant-level
export bundles. It must not contain raw GVCFs, VDS, MatrixTables, metadata tables, patient IDs,
`sample_id`, or internal reference/data paths.

---

## Target Layout

```
/mnt/sdb/
├── projects/                            ← code and tools
│   ├── clingen-cohort-vds-browser/      ← canonical git clone (move from testing/)
│   ├── ensembl-vep/                     ← VEP binary (already here)
│   ├── bcftools/                        ← already here
│   └── samtools/                        ← already here
│
├── VEP/                                 ← VEP reference data (already here)
│   ├── VEP_cache/                       ← GRCh37 v108 merged cache
│   └── ref_fasta/                       ← ucsc.hg19.fasta
│
├── reference/                           ← annotation reference files (NOT in git)
│   ├── clinvar/                         ← move from Plugins/clinvar.vcf.gz
│   │   ├── clinvar.vcf.gz
│   │   └── clinvar.vcf.gz.tbi
│   ├── gnomad/                          ← move from Plugins/gnomad.*
│   │   ├── gnomad.exomes.r2.1.1.sites.ht/
│   │   ├── gnomad.exomes.r2.1.1.sites.vcf.bgz
│   │   └── gnomad.exomes.r2.1.1.sites.vcf.bgz.tbi
│   └── dbnsfp/                          ← dbNSFP v5.3.1 files (to download)
│       ├── dbNSFP5.3.1_variant.chr1.gz
│       ├── dbNSFP5.3.1_variant.chr1.gz.tbi
│       └── ...
│
└── data/                                ← genomic data (never in git)
    ├── raw_gvcfs/                       ← was andmebaas_test_valim/
    ├── filtered_gvcfs/                  ← was andmebaas_test_valim_filtered/
    ├── vds/                             ← Hail VDS files
    │   ├── cohort_10k.vds               ← move from gvcf_ustina/
    │   └── cohort_2026-03-11_run001.vds ← move from gvcf_ustina/
    ├── mt/                              ← Hail MatrixTables
    │   └── cohort_annotated.mt          ← move from gvcf_ustina/
    ├── tmp/                             ← combiner temp space (was temp/)
    └── logs/                            ← Hail log files (hail-*.log)
```

---

## Migration Steps

Run these manually on the VM. Each step is reversible before the next one.

### 1. Create target directories

```bash
mkdir -p /mnt/sdb/reference/clinvar
mkdir -p /mnt/sdb/reference/gnomad
mkdir -p /mnt/sdb/reference/dbnsfp
mkdir -p /mnt/sdb/data/raw_gvcfs
mkdir -p /mnt/sdb/data/filtered_gvcfs
mkdir -p /mnt/sdb/data/vds
mkdir -p /mnt/sdb/data/mt
mkdir -p /mnt/sdb/data/tmp
mkdir -p /mnt/sdb/data/logs
```

### 2. Move the code clone to projects/

```bash
mv /mnt/sdb/gvcf_ustina/testing/clingen-cohort-vds-browser \
   /mnt/sdb/projects/clingen-cohort-vds-browser
```

### 3. Move reference data out of Plugins/ into reference/

Run from inside the moved repo:

```bash
cd /mnt/sdb/projects/clingen-cohort-vds-browser

mv Plugins/clinvar.vcf.gz     /mnt/sdb/reference/clinvar/
mv Plugins/clinvar.vcf.gz.tbi /mnt/sdb/reference/clinvar/

mv Plugins/gnomad.exomes.r2.1.1.sites.ht      /mnt/sdb/reference/gnomad/
mv Plugins/gnomad.exomes.r2.1.1.sites.vcf.bgz /mnt/sdb/reference/gnomad/
mv Plugins/gnomad.exomes.r2.1.1.sites.vcf.bgz.tbi /mnt/sdb/reference/gnomad/

# Standalone CADD is no longer part of the target annotation plan.
# Keep CADD through dbNSFP fields instead; discard old CADD placeholders after confirming they are
# not needed by any active local run.

# Plugins/ directory in repo should now be empty — remove it from git tracking
rmdir Plugins  # only if empty
```

### 4. Move genomic data files

```bash
mv /mnt/sdb/gvcf_ustina/cohort_10k.vds              /mnt/sdb/data/vds/
mv "/mnt/sdb/gvcf_ustina/cohort_2026-03-11_run001.vds" /mnt/sdb/data/vds/
mv /mnt/sdb/gvcf_ustina/cohort_annotated.mt          /mnt/sdb/data/mt/
mv /mnt/sdb/gvcf_ustina/andmebaas_test_valim          /mnt/sdb/data/raw_gvcfs
mv /mnt/sdb/gvcf_ustina/andmebaas_test_valim_filtered /mnt/sdb/data/filtered_gvcfs
mv /mnt/sdb/gvcf_ustina/temp                          /mnt/sdb/data/tmp

# move Hail log files
mv /mnt/sdb/gvcf_ustina/hail-*.log /mnt/sdb/data/logs/
mv /mnt/sdb/gvcf_ustina/testing/clingen-cohort-vds-browser/hail-*.log /mnt/sdb/data/logs/ 2>/dev/null || true
```

### 5. Clean up the testing/ wrapper

After confirming the code clone is working from `projects/`:

```bash
# remove the agent session-note files
rm /mnt/sdb/gvcf_ustina/testing/*.md
rm /mnt/sdb/gvcf_ustina/testing/start_browser_stack.sh
rm /mnt/sdb/gvcf_ustina/testing/docker-compose-browser-only.yml
rmdir /mnt/sdb/gvcf_ustina/testing   # only if empty
```

### 6. Remove stale .git from the data directory

The data directory `/mnt/sdb/gvcf_ustina/` has a `.git` folder from an earlier working session.
It is NOT the code repo. Remove it after the code has been moved to `projects/`:

```bash
# verify nothing important is tracked
git -C /mnt/sdb/gvcf_ustina status
# if clean or only untracked data files, remove
rm -rf /mnt/sdb/gvcf_ustina/.git
```

### 7. Download dbNSFP (when ready)

```bash
cd /mnt/sdb/reference/dbnsfp
# register at https://www.dbnsfp.org/download and download dbNSFP v5.3.1
# choose the branch/license and GRCh37/VEP-ready files appropriate for the deployment
# then index each chromosome file:
# bgzip and tabix each chr file if not pre-indexed
```

---

## vep_settings.json Paths After Migration

Update `vep_settings.json` to reflect the new reference locations:

```json
{
  "command": [
    "/mnt/sdb/projects/ensembl-vep/vep",
    "--fasta", "/mnt/sdb/VEP/ref_fasta/ucsc.hg19.fasta",
    "--dir_cache", "/mnt/sdb/VEP/VEP_cache/",
    "--dir_plugins", "/mnt/sdb/projects/ensembl-vep/Plugins",
    "--plugin", "dbNSFP,/mnt/sdb/reference/dbnsfp/dbNSFP5.3.1_variant.chr%s.gz,<allowlisted_fields>",
    "..."
  ]
}
```

---

## Current State vs. Target (quick reference)

| Current location | Target location |
|---|---|
| `gvcf_ustina/testing/clingen-cohort-vds-browser/` | `projects/clingen-cohort-vds-browser/` |
| `gvcf_ustina/testing/*.md` (session notes) | delete |
| `Plugins/clinvar.vcf.gz*` | `reference/clinvar/` |
| `Plugins/gnomad.*` | `reference/gnomad/` |
| `gvcf_ustina/cohort_*.vds` | `data/vds/` |
| `gvcf_ustina/cohort_annotated.mt` | `data/mt/` |
| `gvcf_ustina/andmebaas_test_valim/` | `data/raw_gvcfs/` |
| `gvcf_ustina/andmebaas_test_valim_filtered/` | `data/filtered_gvcfs/` |
| `gvcf_ustina/temp/` | `data/tmp/` |
| `gvcf_ustina/hail-*.log` | `data/logs/` |
| `gvcf_ustina/.git` | delete (not the code repo) |
