"""Quick sanity check on cohort_annotated.mt.

Prints:
  - dimensions and row/col field names
  - cohort freq non-null counts
  - VEP annotation fill rate
  - gnomAD join hit rate
  - ClinVar annotation counts
  - 3 example rows (variant + top fields)
"""

import os, sys
import hail as hl

MT_PATH   = '/mnt/sdb/gvcf_ustina/cohort_annotated.mt'
HAIL_JAR  = '/mnt/sdb/markus_files/hail/backend/hail-all-spark.jar'
TMP_DIR   = '/mnt/sdb/tmp/sanity_temp'

os.makedirs(TMP_DIR, exist_ok=True)

hl.init(
    master='local[4]',
    tmp_dir=TMP_DIR,
    spark_conf={
        'spark.driver.memory': '16g',
        'spark.local.dir': TMP_DIR,
        'spark.jars': HAIL_JAR,
        'spark.driver.extraClassPath': HAIL_JAR,
    },
    quiet=True,
    log='/tmp/hail_sanity.log',
)
hl.default_reference('GRCh37')

print(f'\nReading: {MT_PATH}')
mt = hl.read_matrix_table(MT_PATH)

# ── 1. Dimensions ──────────────────────────────────────────────────────────────
n_variants, n_samples = mt.count()
print(f'\n=== Dimensions ===')
print(f'  Variants : {n_variants:,}')
print(f'  Samples  : {n_samples:,}')

# ── 2. Field names ─────────────────────────────────────────────────────────────
print(f'\n=== Row fields ===')
for f, t in mt.row.dtype.items():
    print(f'  {f}: {t}')

print(f'\n=== Col fields ===')
for f, t in mt.col.dtype.items():
    print(f'  {f}: {t}')

# ── 3. Cohort frequencies ──────────────────────────────────────────────────────
print(f'\n=== Cohort frequencies (variant_qc) ===')
qc = mt.aggregate_rows(hl.struct(
    has_ac  = hl.agg.count_where(hl.is_defined(mt.variant_qc.AC[1])),
    mean_af = hl.agg.mean(mt.variant_qc.AF[1]),
    n_hom   = hl.agg.sum(mt.variant_qc.homozygote_count[1]),
))
print(f'  Variants with AC defined : {qc.has_ac:,}')
print(f'  Mean cohort AF           : {qc.mean_af:.6f}')
print(f'  Total hom-alt calls      : {qc.n_hom:,}')

# ── 4. VEP fill rate ───────────────────────────────────────────────────────────
print(f'\n=== VEP annotation ===')
# vep struct is flat: Consequence, IMPACT, SYMBOL, HGVSg/c/p, transcript_id,
# CADD_PHRED, CADD_RAW, ClinVar_CLNSIG, ClinVar_CLNREVSTAT
vep = mt.aggregate_rows(hl.struct(
    has_consequence = hl.agg.count_where(hl.is_defined(mt.vep.Consequence)),
    has_symbol      = hl.agg.count_where(hl.is_defined(mt.vep.SYMBOL)),
    has_cadd        = hl.agg.count_where(hl.is_defined(mt.vep.CADD_PHRED)),
))
print(f'  Variants with Consequence : {vep.has_consequence:,} / {n_variants:,}  ({100*vep.has_consequence/n_variants:.1f}%)')
print(f'  Variants with SYMBOL      : {vep.has_symbol:,} / {n_variants:,}  ({100*vep.has_symbol/n_variants:.1f}%)')
print(f'  Variants with CADD_PHRED  : {vep.has_cadd:,} / {n_variants:,}  ({100*vep.has_cadd/n_variants:.1f}%)')

# ── 5. gnomAD join ─────────────────────────────────────────────────────────────
print(f'\n=== gnomAD join ===')
gn = mt.aggregate_rows(hl.struct(
    has_gnomad_af     = hl.agg.count_where(hl.is_defined(mt.gnomad_af)),
    has_gnomad_nonfin = hl.agg.count_where(hl.is_defined(mt.gnomad_nonfin)),
))
print(f'  Variants with gnomad_af     : {gn.has_gnomad_af:,} / {n_variants:,}  ({100*gn.has_gnomad_af/n_variants:.1f}%)')
print(f'  Variants with gnomad_nonfin : {gn.has_gnomad_nonfin:,} / {n_variants:,}  ({100*gn.has_gnomad_nonfin/n_variants:.1f}%)')

# ── 6. ClinVar ─────────────────────────────────────────────────────────────────
print(f'\n=== ClinVar (from flat vep struct) ===')
cv = mt.aggregate_rows(hl.struct(
    has_clnsig     = hl.agg.count_where(hl.is_defined(mt.vep.ClinVar_CLNSIG)),
    has_pathogenic = hl.agg.count_where(
        hl.is_defined(mt.vep.ClinVar_CLNSIG) &
        mt.vep.ClinVar_CLNSIG.lower().contains('pathogenic')
    ),
))
print(f'  Variants with ClinVar_CLNSIG    : {cv.has_clnsig:,}')
print(f'  Variants pathogenic/likely_path : {cv.has_pathogenic:,}')

# ── 7. Example rows ────────────────────────────────────────────────────────────
print(f'\n=== Example variants (first 3) ===')
rows = mt.rows()
rows.select(
    'variant_qc',
    'gnomad_af',
    'gnomad_nonfin',
    'vep',
).show(3, width=120)

print('\nSanity check complete.')
