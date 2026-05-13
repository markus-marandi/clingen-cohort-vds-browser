/**
 * variant queries for the local cohort dataset.
 *
 * reads from the cohort_variants elasticsearch index which is populated by
 * data-pipeline/cohort_export.py. the schema is intentionally flat compared
 * to gnomAD - each doc stores chrom, pos, ref, alt, ac, an, af, n_hom,
 * filters, and rsids.
 */

import { UserVisibleError } from '../../errors'

const COHORT_VARIANT_INDEX = 'cohort_variants'

// ── helpers ───────────────────────────────────────────────────────────────────

const formatVariant = (source: any) => {
  // ES data from annotated MT uses ac_total/an_total/af_total/hom_count.
  // VDS-fallback export uses ac/an/af/n_hom. Support both.
  const ac = source.ac_total ?? source.ac ?? null
  const an = source.an_total ?? source.an ?? null
  const af = source.af_total ?? source.af ?? null
  const hom = source.hom_count ?? source.n_hom ?? null

  return {
    variant_id: source.variant_id,
    variantId: source.variant_id,
    reference_genome: 'GRCh37',
    chrom: source.chrom,
    pos: source.pos,
    ref: source.ref,
    alt: source.alt,
    rsids: source.rsids ?? [],
    colocated_variants: [],
    colocatedVariants: [],
    multi_nucleotide_variants: [],
    multiNucleotideVariants: [],
    flags: [],
    coverage: { exome: null, genome: null },
    lof_curations: null,
    transcript_consequences: null,
    sortedTranscriptConsequences: null,
    in_silico_predictors: null,
    non_coding_constraint: null,
    // cohort-specific annotation fields (returned as top-level for CohortVariantPage)
    gene_symbol: source.gene_symbol ?? null,
    consequence: source.consequence ?? null,
    impact: source.impact ?? null,
    cdna: source.cdna ?? null,
    p_nomen: source.p_nomen ?? null,
    cadd_score: source.cadd_score ?? null,
    clinvar_sig: source.clinvar_sig ?? null,
    gnomad_af: source.gnomad_af ?? null,
    gnomad_nonfin: source.gnomad_nonfin ?? null,
    exome: {
      ac,
      an,
      af,
      homozygote_count: hom,
      filters: source.filters ?? [],
      flags: [],
      populations: [],
      quality_metrics: null,
    },
    genome: null,
    joint: null,
  }
}

const regionFilter = (chrom: string, start: number, stop: number) => [
  { term: { chrom } },
  { range: { pos: { gte: start, lte: stop } } },
]

// ── count ─────────────────────────────────────────────────────────────────────

const countVariantsInRegion = async (esClient: any, region: any) => {
  const response = await esClient.count({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          filter: regionFilter(region.chrom, region.start, region.stop),
        },
      },
    },
  })
  return response.body.count
}

// ── fetch by id ───────────────────────────────────────────────────────────────

const fetchVariantById = async (esClient: any, variantIdOrRsid: any) => {
  const isRsid = /^rs\d+$/.test(variantIdOrRsid)
  const idField = isRsid ? 'rsids' : 'variant_id'

  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: { bool: { filter: { term: { [idField]: variantIdOrRsid } } } },
    },
    size: 1,
  })

  if (response.body.hits.total.value === 0) {
    throw new UserVisibleError('variant not found')
  }

  if (response.body.hits.total.value > 1) {
    throw new UserVisibleError('multiple variants found, query using variant ID to select one')
  }

  return formatVariant(response.body.hits.hits[0]._source)
}

// ── fetch by gene ─────────────────────────────────────────────────────────────

// gene objects carry chrom/start/stop from the gene table; query the overlap
const fetchVariantsByGene = async (esClient: any, gene: any) => {
  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          filter: regionFilter(gene.chrom, gene.start, gene.stop),
        },
      },
      sort: [{ pos: 'asc' }],
    },
    size: 10000,
  })

  return response.body.hits.hits.map((h: any) => formatVariant(h._source))
}

// ── fetch by region ───────────────────────────────────────────────────────────

const fetchVariantsByRegion = async (esClient: any, region: any) => {
  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          filter: regionFilter(region.chrom, region.start, region.stop),
        },
      },
      sort: [{ pos: 'asc' }],
    },
    size: 10000,
  })

  return response.body.hits.hits.map((h: any) => formatVariant(h._source))
}

// ── fetch by transcript ───────────────────────────────────────────────────────

const fetchVariantsByTranscript = async (esClient: any, transcript: any) => {
  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          filter: regionFilter(transcript.chrom, transcript.start, transcript.stop),
        },
      },
      sort: [{ pos: 'asc' }],
    },
    size: 10000,
  })

  return response.body.hits.hits.map((h: any) => formatVariant(h._source))
}

// ── search / autocomplete ─────────────────────────────────────────────────────

const fetchMatchingVariants = async (esClient: any, { query, limit = 5 }: any) => {
  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          should: [
            { prefix: { variant_id: query } },
            { term: { rsids: query } },
          ],
        },
      },
    },
    size: limit,
  })

  return response.body.hits.hits.map((h: any) => ({
    variant_id: h._source.variant_id,
    chrom: h._source.chrom,
    pos: h._source.pos,
    ref: h._source.ref,
    alt: h._source.alt,
  }))
}

export default {
  countVariantsInRegion,
  fetchVariantById,
  fetchVariantsByGene,
  fetchVariantsByRegion,
  fetchVariantsByTranscript,
  fetchMatchingVariants,
}
