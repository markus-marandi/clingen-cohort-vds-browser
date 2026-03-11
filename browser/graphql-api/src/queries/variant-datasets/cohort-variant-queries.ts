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

const formatVariant = (source: any) => ({
  variant_id: source.variant_id,
  chrom: source.chrom,
  pos: source.pos,
  ref: source.ref,
  alt: source.alt,
  rsids: source.rsids ?? [],
  exome: {
    ac: source.ac,
    an: source.an,
    af: source.af,
    homozygote_count: source.n_hom,
    filters: source.filters ?? [],
  },
  // genome slot is empty for exome-only cohorts
  genome: null,
})

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
