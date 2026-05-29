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
  const normalizedQuery = query?.variantId ?? query?.rsid ?? query?.query ?? query

  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          should: [
            { prefix: { variant_id: normalizedQuery } },
            { term: { rsids: normalizedQuery } },
          ],
          minimum_should_match: 1,
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

const formatConsequenceLabel = (value: string) => {
  const firstConsequence = value.split('&')[0]
  return firstConsequence.replace(/_/g, ' ')
}

const parseRegionSearchQuery = (query: string) => {
  const match = query.trim().match(/^([0-9]{1,2}|X|Y|M|MT)[:-]([0-9,]+)(?:[:-]([0-9,]+))?$/i)

  if (!match) {
    return null
  }

  const chrom = match[1].toUpperCase() === 'MT' ? 'M' : match[1].toUpperCase()
  const start = Number(match[2].replace(/,/g, ''))
  const stop = Number((match[3] || match[2]).replace(/,/g, ''))

  if (!Number.isFinite(start) || !Number.isFinite(stop)) {
    return null
  }

  return {
    chrom,
    start: Math.min(start, stop),
    stop: Math.max(start, stop),
  }
}

const formatSearchVariantResult = (source: any, context?: string) => ({
  label: context ? `${source.variant_id} (${context})` : source.variant_id,
  value: `/variant/${source.variant_id}?dataset=cohort`,
})

const formatGeneResult = (bucket: any) => {
  const topVariant = bucket.top_variant.hits.hits[0]?._source

  if (!topVariant) {
    return null
  }

  return formatSearchVariantResult(
    topVariant,
    `${bucket.key}, ${bucket.doc_count.toLocaleString()} variants`
  )
}

const fetchCohortSearchResults = async (esClient: any, query: string) => {
  const trimmedQuery = query.trim()
  const upperQuery = trimmedQuery.toUpperCase()

  if (!trimmedQuery) {
    return []
  }

  const region = parseRegionSearchQuery(trimmedQuery)

  if (region) {
    const regionResponse = await esClient.search({
      index: COHORT_VARIANT_INDEX,
      body: {
        query: {
          bool: {
            filter: regionFilter(region.chrom, region.start, region.stop),
          },
        },
        sort: [{ pos: 'asc' }],
      },
      size: 25,
    })

    const regionLabel = `${region.chrom}-${region.start}-${region.stop}`

    return regionResponse.body.hits.hits
      .sort((a: any, b: any) => {
        const aAlleleLength = (a._source.ref || '').length + (a._source.alt || '').length
        const bAlleleLength = (b._source.ref || '').length + (b._source.alt || '').length
        return aAlleleLength - bAlleleLength || a._source.pos - b._source.pos
      })
      .slice(0, 5)
      .map((h: any) => formatSearchVariantResult(h._source, regionLabel))
  }

  const variantResponse = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          should: [{ prefix: { variant_id: trimmedQuery } }, { term: { rsids: trimmedQuery } }],
          minimum_should_match: 1,
        },
      },
      sort: [{ pos: 'asc' }],
    },
    size: 5,
  })

  const variantResults = variantResponse.body.hits.hits.map((h: any) =>
    formatSearchVariantResult(h._source)
  )

  const geneResponse = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          filter: [{ prefix: { gene_symbol: upperQuery } }],
        },
      },
      aggs: {
        genes: {
          terms: { field: 'gene_symbol', size: 5, order: { _count: 'desc' } },
          aggs: {
            chrom: { terms: { field: 'chrom', size: 1 } },
            start: { min: { field: 'pos' } },
            stop: { max: { field: 'pos' } },
            top_variant: {
              top_hits: {
                size: 1,
                sort: [{ pos: 'asc' }],
                _source: ['variant_id'],
              },
            },
          },
        },
      },
    },
    size: 0,
  })

  const geneResults = geneResponse.body.aggregations.genes.buckets
    .filter((bucket: any) => bucket.chrom.buckets.length > 0)
    .map(formatGeneResult)
    .filter(Boolean)

  return [...variantResults, ...geneResults]
}

const fetchCohortSummary = async (esClient: any) => {
  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      track_total_hits: true,
      aggs: {
        genes: { cardinality: { field: 'gene_symbol' } },
        max_an: { max: { field: 'an_total' } },
        clinvar: { filter: { exists: { field: 'clinvar_sig' } } },
        consequences: { terms: { field: 'consequence', size: 6 } },
        chromosomes: { terms: { field: 'chrom', size: 30 } },
      },
    },
    size: 0,
  })

  const aggregations = response.body.aggregations
  const variantCount = response.body.hits.total.value
  const maxAn = aggregations.max_an.value

  return {
    variant_count: variantCount,
    gene_count: aggregations.genes.value,
    sample_count: maxAn ? Math.round(maxAn / 2) : null,
    clinvar_count: aggregations.clinvar.doc_count,
    consequence_counts: aggregations.consequences.buckets.map((bucket: any) => ({
      id: bucket.key,
      label: formatConsequenceLabel(bucket.key),
      count: bucket.doc_count,
    })),
    chromosome_counts: aggregations.chromosomes.buckets.map((bucket: any) => ({
      id: bucket.key,
      label: `Chr ${bucket.key}`,
      count: bucket.doc_count,
    })),
  }
}

export default {
  countVariantsInRegion,
  fetchVariantById,
  fetchVariantsByGene,
  fetchVariantsByRegion,
  fetchVariantsByTranscript,
  fetchMatchingVariants,
  fetchCohortSearchResults,
  fetchCohortSummary,
}

export { fetchCohortSearchResults, fetchCohortSummary }
