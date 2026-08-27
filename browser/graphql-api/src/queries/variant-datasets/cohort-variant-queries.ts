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

// gene_symbol is a keyword field populated from VEP SYMBOL (uppercase official symbol).
const geneSymbolFilter = (geneSymbol: string) => [{ term: { gene_symbol: geneSymbol } }]

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

// ── fetch by gene symbol (direct term query) ────────────────────────────────────

// Direct gene_symbol term query (UC-2/4/5/6). More precise than region overlap:
// returns only variants VEP annotated to this exact symbol, not every variant whose
// position happens to fall inside the gene's coordinates. `extraFilters` lets the
// combined-filter queries (UC-4/5/6) add clinvar_sig / consequence / revel_score etc.
const fetchVariantsByGeneSymbol = async (
  esClient: any,
  geneSymbol: string,
  extraFilters: any[] = []
) => {
  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body: {
      query: {
        bool: {
          filter: [...geneSymbolFilter(geneSymbol), ...extraFilters],
        },
      },
      sort: [{ pos: 'asc' }],
    },
    size: 10000,
  })

  return response.body.hits.hits.map((h: any) => formatVariant(h._source))
}

// ── fetch by gene ─────────────────────────────────────────────────────────────

// Prefer a precise gene_symbol term match. Fall back to region overlap when the gene
// has no symbol or the symbol is not present in the index, so the gene page never
// regresses to empty when a symbol source mismatches.
const fetchVariantsByGene = async (esClient: any, gene: any) => {
  const geneSymbol = gene.symbol ?? gene.gene_symbol ?? null

  if (geneSymbol) {
    const bySymbol = await fetchVariantsByGeneSymbol(esClient, geneSymbol)
    if (bySymbol.length > 0) {
      return bySymbol
    }
  }

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

// ── combined filter queries (UC-4/5/6) ──────────────────────────────────────────

// lightweight projection for filtered-variant tables (not the full Variant type)
const formatFilteredVariant = (source: any) => ({
  variant_id: source.variant_id,
  chrom: source.chrom,
  pos: source.pos,
  ref: source.ref,
  alt: source.alt,
  gene_symbol: source.gene_symbol ?? null,
  consequence: source.consequence ?? null,
  impact: source.impact ?? null,
  clinvar_sig: source.clinvar_sig ?? null,
  revel_score: source.revel_score ?? null,
  cadd_score: source.cadd_score ?? null,
  ac: source.ac_total ?? source.ac ?? null,
  an: source.an_total ?? source.an ?? null,
  af: source.af_total ?? source.af ?? null,
  hom_count: source.hom_count ?? source.n_hom ?? null,
})

// Build the ES filter clauses shared by UC-4 (gene + clinvar), UC-5 (gene panel +
// clinvar/revel), and UC-6 (consequence/impact + gene/region). Every argument is
// optional; callers combine whichever filters they need.
const buildCohortFilters = (args: any) => {
  const filter: any[] = []

  if (args.gene_symbol) {
    filter.push({ term: { gene_symbol: args.gene_symbol } })
  }
  if (args.gene_symbols && args.gene_symbols.length > 0) {
    filter.push({ terms: { gene_symbol: args.gene_symbols } })
  }
  if (args.clinvar_sig) {
    // ClinVar_CLNSIG may be a multi-value string (e.g. "Pathogenic/Likely_pathogenic"),
    // so match the requested significance as a substring rather than an exact keyword.
    filter.push({ wildcard: { clinvar_sig: { value: `*${args.clinvar_sig}*` } } })
  }
  if (args.consequence) {
    filter.push({ term: { consequence: args.consequence } })
  }
  if (args.impact) {
    filter.push({ term: { impact: args.impact } })
  }
  if (args.revel_min !== undefined && args.revel_min !== null) {
    filter.push({ range: { revel_score: { gte: args.revel_min } } })
  }
  if (args.chrom) {
    filter.push({ term: { chrom: args.chrom } })
  }
  const hasStart = args.start !== undefined && args.start !== null
  const hasStop = args.stop !== undefined && args.stop !== null
  if (hasStart || hasStop) {
    const range: any = {}
    if (hasStart) range.gte = args.start
    if (hasStop) range.lte = args.stop
    filter.push({ range: { pos: range } })
  }

  return filter
}

const fetchCohortFilteredVariants = async (esClient: any, args: any) => {
  const filter = buildCohortFilters(args)

  if (filter.length === 0) {
    throw new UserVisibleError('provide at least one filter')
  }

  const multiGene = Boolean(args.gene_symbols && args.gene_symbols.length > 0)

  const body: any = {
    query: { bool: { filter } },
    sort: [{ pos: 'asc' }],
  }

  if (multiGene) {
    // per-gene panel summary (UC-5): variant count + mean/max cohort AF per gene
    body.aggs = {
      by_gene: {
        terms: { field: 'gene_symbol', size: args.gene_symbols.length },
        aggs: {
          mean_af: { avg: { field: 'af_total' } },
          max_af: { max: { field: 'af_total' } },
        },
      },
    }
  }

  const response = await esClient.search({
    index: COHORT_VARIANT_INDEX,
    body,
    size: 10000,
  })

  const variants = response.body.hits.hits.map((h: any) => formatFilteredVariant(h._source))

  const geneSummaries = multiGene
    ? response.body.aggregations.by_gene.buckets.map((bucket: any) => ({
        gene_symbol: bucket.key,
        variant_count: bucket.doc_count,
        mean_af: bucket.mean_af.value,
        max_af: bucket.max_af.value,
      }))
    : []

  return { variants, gene_summaries: geneSummaries }
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
  fetchVariantsByGeneSymbol,
  fetchVariantsByRegion,
  fetchVariantsByTranscript,
  fetchMatchingVariants,
  fetchCohortSearchResults,
  fetchCohortSummary,
  fetchCohortFilteredVariants,
}

export { fetchCohortSearchResults, fetchCohortSummary, fetchCohortFilteredVariants }
