import { UserVisibleError } from '../errors'

const SAMPLES_INDEX = process.env.COHORT_SAMPLES_INDEX || 'cohort_samples'
const CARRIERS_INDEX = process.env.COHORT_SAMPLE_VARIANTS_INDEX || 'cohort_sample_variants'

type PageArgs = { limit?: number; offset?: number; after?: string | null }

const searchPage = async (
  esClient: any,
  index: string,
  filter: any[],
  args: PageArgs,
  sort: any[]
) => {
  const limit = args.limit ?? 100
  const offset = args.offset ?? 0
  if (
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 500 ||
    !Number.isInteger(offset) ||
    offset < 0 ||
    offset + limit + 1 > 10000
  ) {
    throw new UserVisibleError(
      'Use a limit of 1–500 and an offset within the first 10,000 results.'
    )
  }
  let searchAfter
  if (args.after) {
    try {
      searchAfter = JSON.parse(args.after)
      if (
        !Array.isArray(searchAfter) ||
        searchAfter.length !== sort.length ||
        searchAfter.some((value: any) => typeof value !== 'string' && typeof value !== 'number')
      ) {
        throw new Error('Invalid cursor')
      }
    } catch {
      throw new UserVisibleError('Invalid page cursor. Return to the first page.')
    }
  }
  let response
  try {
    response = await esClient.search({
      index,
      body: {
        query: { bool: { filter } },
        sort,
        track_total_hits: true,
        ...(searchAfter ? { search_after: searchAfter } : { from: offset }),
        size: limit + 1,
      },
    })
  } catch (error: any) {
    if (
      error.meta?.body?.error?.type === 'index_not_found_exception' ||
      error.body?.error?.type === 'index_not_found_exception'
    ) {
      throw new UserVisibleError('Internal sample data has not been exported for this browser.')
    }
    throw error
  }
  const hits = response.body.hits.hits.slice(0, limit)
  const rows = hits.map((hit: any) => hit._source)
  const total = response.body.hits.total
  const totalCount = typeof total === 'number' ? total : total.value
  const lastSort = hits[hits.length - 1]?.sort
  return {
    rows,
    total_count: totalCount,
    has_more: response.body.hits.hits.length > limit,
    next_cursor:
      response.body.hits.hits.length > limit && lastSort ? JSON.stringify(lastSort) : null,
  }
}

export const fetchCohortSamples = (esClient: any, args: PageArgs = {}) =>
  searchPage(esClient, SAMPLES_INDEX, [], args, [{ sample_id: 'asc' }])

export const fetchCohortSample = async (esClient: any, sampleId: string) => {
  const page = await searchPage(
    esClient,
    SAMPLES_INDEX,
    [{ term: { sample_id: sampleId } }],
    { limit: 1 },
    [{ sample_id: 'asc' }]
  )
  return page.rows[0] || null
}

export const fetchCohortSampleVariants = (esClient: any, sampleId: string, args: PageArgs = {}) =>
  searchPage(esClient, CARRIERS_INDEX, [{ term: { sample_id: sampleId } }], args, [
    { chrom: 'asc' },
    { pos: 'asc' },
    { variant_id: 'asc' },
  ])

const carriers = async (esClient: any, filter: any[], args: PageArgs) => {
  const page = await searchPage(esClient, CARRIERS_INDEX, filter, args, [
    { sample_id: 'asc' },
    { variant_id: 'asc' },
  ])
  const sampleIds = [...new Set(page.rows.map((row: any) => row.sample_id))]
  if (sampleIds.length === 0) return page
  const samplePage = await searchPage(
    esClient,
    SAMPLES_INDEX,
    [{ terms: { sample_id: sampleIds } }],
    { limit: sampleIds.length },
    [{ sample_id: 'asc' }]
  )
  const samples = new Map(samplePage.rows.map((sample: any) => [sample.sample_id, sample]))
  if (sampleIds.some((id) => !samples.has(id))) {
    throw new UserVisibleError(
      'Carrier data and sample metadata are out of sync. Re-export the pilot data.'
    )
  }
  return {
    ...page,
    rows: page.rows.map((variant: any) => ({ variant, sample: samples.get(variant.sample_id) })),
  }
}

export const fetchCohortVariantCarriers = (esClient: any, variantId: string, args: PageArgs = {}) =>
  carriers(esClient, [{ term: { variant_id: variantId } }], args)

export const fetchCohortGeneCarriers = (esClient: any, geneSymbol: string, args: PageArgs = {}) =>
  carriers(esClient, [{ term: { gene_symbol: geneSymbol.trim().toUpperCase() } }], args)
