import {
  fetchCohortSearchResults,
  fetchCohortSummary,
  fetchCohortFilteredVariants,
} from '../../queries/variant-datasets/cohort-variant-queries'
import {
  fetchCohortSamples,
  fetchCohortSample,
  fetchCohortSampleVariants,
  fetchCohortVariantCarriers,
  fetchCohortGeneCarriers,
} from '../../queries/cohort-sample-queries'

const resolvers = {
  Query: {
    cohort_samples: (_obj: any, args: any, ctx: any) => fetchCohortSamples(ctx.esClient, args),
    cohort_sample: (_obj: any, args: any, ctx: any) =>
      fetchCohortSample(ctx.esClient, args.sample_id),
    cohort_sample_variants: (_obj: any, args: any, ctx: any) =>
      fetchCohortSampleVariants(ctx.esClient, args.sample_id, args),
    cohort_variant_carriers: (_obj: any, args: any, ctx: any) =>
      fetchCohortVariantCarriers(ctx.esClient, args.variant_id, args),
    cohort_gene_carriers: (_obj: any, args: any, ctx: any) =>
      fetchCohortGeneCarriers(ctx.esClient, args.gene_symbol, args),
    cohort_search: (_obj: any, args: any, ctx: any) =>
      fetchCohortSearchResults(ctx.esClient, args.query),
    cohort_summary: (_obj: any, _args: any, ctx: any) => fetchCohortSummary(ctx.esClient),
    cohort_filtered_variants: (_obj: any, args: any, ctx: any) =>
      fetchCohortFilteredVariants(ctx.esClient, args),
  },
}

export default resolvers
