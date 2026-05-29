import {
  fetchCohortSearchResults,
  fetchCohortSummary,
} from '../../queries/variant-datasets/cohort-variant-queries'

const resolvers = {
  Query: {
    cohort_search: (_obj: any, args: any, ctx: any) =>
      fetchCohortSearchResults(ctx.esClient, args.query),
    cohort_summary: (_obj: any, _args: any, ctx: any) => fetchCohortSummary(ctx.esClient),
  },
}

export default resolvers
