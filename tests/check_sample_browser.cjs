/* Synthetic integration check. Run with NODE_PATH pointing at browser dependencies. */
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const ts = require('typescript')
const { buildSchema, graphql } = require('graphql')
const React = require('react')
const renderer = require('react-test-renderer')
const { StaticRouter } = require('react-router-dom')

const root = path.resolve(__dirname, '..')
const modules = new Map()
class UserVisibleError extends Error {}

function load(relativePath) {
  const filename = path.resolve(root, relativePath)
  if (modules.has(filename)) return modules.get(filename).exports
  const source = fs.readFileSync(filename, 'utf8')
  const result = ts.transpileModule(source, {
    fileName: filename,
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      jsx: ts.JsxEmit.React,
      esModuleInterop: true,
      target: ts.ScriptTarget.ES2020,
    },
    reportDiagnostics: true,
  })
  assert.equal(
    (result.diagnostics || []).filter((d) => d.category === ts.DiagnosticCategory.Error).length,
    0,
    filename
  )
  const module = { exports: {} }
  modules.set(filename, module)
  const localRequire = (id) => {
    if (id.endsWith('/errors')) return { UserVisibleError }
    if (id === '@gnomad/ui')
      return {
        Page: ({ children }) => React.createElement('main', null, children),
        PageHeading: ({ children }) => React.createElement('h1', null, children),
      }
    if (id.endsWith('/DocumentTitle')) return { __esModule: true, default: () => null }
    if (id.startsWith('.')) {
      const base = path.resolve(path.dirname(filename), id)
      const found = ['.ts', '.tsx'].map((ext) => base + ext).find(fs.existsSync)
      return load(path.relative(root, found))
    }
    return require(id)
  }
  new Function('require', 'module', 'exports', result.outputText)(
    localRequire,
    module,
    module.exports
  )
  return module.exports
}

const queries = load('browser/graphql-api/src/queries/cohort-sample-queries.ts')
const schema = buildSchema(
  'directive @cost(value: Int!) on FIELD_DEFINITION\ntype Query { _noop: Boolean }\n' +
    fs.readFileSync(
      path.join(root, 'browser/graphql-api/src/graphql/types/cohort-samples.graphql'),
      'utf8'
    )
)
const sample = (id, reportStatus = 'None') => ({
  sample_id: id,
  date_of_birth: 'prenatal',
  sex_assigned: 'Unknown',
  panels: ['whole exome'],
  report_status: reportStatus,
  reported_variants: [],
  omitted_report_entries: 0,
})
let sampleRows = [sample('SYN_A', 'Unknown'), sample('SYN_B')]
const carrierRows = [
  {
    sample_id: 'SYN_A',
    variant_id: '1-10-A-G',
    chrom: '1',
    pos: 10,
    ref: 'A',
    alt: 'G',
    gene_symbol: 'TEST',
    genotype: '0/1',
    depth: 20,
    gq: 99,
    var_pct: 0.5,
    in_report: null,
  },
  {
    sample_id: 'SYN_B',
    variant_id: '1-11-A-T',
    chrom: '1',
    pos: 11,
    ref: 'A',
    alt: 'T',
    gene_symbol: 'TEST',
    genotype: '1/1',
    depth: 30,
    gq: 90,
    var_pct: 1,
    in_report: false,
  },
]
const requests = []
const es = {
  async search({ index, body }) {
    requests.push({ index, body })
    assert.equal(body.track_total_hits, true)
    assert.ok(!JSON.stringify(body).includes('.keyword'))
    let rows = index === 'cohort_samples' ? sampleRows : carrierRows
    for (const clause of body.query.bool.filter) {
      if (clause.term) {
        const [key, value] = Object.entries(clause.term)[0]
        rows = rows.filter((row) => row[key] === value)
      } else if (clause.terms) {
        const [key, values] = Object.entries(clause.terms)[0]
        rows = rows.filter((row) => values.includes(row[key]))
      }
    }
    const sortValues = (row) => body.sort.map((key) => row[Object.keys(key)[0]])
    rows = [...rows].sort((a, b) => {
      const left = sortValues(a)
      const right = sortValues(b)
      for (let i = 0; i < left.length; i += 1) {
        if (left[i] < right[i]) return -1
        if (left[i] > right[i]) return 1
      }
      return 0
    })
    const total = rows.length
    const start = body.search_after
      ? rows.findIndex(
          (row) => JSON.stringify(sortValues(row)) === JSON.stringify(body.search_after)
        ) + 1
      : body.from
    return {
      body: {
        hits: {
          total: { value: total },
          hits: rows
            .slice(start, start + body.size)
            .map((row) => ({ _source: row, sort: sortValues(row) })),
        },
      },
    }
  },
}
const rootValue = {
  cohort_samples: (args) => queries.fetchCohortSamples(es, args),
  cohort_sample: (args) => queries.fetchCohortSample(es, args.sample_id),
  cohort_sample_variants: (args) => queries.fetchCohortSampleVariants(es, args.sample_id, args),
  cohort_variant_carriers: (args) => queries.fetchCohortVariantCarriers(es, args.variant_id, args),
  cohort_gene_carriers: (args) => queries.fetchCohortGeneCarriers(es, args.gene_symbol, args),
}

const shared = load('browser/browser-src/CohortSamples/shared.tsx')
const SamplesPage = load('browser/browser-src/CohortSamples/CohortSamplesPage.tsx').default
const SamplePage = load('browser/browser-src/CohortSamples/CohortSamplePage.tsx').default
const Carriers = load('browser/browser-src/CohortSamples/CohortCarriers.tsx').default

global.fetch = async (_url, options) => {
  assert.equal(options.headers['X-GnomAD-Client'], 'browser')
  const { query, variables } = JSON.parse(options.body)
  return {
    ok: true,
    json: () => graphql({ schema, source: query, variableValues: variables, rootValue }),
  }
}

async function render(component, props = {}) {
  let tree
  await renderer.act(async () => {
    tree = renderer.create(
      React.createElement(StaticRouter, { location: '/' }, React.createElement(component, props))
    )
    await new Promise(setImmediate)
  })
  assert.equal(tree.root.findAll((node) => node.props.role === 'alert').length, 0)
  return tree
}

async function main() {
  let page = await queries.fetchCohortGeneCarriers(es, 'test')
  assert.equal(page.rows.length, 2)
  assert.equal(page.rows[0].sample.sample_id, 'SYN_A')
  assert.equal(page.rows[0].variant.in_report, null)
  assert.equal((await queries.fetchCohortVariantCarriers(es, '1-10-A-G')).rows.length, 1)
  assert.equal(await queries.fetchCohortSample(es, 'ABSENT'), null)
  await assert.rejects(() => queries.fetchCohortSamples(es, { limit: 501 }), /1–500/)
  await assert.rejects(
    () =>
      queries.fetchCohortSamples({
        search: async () => {
          throw new Error('backend failure')
        },
      }),
    /backend failure/
  )
  await assert.rejects(
    () =>
      queries.fetchCohortSamples({
        search: async () => {
          throw { meta: { body: { error: { type: 'index_not_found_exception' } } } }
        },
      }),
    /not been exported/
  )
  // Traverse beyond the normal ES result window using the cursor, not deep offsets.
  const savedCarriers = [...carrierRows]
  carrierRows.splice(
    0,
    carrierRows.length,
    ...Array.from({ length: 10002 }, (_, i) => ({
      ...savedCarriers[0],
      variant_id: `1-${i + 1}-A-G`,
      pos: i + 1,
    }))
  )
  let after = null
  let count = 0
  do {
    const next = await queries.fetchCohortSampleVariants(es, 'SYN_A', { limit: 500, after })
    count += next.rows.length
    after = next.next_cursor
  } while (after)
  assert.equal(count, 10002)
  carrierRows.splice(0, carrierRows.length, ...savedCarriers)
  const saved = sampleRows
  sampleRows = []
  await assert.rejects(() => queries.fetchCohortVariantCarriers(es, '1-10-A-G'), /out of sync/)
  sampleRows = saved
  assert.equal(shared.reportLabel(null), 'Unknown')
  assert.equal(shared.reportLabel(false), 'No')

  const carriers = await render(Carriers, { variantId: '1-10-A-G' })
  assert.ok(carriers.root.findAllByType('a').some((node) => node.props.href === '/sample/SYN_A'))
  assert.ok(JSON.stringify(carriers.toJSON()).includes('0/1'))
  carriers.unmount()
  const gene = await render(Carriers, { geneSymbol: 'TEST' })
  assert.equal(gene.root.findAllByType('tbody')[0].findAllByType('tr').length, 2)
  gene.unmount()
  const samplePage = await render(SamplePage, { sampleId: 'SYN_A' })
  const text = JSON.stringify(samplePage.toJSON())
  assert.ok(text.includes('prenatal'))
  assert.ok(text.includes('Unknown'))
  assert.ok(
    samplePage.root
      .findAllByType('a')
      .some((node) => node.props.href === '/variant/1-10-A-G?dataset=cohort')
  )
  samplePage.unmount()

  sampleRows = Array.from({ length: 101 }, (_, i) => sample(`SYN_${i}`))
  const samplesPage = await render(SamplesPage)
  assert.equal(samplesPage.root.findAllByType('tbody')[0].findAllByType('tr').length, 100)
  await renderer.act(async () => {
    samplesPage.root
      .findAllByType('button')
      .find((node) => node.children.includes('Next'))
      .props.onClick()
    await new Promise(setImmediate)
  })
  assert.equal(samplesPage.root.findAllByType('tbody')[0].findAllByType('tr').length, 1)
  assert.ok(requests.some((request) => request.body.search_after))
  samplesPage.unmount()
  console.log(
    'PASS: sample/carrier queries, GraphQL operations, rendered pages, links, pagination, and error behavior (synthetic data).'
  )
}

main().catch((error) => {
  console.error(error)
  process.exitCode = 1
})
