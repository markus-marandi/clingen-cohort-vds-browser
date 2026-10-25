import React from 'react'
import { Link } from 'react-router-dom'
import {
  Sample,
  SampleVariant,
  ResultPage,
  SAMPLE_FIELDS,
  VARIANT_FIELDS,
  useCohortQuery,
  SampleLink,
  VariantLink,
  Table,
  TableScroll,
  Paging,
  usePagination,
  reportLabel,
} from './shared'

export const VARIANT_CARRIERS_QUERY = `query PilotVariantCarriers($id: String!, $offset: Int!, $after: String) {
  page: cohort_variant_carriers(variant_id: $id, offset: $offset, after: $after) {
    total_count has_more next_cursor rows { sample { ${SAMPLE_FIELDS} } variant { ${VARIANT_FIELDS} } }
  }
}`
export const GENE_CARRIERS_QUERY = `query PilotGeneCarriers($id: String!, $offset: Int!, $after: String) {
  page: cohort_gene_carriers(gene_symbol: $id, offset: $offset, after: $after) {
    total_count has_more next_cursor rows { sample { ${SAMPLE_FIELDS} } variant { ${VARIANT_FIELDS} } }
  }
}`

const CohortCarriers = ({ variantId, geneSymbol }: { variantId?: string; geneSymbol?: string }) => {
  const { offset, after, move } = usePagination()
  const { data, loading, error } = useCohortQuery<{
    page: ResultPage<{ sample: Sample; variant: SampleVariant }>
  }>(variantId ? VARIANT_CARRIERS_QUERY : GENE_CARRIERS_QUERY, {
    id: variantId || geneSymbol,
    offset: 0,
    after,
  })

  return (
    <section style={{ margin: '24px 0' }}>
      <h2>Carrier samples</h2>
      <p>
        <Link to="/samples">Browse cohort samples</Link>
      </p>
      {loading && <p role="status">Loading carrier samples…</p>}
      {error && <p role="alert">{error}</p>}
      {data && (
        <>
          <Paging
            offset={offset}
            total={data.page.total_count}
            hasMore={data.page.has_more && !!data.page.next_cursor}
            onChange={(next) => move(next, data.page.next_cursor)}
          />
          {!data.page.rows.length ? (
            <p>No carrier calls found.</p>
          ) : (
            <TableScroll>
              <Table>
                <thead>
                  <tr>
                    <th>Sample</th>
                    <th>Variant</th>
                    <th>Genotype</th>
                    <th>Depth</th>
                    <th>GQ</th>
                    <th>Alt fraction</th>
                    <th>Panels</th>
                    <th>In report</th>
                  </tr>
                </thead>
                <tbody>
                  {data.page.rows.map(({ sample, variant }) => (
                    <tr key={`${sample.sample_id}:${variant.variant_id}`}>
                      <td>
                        <SampleLink sampleId={sample.sample_id} />
                      </td>
                      <td>
                        <VariantLink variantId={variant.variant_id} />
                      </td>
                      <td>{variant.genotype}</td>
                      <td>{variant.depth ?? '—'}</td>
                      <td>{variant.gq ?? '—'}</td>
                      <td>
                        {variant.var_pct == null ? '—' : `${(100 * variant.var_pct).toFixed(1)}%`}
                      </td>
                      <td>{sample.panels.join(', ') || 'Not supplied'}</td>
                      <td>{reportLabel(variant.in_report)}</td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </TableScroll>
          )}
        </>
      )}
    </section>
  )
}

export default CohortCarriers
