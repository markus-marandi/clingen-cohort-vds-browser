import React from 'react'
import { Page, PageHeading } from '@gnomad/ui'
import { Link } from 'react-router-dom'
import DocumentTitle from '../DocumentTitle'
import {
  Sample,
  SampleVariant,
  ResultPage,
  SAMPLE_FIELDS,
  VARIANT_FIELDS,
  useCohortQuery,
  Table,
  TableScroll,
  VariantLink,
  VariantRows,
  Paging,
  usePagination,
} from './shared'

export const SAMPLE_QUERY = `query PilotSample($id: String!, $offset: Int!, $after: String) {
  sample: cohort_sample(sample_id: $id) { ${SAMPLE_FIELDS} }
  variants: cohort_sample_variants(sample_id: $id, offset: $offset, after: $after) {
    total_count has_more next_cursor rows { ${VARIANT_FIELDS} }
  }
}`

const FIELDS: [keyof Sample, string][] = [
  ['sex_assigned', 'Assigned sex'],
  ['date_of_birth', 'Date of birth'],
  ['date_seq', 'Sequencing date'],
  ['run_id', 'Run'],
  ['care_site', 'Care site'],
  ['material', 'Material'],
  ['health_status', 'Health status'],
  ['test', 'Assay'],
  ['instrument', 'Instrument'],
  ['ref_genome', 'Reference genome'],
]

const CohortSamplePage = ({ sampleId }: { sampleId: string }) => {
  const { offset, after, move } = usePagination()
  const { data, loading, error } = useCohortQuery<{
    sample: Sample | null
    variants: ResultPage<SampleVariant>
  }>(SAMPLE_QUERY, { id: sampleId, offset: 0, after })
  const sample = data?.sample

  return (
    <Page>
      <DocumentTitle title={`Sample ${sampleId}`} />
      <PageHeading>Sample {sampleId}</PageHeading>
      <p>
        <Link to="/samples">All samples</Link>
      </p>
      {loading && <p role="status">Loading sample…</p>}
      {error && <p role="alert">{error}</p>}
      {data && !sample && <p>Sample not found.</p>}
      {sample && data && (
        <>
          <h2>Sample metadata</h2>
          <TableScroll>
            <Table>
              <tbody>
                {FIELDS.map(([key, label]) => (
                  <tr key={key}>
                    <th>{label}</th>
                    <td>{String(sample[key] ?? 'Not supplied')}</td>
                  </tr>
                ))}
                <tr>
                  <th>Panels</th>
                  <td>{sample.panels.join(', ') || 'Not supplied'}</td>
                </tr>
              </tbody>
            </Table>
          </TableScroll>
          <h2>Reported variants</h2>
          <p>Report status: {sample.report_status}</p>
          {sample.reported_variants.length > 0 && (
            <ul>
              {sample.reported_variants.map((id) => (
                <li key={id}>
                  <VariantLink variantId={id} />
                </li>
              ))}
            </ul>
          )}
          {sample.omitted_report_entries > 0 && (
            <p>
              {sample.omitted_report_entries} malformed report entry omitted from variant links.
            </p>
          )}
          <h2>Called variants</h2>
          <Paging
            offset={offset}
            total={data.variants.total_count}
            hasMore={data.variants.has_more && !!data.variants.next_cursor}
            onChange={(next) => move(next, data.variants.next_cursor)}
          />
          {data.variants.rows.length ? (
            <VariantRows variants={data.variants.rows} />
          ) : (
            <p>No non-reference calls found.</p>
          )}
        </>
      )}
    </Page>
  )
}

export default CohortSamplePage
