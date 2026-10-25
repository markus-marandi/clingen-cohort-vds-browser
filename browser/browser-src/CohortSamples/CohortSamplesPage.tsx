import React from 'react'
import { Page, PageHeading } from '@gnomad/ui'
import DocumentTitle from '../DocumentTitle'
import {
  Sample,
  ResultPage,
  SAMPLE_FIELDS,
  useCohortQuery,
  SampleLink,
  Table,
  TableScroll,
  Paging,
  usePagination,
} from './shared'

export const SAMPLES_QUERY = `query PilotSamples($offset: Int!, $after: String) {
  page: cohort_samples(offset: $offset, after: $after) { total_count has_more next_cursor rows { ${SAMPLE_FIELDS} } }
}`

const CohortSamplesPage = () => {
  const { offset, after, move } = usePagination()
  const { data, loading, error } = useCohortQuery<{ page: ResultPage<Sample> }>(SAMPLES_QUERY, {
    offset: 0,
    after,
  })
  return (
    <Page>
      <DocumentTitle title="Cohort samples" />
      <PageHeading>Cohort samples</PageHeading>
      {loading && <p role="status">Loading samples…</p>}
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
            <p>No samples found.</p>
          ) : (
            <TableScroll>
              <Table>
                <thead>
                  <tr>
                    <th>Sample</th>
                    <th>Assay</th>
                    <th>Panels</th>
                    <th>Report status</th>
                  </tr>
                </thead>
                <tbody>
                  {data.page.rows.map((sample) => (
                    <tr key={sample.sample_id}>
                      <td>
                        <SampleLink sampleId={sample.sample_id} />
                      </td>
                      <td>{sample.test || '—'}</td>
                      <td>{sample.panels.join(', ') || 'Not supplied'}</td>
                      <td>{sample.report_status}</td>
                    </tr>
                  ))}
                </tbody>
              </Table>
            </TableScroll>
          )}
        </>
      )}
    </Page>
  )
}

export default CohortSamplesPage
