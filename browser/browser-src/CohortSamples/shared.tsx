import React, { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import styled from 'styled-components'

export type Sample = {
  sample_id: string
  sex_assigned: string | null
  date_of_birth: string | null
  date_seq: string | null
  run_id: string | null
  care_site: string | null
  material: string | null
  health_status: string | null
  test: string | null
  instrument: string | null
  ref_genome: string | null
  panels: string[]
  report_status: string
  reported_variants: string[]
  omitted_report_entries: number
}

export type SampleVariant = {
  sample_id: string
  variant_id: string
  gene_symbol: string | null
  consequence: string | null
  genotype: string
  depth: number | null
  gq: number | null
  var_pct: number | null
  in_report: boolean | null
}

export type ResultPage<T> = {
  rows: T[]
  total_count: number
  has_more: boolean
  next_cursor: string | null
}

export const SAMPLE_FIELDS = `sample_id sex_assigned date_of_birth date_seq run_id
  care_site material health_status test instrument ref_genome panels report_status
  reported_variants omitted_report_entries`
export const VARIANT_FIELDS = `sample_id variant_id gene_symbol consequence genotype
  depth gq var_pct in_report`

export const Table = styled.table`
  width: 100%;
  border-collapse: collapse;

  th,
  td {
    padding: 8px;
    border: 1px solid #ddd;
    text-align: left;
  }
`

export const TableScroll = styled.div`
  overflow-x: auto;
  margin: 16px 0;
`

export const useCohortQuery = <T,>(query: string, variables: Record<string, unknown>) => {
  const [state, setState] = useState<{
    data: T | null
    error: string | null
    loading: boolean
  }>({
    data: null,
    error: null,
    loading: true,
  })
  const body = JSON.stringify({ query, variables })
  useEffect(() => {
    const controller = new AbortController()
    setState({ data: null, error: null, loading: true })
    fetch('/api/', {
      method: 'POST',
      body,
      signal: controller.signal,
      headers: {
        'Content-Type': 'application/json',
        'X-GnomAD-Client': 'browser',
      },
    })
      .then(async (response) => {
        if (!response.ok) throw new Error('Unable to load internal sample data.')
        const result = await response.json()
        if (result.errors?.length) throw new Error(result.errors[0].message)
        if (!result.data) throw new Error('No sample response returned.')
        if (!controller.signal.aborted) setState({ data: result.data, error: null, loading: false })
      })
      .catch((error: Error) => {
        if (!controller.signal.aborted)
          setState({ data: null, error: error.message, loading: false })
      })
    return () => controller.abort()
  }, [body])
  return state
}

export const SampleLink = ({ sampleId }: { sampleId: string }) => (
  <Link to={`/sample/${encodeURIComponent(sampleId)}`}>{sampleId}</Link>
)

export const VariantLink = ({ variantId }: { variantId: string }) => (
  <Link to={`/variant/${encodeURIComponent(variantId)}?dataset=cohort`}>{variantId}</Link>
)

export const reportLabel = (value: boolean | null) => {
  if (value === null) return 'Unknown'
  return value ? 'Yes' : 'No'
}

// Cursors avoid Elasticsearch's 10,000-result offset window for sample calls.
export const usePagination = () => {
  const [page, setPage] = useState(0)
  const [cursors, setCursors] = useState<(string | null)[]>([null])
  return {
    offset: page * 100,
    after: cursors[page],
    move: (offset: number, nextCursor: string | null) => {
      const nextPage = offset / 100
      if (nextPage > page) {
        if (!nextCursor) return
        setCursors([...cursors.slice(0, page + 1), nextCursor])
      }
      setPage(nextPage)
    },
  }
}

export const Paging = ({
  offset,
  total,
  hasMore,
  onChange,
}: {
  offset: number
  total: number
  hasMore: boolean
  onChange: (offset: number) => void
}) => (
  <p>
    {total.toLocaleString()} matching records.{' '}
    <button
      type="button"
      disabled={offset === 0}
      onClick={() => onChange(Math.max(0, offset - 100))}
    >
      Previous
    </button>{' '}
    <button type="button" disabled={!hasMore} onClick={() => onChange(offset + 100)}>
      Next
    </button>
    {total > 0 && ` Showing ${offset + 1}–${Math.min(offset + 100, total)}.`}
  </p>
)

export const VariantRows = ({ variants }: { variants: SampleVariant[] }) => (
  <TableScroll>
    <Table>
      <thead>
        <tr>
          <th>Variant</th>
          <th>Gene</th>
          <th>Consequence</th>
          <th>Genotype</th>
          <th>Depth</th>
          <th>GQ</th>
          <th>Alt fraction</th>
          <th>In report</th>
        </tr>
      </thead>
      <tbody>
        {variants.map((variant) => (
          <tr key={variant.variant_id}>
            <td>
              <VariantLink variantId={variant.variant_id} />
            </td>
            <td>{variant.gene_symbol ?? 'unavailable'}</td>
            <td>{variant.consequence ?? 'unavailable'}</td>
            <td>{variant.genotype}</td>
            <td>{variant.depth ?? '—'}</td>
            <td>{variant.gq ?? '—'}</td>
            <td>{variant.var_pct == null ? '—' : `${(100 * variant.var_pct).toFixed(1)}%`}</td>
            <td>{reportLabel(variant.in_report)}</td>
          </tr>
        ))}
      </tbody>
    </Table>
  </TableScroll>
)
