import React, { useCallback, useState } from 'react'
import { Link } from 'react-router-dom'
import styled from 'styled-components'

import DocumentTitle from './DocumentTitle'
import InfoPage from './InfoPage'

// Combined-filter browse page (UC-4/5/6). Drives the cohort_filtered_variants GraphQL
// query: gene_symbol(s) + ClinVar significance, consequence/impact, and a REVEL threshold,
// optionally scoped to a chrom/pos region. Multi-gene input returns a per-gene summary.

const Page = styled(InfoPage)`
  max-width: 1100px;
  padding: clamp(24px, 5vw, 48px) clamp(16px, 4vw, 32px);
`

const Heading = styled.h1`
  margin: 0 0 6px;
  color: var(--navy);
  font-size: clamp(24px, 4vw, 30px);
  font-weight: 700;
`

const Lead = styled.p`
  margin: 0 0 28px;
  color: var(--text-secondary);
  font-size: 15px;
  line-height: 1.5;
`

const Form = styled.form`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 16px;
  align-items: end;
  padding: 20px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--gray-50);
`

const Field = styled.label`
  display: flex;
  flex-direction: column;
  gap: 6px;
  color: var(--navy);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.04em;
  text-transform: uppercase;
`

const controlStyles = `
  padding: 8px 10px;
  border: 1px solid var(--border);
  border-radius: 6px;
  background: var(--white);
  color: var(--navy);
  font-size: 14px;
  font-weight: 500;
  text-transform: none;
  letter-spacing: 0;
`

const Input = styled.input`
  ${controlStyles}
`

const Select = styled.select`
  ${controlStyles}
`

const Actions = styled.div`
  display: flex;
  gap: 10px;
  align-items: center;
`

const SubmitButton = styled.button`
  padding: 9px 18px;
  border: none;
  border-radius: 6px;
  background: var(--teal);
  color: var(--white);
  font-size: 14px;
  font-weight: 700;
  cursor: pointer;

  &:disabled {
    opacity: 0.6;
    cursor: default;
  }
`

const Status = styled.p`
  margin: 20px 0 0;
  color: var(--text-secondary);
  font-size: 14px;
`

const TableWrapper = styled.div`
  overflow-x: auto;
  margin-top: 24px;
`

const Table = styled.table`
  width: 100%;
  border-collapse: collapse;
  font-size: 13px;

  th,
  td {
    padding: 8px 10px;
    border-bottom: 1px solid var(--border);
    text-align: left;
    white-space: nowrap;
  }

  th {
    color: var(--text-secondary);
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  tbody tr:hover {
    background: var(--teal-20);
  }
`

const SectionTitle = styled.h2`
  margin: 32px 0 8px;
  color: var(--navy);
  font-size: 16px;
  font-weight: 700;
`

// VEP IMPACT categories.
const IMPACT_OPTIONS = ['HIGH', 'MODERATE', 'LOW', 'MODIFIER']

// Common VEP consequence terms for the cohort; not exhaustive.
const CONSEQUENCE_OPTIONS = [
  'missense_variant',
  'synonymous_variant',
  'stop_gained',
  'stop_lost',
  'start_lost',
  'frameshift_variant',
  'inframe_insertion',
  'inframe_deletion',
  'splice_acceptor_variant',
  'splice_donor_variant',
  'splice_region_variant',
  'intron_variant',
  '5_prime_UTR_variant',
  '3_prime_UTR_variant',
]

// Canonical ClinVar significance values; matched as substrings server-side so a
// compound value like "Pathogenic/Likely_pathogenic" still hits "Pathogenic".
const CLINVAR_OPTIONS = [
  'Pathogenic',
  'Likely_pathogenic',
  'Uncertain_significance',
  'Likely_benign',
  'Benign',
  'Conflicting_interpretations_of_pathogenicity',
]

const QUERY = `
  query CohortFilteredVariants(
    $gene_symbol: String
    $gene_symbols: [String!]
    $clinvar_sig: String
    $consequence: String
    $impact: String
    $revel_min: Float
    $chrom: String
    $start: Int
    $stop: Int
  ) {
    cohort_filtered_variants(
      gene_symbol: $gene_symbol
      gene_symbols: $gene_symbols
      clinvar_sig: $clinvar_sig
      consequence: $consequence
      impact: $impact
      revel_min: $revel_min
      chrom: $chrom
      start: $start
      stop: $stop
    ) {
      variants {
        variant_id
        gene_symbol
        consequence
        impact
        clinvar_sig
        revel_score
        cadd_score
        af
        ac
        an
        hom_count
      }
      gene_summaries {
        gene_symbol
        variant_count
        mean_af
        max_af
      }
    }
  }
`

type FilteredVariant = {
  variant_id: string
  gene_symbol: string | null
  consequence: string | null
  impact: string | null
  clinvar_sig: string | null
  revel_score: number | null
  cadd_score: number | null
  af: number | null
  ac: number | null
  an: number | null
  hom_count: number | null
}

type GeneSummary = {
  gene_symbol: string
  variant_count: number
  mean_af: number | null
  max_af: number | null
}

const parseGeneSymbols = (raw: string): string[] =>
  raw
    .split(/[\s,]+/)
    .map((token) => token.trim().toUpperCase())
    .filter(Boolean)

const formatFloat = (value: number | null, digits = 3) =>
  value === null || value === undefined ? '—' : value.toFixed(digits)

const formatInt = (value: number | null) =>
  value === null || value === undefined ? '—' : value.toLocaleString()

const CohortFilterPage = () => {
  const [genes, setGenes] = useState('')
  const [clinvarSig, setClinvarSig] = useState('')
  const [consequence, setConsequence] = useState('')
  const [impact, setImpact] = useState('')
  const [revelMin, setRevelMin] = useState('')
  const [chrom, setChrom] = useState('')
  const [start, setStart] = useState('')
  const [stop, setStop] = useState('')

  const [status, setStatus] = useState<'idle' | 'loading' | 'done' | 'error'>('idle')
  const [variants, setVariants] = useState<FilteredVariant[]>([])
  const [geneSummaries, setGeneSummaries] = useState<GeneSummary[]>([])

  const onSubmit = useCallback(
    (event: React.FormEvent) => {
      event.preventDefault()

      const geneSymbols = parseGeneSymbols(genes)
      const variables: Record<string, unknown> = {}
      if (geneSymbols.length === 1) {
        variables.gene_symbol = geneSymbols[0]
      } else if (geneSymbols.length > 1) {
        variables.gene_symbols = geneSymbols
      }
      if (clinvarSig) variables.clinvar_sig = clinvarSig
      if (consequence) variables.consequence = consequence
      if (impact) variables.impact = impact
      if (revelMin) variables.revel_min = Number(revelMin)
      if (chrom) variables.chrom = chrom.trim()
      if (start) variables.start = Number(start)
      if (stop) variables.stop = Number(stop)

      if (Object.keys(variables).length === 0) {
        setStatus('error')
        setVariants([])
        setGeneSummaries([])
        return
      }

      setStatus('loading')
      fetch('/api/', {
        body: JSON.stringify({ query: QUERY, variables }),
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
      })
        .then((response) => response.json())
        .then((response) => {
          const result = response.data?.cohort_filtered_variants
          if (!result) {
            setStatus('error')
            return
          }
          setVariants(result.variants ?? [])
          setGeneSummaries(result.gene_summaries ?? [])
          setStatus('done')
        })
        .catch(() => setStatus('error'))
    },
    [genes, clinvarSig, consequence, impact, revelMin, chrom, start, stop]
  )

  return (
    <Page>
      <DocumentTitle title="Filter variants" />
      <Heading>Filter cohort variants</Heading>
      <Lead>
        Combine gene, ClinVar significance, consequence/impact, and REVEL thresholds
        (use cases UC-4, UC-5, UC-6). Enter multiple genes (comma or space separated) to
        get a per-gene panel summary.
      </Lead>

      <Form onSubmit={onSubmit}>
        <Field>
          Gene(s)
          <Input
            type="text"
            value={genes}
            onChange={(e) => setGenes(e.target.value)}
            placeholder="e.g. LDLR, APOB, PCSK9"
          />
        </Field>
        <Field>
          ClinVar significance
          <Select value={clinvarSig} onChange={(e) => setClinvarSig(e.target.value)}>
            <option value="">Any</option>
            {CLINVAR_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option.replace(/_/g, ' ')}
              </option>
            ))}
          </Select>
        </Field>
        <Field>
          Consequence
          <Select value={consequence} onChange={(e) => setConsequence(e.target.value)}>
            <option value="">Any</option>
            {CONSEQUENCE_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option.replace(/_/g, ' ')}
              </option>
            ))}
          </Select>
        </Field>
        <Field>
          Impact
          <Select value={impact} onChange={(e) => setImpact(e.target.value)}>
            <option value="">Any</option>
            {IMPACT_OPTIONS.map((option) => (
              <option key={option} value={option}>
                {option}
              </option>
            ))}
          </Select>
        </Field>
        <Field>
          REVEL ≥
          <Input
            type="number"
            min="0"
            max="1"
            step="0.05"
            value={revelMin}
            onChange={(e) => setRevelMin(e.target.value)}
            placeholder="e.g. 0.75"
          />
        </Field>
        <Field>
          Chrom
          <Input
            type="text"
            value={chrom}
            onChange={(e) => setChrom(e.target.value)}
            placeholder="e.g. 17"
          />
        </Field>
        <Field>
          Start
          <Input type="number" value={start} onChange={(e) => setStart(e.target.value)} />
        </Field>
        <Field>
          Stop
          <Input type="number" value={stop} onChange={(e) => setStop(e.target.value)} />
        </Field>
        <Actions>
          <SubmitButton type="submit" disabled={status === 'loading'}>
            {status === 'loading' ? 'Searching…' : 'Search'}
          </SubmitButton>
        </Actions>
      </Form>

      {status === 'error' && <Status>Provide at least one filter, or try again.</Status>}
      {status === 'done' && (
        <Status>
          {variants.length.toLocaleString()} variant{variants.length === 1 ? '' : 's'} matched.
        </Status>
      )}

      {geneSummaries.length > 0 && (
        <>
          <SectionTitle>Per-gene summary</SectionTitle>
          <TableWrapper>
            <Table>
              <thead>
                <tr>
                  <th>Gene</th>
                  <th>Variants</th>
                  <th>Mean AF</th>
                  <th>Max AF</th>
                </tr>
              </thead>
              <tbody>
                {geneSummaries.map((row) => (
                  <tr key={row.gene_symbol}>
                    <td>{row.gene_symbol}</td>
                    <td>{formatInt(row.variant_count)}</td>
                    <td>{formatFloat(row.mean_af, 6)}</td>
                    <td>{formatFloat(row.max_af, 6)}</td>
                  </tr>
                ))}
              </tbody>
            </Table>
          </TableWrapper>
        </>
      )}

      {variants.length > 0 && (
        <>
          <SectionTitle>Variants</SectionTitle>
          <TableWrapper>
            <Table>
              <thead>
                <tr>
                  <th>Variant</th>
                  <th>Gene</th>
                  <th>Consequence</th>
                  <th>Impact</th>
                  <th>ClinVar</th>
                  <th>REVEL</th>
                  <th>CADD</th>
                  <th>AF</th>
                  <th>AC</th>
                  <th>Hom</th>
                </tr>
              </thead>
              <tbody>
                {variants.map((variant) => (
                  <tr key={variant.variant_id}>
                    <td>
                      <Link to={`/variant/${variant.variant_id}?dataset=cohort`}>
                        {variant.variant_id}
                      </Link>
                    </td>
                    <td>{variant.gene_symbol ?? '—'}</td>
                    <td>{variant.consequence ? variant.consequence.replace(/_/g, ' ') : '—'}</td>
                    <td>{variant.impact ?? '—'}</td>
                    <td>{variant.clinvar_sig ?? '—'}</td>
                    <td>{formatFloat(variant.revel_score)}</td>
                    <td>{formatFloat(variant.cadd_score, 1)}</td>
                    <td>{formatFloat(variant.af, 6)}</td>
                    <td>{formatInt(variant.ac)}</td>
                    <td>{formatInt(variant.hom_count)}</td>
                  </tr>
                ))}
              </tbody>
            </Table>
          </TableWrapper>
        </>
      )}
    </Page>
  )
}

export default CohortFilterPage
