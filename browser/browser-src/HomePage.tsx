import queryString from 'query-string'
import React, { useEffect, useMemo, useState } from 'react'
import styled from 'styled-components'

import DocumentTitle from './DocumentTitle'
import BrandMark from './Brand'
import InfoPage from './InfoPage'
import Link from './Link'
import Searchbox from './Searchbox'

const Page = styled(InfoPage)`
  max-width: 1200px;
  padding-top: clamp(34px, 7vw, 68px);
  padding-right: clamp(16px, 4vw, 32px);
  padding-left: clamp(16px, 4vw, 32px);
`

const Hero = styled.section`
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
  max-width: 780px;
  margin: 0 auto;
  text-align: center;
`

const HeroMark = styled(BrandMark)`
  width: 128px;
  height: 128px;
  margin-bottom: 18px;
  object-fit: contain;

  @media (max-width: 520px) {
    width: 104px;
    height: 104px;
  }
`

const Eyebrow = styled.div`
  margin-bottom: 8px;
  color: var(--text-secondary);
  font-size: 12px;
  font-weight: 700;
  letter-spacing: 0.12em;
  text-transform: uppercase;
`

const Heading = styled.h1`
  margin: 0 0 10px;
  color: var(--navy);
  font-size: clamp(28px, 5vw, 32px);
  font-weight: 700;
  letter-spacing: 0;
  line-height: 1.18;
`

const Lead = styled.p`
  max-width: 620px;
  margin: 0 0 34px;
  color: var(--text-secondary);
  font-size: 16px;
  line-height: 1.5;
`

const SearchPanel = styled.div`
  width: 100%;
  max-width: 640px;
`

const Hint = styled.p`
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  align-items: center;
  gap: 6px;
  margin: 12px 0 34px;
  color: var(--text-secondary);
  font-size: 13px;
`

const HintLink = styled(Link)`
  display: inline-flex;
  max-width: 100%;
  padding: 2px 7px;
  border-radius: 4px;
  background: var(--navy-20);
  color: var(--navy);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: 12px;
  font-weight: 700;
  line-height: 1.35;
  text-decoration: none;

  &:hover {
    background: var(--teal-20);
    color: var(--teal-dark);
  }
`

const QuickLinks = styled.nav`
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(132px, 1fr));
  gap: 12px;
  width: 100%;
  max-width: 720px;
  margin-bottom: 36px;
`

const QuickLink = styled(Link)`
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 8px;
  min-height: 92px;
  padding: 18px 14px;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--white);
  color: var(--navy);
  text-decoration: none;
  transition: border-color 120ms ease, background 120ms ease, box-shadow 120ms ease,
    transform 120ms ease;

  &:hover {
    border-color: var(--teal);
    background: var(--teal-20);
    box-shadow: var(--shadow-sm);
    color: var(--navy);
    transform: translateY(-1px);
  }
`

const QuickLinkCode = styled.span`
  color: var(--teal-dark);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', monospace;
  font-size: 16px;
  font-weight: 700;
`

const QuickLinkLabel = styled.span`
  font-size: 12px;
  font-weight: 800;
  letter-spacing: 0.08em;
  text-transform: uppercase;
`

const Divider = styled.div`
  display: flex;
  align-items: center;
  width: 100%;
  max-width: 640px;
  gap: 12px;
  margin-bottom: 18px;
  color: var(--text-secondary);
  font-size: 12px;

  &::before,
  &::after {
    flex: 1;
    height: 1px;
    background: var(--border);
    content: '';
  }
`

const ExamplePills = styled.div`
  display: flex;
  flex-wrap: wrap;
  justify-content: center;
  max-width: 720px;
  gap: 8px;
`

const ExamplePill = styled(Link)`
  padding: 5px 12px;
  border-radius: 999px;
  background: var(--navy-20);
  color: var(--navy);
  font-size: 13px;
  font-weight: 600;
  text-decoration: none;

  &:hover {
    background: var(--teal-20);
    color: var(--teal-dark);
  }
`

const Stats = styled.section`
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 1px;
  overflow: hidden;
  width: 100%;
  margin: 0;
  border: 1px solid var(--border);
  border-radius: 8px;
  background: var(--border);

  @media (max-width: 420px) {
    grid-template-columns: 1fr;
  }
`

const Stat = styled.div`
  padding: 22px 18px;
  background: var(--gray-50);
  text-align: center;
`

const StatValue = styled.div`
  color: var(--navy);
  font-size: 24px;
  font-weight: 700;
  letter-spacing: 0;
`

const StatLabel = styled.div`
  margin-top: 4px;
  color: var(--text-secondary);
  font-size: 11px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
`

const SummaryPanel = styled.section`
  display: grid;
  grid-template-columns: minmax(0, 0.9fr) minmax(280px, 1.1fr);
  gap: 22px;
  align-items: stretch;
  max-width: 1040px;
  margin: 56px auto 0;

  @media (max-width: 820px) {
    grid-template-columns: 1fr;
  }
`

const Chart = styled.div`
  padding: 20px;
  border: 1px solid var(--border);
  border-left: 4px solid var(--teal);
  border-radius: 8px;
  background: var(--gray-50);
`

const ChartTitle = styled.h2`
  margin: 0 0 16px;
  color: var(--navy);
  font-size: 16px;
  font-weight: 700;
`

const BarRow = styled.div`
  display: grid;
  grid-template-columns: minmax(110px, 1fr) minmax(120px, 2fr) minmax(56px, auto);
  gap: 10px;
  align-items: center;
  margin-top: 10px;
  color: var(--text-secondary);
  font-size: 12px;

  @media (max-width: 520px) {
    grid-template-columns: 1fr;
    gap: 4px;
  }
`

const BarTrack = styled.div`
  overflow: hidden;
  height: 8px;
  border-radius: 999px;
  background: var(--navy-20);
`

const BarFill = styled.div<{ $width: number; $tone: 'teal' | 'pink' | 'navy' }>`
  width: ${(props) => props.$width}%;
  height: 100%;
  border-radius: 999px;
  background: ${(props) =>
    props.$tone === 'pink'
      ? 'var(--pink)'
      : props.$tone === 'navy'
      ? 'var(--navy)'
      : 'var(--teal)'};
`

type CohortSummaryBucket = {
  id: string
  label: string
  count: number
}

type CohortSummary = {
  variant_count: number
  gene_count: number | null
  sample_count: number | null
  clinvar_count: number | null
  consequence_counts: CohortSummaryBucket[]
}

const formatNumber = (value: number | null | undefined) => {
  if (value === null || value === undefined) {
    return '—'
  }
  return value.toLocaleString()
}

const searchTo = (query: string) => ({
  pathname: '/awesome',
  search: queryString.stringify({ query }),
})

const quickLinks = [
  { code: 'GENE', label: 'MUC4', query: 'MUC4' },
  { code: 'REG', label: '1:874778-906100', query: '1-874778-906100' },
  { code: 'VAR', label: '1-877831-T-C', query: '1-877831-T-C' },
  { code: 'CLIN', label: 'ClinVar hit', query: '1-878314-G-C' },
]

const examples = [
  'MUC4',
  'TTN',
  'SAMD11',
  'DOC2B',
  '1-877831-T-C',
  '17-6115-G-C',
  '1-874778-906100',
]

const fallbackSummary = {
  variant_count: 150659,
  gene_count: null,
  sample_count: 20,
  clinvar_count: null,
  consequence_counts: [],
}

export default () => {
  const [summary, setSummary] = useState<CohortSummary>(fallbackSummary)

  useEffect(() => {
    fetch('/api/', {
      body: JSON.stringify({
        query: `
          query CohortSummary {
            cohort_summary {
              variant_count
              gene_count
              sample_count
              clinvar_count
              consequence_counts {
                id
                label
                count
              }
            }
          }
        `,
      }),
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    })
      .then((response) => response.json())
      .then((response) => {
        if (response.data?.cohort_summary) {
          setSummary(response.data.cohort_summary)
        }
      })
      .catch(() => {})
  }, [])

  const stats = useMemo(
    () => [
      { label: 'Variants', value: formatNumber(summary.variant_count) },
      { label: 'Genes', value: formatNumber(summary.gene_count) },
      { label: 'Samples', value: formatNumber(summary.sample_count) },
      { label: 'ClinVar annotated', value: formatNumber(summary.clinvar_count) },
    ],
    [summary]
  )

  const maxConsequenceCount = Math.max(
    ...summary.consequence_counts.map((bucket) => bucket.count),
    1
  )

  return (
    <Page>
      <DocumentTitle title="Clinical Variant Browser" />
      <Hero>
        <HeroMark title="Tartu Ülikooli Kliinikum" />
        <Eyebrow>Tartu Ülikooli Kliinikum</Eyebrow>
        <Heading>Clinical Variant Browser</Heading>
        <Lead>
          Population-level variant frequencies from the local rare disease sequencing cohort.
        </Lead>

        <SearchPanel>
          <Searchbox
            id="home-search"
            placeholder="Search by gene, variant, rsID or region"
            width="100%"
          />
        </SearchPanel>
        <Hint>
          <span>Try</span>
          {examples.slice(0, 3).map((query) => (
            <HintLink key={query} preserveSelectedDataset={false} to={searchTo(query)}>
              {query}
            </HintLink>
          ))}
        </Hint>

        <QuickLinks aria-label="Quick search types">
          {quickLinks.map((item) => (
            <QuickLink key={item.label} preserveSelectedDataset={false} to={searchTo(item.query)}>
              <QuickLinkCode>{item.code}</QuickLinkCode>
              <QuickLinkLabel>{item.label}</QuickLinkLabel>
            </QuickLink>
          ))}
        </QuickLinks>

        <Divider>Example searches</Divider>
        <ExamplePills>
          {examples.map((query) => (
            <ExamplePill key={query} preserveSelectedDataset={false} to={searchTo(query)}>
              {query}
            </ExamplePill>
          ))}
        </ExamplePills>
      </Hero>

      <SummaryPanel>
        <Stats aria-label="Cohort summary">
          {stats.map((stat) => (
            <Stat key={stat.label}>
              <StatValue>{stat.value}</StatValue>
              <StatLabel>{stat.label}</StatLabel>
            </Stat>
          ))}
        </Stats>

        <Chart aria-label="Top variant consequences">
          <ChartTitle>Top Consequences</ChartTitle>
          {summary.consequence_counts.map((bucket, index) => (
            <BarRow key={bucket.id}>
              <span>{bucket.label}</span>
              <BarTrack>
                <BarFill
                  $tone={index === 1 ? 'pink' : index === 2 ? 'navy' : 'teal'}
                  $width={(bucket.count / maxConsequenceCount) * 100}
                />
              </BarTrack>
              <strong>{formatNumber(bucket.count)}</strong>
            </BarRow>
          ))}
          {summary.consequence_counts.length === 0 && (
            <BarRow>
              <span>Loading</span>
              <BarTrack>
                <BarFill $tone="teal" $width={40} />
              </BarTrack>
              <strong>—</strong>
            </BarRow>
          )}
        </Chart>
      </SummaryPanel>
    </Page>
  )
}
