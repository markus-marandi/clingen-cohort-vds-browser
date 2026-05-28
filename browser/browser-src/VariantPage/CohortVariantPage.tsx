import React from 'react'
import styled from 'styled-components'
import { Page, PageHeading } from '@gnomad/ui'
import { DatasetId } from '@gnomad/dataset-metadata/metadata'
import DocumentTitle from '../DocumentTitle'
import Query from '../Query'
import StatusMessage from '../StatusMessage'

const COHORT_VARIANT_QUERY = `
  query CohortVariant($variantId: String!, $datasetId: DatasetId!) {
    variant(variantId: $variantId, dataset: $datasetId) {
      variant_id
      chrom
      pos
      ref
      alt
      rsids
      gene_symbol
      consequence
      impact
      cdna
      p_nomen
      cadd_score
      clinvar_sig
      gnomad_af
      gnomad_nonfin
      exome {
        ac
        an
        af
        homozygote_count
        filters
      }
    }
  }
`

const Table = styled.table`
  border-collapse: collapse;
  font-size: 14px;
  margin-top: 1rem;

  th, td {
    border: 1px solid #ddd;
    padding: 6px 12px;
    text-align: left;
  }

  th {
    background: #f5f5f5;
    font-weight: 600;
  }

  tr:nth-child(even) td {
    background: #fafafa;
  }
`

const Section = styled.div`
  margin-top: 2rem;
`

const SectionHeading = styled.h2`
  font-size: 18px;
  font-weight: 600;
  margin-bottom: 0.5rem;
  border-bottom: 2px solid #428bca;
  padding-bottom: 4px;
`

const Row = ({ label, value }: { label: string; value: any }) => {
  if (value === null || value === undefined || value === '') return null
  return (
    <tr>
      <th>{label}</th>
      <td>{String(value)}</td>
    </tr>
  )
}

const fmt = (v: number | null | undefined, decimals = 4) =>
  v == null ? '—' : v.toFixed(decimals)

type Props = {
  datasetId: DatasetId
  variantId: string
}

const CohortVariantPage = ({ datasetId, variantId }: Props) => (
  <Page>
    <DocumentTitle title={`${variantId} | Tartu University Hospital`} />
    <PageHeading>
      SNV: {variantId}
    </PageHeading>

    <Query
      query={COHORT_VARIANT_QUERY}
      variables={{ variantId, datasetId }}
    >
      {({ data, error, graphQLErrors }: any) => {
        if (error) return <StatusMessage>Unable to load variant ({error.message})</StatusMessage>
        if (graphQLErrors?.length) {
          return <StatusMessage>{graphQLErrors[0].message}</StatusMessage>
        }
        const v = data?.variant
        if (!v) return <StatusMessage>Variant not found in cohort</StatusMessage>

        const exome = v.exome ?? {}
        const af = exome.af ?? (exome.ac && exome.an ? exome.ac / exome.an : null)

        return (
          <>
            <Section>
              <SectionHeading>Variant</SectionHeading>
              <Table>
                <tbody>
                  <Row label="Chromosome" value={v.chrom} />
                  <Row label="Position" value={v.pos} />
                  <Row label="Reference" value={v.ref} />
                  <Row label="Alternate" value={v.alt} />
                  {v.rsids?.length > 0 && <Row label="rsID" value={v.rsids.join(', ')} />}
                </tbody>
              </Table>
            </Section>

            <Section>
              <SectionHeading>Tartu University Hospital Frequency</SectionHeading>
              <Table>
                <tbody>
                  <Row label="Allele Count" value={exome.ac} />
                  <Row label="Allele Number" value={exome.an} />
                  <Row label="Allele Frequency" value={af != null ? fmt(af, 6) : null} />
                  <Row label="Homozygote Count" value={exome.homozygote_count} />
                  {exome.filters?.length > 0 && (
                    <Row label="Filters" value={exome.filters.join(', ')} />
                  )}
                </tbody>
              </Table>
            </Section>

            {(v.gene_symbol || v.consequence) && (
              <Section>
                <SectionHeading>Annotation</SectionHeading>
                <Table>
                  <tbody>
                    <Row label="Gene" value={v.gene_symbol} />
                    <Row label="Consequence" value={v.consequence} />
                    <Row label="Impact" value={v.impact} />
                    <Row label="cDNA" value={v.cdna} />
                    <Row label="Protein" value={v.p_nomen} />
                    <Row label="CADD Phred" value={v.cadd_score != null ? fmt(v.cadd_score, 2) : null} />
                    <Row label="ClinVar Significance" value={v.clinvar_sig} />
                  </tbody>
                </Table>
              </Section>
            )}

            {(v.gnomad_af != null || v.gnomad_nonfin != null) && (
              <Section>
                <SectionHeading>Population Frequency</SectionHeading>
                <Table>
                  <tbody>
                    <Row label="gnomAD AF (all)" value={v.gnomad_af != null ? fmt(v.gnomad_af, 6) : null} />
                    <Row label="gnomAD AF (non-Finnish)" value={v.gnomad_nonfin != null ? fmt(v.gnomad_nonfin, 6) : null} />
                  </tbody>
                </Table>
              </Section>
            )}
          </>
        )
      }}
    </Query>
  </Page>
)

export default CohortVariantPage
