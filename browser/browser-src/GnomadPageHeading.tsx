import React from 'react'
import styled from 'styled-components'

import DatasetSelector, { DatasetOptions } from './DatasetSelector'
import InfoButton from './help/InfoButton'

import { DatasetId } from '@gnomad/dataset-metadata/metadata'

const PageHeadingWrapper = styled.div`
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: flex-start;
  gap: 20px;
  padding: 28px 0 18px;
  border-bottom: 1px solid var(--border);
  margin: 0 0 22px;
  background: var(--white);

  @media (max-width: 1200px) {
    flex-direction: column;
    align-items: flex-start;
  }

  @media (max-width: 900px) {
    align-items: stretch;
  }
`

const PageHeadingInnerWrapper = styled.div`
  display: flex;
  flex-shrink: 1;
  flex-direction: row;
  align-items: center;
  overflow: hidden;
  max-width: 100%;
  padding-left: 14px;
  border-left: 4px solid var(--teal);

  @media (max-width: 900px) {
    flex-direction: column;
    align-items: flex-start;
  }
`

const CenterPanel = styled.div`
  flex-shrink: 0;

  @media (min-width: 901px) {
    margin-left: 14px;
  }

  @media (max-width: 900px) {
    margin-top: 8px;
  }
`

const PageHeadingText = styled.h1`
  overflow: hidden;
  max-width: 100%;
  margin: 0;
  color: var(--navy);
  font-size: 34px;
  font-weight: 700;
  letter-spacing: 0;
  line-height: 1.1;
  text-overflow: ellipsis;
  white-space: nowrap;
`

const PageControlsWrapper = styled.div`
  display: flex;
  flex-shrink: 0;
  align-items: center;
  gap: 8px;
  color: var(--text-secondary);
  font-size: 13px;
  font-weight: 600;

  @media (max-width: 900px) {
    width: 100%;
    justify-content: flex-start;
  }
`

type Props = {
  children: React.ReactNode
  extra?: React.ReactNode
  datasetOptions: DatasetOptions
  selectedDataset: DatasetId
}

const GnomadPageHeading = ({ children, extra, datasetOptions, selectedDataset }: Props) => (
  <PageHeadingWrapper>
    <PageHeadingInnerWrapper>
      <PageHeadingText>{children}</PageHeadingText>
      {extra && <CenterPanel>{extra}</CenterPanel>}
    </PageHeadingInnerWrapper>
    <PageControlsWrapper>
      <span>Dataset</span>
      <DatasetSelector datasetOptions={datasetOptions} selectedDataset={selectedDataset} />
      <span>
        <InfoButton topic="dataset-selection" />
      </span>
    </PageControlsWrapper>
  </PageHeadingWrapper>
)

GnomadPageHeading.defaultProps = {
  extra: undefined,
}

export default GnomadPageHeading
