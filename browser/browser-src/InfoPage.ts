import styled from 'styled-components'

import { Page } from '@gnomad/ui'

export default styled(Page)`
  max-width: 1200px;
  padding: 0 24px;
  color: var(--text);
  font-size: 16px;

  p {
    margin-bottom: 1em;
    line-height: 1.5;
  }

  h1,
  h2,
  h3 {
    color: var(--navy);
    letter-spacing: 0;
  }

  h1 {
    font-size: 32px;
    line-height: 1.2;
  }

  h2 {
    font-size: 22px;
  }
`
