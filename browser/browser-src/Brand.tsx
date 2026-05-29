import React from 'react'

import TykLogo from './tyk-logo.png'

type BrandMarkProps = {
  className?: string
  title?: string
  variant?: 'color' | 'nav'
}

const BrandMark = ({ className, title }: BrandMarkProps) => (
  <img
    alt={title || ''}
    aria-hidden={title ? undefined : true}
    className={className}
    src={TykLogo}
  />
)

export default BrandMark
