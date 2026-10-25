import React, { useCallback, useState } from 'react'
import { Link, useLocation } from 'react-router-dom'
import styled from 'styled-components'

import BrandMark from './Brand'
import Searchbox from './Searchbox'

const NAV_LINKS = [
  { label: 'Filter', to: '/filter' },
  { label: 'About', to: '/about' },
  { label: 'Data', to: '/data' },
  { label: 'Publications', to: '/publications' },
  { label: 'Stats', to: '/stats' },
  { label: 'Help', to: '/help' },
]

const Wrapper = styled.header`
  position: relative;
  display: flex;
  gap: 22px;
  align-items: center;
  box-sizing: border-box;
  width: 100%;
  min-height: 66px;
  padding: 0 32px;
  border-bottom: 2px solid rgba(0, 207, 180, 0.42);
  background: var(--navy);
  color: var(--white);

  a {
    color: inherit;
    text-decoration: none;
  }

  @media (max-width: 1050px) {
    flex-wrap: wrap;
    gap: 14px;
    padding: 12px 20px;
  }
`

const LogoLink = styled(Link)`
  display: inline-flex;
  flex: 0 1 auto;
  align-items: center;
  min-width: 245px;
  gap: 12px;

  @media (max-width: 1050px) {
    flex: 1 1 auto;
    min-width: 0;
  }
`

const LogoMark = styled(BrandMark)`
  flex: 0 0 auto;
  width: 46px;
  height: 46px;
  padding: 3px;
  border-radius: 50%;
  background: var(--white);
  object-fit: contain;
`

const LogoText = styled.span`
  display: flex;
  flex-direction: column;
  min-width: 0;
  line-height: 1.16;
`

const Organization = styled.span`
  overflow: hidden;
  color: var(--white);
  font-size: 14px;
  font-weight: 700;
  text-overflow: ellipsis;
  white-space: nowrap;
`

const Product = styled.span`
  color: var(--teal);
  font-size: 10px;
  font-weight: 700;
  letter-spacing: 0.08em;
  text-transform: uppercase;
`

const NavSearch = styled.div`
  flex: 0 1 auto;
  min-width: 280px;
  max-width: 520px;

  @media (max-width: 1050px) {
    flex: 1 1 100%;
    width: 100%;
    min-width: 0;
    max-width: none;
  }
`

const ToggleMenuButton = styled.button`
  display: none;
  width: 40px;
  height: 36px;
  border: 1px solid rgba(255, 255, 255, 0.32);
  border-radius: 4px;
  background: rgba(255, 255, 255, 0.08);
  color: var(--white);
  cursor: pointer;
  font: inherit;
  font-size: 20px;
  line-height: 1;

  &:focus {
    outline: none;
    box-shadow: 0 0 0 3px rgba(0, 207, 180, 0.22);
  }

  @media (max-width: 1050px) {
    display: inline-flex;
    justify-content: center;
    align-items: center;
  }
`

const Menu = styled.ul<{ $isExpanded: boolean }>`
  display: flex;
  justify-content: flex-end;
  align-items: stretch;
  height: 100%;
  padding: 0;
  margin: 0;
  list-style: none;

  @media (max-width: 1050px) {
    display: ${(props) => (props.$isExpanded ? 'flex' : 'none')};
    flex: 1 1 100%;
    flex-wrap: wrap;
    gap: 6px;
    width: 100%;
    height: auto;

    li {
      flex: 1 1 calc(16.66% - 5px);
    }
  }

  @media (max-width: 680px) {
    li {
      flex: 1 1 calc(50% - 3px);
    }
  }
`

const NavLink = styled(Link)<{ $isActive: boolean }>`
  display: flex;
  align-items: center;
  height: 100%;
  padding: 0 12px;
  border-bottom: 2px solid ${(props) => (props.$isActive ? 'var(--teal)' : 'transparent')};
  color: ${(props) => (props.$isActive ? 'var(--teal)' : 'rgba(255, 255, 255, 0.76)')};
  font-size: 13px;
  font-weight: 600;
  transition: background 0.12s ease, color 0.12s ease, border-color 0.12s ease;

  &:hover {
    background: rgba(255, 255, 255, 0.08);
    color: var(--white);
  }

  @media (max-width: 1050px) {
    justify-content: center;
    min-height: 38px;
    padding: 8px;
    border: 1px solid rgba(255, 255, 255, 0.12);
    border-bottom: 2px solid
      ${(props) => (props.$isActive ? 'var(--teal)' : 'rgba(255, 255, 255, 0.12)')};
    border-radius: 4px;
    background: ${(props) =>
      props.$isActive ? 'rgba(0, 207, 180, 0.1)' : 'rgba(255, 255, 255, 0.05)'};
  }
`

const NavBar = () => {
  const location = useLocation()
  const [isMenuExpanded, setIsMenuExpanded] = useState(false)
  const toggleMenu = useCallback(() => {
    setIsMenuExpanded((previousValue) => !previousValue)
  }, [])
  const closeMenu = useCallback(() => {
    setIsMenuExpanded(false)
  }, [])

  return (
    <Wrapper>
      <LogoLink to="/" onClick={closeMenu}>
        <LogoMark title="Tartu Ülikooli Kliinikum" variant="nav" />
        <LogoText>
          <Organization>Tartu Ülikooli Kliinikum</Organization>
          <Product>Variant Browser</Product>
        </LogoText>
      </LogoLink>

      <NavSearch>
        <Searchbox id="navbar-search" placeholder="Gene, variant or region" width="100%" />
      </NavSearch>

      <ToggleMenuButton
        aria-expanded={isMenuExpanded}
        aria-label="Toggle navigation"
        onClick={toggleMenu}
      >
        ☰
      </ToggleMenuButton>

      <Menu $isExpanded={isMenuExpanded}>
        {NAV_LINKS.map(({ label, to }) => (
          <li key={to}>
            <NavLink
              $isActive={location.pathname === to || location.pathname.startsWith(`${to}/`)}
              to={to}
              onClick={closeMenu}
            >
              {label}
            </NavLink>
          </li>
        ))}
      </Menu>
    </Wrapper>
  )
}

export default NavBar
