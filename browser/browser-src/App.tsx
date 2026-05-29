import React, { Suspense, lazy, useEffect, useState } from 'react'
import { BrowserRouter as Router, Route, useLocation } from 'react-router-dom'
import styled, { createGlobalStyle } from 'styled-components'

import Delayed from './Delayed'
import ErrorBoundary from './ErrorBoundary'

import Notifications, { showNotification } from './Notifications'
import StatusMessage from './StatusMessage'
import userPreferences from './userPreferences'

const NavBar = lazy(() => import('./NavBar'))
const Routes = lazy(() => import('./Routes'))

const GlobalStyle = createGlobalStyle`
  :root {
    --navy: #001A72;
    --navy-80: #33488E;
    --navy-60: #6676AA;
    --navy-20: #CCD1E3;
    --teal: #00CFB4;
    --teal-dark: #006659;
    --teal-20: #CCF5F0;
    --pink: #EF426F;
    --pink-dark: #B71C45;
    --pink-20: #FCD9E2;
    --white: #FFFFFF;
    --gray-50: #F7F8FB;
    --gray-100: #E8EBF2;
    --text: #001A72;
    --text-secondary: #444;
    --border: #E0E3EE;
    --shadow-sm: 0 1px 3px rgba(0, 26, 114, 0.07);
    --shadow-md: 0 8px 24px rgba(0, 26, 114, 0.10);
  }

  html,
  body {
    width: 100%;
    min-height: 100%;
    background: var(--white);
    color: var(--text);
    font-family: Inter, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    font-size: 16px;
    line-height: 1.5;
  }

  * {
    box-sizing: border-box;
  }

  button,
  input,
  select,
  textarea {
    font-family: inherit;
  }

  a {
    color: var(--teal-dark);
  }

  a:hover {
    color: var(--navy);
  }

  h1,
  h2,
  h3,
  h4 {
    color: var(--navy);
    letter-spacing: 0;
  }

  select,
  input[type='text'],
  input[type='search'],
  input:not([type]) {
    border-color: var(--border);
    border-radius: 4px;
    color: var(--text);
  }

  select:focus,
  input[type='text']:focus,
  input[type='search']:focus,
  input:not([type]):focus {
    border-color: var(--teal) !important;
    outline: none;
    box-shadow: 0 0 0 3px rgba(0, 207, 180, 0.18) !important;
  }

  button:focus {
    outline: none;
    box-shadow: 0 0 0 3px rgba(0, 207, 180, 0.18);
  }

  [role='grid'] {
    overflow: hidden;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: var(--white);
    color: #1A2140;
    font-size: 13px;
  }

  [role='grid'] [role='row'] {
    border-top-color: var(--border) !important;
  }

  [role='grid'] [role='row']:first-child {
    border-top: none !important;
    background: var(--navy);
    color: var(--white);
  }

  [role='grid'] [role='columnheader'] {
    border-top: none !important;
    background-color: var(--navy) !important;
    background-image: none !important;
    color: var(--white);
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  [role='grid'] [role='columnheader'][aria-sort='ascending'],
  [role='grid'] [role='columnheader'][aria-sort='descending'] {
    background-color: var(--navy-80) !important;
  }

  [role='grid'] [role='columnheader'][aria-sort='ascending'] button::after {
    color: var(--teal);
    content: ' ↑';
  }

  [role='grid'] [role='columnheader'][aria-sort='descending'] button::after {
    color: var(--teal);
    content: ' ↓';
  }

  [role='grid'] .grid-row-stripe {
    background: var(--gray-50) !important;
  }

  [role='grid'] .grid-row:hover {
    background: var(--teal-20) !important;
  }

  [role='grid'] .grid-row-highlight {
    box-shadow: inset 0 0 0 2px var(--teal) !important;
  }

  table {
    color: #1A2140;
  }

  table thead th,
  table thead td {
    border-bottom-color: var(--navy) !important;
    color: var(--navy);
    font-weight: 700;
  }

  table tbody tr:nth-child(even) {
    background: var(--gray-50);
  }

  table tbody td,
  table tbody th {
    border-bottom-color: var(--border) !important;
  }
`

const scrollToAnchorOrStartOfPage = (location: any) => {
  if (location.hash) {
    setTimeout(() => {
      const anchor = document.querySelector(`a${location.hash}`)
      if (anchor) {
        anchor.scrollIntoView()
      } else {
        window.scrollTo(0, 0)
      }
    }, 0)
  } else {
    window.scrollTo(0, 0)
  }
}

const PageLoading = () => {
  const location = useLocation()
  useEffect(() => () => {
    scrollToAnchorOrStartOfPage(location)
  })
  return null
}

const GoogleAnalytics = () => {
  const location = useLocation()
  useEffect(() => {
    if ((window as any).gtag) {
      ;(window as any).gtag('config', (window as any).gaTrackingId, {
        page_path: location.pathname,
      })
    }
  }, [location.pathname])
  return null
}

const TopBarWrapper = styled.div`
  box-shadow: var(--shadow-md);

  @media print {
    display: none;
  }
`

const App = () => {
  const [isLoading, setIsLoading] = useState(true)
  useEffect(() => {
    userPreferences.loadPreferences().then(
      () => {
        setIsLoading(false)
      },
      (error: any) => {
        setIsLoading(false)
        showNotification({
          title: 'Error',
          message: error.message,
          status: 'error',
        })
      }
    )
  }, [])

  return (
    <Router>
      <GlobalStyle />
      <Route path="/" component={GoogleAnalytics} />

      <Route
        path="/"
        render={({ location }: any) => {
          scrollToAnchorOrStartOfPage(location)
          return null
        }}
      />

      <ErrorBoundary>
        {isLoading ? (
          <Delayed>
            <StatusMessage>Loading</StatusMessage>
          </Delayed>
        ) : (
          <Suspense fallback={null}>
            <TopBarWrapper>
              <NavBar />
            </TopBarWrapper>
            <Notifications />

            <Suspense fallback={<PageLoading />}>
              <Routes />
            </Suspense>
          </Suspense>
        )}
      </ErrorBoundary>
    </Router>
  )
}

export default App
