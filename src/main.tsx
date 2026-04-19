import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.tsx'
import './index.css'

window.addEventListener('error', (event) => {
  console.error('[GlobalError]', {
    message: event.message,
    filename: event.filename,
    line: event.lineno,
    column: event.colno,
    error: event.error,
    timestamp: new Date().toISOString(),
    location: window.location.href,
  })
})

window.addEventListener('unhandledrejection', (event) => {
  console.error('[UnhandledRejection]', {
    reason: event.reason,
    timestamp: new Date().toISOString(),
    location: window.location.href,
  })
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>,
)
