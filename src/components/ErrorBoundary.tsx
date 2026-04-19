import React from 'react'

interface ErrorBoundaryState {
  hasError: boolean
  message?: string
}

export class ErrorBoundary extends React.Component<React.PropsWithChildren, ErrorBoundaryState> {
  constructor(props: React.PropsWithChildren) {
    super(props)
    this.state = { hasError: false }
  }

  static getDerivedStateFromError(error: Error): ErrorBoundaryState {
    return {
      hasError: true,
      message: error.message || 'Erro inesperado',
    }
  }

  componentDidCatch(error: Error, info: React.ErrorInfo): void {
    console.error('[ErrorBoundary] Falha de renderização capturada', {
      message: error.message,
      stack: error.stack,
      componentStack: info.componentStack,
      timestamp: new Date().toISOString(),
      location: window.location.href,
    })
  }

  handleReload = () => {
    window.location.reload()
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="min-h-screen flex items-center justify-center bg-gray-50 p-4">
          <div className="max-w-md w-full bg-white border border-gray-200 rounded-2xl shadow-sm p-6 text-center">
            <h1 className="text-xl font-semibold text-gray-900 mb-2">O app encontrou um erro</h1>
            <p className="text-sm text-gray-600 mb-1">
              Ocorreu uma falha ao renderizar a página após o login.
            </p>
            <p className="text-xs text-gray-500 mb-6 break-words">
              Detalhe: {this.state.message || 'erro desconhecido'}
            </p>
            <button className="btn-primary w-full justify-center" onClick={this.handleReload}>
              Recarregar aplicação
            </button>
          </div>
        </div>
      )
    }

    return this.props.children
  }
}

