import { HashRouter, Routes, Route, Navigate } from 'react-router-dom'
import { Toaster } from 'react-hot-toast'
import { AuthProvider, useAuth } from '@/contexts/AuthContext'
import { AppLayout } from '@/components/layout/AppLayout'
import { ErrorBoundary } from '@/components/ErrorBoundary'

// Pages
import { LoginPage } from '@/pages/LoginPage'
import { DashboardPage } from '@/pages/DashboardPage'
import { RoteirizacaoPage } from '@/pages/RoteirizacaoPage'
import { FilialPage } from '@/pages/cadastros/FilialPage'
import { VeiculoPage } from '@/pages/cadastros/VeiculoPage'
import { UsuarioPage } from '@/pages/cadastros/UsuarioPage'
import { TabelaAnttPage } from '@/pages/cadastros/TabelaAnttPage'
import { HistoricoPage } from '@/pages/HistoricoPage'

function ProtectedRoute({ children, masterOnly = false }: { children: React.ReactNode; masterOnly?: boolean }) {
  const { user, profile, loading } = useAuth()

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center">
          <div className="w-12 h-12 border-4 border-brand-600 border-t-transparent rounded-full animate-spin mx-auto mb-4" />
          <p className="text-gray-500 text-sm">Carregando...</p>
        </div>
      </div>
    )
  }

  if (!user || !profile) {
    return <Navigate to="/login" replace />
  }

  if (!profile.ativo) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center max-w-md">
          <div className="w-16 h-16 bg-red-100 rounded-full flex items-center justify-center mx-auto mb-4">
            <span className="text-red-600 text-2xl">⚠</span>
          </div>
          <h2 className="text-xl font-semibold text-gray-900 mb-2">Conta Desativada</h2>
          <p className="text-gray-500">Sua conta está desativada. Entre em contato com o administrador.</p>
        </div>
      </div>
    )
  }

  if (masterOnly && profile.perfil !== 'master') {
    return <Navigate to="/dashboard" replace />
  }

  return <>{children}</>
}

function AppRoutes() {
  const { user, loading } = useAuth()

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="w-12 h-12 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" />
      </div>
    )
  }

  return (
    <Routes>
      <Route path="/login" element={user ? <Navigate to="/dashboard" replace /> : <LoginPage />} />

      <Route
        path="/"
        element={
          <ProtectedRoute>
            <AppLayout />
          </ProtectedRoute>
        }
      >
        <Route index element={<Navigate to="/dashboard" replace />} />

        <Route path="dashboard" element={<DashboardPage />} />
        <Route path="roteirizacao" element={<RoteirizacaoPage />} />
        <Route path="historico" element={<HistoricoPage />} />

        {/* Rotas exclusivas do Master */}
        <Route path="cadastros">
          <Route path="filiais" element={<ProtectedRoute masterOnly><FilialPage /></ProtectedRoute>} />
          <Route path="veiculos" element={<ProtectedRoute masterOnly><VeiculoPage /></ProtectedRoute>} />
          <Route path="usuarios" element={<ProtectedRoute masterOnly><UsuarioPage /></ProtectedRoute>} />
          <Route path="tabela-antt" element={<ProtectedRoute masterOnly><TabelaAnttPage /></ProtectedRoute>} />
        </Route>
      </Route>

      <Route path="*" element={<Navigate to="/dashboard" replace />} />
    </Routes>
  )
}

export default function App() {
  return (
    <HashRouter>
      <ErrorBoundary>
        <AuthProvider>
          <AppRoutes />
          <Toaster
            position="top-right"
            toastOptions={{
              duration: 4000,
              style: {
                background: '#1e3a8a',
                color: '#fff',
                borderRadius: '8px',
                fontSize: '14px',
              },
              success: {
                style: { background: '#10b981' },
                iconTheme: { primary: '#fff', secondary: '#10b981' },
              },
              error: {
                style: { background: '#ef4444' },
                iconTheme: { primary: '#fff', secondary: '#ef4444' },
              },
            }}
          />
        </AuthProvider>
      </ErrorBoundary>
    </HashRouter>
  )
}
