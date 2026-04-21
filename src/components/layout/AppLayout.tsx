import { Outlet, NavLink, useNavigate } from 'react-router-dom'
import { useState } from 'react'
import {
  LayoutDashboard, Route, Building2, Truck, Users,
  FileSpreadsheet, History, LogOut, Menu, X, ChevronDown,
  ChevronRight, Settings
} from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'

interface NavItem {
  label: string
  to: string
  icon: React.ReactNode
  masterOnly?: boolean
}

interface NavGroup {
  label: string
  icon: React.ReactNode
  masterOnly?: boolean
  items: NavItem[]
}

const navItems: (NavItem | NavGroup)[] = [
  {
    label: 'Dashboard',
    to: '/dashboard',
    icon: <LayoutDashboard size={18} />,
  },
  {
    label: 'Roteirização',
    to: '/roteirizacao',
    icon: <Route size={18} />,
  },
  {
    label: 'Aprovar Roteirização',
    to: '/historico',
    icon: <History size={18} />,
  },
  {
    label: 'Cadastros',
    icon: <Settings size={18} />,
    masterOnly: true,
    items: [
      { label: 'Filiais', to: '/cadastros/filiais', icon: <Building2 size={16} />, masterOnly: true },
      { label: 'Veículos', to: '/cadastros/veiculos', icon: <Truck size={16} />, masterOnly: true },
      { label: 'Usuários', to: '/cadastros/usuarios', icon: <Users size={16} />, masterOnly: true },
      { label: 'Tabela ANTT', to: '/cadastros/tabela-antt', icon: <FileSpreadsheet size={16} />, masterOnly: true },
    ],
  },
]

function isNavGroup(item: NavItem | NavGroup): item is NavGroup {
  return 'items' in item
}

export function AppLayout() {
  const { profile, isMaster, signOut } = useAuth()
  const navigate = useNavigate()
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [cadastrosOpen, setCadastrosOpen] = useState(false)

  const handleSignOut = async () => {
    await signOut()
    navigate('/login')
  }

  return (
    <div className="flex h-screen bg-gray-50 overflow-hidden">
      {/* Sidebar */}
      <aside
        className={`
          flex flex-col bg-brand-900 transition-all duration-300 flex-shrink-0
          ${sidebarOpen ? 'w-64' : 'w-16'}
        `}
      >
        {/* Logo */}
        <div className="flex items-center gap-3 px-4 py-5 border-b border-white/10">
          <div className="w-8 h-8 bg-brand-500 rounded-lg flex items-center justify-center flex-shrink-0">
            <Route size={18} className="text-white" />
          </div>
          {sidebarOpen && (
            <div className="overflow-hidden">
              <p className="text-white font-bold text-sm leading-none">REC</p>
              <p className="text-brand-300 text-xs mt-0.5">Roteirizador</p>
            </div>
          )}
          <button
            onClick={() => setSidebarOpen(!sidebarOpen)}
            className="ml-auto text-brand-300 hover:text-white transition-colors"
          >
            {sidebarOpen ? <X size={18} /> : <Menu size={18} />}
          </button>
        </div>

        {/* Perfil do usuário */}
        {sidebarOpen && (
          <div className="px-4 py-3 border-b border-white/10">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 bg-brand-600 rounded-full flex items-center justify-center flex-shrink-0">
                <span className="text-white text-xs font-bold">
                  {profile?.nome?.charAt(0).toUpperCase() || 'U'}
                </span>
              </div>
              <div className="overflow-hidden">
                <p className="text-white text-xs font-medium truncate">{profile?.nome}</p>
                <p className="text-brand-300 text-xs truncate">
                  {isMaster ? 'Master' : profile?.filial_nome || 'Roteirizador'}
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Navegação */}
        <nav className="flex-1 px-2 py-3 space-y-0.5 overflow-y-auto">
          {navItems.map((item, idx) => {
            if (isNavGroup(item)) {
              if (item.masterOnly && !isMaster) return null
              return (
                <div key={idx}>
                  <button
                    onClick={() => setCadastrosOpen(!cadastrosOpen)}
                    className={`
                      w-full flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium
                      transition-all duration-150 text-gray-300 hover:bg-white/10 hover:text-white
                    `}
                  >
                    <span className="flex-shrink-0">{item.icon}</span>
                    {sidebarOpen && (
                      <>
                        <span className="flex-1 text-left">{item.label}</span>
                        {cadastrosOpen ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                      </>
                    )}
                  </button>
                  {sidebarOpen && cadastrosOpen && (
                    <div className="ml-4 mt-0.5 space-y-0.5">
                      {item.items.map((subItem) => (
                        <NavLink
                          key={subItem.to}
                          to={subItem.to}
                          className={({ isActive }) =>
                            isActive ? 'sidebar-item-active' : 'sidebar-item-inactive'
                          }
                        >
                          <span className="flex-shrink-0">{subItem.icon}</span>
                          <span>{subItem.label}</span>
                        </NavLink>
                      ))}
                    </div>
                  )}
                </div>
              )
            }

            if (item.masterOnly && !isMaster) return null

            return (
              <NavLink
                key={item.to}
                to={item.to}
                className={({ isActive }) =>
                  isActive ? 'sidebar-item-active' : 'sidebar-item-inactive'
                }
                title={!sidebarOpen ? item.label : undefined}
              >
                <span className="flex-shrink-0">{item.icon}</span>
                {sidebarOpen && <span>{item.label}</span>}
              </NavLink>
            )
          })}
        </nav>

        {/* Logout */}
        <div className="px-2 py-3 border-t border-white/10">
          <button
            onClick={handleSignOut}
            className="sidebar-item-inactive w-full"
            title={!sidebarOpen ? 'Sair' : undefined}
          >
            <LogOut size={18} className="flex-shrink-0" />
            {sidebarOpen && <span>Sair</span>}
          </button>
        </div>
      </aside>

      {/* Conteúdo principal */}
      <div className="flex-1 flex flex-col overflow-hidden">
        {/* Header */}
        <header className="bg-white border-b border-gray-200 px-6 py-3 flex items-center justify-between flex-shrink-0">
          <div />
          <div className="flex items-center gap-3">
            <span className={`badge ${isMaster ? 'badge-blue' : 'badge-green'}`}>
              {isMaster ? 'Master' : 'Roteirizador'}
            </span>
            {!isMaster && profile?.filial_nome && (
              <span className="badge badge-gray">{profile.filial_nome}</span>
            )}
            <div className="w-8 h-8 bg-brand-700 rounded-full flex items-center justify-center">
              <span className="text-white text-xs font-bold">
                {profile?.nome?.charAt(0).toUpperCase() || 'U'}
              </span>
            </div>
          </div>
        </header>

        {/* Página */}
        <main className="flex-1 overflow-y-auto p-6">
          <Outlet />
        </main>
      </div>
    </div>
  )
}
