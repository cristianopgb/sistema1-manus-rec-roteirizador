import { useState, useEffect } from 'react'
import {
  BarChart3, TrendingUp, Package, Truck, MapPin,
  Clock, CheckCircle, AlertTriangle, Building2,
  RefreshCw, Loader2, Calendar
} from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import { roteirizacaoService } from '@/services/roteirizacao.service'
import { RodadaRoteirizacao } from '@/types'
import toast from 'react-hot-toast'

interface KpiCard {
  label: string
  value: string | number
  sub?: string
  icon: React.ElementType
  color: string
  bg: string
}

export function DashboardPage() {
  const { user, filialAtiva, isMaster } = useAuth()
  const [rodadas, setRodadas] = useState<RodadaRoteirizacao[]>([])
  const [loading, setLoading] = useState(true)
  const [periodo, setPeriodo] = useState<'7d' | '30d' | '90d'>('30d')

  const carregar = async () => {
    setLoading(true)
    try {
      const data = await roteirizacaoService.listarRodadas(
        isMaster ? undefined : filialAtiva?.id
      )
      setRodadas(data)
    } catch {
      toast.error('Erro ao carregar dados do dashboard')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { carregar() }, [filialAtiva])

  // Filtrar por período
  const diasAtras = periodo === '7d' ? 7 : periodo === '30d' ? 30 : 90
  const dataCorte = new Date()
  dataCorte.setDate(dataCorte.getDate() - diasAtras)

  const rodadasFiltradas = rodadas.filter(
    (r) => new Date(r.created_at) >= dataCorte
  )

  // KPIs agregados
  const totalRodadas = rodadasFiltradas.length
  const totalManifestos = rodadasFiltradas.reduce((s, r) => s + (r.total_manifestos || 0), 0)
  const totalItens = rodadasFiltradas.reduce((s, r) => s + (r.total_itens_manifestados || 0), 0)
  const totalNaoRoteirizados = rodadasFiltradas.reduce((s, r) => s + (r.total_nao_roteirizados || 0), 0)
  const kmTotal = rodadasFiltradas.reduce((s, r) => s + (r.km_total_frota || 0), 0)
  const kmMedioPorManifesto = totalManifestos > 0 ? kmTotal / totalManifestos : 0
  const ocupacaoMedia = rodadasFiltradas.length > 0
    ? rodadasFiltradas.reduce((s, r) => s + (r.ocupacao_media_percentual || 0), 0) / rodadasFiltradas.length
    : 0
  const tempoMedio = rodadasFiltradas.length > 0
    ? rodadasFiltradas.reduce((s, r) => s + (r.tempo_processamento_ms || 0), 0) / rodadasFiltradas.length / 1000
    : 0

  // Taxa de roteirização
  const totalEntradas = rodadasFiltradas.reduce((s, r) => s + (r.total_cargas_entrada || 0), 0)
  const taxaRoteirizacao = totalEntradas > 0 ? (totalItens / totalEntradas) * 100 : 0

  const kpis: KpiCard[] = [
    {
      label: 'Rodadas no Período',
      value: totalRodadas,
      sub: `últimos ${diasAtras} dias`,
      icon: RefreshCw,
      color: 'text-brand-700',
      bg: 'bg-brand-50',
    },
    {
      label: 'Manifestos Gerados',
      value: totalManifestos.toLocaleString('pt-BR'),
      sub: `${totalItens.toLocaleString('pt-BR')} entregas`,
      icon: Package,
      color: 'text-green-700',
      bg: 'bg-green-50',
    },
    {
      label: 'Taxa de Roteirização',
      value: `${taxaRoteirizacao.toFixed(1)}%`,
      sub: `${totalNaoRoteirizados.toLocaleString('pt-BR')} não roteirizados`,
      icon: TrendingUp,
      color: 'text-blue-700',
      bg: 'bg-blue-50',
    },
    {
      label: 'Ocupação Média',
      value: `${ocupacaoMedia.toFixed(1)}%`,
      sub: 'dos veículos utilizados',
      icon: BarChart3,
      color: ocupacaoMedia >= 80 ? 'text-green-700' : 'text-amber-700',
      bg: ocupacaoMedia >= 80 ? 'bg-green-50' : 'bg-amber-50',
    },
    {
      label: 'KM Médio por Manifesto',
      value: `${kmMedioPorManifesto.toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} km`,
      sub: 'média consolidada',
      icon: MapPin,
      color: 'text-purple-700',
      bg: 'bg-purple-50',
    },
    {
      label: 'Tempo Médio Motor',
      value: `${tempoMedio.toFixed(1)}s`,
      sub: 'por rodada',
      icon: Clock,
      color: 'text-gray-700',
      bg: 'bg-gray-50',
    },
  ]

  // Agrupar por filial (para master)
  const porFilial = isMaster
    ? rodadasFiltradas.reduce<Record<string, { nome: string; rodadas: number; manifestos: number; itens: number; ocupacao: number[] }>>((acc, r) => {
        const id = r.filial_id
        if (!acc[id]) acc[id] = { nome: r.filial_nome || id, rodadas: 0, manifestos: 0, itens: 0, ocupacao: [] }
        acc[id].rodadas++
        acc[id].manifestos += r.total_manifestos || 0
        acc[id].itens += r.total_itens_manifestados || 0
        if (r.ocupacao_media_percentual) acc[id].ocupacao.push(r.ocupacao_media_percentual)
        return acc
      }, {})
    : {}

  return (
    <div className="fade-in">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div>
          <h1>Dashboard</h1>
          <p className="text-gray-500 text-sm">
            {isMaster ? 'Visão consolidada de todas as filiais' : `Filial: ${filialAtiva?.nome}`}
          </p>
        </div>
        <div className="flex items-center gap-3">
          {/* Seletor de período */}
          <div className="flex gap-1 bg-gray-100 rounded-xl p-1">
            {(['7d', '30d', '90d'] as const).map((p) => (
              <button
                key={p}
                onClick={() => setPeriodo(p)}
                className={`px-3 py-1.5 rounded-lg text-sm font-medium transition-all ${
                  periodo === p ? 'bg-white shadow text-brand-700' : 'text-gray-600'
                }`}
              >
                {p === '7d' ? '7 dias' : p === '30d' ? '30 dias' : '90 dias'}
              </button>
            ))}
          </div>
          <button className="btn-ghost btn-sm" onClick={carregar} disabled={loading}>
            <RefreshCw size={14} className={loading ? 'animate-spin' : ''} />
          </button>
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-24">
          <Loader2 size={32} className="animate-spin text-brand-600" />
        </div>
      ) : (
        <>
          {/* KPIs */}
          <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4 mb-8">
            {kpis.map(({ label, value, sub, icon: Icon, color, bg }) => (
              <div key={label} className="card p-4">
                <div className={`w-9 h-9 ${bg} rounded-xl flex items-center justify-center mb-3`}>
                  <Icon size={18} className={color} />
                </div>
                <div className="text-xl font-bold text-gray-900">{value}</div>
                <div className="text-xs font-medium text-gray-700 mt-0.5">{label}</div>
                {sub && <div className="text-xs text-gray-400 mt-0.5">{sub}</div>}
              </div>
            ))}
          </div>

          {/* Tabela por filial (apenas Master) */}
          {isMaster && Object.keys(porFilial).length > 0 && (
            <div className="card mb-8">
              <div className="card-header">
                <div className="flex items-center gap-2">
                  <Building2 size={16} className="text-gray-600" />
                  <h3>Desempenho por Filial</h3>
                </div>
              </div>
              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Filial</th>
                      <th className="text-center align-middle">Rodadas</th>
                      <th className="text-center align-middle">Manifestos</th>
                      <th className="text-center align-middle">Entregas</th>
                      <th className="text-center align-middle">Ocup. Média</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.values(porFilial)
                      .sort((a, b) => b.rodadas - a.rodadas)
                      .map((filial) => {
                        const ocup = filial.ocupacao.length > 0
                          ? filial.ocupacao.reduce((s, v) => s + v, 0) / filial.ocupacao.length
                          : 0
                        return (
                          <tr key={filial.nome}>
                            <td className="font-medium">{filial.nome}</td>
                            <td className="text-center align-middle">{filial.rodadas}</td>
                            <td className="text-center align-middle">{filial.manifestos.toLocaleString('pt-BR')}</td>
                            <td className="text-center align-middle">{filial.itens.toLocaleString('pt-BR')}</td>
                            <td className="text-center align-middle">
                              <span className={`font-semibold ${ocup >= 80 ? 'text-green-600' : 'text-amber-600'}`}>
                                {ocup.toFixed(1)}%
                              </span>
                            </td>
                          </tr>
                        )
                      })}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Histórico de rodadas */}
          <div className="card">
            <div className="card-header">
              <div className="flex items-center gap-2">
                <Calendar size={16} className="text-gray-600" />
                <h3>Histórico de Rodadas</h3>
              </div>
              <span className="text-sm text-gray-500">{rodadasFiltradas.length} rodada(s)</span>
            </div>

            {rodadasFiltradas.length === 0 ? (
              <div className="text-center py-12 text-gray-400">
                <BarChart3 size={40} className="mx-auto mb-3 opacity-30" />
                <p>Nenhuma rodada no período selecionado</p>
              </div>
            ) : (
              <div className="table-container">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Data / Hora</th>
                      {isMaster && <th>Filial</th>}
                      <th>Usuário</th>
                      <th>Tipo</th>
                      <th className="text-right">Entrada</th>
                      <th className="text-right">Manifestos</th>
                      <th className="text-right">Entregas</th>
                      <th className="text-right">Não Rot.</th>
                      <th className="text-right">KM Médio/Manifesto</th>
                      <th className="text-right">Ocupação</th>
                      <th className="text-right">Tempo</th>
                      <th>Status</th>
                    </tr>
                  </thead>
                  <tbody>
                    {rodadasFiltradas.map((r) => (
                      <tr key={r.id}>
                        <td className="whitespace-nowrap text-sm">
                          {new Date(r.created_at).toLocaleString('pt-BR', {
                            day: '2-digit', month: '2-digit', year: '2-digit',
                            hour: '2-digit', minute: '2-digit'
                          })}
                        </td>
                        {isMaster && <td className="text-gray-600">{r.filial_nome}</td>}
                        <td className="text-gray-600">{r.usuario_nome || '—'}</td>
                        <td>
                          <span className="badge-gray text-xs">{r.tipo_roteirizacao || 'carteira'}</span>
                        </td>
                        <td className="text-right font-mono">{r.total_cargas_entrada?.toLocaleString('pt-BR')}</td>
                        <td className="text-right font-mono font-semibold text-brand-700">
                          {r.total_manifestos?.toLocaleString('pt-BR')}
                        </td>
                        <td className="text-right font-mono">{r.total_itens_manifestados?.toLocaleString('pt-BR')}</td>
                        <td className="text-right font-mono text-amber-600">
                          {r.total_nao_roteirizados?.toLocaleString('pt-BR')}
                        </td>
                        <td className="text-right font-mono text-sm">
                          {((r.total_manifestos || 0) > 0 ? (r.km_total_frota || 0) / r.total_manifestos : 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} km
                        </td>
                        <td className="text-right">
                          <span className={`font-semibold ${
                            (r.ocupacao_media_percentual || 0) >= 80 ? 'text-green-600' : 'text-amber-600'
                          }`}>
                            {r.ocupacao_media_percentual?.toFixed(1)}%
                          </span>
                        </td>
                        <td className="text-right text-sm text-gray-500">
                          {((r.tempo_processamento_ms || 0) / 1000).toFixed(1)}s
                        </td>
                        <td>
                          <span className={r.status === 'sucesso' ? 'badge-green' : 'badge-red'}>
                            {r.status}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}
    </div>
  )
}
