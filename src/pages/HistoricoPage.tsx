import { useState, useEffect } from 'react'
import { supabase } from '@/lib/supabase'
import { useAuth } from '@/contexts/AuthContext'
import { RodadaRoteirizacao } from '@/types'
import { format } from 'date-fns'
import { ptBR } from 'date-fns/locale'

export function HistoricoPage() {
  const { isMaster, filialAtiva } = useAuth()
  const [rodadas, setRodadas] = useState<RodadaRoteirizacao[]>([])
  const [loading, setLoading] = useState(true)
  const [busca, setBusca] = useState('')

  useEffect(() => {
    const fetchRodadas = async () => {
      setLoading(true)
      let query = supabase
        .from('rodadas_roteirizacao')
        .select('*')
        .order('created_at', { ascending: false })
        .limit(100)

      if (!isMaster && filialAtiva) {
        query = query.eq('filial_id', filialAtiva)
      }

      const { data, error } = await query
      if (!error && data) {
        setRodadas(data as RodadaRoteirizacao[])
      }
      setLoading(false)
    }

    fetchRodadas()
  }, [isMaster, filialAtiva])

  const rodadasFiltradas = rodadas.filter((r) => {
    const termo = busca.toLowerCase()
    return (
      r.filial_nome?.toLowerCase().includes(termo) ||
      r.usuario_nome?.toLowerCase().includes(termo) ||
      r.status.toLowerCase().includes(termo)
    )
  })

  const statusBadge = (status: string) => {
    const map: Record<string, string> = {
      sucesso: 'bg-green-100 text-green-700',
      erro: 'bg-red-100 text-red-700',
      processando: 'bg-yellow-100 text-yellow-700',
      parcial: 'bg-orange-100 text-orange-700',
    }
    return map[status] || 'bg-gray-100 text-gray-700'
  }

  return (
    <div className="space-y-6">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Histórico de Rodadas</h1>
          <p className="text-sm text-gray-500 mt-1">
            {isMaster ? 'Todas as rodadas de roteirização' : 'Rodadas da sua filial'}
          </p>
        </div>
      </div>

      {/* Busca */}
      <div className="bg-white rounded-xl border border-gray-200 p-4">
        <input
          type="text"
          placeholder="Buscar por filial, usuário ou status..."
          value={busca}
          onChange={(e) => setBusca(e.target.value)}
          className="w-full px-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brand-500"
        />
      </div>

      {/* Tabela */}
      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center h-48">
            <div className="w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" />
          </div>
        ) : rodadasFiltradas.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-48 text-gray-400">
            <svg className="w-12 h-12 mb-3" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
            </svg>
            <p className="text-sm">Nenhuma rodada encontrada</p>
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Data</th>
                  {isMaster && <th className="text-left px-4 py-3 font-semibold text-gray-600">Filial</th>}
                  <th className="text-left px-4 py-3 font-semibold text-gray-600">Usuário</th>
                  <th className="text-center px-4 py-3 font-semibold text-gray-600">Status</th>
                  <th className="text-right px-4 py-3 font-semibold text-gray-600">Entrada</th>
                  <th className="text-right px-4 py-3 font-semibold text-gray-600">Manifestos</th>
                  <th className="text-right px-4 py-3 font-semibold text-gray-600">Itens</th>
                  <th className="text-right px-4 py-3 font-semibold text-gray-600">Ocupação</th>
                  <th className="text-right px-4 py-3 font-semibold text-gray-600">KM Total</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100">
                {rodadasFiltradas.map((r) => (
                  <tr key={r.id} className="hover:bg-gray-50 transition-colors">
                    <td className="px-4 py-3 text-gray-700">
                      {r.created_at
                        ? format(new Date(r.created_at), "dd/MM/yyyy HH:mm", { locale: ptBR })
                        : '—'}
                    </td>
                    {isMaster && (
                      <td className="px-4 py-3 text-gray-700">{r.filial_nome || '—'}</td>
                    )}
                    <td className="px-4 py-3 text-gray-700">{r.usuario_nome || '—'}</td>
                    <td className="px-4 py-3 text-center">
                      <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${statusBadge(r.status)}`}>
                        {r.status}
                      </span>
                    </td>
                    <td className="px-4 py-3 text-right text-gray-700">{r.total_cargas_entrada?.toLocaleString('pt-BR') || '—'}</td>
                    <td className="px-4 py-3 text-right font-semibold text-brand-700">{r.total_manifestos?.toLocaleString('pt-BR') || '—'}</td>
                    <td className="px-4 py-3 text-right text-gray-700">{r.total_itens_manifestados?.toLocaleString('pt-BR') || '—'}</td>
                    <td className="px-4 py-3 text-right text-gray-700">
                      {r.ocupacao_media_percentual != null ? `${r.ocupacao_media_percentual.toFixed(1)}%` : '—'}
                    </td>
                    <td className="px-4 py-3 text-right text-gray-700">
                      {r.km_total_frota != null ? `${r.km_total_frota.toLocaleString('pt-BR')} km` : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
