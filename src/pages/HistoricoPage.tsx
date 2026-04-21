import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { supabase } from '@/lib/supabase'
import { useAuth } from '@/contexts/AuthContext'
import {
  RodadaRoteirizacao,
  ManifestoRoteirizacaoDetalhe,
  RemanescenteRoteirizacao,
  EstatisticasRoteirizacao,
  ManifestoItemRoteirizacao,
} from '@/types'
import { format } from 'date-fns'
import { ptBR } from 'date-fns/locale'
import { roteirizacaoService } from '@/services/roteirizacao.service'
import toast from 'react-hot-toast'

export function HistoricoPage() {
  const { isMaster, filialAtiva } = useAuth()
  const [searchParams] = useSearchParams()
  const rodadaEmFoco = searchParams.get('rodada')

  const [rodadas, setRodadas] = useState<RodadaRoteirizacao[]>([])
  const [loading, setLoading] = useState(true)
  const [busca, setBusca] = useState('')
  const [rodadaSelecionada, setRodadaSelecionada] = useState<RodadaRoteirizacao | null>(null)
  const [tabAtiva, setTabAtiva] = useState<'manifestos' | 'remanescentes' | 'estatisticas'>('manifestos')
  const [manifestos, setManifestos] = useState<ManifestoRoteirizacaoDetalhe[]>([])
  const [remanescentes, setRemanescentes] = useState<RemanescenteRoteirizacao[]>([])
  const [estatisticas, setEstatisticas] = useState<EstatisticasRoteirizacao | null>(null)
  const [detalhesLoading, setDetalhesLoading] = useState(false)

  const [manifestoAtivo, setManifestoAtivo] = useState<ManifestoRoteirizacaoDetalhe | null>(null)
  const [itensManifesto, setItensManifesto] = useState<ManifestoItemRoteirizacao[]>([])
  const [itensOriginais, setItensOriginais] = useState<ManifestoItemRoteirizacao[]>([])
  const [manifestoLoading, setManifestoLoading] = useState(false)

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
        const rows = data as RodadaRoteirizacao[]
        setRodadas(rows)
        if (rodadaEmFoco) {
          const foco = rows.find((r) => r.id === rodadaEmFoco)
          if (foco) void abrirRodada(foco)
        }
      }
      setLoading(false)
    }

    void fetchRodadas()
  }, [isMaster, filialAtiva, rodadaEmFoco])

  const abrirRodada = async (rodada: RodadaRoteirizacao) => {
    setRodadaSelecionada(rodada)
    setDetalhesLoading(true)
    setManifestoAtivo(null)
    setItensManifesto([])
    try {
      const detalhes = await roteirizacaoService.buscarDetalhesAprovacao(rodada.id)
      setManifestos(detalhes.manifestos)
      setRemanescentes(detalhes.remanescentes)
      setEstatisticas(detalhes.estatisticas)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Erro ao carregar detalhes da rodada')
    } finally {
      setDetalhesLoading(false)
    }
  }

  const abrirManifesto = async (manifesto: ManifestoRoteirizacaoDetalhe) => {
    if (!rodadaSelecionada) return
    setManifestoAtivo(manifesto)
    setManifestoLoading(true)
    try {
      const data = await roteirizacaoService.buscarManifestoOperacional(rodadaSelecionada.id, manifesto.manifesto_id)
      setItensManifesto(data.itens)
      setItensOriginais(data.itens)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Erro ao carregar entregas do manifesto')
    } finally {
      setManifestoLoading(false)
    }
  }

  const rodadaSelecionadaResumo = useMemo(() => {
    if (!rodadaSelecionada) return null
    return {
      data: rodadaSelecionada.created_at ? format(new Date(rodadaSelecionada.created_at), 'dd/MM/yyyy HH:mm', { locale: ptBR }) : '—',
      filial: rodadaSelecionada.filial_nome || '—',
    }
  }, [rodadaSelecionada])

  const alterarSequencia = (indexAtual: number, direcao: -1 | 1) => {
    const destino = indexAtual + direcao
    if (destino < 0 || destino >= itensManifesto.length) return
    const copia = [...itensManifesto]
    const [item] = copia.splice(indexAtual, 1)
    copia.splice(destino, 0, item)
    setItensManifesto(copia.map((row, idx) => ({ ...row, sequencia: idx + 1 })))
  }

  const salvarSequencia = async () => {
    if (!rodadaSelecionada || !manifestoAtivo) return
    try {
      await roteirizacaoService.salvarOrdemManifestoItens(rodadaSelecionada.id, manifestoAtivo.manifesto_id, itensManifesto)
      setItensOriginais(itensManifesto)
      toast.success('Sequência atualizada com sucesso')
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Não foi possível salvar sequência')
    }
  }

  const desfazerSequencia = () => setItensManifesto(itensOriginais)

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
          <h1 className="text-2xl font-bold text-gray-900">Aprovar Roteirização</h1>
          <p className="text-sm text-gray-500 mt-1">
            {isMaster ? 'Rodadas processadas para aprovação operacional' : 'Rodadas processadas da sua filial'}
          </p>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-4">
        <input
          type="text"
          placeholder="Buscar por filial, usuário ou status..."
          value={busca}
          onChange={(e) => setBusca(e.target.value)}
          className="w-full px-4 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brand-500"
        />
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center h-48"><div className="w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" /></div>
        ) : rodadasFiltradas.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-gray-400">Nenhuma rodada encontrada</div>
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
                  <tr key={r.id} className={`hover:bg-gray-50 transition-colors cursor-pointer ${r.id === rodadaSelecionada?.id ? 'bg-brand-50' : ''}`} onClick={() => void abrirRodada(r)}>
                    <td className="px-4 py-3 text-gray-700">{r.created_at ? format(new Date(r.created_at), 'dd/MM/yyyy HH:mm', { locale: ptBR }) : '—'}</td>
                    {isMaster && <td className="px-4 py-3 text-gray-700">{r.filial_nome || '—'}</td>}
                    <td className="px-4 py-3 text-gray-700">{r.usuario_nome || '—'}</td>
                    <td className="px-4 py-3 text-center"><span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium ${statusBadge(r.status)}`}>{r.status}</span></td>
                    <td className="px-4 py-3 text-right text-gray-700">{r.total_cargas_entrada?.toLocaleString('pt-BR') || '—'}</td>
                    <td className="px-4 py-3 text-right font-semibold text-brand-700">{r.total_manifestos?.toLocaleString('pt-BR') || '—'}</td>
                    <td className="px-4 py-3 text-right text-gray-700">{r.total_itens_manifestados?.toLocaleString('pt-BR') || '—'}</td>
                    <td className="px-4 py-3 text-right text-gray-700">{r.ocupacao_media_percentual != null ? `${r.ocupacao_media_percentual.toFixed(1)}%` : '—'}</td>
                    <td className="px-4 py-3 text-right text-gray-700">{r.km_total_frota != null ? `${r.km_total_frota.toLocaleString('pt-BR')} km` : '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {rodadaSelecionada && (
        <div className="bg-white rounded-xl border border-gray-200 p-4 space-y-4">
          <div>
            <h2 className="text-lg font-semibold text-gray-900">Rodada selecionada</h2>
            <p className="text-sm text-gray-500">Data: {rodadaSelecionadaResumo?.data} · Filial: {rodadaSelecionadaResumo?.filial}</p>
          </div>
          <div className="flex gap-2">
            {[
              { key: 'manifestos', label: `Manifestos (${manifestos.length})` },
              { key: 'remanescentes', label: `Remanescentes (${remanescentes.length})` },
              { key: 'estatisticas', label: 'Estatísticas' },
            ].map((tab) => (
              <button key={tab.key} className={`px-3 py-2 rounded-lg text-sm ${tabAtiva === tab.key ? 'bg-brand-100 text-brand-800' : 'bg-gray-100 text-gray-700'}`} onClick={() => setTabAtiva(tab.key as typeof tabAtiva)}>{tab.label}</button>
            ))}
          </div>

          {detalhesLoading && <div className="text-sm text-gray-500">Carregando detalhes...</div>}

          {!detalhesLoading && tabAtiva === 'manifestos' && (
            <div className="space-y-3">
              {manifestos.length === 0 ? <p className="text-sm text-gray-500">Sem manifestos estruturados para esta rodada.</p> : manifestos.map((m) => (
                <button key={m.id} onClick={() => void abrirManifesto(m)} className="w-full text-left border border-gray-200 rounded-lg p-3 hover:bg-gray-50">
                  <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-9 gap-2 text-xs">
                    <div><span className="text-gray-500 block">Manifesto</span><strong>{m.manifesto_id}</strong></div>
                    <div><span className="text-gray-500 block">Entregas</span><strong>{m.qtd_entregas}</strong></div>
                    <div><span className="text-gray-500 block">Clientes</span><strong>{m.qtd_clientes}</strong></div>
                    <div><span className="text-gray-500 block">Peso</span><strong>{m.peso_total.toLocaleString('pt-BR')}</strong></div>
                    <div><span className="text-gray-500 block">KM</span><strong>{m.km_total.toLocaleString('pt-BR')}</strong></div>
                    <div><span className="text-gray-500 block">Ocupação</span><strong>{m.ocupacao.toFixed(1)}%</strong></div>
                    <div><span className="text-gray-500 block">Veículo/Perfil</span><strong>{m.veiculo_perfil || m.veiculo_tipo || '—'}</strong></div>
                    <div><span className="text-gray-500 block">Eixos</span><strong>{m.qtd_eixos ?? '—'}</strong></div>
                    <div><span className="text-gray-500 block">Frete mínimo</span><strong>R$ {m.frete_minimo.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong></div>
                  </div>
                </button>
              ))}
            </div>
          )}

          {!detalhesLoading && tabAtiva === 'remanescentes' && (
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead><tr className="text-left border-b"><th className="py-2">Documento</th><th>Cliente</th><th>Cidade</th><th>Motivo</th><th>Etapa</th></tr></thead>
                <tbody>
                  {remanescentes.map((r) => (
                    <tr key={r.id} className="border-b">
                      <td className="py-2">{r.nro_documento || '—'}</td><td>{r.destinatario || '—'}</td><td>{r.cidade || '—'} / {r.uf || '—'}</td><td>{r.motivo || '—'}</td><td>{r.etapa_origem || '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {!detalhesLoading && tabAtiva === 'estatisticas' && (
            <div className="grid md:grid-cols-4 gap-3 text-sm">
              <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total carteira</div><strong>{estatisticas?.total_carteira ?? 0}</strong></div>
              <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total roteirizado</div><strong>{estatisticas?.total_roteirizado ?? 0}</strong></div>
              <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total remanescente</div><strong>{estatisticas?.total_remanescente ?? 0}</strong></div>
              <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total manifestos</div><strong>{estatisticas?.total_manifestos ?? 0}</strong></div>
              <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Ocupação média</div><strong>{(estatisticas?.ocupacao_media ?? 0).toFixed(1)}%</strong></div>
              <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">KM total</div><strong>{(estatisticas?.km_total ?? 0).toLocaleString('pt-BR')} km</strong></div>
              <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Tempo de execução</div><strong>{(estatisticas?.tempo_execucao_ms ?? 0).toLocaleString('pt-BR')} ms</strong></div>
            </div>
          )}
        </div>
      )}

      {manifestoAtivo && rodadaSelecionada && (
        <div className="bg-white rounded-xl border border-gray-200 p-4 space-y-4">
          <div className="flex items-center justify-between">
            <h3 className="text-lg font-semibold">Manifesto {manifestoAtivo.manifesto_id}</h3>
            <button className="px-3 py-1.5 text-sm rounded-lg bg-gray-100" onClick={() => setManifestoAtivo(null)}>Fechar</button>
          </div>

          <div className="grid md:grid-cols-4 gap-3 text-sm">
            <div><span className="text-gray-500 block">Filial</span><strong>{rodadaSelecionada.filial_nome || '—'}</strong></div>
            <div><span className="text-gray-500 block">Data</span><strong>{rodadaSelecionada.created_at ? format(new Date(rodadaSelecionada.created_at), 'dd/MM/yyyy HH:mm', { locale: ptBR }) : '—'}</strong></div>
            <div><span className="text-gray-500 block">Veículo / Perfil</span><strong>{manifestoAtivo.veiculo_perfil || manifestoAtivo.veiculo_tipo || '—'}</strong></div>
            <div><span className="text-gray-500 block">Qtd. eixos</span><strong>{manifestoAtivo.qtd_eixos ?? '—'}</strong></div>
            <div><span className="text-gray-500 block">KM total</span><strong>{manifestoAtivo.km_total.toLocaleString('pt-BR')}</strong></div>
            <div><span className="text-gray-500 block">Peso total</span><strong>{manifestoAtivo.peso_total.toLocaleString('pt-BR')}</strong></div>
            <div><span className="text-gray-500 block">Qtd. entregas</span><strong>{manifestoAtivo.qtd_entregas}</strong></div>
            <div><span className="text-gray-500 block">Frete mínimo</span><strong>R$ {manifestoAtivo.frete_minimo.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}</strong></div>
          </div>

          {manifestoLoading ? <div className="text-sm text-gray-500">Carregando entregas...</div> : (
            <>
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead className="border-b"><tr><th className="text-left py-2">Sequência</th><th className="text-left">Documento</th><th className="text-left">Destinatário</th><th className="text-left">Cidade</th><th className="text-left">UF</th><th className="text-left">Peso</th><th className="text-left">Janela</th><th className="text-left">Ações</th></tr></thead>
                  <tbody>
                    {itensManifesto.map((item, index) => (
                      <tr key={item.id} className="border-b">
                        <td className="py-2">{item.sequencia}</td>
                        <td>{item.nro_documento || '—'}</td>
                        <td>{item.destinatario || '—'}</td>
                        <td>{item.cidade || '—'}</td>
                        <td>{item.uf || '—'}</td>
                        <td>{item.peso?.toLocaleString('pt-BR') || '—'}</td>
                        <td>{item.inicio_entrega || '—'} - {item.fim_entrega || '—'}</td>
                        <td className="space-x-2"><button onClick={() => alterarSequencia(index, -1)} className="px-2 py-1 bg-gray-100 rounded">↑</button><button onClick={() => alterarSequencia(index, 1)} className="px-2 py-1 bg-gray-100 rounded">↓</button></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="flex gap-2">
                <button onClick={() => void salvarSequencia()} className="px-4 py-2 text-sm rounded-lg bg-brand-600 text-white">Salvar ordem</button>
                <button onClick={desfazerSequencia} className="px-4 py-2 text-sm rounded-lg bg-gray-100">Desfazer</button>
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
