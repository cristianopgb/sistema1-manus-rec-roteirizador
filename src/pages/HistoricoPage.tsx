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
import { gerarPdfManifestoOperacional } from '@/services/pdf.service'
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
  const [modalManifestoAberto, setModalManifestoAberto] = useState(false)
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
    setModalManifestoAberto(false)
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

  useEffect(() => {
    if (tabAtiva !== 'manifestos') {
      setManifestoAtivo(null)
      setModalManifestoAberto(false)
      setItensManifesto([])
      setItensOriginais([])
    }
  }, [tabAtiva])

  const abrirManifesto = async (manifesto: ManifestoRoteirizacaoDetalhe) => {
    if (!rodadaSelecionada) return
    console.log('[UI] abrindo modal do manifesto:', manifesto.manifesto_id)
    setManifestoAtivo(manifesto)
    setModalManifestoAberto(true)
    setManifestoLoading(true)
    try {
      const data = await roteirizacaoService.buscarManifestoOperacional(rodadaSelecionada.id, manifesto.manifesto_id)
      setItensManifesto(data.itens)
      setItensOriginais(data.itens)
      console.log('[UI] itens carregados no modal:', data.itens.length)
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Erro ao carregar entregas do manifesto')
    } finally {
      setManifestoLoading(false)
    }
  }

  const fecharModalManifesto = () => {
    setModalManifestoAberto(false)
    setManifestoAtivo(null)
    setItensManifesto([])
    setItensOriginais([])
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
    if (!rodadaSelecionada || !manifestoAtivo || itensManifesto.length === 0) return
    try {
      await roteirizacaoService.salvarOrdemManifestoItens(rodadaSelecionada.id, manifestoAtivo.manifesto_id, itensManifesto)
      setItensOriginais(itensManifesto)
      toast.success('Sequência atualizada com sucesso')
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Não foi possível salvar sequência')
    }
  }

  const desfazerSequencia = () => setItensManifesto(itensOriginais)

  const exportarManifestoPdf = async () => {
    if (!manifestoAtivo || !rodadaSelecionada) return
    try {
      await gerarPdfManifestoOperacional(manifestoAtivo, itensManifesto, {
        filialNome: rodadaSelecionada.filial_nome,
        dataRodada: rodadaSelecionada.created_at ? format(new Date(rodadaSelecionada.created_at), 'dd/MM/yyyy HH:mm', { locale: ptBR }) : null,
      })
      toast.success('PDF exportado com sucesso')
    } catch (error) {
      toast.error(error instanceof Error ? error.message : 'Erro ao exportar PDF')
    }
  }

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

  const indicadoresTriagem = useMemo(() => {
    if (!rodadaSelecionada) return null
    const resposta = (rodadaSelecionada.resposta_motor ?? {}) as Record<string, unknown>
    const resumoExecucao = (resposta.resumo_execucao ?? {}) as Record<string, unknown>
    const resumoNegocio = (resposta.resumo_negocio ?? {}) as Record<string, unknown>
    const toNum = (value: unknown, fallback = 0) => {
      const parsed = Number(value)
      return Number.isFinite(parsed) ? parsed : fallback
    }
    const totalCarteira = Math.trunc(toNum(
      rodadaSelecionada.total_cargas_entrada,
      toNum((resposta as Record<string, unknown>).total_carteira, toNum(resumoNegocio.total_carteira, toNum(resumoExecucao.total_carteira, 0))),
    ))
    const carteiraRoteirizavel = Math.trunc(toNum(
      resumoNegocio.carteira_roteirizavel,
      toNum(resumoExecucao.carteira_roteirizavel, totalCarteira),
    ))
    const agendasVencidas = Math.trunc(toNum(
      resumoNegocio.carteira_agendas_vencidas,
      toNum(resumoExecucao.carteira_agendas_vencidas, Array.isArray(resposta.cargas_agenda_vencida) ? resposta.cargas_agenda_vencida.length : 0),
    ))
    const agendamentoFuturo = Math.trunc(toNum(
      resumoNegocio.carteira_agendamento_futuro,
      toNum(resumoExecucao.carteira_agendamento_futuro, Array.isArray(resposta.cargas_agendamento_futuro) ? resposta.cargas_agendamento_futuro.length : 0),
    ))
    const excecoesTriagem = Math.trunc(toNum(
      resumoNegocio.carteira_excecoes_triagem,
      toNum(resumoExecucao.carteira_excecoes_triagem, Array.isArray(resposta.cargas_excecao_triagem) ? resposta.cargas_excecao_triagem.length : 0),
    ))
    const itensRoteirizados = estatisticas?.total_roteirizado ?? manifestos.reduce((acc, manifesto) => acc + (manifesto.qtd_entregas || 0), 0)
    const totalManifestos = estatisticas?.total_manifestos ?? manifestos.length
    const kmTotal = estatisticas?.km_total ?? manifestos.reduce((acc, manifesto) => acc + (manifesto.km_total || 0), 0)
    const ocupacaoMedia = estatisticas?.ocupacao_media ?? (manifestos.length ? manifestos.reduce((acc, manifesto) => acc + (manifesto.ocupacao || 0), 0) / manifestos.length : 0)
    const tempoExecucao = estatisticas?.tempo_execucao_ms ?? rodadaSelecionada.tempo_processamento_ms ?? 0
    const itensNaoRoteirizados = Math.max(0, carteiraRoteirizavel - itensRoteirizados)
    const naoAtendidosUniversoTotal = Math.max(0, totalCarteira - itensRoteirizados)
    const taxaAproveitamento = totalCarteira > 0 ? (itensRoteirizados / totalCarteira) * 100 : 0

    const indicadores = {
      totalCarteira,
      carteiraRoteirizavel,
      itensRoteirizados,
      itensNaoRoteirizados,
      totalManifestos,
      itensPorManifesto: totalManifestos > 0 ? itensRoteirizados / totalManifestos : 0,
      ocupacaoMedia,
      kmTotal,
      kmMedioManifesto: totalManifestos > 0 ? kmTotal / totalManifestos : 0,
      tempoExecucao,
      agendasVencidas,
      agendamentoFuturo,
      excecoesTriagem,
      taxaAproveitamento,
      naoAtendidosUniversoTotal,
    }
    console.log('[ESTATISTICAS] indicadores calculados:', indicadores)
    return indicadores
  }, [estatisticas, manifestos, rodadaSelecionada])

  const itensAgendados = useMemo(() => (
    itensManifesto.filter((item) => Boolean(item.inicio_entrega || item.fim_entrega || (item as unknown as Record<string, unknown>).data_agenda))
  ), [itensManifesto])

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Aprovar Roteirização</h1>
          <p className="text-sm text-gray-500 mt-1">
            {isMaster ? 'Rodadas processadas para aprovação operacional' : 'Rodadas processadas da sua filial'}
          </p>
        </div>
      </div>

      <div className="bg-white rounded-xl border border-gray-200 p-3">
        <input
          type="text"
          placeholder="Buscar por filial, usuário ou status..."
          value={busca}
          onChange={(e) => setBusca(e.target.value)}
          className="w-full px-3 py-2 border border-gray-200 rounded-lg text-sm focus:outline-none focus:ring-2 focus:ring-brand-500"
        />
      </div>

      <div className="bg-white rounded-xl border border-gray-200 overflow-hidden">
        {loading ? (
          <div className="flex items-center justify-center h-48"><div className="w-8 h-8 border-4 border-brand-600 border-t-transparent rounded-full animate-spin" /></div>
        ) : rodadasFiltradas.length === 0 ? (
          <div className="flex items-center justify-center h-32 text-gray-400">Nenhuma rodada encontrada</div>
        ) : (
          <div className="max-h-[45vh] min-h-[320px] overflow-y-auto">
            <div className="overflow-x-auto">
              <table className="w-full text-xs md:text-sm">
                <thead className="sticky top-0 z-10 bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="text-left px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[150px]">Data</th>
                  {isMaster && <th className="text-left px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[140px]">Filial</th>}
                  <th className="text-left px-3 py-2 font-semibold text-gray-600 whitespace-nowrap min-w-[140px]">Usuário</th>
                  <th className="text-center px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[110px]">Status</th>
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[90px]">Entrada</th>
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[110px]">Manifestos</th>
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[90px]">Itens</th>
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[95px]">Ocupação</th>
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[120px]">KM Total</th>
                </tr>
                </thead>
                <tbody className="divide-y divide-gray-100">
                  {rodadasFiltradas.map((r) => (
                    <tr key={r.id} className={`hover:bg-gray-50 transition-colors cursor-pointer ${r.id === rodadaSelecionada?.id ? 'bg-brand-50' : ''}`} onClick={() => void abrirRodada(r)}>
                      <td className="px-3 py-2 text-gray-700 whitespace-nowrap">{r.created_at ? format(new Date(r.created_at), 'dd/MM/yyyy HH:mm', { locale: ptBR }) : '—'}</td>
                      {isMaster && <td className="px-3 py-2 text-gray-700">{r.filial_nome || '—'}</td>}
                      <td className="px-3 py-2 text-gray-700">{r.usuario_nome || '—'}</td>
                      <td className="px-3 py-2 text-center"><span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-medium ${statusBadge(r.status)}`}>{r.status}</span></td>
                      <td className="px-3 py-2 text-right text-gray-700">{r.total_cargas_entrada?.toLocaleString('pt-BR') || '—'}</td>
                      <td className="px-3 py-2 text-right font-semibold text-brand-700">{r.total_manifestos?.toLocaleString('pt-BR') || '—'}</td>
                      <td className="px-3 py-2 text-right text-gray-700">{r.total_itens_manifestados?.toLocaleString('pt-BR') || '—'}</td>
                      <td className="px-3 py-2 text-right text-gray-700">{r.ocupacao_media_percentual != null ? `${r.ocupacao_media_percentual.toFixed(1)}%` : '—'}</td>
                      <td className="px-3 py-2 text-right text-gray-700 whitespace-nowrap">{r.km_total_frota != null ? `${r.km_total_frota.toLocaleString('pt-BR')} km` : '—'}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        )}
      </div>

      {rodadaSelecionada && (
        <div className="bg-white rounded-xl border border-gray-200 p-4 space-y-3">
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
            <div className="space-y-4">
              <div className="grid md:grid-cols-4 gap-3 text-sm">
                <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total carteira</div><strong>{estatisticas?.total_carteira ?? 0}</strong></div>
                <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total roteirizado</div><strong>{estatisticas?.total_roteirizado ?? 0}</strong></div>
                <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total remanescente final</div><strong>{estatisticas?.total_remanescente ?? 0}</strong></div>
                <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total manifestos</div><strong>{estatisticas?.total_manifestos ?? 0}</strong></div>
              </div>
              <div className="grid md:grid-cols-3 gap-3 text-sm">
                <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Ocupação média</div><strong>{(estatisticas?.ocupacao_media ?? 0).toFixed(1)}%</strong></div>
                <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">KM total</div><strong>{(estatisticas?.km_total ?? 0).toLocaleString('pt-BR')} km</strong></div>
                <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Tempo de execução</div><strong>{(estatisticas?.tempo_execucao_ms ?? 0).toLocaleString('pt-BR')} ms</strong></div>
              </div>
              {indicadoresTriagem && (
                <div className="space-y-2">
                  <h4 className="text-sm font-semibold text-gray-700">Indicadores de triagem e aproveitamento</h4>
                  <div className="grid md:grid-cols-4 gap-3 text-sm">
                    <div className="p-3 rounded-lg bg-blue-50"><div className="text-gray-600">Total de Cargas</div><strong>{indicadoresTriagem.totalCarteira}</strong></div>
                    <div className="p-3 rounded-lg bg-blue-50"><div className="text-gray-600">Cargas Roteirizáveis</div><strong>{indicadoresTriagem.carteiraRoteirizavel}</strong></div>
                    <div className="p-3 rounded-lg bg-green-50"><div className="text-gray-600">Itens Roteirizados</div><strong>{indicadoresTriagem.itensRoteirizados}</strong></div>
                    <div className="p-3 rounded-lg bg-amber-50"><div className="text-gray-600">Itens Não Roteirizados</div><strong>{indicadoresTriagem.itensNaoRoteirizados}</strong></div>
                    <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-600">Não Atendidos (Universo Total)</div><strong>{indicadoresTriagem.naoAtendidosUniversoTotal}</strong></div>
                    <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-600">Manifestos Gerados</div><strong>{indicadoresTriagem.totalManifestos}</strong></div>
                    <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-600">Itens por Manifesto</div><strong>{indicadoresTriagem.itensPorManifesto.toFixed(2)}</strong></div>
                    <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-600">Ocupação Média (%)</div><strong>{indicadoresTriagem.ocupacaoMedia.toFixed(1)}%</strong></div>
                    <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-600">KM Total</div><strong>{indicadoresTriagem.kmTotal.toLocaleString('pt-BR')} km</strong></div>
                    <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-600">KM Médio por Manifesto</div><strong>{indicadoresTriagem.kmMedioManifesto.toFixed(2)} km</strong></div>
                    <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-600">Tempo de Execução (ms)</div><strong>{Math.trunc(indicadoresTriagem.tempoExecucao).toLocaleString('pt-BR')}</strong></div>
                    <div className="p-3 rounded-lg bg-rose-50"><div className="text-gray-600">Cargas com Agenda Vencida</div><strong>{indicadoresTriagem.agendasVencidas}</strong></div>
                    <div className="p-3 rounded-lg bg-indigo-50"><div className="text-gray-600">Cargas com Agendamento Futuro</div><strong>{indicadoresTriagem.agendamentoFuturo}</strong></div>
                    <div className="p-3 rounded-lg bg-red-50"><div className="text-gray-600">Exceções de Triagem</div><strong>{indicadoresTriagem.excecoesTriagem}</strong></div>
                    <div className="p-3 rounded-lg bg-emerald-50"><div className="text-gray-600">Taxa de Aproveitamento (%)</div><strong>{indicadoresTriagem.taxaAproveitamento.toFixed(1)}%</strong></div>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      )}

      {modalManifestoAberto && manifestoAtivo && rodadaSelecionada && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 p-4">
          <div className="bg-white w-full max-w-6xl rounded-xl border border-gray-200 shadow-2xl max-h-[92vh] overflow-y-auto p-4 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="text-lg font-semibold">Manifesto {manifestoAtivo.manifesto_id}</h3>
              <button className="px-3 py-1.5 text-sm rounded-lg bg-gray-100" onClick={fecharModalManifesto}>Fechar</button>
            </div>

            <div className="grid md:grid-cols-4 gap-3 text-sm">
              <div><span className="text-gray-500 block">Manifesto</span><strong>{manifestoAtivo.manifesto_id}</strong></div>
              <div><span className="text-gray-500 block">Filial</span><strong>{rodadaSelecionada.filial_nome || '—'}</strong></div>
              <div><span className="text-gray-500 block">Data da rodada</span><strong>{rodadaSelecionada.created_at ? format(new Date(rodadaSelecionada.created_at), 'dd/MM/yyyy HH:mm', { locale: ptBR }) : '—'}</strong></div>
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
                <div className="border border-gray-200 rounded-lg p-3 space-y-3">
                  <h4 className="text-sm font-semibold text-gray-800">Romaneio / Resumo operacional</h4>
                  <div className="grid md:grid-cols-3 gap-2 text-xs">
                    {[
                      ['Linha / rota', manifestoAtivo.tipo_manifesto || manifestoAtivo.manifesto_id || '—'],
                      ['Remetente', '—'],
                      ['N. Fiscal(s)/Data', '—'],
                      ['Destinatário', itensManifesto[0]?.destinatario || '—'],
                      ['Cidade', itensManifesto[0] ? `${itensManifesto[0].cidade || '—'} / ${itensManifesto[0].uf || '—'}` : '—'],
                      ['Doc CTRC / documento', itensManifesto[0]?.nro_documento || '—'],
                      ['Peso bruto', manifestoAtivo.peso_total?.toLocaleString('pt-BR') || '—'],
                      ['Peso KG', manifestoAtivo.peso_total?.toLocaleString('pt-BR') || '—'],
                      ['Valor da mercadoria', '—'],
                      ['Tipo de carga', manifestoAtivo.tipo_manifesto || '—'],
                      ['Data chegada', '—'],
                      ['Data descarga', '—'],
                      ['Senha do SAR', '—'],
                      ['Atendente', rodadaSelecionada.usuario_nome || '—'],
                    ].map(([label, value]) => (
                      <div key={label} className="bg-gray-50 rounded p-2 border border-gray-100">
                        <div className="text-gray-500">{label}</div>
                        <div className="font-semibold text-gray-800">{value || '—'}</div>
                      </div>
                    ))}
                  </div>
                </div>
                {itensAgendados.length > 0 ? (
                  <div className="border border-amber-300 bg-amber-50 rounded-lg p-3 space-y-2">
                    <h4 className="text-sm font-semibold text-amber-900">Cargas agendadas em destaque ({itensAgendados.length})</h4>
                    <div className="overflow-x-auto">
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="border-b border-amber-300 text-left">
                            <th className="py-1">CTE / Documento</th>
                            <th>Destinatário</th>
                            <th>Cidade</th>
                            <th>UF</th>
                            <th>Data</th>
                            <th>Hora</th>
                            <th>Info agendamento</th>
                          </tr>
                        </thead>
                        <tbody>
                          {itensAgendados.map((item) => {
                            const extra = item as unknown as Record<string, unknown>
                            return (
                              <tr key={`ag-${item.id}`} className="border-b border-amber-200">
                                <td className="py-1">{item.nro_documento || '—'}</td>
                                <td>{item.destinatario || '—'}</td>
                                <td>{item.cidade || '—'}</td>
                                <td>{item.uf || '—'}</td>
                                <td>{String(extra.data_agenda ?? '—')}</td>
                                <td>{`${item.inicio_entrega || '—'} - ${item.fim_entrega || '—'}`}</td>
                                <td>{String(extra.janela ?? extra.info_agendamento ?? 'Agendada')}</td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : (
                  <p className="text-xs text-gray-500">Sem cargas agendadas.</p>
                )}
                <div className="flex flex-wrap gap-2">
                  <button disabled={itensManifesto.length === 0} onClick={() => void salvarSequencia()} className="px-4 py-2 text-sm rounded-lg bg-brand-600 text-white disabled:opacity-40 disabled:cursor-not-allowed">Salvar ordem</button>
                  <button disabled={itensManifesto.length === 0} onClick={desfazerSequencia} className="px-4 py-2 text-sm rounded-lg bg-gray-100 disabled:opacity-40 disabled:cursor-not-allowed">Desfazer</button>
                  <button disabled={!manifestoAtivo} onClick={() => void exportarManifestoPdf()} className="px-4 py-2 text-sm rounded-lg bg-purple-600 text-white disabled:opacity-40 disabled:cursor-not-allowed">Exportar PDF</button>
                </div>
              </>
            )}
          </div>
        </div>
      )}

    </div>
  )
}
