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

type TipoRemanescenteTab = 'todos' | 'roteirizavel_saldo_final' | 'nao_roteirizavel_triagem'

const normalizarTipoRemanescente = (r: RemanescenteRoteirizacao): 'roteirizavel_saldo_final' | 'nao_roteirizavel_triagem' | 'desconhecido' => {
  if (r.tipo_remanescente === 'roteirizavel_saldo_final' || r.tipo_remanescente === 'nao_roteirizavel_triagem') {
    return r.tipo_remanescente
  }
  if (r.etapa_origem === 'm3_triagem') return 'nao_roteirizavel_triagem'
  if (r.etapa_origem === 'saldo_final_roteirizacao') return 'roteirizavel_saldo_final'
  return 'desconhecido'
}

const formatarNumeroCurto = (valor: number | null | undefined, sufixo?: string): string => {
  if (typeof valor !== 'number' || !Number.isFinite(valor)) return '-'
  const numero = valor.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 1 })
  return sufixo ? `${numero} ${sufixo}` : numero
}

const formatarCidadeUf = (cidade: string | null, uf: string | null): string => {
  const cidadeTexto = String(cidade ?? '').trim()
  const ufTexto = String(uf ?? '').trim()
  if (!cidadeTexto && !ufTexto) return '-'
  return `${cidadeTexto || '-'} / ${ufTexto || '-'}`
}

const textoSeguro = (valor: unknown): string => {
  const txt = String(valor ?? '').trim()
  return txt || '-'
}

const CAMPOS_EXCLUSIVO = ['veiculo_exclusivo_flag', 'veiculo_exclusivo', 'exclusivo_flag', 'carro_dedicado', 'carro_dedicado_flag', 'exclusivo', 'dedicado']
const CAMPOS_AGENDAMENTO = ['agendada', 'agenda', 'agendam', 'data_agenda', 'hora_agenda', 'janela', 'inicio_entrega', 'fim_entrega', 'status_agendamento', 'agendamento', 'info_agendamento']
const CAMPOS_RESTRICAO = ['restricao_veiculo', 'cliente_com_restricao', 'restricao', 'restricoes', 'tem_restricao']

const formatarKm = (valor: number | null | undefined): string => `${(valor ?? 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })} km`
const formatarMoeda = (valor: number | null | undefined): string => `R$ ${(valor ?? 0).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`
const formatarPeso = (valor: number | null | undefined): string => (valor ?? 0).toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 3 })
const formatarPercentual = (valor: number | null | undefined): string => `${(valor ?? 0).toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 })}%`

const normalizarTexto = (valor: unknown): string => String(valor ?? '').trim().toLowerCase()
const normalizarTextoBusca = (valor: unknown): string => normalizarTexto(valor)
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')

const juntarRegistrosAninhados = (registro: Record<string, unknown>): Record<string, unknown> => {
  const registroFinal: Record<string, unknown> = { ...registro }
  const candidatos = [
    registro.payload_apoio_json,
    registro.extra_json,
    registro.metadados,
    registro.metadata,
  ]
  candidatos.forEach((candidato) => {
    if (candidato && typeof candidato === 'object' && !Array.isArray(candidato)) {
      Object.assign(registroFinal, candidato as Record<string, unknown>)
    }
  })
  return registroFinal
}

const coletarTextosRegistro = (registro: Record<string, unknown>): string[] => {
  const coletados: string[] = []
  const visitar = (valor: unknown, profundidade = 0) => {
    if (valor === null || valor === undefined || profundidade > 3) return
    if (typeof valor === 'string') {
      const normalizado = normalizarTextoBusca(valor)
      if (normalizado) coletados.push(normalizado)
      return
    }
    if (typeof valor === 'number' || typeof valor === 'boolean') {
      coletados.push(String(valor).toLowerCase())
      return
    }
    if (Array.isArray(valor)) {
      valor.forEach((item) => visitar(item, profundidade + 1))
      return
    }
    if (typeof valor === 'object') {
      Object.values(valor as Record<string, unknown>).forEach((item) => visitar(item, profundidade + 1))
    }
  }
  visitar(registro)
  return coletados
}

const isTruthyFlag = (valor: unknown): boolean => {
  if (typeof valor === 'boolean') return valor
  const texto = normalizarTextoBusca(valor)
  if (!texto) return false
  return ['1', 'true', 'sim', 's', 'y', 'yes', 'exclusivo', 'agendado', 'restricao'].includes(texto)
}

type EspecificidadeVisual = 'normal' | 'exclusivo' | 'agendada' | 'restricao' | 'multipla'

const temCampoVerdadeiro = (registro: Record<string, unknown>, campos: string[]): boolean => {
  const registroExpandido = juntarRegistrosAninhados(registro)
  return campos.some((campo) => isTruthyFlag(registroExpandido[campo]))
}

const temTextoIndicativo = (registro: Record<string, unknown>, termos: string[], termosNegativos: string[] = []): boolean => {
  const registroExpandido = juntarRegistrosAninhados(registro)
  const textos = coletarTextosRegistro(registroExpandido)
  return textos.some((texto) => {
    const temPositivo = termos.some((termo) => texto.includes(termo))
    if (!temPositivo) return false
    return !termosNegativos.some((termoNegativo) => texto.includes(termoNegativo))
  })
}

const temAgendamento = (registro: Record<string, unknown>): boolean => {
  const registroExpandido = juntarRegistrosAninhados(registro)
  if (temCampoVerdadeiro(registroExpandido, CAMPOS_AGENDAMENTO)) return true
  return CAMPOS_AGENDAMENTO.some((campo) => {
    const valor = registroExpandido[campo]
    return typeof valor === 'string' && valor.trim().length > 0
  }) || temTextoIndicativo(registroExpandido, ['agendad', 'agenda', 'janela'], ['sem agenda', 'nao agend'])
}

const obterEspecificidadeVisual = (registro: Record<string, unknown>): EspecificidadeVisual => {
  const registroExpandido = juntarRegistrosAninhados(registro)
  const exclusivo = temCampoVerdadeiro(registroExpandido, CAMPOS_EXCLUSIVO)
    || temTextoIndicativo(registroExpandido, ['exclusiv', 'dedicad'], ['nao exclusiv'])
  const agendada = temAgendamento(registroExpandido)
  const restricao = temCampoVerdadeiro(registroExpandido, CAMPOS_RESTRICAO)
    || temTextoIndicativo(registroExpandido, ['restri'], ['sem restri', 'nao restri'])
  const total = [exclusivo, agendada, restricao].filter(Boolean).length
  if (total > 1) return 'multipla'
  if (exclusivo) return 'exclusivo'
  if (agendada) return 'agendada'
  if (restricao) return 'restricao'
  return 'normal'
}

const estiloEspecificidade: Record<EspecificidadeVisual, { marcador: string; fundo: string; badge: string; legenda: string }> = {
  normal: { marcador: 'bg-transparent', fundo: 'bg-white', badge: 'bg-gray-100 text-gray-600', legenda: 'Normal' },
  exclusivo: { marcador: 'bg-blue-500', fundo: 'bg-blue-50/40', badge: 'bg-blue-100 text-blue-700', legenda: 'Exclusivo' },
  agendada: { marcador: 'bg-amber-400', fundo: 'bg-amber-50/40', badge: 'bg-amber-100 text-amber-700', legenda: 'Agendada' },
  restricao: { marcador: 'bg-emerald-500', fundo: 'bg-emerald-50/40', badge: 'bg-emerald-100 text-emerald-700', legenda: 'Restrição' },
  multipla: { marcador: 'bg-red-500', fundo: 'bg-red-50/40', badge: 'bg-red-100 text-red-700', legenda: 'Múltiplas' },
}

const formatarData = (valor: unknown, comHora = false): string => {
  if (!valor) return '—'
  const data = new Date(String(valor))
  if (Number.isNaN(data.getTime())) return '—'
  return format(data, comHora ? 'dd/MM/yyyy HH:mm' : 'dd/MM/yyyy', { locale: ptBR })
}

const coletarValores = (itens: ManifestoItemRoteirizacao[], campos: string[]): string[] => {
  const set = new Set<string>()
  itens.forEach((item) => {
    const row = item as unknown as Record<string, unknown>
    campos.forEach((campo) => {
      const valor = String(row[campo] ?? '').trim()
      if (valor) set.add(valor)
    })
  })
  return Array.from(set)
}

const juntarValores = (valores: string[], separador = ' / '): string => (valores.length ? valores.join(separador) : '—')

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
  const [subabaRemanescentes, setSubabaRemanescentes] = useState<TipoRemanescenteTab>('todos')
  const [buscaRemanescentes, setBuscaRemanescentes] = useState('')
  const [filtroTipoRemanescente, setFiltroTipoRemanescente] = useState<'todos' | 'roteirizavel_saldo_final' | 'nao_roteirizavel_triagem'>('todos')
  const [filtroMesorregiao, setFiltroMesorregiao] = useState('todos')
  const [filtroSubregiao, setFiltroSubregiao] = useState('todos')
  const [filtroMotivo, setFiltroMotivo] = useState('todos')

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
      setSubabaRemanescentes('todos')
      setBuscaRemanescentes('')
      setFiltroTipoRemanescente('todos')
      setFiltroMesorregiao('todos')
      setFiltroSubregiao('todos')
      setFiltroMotivo('todos')
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

  const remanescentesNormalizados = useMemo(() => remanescentes.map((item) => ({
    ...item,
    tipo_normalizado: normalizarTipoRemanescente(item),
  })), [remanescentes])

  const estatisticasComposicao = useMemo(() => {
    if (!rodadaSelecionada) return null

    const respostaMotor = (rodadaSelecionada.resposta_motor ?? {}) as Record<string, unknown>
    const resumoNegocio = (respostaMotor.resumo_negocio ?? {}) as Record<string, unknown>
    const resumoExecucao = (respostaMotor.resumo_execucao ?? {}) as Record<string, unknown>
    const estatisticasRaw = (estatisticas ?? {}) as Record<string, unknown>

    const toNum = (value: unknown): number | null => {
      const parsed = Number(value)
      return Number.isFinite(parsed) ? parsed : null
    }

    const toInt = (value: unknown): number | null => {
      const parsed = toNum(value)
      return parsed === null ? null : Math.max(0, Math.trunc(parsed))
    }

    const getByPath = (source: Record<string, unknown>, path: string): unknown => (
      path.split('.').reduce<unknown>((acc, key) => {
        if (!acc || typeof acc !== 'object' || Array.isArray(acc)) return null
        return (acc as Record<string, unknown>)[key]
      }, source)
    )

    const pickFirstInt = (source: Record<string, unknown>, paths: string[]): number | null => {
      for (const path of paths) {
        const value = toInt(getByPath(source, path))
        if (value !== null) return value
      }
      return null
    }

    const pickFirstArrayLength = (source: Record<string, unknown>, paths: string[]): number | null => {
      for (const path of paths) {
        const value = getByPath(source, path)
        if (Array.isArray(value)) return value.length
      }
      return null
    }

    const possuiTexto = (value: unknown, termos: string[]): boolean => {
      const texto = String(value ?? '').toLowerCase().normalize('NFD').replace(/[̀-ͯ]/g, '')
      return termos.some((termo) => texto.includes(termo))
    }

    const totalCarteira = Math.max(0, Math.trunc(
      estatisticas?.total_carteira
      ?? rodadaSelecionada.total_cargas_entrada
      ?? pickFirstInt(resumoNegocio, ['total_carteira'])
      ?? pickFirstInt(resumoExecucao, ['total_carteira'])
      ?? pickFirstInt(respostaMotor, ['total_carteira'])
      ?? 0,
    ))

    const itensRoteirizados = Math.max(0, Math.trunc(
      estatisticas?.total_roteirizado
      ?? rodadaSelecionada.total_itens_manifestados
      ?? manifestos.reduce((acc, manifesto) => acc + (manifesto.qtd_entregas || 0), 0),
    ))

    const totalManifestos = Math.max(0, Math.trunc(
      estatisticas?.total_manifestos
      ?? rodadaSelecionada.total_manifestos
      ?? manifestos.length,
    ))

    const ocupacaoMedia = estatisticas?.ocupacao_media
      ?? rodadaSelecionada.ocupacao_media_percentual
      ?? (manifestos.length ? manifestos.reduce((acc, manifesto) => acc + (manifesto.ocupacao || 0), 0) / manifestos.length : 0)

    const kmTotal = estatisticas?.km_total
      ?? rodadaSelecionada.km_total_frota
      ?? manifestos.reduce((acc, manifesto) => acc + (manifesto.km_total || 0), 0)

    const tempoExecucaoMs = estatisticas?.tempo_execucao_ms
      ?? rodadaSelecionada.tempo_processamento_ms
      ?? 0

    const remanescentesNaoRoteirizaveis = remanescentesNormalizados.filter((item) => item.tipo_normalizado === 'nao_roteirizavel_triagem')
    const saldoFinalRoteirizavelBase = remanescentesNormalizados.filter((item) => item.tipo_normalizado === 'roteirizavel_saldo_final').length

    const agendamentoFuturoBase = remanescentesNaoRoteirizaveis.filter((item) => (
      possuiTexto(item.etapa_origem, ['agendamento_futuro', 'agenda_futura', 'agenda futura'])
      || possuiTexto(item.motivo, ['agendamento futuro', 'agenda futura'])
      || possuiTexto(item.motivo_triagem, ['agendamento futuro', 'agenda futura'])
      || possuiTexto(item.status_triagem, ['agendamento futuro', 'agenda futura'])
    )).length

    const aguardandoAgendamentoBase = remanescentesNaoRoteirizaveis.filter((item) => (
      possuiTexto(item.etapa_origem, ['aguardando_agendamento'])
      || possuiTexto(item.motivo, ['aguardando agendamento'])
      || possuiTexto(item.motivo_triagem, ['aguardando agendamento'])
      || possuiTexto(item.status_triagem, ['aguardando agendamento'])
    )).length

    const excecoesTriagemBase = remanescentesNaoRoteirizaveis.filter((item) => (
      possuiTexto(item.etapa_origem, ['excecao_triagem'])
      || possuiTexto(item.motivo, ['excecao'])
      || possuiTexto(item.motivo_triagem, ['excecao'])
      || possuiTexto(item.status_triagem, ['excecao'])
    )).length

    const naoRoteirizaveisTriagemBase = Math.max(0, remanescentesNaoRoteirizaveis.length - agendamentoFuturoBase - aguardandoAgendamentoBase - excecoesTriagemBase)

    const saldoFinalRoteirizavel = saldoFinalRoteirizavelBase || pickFirstInt(estatisticasRaw, ['saldo_final_roteirizavel'])
      || pickFirstArrayLength(respostaMotor, [
        'remanescentes.roteirizavel_saldo_final',
        'remanescentes.saldo_final_roteirizacao',
        'remanescentes.roteirizaveis_saldo_final',
      ])
      || (estatisticas?.total_remanescente ?? 0)

    const naoRoteirizaveisTriagem = naoRoteirizaveisTriagemBase || pickFirstInt(estatisticasRaw, ['nao_roteirizaveis_triagem'])
      || pickFirstArrayLength(respostaMotor, ['remanescentes.nao_roteirizaveis_m3'])
      || 0

    const agendamentoFuturo = agendamentoFuturoBase || pickFirstInt(estatisticasRaw, ['agendamento_futuro'])
      || pickFirstInt(resumoNegocio, ['agendamento_futuro', 'carteira_agendamento_futuro'])
      || pickFirstInt(resumoExecucao, ['agendamento_futuro', 'carteira_agendamento_futuro'])
      || pickFirstArrayLength(respostaMotor, ['cargas_agendamento_futuro', 'remanescentes.agendamento_futuro'])
      || 0

    const aguardandoAgendamento = aguardandoAgendamentoBase || pickFirstInt(estatisticasRaw, ['aguardando_agendamento'])
      || pickFirstInt(resumoNegocio, ['aguardando_agendamento', 'carteira_aguardando_agendamento'])
      || pickFirstInt(resumoExecucao, ['aguardando_agendamento', 'carteira_aguardando_agendamento'])
      || pickFirstArrayLength(respostaMotor, ['remanescentes.aguardando_agendamento'])
      || 0

    const excecoesTriagem = excecoesTriagemBase || pickFirstInt(estatisticasRaw, ['excecoes_triagem'])
      || pickFirstInt(resumoNegocio, ['excecoes_triagem', 'carteira_excecoes_triagem'])
      || pickFirstInt(resumoExecucao, ['excecoes_triagem', 'carteira_excecoes_triagem'])
      || pickFirstArrayLength(respostaMotor, ['cargas_excecao_triagem', 'remanescentes.excecoes_triagem'])
      || 0

    const naoRoteirizadosTotal = Math.max(0, totalCarteira - itensRoteirizados)
    const classificadosSemOutros = saldoFinalRoteirizavel + naoRoteirizaveisTriagem + agendamentoFuturo + aguardandoAgendamento + excecoesTriagem
    let outrosNaoClassificados = naoRoteirizadosTotal - classificadosSemOutros

    if (outrosNaoClassificados < 0) {
      console.warn('[ESTATISTICAS] composição dos não roteirizados excedeu o total', {
        naoRoteirizadosTotal,
        saldoFinalRoteirizavel,
        naoRoteirizaveisTriagem,
        agendamentoFuturo,
        aguardandoAgendamento,
        excecoesTriagem,
      })
      outrosNaoClassificados = 0
    }

    const totalClassificado = itensRoteirizados + saldoFinalRoteirizavel + naoRoteirizaveisTriagem + agendamentoFuturo + aguardandoAgendamento + excecoesTriagem + outrosNaoClassificados
    const diferencaClassificacao = totalCarteira - totalClassificado

    return {
      totalCarteira,
      itensRoteirizados,
      naoRoteirizadosTotal,
      saldoFinalRoteirizavel,
      naoRoteirizaveisTriagem,
      agendamentoFuturo,
      aguardandoAgendamento,
      excecoesTriagem,
      outrosNaoClassificados,
      totalManifestos,
      ocupacaoMedia,
      kmTotal,
      kmMedioManifesto: totalManifestos > 0 ? kmTotal / totalManifestos : 0,
      itensPorManifesto: totalManifestos > 0 ? itensRoteirizados / totalManifestos : 0,
      tempoExecucaoMs,
      taxaRoteirizacao: totalCarteira > 0 ? (itensRoteirizados / totalCarteira) * 100 : 0,
      aproveitamentoUniversoRoteirizavel: (itensRoteirizados + saldoFinalRoteirizavel) > 0
        ? (itensRoteirizados / (itensRoteirizados + saldoFinalRoteirizavel)) * 100
        : 0,
      totalClassificado,
      diferencaClassificacao,
    }
  }, [estatisticas, manifestos, remanescentesNormalizados, rodadaSelecionada])

  const itensAgendados = useMemo(() => (
    itensManifesto.filter((item) => temAgendamento(item as unknown as Record<string, unknown>))
  ), [itensManifesto])

  const manifestosComSinalizacao = useMemo(() => (
    manifestos.map((manifesto) => ({
      manifesto,
      especificidade: obterEspecificidadeVisual(manifesto as unknown as Record<string, unknown>),
    }))
  ), [manifestos])

  const itensComSinalizacao = useMemo(() => (
    itensManifesto.map((item) => ({
      item,
      especificidade: obterEspecificidadeVisual(item as unknown as Record<string, unknown>),
    }))
  ), [itensManifesto])

  const resumoOperacional = useMemo(() => {
    const remetentes = coletarValores(itensManifesto, ['remetente', 'tomad'])
    const destinatarios = coletarValores(itensManifesto, ['destinatario'])
    const cidades = coletarValores(itensManifesto, ['cidade'])
    const docs = coletarValores(itensManifesto, ['nro_documento', 'documento', 'ctrc'])
    const linhaRota = coletarValores(itensManifesto, ['linha', 'rota', 'tipo_carga', 'tipo_ca'])
    const tipoCarga = coletarValores(itensManifesto, ['tipo_carga', 'tipo_ca', 'classif'])
    const senhaSar = coletarValores(itensManifesto, ['senha_sar', 'sar'])
    const atendente = coletarValores(itensManifesto, ['atendente'])
    const nfsDatas = itensManifesto.map((item) => {
      const row = item as unknown as Record<string, unknown>
      const nf = String(row.nf ?? row.nro_nf ?? row.nota_fiscal ?? '').trim()
      const dataNf = formatarData(row.data_nf)
      if (!nf && dataNf === '—') return ''
      return `NF ${nf || '—'}${dataNf !== '—' ? ` - ${dataNf}` : ''}`
    }).filter(Boolean)

    const pesoBruto = itensManifesto.reduce((acc, item) => acc + (item.peso ?? 0), 0)
    const valorMercadoria = itensManifesto.reduce((acc, item) => {
      const row = item as unknown as Record<string, unknown>
      const valor = Number(row.valor_mercadoria ?? row.vlr_merc ?? 0)
      return acc + (Number.isFinite(valor) ? valor : 0)
    }, 0)
    const dataChegada = juntarValores(coletarValores(itensManifesto, ['data_chegada']).map((v) => formatarData(v)))
    const dataDescarga = juntarValores(coletarValores(itensManifesto, ['data_descarga', 'data_des']).map((v) => formatarData(v)))

    return {
      linhaRota: juntarValores(linhaRota),
      remetente: juntarValores(remetentes),
      nfData: juntarValores(nfsDatas),
      destinatario: juntarValores(destinatarios),
      cidade: juntarValores(cidades),
      documento: juntarValores(docs),
      pesoBruto: pesoBruto > 0 ? formatarPeso(pesoBruto) : '—',
      pesoKg: pesoBruto > 0 ? formatarPeso(pesoBruto) : '—',
      valorMercadoria: valorMercadoria > 0 ? formatarMoeda(valorMercadoria) : '—',
      tipoCarga: juntarValores(tipoCarga),
      dataChegada,
      dataDescarga,
      senhaSar: juntarValores(senhaSar),
      atendente: juntarValores(atendente),
    }
  }, [itensManifesto])

  const resumoRemanescentes = useMemo(() => {
    const total = remanescentesNormalizados.length
    const roteirizaveis = remanescentesNormalizados.filter((item) => item.tipo_normalizado === 'roteirizavel_saldo_final')
    const naoRoteirizaveis = remanescentesNormalizados.filter((item) => item.tipo_normalizado === 'nao_roteirizavel_triagem')
    const pesoTotal = remanescentesNormalizados.reduce((acc, item) => acc + (item.peso_calculado ?? 0), 0)
    const kmMedio = total > 0
      ? remanescentesNormalizados.reduce((acc, item) => acc + (item.distancia_rodoviaria_est_km ?? 0), 0) / total
      : 0
    return { total, roteirizaveis: roteirizaveis.length, naoRoteirizaveis: naoRoteirizaveis.length, pesoTotal, kmMedio }
  }, [remanescentesNormalizados])

  const opcoesMesorregiao = useMemo(
    () => Array.from(new Set(remanescentesNormalizados.map((item) => textoSeguro(item.mesorregiao)).filter((v) => v !== '-'))).sort(),
    [remanescentesNormalizados],
  )

  const opcoesSubregiao = useMemo(
    () => Array.from(new Set(remanescentesNormalizados.map((item) => textoSeguro(item.subregiao)).filter((v) => v !== '-'))).sort(),
    [remanescentesNormalizados],
  )

  const opcoesMotivo = useMemo(
    () => Array.from(new Set(remanescentesNormalizados.map((item) => textoSeguro(item.motivo)).filter((v) => v !== '-'))).slice(0, 50),
    [remanescentesNormalizados],
  )

  const remanescentesFiltrados = useMemo(() => {
    const termo = buscaRemanescentes.toLowerCase().trim()
    return remanescentesNormalizados
      .filter((item) => subabaRemanescentes === 'todos' || item.tipo_normalizado === subabaRemanescentes)
      .filter((item) => filtroTipoRemanescente === 'todos' || item.tipo_normalizado === filtroTipoRemanescente)
      .filter((item) => filtroMesorregiao === 'todos' || textoSeguro(item.mesorregiao) === filtroMesorregiao)
      .filter((item) => filtroSubregiao === 'todos' || textoSeguro(item.subregiao) === filtroSubregiao)
      .filter((item) => filtroMotivo === 'todos' || textoSeguro(item.motivo) === filtroMotivo)
      .filter((item) => {
        if (!termo) return true
        const indexavel = [
          item.nro_documento,
          item.destinatario,
          item.cidade,
        ].map((value) => String(value ?? '').toLowerCase())
        return indexavel.some((valor) => valor.includes(termo))
      })
  }, [buscaRemanescentes, filtroMesorregiao, filtroMotivo, filtroSubregiao, filtroTipoRemanescente, remanescentesNormalizados, subabaRemanescentes])

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
                  <th className="text-right px-3 py-2 font-semibold text-gray-600 whitespace-nowrap w-[150px]">KM Médio/Manifesto</th>
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
                      <td className="px-3 py-2 text-right text-gray-700">{r.ocupacao_media_percentual != null ? formatarPercentual(r.ocupacao_media_percentual) : '—'}</td>
                      <td className="px-3 py-2 text-right text-gray-700 whitespace-nowrap">{formatarKm((r.total_manifestos || 0) > 0 ? (r.km_total_frota || 0) / r.total_manifestos : 0)}</td>
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
              <div className="flex flex-wrap items-center gap-2 text-[11px] text-gray-600">
                {(['exclusivo', 'agendada', 'restricao', 'multipla'] as EspecificidadeVisual[]).map((tipo) => (
                  <span key={tipo} className={`inline-flex items-center gap-1.5 px-2 py-1 rounded-full ${estiloEspecificidade[tipo].badge}`}>
                    <span className={`w-2 h-2 rounded-full ${estiloEspecificidade[tipo].marcador}`} />
                    {tipo === 'exclusivo' ? 'Azul = Exclusivo' : tipo === 'agendada' ? 'Amarelo = Agendada' : tipo === 'restricao' ? 'Verde = Restrição' : 'Vermelho = Múltiplas especificidades'}
                  </span>
                ))}
              </div>
              {manifestosComSinalizacao.length === 0 ? <p className="text-sm text-gray-500">Sem manifestos estruturados para esta rodada.</p> : manifestosComSinalizacao.map(({ manifesto: m, especificidade }) => (
                <button key={m.id} onClick={() => void abrirManifesto(m)} className={`w-full text-left border border-gray-200 rounded-lg p-3 hover:bg-gray-50 relative overflow-hidden ${estiloEspecificidade[especificidade].fundo}`}>
                  <span className={`absolute left-0 top-0 h-full w-1 ${estiloEspecificidade[especificidade].marcador}`} />
                  <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-9 gap-2 text-xs">
                    <div><span className="text-gray-500 block">Manifesto</span><strong>{m.manifesto_id}</strong></div>
                    <div><span className="text-gray-500 block">Entregas</span><strong>{m.qtd_entregas}</strong></div>
                    <div><span className="text-gray-500 block">Clientes</span><strong>{m.qtd_clientes}</strong></div>
                    <div><span className="text-gray-500 block">Peso</span><strong>{m.peso_total.toLocaleString('pt-BR')}</strong></div>
                    <div><span className="text-gray-500 block">KM</span><strong>{m.km_total.toLocaleString('pt-BR')}</strong></div>
                    <div><span className="text-gray-500 block">Ocupação</span><strong>{m.ocupacao.toFixed(1)}%</strong></div>
                    <div><span className="text-gray-500 block">Veículo/Perfil</span><strong>{m.veiculo_perfil || m.veiculo_tipo || '—'}</strong></div>
                    <div><span className="text-gray-500 block">Eixos</span><strong>{m.qtd_eixos ?? '—'}</strong></div>
                    <div><span className="text-gray-500 block">Frete mínimo</span><strong>{formatarMoeda(m.frete_minimo)}</strong></div>
                    <div><span className="text-gray-500 block">Sinalização</span><span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] ${estiloEspecificidade[especificidade].badge}`}>{estiloEspecificidade[especificidade].legenda}</span></div>
                  </div>
                </button>
              ))}

            </div>
          )}

          {!detalhesLoading && tabAtiva === 'remanescentes' && (
            <div className="space-y-3">
              <div className="grid grid-cols-1 sm:grid-cols-2 xl:grid-cols-5 gap-2">
                <div className="p-3 rounded-lg bg-gray-50 text-xs"><div className="text-gray-500">Total remanescentes</div><strong>{resumoRemanescentes.total}</strong></div>
                <div className="p-3 rounded-lg bg-blue-50 text-xs"><div className="text-gray-500">Roteirizáveis não atendidas</div><strong>{resumoRemanescentes.roteirizaveis}</strong></div>
                <div className="p-3 rounded-lg bg-amber-50 text-xs"><div className="text-gray-500">Não roteirizáveis na triagem</div><strong>{resumoRemanescentes.naoRoteirizaveis}</strong></div>
                <div className="p-3 rounded-lg bg-gray-50 text-xs"><div className="text-gray-500">Peso total remanescente</div><strong>{formatarNumeroCurto(resumoRemanescentes.pesoTotal, 'kg')}</strong></div>
                <div className="p-3 rounded-lg bg-gray-50 text-xs"><div className="text-gray-500">KM médio remanescente</div><strong>{formatarNumeroCurto(resumoRemanescentes.kmMedio, 'km')}</strong></div>
              </div>

              <div className="flex flex-wrap gap-2">
                {[
                  { key: 'todos', label: `Todos (${resumoRemanescentes.total})` },
                  { key: 'roteirizavel_saldo_final', label: `Roteirizáveis não atendidas (${resumoRemanescentes.roteirizaveis})` },
                  { key: 'nao_roteirizavel_triagem', label: `Não roteirizáveis na triagem (${resumoRemanescentes.naoRoteirizaveis})` },
                ].map((aba) => (
                  <button
                    key={aba.key}
                    className={`px-3 py-1.5 rounded-lg text-xs ${subabaRemanescentes === aba.key ? 'bg-brand-100 text-brand-800' : 'bg-gray-100 text-gray-700'}`}
                    onClick={() => setSubabaRemanescentes(aba.key as TipoRemanescenteTab)}
                  >
                    {aba.label}
                  </button>
                ))}
              </div>

              <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-5 gap-2 text-xs">
                <input className="input text-xs h-9" placeholder="Buscar documento, cliente ou cidade" value={buscaRemanescentes} onChange={(e) => setBuscaRemanescentes(e.target.value)} />
                <select className="input text-xs h-9" value={filtroTipoRemanescente} onChange={(e) => setFiltroTipoRemanescente(e.target.value as typeof filtroTipoRemanescente)}>
                  <option value="todos">Tipo: Todos</option>
                  <option value="roteirizavel_saldo_final">Roteirizável não atendida</option>
                  <option value="nao_roteirizavel_triagem">Não roteirizável na triagem</option>
                </select>
                <select className="input text-xs h-9" value={filtroMesorregiao} onChange={(e) => setFiltroMesorregiao(e.target.value)}>
                  <option value="todos">Mesorregião: Todas</option>
                  {opcoesMesorregiao.map((item) => <option key={item} value={item}>{item}</option>)}
                </select>
                <select className="input text-xs h-9" value={filtroSubregiao} onChange={(e) => setFiltroSubregiao(e.target.value)}>
                  <option value="todos">Sub-região: Todas</option>
                  {opcoesSubregiao.map((item) => <option key={item} value={item}>{item}</option>)}
                </select>
                <select className="input text-xs h-9" value={filtroMotivo} onChange={(e) => setFiltroMotivo(e.target.value)}>
                  <option value="todos">Motivo: Todos</option>
                  {opcoesMotivo.map((item) => <option key={item} value={item}>{item}</option>)}
                </select>
              </div>

              <div className="max-h-[360px] overflow-y-auto overflow-x-auto rounded-lg border">
                <table className="min-w-[1400px] text-[11px] w-full">
                  <thead>
                    {subabaRemanescentes === 'nao_roteirizavel_triagem' && (
                      <tr className="text-left border-b sticky top-0 z-10 bg-white">
                        {['Documento', 'Cliente', 'Cidade/UF', 'Peso', 'KM', 'Mesorregião', 'Sub-região', 'Status triagem', 'Motivo triagem', 'Etapa'].map((h) => <th key={h} className="px-2 py-1.5">{h}</th>)}
                      </tr>
                    )}
                    {subabaRemanescentes === 'roteirizavel_saldo_final' && (
                      <tr className="text-left border-b sticky top-0 z-10 bg-white">
                        {['Documento', 'Cliente', 'Cidade/UF', 'Peso', 'KM', 'Mesorregião', 'Sub-região', 'Corredor', 'Índice corredor', 'Motivo M6.2 detalhado', 'Motivo M6.2 final', 'Motivo M5.4', 'Motivo M5.3', 'Motivo exibido', 'Etapa'].map((h) => <th key={h} className="px-2 py-1.5">{h}</th>)}
                      </tr>
                    )}
                    {subabaRemanescentes === 'todos' && (
                      <tr className="text-left border-b sticky top-0 z-10 bg-white">
                        {['Tipo', 'Documento', 'Cliente', 'Cidade/UF', 'Peso', 'KM', 'Mesorregião', 'Sub-região', 'Motivo exibido', 'Etapa'].map((h) => <th key={h} className="px-2 py-1.5">{h}</th>)}
                      </tr>
                    )}
                  </thead>
                  <tbody>
                    {remanescentesFiltrados.map((r) => (
                      <tr key={r.id} className="border-b">
                        {subabaRemanescentes === 'nao_roteirizavel_triagem' && (
                          <>
                            <td className="px-2 py-1.5">{textoSeguro(r.nro_documento)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.destinatario)}>{textoSeguro(r.destinatario)}</td>
                            <td className="px-2 py-1.5">{formatarCidadeUf(r.cidade, r.uf)}</td>
                            <td className="px-2 py-1.5">{formatarNumeroCurto(r.peso_calculado, 'kg')}</td>
                            <td className="px-2 py-1.5">{formatarNumeroCurto(r.distancia_rodoviaria_est_km, 'km')}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.mesorregiao)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.subregiao)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.status_triagem)}>{textoSeguro(r.status_triagem)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.motivo_triagem)}>{textoSeguro(r.motivo_triagem)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.etapa_origem)}</td>
                          </>
                        )}
                        {subabaRemanescentes === 'roteirizavel_saldo_final' && (
                          <>
                            <td className="px-2 py-1.5">{textoSeguro(r.nro_documento)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.destinatario)}>{textoSeguro(r.destinatario)}</td>
                            <td className="px-2 py-1.5">{formatarCidadeUf(r.cidade, r.uf)}</td>
                            <td className="px-2 py-1.5">{formatarNumeroCurto(r.peso_calculado, 'kg')}</td>
                            <td className="px-2 py-1.5">{formatarNumeroCurto(r.distancia_rodoviaria_est_km, 'km')}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.mesorregiao)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.subregiao)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.corredor_30g)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.corredor_30g_idx)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.motivo_detalhado_m6_2)}>{textoSeguro(r.motivo_detalhado_m6_2)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.motivo_final_remanescente_m6_2)}>{textoSeguro(r.motivo_final_remanescente_m6_2)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.motivo_final_remanescente_m5_4)}>{textoSeguro(r.motivo_final_remanescente_m5_4)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.motivo_final_remanescente_m5_3)}>{textoSeguro(r.motivo_final_remanescente_m5_3)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.motivo)}>{textoSeguro(r.motivo)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.etapa_origem)}</td>
                          </>
                        )}
                        {subabaRemanescentes === 'todos' && (
                          <>
                            <td className="px-2 py-1.5">
                              <span className={`px-2 py-0.5 rounded-full text-[10px] ${r.tipo_normalizado === 'roteirizavel_saldo_final' ? 'bg-blue-100 text-blue-800' : 'bg-amber-100 text-amber-800'}`}>
                                {r.tipo_normalizado === 'roteirizavel_saldo_final' ? 'Roteirizável não atendida' : 'Não roteirizável'}
                              </span>
                            </td>
                            <td className="px-2 py-1.5">{textoSeguro(r.nro_documento)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.destinatario)}>{textoSeguro(r.destinatario)}</td>
                            <td className="px-2 py-1.5">{formatarCidadeUf(r.cidade, r.uf)}</td>
                            <td className="px-2 py-1.5">{formatarNumeroCurto(r.peso_calculado, 'kg')}</td>
                            <td className="px-2 py-1.5">{formatarNumeroCurto(r.distancia_rodoviaria_est_km, 'km')}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.mesorregiao)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.subregiao)}</td>
                            <td className="px-2 py-1.5 max-w-[180px] truncate" title={textoSeguro(r.motivo)}>{textoSeguro(r.motivo)}</td>
                            <td className="px-2 py-1.5">{textoSeguro(r.etapa_origem)}</td>
                          </>
                        )}
                      </tr>
                    ))}
                    {remanescentesFiltrados.length === 0 && (
                      <tr>
                        <td className="px-2 py-2 text-gray-400" colSpan={subabaRemanescentes === 'todos' ? 10 : subabaRemanescentes === 'roteirizavel_saldo_final' ? 15 : 10}>
                          Sem remanescentes para os filtros selecionados.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {!detalhesLoading && tabAtiva === 'estatisticas' && estatisticasComposicao && (
            <div className="space-y-4 text-sm">
              <div className="space-y-2">
                <h4 className="text-sm font-semibold text-gray-700">Resumo geral</h4>
                <div className="grid md:grid-cols-4 gap-3">
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total carteira</div><strong>{estatisticasComposicao.totalCarteira}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Itens roteirizados</div><strong>{estatisticasComposicao.itensRoteirizados}</strong></div>
                  <div className="p-3 rounded-lg bg-amber-50"><div className="text-gray-600">Não roteirizados total</div><strong>{estatisticasComposicao.naoRoteirizadosTotal}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Total manifestos</div><strong>{estatisticasComposicao.totalManifestos}</strong></div>
                </div>
              </div>

              <div className="space-y-2">
                <h4 className="text-sm font-semibold text-gray-700">Performance da roteirização</h4>
                <div className="grid md:grid-cols-4 gap-3">
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Ocupação média</div><strong>{estatisticasComposicao.ocupacaoMedia.toFixed(1)}%</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">KM total</div><strong>{estatisticasComposicao.kmTotal.toLocaleString('pt-BR')} km</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">KM médio por manifesto</div><strong>{estatisticasComposicao.kmMedioManifesto.toFixed(2)} km</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Itens por manifesto</div><strong>{estatisticasComposicao.itensPorManifesto.toFixed(2)}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Tempo de execução</div><strong>{Math.trunc(estatisticasComposicao.tempoExecucaoMs).toLocaleString('pt-BR')} ms</strong></div>
                  <div className="p-3 rounded-lg bg-emerald-50"><div className="text-gray-600">Taxa de roteirização</div><strong>{estatisticasComposicao.taxaRoteirizacao.toFixed(1)}%</strong></div>
                  <div className="p-3 rounded-lg bg-blue-50"><div className="text-gray-600">Aproveitamento do universo roteirizável</div><strong>{estatisticasComposicao.aproveitamentoUniversoRoteirizavel.toFixed(1)}%</strong></div>
                </div>
              </div>

              <div className="space-y-2">
                <h4 className="text-sm font-semibold text-gray-700">Composição dos não roteirizados</h4>
                <div className="grid md:grid-cols-4 gap-3">
                  <div className="p-3 rounded-lg bg-amber-50"><div className="text-gray-600">Não roteirizados total</div><strong>{estatisticasComposicao.naoRoteirizadosTotal}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Saldo final roteirizável</div><strong>{estatisticasComposicao.saldoFinalRoteirizavel}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Não roteirizáveis na triagem</div><strong>{estatisticasComposicao.naoRoteirizaveisTriagem}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Agendamento futuro</div><strong>{estatisticasComposicao.agendamentoFuturo}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Aguardando agendamento</div><strong>{estatisticasComposicao.aguardandoAgendamento}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Exceções de triagem</div><strong>{estatisticasComposicao.excecoesTriagem}</strong></div>
                  <div className="p-3 rounded-lg bg-gray-50"><div className="text-gray-500">Outros não classificados</div><strong>{estatisticasComposicao.outrosNaoClassificados}</strong></div>
                </div>
                {estatisticasComposicao.diferencaClassificacao === 0 ? (
                  <div className="inline-flex items-center px-2.5 py-1 rounded-full text-xs font-medium bg-green-100 text-green-700">
                    Conta fechada
                  </div>
                ) : (
                  <div className="p-3 rounded-lg border border-red-200 bg-red-50 text-red-700">
                    Diferença de classificação: {estatisticasComposicao.diferencaClassificacao}
                  </div>
                )}
              </div>
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
              <div><span className="text-gray-500 block">KM total</span><strong>{formatarKm(manifestoAtivo.km_total)}</strong></div>
              <div><span className="text-gray-500 block">Peso total</span><strong>{formatarPeso(manifestoAtivo.peso_total)} kg</strong></div>
              <div><span className="text-gray-500 block">Qtd. entregas</span><strong>{manifestoAtivo.qtd_entregas}</strong></div>
              <div><span className="text-gray-500 block">Frete mínimo</span><strong>{formatarMoeda(manifestoAtivo.frete_minimo)}</strong></div>
            </div>

            {manifestoLoading ? <div className="text-sm text-gray-500">Carregando entregas...</div> : (
              <>
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead className="border-b"><tr><th className="text-left py-2">Tipo</th><th className="text-left py-2">Sequência</th><th className="text-left">Documento</th><th className="text-left">Destinatário</th><th className="text-left">Cidade</th><th className="text-left">UF</th><th className="text-left">Peso</th><th className="text-left">Janela</th><th className="text-left">Ações</th></tr></thead>
                    <tbody>
                      {itensComSinalizacao.map(({ item, especificidade }, index) => (
                        <tr key={item.id} className={`border-b ${estiloEspecificidade[especificidade].fundo}`}>
                          <td className="py-2">
                            <span className={`inline-flex items-center rounded-full px-2 py-0.5 text-[10px] ${estiloEspecificidade[especificidade].badge}`}>
                              {estiloEspecificidade[especificidade].legenda}
                            </span>
                          </td>
                          <td className="py-2">{item.sequencia}</td>
                          <td>{item.nro_documento || '—'}</td>
                          <td>{item.destinatario || '—'}</td>
                          <td>{item.cidade || '—'}</td>
                          <td>{item.uf || '—'}</td>
                          <td>{item.peso != null ? `${formatarPeso(item.peso)} kg` : '—'}</td>
                          <td>{item.inicio_entrega || '—'}{item.fim_entrega ? ` - ${item.fim_entrega}` : ''}</td>
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
                      ['Linha / rota', resumoOperacional.linhaRota !== '—' ? resumoOperacional.linhaRota : (manifestoAtivo.tipo_manifesto || manifestoAtivo.manifesto_id || '—')],
                      ['Remetente', resumoOperacional.remetente],
                      ['N. Fiscal(s)/Data', resumoOperacional.nfData],
                      ['Destinatário', resumoOperacional.destinatario],
                      ['Cidade', resumoOperacional.cidade],
                      ['Doc CTRC / documento', resumoOperacional.documento],
                      ['Peso bruto', resumoOperacional.pesoBruto],
                      ['Peso KG', resumoOperacional.pesoKg],
                      ['Valor da mercadoria', resumoOperacional.valorMercadoria],
                      ['Tipo de carga', resumoOperacional.tipoCarga !== '—' ? resumoOperacional.tipoCarga : (manifestoAtivo.tipo_manifesto || '—')],
                      ['Data chegada', resumoOperacional.dataChegada],
                      ['Data descarga', resumoOperacional.dataDescarga],
                      ['Senha do SAR', resumoOperacional.senhaSar],
                      ['Atendente', resumoOperacional.atendente !== '—' ? resumoOperacional.atendente : (rodadaSelecionada.usuario_nome || '—')],
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
                    <h4 className="text-sm font-semibold text-amber-900">Agendamentos ({itensAgendados.length})</h4>
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
                                <td>{formatarData(extra.data_agenda ?? item.inicio_entrega)}</td>
                                <td>{item.fim_entrega || item.inicio_entrega || '—'}</td>
                                <td>{`Agendamento para ${formatarData(extra.data_agenda ?? item.inicio_entrega)}${(item.fim_entrega || item.inicio_entrega) ? ` - ${item.fim_entrega || item.inicio_entrega}` : ''}`}</td>
                              </tr>
                            )
                          })}
                        </tbody>
                      </table>
                    </div>
                  </div>
                ) : null}
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
