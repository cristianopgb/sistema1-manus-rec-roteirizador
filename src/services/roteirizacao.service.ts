import { supabase } from '@/lib/supabase'
import { anttService } from './antt.service'
import {
  buildMotor2Url,
  getMotor2BaseUrl,
  MOTOR_2_HEALTH_PATH,
  MOTOR_2_ROTEIRIZAR_PATH,
} from '@/config/motor2'
import {
  PayloadMotor, RespostaMotor, ManifestoComFrete,
  RodadaRoteirizacao, FiltrosRoteirizacao, CarteiraCarga,
  Filial, FiltrosCarteira, ConfiguracaoFrotaItem, CarteiraCargaContratoMotor,
  ManifestoRoteirizacaoDetalhe, ManifestoItemRoteirizacao, RemanescenteRoteirizacao, EstatisticasRoteirizacao,
  RotaManifestoGoogle, RotaManifestoParadaGoogle
} from '@/types'
import { normalizeHorarioJanela } from '@/lib/time-normalizers'
import {
  normalizeAgendam,
  normalizeDataDesDataNF,
  normalizeDle,
} from '@/lib/date-normalizers'

const CAMPOS_MULTISELECT: Array<keyof Pick<FiltrosCarteira, 'filial_r' | 'uf' | 'destin' | 'cidade' | 'tomad' | 'mesoregiao' | 'prioridade' | 'restricao_veiculo'>> = [
  'filial_r',
  'uf',
  'destin',
  'cidade',
  'tomad',
  'mesoregiao',
  'prioridade',
  'restricao_veiculo',
]

const CAMPOS_DATA_RANGE: Array<{ de: keyof FiltrosCarteira; ate: keyof FiltrosCarteira; coluna: string }> = [
  { de: 'agendam_de', ate: 'agendam_ate', coluna: 'agendam' },
  { de: 'dle_de', ate: 'dle_ate', coluna: 'dle' },
  { de: 'data_des_de', ate: 'data_des_ate', coluna: 'data_des' },
  { de: 'data_nf_de', ate: 'data_nf_ate', coluna: 'data_nf' },
]

const MOTOR_2_ROTEIRIZAR_TIMEOUT_MS = 900_000
const MAX_CONCORRENCIA_ROTAS_GOOGLE = 2
const MENSAGEM_MANUAL_FRETE = 'Rota Google não calculada. Frete mínimo deve ser calculado manualmente.'



type RejeicaoPersistencia = {
  rodada_id: string
  grupo: string
  indice: number | null
  motivo: string
  severidade: 'info' | 'warning' | 'erro_nao_fatal' | 'erro_fatal'
  item_json: Record<string, unknown> | null
  contexto_json: Record<string, unknown> | null
}

const getFirstValue = (item: Record<string, unknown>, keys: string[]): unknown => {
  for (const key of keys) {
    const value = item[key]
    if (value !== undefined && value !== null && String(value).trim() !== '') return value
  }
  return null
}

const safeMapAndFilter = <TInput extends Record<string, unknown>, TOutput>({
  grupo,
  itens,
  rodadaId,
  contexto,
  mapper,
}: {
  grupo: string
  itens: TInput[]
  rodadaId: string
  contexto?: Record<string, unknown>
  mapper: (item: TInput, index: number) => { value: TOutput | null; reason?: string }
}): { validos: TOutput[]; rejeitados: RejeicaoPersistencia[] } => {
  const validos: TOutput[] = []
  const rejeitados: RejeicaoPersistencia[] = []
  itens.forEach((item, index) => {
    try {
      const result = mapper(item, index)
      if (result.value) {
        validos.push(result.value)
        return
      }
      const motivo = result.reason ?? 'Item sem campos mínimos para persistência'
      console.warn('[PERSISTENCIA] item rejeitado', { rodadaId, grupo, indice: index, motivo, item })
      rejeitados.push({ rodada_id: rodadaId, grupo, indice: index, motivo, severidade: 'warning', item_json: item, contexto_json: contexto ?? null })
    } catch (error) {
      const motivo = error instanceof Error ? error.message : 'Erro inesperado ao mapear item'
      console.warn('[PERSISTENCIA] item rejeitado', { rodadaId, grupo, indice: index, motivo, item })
      rejeitados.push({ rodada_id: rodadaId, grupo, indice: index, motivo, severidade: 'warning', item_json: item, contexto_json: contexto ?? null })
    }
  })
  return { validos, rejeitados }
}

const isRotaGoogleValidaParaFrete = (rotaGoogle: RotaManifestoGoogle | null | undefined): boolean => Boolean(
  rotaGoogle &&
  ['ok', 'reutilizada'].includes(String(rotaGoogle.google_status)) &&
  Number(rotaGoogle.km_google_maps) > 0,
)

type RotaManifestoGoogleInput = {
  rodada_id: string
  manifesto_id: string
  manifesto_db_id: string | null
  rota_hash: string
  origem_latitude: number
  origem_longitude: number
  destino_latitude: number | null
  destino_longitude: number | null
  paradas_json: RotaManifestoParadaGoogle[]
  qtd_paradas: number
  km_estimado_motor: number | null
  google_status: RotaManifestoGoogle['google_status']
  google_erro: string | null
  fonte: string
}

type FreteStatus = 'pendente' | 'calculado' | 'erro' | 'sem_tabela_antt' | 'sem_qtd_eixos' | 'sem_km_google' | 'calculo_manual_necessario'

const normalizarCarteiraItem = (item: any): CarteiraCarga => {
  const { id, upload_id, status_validacao, erro_validacao, created_at, dados_originais_json, ...rest } = item
  return ({
    ...rest,
    _carteira_item_id: id,
    _upload_id: upload_id,
    _status_validacao: status_validacao,
    _erro_validacao: erro_validacao,
    _created_at: created_at,
    _dados_originais: dados_originais_json,
  }) as CarteiraCarga
}

const aplicarFiltrosCarteira = (query: any, filtros?: FiltrosCarteira) => {
  let filtered = query
  const filtrosAtivos = filtros

  if (!filtrosAtivos) return filtered

  for (const campo of CAMPOS_MULTISELECT) {
    const valores = filtrosAtivos[campo]
    if (Array.isArray(valores) && valores.length > 0) {
      filtered = filtered.in(campo, valores)
    }
  }

  if (filtrosAtivos.carro_dedicado === 'sim') {
    filtered = filtered.eq('carro_dedicado', true)
  } else if (filtrosAtivos.carro_dedicado === 'nao') {
    filtered = filtered.eq('carro_dedicado', false)
  }

  for (const { de, ate, coluna } of CAMPOS_DATA_RANGE) {
    if (filtrosAtivos[de]) filtered = filtered.gte(coluna, filtrosAtivos[de])
    if (filtrosAtivos[ate]) filtered = filtered.lte(coluna, filtrosAtivos[ate])
  }

  return filtered
}

const isLinhaCarteiraSemConteudo = (row: Record<string, unknown>): boolean => {
  const campos = Object.entries(row).filter(([key]) => !key.startsWith('_') && key !== 'linha_numero')
  if (!campos.length) return true
  return campos.every(([, value]) => {
    if (value === null || value === undefined) return true
    return String(value).trim() === ''
  })
}

const toIsoCompleto = (value: string): string => {
  const parsed = new Date(value)
  if (Number.isNaN(parsed.getTime())) return new Date().toISOString()
  return parsed.toISOString()
}

const mapVeiculoToMotor = (veiculo: Record<string, unknown>) => ({
  id: String(veiculo.id ?? ''),
  filial_id: String(veiculo.filial_id ?? ''),
  tipo: typeof veiculo.tipo === 'string' ? veiculo.tipo : null,
  perfil: typeof veiculo.tipo === 'string' ? veiculo.tipo : null,
  placa: typeof veiculo.placa === 'string' ? veiculo.placa : null,
  capacidade_peso_kg: typeof veiculo.capacidade_peso_kg === 'number' ? veiculo.capacidade_peso_kg : 0,
  capacidade_vol_m3: typeof (veiculo.capacidade_vol_m3 ?? veiculo.capacidade_volume_m3) === 'number'
    ? (veiculo.capacidade_vol_m3 ?? veiculo.capacidade_volume_m3) as number
    : 0,
  qtd_eixos: typeof (veiculo.qtd_eixos ?? veiculo.num_eixos) === 'number'
    ? (veiculo.qtd_eixos ?? veiculo.num_eixos) as number
    : 0,
  max_km_distancia: typeof veiculo.max_km_distancia === 'number' ? veiculo.max_km_distancia : null,
  max_entregas: typeof veiculo.max_entregas === 'number' ? veiculo.max_entregas : null,
  ocupacao_minima_perc: typeof veiculo.ocupacao_minima_perc === 'number' ? veiculo.ocupacao_minima_perc : null,
  ocupacao_maxima_perc: typeof veiculo.ocupacao_maxima_perc === 'number' ? veiculo.ocupacao_maxima_perc : null,
  ativo: veiculo.ativo === true,
})

const mapCarteiraItemToMotorContract = (item: CarteiraCarga, index: number): CarteiraCargaContratoMotor => {
  const agendamOriginal = getFirstValue(item as unknown as Record<string, unknown>, ['agendam', 'Agendam.', 'data_agenda', 'agenda'])
  const dleOriginal = getFirstValue(item as unknown as Record<string, unknown>, ['dle', 'D.L.E.', 'data_leadtime', 'data_limite', 'data_limite_entrega'])
  const inicioEntregaNormalizado = normalizeHorarioJanela(item.inicio_entrega)
  const fimEnNormalizado = normalizeHorarioJanela(item.fim_entrega)
  const dataDesNormalizada = normalizeDataDesDataNF(item.data_des)
  const dataNFNormalizada = normalizeDataDesDataNF(item.data_nf)
  const dleNormalizada = normalizeDle(dleOriginal)
  const agendamNormalizada = normalizeAgendam(agendamOriginal)
  const peso = toPayloadNumber(item.peso)
  const pesoCalculo = toPayloadNumber(item.peso_calculo)
  const valorMercadoria = toPayloadNumber(item.vlr_merc)
  const quantidade = toPayloadNumber(item.qtd)
  const pesoCubico = toPayloadNumber(item.peso_cubico)
  const latitude = toPayloadNumber(item.latitude)
  const longitude = toPayloadNumber(item.longitude)
  if (import.meta.env.DEV) {
    console.log('[PAYLOAD] Inicio Ent. original:', item.inicio_entrega)
    console.log('[PAYLOAD] Inicio Ent. normalizado:', inicioEntregaNormalizado)
    console.log('[PAYLOAD] Fim En original:', item.fim_entrega)
    console.log('[PAYLOAD] Fim En normalizado:', fimEnNormalizado)
    if (item.inicio_entrega !== null && item.inicio_entrega !== undefined && inicioEntregaNormalizado === null) {
      console.log('[PAYLOAD] Inicio Ent. inválido convertido para null no índice:', index, 'item_id:', item._carteira_item_id)
    }
    if (item.fim_entrega !== null && item.fim_entrega !== undefined && fimEnNormalizado === null) {
      console.log('[PAYLOAD] Fim En inválido convertido para null no índice:', index, 'item_id:', item._carteira_item_id)
    }
    if (index === 0) {
      console.log('[PAYLOAD DATAS] exemplo item carteira:', {
        nroDocumento: item.nro_doc,
        dataDesOriginal: item.data_des,
        dataDesNormalizada,
        dataNFOriginal: item.data_nf,
        dataNFNormalizada,
        dleOriginal,
        dleNormalizada,
        agendamOriginal,
        agendamNormalizada,
      })
    }
    if (agendamOriginal !== null && agendamOriginal !== undefined) {
      console.log(`[DATA PAYLOAD] campo=Agendam. original=${String(agendamOriginal)} normalizado=${agendamNormalizada ?? 'null'}`)
    }
    if (dleOriginal !== null && dleOriginal !== undefined) {
      console.log(`[DATA PAYLOAD] campo=D.L.E. original=${String(dleOriginal)} normalizado=${dleNormalizada ?? 'null'}`)
    }
  }

  return ({
  'Filial R': item.filial_r,
  Romane: item.romane,
  'Filial D': item.filial_d,
  'Série': item.serie,
  'Nro Doc.': item.nro_doc,
  'Data Des': dataDesNormalizada,
  'Data NF': dataNFNormalizada,
  'D.L.E.': dleNormalizada,
  'Agendam.': agendamNormalizada,
  Palet: item.palet,
  Conf: item.conf,
  Peso: peso,
  'Vlr.Merc.': valorMercadoria,
  'Qtd.': quantidade,
  'Peso Cub.': pesoCubico,
  Classif: item.classif,
  Tomad: item.tomad,
  Destin: item.destin,
  Bairro: item.bairro,
  Cidad: item.cidade,
  UF: item.uf,
  'NF / Serie': item.nf_serie,
  'Tipo Ca': item.tipo_ca,
  'Qtd.NF': item.qtd_nf,
  Mesoregião: item.mesoregiao,
  'Sub-Região': item.sub_regiao,
  'Ocorrências NF': item.ocorrencias_nf,
  Remetente: item.remetente,
  Observação: item.observacao,
  'Ref Cliente': item.ref_cliente,
  'Cidade Dest.': item.cidade_dest,
  Agenda: item.agenda,
  'Tipo Carga': item.tipo_carga,
  'Última Ocorrência': item.ultima_ocorrencia,
  'Status R': item.status_r,
  Latitude: latitude,
  Longitude: longitude,
  'Peso Calculo': pesoCalculo,
  Prioridade: item.prioridade,
  'Restrição Veículo': item.restricao_veiculo,
  'Carro Dedicado': item.carro_dedicado,
  'Inicio Ent.': inicioEntregaNormalizado,
  'Fim En': fimEnNormalizado,
})
}

const extrairMensagemErro = (body: unknown): string => {
  if (!body) return 'Erro de validação sem detalhes'
  if (typeof body === 'string') return body
  if (typeof body === 'object') {
    const maybe = body as Record<string, unknown>
    if (typeof maybe.message === 'string') return maybe.message
    if (typeof maybe.detail === 'string') return maybe.detail
    if (Array.isArray(maybe.detail)) {
      return maybe.detail.map((item) => JSON.stringify(item)).join(' | ')
    }
    if (typeof maybe.erro === 'string') return maybe.erro
  }
  return JSON.stringify(body)
}

const toNumber = (value: unknown, fallback = 0): number => {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const parsed = Number(value.replace(',', '.'))
    if (Number.isFinite(parsed)) return parsed
  }
  return fallback
}

const normalizarPerfilVeiculo = (valor: unknown): string => {
  return String(valor ?? '')
    .trim()
    .toUpperCase()
    .normalize('NFD')
    .replace(/[\u0300-\u036f]/g, '')
}

const roundCurrency = (valor: number): number => {
  return Math.round((valor + Number.EPSILON) * 100) / 100
}

const pickFirstNumber = (...values: unknown[]): number | null => {
  for (const value of values) {
    if (value === null || value === undefined) continue
    const parsed = toNumber(value, Number.NaN)
    if (Number.isFinite(parsed)) return parsed
  }
  return null
}

const toPayloadNumber = (value: unknown): number | null => {
  if (value === null || value === undefined) return null
  if (typeof value === 'number') return Number.isFinite(value) ? value : null
  if (typeof value !== 'string') return null
  const text = value.trim()
  if (!text) return null
  const sanitized = text.replace(/[^\d,.-]/g, '')
  if (!sanitized) return null
  const lastComma = sanitized.lastIndexOf(',')
  const lastDot = sanitized.lastIndexOf('.')
  let normalized = sanitized
  if (lastComma >= 0 && lastDot >= 0) {
    normalized = lastComma > lastDot
      ? sanitized.replace(/\./g, '').replace(',', '.')
      : sanitized.replace(/,/g, '')
  } else if (lastComma >= 0) {
    normalized = sanitized.replace(',', '.')
  }
  const parsed = Number(normalized)
  return Number.isFinite(parsed) ? parsed : null
}

const roundCoordinate = (value: number): number => Number(value.toFixed(6))

const isValidCoordinate = (latitude: number | null | undefined, longitude: number | null | undefined): boolean => (
  typeof latitude === 'number'
  && typeof longitude === 'number'
  && Number.isFinite(latitude)
  && Number.isFinite(longitude)
  && latitude >= -90
  && latitude <= 90
  && longitude >= -180
  && longitude <= 180
)

const gerarHashRotaGoogle = (
  origem: { latitude: number; longitude: number },
  paradas: Array<{ latitude: number; longitude: number }>,
): string => {
  const origemNorm = `${roundCoordinate(origem.latitude)},${roundCoordinate(origem.longitude)}`
  const paradasNorm = paradas.map((p) => `${roundCoordinate(p.latitude)},${roundCoordinate(p.longitude)}`).join('|')
  const base = `${origemNorm}->${paradasNorm}`
  let hash = 0
  for (let i = 0; i < base.length; i += 1) {
    hash = ((hash << 5) - hash) + base.charCodeAt(i)
    hash |= 0
  }
  return `rg_${Math.abs(hash).toString(16)}_${base.length}`
}

const consolidarParadasManifesto = (itens: ManifestoItemRoteirizacao[]): RotaManifestoParadaGoogle[] => {
  const ordenados = [...itens].sort((a, b) => (a.sequencia ?? 0) - (b.sequencia ?? 0))
  const mapa = new Map<string, RotaManifestoParadaGoogle>()
  const ordemChaves: string[] = []
  for (const item of ordenados) {
    if (!isValidCoordinate(item.latitude, item.longitude)) continue
    const lat = roundCoordinate(item.latitude as number)
    const lng = roundCoordinate(item.longitude as number)
    const cidade = String(item.cidade ?? '').trim().toUpperCase() || null
    const uf = String(item.uf ?? '').trim().toUpperCase() || null
    const chave = `${lat}|${lng}|${cidade ?? ''}|${uf ?? ''}`
    if (!mapa.has(chave)) {
      mapa.set(chave, {
        ordem: 0,
        latitude: Number((item.latitude as number).toFixed(8)),
        longitude: Number((item.longitude as number).toFixed(8)),
        cidade,
        uf,
        destinatarios: [],
        documentos: [],
      })
      ordemChaves.push(chave)
    }
    const parada = mapa.get(chave) as RotaManifestoParadaGoogle
    const destinatario = String(item.destinatario ?? '').trim()
    const documento = String(item.nro_documento ?? '').trim()
    if (destinatario && !parada.destinatarios.includes(destinatario)) parada.destinatarios.push(destinatario)
    if (documento && !parada.documentos.includes(documento)) parada.documentos.push(documento)
  }
  return ordemChaves.map((chave, index) => ({ ...mapa.get(chave) as RotaManifestoParadaGoogle, ordem: index + 1 }))
}

const isMotor2TimeoutError = (err: unknown): boolean => {
  if (!(err instanceof Error)) return false
  const message = err.message.toLowerCase()
  return err.name === 'TimeoutError'
    || err.name === 'AbortError'
    || message.includes('signal timed out')
    || message.includes('timed out')
}

const toText = (value: unknown): string | null => {
  if (value === null || value === undefined) return null
  const text = String(value).trim()
  return text.length ? text : null
}

const extrairMensagemErroRuntime = (erro: unknown): string => {
  if (erro instanceof Error && erro.message) return erro.message
  if (typeof erro === 'string') return erro
  if (erro && typeof erro === 'object') {
    const maybe = erro as Record<string, unknown>
    if (typeof maybe.message === 'string' && maybe.message.trim().length > 0) return maybe.message
  }
  try {
    return JSON.stringify(erro)
  } catch {
    return String(erro)
  }
}

const pickFirstText = (...values: unknown[]): string | null => {
  for (const value of values) {
    const text = toText(value)
    if (text) return text
  }
  return null
}

const toBooleanLike = (value: unknown): boolean => {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  const text = toText(value)?.toLowerCase()
  if (!text) return false
  return ['1', 'true', 'sim', 's', 'yes', 'y', 'ok'].includes(text)
}

const toBooleanKeyword = (value: unknown, keywords: string[]): boolean => {
  if (typeof value === 'boolean') return value
  if (typeof value === 'number') return value !== 0
  const text = toText(value)?.toLowerCase()
  if (!text) return false
  return keywords.includes(text)
}

const hasNonEmptyText = (value: unknown): boolean => {
  if (typeof value !== 'string') return false
  const text = value.trim().toLowerCase()
  if (!text) return false
  return !['nat', 'null', 'undefined', '-', '—', 'na', 'n/a', 'sem agendamento', 'nao agendado'].includes(text)
}

const temSinalizacaoExclusiva = (registro: Record<string, unknown>): boolean => {
  return toBooleanKeyword(registro.exclusivo_flag, ['exclusivo', 'dedicado'])
    || toBooleanKeyword(registro.veiculo_exclusivo_flag, ['exclusivo', 'dedicado'])
    || toBooleanKeyword(registro.veiculo_exclusivo, ['exclusivo', 'dedicado'])
    || toBooleanKeyword(registro.exclusivo, ['exclusivo', 'dedicado'])
    || toBooleanKeyword(registro.carro_dedicado, ['exclusivo', 'dedicado'])
    || toBooleanKeyword(registro.carro_dedicado_flag, ['exclusivo', 'dedicado'])
}

const temSinalizacaoAgendamento = (registro: Record<string, unknown>): boolean => {
  if (toBooleanKeyword(registro.agendada, ['agendado', 'agendada']) || toBooleanKeyword(registro.agendado, ['agendado', 'agendada'])) return true
  const sinalizacaoVisual = toRecord(registro.sinalizacao_visual)
  if (sinalizacaoVisual && toBooleanKeyword(sinalizacaoVisual.agendada, ['agendado', 'agendada'])) return true
  return hasNonEmptyText(registro.data_agenda)
    || hasNonEmptyText(registro.data_agendamento)
    || hasNonEmptyText(registro.dt_agendamento)
    || hasNonEmptyText(registro.data_programada)
    || hasNonEmptyText(registro.agendam)
    || hasNonEmptyText(registro['Agendam.'])
    || hasNonEmptyText(registro.agenda)
}

const temSinalizacaoRestricao = (registro: Record<string, unknown>): boolean => {
  return toBooleanKeyword(registro.restricao, ['restricao', 'restrição'])
    || toBooleanKeyword(registro.restricoes, ['restricao', 'restrição'])
    || toBooleanKeyword(registro.restricao_veiculo, ['restricao', 'restrição'])
    || toBooleanKeyword(registro.cliente_com_restricao, ['restricao', 'restrição'])
    || toBooleanKeyword(registro.tem_restricao, ['restricao', 'restrição'])
    || String(registro.motivo ?? '').toLowerCase().includes('restri')
    || String(registro.status_triagem ?? '').toLowerCase().includes('restri')
}

const toRecord = (value: unknown): Record<string, unknown> | null => {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return null
  return value as Record<string, unknown>
}

const toRecordArray = (value: unknown): Array<Record<string, unknown>> => {
  if (!Array.isArray(value)) return []
  return value
    .map((item) => toRecord(item))
    .filter((item): item is Record<string, unknown> => !!item)
}

const assertArray = (value: unknown, campo: string): Array<Record<string, unknown>> => {
  if (!Array.isArray(value)) {
    throw new Error(`Contrato M7 inválido: campo obrigatório ${campo} ausente ou não é array`)
  }
  return toRecordArray(value)
}

const validarContratoM7 = (resposta: RespostaMotor): void => {
  if (!resposta || typeof resposta !== 'object') {
    throw new Error('Contrato M7 inválido: resposta ausente')
  }
  if (resposta.status !== 'ok') {
    throw new Error(`Contrato M7 inválido: status esperado "ok", recebido "${String(resposta.status)}"`)
  }
  if (resposta.pipeline_real_ate !== 'M7') {
    throw new Error(`Contrato M7 inválido: pipeline_real_ate esperado "M7", recebido "${String(resposta.pipeline_real_ate)}"`)
  }
  assertArray(resposta.manifestos_m7, 'manifestos_m7')
  assertArray(resposta.itens_manifestos_sequenciados_m7, 'itens_manifestos_sequenciados_m7')
  assertArray(resposta.paradas_m7, 'paradas_m7')
  assertArray(resposta.manifestos_sequenciamento_resumo_m7, 'manifestos_sequenciamento_resumo_m7')
  const remanescentes = toRecord(resposta.remanescentes)
  if (!remanescentes) {
    throw new Error('Contrato M7 inválido: campo obrigatório remanescentes ausente ou inválido')
  }
  assertArray(remanescentes.saldo_final_roteirizacao, 'remanescentes.saldo_final_roteirizacao')
  assertArray(remanescentes.nao_roteirizaveis_m3, 'remanescentes.nao_roteirizaveis_m3')
  const resumoExecucao = toRecord(resposta.resumo_execucao)
  if (!resumoExecucao) {
    throw new Error('Contrato M7 inválido: campo obrigatório resumo_execucao ausente ou inválido')
  }
  if (!toRecord(resumoExecucao.tempos_ms)) {
    throw new Error('Contrato M7 inválido: campo obrigatório resumo_execucao.tempos_ms ausente ou inválido')
  }
}

const extrairManifestosM7 = (resposta: RespostaMotor): Array<Record<string, unknown>> => assertArray(resposta.manifestos_m7, 'manifestos_m7')
const extrairItensM7 = (resposta: RespostaMotor): Array<Record<string, unknown>> => assertArray(resposta.itens_manifestos_sequenciados_m7, 'itens_manifestos_sequenciados_m7')
const extrairParadasM7 = (resposta: RespostaMotor): Array<Record<string, unknown>> => assertArray(resposta.paradas_m7, 'paradas_m7')
const extrairResumoSequenciamentoM7 = (resposta: RespostaMotor): Array<Record<string, unknown>> => assertArray(resposta.manifestos_sequenciamento_resumo_m7, 'manifestos_sequenciamento_resumo_m7')
const extrairRemanescentesM7 = (resposta: RespostaMotor): {
  nao_roteirizaveis_m3: Array<Record<string, unknown>>
  saldo_final_roteirizacao: Array<Record<string, unknown>>
} => {
  const remanescentes = toRecord(resposta.remanescentes)
  if (!remanescentes) {
    throw new Error('Contrato M7 inválido: campo obrigatório remanescentes ausente ou inválido')
  }
  return {
    nao_roteirizaveis_m3: assertArray(remanescentes.nao_roteirizaveis_m3, 'remanescentes.nao_roteirizaveis_m3'),
    saldo_final_roteirizacao: assertArray(remanescentes.saldo_final_roteirizacao, 'remanescentes.saldo_final_roteirizacao'),
  }
}

const mapearStatusMotorParaStatusRodada = (
  statusMotor: unknown,
  houveErroPosProcessamento: boolean,
): RodadaRoteirizacao['status'] => {
  if (houveErroPosProcessamento) return 'erro'
  if (statusMotor === 'erro') return 'erro'
  if (statusMotor === 'ok') return 'sucesso'
  return 'sucesso'
}

export const roteirizacaoService = {
  async persistirEstruturaRodada(
    rodadaId: string,
    resposta: RespostaMotor,
    veiculos: Array<{ id: string; perfil?: string | null; tipo?: string | null; qtd_eixos?: number | null; capacidade_peso_kg?: number | null }>
  ): Promise<void> {
    console.log('[PERSISTENCIA] início modo tolerante', { rodadaId })
    validarContratoM7(resposta)
    const manifestosM7 = extrairManifestosM7(resposta)
    const itensM7 = extrairItensM7(resposta)
    const paradasM7 = extrairParadasM7(resposta)
    const resumoSequenciamentoM7 = extrairResumoSequenciamentoM7(resposta)
    const remanescentesM7 = extrairRemanescentesM7(resposta)
    console.log('[CONTRATO M7 VALIDADO]', {
      manifestos_m7: manifestosM7.length,
      itens_manifestos_sequenciados_m7: itensM7.length,
      paradas_m7: paradasM7.length,
      saldo_final_roteirizacao: remanescentesM7.saldo_final_roteirizacao.length,
      nao_roteirizaveis_m3: remanescentesM7.nao_roteirizaveis_m3.length,
    })
    console.log('[PERSISTENCIA M7] antes de salvar', {
      manifestosM7: manifestosM7.length,
      itensM7: itensM7.length,
      paradasM7: paradasM7.length,
      saldoFinal: remanescentesM7.saldo_final_roteirizacao.length,
      naoRoteirizaveisM3: remanescentesM7.nao_roteirizaveis_m3.length,
    })

    const [{ error: payloadManifestoProbeError }, { error: payloadItemProbeError }] = await Promise.all([
      supabase.from('manifestos_roteirizacao').select('id, payload_apoio_json').limit(1),
      supabase.from('manifestos_itens').select('id, payload_apoio_json').limit(1),
    ])
    const inserirPayloadManifesto = !payloadManifestoProbeError
    const inserirPayloadItem = !payloadItemProbeError

    const itensM7PorManifesto = itensM7.reduce<Record<string, Array<Record<string, unknown>>>>((acc, item) => {
      const manifestoId = toText(item.manifesto_id)
      if (!manifestoId) return acc
      if (!acc[manifestoId]) acc[manifestoId] = []
      acc[manifestoId].push(item)
      return acc
    }, {})

    const rejeicoesPersistencia: RejeicaoPersistencia[] = []
    const { validos: registrosManifestos, rejeitados: rejeitadosManifestos } = safeMapAndFilter({
      grupo: 'manifestos_m7',
      itens: manifestosM7,
      rodadaId,
      mapper: (manifestoRaw, index) => {
      const manifestoId = toText(manifestoRaw.manifesto_id) ?? `MANIFESTO_SEM_ID_${String(index + 1).padStart(4, '0')}`
      if (!toText(manifestoRaw.manifesto_id)) {
        console.warn('[PERSISTENCIA] item rejeitado', { rodadaId, grupo: 'manifestos_m7', indice: index, motivo: 'manifesto_id ausente, gerado identificador técnico', item: manifestoRaw })
      }
      const itensDoManifesto = itensM7PorManifesto[manifestoId] ?? []
      const exclusivoManifesto = temSinalizacaoExclusiva(manifestoRaw) || itensDoManifesto.some((item) => temSinalizacaoExclusiva(item))
      const agendamentoManifesto = temSinalizacaoAgendamento(manifestoRaw) || itensDoManifesto.some((item) => temSinalizacaoAgendamento(item))
      const restricaoManifesto = temSinalizacaoRestricao(manifestoRaw) || itensDoManifesto.some((item) => temSinalizacaoRestricao(item))
      const perfilManifesto = toText(
        manifestoRaw.perfil_final_m6_2
        ?? manifestoRaw.veiculo_perfil
        ?? manifestoRaw.veiculo_tipo
      )
      if (!perfilManifesto) return { value: null, reason: `Manifesto ${manifestoId} sem perfil de veículo` }

      const veiculoCadastro = veiculos.find((veiculo) => {
        return normalizarPerfilVeiculo(veiculo.perfil ?? veiculo.tipo) === normalizarPerfilVeiculo(perfilManifesto)
      })
      const qtdEixosRaw = veiculoCadastro?.qtd_eixos
      const qtdEixos = Number.isFinite(toNumber(qtdEixosRaw, Number.NaN))
        ? Math.trunc(toNumber(qtdEixosRaw, Number.NaN))
        : null
      const kmTotal = toNumber(manifestoRaw.km_total_estimado_m6_2, 0)
      if (index === 0) {
        console.log('[PERSISTENCIA] manifesto operacional', {
          manifestoId,
          perfilManifesto,
          qtdEixos,
          kmTotal,
        })
      }

      return { value: {
        rodada_id: rodadaId,
        manifesto_id: manifestoId,
        veiculo_perfil: perfilManifesto,
        veiculo_tipo: perfilManifesto,
        qtd_eixos: qtdEixos,
        exclusivo_flag: exclusivoManifesto,
        peso_total: toNumber(manifestoRaw.peso_final_m6_2, Number.NaN),
        ocupacao: toNumber(manifestoRaw.ocupacao_final_m6_2, Number.NaN),
        km_total: kmTotal,
        qtd_entregas: Math.trunc(toNumber(manifestoRaw.qtd_itens_final_m6_2, 0)),
        qtd_clientes: Math.trunc(toNumber(manifestoRaw.qtd_paradas_final_m6_2, 0)),
        frete_minimo: null,
        origem_modulo: pickFirstText(manifestoRaw.origem_manifesto_modulo),
        tipo_manifesto: pickFirstText(manifestoRaw.origem_manifesto_tipo),
        ...(inserirPayloadManifesto ? {
          payload_apoio_json: {
            ...manifestoRaw,
            sinalizacao_visual: {
              exclusivo: exclusivoManifesto,
              agendada: agendamentoManifesto,
              restricao: restricaoManifesto,
            },
          },
        } : {}),
      }}
    }})
    rejeicoesPersistencia.push(...rejeitadosManifestos)
    console.log('[PERSISTENCIA] manifestos preparados', {
      totalManifestos: registrosManifestos.length,
      comQtdEixos: registrosManifestos.filter((m) => Number(m.qtd_eixos) > 0).length,
    })

    const { validos: registrosItens, rejeitados: rejeitadosItens } = safeMapAndFilter({
      grupo: 'itens_manifestos_sequenciados_m7',
      itens: itensM7,
      rodadaId,
      mapper: (item, index) => {
      const manifestoId = toText(item.manifesto_id)
      const idLinhaPipeline = toText(item.id_linha_pipeline)
      if (!manifestoId || !idLinhaPipeline) return { value: null, reason: 'Item sem manifesto_id ou id_linha_pipeline' }
      const obrigatorios = ['nro_documento', 'destinatario', 'cidade', 'uf', 'peso_calculado', 'ordem_entrega_doc_m7', 'ordem_carregamento_doc_m7'] as const
      for (const campo of obrigatorios) {
        if (item[campo] === undefined || item[campo] === null || (typeof item[campo] === 'string' && String(item[campo]).trim() === '')) {
          return { value: null, reason: `Item sem ${campo}` }
        }
      }
      const ordemParadaM7 = item.ordem_parada_m7 ?? item.ordem_entrega_parada_m7
      if (ordemParadaM7 === undefined || ordemParadaM7 === null || String(ordemParadaM7).trim() === '') {
        return { value: null, reason: 'Item sem ordem_parada_m7/ordem_entrega_parada_m7' }
      }
      const exclusivoItem = temSinalizacaoExclusiva(item)
      const agendamentoItem = temSinalizacaoAgendamento(item)
      const restricaoItem = temSinalizacaoRestricao(item)
      return { value: {
        rodada_id: rodadaId,
        manifesto_id: manifestoId,
        sequencia: Math.trunc(toNumber(item.ordem_entrega_doc_m7, Number.NaN)),
        nro_documento: toText(item.nro_documento),
        destinatario: toText(item.destinatario),
        cidade: toText(item.cidade),
        uf: toText(item.uf),
        peso: toNumber(item.peso_calculado, Number.NaN),
        distancia_km: toNumber(item.distancia_rodoviaria_est_km, 0),
        inicio_entrega: pickFirstText(
          item.inicio_entrega,
          item.hora_inicio_entrega,
          item.hora_inicio_janela,
          item.inicio_janela_entrega,
          item.data_agenda,
        ),
        fim_entrega: pickFirstText(
          item.fim_entrega,
          item.hora_fim_entrega,
          item.hora_fim_janela,
          item.fim_janela_entrega,
          item.hora_agenda,
        ),
        ...(inserirPayloadItem ? {
          payload_apoio_json: {
            ...item,
            sinalizacao_visual: {
              exclusivo: exclusivoItem,
              agendada: agendamentoItem,
              restricao: restricaoItem,
            },
          },
        } : {}),
        latitude: pickFirstNumber(item.latitude_destinatario),
        longitude: pickFirstNumber(item.longitude_destinatario),
      }}
    }})
    rejeicoesPersistencia.push(...rejeitadosItens)

    const toTextRemanescente = (valor: unknown): string => {
      return String(valor ?? '').trim()
    }

    const toNumberOrNull = (valor: unknown): number | null => {
      const n = Number(valor)
      return Number.isFinite(n) ? n : null
    }

    const { error: payloadApoioProbeError } = await supabase
      .from('remanescentes_roteirizacao')
      .select('id, payload_apoio_json')
      .limit(1)
    const inserirPayloadApoio = !payloadApoioProbeError

    const { validos: registrosNaoRoteirizaveisM3, rejeitados: rejeitadosNaoRoteirizaveis } = safeMapAndFilter({
      grupo: 'remanescentes.nao_roteirizaveis_m3',
      itens: remanescentesM7.nao_roteirizaveis_m3,
      rodadaId,
      mapper: (item) => {
      const nroDocumento = toTextRemanescente(getFirstValue(item, ['nro_documento', 'documento', 'nro_doc', 'numero_documento', 'Nro Doc.', 'doc', 'nf', 'nota_fiscal']))
      const destinatario = toTextRemanescente(getFirstValue(item, ['destinatario', 'destinatário', 'cliente', 'nome_cliente', 'razao_social_destinatario', 'Destina', 'destina']))
      const cidade = toTextRemanescente(getFirstValue(item, ['cidade', 'cidade_destino', 'cidade_destinatario', 'Cidade Dest.', 'Cida', 'cida']))
      const uf = toTextRemanescente(getFirstValue(item, ['uf', 'uf_destino', 'uf_destinatario', 'UF']))
      const motivoTriagem = toTextRemanescente(getFirstValue(item, ['motivo', 'motivo_triagem', 'status_triagem', 'motivo_nao_roteirizado', 'regra', 'observacao', 'observação']))
      const statusTriagem = toTextRemanescente(item.status_triagem)
      if (!nroDocumento && !destinatario && !cidade && !motivoTriagem) return { value: null, reason: 'Item sem campos mínimos para remanescente M3' }
      const registroBase = {
        rodada_id: rodadaId,
        tipo_remanescente: 'nao_roteirizavel_triagem',
        id_linha_pipeline: toTextRemanescente(item.id_linha_pipeline) || null,
        nro_documento: nroDocumento,
        destinatario,
        cidade,
        uf,
        peso_calculado: toNumberOrNull(item.peso_calculado),
        distancia_rodoviaria_est_km: toNumberOrNull(item.distancia_rodoviaria_est_km),
        mesorregiao: toTextRemanescente(item.mesorregiao) || null,
        subregiao: toTextRemanescente(item.subregiao) || null,
        status_triagem: statusTriagem || null,
        motivo_triagem: motivoTriagem || null,
        motivo: motivoTriagem || statusTriagem || 'Não roteirizável na triagem',
        etapa_origem: 'm3_triagem',
      }
      return { value: inserirPayloadApoio ? { ...registroBase, payload_apoio_json: item } : registroBase }
    }})
    rejeicoesPersistencia.push(...rejeitadosNaoRoteirizaveis)

    const { validos: registrosSaldoFinal, rejeitados: rejeitadosSaldoFinal } = safeMapAndFilter({
      grupo: 'remanescentes.saldo_final_roteirizacao',
      itens: remanescentesM7.saldo_final_roteirizacao,
      rodadaId,
      mapper: (item) => {
      const nroDocumento = toTextRemanescente(item.nro_documento)
      const destinatario = toTextRemanescente(item.destinatario)
      const cidade = toTextRemanescente(item.cidade)
      const uf = toTextRemanescente(item.uf)
      const motivoDetalhadoM62 = toTextRemanescente(item.motivo_detalhado_m6_2)
      const motivoFinalM62 = toTextRemanescente(item.motivo_final_remanescente_m6_2)
      const motivoFinalM54 = toTextRemanescente(item.motivo_final_remanescente_m5_4)
      const motivoFinalM53 = toTextRemanescente(item.motivo_final_remanescente_m5_3)
      const motivoExistente = toTextRemanescente(item.motivo)
      if (!nroDocumento && !destinatario && !cidade && !motivoExistente) return { value: null, reason: 'Item sem campos mínimos para saldo final' }
      const registroBase = {
        rodada_id: rodadaId,
        tipo_remanescente: 'roteirizavel_saldo_final',
        id_linha_pipeline: toTextRemanescente(item.id_linha_pipeline) || null,
        nro_documento: nroDocumento,
        destinatario,
        cidade,
        uf,
        peso_calculado: toNumberOrNull(item.peso_calculado),
        distancia_rodoviaria_est_km: toNumberOrNull(item.distancia_rodoviaria_est_km),
        mesorregiao: toTextRemanescente(item.mesorregiao) || null,
        subregiao: toTextRemanescente(item.subregiao) || null,
        corredor_30g: toTextRemanescente(item.corredor_30g) || null,
        corredor_30g_idx: toNumberOrNull(item.corredor_30g_idx),
        motivo_detalhado_m6_2: motivoDetalhadoM62 || null,
        motivo_final_remanescente_m6_2: motivoFinalM62 || null,
        motivo_final_remanescente_m5_4: motivoFinalM54 || null,
        motivo_final_remanescente_m5_3: motivoFinalM53 || null,
        motivo: motivoDetalhadoM62 ||
          motivoFinalM62 ||
          motivoFinalM54 ||
          motivoFinalM53 ||
          motivoExistente ||
          'Saldo final da roteirização',
        etapa_origem: 'saldo_final_roteirizacao',
      }
      return { value: inserirPayloadApoio ? { ...registroBase, payload_apoio_json: item } : registroBase }
    }})
    rejeicoesPersistencia.push(...rejeitadosSaldoFinal)
    const registrosRemanescentes = [...registrosNaoRoteirizaveisM3, ...registrosSaldoFinal]
    console.log('[PERSISTENCIA] remanescentes preparados:', registrosRemanescentes.length)
    console.log('[PERSISTENCIA M7] remanescentes preparados', {
      saldoFinal: remanescentesM7.saldo_final_roteirizacao.length,
      naoRoteirizaveisM3: remanescentesM7.nao_roteirizaveis_m3.length,
      registrosRemanescentes: registrosRemanescentes.length,
    })

    const { error: deleteItensError } = await supabase.from('manifestos_itens').delete().eq('rodada_id', rodadaId)
    if (deleteItensError) throw deleteItensError
    const { error: deleteManifestosError } = await supabase.from('manifestos_roteirizacao').delete().eq('rodada_id', rodadaId)
    if (deleteManifestosError) throw deleteManifestosError
    const { error: deleteRotasGoogleError } = await supabase.from('rotas_manifestos_google').delete().eq('rodada_id', rodadaId)
    if (deleteRotasGoogleError) throw deleteRotasGoogleError
    const { error: deleteRemanescentesError } = await supabase.from('remanescentes_roteirizacao').delete().eq('rodada_id', rodadaId)
    if (deleteRemanescentesError) throw deleteRemanescentesError
    const { error: deleteEstatisticasError } = await supabase.from('estatisticas_roteirizacao').delete().eq('rodada_id', rodadaId)
    if (deleteEstatisticasError) throw deleteEstatisticasError

    if (registrosManifestos.length) {
      const { error } = await supabase.from('manifestos_roteirizacao').insert(registrosManifestos)
      if (error) throw error
    }
    const totalManifestosSalvos = registrosManifestos.length

    if (registrosItens.length) {
      const { error } = await supabase.from('manifestos_itens').insert(registrosItens)
      if (error) throw error
    }
    const totalItensSalvos = registrosItens.length
    console.log('[PERSISTENCIA] itens salvos:', totalItensSalvos)

    if (registrosRemanescentes.length) {
      const { error } = await supabase.from('remanescentes_roteirizacao').insert(registrosRemanescentes)
      if (error) throw error
    }
    const totalRemanescentesSalvos = registrosRemanescentes.length
    console.log('[PERSISTENCIA] remanescentes salvos:', totalRemanescentesSalvos)
    if (rejeicoesPersistencia.length > 0) {
      const { error: rejeicoesError } = await supabase.from('roteirizacao_persistencia_rejeicoes').insert(rejeicoesPersistencia)
      if (rejeicoesError) console.warn('[PERSISTENCIA] falha ao registrar rejeições', rejeicoesError)
    }
    const resumoNegocio = toRecord(resposta.resumo_negocio)
    const respostaRecord = resposta as unknown as Record<string, unknown>
    const totalCarteiraRaw = respostaRecord.total_carteira ?? resumoNegocio?.total_carteira
    if (totalCarteiraRaw === undefined || totalCarteiraRaw === null) {
      throw new Error('Contrato M7 inválido: campo obrigatório total_carteira (raiz ou resumo_negocio.total_carteira) ausente')
    }
    const totalCarteira = Math.trunc(toNumber(totalCarteiraRaw, Number.NaN))
    if (!Number.isFinite(totalCarteira)) {
      throw new Error('Contrato M7 inválido: total_carteira (raiz ou resumo_negocio.total_carteira) deve ser numérico')
    }
    const resumoExecucao = toRecord(resposta.resumo_execucao)
    const temposMs = toRecord(resumoExecucao?.tempos_ms)
    if (!temposMs || temposMs.tempo_total_pipeline_ms === undefined || temposMs.tempo_total_pipeline_ms === null) {
      throw new Error('Contrato M7 inválido: campo obrigatório resumo_execucao.tempos_ms.tempo_total_pipeline_ms ausente')
    }
    const tempoExecucaoMs = Math.trunc(toNumber(temposMs.tempo_total_pipeline_ms, Number.NaN))
    if (!Number.isFinite(tempoExecucaoMs)) {
      throw new Error('Contrato M7 inválido: resumo_execucao.tempos_ms.tempo_total_pipeline_ms deve ser numérico')
    }

    const totalManifestos = totalManifestosSalvos
    const totalRoteirizado = totalItensSalvos
    const totalRemanescente = totalRemanescentesSalvos
    const kmTotalRodada = registrosManifestos.reduce((acc, manifesto) => acc + toNumber(manifesto.km_total, 0), 0)
    const ocupacaoMediaRodada = totalManifestos > 0
      ? registrosManifestos.reduce((acc, manifesto) => acc + toNumber(manifesto.ocupacao, 0), 0) / totalManifestos
      : 0

    const remanescentesEsperados = remanescentesM7.nao_roteirizaveis_m3.length + remanescentesM7.saldo_final_roteirizacao.length
    const resumoPersistencia = {
      manifestos: { recebidos: manifestosM7.length, salvos: totalManifestosSalvos, rejeitados: manifestosM7.length - totalManifestosSalvos },
      itens_manifestos: { recebidos: itensM7.length, salvos: totalItensSalvos, rejeitados: itensM7.length - totalItensSalvos },
      remanescentes: { recebidos: remanescentesEsperados, salvos: totalRemanescentesSalvos, rejeitados: remanescentesEsperados - totalRemanescentesSalvos },
      rejeicoes_total: rejeicoesPersistencia.length,
    }
    console.log('[PERSISTENCIA] resumo', resumoPersistencia)

    const registroEstatistica = {
      rodada_id: rodadaId,
      total_carteira: totalCarteira,
      total_roteirizado: totalRoteirizado,
      total_remanescente: totalRemanescente,
      total_manifestos: totalManifestos,
      km_total: kmTotalRodada,
      ocupacao_media: ocupacaoMediaRodada,
      tempo_execucao_ms: tempoExecucaoMs,
    }

    console.log('[ESTATISTICAS] km_total_rodada:', kmTotalRodada)
    console.log('[ESTATISTICAS] ocupacao_media_rodada:', ocupacaoMediaRodada)
    console.log('[ESTATISTICAS] tempo_execucao_ms:', tempoExecucaoMs)

    const { error: estError } = await supabase.from('estatisticas_roteirizacao').upsert(registroEstatistica)
    if (estError) throw estError

    const { error: rodadaAggError } = await supabase
      .from('rodadas_roteirizacao')
      .update({
        status: 'sucesso',
        total_cargas_entrada: totalCarteira,
        total_manifestos: totalManifestos,
        total_itens_manifestados: totalRoteirizado,
        total_nao_roteirizados: totalRemanescente,
        km_total_frota: kmTotalRodada,
        ocupacao_media_percentual: ocupacaoMediaRodada,
        tempo_processamento_ms: tempoExecucaoMs,
        resposta_motor: resposta as unknown as Record<string, unknown>,
        resumo_persistencia_json: resumoPersistencia,
        erro_mensagem: null,
      })
      .eq('id', rodadaId)
    if (rodadaAggError) throw rodadaAggError

    console.log('[PERSISTENCIA M7] salvos', {
      manifestosSalvos: totalManifestosSalvos,
      itensSalvos: totalItensSalvos,
      remanescentesSalvos: totalRemanescentesSalvos,
      estatisticasSalvas: true,
    })
    void resumoSequenciamentoM7
    console.log('[PERSISTENCIA] estatisticas salvas para rodada:', rodadaId)
    console.log('[ROTEIRIZACAO] persistência tolerante concluída', { rodadaId, rejeicoes_total: rejeicoesPersistencia.length })
  },

  async buscarCarteiraFiltrada(uploadId: string, filtros?: FiltrosCarteira): Promise<CarteiraCarga[]> {
    const baseQuery = supabase
      .from('carteira_itens')
      .select('*')
      .eq('upload_id', uploadId)
      .eq('status_validacao', 'valida')
      .order('linha_numero', { ascending: true })

    const { data, error } = await aplicarFiltrosCarteira(baseQuery, filtros)
    if (error) throw error

    return (data || [])
      .map(normalizarCarteiraItem)
      .filter((item: CarteiraCarga) => !isLinhaCarteiraSemConteudo(item as Record<string, unknown>))
  },

  /**
   * Dispara a roteirização: monta o payload, chama o motor, calcula frete ANTT e salva a rodada
   */
  async roteirizar(
    filial: Filial,
    uploadId: string,
    filtros: FiltrosRoteirizacao,
    usuarioId: string,
    carteiraFiltrada: CarteiraCarga[],
    configuracaoFrota: ConfiguracaoFrotaItem[]
  ): Promise<{ rodada: RodadaRoteirizacao; manifestos: ManifestoComFrete[] }> {
    const inicio = Date.now()
    const carteira = carteiraFiltrada

    if (!carteira.length) {
      throw new Error('Nenhum item válido encontrado para este upload.')
    }

    const rodadaId = crypto.randomUUID()
    const dataBaseRoteirizacaoIso = toIsoCompleto(filtros.data_base)
    const dataExecucaoIso = new Date().toISOString()
    const carteiraContrato = carteira.map((item, index) => mapCarteiraItemToMotorContract(item, index))
    if (import.meta.env.DEV && carteiraContrato.length > 0) {
      const exemplo = carteiraContrato[0] as Record<string, unknown>
      console.log('[PAYLOAD NUMERICO] exemplo item carteira:', {
        nroDocumento: exemplo['Nro Doc.'],
        peso: exemplo.Peso,
        pesoCalculo: exemplo['Peso Calculo'],
        valorMercadoria: exemplo['Vlr.Merc.'],
        quantidade: exemplo['Qtd.'],
        pesoCubico: exemplo['Peso Cub.'],
        latitude: exemplo.Latitude,
        longitude: exemplo.Longitude,
      })
    }
    const { data: veiculosData, error: veiculosError } = await supabase
      .from('veiculos')
      .select('id, filial_id, tipo, placa, capacidade_peso_kg, capacidade_volume_m3, num_eixos, max_km_distancia, max_entregas, ocupacao_minima_perc, ocupacao_maxima_perc, ativo')
      .eq('filial_id', filial.id)
      .eq('ativo', true)
      .order('tipo')

    if (veiculosError) throw veiculosError
    const veiculos = (veiculosData ?? []).map((item) => mapVeiculoToMotor(item as Record<string, unknown>))
    if (import.meta.env.DEV && veiculos.length === 0) {
      console.log('[MOTOR2] nenhum veículo ativo encontrado para a filial operacional:', filial.id)
    }
    if (import.meta.env.DEV) {
      console.log('[MOTOR2] veiculos payload final:', veiculos)
      console.log('[MOTOR2] primeiro veiculo serializado:', veiculos?.[0])
    }
    if (veiculos.length === 0) {
      throw new Error('Nenhum veículo ativo encontrado para a filial operacional selecionada.')
    }

    const { data: usuarioPerfil } = await supabase
      .from('usuarios_perfil')
      .select('nome')
      .eq('id', usuarioId)
      .maybeSingle()

    const usuarioNome = usuarioPerfil?.nome || 'Usuário'

    // 1. Montar payload para o motor
    const payload: PayloadMotor = {
      rodada_id: rodadaId,
      upload_id: uploadId,
      usuario_id: usuarioId,
      filial_id: filial.id,
      data_base_roteirizacao: dataBaseRoteirizacaoIso,
      tipo_roteirizacao: filtros.tipo_roteirizacao,
      filtros_aplicados: filtros.filtros_aplicados,
      configuracao_frota: filtros.tipo_roteirizacao === 'frota' ? configuracaoFrota : [],
      veiculos,
      filial: {
        id: filial.id,
        nome: filial.nome,
        cidade: filial.cidade,
        uf: filial.uf,
        latitude: filial.latitude,
        longitude: filial.longitude,
      },
      parametros: {
        usuario_id: usuarioId,
        usuario_nome: usuarioNome,
        filial_id: filial.id,
        filial_nome: filial.nome,
        upload_id: uploadId,
        rodada_id: rodadaId,
        data_execucao: dataExecucaoIso,
        data_base_roteirizacao: dataBaseRoteirizacaoIso,
        origem_sistema: 'sistema1',
        modelo_roteirizacao: 'roteirizador_rec',
        tipo_roteirizacao: filtros.tipo_roteirizacao,
        filtros_aplicados: filtros.filtros_aplicados,
      },
      carteira: carteiraContrato,
    }

    const payloadResumido = {
      rodada_id: rodadaId,
      upload_id: uploadId,
      usuario_id: usuarioId,
      filial_id: filial.id,
      tipo_roteirizacao: filtros.tipo_roteirizacao,
      data_base_roteirizacao: dataBaseRoteirizacaoIso,
      total_carteira: carteiraContrato.length,
      total_veiculos: veiculos.length,
    }

    const { error: rodadaInicialError } = await supabase
      .from('rodadas_roteirizacao')
      .insert({
        id: rodadaId,
        filial_id: filial.id,
        filial_nome: filial.nome,
        usuario_id: usuarioId,
        usuario_nome: usuarioNome,
        upload_id: uploadId,
        status: 'processando',
        tipo_roteirizacao: filtros.tipo_roteirizacao,
        data_base_roteirizacao: dataBaseRoteirizacaoIso,
        total_cargas_entrada: carteiraContrato.length,
        payload_enviado: payloadResumido as unknown as Record<string, unknown>,
      })
    if (rodadaInicialError) {
      console.error('Erro ao criar rodada inicial:', rodadaInicialError)
    }

    // 2. Chamar o motor Python
    let resposta: RespostaMotor
    const motorBaseUrl = getMotor2BaseUrl()
    const finalUrl = buildMotor2Url(MOTOR_2_ROTEIRIZAR_PATH)

    if (import.meta.env.DEV) {
      console.log('[ROTEIRIZACAO] tipo_roteirizacao salvo/enviado:', payload.tipo_roteirizacao)
      console.log('[MOTOR2] veiculos enviados:', veiculos.length)
      console.log('[MOTOR2] configuracao_frota enviada:', payload.configuracao_frota)
      console.log('[MOTOR2] tipo_roteirizacao enviado:', payload.tipo_roteirizacao)
      console.log('[Motor2] Base URL:', motorBaseUrl)
      console.log('[Motor2] Path:', MOTOR_2_ROTEIRIZAR_PATH)
      console.log('[Motor2] Final URL:', finalUrl)
      console.log('[Motor2] Payload final:', payload)
    }

    const inicioChamadaMotor = Date.now()
    console.log('[MOTOR2] timeout roteirizar ms:', MOTOR_2_ROTEIRIZAR_TIMEOUT_MS)
    console.log('[MOTOR2] chamada iniciada:', finalUrl)

    try {
      const response = await fetch(finalUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(MOTOR_2_ROTEIRIZAR_TIMEOUT_MS),
      })
      console.log('[MOTOR2] resposta recebida em ms:', Date.now() - inicioChamadaMotor)

      if (!response.ok) {
        if (response.status === 404) {
          throw new Error('Endpoint do Motor 2 não encontrado (HTTP 404). Verifique a URL e o path configurados.')
        }

        if (response.status === 422) {
          const body422 = await response.json().catch(() => null)
          console.error('[Motor2] Body do erro 422:', body422)
          const mensagem422 = extrairMensagemErro(body422)
          throw new Error(`Motor retornou HTTP 422: ${mensagem422}`)
        }

        throw new Error(`Motor retornou HTTP ${response.status}`)
      }

      resposta = await response.json() as RespostaMotor
    } catch (err) {
      const timeoutError = isMotor2TimeoutError(err)
      if (timeoutError) {
        console.error('[MOTOR2] timeout aguardando resposta do motor', {
          timeoutMs: MOTOR_2_ROTEIRIZAR_TIMEOUT_MS,
          elapsedMs: Date.now() - inicioChamadaMotor,
          finalUrl,
        })
      }

      const mensagem = timeoutError
        ? 'Timeout ao aguardar resposta do Motor 2. O processamento excedeu o limite de 15 minutos no Sistema 1.'
        : (err instanceof Error ? err.message : 'Erro de comunicação com o motor')
      const status = err instanceof Error && /HTTP (\d{3})/.test(err.message)
        ? Number(err.message.match(/HTTP (\d{3})/)?.[1])
        : undefined

      console.error('[Motor2] Falha ao chamar endpoint de roteirização', {
        finalUrl,
        status,
        error: err,
      })

      await supabase
        .from('rodadas_roteirizacao')
        .update({
          status: 'erro',
          erro_mensagem: mensagem,
          tempo_processamento_ms: Date.now() - inicio,
          payload_enviado: payload as unknown as Record<string, unknown>,
        })
        .eq('id', rodadaId)

      if (mensagem.includes('VITE_MOTOR_2_URL')) {
        throw new Error(`Configuração inválida do Motor 2: ${mensagem}`)
      }

      throw new Error(`Falha ao comunicar com o Motor de Roteirização: ${mensagem}`)
    }

    console.log('[ROTEIRIZACAO] resposta HTTP recebida do motor')
    console.log('[ROTEIRIZACAO] iniciando fluxo pós-retorno')
    console.log('[ROTEIRIZACAO] chaves da resposta do motor:', Object.keys(resposta || {}))
    console.log('[ROTEIRIZACAO] manifestos_m7:', resposta?.manifestos_m7?.length || 0)
    console.log('[ROTEIRIZACAO] itens_manifestos_sequenciados_m7:', resposta?.itens_manifestos_sequenciados_m7?.length || 0)
    console.log('[ROTEIRIZACAO] manifestos_fechados:', toRecordArray(resposta.manifestos_fechados).length)
    console.log('[ROTEIRIZACAO] manifestos_compostos:', toRecordArray(resposta.manifestos_compostos).length)
    console.log('[ROTEIRIZACAO] nao_roteirizados:', resposta?.nao_roteirizados?.length || 0)

    const manifestosResposta = Array.isArray(resposta.manifestos) ? resposta.manifestos : []

    let statusFinal: RodadaRoteirizacao['status'] = 'erro'
    let erroPosRetorno: string | null = null
    let rodadaData: Partial<RodadaRoteirizacao> | null = null
    let manifestosComFrete: ManifestoComFrete[] = []

    try {
      if (resposta.status === 'erro') {
        throw new Error(resposta.erro?.mensagem || 'O motor retornou um erro desconhecido')
      }

      // 3. Calcular frete mínimo ANTT para cada manifesto (deslocamento + carga/descarga da categoria Carga geral)
      const tabelaAntt = await anttService.listar()
      manifestosComFrete = await Promise.all(
        manifestosResposta.map(async (manifesto) => {
          const tipoCargaId = 5
          const numEixos = manifesto.num_eixos || 2

          const coef = tabelaAntt.find(
            (t) => t.codigo_tipo === tipoCargaId && t.num_eixos === numEixos
          )

          const coefDeslocamento = coef?.coef_ccd || 0
          const cargaDescarga = pickFirstNumber(
            (coef as unknown as Record<string, unknown>)?.valor_carga_descarga,
            (coef as unknown as Record<string, unknown>)?.carga_descarga,
            coef?.coef_cc,
            0,
          ) ?? 0
          const freteMinimo = anttService.calcularFreteMinimo(manifesto.km_estimado, coefDeslocamento, manifesto.km_estimado > 0 ? cargaDescarga : 0)

          return {
            ...manifesto,
            frete_minimo_antt: freteMinimo,
            tipo_carga_antt: String(tipoCargaId),
            coeficiente_deslocamento: coefDeslocamento,
            coeficiente_carga_descarga: cargaDescarga,
            aprovado: false,
            excluido: false,
          }
        })
      )

      await this.persistirEstruturaRodada(rodadaId, resposta, veiculos)
      try {
        await this.calcularRotasGoogleRodada(rodadaId)
      } catch (error) {
        console.warn('[GOOGLE ROUTES] falha parcial ou total; fretes sem rota Google serão marcados como cálculo manual necessário', error)
      }
      try {
        await this.calcularFretesManifestosRodada(rodadaId)
      } catch (error) {
        console.error('[FRETE MINIMO] falha geral ao calcular fretes da rodada', error)
      }
      statusFinal = mapearStatusMotorParaStatusRodada(resposta.status, false)
    } catch (erro) {
      console.error('[ROTEIRIZACAO] erro no fluxo pós-retorno', erro)
      erroPosRetorno = extrairMensagemErroRuntime(erro)
      statusFinal = 'erro'
    } finally {
      console.log('[ROTEIRIZACAO] status motor recebido:', resposta.status)
      statusFinal = mapearStatusMotorParaStatusRodada(resposta.status, Boolean(erroPosRetorno))
      console.log('[ROTEIRIZACAO] status mapeado para rodada:', statusFinal)
      const tempoMs = Date.now() - inicio
      const rodadaPayload: Record<string, unknown> = {
        status: statusFinal,
        payload_enviado: payload as unknown as Record<string, unknown>,
        resposta_motor: resposta as unknown as Record<string, unknown>,
        erro_mensagem: statusFinal === 'erro' ? (erroPosRetorno || resposta.erro?.mensagem || null) : null,
      }
      if (statusFinal === 'erro') {
        rodadaPayload.tempo_processamento_ms = tempoMs
      }

      const { data, error: rodadaError } = await supabase
        .from('rodadas_roteirizacao')
        .update(rodadaPayload)
        .eq('id', rodadaId)
        .select()
        .single()

      if (rodadaError) {
        console.error('Erro ao salvar rodada:', rodadaError)
      }

      console.log('[ROTEIRIZACAO] rodada finalizada com status:', statusFinal)
      console.log('[ROTEIRIZACAO] rodada atualizada:', rodadaId)
      rodadaData = data as Partial<RodadaRoteirizacao> | null
    }

    if (statusFinal === 'erro') {
      throw new Error(erroPosRetorno || resposta.erro?.mensagem || 'O fluxo pós-retorno falhou')
    }

    const tempoMs = Date.now() - inicio

    const rodada: RodadaRoteirizacao = {
      id: rodadaData?.id || rodadaId,
      filial_id: filial.id,
      filial_nome: filial.nome,
      usuario_id: usuarioId,
      upload_id: uploadId,
      status: statusFinal,
      tipo_roteirizacao: filtros.tipo_roteirizacao,
      total_cargas_entrada: rodadaData?.total_cargas_entrada ?? 0,
      total_manifestos: rodadaData?.total_manifestos ?? 0,
      total_itens_manifestados: rodadaData?.total_itens_manifestados ?? 0,
      total_nao_roteirizados: rodadaData?.total_nao_roteirizados ?? 0,
      km_total_frota: rodadaData?.km_total_frota ?? 0,
      ocupacao_media_percentual: rodadaData?.ocupacao_media_percentual ?? 0,
      tempo_processamento_ms: rodadaData?.tempo_processamento_ms ?? tempoMs,
      resposta_motor: resposta,
      created_at: new Date().toISOString(),
    }

    return { rodada, manifestos: manifestosComFrete }
  },

  async listarRodadas(filialId?: string): Promise<RodadaRoteirizacao[]> {
    let query = supabase
      .from('rodadas_roteirizacao')
      .select('*, filiais:filial_id(nome)')
      .order('created_at', { ascending: false })
      .limit(100)

    if (filialId) query = query.eq('filial_id', filialId)

    const { data, error } = await query
    if (error) throw error

    return (data || []).map((r) => ({
      ...r,
      filial_nome: (r.filiais as { nome: string } | null)?.nome,
      usuario_nome: r.usuario_nome,
    })) as RodadaRoteirizacao[]
  },

  async buscarRodada(id: string): Promise<RodadaRoteirizacao> {
    const { data, error } = await supabase
      .from('rodadas_roteirizacao')
      .select('*, filiais:filial_id(nome)')
      .eq('id', id)
      .single()
    if (error) throw error
    return {
      ...data,
      filial_nome: (data.filiais as { nome: string } | null)?.nome,
      usuario_nome: data.usuario_nome,
    } as RodadaRoteirizacao
  },

  async buscarDetalhesAprovacao(rodadaId: string): Promise<{
    manifestos: ManifestoRoteirizacaoDetalhe[]
    remanescentes: RemanescenteRoteirizacao[]
    estatisticas: EstatisticasRoteirizacao | null
  }> {
    const [manifestosRes, remanescentesRes, estatisticasRes] = await Promise.all([
      supabase.from('manifestos_roteirizacao').select('*').eq('rodada_id', rodadaId).order('manifesto_id'),
      supabase
        .from('remanescentes_roteirizacao')
        .select('id, rodada_id, tipo_remanescente, id_linha_pipeline, nro_documento, destinatario, cidade, uf, peso_calculado, distancia_rodoviaria_est_km, mesorregiao, subregiao, corredor_30g, corredor_30g_idx, status_triagem, motivo_triagem, motivo_detalhado_m6_2, motivo_final_remanescente_m6_2, motivo_final_remanescente_m5_4, motivo_final_remanescente_m5_3, motivo, etapa_origem, payload_apoio_json, created_at')
        .eq('rodada_id', rodadaId)
        .order('created_at'),
      supabase.from('estatisticas_roteirizacao').select('*').eq('rodada_id', rodadaId).maybeSingle(),
    ])

    if (manifestosRes.error) throw manifestosRes.error
    if (remanescentesRes.error) throw remanescentesRes.error
    if (estatisticasRes.error) throw estatisticasRes.error

    return {
      manifestos: (manifestosRes.data ?? []) as ManifestoRoteirizacaoDetalhe[],
      remanescentes: (remanescentesRes.data ?? []) as RemanescenteRoteirizacao[],
      estatisticas: (estatisticasRes.data as EstatisticasRoteirizacao | null) ?? null,
    }
  },

  async buscarItensManifestosRodada(rodadaId: string): Promise<ManifestoItemRoteirizacao[]> {
    const { data, error } = await supabase
      .from('manifestos_itens')
      .select('id, rodada_id, manifesto_id, sequencia, nro_documento, destinatario, cidade, uf, peso, distancia_km, inicio_entrega, fim_entrega, payload_apoio_json, latitude, longitude, created_at, updated_at')
      .eq('rodada_id', rodadaId)
      .order('manifesto_id')
      .order('sequencia')

    if (error) throw error
    return (data ?? []) as ManifestoItemRoteirizacao[]
  },

  async buscarManifestoOperacional(rodadaId: string, manifestoId: string): Promise<{
    manifesto: ManifestoRoteirizacaoDetalhe | null
    itens: ManifestoItemRoteirizacao[]
  }> {
    const [manifestoRes, itensRes] = await Promise.all([
      supabase
        .from('manifestos_roteirizacao')
        .select('*')
        .eq('rodada_id', rodadaId)
        .eq('manifesto_id', manifestoId)
        .maybeSingle(),
      supabase
        .from('manifestos_itens')
        .select('*')
        .eq('rodada_id', rodadaId)
        .eq('manifesto_id', manifestoId)
        .order('sequencia'),
    ])
    if (manifestoRes.error) throw manifestoRes.error
    if (itensRes.error) throw itensRes.error
    return {
      manifesto: (manifestoRes.data as ManifestoRoteirizacaoDetalhe | null) ?? null,
      itens: (itensRes.data ?? []) as ManifestoItemRoteirizacao[],
    }
  },

  async atualizarFreteManifesto(rodadaId: string, manifestoId: string, rotaGoogleParam?: RotaManifestoGoogle | null): Promise<FreteStatus> {
    const [manifestoRes, rotaGoogleRes, tabelaAntt] = await Promise.all([
      supabase.from('manifestos_roteirizacao').select('*').eq('rodada_id', rodadaId).eq('manifesto_id', manifestoId).single(),
      rotaGoogleParam !== undefined
        ? Promise.resolve({ data: rotaGoogleParam, error: null })
        : supabase.from('rotas_manifestos_google').select('*').eq('rodada_id', rodadaId).eq('manifesto_id', manifestoId).maybeSingle(),
      anttService.listar(),
    ])
    if (manifestoRes.error) throw manifestoRes.error
    if (rotaGoogleRes.error) throw rotaGoogleRes.error
    const manifesto = manifestoRes.data as ManifestoRoteirizacaoDetalhe
    const rotaGoogle = rotaGoogleRes.data as RotaManifestoGoogle | null
    const kmGoogle = Number(rotaGoogle?.km_google_maps ?? 0)
    const rotaValidaParaFrete = isRotaGoogleValidaParaFrete(rotaGoogle)
    console.log('[FRETE MINIMO] rota avaliada', { rodadaId, manifestoId, google_status: rotaGoogle?.google_status, km_google_maps: rotaGoogle?.km_google_maps, rotaValidaParaFrete })
    let payload: Record<string, unknown>
    if (!rotaValidaParaFrete) {
      payload = { frete_status: 'calculo_manual_necessario', frete_minimo_valor: null, km_frete: null, fonte_km_frete: null, rota_google_id: rotaGoogle?.id ?? null, frete_erro: MENSAGEM_MANUAL_FRETE, frete_calculado_em: null, frete_minimo_detalhes_json: { motivo: 'rota_google_indisponivel', google_status: rotaGoogle?.google_status ?? null, km_google_maps: rotaGoogle?.km_google_maps ?? null, manifesto_id: manifesto.manifesto_id }, frete_minimo: 0 }
    } else if (!manifesto.qtd_eixos || manifesto.qtd_eixos <= 0) {
      payload = { frete_status: 'sem_qtd_eixos', frete_minimo_valor: null, km_frete: kmGoogle, fonte_km_frete: 'google_routes_api', rota_google_id: rotaGoogle?.id ?? null, frete_erro: 'Não foi possível identificar a quantidade de eixos do veículo.', frete_calculado_em: null, frete_minimo: 0 }
    } else {
      const coef = tabelaAntt.find((t) => t.codigo_tipo === 5 && t.num_eixos === manifesto.qtd_eixos)
      if (!coef) {
        payload = { frete_status: 'sem_tabela_antt', frete_minimo_valor: null, km_frete: kmGoogle, fonte_km_frete: 'google_routes_api', rota_google_id: rotaGoogle?.id ?? null, frete_erro: 'Tabela ANTT não encontrada para os parâmetros do manifesto.', frete_calculado_em: null, frete_minimo: 0 }
      } else {
        const valor = anttService.calcularFreteMinimo(kmGoogle, Number(coef.coef_ccd ?? 0), Number(coef.coef_cc ?? 0))
        payload = { frete_status: 'calculado', frete_minimo_valor: valor, km_frete: kmGoogle, fonte_km_frete: 'google_routes_api', rota_google_id: rotaGoogle?.id ?? null, frete_erro: null, frete_calculado_em: new Date().toISOString(), frete_minimo_detalhes_json: { km_utilizado: kmGoogle, fonte_km: 'google_routes_api', qtd_eixos: manifesto.qtd_eixos, tabela_antt_id: coef.id ?? null, valor_por_km: coef.coef_ccd ?? 0, valor_descarga: coef.coef_cc ?? 0, valor_total: valor }, frete_minimo: valor }
      }
    }
    const status = payload.frete_status as FreteStatus
    const { error: updateError } = await supabase.from('manifestos_roteirizacao').update(payload).eq('rodada_id', rodadaId).eq('manifesto_id', manifestoId)
    if (updateError) throw updateError
    console.log('[FRETE MINIMO] resultado', { rodadaId, manifestoId, frete_status: status, frete_minimo_valor: payload.frete_minimo_valor ?? null, km_frete: payload.km_frete ?? null, fonte_km_frete: payload.fonte_km_frete ?? null })
    return status
  },

  async recalcularFretesManifestosRodada(rodadaId: string): Promise<void> {
    const { data: manifestos, error } = await supabase.from('manifestos_roteirizacao').select('manifesto_id, rota_google_id').eq('rodada_id', rodadaId).order('manifesto_id')
    if (error) throw error
    for (const manifesto of manifestos ?? []) {
      try {
        let rotaGoogle: RotaManifestoGoogle | null = null
        const rotaDireta = await supabase.from('rotas_manifestos_google').select('*').eq('rodada_id', rodadaId).eq('manifesto_id', String(manifesto.manifesto_id)).maybeSingle()
        if (rotaDireta.error) throw rotaDireta.error
        rotaGoogle = (rotaDireta.data as RotaManifestoGoogle | null) ?? null
        if (!rotaGoogle && manifesto.rota_google_id) {
          const rotaPorId = await supabase.from('rotas_manifestos_google').select('*').eq('id', String(manifesto.rota_google_id)).maybeSingle()
          if (rotaPorId.error) throw rotaPorId.error
          rotaGoogle = (rotaPorId.data as RotaManifestoGoogle | null) ?? null
        }
        await this.atualizarFreteManifesto(rodadaId, String(manifesto.manifesto_id), rotaGoogle)
      } catch (err) { console.warn('[FRETE MINIMO] erro por manifesto', { rodada_id: rodadaId, manifesto_id: manifesto.manifesto_id, err }) }
    }
  },

  async calcularFretesManifestosRodada(rodadaId: string): Promise<void> {
    await this.recalcularFretesManifestosRodada(rodadaId)
  },

  async montarRotaManifestoGoogle(rodadaId: string, manifestoId: string): Promise<RotaManifestoGoogleInput> {
    const [manifestoRes, itensRes, rodadaRes] = await Promise.all([
      supabase
        .from('manifestos_roteirizacao')
        .select('id, rodada_id, manifesto_id, km_total')
        .eq('rodada_id', rodadaId)
        .eq('manifesto_id', manifestoId)
        .maybeSingle(),
      supabase
        .from('manifestos_itens')
        .select('id, rodada_id, manifesto_id, sequencia, nro_documento, destinatario, cidade, uf, latitude, longitude')
        .eq('rodada_id', rodadaId)
        .eq('manifesto_id', manifestoId),
      supabase
        .from('rodadas_roteirizacao')
        .select('filial_id, filiais:filial_id(latitude, longitude)')
        .eq('id', rodadaId)
        .single(),
    ])

    if (manifestoRes.error) throw manifestoRes.error
    if (itensRes.error) throw itensRes.error
    if (rodadaRes.error) throw rodadaRes.error
    if (!manifestoRes.data) throw new Error(`Manifesto ${manifestoId} não encontrado na rodada`)

    const filialData = rodadaRes.data.filiais as { latitude: number | null; longitude: number | null } | Array<{ latitude: number | null; longitude: number | null }> | null
    const filial = Array.isArray(filialData) ? (filialData[0] ?? null) : filialData
    const origemLat = filial?.latitude ?? null
    const origemLng = filial?.longitude ?? null
    const itens = (itensRes.data ?? []) as ManifestoItemRoteirizacao[]
    const paradas = consolidarParadasManifesto(itens)
    const origemValida = isValidCoordinate(origemLat, origemLng)
    const statusInicial: RotaManifestoGoogle['google_status'] = !origemValida
      ? 'sem_coordenadas'
      : (paradas.length === 0 ? 'sem_paradas' : 'pendente')

    const origemHash = {
      latitude: origemValida ? origemLat as number : 0,
      longitude: origemValida ? origemLng as number : 0,
    }
    const rotaHash = gerarHashRotaGoogle(origemHash, paradas)
    const ultimaParada = paradas[paradas.length - 1]

    return {
      rodada_id: rodadaId,
      manifesto_id: manifestoId,
      manifesto_db_id: manifestoRes.data.id,
      rota_hash: rotaHash,
      origem_latitude: origemValida ? Number((origemLat as number).toFixed(8)) : 0,
      origem_longitude: origemValida ? Number((origemLng as number).toFixed(8)) : 0,
      destino_latitude: ultimaParada?.latitude ?? null,
      destino_longitude: ultimaParada?.longitude ?? null,
      paradas_json: paradas,
      qtd_paradas: paradas.length,
      km_estimado_motor: manifestoRes.data.km_total ?? null,
      google_status: statusInicial,
      google_erro: statusInicial === 'sem_coordenadas'
        ? 'Filial sem coordenadas válidas'
        : (statusInicial === 'sem_paradas' ? 'Manifesto sem paradas válidas' : null),
      fonte: 'google_routes_api',
    }
  },

  async salvarOuAtualizarRotaPendente(rota: RotaManifestoGoogleInput): Promise<RotaManifestoGoogle> {
    const { data: cacheOk } = await supabase
      .from('rotas_manifestos_google')
      .select('id, rodada_id, manifesto_id, distancia_metros_google, km_google_maps, duracao_segundos_google, polyline_google, legs_json, response_json, request_json')
      .eq('rota_hash', rota.rota_hash)
      .eq('google_status', 'ok')
      .neq('rodada_id', rota.rodada_id)
      .limit(1)
      .maybeSingle()

    const payload = {
      ...rota,
      distancia_metros_google: cacheOk?.distancia_metros_google ?? null,
      km_google_maps: cacheOk?.km_google_maps ?? null,
      duracao_segundos_google: cacheOk?.duracao_segundos_google ?? null,
      polyline_google: cacheOk?.polyline_google ?? null,
      legs_json: cacheOk?.legs_json ?? null,
      request_json: cacheOk?.request_json ?? null,
      response_json: cacheOk?.response_json ?? null,
      google_status: cacheOk ? 'reutilizada' : rota.google_status,
      google_erro: cacheOk ? null : rota.google_erro,
    }

    const { data, error } = await supabase
      .from('rotas_manifestos_google')
      .upsert(payload, { onConflict: 'rodada_id,manifesto_id' })
      .select('*')
      .single()
    if (error) throw error
    return data as RotaManifestoGoogle
  },

  async calcularRotaGoogleManifesto(rodadaId: string, manifestoId: string): Promise<RotaManifestoGoogle | null> {
    const rotaMontada = await this.montarRotaManifestoGoogle(rodadaId, manifestoId)
    const rotaSalva = await this.salvarOuAtualizarRotaPendente(rotaMontada)
    if (rotaSalva.google_status === 'reutilizada' || rotaSalva.google_status === 'sem_coordenadas' || rotaSalva.google_status === 'sem_paradas') {
      await this.atualizarFreteManifesto(rodadaId, manifestoId)
      return rotaSalva
    }

    const { data, error } = await supabase.functions.invoke('calcular-rota-google', {
      body: { rodada_id: rodadaId, manifesto_id: manifestoId },
    })

    if (error) {
      await supabase
        .from('rotas_manifestos_google')
        .update({
          google_status: 'erro',
          google_erro: 'Falha ao acionar Edge Function de cálculo de rota',
        })
        .eq('rodada_id', rodadaId)
        .eq('manifesto_id', manifestoId)
      return null
    }

    const rotaFinal = (data?.rota as RotaManifestoGoogle | undefined) ?? rotaSalva
    await this.atualizarFreteManifesto(rodadaId, manifestoId)
    return rotaFinal
  },

  async calcularRotasGoogleRodada(rodadaId: string): Promise<void> {
    const { data: manifestos, error } = await supabase
      .from('manifestos_roteirizacao')
      .select('manifesto_id')
      .eq('rodada_id', rodadaId)
      .order('manifesto_id')
    if (error) throw error
    const fila = (manifestos ?? []).map((m) => String(m.manifesto_id))
    for (let i = 0; i < fila.length; i += MAX_CONCORRENCIA_ROTAS_GOOGLE) {
      const lote = fila.slice(i, i + MAX_CONCORRENCIA_ROTAS_GOOGLE)
      await Promise.all(lote.map(async (manifestoId) => {
        try {
          const rota = await this.calcularRotaGoogleManifesto(rodadaId, manifestoId)
          console.log('[GOOGLE ROUTES]', {
            rodada_id: rodadaId,
            manifesto_id: manifestoId,
            status: rota?.google_status ?? 'erro',
            km_google_maps: rota?.km_google_maps ?? null,
          })
        } catch (err) {
          console.warn('[GOOGLE ROUTES] erro por manifesto', { rodada_id: rodadaId, manifesto_id: manifestoId, err })
        }
      }))
    }
  },

  async buscarRotaManifestoGoogle(rodadaId: string, manifestoId: string): Promise<RotaManifestoGoogle | null> {
    const { data, error } = await supabase
      .from('rotas_manifestos_google')
      .select('*')
      .eq('rodada_id', rodadaId)
      .eq('manifesto_id', manifestoId)
      .maybeSingle()
    if (error) throw error
    return (data as RotaManifestoGoogle | null) ?? null
  },

  async listarRotasManifestosGoogle(rodadaId?: string): Promise<RotaManifestoGoogle[]> {
    let query = supabase
      .from('rotas_manifestos_google')
      .select('*')
      .order('created_at', { ascending: false })
    if (rodadaId) query = query.eq('rodada_id', rodadaId)
    const { data, error } = await query
    if (error) throw error
    return (data ?? []) as RotaManifestoGoogle[]
  },

  async salvarOrdemManifestoItens(rodadaId: string, manifestoId: string, itens: ManifestoItemRoteirizacao[]): Promise<void> {
    const updates = itens
      .sort((a, b) => a.sequencia - b.sequencia)
      .map((item, index) =>
        supabase
          .from('manifestos_itens')
          .update({ sequencia: index + 1, updated_at: new Date().toISOString() })
          .eq('id', item.id)
          .eq('rodada_id', rodadaId)
          .eq('manifesto_id', manifestoId)
      )

    const resultados = await Promise.all(updates)
    const erro = resultados.find((res) => res.error)?.error
    if (erro) throw erro

      try {
      await this.calcularRotaGoogleManifesto(rodadaId, manifestoId)
      await this.atualizarFreteManifesto(rodadaId, manifestoId)
    } catch (error) {
      await supabase
        .from('rotas_manifestos_google')
        .update({
          google_status: 'erro',
          google_erro: 'Falha ao recalcular rota após alteração manual de sequência',
        })
        .eq('rodada_id', rodadaId)
        .eq('manifesto_id', manifestoId)
      console.warn('[GOOGLE ROUTES] falha ao recalcular após salvar ordem', error)
      await this.atualizarFreteManifesto(rodadaId, manifestoId)
    }
  },

  async verificarMotor(): Promise<boolean> {
    try {
      const endpoint = buildMotor2Url(MOTOR_2_HEALTH_PATH)
      if (import.meta.env.DEV) {
        console.debug('[Motor2] verificando saúde', { endpoint, method: 'GET' })
      }

      const response = await fetch(endpoint, {
        signal: AbortSignal.timeout(5000),
      })
      return response.ok
    } catch {
      return false
    }
  },
}
