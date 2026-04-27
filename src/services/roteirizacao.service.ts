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
  ManifestoRoteirizacaoDetalhe, ManifestoItemRoteirizacao, RemanescenteRoteirizacao, EstatisticasRoteirizacao
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
  const inicioEntregaNormalizado = normalizeHorarioJanela(item.inicio_entrega)
  const fimEnNormalizado = normalizeHorarioJanela(item.fim_entrega)
  const dataDesNormalizada = normalizeDataDesDataNF(item.data_des)
  const dataNFNormalizada = normalizeDataDesDataNF(item.data_nf)
  const dleNormalizada = normalizeDle(item.dle)
  const agendamNormalizada = normalizeAgendam(item.agendam)
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
        dleOriginal: item.dle,
        dleNormalizada,
        agendamOriginal: item.agendam,
        agendamNormalizada,
      })
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

const extrairMotivoRemanescenteSaldoFinal = (item: Record<string, unknown>): string => {
  return (
    toText(item.motivo_detalhado_m6_2) ||
    toText(item.motivo_final_remanescente_m6_2) ||
    toText(item.motivo_final_remanescente_m5_4) ||
    toText(item.motivo_final_remanescente_m5_3) ||
    toText(item.motivo) ||
    toText(item.status_triagem) ||
    'Saldo final da roteirização'
  )
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
    console.log('[PERSISTENCIA] iniciando persistência estruturada da rodada', rodadaId)
    void veiculos
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

    const registrosManifestos = manifestosM7.map((manifestoRaw, index) => {
      const manifestoId = toText(manifestoRaw.manifesto_id)
      if (!manifestoId) throw new Error(`Manifesto M7 inválido: índice ${index} sem manifesto_id`)
      const perfilFinal = toText(manifestoRaw.perfil_final_m6_2)
      if (!perfilFinal) throw new Error(`Manifesto M7 inválido: manifesto_id ${manifestoId} sem perfil_final_m6_2`)
      if (manifestoRaw.peso_final_m6_2 === undefined || manifestoRaw.peso_final_m6_2 === null) {
        throw new Error(`Manifesto M7 inválido: manifesto_id ${manifestoId} sem peso_final_m6_2`)
      }
      if (manifestoRaw.ocupacao_final_m6_2 === undefined || manifestoRaw.ocupacao_final_m6_2 === null) {
        throw new Error(`Manifesto M7 inválido: manifesto_id ${manifestoId} sem ocupacao_final_m6_2`)
      }
      if (manifestoRaw.km_total_estimado_m6_2 === undefined || manifestoRaw.km_total_estimado_m6_2 === null) {
        throw new Error(`Manifesto M7 inválido: manifesto_id ${manifestoId} sem km_total_estimado_m6_2`)
      }
      return {
        rodada_id: rodadaId,
        manifesto_id: manifestoId,
        veiculo_perfil: perfilFinal,
        veiculo_tipo: perfilFinal,
        peso_total: toNumber(manifestoRaw.peso_final_m6_2, Number.NaN),
        ocupacao: toNumber(manifestoRaw.ocupacao_final_m6_2, Number.NaN),
        km_total: toNumber(manifestoRaw.km_total_estimado_m6_2, Number.NaN),
        qtd_entregas: Math.trunc(toNumber(manifestoRaw.qtd_itens_final_m6_2, 0)),
        qtd_clientes: Math.trunc(toNumber(manifestoRaw.qtd_paradas_final_m6_2, 0)),
        origem_modulo: pickFirstText(manifestoRaw.origem_manifesto_modulo),
        tipo_manifesto: pickFirstText(manifestoRaw.origem_manifesto_tipo),
      }
    })

    const registrosItens = itensM7.map((item, index) => {
      const manifestoId = toText(item.manifesto_id)
      if (!manifestoId) throw new Error(`Item M7 inválido: id_linha_pipeline ${toText(item.id_linha_pipeline) ?? `indice_${index}`} sem manifesto_id`)
      const idLinhaPipeline = toText(item.id_linha_pipeline)
      if (!idLinhaPipeline) throw new Error(`Item M7 inválido: índice ${index} sem id_linha_pipeline`)
      const obrigatorios = ['nro_documento', 'destinatario', 'cidade', 'uf', 'peso_calculado', 'ordem_entrega_doc_m7', 'ordem_carregamento_doc_m7'] as const
      for (const campo of obrigatorios) {
        if (item[campo] === undefined || item[campo] === null || (typeof item[campo] === 'string' && String(item[campo]).trim() === '')) {
          throw new Error(`Item M7 inválido: id_linha_pipeline ${idLinhaPipeline} sem ${campo}`)
        }
      }
      const ordemParadaM7 = item.ordem_parada_m7 ?? item.ordem_entrega_parada_m7
      if (ordemParadaM7 === undefined || ordemParadaM7 === null || String(ordemParadaM7).trim() === '') {
        throw new Error(`Item M7 inválido: id_linha_pipeline ${idLinhaPipeline} sem ordem_parada_m7 ou ordem_entrega_parada_m7`)
      }
      return {
        rodada_id: rodadaId,
        manifesto_id: manifestoId,
        sequencia: Math.trunc(toNumber(item.ordem_entrega_doc_m7, Number.NaN)),
        nro_documento: toText(item.nro_documento),
        destinatario: toText(item.destinatario),
        cidade: toText(item.cidade),
        uf: toText(item.uf),
        peso: toNumber(item.peso_calculado, Number.NaN),
        distancia_km: toNumber(item.distancia_rodoviaria_est_km, 0),
        latitude: pickFirstNumber(item.latitude_destinatario),
        longitude: pickFirstNumber(item.longitude_destinatario),
      }
    })

    const registrosNaoRoteirizaveisM3 = remanescentesM7.nao_roteirizaveis_m3.map((item, index) => {
      const nroDocumento = toText(item.nro_documento)
      const destinatario = toText(item.destinatario)
      const cidade = toText(item.cidade)
      const uf = toText(item.uf)
      const motivoTriagem = toText(item.motivo_triagem)
      const statusTriagem = toText(item.status_triagem)
      if (!nroDocumento || !destinatario || !cidade || !uf) {
        throw new Error(`Remanescente M7 inválido (nao_roteirizaveis_m3): índice ${index} sem campos mínimos`)
      }
      return {
        rodada_id: rodadaId,
        nro_documento: nroDocumento,
        destinatario,
        cidade,
        uf,
        motivo: motivoTriagem || statusTriagem || 'Não roteirizável na triagem',
        etapa_origem: 'm3_triagem',
      }
    })

    const registrosSaldoFinal = remanescentesM7.saldo_final_roteirizacao.map((item, index) => {
      const nroDocumento = toText(item.nro_documento)
      const destinatario = toText(item.destinatario)
      const cidade = toText(item.cidade)
      const uf = toText(item.uf)
      if (!nroDocumento || !destinatario || !cidade || !uf) {
        throw new Error(`Remanescente M7 inválido (saldo_final_roteirizacao): índice ${index} sem campos mínimos`)
      }
      return {
        rodada_id: rodadaId,
        nro_documento: nroDocumento,
        destinatario,
        cidade,
        uf,
        motivo: extrairMotivoRemanescenteSaldoFinal(item),
        etapa_origem: 'saldo_final_roteirizacao',
      }
    })
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
    if (!Number.isFinite(registrosManifestos.reduce((acc, manifesto) => acc + toNumber(manifesto.peso_total, Number.NaN), 0))) {
      throw new Error('Manifesto M7 inválido: peso_final_m6_2 deve ser numérico em todos os manifestos')
    }
    if (!Number.isFinite(registrosManifestos.reduce((acc, manifesto) => acc + toNumber(manifesto.km_total, Number.NaN), 0))) {
      throw new Error('Manifesto M7 inválido: km_total_estimado_m6_2 deve ser numérico em todos os manifestos')
    }
    if (!Number.isFinite(registrosManifestos.reduce((acc, manifesto) => acc + toNumber(manifesto.ocupacao, Number.NaN), 0))) {
      throw new Error('Manifesto M7 inválido: ocupacao_final_m6_2 deve ser numérico em todos os manifestos')
    }
    if (!Number.isFinite(registrosItens.reduce((acc, item) => acc + toNumber(item.sequencia, Number.NaN), 0))) {
      throw new Error('Item M7 inválido: ordem_entrega_doc_m7 deve ser numérico em todos os itens')
    }
    if (!Number.isFinite(registrosItens.reduce((acc, item) => acc + toNumber(item.peso, Number.NaN), 0))) {
      throw new Error('Item M7 inválido: peso_calculado deve ser numérico em todos os itens')
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

    if (totalManifestosSalvos !== manifestosM7.length) {
      throw new Error(`Integridade M7 inválida: manifestos salvos ${totalManifestosSalvos} diferente de manifestos_m7 ${manifestosM7.length}`)
    }
    if (totalItensSalvos !== itensM7.length) {
      throw new Error(`Integridade M7 inválida: itens salvos ${totalItensSalvos} diferente de itens_manifestos_sequenciados_m7 ${itensM7.length}`)
    }
    const remanescentesEsperados = remanescentesM7.nao_roteirizaveis_m3.length + remanescentesM7.saldo_final_roteirizacao.length
    if (totalRemanescentesSalvos !== remanescentesEsperados) {
      throw new Error(`Integridade M7 inválida: remanescentes salvos ${totalRemanescentesSalvos} diferente do esperado ${remanescentesEsperados}`)
    }

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

    try {
      const response = await fetch(finalUrl, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(180_000),
      })

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
      const mensagem = err instanceof Error ? err.message : 'Erro de comunicação com o motor'
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
      supabase.from('remanescentes_roteirizacao').select('*').eq('rodada_id', rodadaId).order('created_at'),
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
