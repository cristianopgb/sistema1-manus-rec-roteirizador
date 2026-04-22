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

const pickFirstText = (...values: unknown[]): string | null => {
  for (const value of values) {
    const text = toText(value)
    if (text) return text
  }
  return null
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

const extrairManifestosResposta = (resposta: RespostaMotor): Array<Record<string, unknown>> => {
  const manifestosFechados = toRecordArray(resposta.manifestos_fechados)
  const manifestosCompostos = toRecordArray(resposta.manifestos_compostos)
  const manifestosResultado = toRecordArray(resposta.resultado_roteirizacao)
  const manifestosM7 = toRecordArray(resposta.manifestos_m7)
  const manifestosLegados = Array.isArray(resposta.manifestos)
    ? resposta.manifestos.map((item) => item as unknown as Record<string, unknown>)
    : []

  const manifestos = [
    ...manifestosFechados,
    ...manifestosCompostos,
    ...manifestosResultado,
  ]

  return manifestos.length ? manifestos : (manifestosM7.length ? manifestosM7 : manifestosLegados)
}

const extrairItensManifestoRaw = (manifestoRaw: Record<string, unknown>): Array<Record<string, unknown>> => {
  const colecoes = [
    manifestoRaw.itens,
    manifestoRaw.entregas,
    manifestoRaw.documentos,
    manifestoRaw.lista_documentos,
    manifestoRaw.lista_entregas,
  ]

  for (const colecao of colecoes) {
    const itens = toRecordArray(colecao)
    if (itens.length) return itens
  }

  return []
}

const extrairItensResposta = (
  resposta: RespostaMotor,
  manifestosFonte: Array<Record<string, unknown>>,
): Array<Record<string, unknown>> => {
  const itensM7 = toRecordArray(resposta.itens_manifestos_sequenciados_m7)
  const itensResultado = toRecordArray(resposta.itens_manifestos)
  if (itensM7.length || itensResultado.length) {
    return [...itensM7, ...itensResultado]
  }

  return manifestosFonte.flatMap((manifestoRaw) => {
    const manifestoId = pickFirstText(manifestoRaw.manifesto_id, manifestoRaw.id_manifesto, manifestoRaw.numero_manifesto)
    const itensManifesto = extrairItensManifestoRaw(manifestoRaw)
    return itensManifesto.map((item) => ({ ...item, manifesto_id: manifestoId ?? item.manifesto_id ?? item.id_manifesto }))
  })
}

const extrairResumoRodada = (resposta: RespostaMotor, manifestosTotal: number, itensTotal: number, remanescentesTotal: number) => {
  const resumo = toRecord(resposta.resumo) ?? {}
  const resumoNegocio = toRecord(resposta.resumo_negocio) ?? {}
  const resumoExecucao = toRecord(resposta.resumo_execucao) ?? {}
  const logsPipeline = toRecordArray(resposta.logs_pipeline)
  const totalCarteiraLogs = logsPipeline.reduce((acc, item) => {
    const entrada = toNumber(item.entrada, 0)
    return entrada > 0 ? Math.max(acc, entrada) : acc
  }, 0)

  return {
    total_cargas_entrada: Math.trunc(toNumber(
      resumo.total_cargas_entrada,
      toNumber(
        resumoNegocio.total_carteira,
        toNumber(resumoExecucao.total_carteira, totalCarteiraLogs),
      ),
    )),
    total_manifestos: Math.trunc(toNumber(
      resumo.total_manifestos,
      toNumber(resumoNegocio.total_manifestos, manifestosTotal),
    )),
    total_itens_manifestados: Math.trunc(toNumber(
      resumo.total_itens_manifestados,
      toNumber(
        resumoNegocio.total_roteirizado,
        toNumber(resumoExecucao.total_itens_manifestados, itensTotal),
      ),
    )),
    total_nao_roteirizados: Math.trunc(toNumber(
      resumo.total_nao_roteirizados,
      toNumber(
        resumoNegocio.total_remanescente,
        toNumber(resumoExecucao.total_nao_roteirizados, remanescentesTotal),
      ),
    )),
    km_total_frota: toNumber(
      resumo.km_total_frota,
      toNumber(resumoNegocio.km_total, toNumber(resumoExecucao.km_total_frota, 0)),
    ),
    ocupacao_media_percentual: toNumber(
      resumo.ocupacao_media_percentual,
      toNumber(resumoNegocio.ocupacao_media, toNumber(resumoExecucao.ocupacao_media_percentual, 0)),
    ),
    tempo_processamento_ms: Math.trunc(toNumber(resumo.tempo_processamento_ms, toNumber(resumoExecucao.tempo_execucao_ms, 0))),
  }
}

export const roteirizacaoService = {
  async persistirEstruturaRodada(
    rodadaId: string,
    resposta: RespostaMotor,
    veiculos: Array<{ id: string; perfil?: string | null; tipo?: string | null; qtd_eixos?: number | null; capacidade_peso_kg?: number | null }>
  ): Promise<void> {
    console.log('[PERSISTENCIA] iniciando persistência estruturada da rodada', rodadaId)
    const manifestosM7 = toRecordArray(resposta.manifestos_m7)
    const itensM7 = toRecordArray(resposta.itens_manifestos_sequenciados_m7)
    const resumoManifestosM7 = toRecordArray(resposta.manifestos_sequenciamento_resumo_m7)
    const remanescentes = toRecord(resposta.remanescentes) ?? {}
    const resumoExecucao = toRecord(resposta.resumo_execucao) ?? {}
    const resumoNegocio = toRecord(resposta.resumo_negocio) ?? {}
    const auditoriaSerializacao = toRecord(resposta.auditoria_serializacao) ?? {}
    const auditoriaM7 = toRecord(resposta.auditoria_m7) ?? {}
    const manifestosFechados = toRecordArray(resposta.manifestos_fechados)
    const manifestosCompostos = toRecordArray(resposta.manifestos_compostos)
    const manifestosFonte = manifestosM7.length ? manifestosM7 : extrairManifestosResposta(resposta)
    const itensFonte = itensM7.length ? itensM7 : extrairItensResposta(resposta, manifestosFonte)
    void resumoExecucao
    void resumoNegocio
    void auditoriaSerializacao
    void auditoriaM7
    console.log('[EXTRACTOR] manifestos_m7:', manifestosM7.length)
    console.log('[EXTRACTOR] itens_manifestos_sequenciados_m7:', itensM7.length)
    console.log('[EXTRACTOR] manifestos_sequenciamento_resumo_m7:', resumoManifestosM7.length)
    console.log('[EXTRACTOR] remanescentes.nao_roteirizaveis_m3:', toRecordArray(remanescentes.nao_roteirizaveis_m3).length)
    console.log('[EXTRACTOR] remanescentes.saldo_final_roteirizacao:', toRecordArray(remanescentes.saldo_final_roteirizacao).length)
    console.log('[EXTRACTOR] manifestos_fechados:', manifestosFechados.length)
    console.log('[EXTRACTOR] manifestos_compostos:', manifestosCompostos.length)
    console.log('[EXTRACTOR] total manifestos final:', manifestosFonte.length)
    console.log('[PERSISTENCIA] manifestos_m7 recebidos:', manifestosM7.length)
    console.log('[PERSISTENCIA] itens_m7 recebidos:', itensM7.length)

    const mapManifestoEixos = new Map<string, number | null>()
    const mapManifestoVeiculoId = new Map<string, string | null>()
    const mapManifestoCapacidadeKg = new Map<string, number | null>()
    const resumoManifestoPorId = new Map<string, Record<string, unknown>>()
    resumoManifestosM7.forEach((resumoManifesto) => {
      const manifestoId = pickFirstText(resumoManifesto.manifesto_id, resumoManifesto.id_manifesto, resumoManifesto.numero_manifesto)
      if (!manifestoId) return
      resumoManifestoPorId.set(manifestoId, resumoManifesto)
    })
    const registrosManifestos = manifestosFonte.map((manifestoRaw) => {
      const manifestoId = pickFirstText(manifestoRaw.manifesto_id, manifestoRaw.id_manifesto, manifestoRaw.numero_manifesto) || crypto.randomUUID()
      const veiculoId = pickFirstText(manifestoRaw.veiculo_id)
      const veiculoPerfil = pickFirstText(manifestoRaw.veiculo_perfil, manifestoRaw.veiculo_codigo, manifestoRaw.perfil, manifestoRaw.tipo_veiculo)
      const veiculoMatch = veiculos.find((v) => v.id === veiculoId || (!!veiculoPerfil && (v.perfil === veiculoPerfil || v.tipo === veiculoPerfil)))
      const qtdEixos = Number.isFinite(toNumber(manifestoRaw.qtd_eixos, NaN))
        ? toNumber(manifestoRaw.qtd_eixos, 0)
        : (veiculoMatch?.qtd_eixos ?? toNumber((manifestoRaw as any).num_eixos, 0))
      mapManifestoEixos.set(manifestoId, qtdEixos || null)
      mapManifestoVeiculoId.set(manifestoId, veiculoMatch?.id || veiculoId || null)
      mapManifestoCapacidadeKg.set(manifestoId, pickFirstNumber(manifestoRaw.capacidade_peso_kg, manifestoRaw.capacidade_kg, manifestoRaw.capacidade) ?? (veiculoMatch?.capacidade_peso_kg ?? null))
      return {
        rodada_id: rodadaId,
        manifesto_id: manifestoId,
        origem_modulo: pickFirstText(manifestoRaw.origem_modulo),
        tipo_manifesto: pickFirstText(manifestoRaw.tipo_manifesto, manifestoRaw.grupo_manifesto),
        veiculo_perfil: veiculoPerfil,
        veiculo_tipo: pickFirstText(manifestoRaw.veiculo_tipo, manifestoRaw.tipo_veiculo),
        veiculo_id: veiculoMatch?.id || veiculoId,
        qtd_eixos: qtdEixos || null,
        exclusivo_flag: Boolean(manifestoRaw.exclusivo_flag),
        peso_total: toNumber(manifestoRaw.peso_total, toNumber((manifestoRaw as any).total_peso_kg, 0)),
        km_total: toNumber(manifestoRaw.km_total, toNumber((manifestoRaw as any).km_estimado, 0)),
        ocupacao: toNumber(manifestoRaw.ocupacao, toNumber((manifestoRaw as any).ocupacao_percentual, 0)),
        qtd_entregas: Math.trunc(toNumber(manifestoRaw.qtd_entregas, toNumber((manifestoRaw as any).total_entregas, 0))),
        qtd_clientes: Math.trunc(toNumber(manifestoRaw.qtd_clientes, 0)),
      }
    })

    const eixosVeiculoMap = new Map<string, number>()
    mapManifestoVeiculoId.forEach((veiculoId) => {
      if (!veiculoId) return
      const veiculo = veiculos.find((v) => v.id === veiculoId)
      if (veiculo?.qtd_eixos) eixosVeiculoMap.set(veiculoId, veiculo.qtd_eixos)
    })

    const tabelaAntt = await anttService.listar()
    const anttCargaGeral = tabelaAntt.filter((item) => item.codigo_tipo === 5)
    const registrosItens = itensFonte
      .filter((item) => {
        const manifestoId = pickFirstText(item.manifesto_id, item.id_manifesto, item.numero_manifesto)
        return !!manifestoId
      })
      .map((item, index) => {
        const manifestoId = pickFirstText(item.manifesto_id, item.id_manifesto, item.numero_manifesto) || 'sem_manifesto'
        const seq = Math.trunc(toNumber(item.sequencia, index + 1))
        return {
          rodada_id: rodadaId,
          manifesto_id: manifestoId,
          sequencia: seq > 0 ? seq : index + 1,
          nro_documento: pickFirstText(item.nro_documento, item.nro_doc, item.doc_ctrc),
          destinatario: pickFirstText(item.destinatario, item.cliente, item.destin),
          cidade: pickFirstText(item.cidade, item.cidad, item.cidade_dest),
          uf: pickFirstText(item.uf),
          peso: toNumber(item.peso, toNumber(item.peso_kg, 0)),
          distancia_km: toNumber(item.distancia_km, toNumber(item.km, 0)),
          inicio_entrega: pickFirstText(item.inicio_entrega, item.hora_agenda),
          fim_entrega: pickFirstText(item.fim_entrega),
          latitude: pickFirstNumber(item.latitude, item.latitude_destinatario),
          longitude: pickFirstNumber(item.longitude, item.longitude_destinatario),
        }
      })

    const agregadosManifesto = new Map<string, {
      qtd_entregas: number
      destinatarios: Set<string>
      peso_total: number
      distancia_total: number
    }>()
    registrosItens.forEach((item) => {
      const atual = agregadosManifesto.get(item.manifesto_id) ?? {
        qtd_entregas: 0,
        destinatarios: new Set<string>(),
        peso_total: 0,
        distancia_total: 0,
      }
      atual.qtd_entregas += 1
      if (item.destinatario) atual.destinatarios.add(item.destinatario)
      atual.peso_total += toNumber(item.peso, 0)
      atual.distancia_total += toNumber(item.distancia_km, 0)
      agregadosManifesto.set(item.manifesto_id, atual)
    })

    const manifestosComFrete = registrosManifestos.map((registro) => {
      const agregado = agregadosManifesto.get(registro.manifesto_id)
      const resumoM7 = resumoManifestoPorId.get(registro.manifesto_id)
      const pesoTotal = agregado ? agregado.peso_total : toNumber(registro.peso_total, 0)
      const kmResumo = pickFirstNumber(
        resumoM7?.km_total,
        resumoM7?.km_total_manifesto,
        resumoM7?.distancia_total_km,
        resumoM7?.distancia_km_total,
      )
      const kmTotal = kmResumo ?? pickFirstNumber(registro.km_total, agregado?.distancia_total, 0) ?? 0
      const capacidadeKg = mapManifestoCapacidadeKg.get(registro.manifesto_id) ?? null
      const ocupacao = capacidadeKg && capacidadeKg > 0
        ? (pesoTotal / capacidadeKg) * 100
        : toNumber(registro.ocupacao, 0)
      const qtdEixos = registro.qtd_eixos ?? (registro.veiculo_id ? eixosVeiculoMap.get(registro.veiculo_id) ?? null : null)
      const coef = anttCargaGeral.find((item) => item.num_eixos === qtdEixos)
      return {
        ...registro,
        qtd_entregas: agregado?.qtd_entregas ?? toNumber(registro.qtd_entregas, 0),
        qtd_clientes: agregado?.destinatarios.size ?? toNumber(registro.qtd_clientes, 0),
        peso_total: pesoTotal,
        km_total: kmTotal,
        ocupacao,
        qtd_eixos: qtdEixos,
        frete_minimo: anttService.calcularFreteMinimo(kmTotal, coef?.coef_ccd ?? 0),
      }
    })
    const naoRoteirizaveisM3 = toRecordArray(remanescentes.nao_roteirizaveis_m3)
    const saldoFinalRoteirizacao = toRecordArray(remanescentes.saldo_final_roteirizacao)
    const registrosRemanescentes = [...naoRoteirizaveisM3, ...saldoFinalRoteirizacao].map((item) => ({
      rodada_id: rodadaId,
      nro_documento: pickFirstText(item.nro_documento),
      destinatario: pickFirstText(item.destinatario),
      cidade: pickFirstText(item.cidade),
      uf: pickFirstText(item.uf),
      motivo: pickFirstText(item.motivo),
      etapa_origem: pickFirstText((item as unknown as Record<string, unknown>).etapa_origem, item.status_triagem),
      grupo_remanescente: naoRoteirizaveisM3.includes(item) ? 'nao_roteirizaveis_m3' : 'saldo_final_roteirizacao',
      payload_apoio_json: toRecord(item.payload_apoio_json) ?? null,
    }))
    const totalRemanescentes = registrosRemanescentes.length
    console.log('[PERSISTENCIA] remanescentes totais:', totalRemanescentes)

    const resumoRodada = extrairResumoRodada(
      resposta,
      manifestosComFrete.length,
      registrosItens.length,
      registrosRemanescentes.length,
    )
    const registroEstatistica = {
      rodada_id: rodadaId,
      total_carteira: resumoRodada.total_cargas_entrada,
      total_roteirizado: resumoRodada.total_itens_manifestados,
      total_remanescente: resumoRodada.total_nao_roteirizados,
      total_manifestos: resumoRodada.total_manifestos,
      km_total: resumoRodada.km_total_frota,
      ocupacao_media: resumoRodada.ocupacao_media_percentual,
      tempo_execucao_ms: resumoRodada.tempo_processamento_ms,
    }

    await supabase.from('manifestos_roteirizacao').delete().eq('rodada_id', rodadaId)
    await supabase.from('manifestos_itens').delete().eq('rodada_id', rodadaId)
    await supabase.from('remanescentes_roteirizacao').delete().eq('rodada_id', rodadaId)
    await supabase.from('estatisticas_roteirizacao').delete().eq('rodada_id', rodadaId)

    if (manifestosComFrete.length) {
      const { error } = await supabase.from('manifestos_roteirizacao').insert(manifestosComFrete)
      if (error) throw error
    }
    const totalManifestosSalvos = manifestosComFrete.length
    console.log('[PERSISTENCIA] manifestos salvos:', totalManifestosSalvos)
    console.log('[PERSISTENCIA] manifestos atualizados com agregados:', totalManifestosSalvos)

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

    const { error: estError } = await supabase.from('estatisticas_roteirizacao').upsert(registroEstatistica)
    if (estError) throw estError
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
    const manifestosEstruturados = extrairManifestosResposta(resposta)
    const itensEstruturados = extrairItensResposta(resposta, manifestosEstruturados)
    const remanescentes = Array.isArray(resposta.nao_roteirizados) ? resposta.nao_roteirizados.length : 0
    const resumoResposta = extrairResumoRodada(
      resposta,
      manifestosEstruturados.length,
      itensEstruturados.length,
      remanescentes,
    )

    let statusFinal: RodadaRoteirizacao['status'] = 'erro'
    let erroPosRetorno: string | null = null
    let rodadaData: { id?: string } | null = null
    let manifestosComFrete: ManifestoComFrete[] = []

    try {
      if (resposta.status === 'erro') {
        throw new Error(resposta.erro?.mensagem || 'O motor retornou um erro desconhecido')
      }

      // 3. Calcular frete mínimo ANTT para cada manifesto (somente deslocamento da categoria Carga geral)
      const tabelaAntt = await anttService.listar()
      manifestosComFrete = await Promise.all(
        manifestosResposta.map(async (manifesto) => {
          const tipoCargaId = 5
          const numEixos = manifesto.num_eixos || 2

          const coef = tabelaAntt.find(
            (t) => t.codigo_tipo === tipoCargaId && t.num_eixos === numEixos
          )

          const coefDeslocamento = coef?.coef_ccd || 0
          const freteMinimo = anttService.calcularFreteMinimo(manifesto.km_estimado, coefDeslocamento)

          return {
            ...manifesto,
            frete_minimo_antt: freteMinimo,
            tipo_carga_antt: String(tipoCargaId),
            coeficiente_deslocamento: coefDeslocamento,
            coeficiente_carga_descarga: 0,
            aprovado: false,
            excluido: false,
          }
        })
      )

      await this.persistirEstruturaRodada(rodadaId, resposta, veiculos)
      statusFinal = resposta.status as RodadaRoteirizacao['status']
    } catch (erro) {
      console.error('[ROTEIRIZACAO] erro no fluxo pós-retorno', erro)
      erroPosRetorno = erro instanceof Error ? erro.message : 'Erro desconhecido no pós-retorno da roteirização'
      statusFinal = 'erro'
    } finally {
      const tempoMs = Date.now() - inicio
      const rodadaPayload = {
        status: statusFinal,
        total_cargas_entrada: resumoResposta.total_cargas_entrada,
        total_manifestos: resumoResposta.total_manifestos,
        total_itens_manifestados: resumoResposta.total_itens_manifestados,
        total_nao_roteirizados: resumoResposta.total_nao_roteirizados,
        km_total_frota: resumoResposta.km_total_frota,
        ocupacao_media_percentual: resumoResposta.ocupacao_media_percentual,
        tempo_processamento_ms: tempoMs,
        payload_enviado: payload as unknown as Record<string, unknown>,
        resposta_motor: resposta as unknown as Record<string, unknown>,
        erro_mensagem: statusFinal === 'erro' ? (erroPosRetorno || resposta.erro?.mensagem || 'Erro no pós-processamento') : null,
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
      rodadaData = data as { id?: string } | null
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
      total_cargas_entrada: resumoResposta.total_cargas_entrada,
      total_manifestos: resumoResposta.total_manifestos,
      total_itens_manifestados: resumoResposta.total_itens_manifestados,
      total_nao_roteirizados: resumoResposta.total_nao_roteirizados,
      km_total_frota: resumoResposta.km_total_frota,
      ocupacao_media_percentual: resumoResposta.ocupacao_media_percentual,
      tempo_processamento_ms: tempoMs,
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
