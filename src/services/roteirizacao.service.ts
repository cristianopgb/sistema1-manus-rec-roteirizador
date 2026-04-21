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
  Filial, FiltrosCarteira, ConfiguracaoFrotaItem, CarteiraCargaContratoMotor
} from '@/types'
import { normalizeHorarioJanela } from '@/lib/time-normalizers'

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
  }

  return ({
  'Filial R': item.filial_r,
  Romane: item.romane,
  'Filial D': item.filial_d,
  'Série': item.serie,
  'Nro Doc.': item.nro_doc,
  'Data Des': item.data_des,
  'Data NF': item.data_nf,
  'D.L.E.': item.dle,
  'Agendam.': item.agendam,
  Palet: item.palet,
  Conf: item.conf,
  Peso: item.peso,
  'Vlr.Merc.': item.vlr_merc,
  'Qtd.': item.qtd,
  'Peso Cub.': item.peso_cubico,
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
  Latitude: item.latitude,
  Longitude: item.longitude,
  'Peso Calculo': item.peso_calculo,
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

export const roteirizacaoService = {
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

    if (resposta.status === 'erro') {
      await supabase
        .from('rodadas_roteirizacao')
        .update({
          status: 'erro',
          erro_mensagem: resposta.erro?.mensagem || 'O motor retornou um erro desconhecido',
          tempo_processamento_ms: Date.now() - inicio,
          resposta_motor: resposta as unknown as Record<string, unknown>,
          payload_enviado: payload as unknown as Record<string, unknown>,
        })
        .eq('id', rodadaId)
      throw new Error(resposta.erro?.mensagem || 'O motor retornou um erro desconhecido')
    }

    // 3. Calcular frete mínimo ANTT para cada manifesto
    const tabelaAntt = await anttService.listar()
    const manifestosComFrete: ManifestoComFrete[] = await Promise.all(
      resposta.manifestos.map(async (manifesto) => {
        const tipoCargaId = 5
        const numEixos = manifesto.num_eixos || 2

        const coef = tabelaAntt.find(
          (t) => t.codigo_tipo === tipoCargaId && t.num_eixos === numEixos
        )

        const coefDeslocamento = coef?.coef_ccd || 0
        const coefCargaDescarga = coef?.coef_cc || 0
        const freteMinimo = anttService.calcularFreteMinimo(
          manifesto.km_estimado,
          coefDeslocamento,
          coefCargaDescarga
        )

        return {
          ...manifesto,
          frete_minimo_antt: freteMinimo,
          tipo_carga_antt: String(tipoCargaId),
          coeficiente_deslocamento: coefDeslocamento,
          coeficiente_carga_descarga: coefCargaDescarga,
          aprovado: false,
          excluido: false,
        }
      })
    )

    // 4. Salvar rodada no Supabase
    const tempoMs = Date.now() - inicio
    const { data: rodadaData, error: rodadaError } = await supabase
      .from('rodadas_roteirizacao')
      .update({
        status: resposta.status,
        total_cargas_entrada: resposta.resumo.total_cargas_entrada,
        total_manifestos: resposta.resumo.total_manifestos,
        total_itens_manifestados: resposta.resumo.total_itens_manifestados,
        total_nao_roteirizados: resposta.resumo.total_nao_roteirizados,
        km_total_frota: resposta.resumo.km_total_frota,
        ocupacao_media_percentual: resposta.resumo.ocupacao_media_percentual,
        tempo_processamento_ms: tempoMs,
        payload_enviado: payload as unknown as Record<string, unknown>,
        resposta_motor: resposta as unknown as Record<string, unknown>,
      })
      .eq('id', rodadaId)
      .select()
      .single()

    if (rodadaError) {
      console.error('Erro ao salvar rodada:', rodadaError)
    }

    const rodada: RodadaRoteirizacao = {
      id: rodadaData?.id || rodadaId,
      filial_id: filial.id,
      filial_nome: filial.nome,
      usuario_id: usuarioId,
      upload_id: uploadId,
      status: resposta.status as RodadaRoteirizacao['status'],
      tipo_roteirizacao: filtros.tipo_roteirizacao,
      total_cargas_entrada: resposta.resumo.total_cargas_entrada,
      total_manifestos: resposta.resumo.total_manifestos,
      total_itens_manifestados: resposta.resumo.total_itens_manifestados,
      total_nao_roteirizados: resposta.resumo.total_nao_roteirizados,
      km_total_frota: resposta.resumo.km_total_frota,
      ocupacao_media_percentual: resposta.resumo.ocupacao_media_percentual,
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
