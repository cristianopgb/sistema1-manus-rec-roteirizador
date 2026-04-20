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
  Filial, FiltrosCarteira, ConfiguracaoFrotaItem
} from '@/types'

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

    return (data || []).map(normalizarCarteiraItem)
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

    // 1. Montar payload para o motor
    const payload: PayloadMotor = {
      rodada_id: rodadaId,
      upload_id: uploadId,
      usuario_id: usuarioId,
      filial_id: filial.id,
      data_base_roteirizacao: filtros.data_base,
      tipo_roteirizacao: filtros.tipo_roteirizacao,
      filtros_aplicados: filtros.filtros_aplicados,
      configuracao_frota: filtros.tipo_roteirizacao === 'frota' ? configuracaoFrota : [],
      carteira,
    }

    // 2. Chamar o motor Python
    let resposta: RespostaMotor
    const motorBaseUrl = getMotor2BaseUrl()
    const finalUrl = buildMotor2Url(MOTOR_2_ROTEIRIZAR_PATH)

    if (import.meta.env.DEV) {
      console.log('[Motor2] Base URL:', motorBaseUrl)
      console.log('[Motor2] Path:', MOTOR_2_ROTEIRIZAR_PATH)
      console.log('[Motor2] Final URL:', finalUrl)
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

      if (mensagem.includes('VITE_MOTOR_2_URL')) {
        throw new Error(`Configuração inválida do Motor 2: ${mensagem}`)
      }

      throw new Error(`Falha ao comunicar com o Motor de Roteirização: ${mensagem}`)
    }

    if (resposta.status === 'erro') {
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
      .insert({
        id: rodadaId,
        filial_id: filial.id,
        usuario_id: usuarioId,
        upload_id: uploadId,
        status: resposta.status,
        tipo_roteirizacao: filtros.tipo_roteirizacao,
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
