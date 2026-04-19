import { supabase } from '@/lib/supabase'
import { anttService } from './antt.service'
import { buildMotor2Url } from '@/config/motor2'
import {
  PayloadMotor, RespostaMotor, ManifestoComFrete,
  RodadaRoteirizacao, FiltrosRoteirizacao, CarteiraCarga,
  Filial, Veiculo
} from '@/types'

export interface CarteiraFiltros {
  upload_id?: string
  tomad?: string
  destin?: string
  cidade?: string
  uf?: string
  mesoregiao?: string
  sub_regiao?: string
  status_validacao?: string
}

const aplicarFiltrosCarteira = (query: any, filtros?: CarteiraFiltros) => {
  let filtered = query.eq('status_validacao', filtros?.status_validacao || 'valida')

  if (filtros?.tomad) filtered = filtered.eq('tomad', filtros.tomad)
  if (filtros?.destin) filtered = filtered.eq('destin', filtros.destin)
  if (filtros?.cidade) filtered = filtered.eq('cidade', filtros.cidade)
  if (filtros?.uf) filtered = filtered.eq('uf', filtros.uf)
  if (filtros?.mesoregiao) filtered = filtered.eq('mesoregiao', filtros.mesoregiao)
  if (filtros?.sub_regiao) filtered = filtered.eq('sub_regiao', filtros.sub_regiao)

  return filtered
}

export const roteirizacaoService = {
  async buscarCarteiraPorUploadId(uploadId: string, filtros?: Omit<CarteiraFiltros, 'upload_id'>): Promise<CarteiraCarga[]> {
    const query = supabase
      .from('carteira_itens')
      .select('*')
      .eq('upload_id', uploadId)
      .order('linha_numero', { ascending: true })

    const { data, error } = await aplicarFiltrosCarteira(query, filtros)
    if (error) throw error

    return (data || []).map((item: any) => {
      const { id, upload_id, status_validacao, erro_validacao, created_at, dados_originais_json, ...rest } = item
      return ({
      ...rest,
      _carteira_item_id: id,
      _upload_id: upload_id,
      _status_validacao: status_validacao,
      _erro_validacao: erro_validacao,
      _created_at: created_at,
      _dados_originais: dados_originais_json,
      })
    }) as CarteiraCarga[]
  },

  async filtrarCarteiraItens(uploadId: string, filtros?: Omit<CarteiraFiltros, 'upload_id'>): Promise<CarteiraCarga[]> {
    return this.buscarCarteiraPorUploadId(uploadId, filtros)
  },

  /**
   * Dispara a roteirização: monta o payload, chama o motor, calcula frete ANTT e salva a rodada
   */
  async roteirizar(
    filial: Filial,
    veiculos: Veiculo[],
    uploadId: string,
    filtros: FiltrosRoteirizacao,
    usuarioId: string,
    filtrosCarteira?: Omit<CarteiraFiltros, 'upload_id'>
  ): Promise<{ rodada: RodadaRoteirizacao; manifestos: ManifestoComFrete[] }> {
    const inicio = Date.now()
    const carteira = await this.buscarCarteiraPorUploadId(uploadId, filtrosCarteira)

    if (!carteira.length) {
      throw new Error('Nenhum item válido encontrado para este upload.')
    }

    // 1. Montar payload para o motor
    const payload: PayloadMotor = {
      filial: {
        id: filial.id,
        codigo: filial.codigo,
        nome: filial.nome,
        cidade: filial.cidade,
        uf: filial.uf,
        latitude: filial.latitude,
        longitude: filial.longitude,
      },
      parametros: {
        data_base_roteirizacao: filtros.data_base,
        tipo_roteirizacao: filtros.tipo_roteirizacao,
        filial_id: filial.id,
        filial_nome: filial.nome,
      },
      veiculos: veiculos.map((v) => ({
        id: v.id,
        codigo: v.codigo || `VEIC-${v.id.slice(0, 8).toUpperCase()}`,
        placa: v.placa || 'N/I',
        tipo: v.tipo,
        capacidade_peso_kg: v.capacidade_peso_kg,
        capacidade_volume_m3: v.capacidade_volume_m3,
        num_eixos: v.num_eixos,
        max_km_distancia: v.max_km_distancia,
        max_entregas: v.max_entregas,
        ocupacao_minima_perc: v.ocupacao_minima_perc,
        ocupacao_maxima_perc: v.ocupacao_maxima_perc,
        motorista: v.motorista || undefined,
      })),
      carteira,
    }

    // 2. Chamar o motor Python
    let resposta: RespostaMotor
    try {
      const endpoint = buildMotor2Url('/roteirizar')
      if (import.meta.env.DEV) {
        console.debug('[Motor2] iniciando chamada', { endpoint, method: 'POST' })
      }

      const response = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(180_000),
      })

      if (!response.ok) {
        throw new Error(`Motor retornou HTTP ${response.status}`)
      }

      resposta = await response.json() as RespostaMotor
    } catch (err) {
      const mensagem = err instanceof Error ? err.message : 'Erro de comunicação com o motor'
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
          (t) => t.tipo_carga_id === tipoCargaId && t.num_eixos === numEixos
        )

        const coefDeslocamento = coef?.coeficiente_deslocamento || 0
        const coefCargaDescarga = coef?.coeficiente_carga_descarga || 0
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
      id: rodadaData?.id || crypto.randomUUID(),
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
      const endpoint = buildMotor2Url('/health')
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
