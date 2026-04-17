import { supabase } from '@/lib/supabase'
import { anttService } from './antt.service'
import {
  PayloadMotor, RespostaMotor, ManifestoComFrete,
  RodadaRoteirizacao, FiltrosRoteirizacao, CarteiraCarga,
  Filial, Veiculo
} from '@/types'

const MOTOR_URL = import.meta.env.VITE_MOTOR_URL || 'http://localhost:8000'

export const roteirizacaoService = {

  /**
   * Dispara a roteirização: monta o payload, chama o motor, calcula frete ANTT e salva a rodada
   */
  async roteirizar(
    filial: Filial,
    veiculos: Veiculo[],
    carteira: CarteiraCarga[],
    filtros: FiltrosRoteirizacao,
    usuarioId: string
  ): Promise<{ rodada: RodadaRoteirizacao; manifestos: ManifestoComFrete[] }> {
    const inicio = Date.now()

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
        codigo: v.codigo,
        placa: v.placa,
        tipo: v.tipo,
        capacidade_peso_kg: v.capacidade_peso_kg,
        capacidade_volume_m3: v.capacidade_volume_m3,
        num_eixos: v.num_eixos,
        max_km_distancia: v.max_km_distancia,
        max_entregas: v.max_entregas,
        ocupacao_minima_perc: v.ocupacao_minima_perc,
        ocupacao_maxima_perc: v.ocupacao_maxima_perc,
        motorista: v.motorista,
      })),
      carteira,
    }

    // 2. Chamar o motor Python
    let resposta: RespostaMotor
    try {
      const response = await fetch(`${MOTOR_URL}/roteirizar`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(180_000), // 3 minutos
      })

      if (!response.ok) {
        throw new Error(`Motor retornou HTTP ${response.status}`)
      }

      resposta = await response.json() as RespostaMotor
    } catch (err) {
      const mensagem = err instanceof Error ? err.message : 'Erro de comunicação com o motor'
      throw new Error(`Falha ao comunicar com o Motor de Roteirização: ${mensagem}`)
    }

    if (resposta.status === 'erro') {
      throw new Error(resposta.erro?.mensagem || 'O motor retornou um erro desconhecido')
    }

    // 3. Calcular frete mínimo ANTT para cada manifesto
    const tabelaAntt = await anttService.listar()
    const manifestosComFrete: ManifestoComFrete[] = await Promise.all(
      resposta.manifestos.map(async (manifesto) => {
        // Determinar tipo de carga (padrão: 5 = Carga Geral)
        const tipoCargaId = 5
        const numEixos = manifesto.num_eixos || 2

        // Buscar coeficiente na tabela ANTT
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

  /**
   * Listar histórico de rodadas
   */
  async listarRodadas(filialId?: string): Promise<RodadaRoteirizacao[]> {
    let query = supabase
      .from('rodadas_roteirizacao')
      .select('*, filiais:filial_id(nome), perfis_usuarios:usuario_id(nome)')
      .order('created_at', { ascending: false })
      .limit(100)

    if (filialId) query = query.eq('filial_id', filialId)

    const { data, error } = await query
    if (error) throw error

    return (data || []).map((r) => ({
      ...r,
      filial_nome: (r.filiais as { nome: string } | null)?.nome,
      usuario_nome: (r.perfis_usuarios as { nome: string } | null)?.nome,
    })) as RodadaRoteirizacao[]
  },

  /**
   * Buscar rodada por ID com resposta completa do motor
   */
  async buscarRodada(id: string): Promise<RodadaRoteirizacao> {
    const { data, error } = await supabase
      .from('rodadas_roteirizacao')
      .select('*, filiais:filial_id(nome), perfis_usuarios:usuario_id(nome)')
      .eq('id', id)
      .single()
    if (error) throw error
    return {
      ...data,
      filial_nome: (data.filiais as { nome: string } | null)?.nome,
      usuario_nome: (data.perfis_usuarios as { nome: string } | null)?.nome,
    } as RodadaRoteirizacao
  },

  /**
   * Verificar saúde do motor
   */
  async verificarMotor(): Promise<boolean> {
    try {
      const response = await fetch(`${MOTOR_URL}/health`, {
        signal: AbortSignal.timeout(5000),
      })
      return response.ok
    } catch {
      return false
    }
  },
}
