import { supabase } from '@/lib/supabase'
import { TabelaAntt } from '@/types'

export const anttService = {
  async listar(): Promise<TabelaAntt[]> {
    const { data, error } = await supabase
      .from('tabela_antt')
      .select('*')
      .eq('ativa', true)
      .order('codigo_tipo', { ascending: true })
      .order('num_eixos', { ascending: true })
    if (error) {
      console.error('[ANTT] Falha ao carregar tabela_antt', error)
      return []
    }
    return data as TabelaAntt[]
  },

  async buscarCoeficiente(tipoCargaId: number, numEixos: number): Promise<TabelaAntt | null> {
    const { data, error } = await supabase
      .from('tabela_antt')
      .select('*')
      .eq('codigo_tipo', tipoCargaId)
      .eq('num_eixos', numEixos)
      .eq('ativa', true)
      .single()
    if (error) {
      console.error('[ANTT] Falha ao buscar coeficiente na tabela_antt', { tipoCargaId, numEixos, error })
      return null
    }
    return data as TabelaAntt
  },

  async atualizar(id: string, dados: Partial<TabelaAntt>): Promise<TabelaAntt> {
    const { data, error } = await supabase
      .from('tabela_antt')
      .update({ ...dados, updated_at: new Date().toISOString() })
      .eq('id', id)
      .select()
      .single()
    if (error) throw error
    return data as TabelaAntt
  },

  async importarTabela(registros: Omit<TabelaAntt, 'id' | 'updated_at' | 'created_at'>[]): Promise<void> {
    // Desativar registros antigos
    await supabase.from('tabela_antt').update({ ativa: false }).eq('ativa', true)
    // Inserir novos
    const { error } = await supabase.from('tabela_antt').insert(
      registros.map((r) => ({ ...r, updated_at: new Date().toISOString() }))
    )
    if (error) throw error
  },

  /**
   * Calcula o frete mínimo ANTT para um manifesto
   * Fórmula: (km * coef_ccd) + coef_cc
   */
  calcularFreteMinimo(
    kmEstimado: number,
    coeficienteDeslocamento: number,
    coeficienteCargaDescarga: number
  ): number {
    return kmEstimado * coeficienteDeslocamento + coeficienteCargaDescarga
  },
}
