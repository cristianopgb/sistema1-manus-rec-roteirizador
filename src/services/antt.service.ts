import { supabase } from '@/lib/supabase'
import { TabelaAntt } from '@/types'

export const anttService = {
  async listar(): Promise<TabelaAntt[]> {
    const { data, error } = await supabase
      .from('tabela_antt')
      .select('*')
      .eq('ativo', true)
      .order('tipo_carga_id')
      .order('num_eixos')
    if (error) throw error
    return data as TabelaAntt[]
  },

  async buscarCoeficiente(tipoCargaId: number, numEixos: number): Promise<TabelaAntt | null> {
    const { data, error } = await supabase
      .from('tabela_antt')
      .select('*')
      .eq('tipo_carga_id', tipoCargaId)
      .eq('num_eixos', numEixos)
      .eq('ativo', true)
      .single()
    if (error) return null
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

  async importarTabela(registros: Omit<TabelaAntt, 'id' | 'updated_at' | 'updated_by'>[]): Promise<void> {
    // Desativar registros antigos
    await supabase.from('tabela_antt').update({ ativo: false }).eq('ativo', true)
    // Inserir novos
    const { error } = await supabase.from('tabela_antt').insert(
      registros.map((r) => ({ ...r, updated_at: new Date().toISOString() }))
    )
    if (error) throw error
  },

  /**
   * Calcula o frete mínimo ANTT para um manifesto
   * Fórmula: (km * coeficiente_deslocamento) + coeficiente_carga_descarga
   */
  calcularFreteMinimo(
    kmEstimado: number,
    coeficienteDeslocamento: number,
    coeficienteCargaDescarga: number
  ): number {
    return kmEstimado * coeficienteDeslocamento + coeficienteCargaDescarga
  },
}
