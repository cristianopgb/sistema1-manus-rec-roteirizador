import { supabase } from '@/lib/supabase'
import { Filial } from '@/types'

export const filiaisService = {
  async listar(): Promise<Filial[]> {
    const { data, error } = await supabase
      .from('filiais')
      .select('*')
      .order('nome')
    if (error) throw error
    return data as Filial[]
  },

  async buscarAtivas(): Promise<Filial[]> {
    const { data, error } = await supabase
      .from('filiais')
      .select('*')
      .eq('ativo', true)
      .order('nome')
    if (error) throw error
    return data as Filial[]
  },

  async buscarPorId(id: string): Promise<Filial> {
    const { data, error } = await supabase
      .from('filiais')
      .select('*')
      .eq('id', id)
      .single()
    if (error) throw error
    return data as Filial
  },

  async criar(filial: Omit<Filial, 'id' | 'created_at'>): Promise<Filial> {
    const { data, error } = await supabase
      .from('filiais')
      .insert(filial)
      .select()
      .single()
    if (error) throw error
    return data as Filial
  },

  async atualizar(id: string, filial: Partial<Filial>): Promise<Filial> {
    const { data, error } = await supabase
      .from('filiais')
      .update(filial)
      .eq('id', id)
      .select()
      .single()
    if (error) throw error
    return data as Filial
  },

  async alternarAtivo(id: string, ativo: boolean): Promise<void> {
    const { error } = await supabase
      .from('filiais')
      .update({ ativo })
      .eq('id', id)
    if (error) throw error
  },
}
