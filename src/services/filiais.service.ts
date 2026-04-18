import { supabase } from '@/lib/supabase'
import { withTimeout } from '@/lib/async'
import { Filial } from '@/types'

type FilialRow = Omit<Filial, 'ativo'> & { ativa?: boolean; ativo?: boolean }

const mapFilialRow = (row: FilialRow): Filial => ({
  ...row,
  ativo: row.ativo ?? row.ativa ?? true,
})

export const filiaisService = {
  async listar(): Promise<Filial[]> {
    const { data, error } = await withTimeout(
      supabase
        .from('filiais')
        .select('*')
        .order('nome'),
      'Carregamento de filiais'
    )
    if (error) throw error
    return (data || []).map((f) => mapFilialRow(f as FilialRow))
  },

  async buscarAtivas(): Promise<Filial[]> {
    const { data, error } = await withTimeout(
      supabase
        .from('filiais')
        .select('*')
        .eq('ativa', true)
        .order('nome'),
      'Carregamento de filiais ativas'
    )
    if (error) throw error
    return (data || []).map((f) => mapFilialRow(f as FilialRow))
  },

  async buscarPorId(id: string): Promise<Filial> {
    const { data, error } = await supabase
      .from('filiais')
      .select('*')
      .eq('id', id)
      .single()
    if (error) throw error
    return mapFilialRow(data as FilialRow)
  },

  async criar(filial: Omit<Filial, 'id' | 'created_at'>): Promise<Filial> {
    const { ativo, ...payload } = filial
    const { data, error } = await withTimeout(
      supabase
        .from('filiais')
        .insert({ ...payload, ativa: ativo })
        .select()
        .single(),
      'Cadastro de filial'
    )
    if (error) throw error
    return mapFilialRow(data as FilialRow)
  },

  async atualizar(id: string, filial: Partial<Filial>): Promise<Filial> {
    const { ativo, ...payload } = filial
    const patch = typeof ativo === 'boolean' ? { ...payload, ativa: ativo } : payload

    const { data, error } = await withTimeout(
      supabase
        .from('filiais')
        .update(patch)
        .eq('id', id)
        .select()
        .single(),
      'Atualização de filial'
    )
    if (error) throw error
    return mapFilialRow(data as FilialRow)
  },

  async alternarAtivo(id: string, ativo: boolean): Promise<void> {
    const { error } = await supabase
      .from('filiais')
      .update({ ativa: ativo })
      .eq('id', id)
    if (error) throw error
  },
}
