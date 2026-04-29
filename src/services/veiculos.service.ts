import { supabase } from '@/lib/supabase'
import { withTimeout } from '@/lib/async'
import { Veiculo } from '@/types'

export const veiculosService = {
  async listar(filialId?: string): Promise<Veiculo[]> {
    let query = supabase
      .from('veiculos')
      .select('*, filiais:filial_id(nome)')
      .order('tipo')
      .order('placa')

    if (filialId) query = query.eq('filial_id', filialId)

    const { data, error } = await withTimeout(query, 'Carregamento de veículos')
    if (error) throw error

    return (data || []).map((v) => ({
      ...v,
      filial_nome: (v.filiais as { nome: string } | null)?.nome,
    })) as Veiculo[]
  },

  async listarAtivos(filialId?: string): Promise<Veiculo[]> {
    let query = supabase
      .from('veiculos')
      .select('*, filiais:filial_id(nome)')
      .eq('ativo', true)
      .order('tipo')

    if (filialId) query = query.eq('filial_id', filialId)

    const { data, error } = await withTimeout(query, 'Carregamento de veículos ativos')
    if (error) throw error

    return (data || []).map((v) => ({
      ...v,
      filial_nome: (v.filiais as { nome: string } | null)?.nome,
    })) as Veiculo[]
  },

  async criar(veiculo: Omit<Veiculo, 'id' | 'created_at' | 'filial_nome'>): Promise<Veiculo> {
    const { data, error } = await withTimeout(
      supabase
        .from('veiculos')
        .insert(veiculo)
        .select()
        .single(),
      'Cadastro de veículo'
    )
    if (error) throw error
    return data as Veiculo
  },

  async criarEmLote(veiculos: Omit<Veiculo, 'id' | 'created_at' | 'filial_nome'>[]): Promise<Veiculo[]> {
    const { data, error } = await withTimeout(
      supabase
        .from('veiculos')
        .insert(veiculos)
        .select(),
      'Cadastro em lote de veículos'
    )
    if (error) throw error
    return (data || []) as Veiculo[]
  },

  async atualizar(id: string, veiculo: Partial<Veiculo>): Promise<Veiculo> {
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    const { filial_nome: _, ...payload } = veiculo
    const { data, error } = await withTimeout(
      supabase
        .from('veiculos')
        .update(payload)
        .eq('id', id)
        .select()
        .single(),
      'Atualização de veículo'
    )
    if (error) throw error
    return data as Veiculo
  },

  async alternarAtivo(id: string, ativo: boolean): Promise<void> {
    const { error } = await supabase
      .from('veiculos')
      .update({ ativo })
      .eq('id', id)
    if (error) throw error
  },
}
