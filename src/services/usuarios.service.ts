import { supabase } from '@/lib/supabase'
import { UserProfile } from '@/types'

export const usuariosService = {
  async listar(): Promise<UserProfile[]> {
    const { data, error } = await supabase
      .from('usuarios_perfil')
      .select('*, filiais:filial_id(nome)')
      .order('nome')
    if (error) throw error
    return (data || []).map((u) => ({
      ...u,
      filial_nome: (u.filiais as { nome: string } | null)?.nome,
    })) as UserProfile[]
  },

  async criar(usuario: {
    email: string
    nome: string
    perfil: 'master' | 'roteirizador'
    filial_id: string | null
    password: string
  }): Promise<{ user: UserProfile | null; error: Error | null }> {
    // Criar usuário via Supabase Auth Admin (requer service role key no backend)
    // Por ora, inserir direto na tabela de perfis (o auth.users deve ser criado via Supabase Dashboard)
    const { data, error } = await supabase
      .from('usuarios_perfil')
      .insert({
        email: usuario.email,
        nome: usuario.nome,
        perfil: usuario.perfil,
        filial_id: usuario.filial_id,
        ativo: true,
      })
      .select()
      .single()

    if (error) return { user: null, error: error as unknown as Error }
    return { user: data as UserProfile, error: null }
  },

  async atualizar(id: string, dados: Partial<UserProfile>): Promise<UserProfile> {
    const { data, error } = await supabase
      .from('usuarios_perfil')
      .update(dados)
      .eq('id', id)
      .select()
      .single()
    if (error) throw error
    return data as UserProfile
  },

  async alternarAtivo(id: string, ativo: boolean): Promise<void> {
    const { error } = await supabase
      .from('usuarios_perfil')
      .update({ ativo })
      .eq('id', id)
    if (error) throw error
  },
}
