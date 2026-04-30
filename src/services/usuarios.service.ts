import { supabase } from '@/lib/supabase'
import { withTimeout, getErrorMessage } from '@/lib/async'
import { UserProfile } from '@/types'

const USUARIOS_TIMEOUT_MS = 20_000

export const usuariosService = {
  async listar(): Promise<UserProfile[]> {
    try {
      const { data, error } = await withTimeout(
        supabase
          .from('usuarios_perfil')
          .select('*, filiais:filial_id(nome)')
          .order('nome'),
        USUARIOS_TIMEOUT_MS,
        'Carregamento de usuários'
      )

      if (error) {
        console.error('[usuarios.service:listar] Supabase retornou erro', error)
        throw error
      }

      return (data || []).map((u) => ({
        ...u,
        filial_nome: (u.filiais as { nome: string } | null)?.nome,
      })) as UserProfile[]
    } catch (error) {
      console.error('[usuarios.service:listar] Falha ao carregar usuários', {
        message: getErrorMessage(error, 'Erro ao carregar usuários'),
        error,
      })
      throw error
    }
  },

  async criar(usuario: {
    email: string
    nome: string
    perfil: 'master' | 'roteirizador'
    filial_id: string | null
    password: string
  }): Promise<{ user: UserProfile | null; error: Error | null }> {
    const { data: signUpData, error: signUpError } = await withTimeout(
      supabase.auth.signUp({
        email: usuario.email,
        password: usuario.password,
      }),
      'Cadastro de autenticação'
    )

    if (signUpError || !signUpData.user?.id) {
      console.error('[usuarios.service:criar] Falha ao criar autenticação', { email: usuario.email, signUpError })
      return { user: null, error: (signUpError as unknown as Error) || new Error('Não foi possível criar autenticação do usuário.') }
    }

    const { data, error } = await withTimeout(
      supabase
        .from('usuarios_perfil')
        .upsert({
          id: signUpData.user.id,
          email: usuario.email,
          nome: usuario.nome,
          perfil: usuario.perfil,
          filial_id: usuario.filial_id,
          ativo: true,
        }, { onConflict: 'id' })
        .select()
        .single(),
      'Criação/atualização do perfil de usuário'
    )

    if (error) {
      console.error('[usuarios.service:criar] Falha ao atualizar perfil do usuário criado', { email: usuario.email, error })
      return { user: null, error: error as unknown as Error }
    }

    return { user: data as UserProfile, error: null }
  },

  async atualizar(id: string, dados: Partial<UserProfile>): Promise<UserProfile> {
    const { data, error } = await withTimeout(
      supabase
        .from('usuarios_perfil')
        .update(dados)
        .eq('id', id)
        .select()
        .single(),
      'Atualização de usuário'
    )

    if (error) {
      console.error('[usuarios.service:atualizar] Falha ao atualizar usuário', { id, error })
      throw error
    }

    return data as UserProfile
  },

  async alternarAtivo(id: string, ativo: boolean): Promise<void> {
    const { error } = await supabase
      .from('usuarios_perfil')
      .update({ ativo })
      .eq('id', id)

    if (error) {
      console.error('[usuarios.service:alternarAtivo] Falha ao alterar status', { id, ativo, error })
      throw error
    }
  },
}
