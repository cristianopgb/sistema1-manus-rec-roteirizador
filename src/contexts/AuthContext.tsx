import React, { createContext, useContext, useEffect, useState, useCallback } from 'react'
import { User, Session } from '@supabase/supabase-js'
import { supabase } from '@/lib/supabase'
import { withTimeout } from '@/lib/async'
import { UserProfile, Filial } from '@/types'

interface AuthContextType {
  user: User | null
  session: Session | null
  profile: UserProfile | null
  loading: boolean
  isMaster: boolean
  isRoteirizador: boolean
  filialId: string | null
  filialAtiva: Filial | null
  signIn: (email: string, password: string) => Promise<{ error: Error | null }>
  signOut: () => Promise<void>
  refreshProfile: () => Promise<void>
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [session, setSession] = useState<Session | null>(null)
  const [profile, setProfile] = useState<UserProfile | null>(null)
  const [filialAtiva, setFilialAtiva] = useState<Filial | null>(null)
  const [loading, setLoading] = useState(true)

  const fetchFilial = useCallback(async (filialId: string): Promise<Filial | null> => {
    try {
      const { data, error } = await withTimeout(
        supabase
          .from('filiais')
          .select('*')
          .eq('id', filialId)
          .single(),
        'Carregamento da filial ativa'
      )
      if (error) return null
      return data as Filial
    } catch {
      return null
    }
  }, [])

  const fetchProfile = useCallback(async (userId: string): Promise<UserProfile | null> => {
    try {
      const { data, error } = await withTimeout(
        supabase
          .from('usuarios_perfil')
          .select('id, email, nome, perfil, filial_id, ativo, created_at')
          .eq('id', userId)
          .single(),
        'Carregamento de perfil'
      )

      if (error) {
        console.error('Erro ao buscar perfil:', error)
        return null
      }

      return data as UserProfile
    } catch (err) {
      console.error('Erro inesperado ao buscar perfil:', err)
      return null
    }
  }, [])

  const refreshProfile = useCallback(async () => {
    if (user) {
      const p = await fetchProfile(user.id)
      setProfile(p)
      if (p?.filial_id) {
        const f = await fetchFilial(p.filial_id)
        setFilialAtiva(f)
      } else {
        setFilialAtiva(null)
      }
    }
  }, [user, fetchProfile, fetchFilial])

  useEffect(() => {
    supabase.auth.getSession()
      .then(({ data: { session }, error }) => {
        if (error) {
          console.error('[Auth] Falha ao obter sessão inicial:', error)
        }

        setSession(session)
        setUser(session?.user ?? null)
        if (session?.user) {
          fetchProfile(session.user.id).then(async (p) => {
            if (!p) {
              console.error('[Auth] Sessão existe, mas perfil não foi encontrado', {
                userId: session.user.id,
                email: session.user.email,
              })
            }

            setProfile(p)
            if (p?.filial_id) {
              const f = await fetchFilial(p.filial_id)
              if (!f) {
                console.error('[Auth] Perfil possui filial_id, mas filial não foi carregada', {
                  userId: session.user.id,
                  filialId: p.filial_id,
                })
              }
              setFilialAtiva(f)
            }
            setLoading(false)
          })
        } else {
          setLoading(false)
        }
      })
      .catch((err) => {
        console.error('[Auth] Erro inesperado ao inicializar sessão:', err)
        setLoading(false)
      })

    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      async (_event, session) => {
        setSession(session)
        setUser(session?.user ?? null)
        if (session?.user) {
          const p = await fetchProfile(session.user.id)
          setProfile(p)
          if (p?.filial_id) {
            const f = await fetchFilial(p.filial_id)
            setFilialAtiva(f)
          } else {
            setFilialAtiva(null)
          }
        } else {
          setProfile(null)
          setFilialAtiva(null)
        }
        setLoading(false)
      }
    )

    return () => subscription.unsubscribe()
  }, [fetchProfile, fetchFilial])

  const signIn = async (email: string, password: string) => {
    try {
      const { error } = await supabase.auth.signInWithPassword({ email, password })
      return { error: error as Error | null }
    } catch (err) {
      return { error: err as Error }
    }
  }

  const signOut = async () => {
    await supabase.auth.signOut()
    setProfile(null)
    setFilialAtiva(null)
  }

  const isMaster = profile?.perfil === 'master'
  const isRoteirizador = profile?.perfil === 'roteirizador'
  const filialId = profile?.filial_id ?? null

  return (
    <AuthContext.Provider
      value={{
        user,
        session,
        profile,
        loading,
        isMaster,
        isRoteirizador,
        filialId,
        filialAtiva,
        signIn,
        signOut,
        refreshProfile,
      }}
    >
      {children}
    </AuthContext.Provider>
  )
}

export function useAuth() {
  const context = useContext(AuthContext)
  if (context === undefined) {
    throw new Error('useAuth deve ser usado dentro de AuthProvider')
  }
  return context
}
