import React, { createContext, useContext, useEffect, useState, useCallback, useRef } from 'react'
import { User, Session } from '@supabase/supabase-js'
import { supabase } from '@/lib/supabase'
import { withTimeout, getErrorMessage } from '@/lib/async'
import { UserProfile, Filial } from '@/types'

const PROFILE_TIMEOUT_MS = 30_000
const FILIAL_TIMEOUT_MS = 20_000
const PROFILE_RETRY_ATTEMPTS = 2
const PROFILE_RETRY_DELAY_MS = 600
type AuthChangeEvent =
  | 'INITIAL_SESSION'
  | 'SIGNED_IN'
  | 'SIGNED_OUT'
  | 'TOKEN_REFRESHED'
  | 'USER_UPDATED'
  | 'PASSWORD_RECOVERY'

interface AuthContextType {
  user: User | null
  session: Session | null
  profile: UserProfile | null
  filialAtiva: Filial | null
  loading: boolean
  authLoading: boolean
  profileLoading: boolean
  profileError: string | null
  filialLoading: boolean
  filialError: string | null
  isMaster: boolean
  isRoteirizador: boolean
  filialId: string | null
  signIn: (email: string, password: string) => Promise<{ error: Error | null }>
  signOut: () => Promise<void>
  refreshProfile: () => Promise<void>
  reloadProfile: () => Promise<void>
  reloadFilial: () => Promise<void>
  reloadAuthContext: () => Promise<void>
}

const AuthContext = createContext<AuthContextType | undefined>(undefined)

const delay = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const [user, setUser] = useState<User | null>(null)
  const [session, setSession] = useState<Session | null>(null)
  const [profile, setProfile] = useState<UserProfile | null>(null)
  const [filialAtiva, setFilialAtiva] = useState<Filial | null>(null)

  const [authLoading, setAuthLoading] = useState(true)
  const [profileLoading, setProfileLoading] = useState(false)
  const [profileError, setProfileError] = useState<string | null>(null)
  const [filialLoading, setFilialLoading] = useState(false)
  const [filialError, setFilialError] = useState<string | null>(null)
  const userRef = useRef<User | null>(null)
  const profileRef = useRef<UserProfile | null>(null)

  useEffect(() => {
    userRef.current = user
  }, [user])

  useEffect(() => {
    profileRef.current = profile
  }, [profile])

  const fetchFilialById = useCallback(async (filialId: string): Promise<Filial | null> => {
    try {
      const { data, error } = await withTimeout(
        supabase
          .from('filiais')
          .select('*')
          .eq('id', filialId)
          .single(),
        FILIAL_TIMEOUT_MS,
        'Carregamento da filial ativa'
      )

      if (error) {
        console.error('[AuthContext] Falha ao buscar filial', { filialId, error })
        return null
      }

      return data as Filial
    } catch (error) {
      console.error('[AuthContext] Erro inesperado ao buscar filial', { filialId, error })
      return null
    }
  }, [])

  const fetchProfileByUserId = useCallback(async (userId: string): Promise<UserProfile | null> => {
    let lastError: unknown = null

    for (let attempt = 1; attempt <= PROFILE_RETRY_ATTEMPTS; attempt += 1) {
      try {
        const { data, error } = await withTimeout(
          supabase
            .from('usuarios_perfil')
            .select('id, email, nome, perfil, filial_id, ativo, created_at')
            .eq('id', userId)
            .single(),
          PROFILE_TIMEOUT_MS,
          'Carregamento de perfil'
        )

        if (error) {
          lastError = error
          console.error('[AuthContext] Tentativa de perfil retornou erro', {
            userId,
            attempt,
            error,
          })
        } else {
          return data as UserProfile
        }
      } catch (error) {
        lastError = error
        console.error('[AuthContext] Tentativa de perfil falhou', {
          userId,
          attempt,
          error,
        })
      }

      if (attempt < PROFILE_RETRY_ATTEMPTS) {
        await delay(PROFILE_RETRY_DELAY_MS)
      }
    }

    console.error('[AuthContext] Perfil indisponível após tentativas', { userId, lastError })
    return null
  }, [])

  const loadFilialFromProfile = useCallback(async (profileData: UserProfile | null, options?: { showLoading?: boolean }) => {
    const showLoading = options?.showLoading ?? true

    if (showLoading) {
      setFilialLoading(true)
    }

    try {
      if (!profileData) {
        setFilialAtiva(null)
        setFilialError(null)
        return
      }

      if (!profileData.filial_id) {
        setFilialAtiva(null)
        if (profileData.perfil === 'master') {
          setFilialError(null)
        } else {
          setFilialError('Perfil sem filial vinculada. Entre em contato com o administrador.')
        }
        return
      }

      setFilialError(null)

      const filial = await fetchFilialById(profileData.filial_id)
      if (!filial) {
        setFilialAtiva(null)
        setFilialError('Não foi possível carregar a filial do perfil.')
        return
      }

      setFilialAtiva(filial)
    } finally {
      if (showLoading) {
        setFilialLoading(false)
      }
    }
  }, [fetchFilialById])

  const loadProfileAndFilial = useCallback(async (
    currentUser: User,
    options?: { showLoading?: boolean; preserveDataOnFailure?: boolean }
  ) => {
    const showLoading = options?.showLoading ?? true
    const preserveDataOnFailure = options?.preserveDataOnFailure ?? false

    if (showLoading) {
      setProfileLoading(true)
    }

    try {
      setProfileError(null)

      const profileData = await fetchProfileByUserId(currentUser.id)
      if (profileData) {
        setProfile(profileData)
      }

      if (!profileData) {
        setProfileError('Não foi possível carregar o perfil. Tente novamente.')
        if (!preserveDataOnFailure) {
          setProfile(null)
          setFilialAtiva(null)
          setFilialError(null)
        }
        return
      }

      await loadFilialFromProfile(profileData, { showLoading })
    } finally {
      if (showLoading) {
        setProfileLoading(false)
      }
    }
  }, [fetchProfileByUserId, loadFilialFromProfile])

  const refreshProfile = useCallback(async () => {
    if (!user) return
    await loadProfileAndFilial(user)
  }, [user, loadProfileAndFilial])

  const reloadProfile = useCallback(async () => {
    if (!user) return
    await loadProfileAndFilial(user)
  }, [user, loadProfileAndFilial])

  const reloadFilial = useCallback(async () => {
    await loadFilialFromProfile(profile)
  }, [profile, loadFilialFromProfile])

  const reloadAuthContext = useCallback(async () => {
    if (!user) return
    await loadProfileAndFilial(user)
  }, [user, loadProfileAndFilial])

  useEffect(() => {
    let mounted = true

    supabase.auth.getSession()
      .then(async ({ data: { session: initialSession }, error }) => {
        if (!mounted) return

        if (error) {
          console.error('[AuthContext] Falha ao obter sessão inicial', error)
        }

        setSession(initialSession)
        setUser(initialSession?.user ?? null)
        setAuthLoading(false)

        if (initialSession?.user) {
          await loadProfileAndFilial(initialSession.user, { showLoading: true })
        } else {
          setProfile(null)
          setFilialAtiva(null)
          setProfileError(null)
          setFilialError(null)
        }
      })
      .catch((error) => {
        if (!mounted) return
        console.error('[AuthContext] Erro inesperado ao inicializar sessão', error)
        setAuthLoading(false)
      })

    const { data: { subscription } } = supabase.auth.onAuthStateChange(
      async (event, nextSession) => {
        if (!mounted) return

        const authEvent = event as AuthChangeEvent
        const prevUserId = userRef.current?.id ?? null
        const nextUserId = nextSession?.user?.id ?? null
        const userChanged = prevUserId !== nextUserId

        setSession(nextSession)
        setUser(nextSession?.user ?? null)
        setAuthLoading(false)

        if (!nextSession?.user) {
          setProfile(null)
          setFilialAtiva(null)
          setProfileError(null)
          setFilialError(null)
          setProfileLoading(false)
          setFilialLoading(false)
          return
        }

        if (authEvent === 'SIGNED_IN' || authEvent === 'USER_UPDATED' || userChanged) {
          await loadProfileAndFilial(nextSession.user, { showLoading: true })
          return
        }

        if (authEvent === 'TOKEN_REFRESHED' || authEvent === 'INITIAL_SESSION' || authEvent === 'PASSWORD_RECOVERY') {
          if (!profileRef.current || userChanged) {
            await loadProfileAndFilial(nextSession.user, {
              showLoading: false,
              preserveDataOnFailure: true,
            })
          }
        }
      }
    )

    return () => {
      mounted = false
      subscription.unsubscribe()
    }
  }, [loadProfileAndFilial])

  const signIn = async (email: string, password: string) => {
    try {
      const { error } = await supabase.auth.signInWithPassword({ email, password })
      return { error: error as Error | null }
    } catch (error) {
      console.error('[AuthContext] Erro no signIn', error)
      return { error: new Error(getErrorMessage(error, 'Erro ao autenticar usuário')) }
    }
  }

  const signOut = async () => {
    await supabase.auth.signOut()
    setProfile(null)
    setFilialAtiva(null)
    setProfileError(null)
    setFilialError(null)
  }

  const isMaster = profile?.perfil === 'master'
  const isRoteirizador = profile?.perfil === 'roteirizador'
  const filialId = profile?.filial_id ?? null
  const loading = authLoading

  return (
    <AuthContext.Provider
      value={{
        user,
        session,
        profile,
        filialAtiva,
        loading,
        authLoading,
        profileLoading,
        profileError,
        filialLoading,
        filialError,
        isMaster,
        isRoteirizador,
        filialId,
        signIn,
        signOut,
        refreshProfile,
        reloadProfile,
        reloadFilial,
        reloadAuthContext,
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
