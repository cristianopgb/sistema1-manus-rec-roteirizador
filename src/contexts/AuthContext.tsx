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
  reloadProfile: (options?: { force?: boolean }) => Promise<void>
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
  const lastLoadedUserIdRef = useRef<string | null>(null)
  const lastProfileLoadedAtRef = useRef<number | null>(null)
  const lastSuccessfulProfileRef = useRef<UserProfile | null>(null)
  const lastSuccessfulFilialRef = useRef<Filial | null>(null)
  const profileRequestInFlightRef = useRef<{ userId: string; promise: Promise<void> } | null>(null)

  useEffect(() => {
    userRef.current = user
  }, [user])

  useEffect(() => {
    profileRef.current = profile
    if (profile) {
      lastSuccessfulProfileRef.current = profile
    }
  }, [profile])

  useEffect(() => {
    if (filialAtiva) {
      lastSuccessfulFilialRef.current = filialAtiva
    }
  }, [filialAtiva])

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

  const loadProfileForUser = useCallback(async (
    userId: string,
    options?: { force?: boolean; showLoading?: boolean }
  ): Promise<void> => {
    const force = options?.force ?? false
    const showLoading = options?.showLoading ?? true
    const inFlight = profileRequestInFlightRef.current

    if (!force && inFlight?.userId === userId) {
      console.log('[AuthContext] Reutilizando carregamento de perfil em andamento', { userId })
      return inFlight.promise
    }

    if (!force && lastLoadedUserIdRef.current === userId && profileRef.current?.id === userId) {
      console.log('[AuthContext] Ignorando reload de perfil para mesmo usuário já carregado', { userId })
      return
    }

    const requestPromise = (async () => {
      if (showLoading) {
        setProfileLoading(true)
      }

      try {
        setProfileError(null)
        const profileData = await fetchProfileByUserId(userId)

        if (!profileData) {
          setProfileError('Não foi possível carregar o perfil. Tente novamente.')
          console.error('[AuthContext] Falha controlada ao carregar perfil', { userId })
          return
        }

        setProfile(profileData)
        lastLoadedUserIdRef.current = userId
        lastProfileLoadedAtRef.current = Date.now()
        lastSuccessfulProfileRef.current = profileData

        await loadFilialFromProfile(profileData, { showLoading })
      } finally {
        if (showLoading) {
          setProfileLoading(false)
        }

        if (profileRequestInFlightRef.current?.userId === userId) {
          profileRequestInFlightRef.current = null
        }
      }
    })()

    profileRequestInFlightRef.current = { userId, promise: requestPromise }
    return requestPromise
  }, [fetchProfileByUserId, loadFilialFromProfile])

  const refreshProfile = useCallback(async () => {
    if (!user) return
    await loadProfileForUser(user.id, { force: true, showLoading: true })
  }, [user, loadProfileForUser])

  const reloadProfile = useCallback(async (options?: { force?: boolean }) => {
    if (!user) return
    await loadProfileForUser(user.id, { force: options?.force ?? true, showLoading: true })
  }, [user, loadProfileForUser])

  const reloadFilial = useCallback(async () => {
    await loadFilialFromProfile(profile)
  }, [profile, loadFilialFromProfile])

  const reloadAuthContext = useCallback(async () => {
    if (!user) return
    await loadProfileForUser(user.id, { force: true, showLoading: true })
  }, [user, loadProfileForUser])

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
          console.log('[AuthContext] Carregando perfil por sessão inicial', {
            event: 'INITIAL_SESSION',
            userId: initialSession.user.id,
          })
          await loadProfileForUser(initialSession.user.id, { showLoading: true })
        } else {
          setProfile(null)
          setFilialAtiva(null)
          setProfileError(null)
          setFilialError(null)
          lastLoadedUserIdRef.current = null
          lastProfileLoadedAtRef.current = null
          lastSuccessfulProfileRef.current = null
          lastSuccessfulFilialRef.current = null
          profileRequestInFlightRef.current = null
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

        if (authEvent === 'SIGNED_OUT' || !nextSession?.user) {
          setProfile(null)
          setFilialAtiva(null)
          setProfileError(null)
          setFilialError(null)
          setProfileLoading(false)
          setFilialLoading(false)
          lastLoadedUserIdRef.current = null
          lastProfileLoadedAtRef.current = null
          lastSuccessfulProfileRef.current = null
          lastSuccessfulFilialRef.current = null
          profileRequestInFlightRef.current = null
          return
        }

        if (authEvent === 'TOKEN_REFRESHED') {
          if (profileRef.current?.id === nextUserId) {
            console.log('[AuthContext] Ignorando reload de perfil para mesmo usuário após refresh de token', {
              userId: nextUserId,
            })
          }
          return
        }

        if (authEvent === 'INITIAL_SESSION') {
          if (!profileRef.current || lastLoadedUserIdRef.current !== nextUserId) {
            console.log('[AuthContext] Carregando perfil por mudança real de sessão', {
              event: authEvent,
              userId: nextUserId,
            })
            await loadProfileForUser(nextSession.user.id, { showLoading: false })
          }
          return
        }

        if (authEvent === 'SIGNED_IN') {
          const shouldLoadProfile = userChanged || !profileRef.current || lastLoadedUserIdRef.current !== nextUserId
          if (!shouldLoadProfile) {
            console.log('[AuthContext] SIGNED_IN ignorado para usuário já carregado', {
              userId: nextUserId,
            })
            return
          }

          console.log('[AuthContext] Carregando perfil por mudança real de sessão', {
            event: authEvent,
            userId: nextUserId,
          })
          await loadProfileForUser(nextSession.user.id, { showLoading: true })
          return
        }

        if (userChanged) {
          console.log('[AuthContext] Carregando perfil por troca de usuário', {
            event: authEvent,
            userId: nextUserId,
          })
          await loadProfileForUser(nextSession.user.id, { force: true, showLoading: true })
        }
      }
    )

    return () => {
      mounted = false
      subscription.unsubscribe()
    }
  }, [loadProfileForUser])

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
    lastLoadedUserIdRef.current = null
    lastProfileLoadedAtRef.current = null
    lastSuccessfulProfileRef.current = null
    lastSuccessfulFilialRef.current = null
    profileRequestInFlightRef.current = null
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
