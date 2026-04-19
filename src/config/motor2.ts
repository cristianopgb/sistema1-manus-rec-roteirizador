const LOCAL_MOTOR_2_FALLBACK_URL = 'http://localhost:8000'
const LOCAL_HOSTNAMES = new Set(['localhost', '127.0.0.1'])

const isLocalRuntime = (): boolean =>
  typeof window !== 'undefined' && LOCAL_HOSTNAMES.has(window.location.hostname)

const isProductionOrPreview = (): boolean =>
  import.meta.env.PROD || import.meta.env.MODE === 'preview'

const normalizeBaseUrl = (value: string): string => value.trim().replace(/\/+$/, '')

export const getMotor2BaseUrl = (): string => {
  const envValue = typeof import.meta.env.VITE_MOTOR_2_URL === 'string'
    ? normalizeBaseUrl(import.meta.env.VITE_MOTOR_2_URL)
    : ''

  if (!envValue) {
    if (isLocalRuntime()) return LOCAL_MOTOR_2_FALLBACK_URL
    throw new Error(
      isProductionOrPreview()
        ? 'VITE_MOTOR_2_URL deve ser definida em produção/preview.'
        : 'VITE_MOTOR_2_URL deve ser definida fora do ambiente local.'
    )
  }

  let parsedUrl: URL
  try {
    parsedUrl = new URL(envValue)
  } catch {
    throw new Error('VITE_MOTOR_2_URL inválida: informe uma URL absoluta válida.')
  }

  const isLoopbackHost = LOCAL_HOSTNAMES.has(parsedUrl.hostname)

  if (isProductionOrPreview() && parsedUrl.protocol !== 'https:') {
    throw new Error('VITE_MOTOR_2_URL deve usar https:// em produção/preview.')
  }

  if (!isLocalRuntime() && isLoopbackHost) {
    throw new Error('VITE_MOTOR_2_URL não pode apontar para localhost fora de ambiente local.')
  }

  return envValue
}

export const buildMotor2Url = (path: string): string => {
  const normalizedPath = path.startsWith('/') ? path : `/${path}`
  return `${getMotor2BaseUrl()}${normalizedPath}`
}
