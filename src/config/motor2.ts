const MOTOR2_BASE_URL = import.meta.env.VITE_MOTOR_URL || 'http://localhost:8000'

export function buildMotor2Url(path: string): string {
  if (!path || !path.trim()) {
    throw new Error('buildMotor2Url: path não pode ser vazio')
  }

  const normalizedBase = MOTOR2_BASE_URL.replace(/\/+$/, '')
  const normalizedPath = path.replace(/^\/+/, '')

  return `${normalizedBase}/${normalizedPath}`
}
