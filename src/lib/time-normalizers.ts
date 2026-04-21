const PLACEHOLDER_VALUES = new Set(['', '-'])

const pad2 = (value: string): string => value.padStart(2, '0')

const normalizeTimeParts = (hourRaw: string, minuteRaw: string, secondRaw?: string): string | null => {
  if (!/^\d{1,2}$/.test(hourRaw) || !/^\d{1,2}$/.test(minuteRaw)) return null
  if (secondRaw !== undefined && !/^\d{1,2}$/.test(secondRaw)) return null

  const hour = Number(hourRaw)
  const minute = Number(minuteRaw)
  const second = secondRaw !== undefined ? Number(secondRaw) : null

  if (!Number.isInteger(hour) || hour < 0 || hour > 23) return null
  if (!Number.isInteger(minute) || minute < 0 || minute > 59) return null
  if (second !== null && (!Number.isInteger(second) || second < 0 || second > 59)) return null

  if (second === null) {
    return `${pad2(String(hour))}:${pad2(String(minute))}`
  }

  return `${pad2(String(hour))}:${pad2(String(minute))}:${pad2(String(second))}`
}

const normalizeTimeString = (rawValue: string): string | null => {
  const semFracao = rawValue.replace(/[.,]\d+$/, '')
  const timeMatch = semFracao.match(/(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?$/)
  if (!timeMatch) return null
  return normalizeTimeParts(timeMatch[1], timeMatch[2], timeMatch[3])
}

/**
 * Normaliza campos de horário de janela para o contrato aceito pelo motor:
 * - HH:MM
 * - HH:MM:SS
 * - null
 */
export const normalizeHorarioJanela = (value: unknown): string | null => {
  if (value === null || value === undefined) return null

  if (typeof value === 'number') {
    return normalizeTimeString(String(value))
  }

  const raw = String(value).trim()
  if (PLACEHOLDER_VALUES.has(raw)) return null

  if (!raw) return null

  return normalizeTimeString(raw)
}

export const normalizeInicioEntrega = normalizeHorarioJanela
