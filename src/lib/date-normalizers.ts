const EXCEL_EPOCH_UTC_MS = Date.UTC(1899, 11, 30)
const MS_PER_DAY = 24 * 60 * 60 * 1000

const pad2 = (value: number): string => String(value).padStart(2, '0')

const toTrimmedText = (value: unknown): string | null => {
  if (value === null || value === undefined) return null
  const text = String(value).trim().replace(/,+\s*$/, '')
  return text.length ? text : null
}

const maybeExcelSerial = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) {
    return value > 0 ? value : null
  }

  if (typeof value === 'string') {
    const sanitized = value.trim().replace(',', '.')
    if (!/^\d+(\.\d+)?$/.test(sanitized)) return null
    const parsed = Number(sanitized)
    if (!Number.isFinite(parsed) || parsed <= 0) return null
    return parsed
  }

  return null
}

const excelSerialToDate = (serial: number): Date => {
  return new Date(EXCEL_EPOCH_UTC_MS + serial * MS_PER_DAY)
}

const parseDateFromUnknown = (value: unknown): Date | null => {
  if (value instanceof Date && !Number.isNaN(value.getTime())) return value

  const serial = maybeExcelSerial(value)
  if (serial !== null) return excelSerialToDate(serial)

  const text = toTrimmedText(value)
  if (!text) return null

  const parsed = new Date(text)
  if (!Number.isNaN(parsed.getTime())) return parsed

  const brDateTime = text.match(
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{1,2})(?::(\d{1,2}))?)?$/
  )
  if (brDateTime) {
    const [, dd, mm, yyyy, hh = '0', min = '0', sec = '0'] = brDateTime
    const date = new Date(
      Date.UTC(Number(yyyy), Number(mm) - 1, Number(dd), Number(hh), Number(min), Number(sec))
    )
    return Number.isNaN(date.getTime()) ? null : date
  }

  return null
}

const formatIsoDateTime = (date: Date): string => {
  const yyyy = date.getUTCFullYear()
  const mm = pad2(date.getUTCMonth() + 1)
  const dd = pad2(date.getUTCDate())
  const hh = pad2(date.getUTCHours())
  const min = pad2(date.getUTCMinutes())
  const sec = pad2(date.getUTCSeconds())
  return `${yyyy}-${mm}-${dd}T${hh}:${min}:${sec}`
}

const formatBrDate = (date: Date): string => {
  const dd = pad2(date.getUTCDate())
  const mm = pad2(date.getUTCMonth() + 1)
  const yyyy = date.getUTCFullYear()
  return `${dd}/${mm}/${yyyy}`
}

const formatBrDateTime = (date: Date): string => {
  return `${formatBrDate(date)} ${pad2(date.getUTCHours())}:${pad2(date.getUTCMinutes())}:${pad2(date.getUTCSeconds())}`
}

export const normalizeDataDesDataNF = (value: unknown): string | null => {
  const parsed = parseDateFromUnknown(value)
  return parsed ? formatIsoDateTime(parsed) : null
}

export const normalizeDle = (value: unknown): string | null => {
  const parsed = parseDateFromUnknown(value)
  return parsed ? formatBrDate(parsed) : null
}

export const normalizeAgendam = (value: unknown): string | null => {
  const parsed = parseDateFromUnknown(value)
  return parsed ? formatBrDateTime(parsed) : null
}
