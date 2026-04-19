export class RequestTimeoutError extends Error {
  constructor(operation: string, timeoutMs: number) {
    super(`${operation} excedeu o tempo limite de ${timeoutMs / 1000}s.`)
    this.name = 'RequestTimeoutError'
  }
}

const resolveDefaultTimeoutMs = () => {
  const rawTimeout = Number(import.meta.env.VITE_REQUEST_TIMEOUT_MS ?? 30000)
  if (Number.isNaN(rawTimeout)) return 30000
  return Math.min(Math.max(rawTimeout, 5000), 120000)
}

export const DEFAULT_REQUEST_TIMEOUT_MS = resolveDefaultTimeoutMs()

export async function withTimeout<T>(
  promise: PromiseLike<T>,
  operation: string,
  timeoutMs = DEFAULT_REQUEST_TIMEOUT_MS
): Promise<T> {
  let timeoutId: ReturnType<typeof setTimeout> | null = null

  const timeoutPromise = new Promise<never>((_, reject) => {
    timeoutId = setTimeout(() => {
      reject(new RequestTimeoutError(operation, timeoutMs))
    }, timeoutMs)
  })

  try {
    return await Promise.race([promise, timeoutPromise])
  } finally {
    if (timeoutId) clearTimeout(timeoutId)
  }
}

export function getErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message) return error.message

  if (typeof error === 'object' && error !== null && 'message' in error) {
    const message = (error as { message?: unknown }).message
    if (typeof message === 'string' && message) return message
  }

  return fallback
}
