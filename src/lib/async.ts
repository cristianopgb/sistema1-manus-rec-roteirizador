export class RequestTimeoutError extends Error {
  constructor(operation: string, timeoutMs: number) {
    super(`${operation} excedeu o tempo limite de ${timeoutMs / 1000}s.`)
    this.name = 'RequestTimeoutError'
  }
}

export async function withTimeout<T>(
  promise: PromiseLike<T>,
  operation: string,
  timeoutMs = 15000
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
