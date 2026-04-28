declare module '@vis.gl/react-google-maps' {
  import type { ComponentType, ReactNode } from 'react'

  export const APIProvider: ComponentType<{ apiKey: string; children?: ReactNode }>
  export const Map: ComponentType<Record<string, unknown> & { children?: ReactNode }>
  export const AdvancedMarker: ComponentType<Record<string, unknown> & { children?: ReactNode }>
  export function useMap(): any
}
