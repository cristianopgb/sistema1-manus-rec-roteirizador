import { useEffect, useMemo, useRef, useState } from 'react'
import { RotaManifestoParadaGoogle } from '@/types'

type PontoMapa = { lat: number; lng: number }

declare global {
  interface Window {
    google?: any
  }
}

const decodeGooglePolyline = (encoded: string): PontoMapa[] => {
  const points: PontoMapa[] = []
  let index = 0
  let lat = 0
  let lng = 0

  while (index < encoded.length) {
    let b: number
    let shift = 0
    let result = 0
    do {
      b = encoded.charCodeAt(index++) - 63
      result |= (b & 0x1f) << shift
      shift += 5
    } while (b >= 0x20)

    const deltaLat = (result & 1) ? ~(result >> 1) : (result >> 1)
    lat += deltaLat

    shift = 0
    result = 0
    do {
      b = encoded.charCodeAt(index++) - 63
      result |= (b & 0x1f) << shift
      shift += 5
    } while (b >= 0x20)

    const deltaLng = (result & 1) ? ~(result >> 1) : (result >> 1)
    lng += deltaLng

    points.push({ lat: lat / 1e5, lng: lng / 1e5 })
  }

  return points
}

const loadGoogleMapsScript = (apiKey: string): Promise<void> => {
  if (window.google?.maps) return Promise.resolve()

  const scriptId = 'google-maps-js-api-script'
  const existing = document.getElementById(scriptId) as HTMLScriptElement | null
  if (existing) {
    return new Promise((resolve, reject) => {
      existing.addEventListener('load', () => resolve())
      existing.addEventListener('error', () => reject(new Error('Falha ao carregar Google Maps JS API')))
    })
  }

  return new Promise((resolve, reject) => {
    const script = document.createElement('script')
    script.id = scriptId
    script.async = true
    script.defer = true
    script.src = `https://maps.googleapis.com/maps/api/js?key=${encodeURIComponent(apiKey)}`
    script.onload = () => resolve()
    script.onerror = () => reject(new Error('Falha ao carregar Google Maps JS API'))
    document.head.appendChild(script)
  })
}

interface RotaMapaProps {
  origem: { latitude: number; longitude: number }
  paradas: RotaManifestoParadaGoogle[]
  polylineGoogle?: string | null
}

export function RotaMapa({ origem, paradas, polylineGoogle }: RotaMapaProps) {
  const apiKey = import.meta.env.VITE_GOOGLE_MAPS_BROWSER_KEY
  const mapRef = useRef<HTMLDivElement | null>(null)
  const [apiErro, setApiErro] = useState<string | null>(null)

  const pontosPolyline = useMemo(() => {
    if (!polylineGoogle) return []
    try {
      return decodeGooglePolyline(polylineGoogle)
    } catch {
      return []
    }
  }, [polylineGoogle])

  useEffect(() => {
    if (!apiKey || !mapRef.current) return
    let markers: any[] = []
    let linha: any | null = null

    const renderMap = async () => {
      try {
        setApiErro(null)
        await loadGoogleMapsScript(apiKey)
        if (!mapRef.current || !window.google?.maps) return

        const googleRef = window.google
        const mapa = new googleRef.maps.Map(mapRef.current, {
          center: { lat: origem.latitude, lng: origem.longitude },
          zoom: 6,
          gestureHandling: 'greedy',
        })

        markers.push(new googleRef.maps.Marker({
          map: mapa,
          position: { lat: origem.latitude, lng: origem.longitude },
          title: 'Filial',
          label: 'F',
        }))

        paradas.forEach((parada, idx) => {
          markers.push(new googleRef.maps.Marker({
            map: mapa,
            position: { lat: parada.latitude, lng: parada.longitude },
            title: `Parada ${parada.ordem}`,
            label: String(idx + 1),
          }))
        })

        const path = pontosPolyline.length
          ? pontosPolyline
          : [{ lat: origem.latitude, lng: origem.longitude }, ...paradas.map((p) => ({ lat: p.latitude, lng: p.longitude }))]

        linha = new googleRef.maps.Polyline({
          path,
          strokeColor: pontosPolyline.length ? '#1d4ed8' : '#6b7280',
          strokeOpacity: 1,
          strokeWeight: 4,
          map: mapa,
        })

        const bounds = new googleRef.maps.LatLngBounds()
        path.forEach((p) => bounds.extend(p))
        if (!bounds.isEmpty()) mapa.fitBounds(bounds, 80)
      } catch (err) {
        setApiErro(err instanceof Error ? err.message : 'Erro ao inicializar mapa')
      }
    }

    void renderMap()

    return () => {
      markers.forEach((m) => m.setMap(null))
      markers = []
      if (linha) linha.setMap(null)
    }
  }, [apiKey, origem.latitude, origem.longitude, paradas, pontosPolyline])

  if (!apiKey) {
    return <div className="p-4 border border-amber-200 bg-amber-50 rounded-lg text-amber-800 text-sm">Configure VITE_GOOGLE_MAPS_BROWSER_KEY para visualizar o mapa.</div>
  }

  return (
    <div className="space-y-2">
      {!polylineGoogle && (
        <div className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-3 py-2">
          Traçado visual aproximado; rota Google ainda não calculada.
        </div>
      )}
      {apiErro ? (
        <div className="p-4 border border-red-200 bg-red-50 rounded-lg text-red-700 text-sm">
          {apiErro}
        </div>
      ) : null}
      <div ref={mapRef} className="h-[420px] w-full rounded-xl overflow-hidden border border-gray-200" />
    </div>
  )
}
