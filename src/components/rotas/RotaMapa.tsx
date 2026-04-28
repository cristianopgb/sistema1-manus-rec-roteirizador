import { useEffect, useMemo } from 'react'
import { APIProvider, AdvancedMarker, Map, useMap } from '@vis.gl/react-google-maps'
import { RotaManifestoParadaGoogle } from '@/types'

type PontoMapa = { lat: number; lng: number }

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

function OverlayRota({ pontos, origem, paradas }: { pontos: PontoMapa[]; origem: PontoMapa; paradas: RotaManifestoParadaGoogle[] }) {
  const map = useMap()

  useEffect(() => {
    const windowWithGoogle = window as unknown as Window & { google?: any }
    if (!map || !windowWithGoogle.google) return
    const googleRef = windowWithGoogle.google as any
    const caminho = pontos.length ? pontos : [origem, ...paradas.map((p) => ({ lat: p.latitude, lng: p.longitude }))]
    const polyline = new googleRef.maps.Polyline({
      path: caminho,
      strokeColor: pontos.length ? '#1d4ed8' : '#6b7280',
      strokeOpacity: 1,
      strokeWeight: 4,
      map,
    })

    const bounds = new googleRef.maps.LatLngBounds()
    caminho.forEach((p) => bounds.extend(p))
    if (!bounds.isEmpty()) map.fitBounds(bounds, 80)

    return () => {
      polyline.setMap(null)
    }
  }, [map, pontos, origem, paradas])

  return null
}

interface RotaMapaProps {
  origem: { latitude: number; longitude: number }
  paradas: RotaManifestoParadaGoogle[]
  polylineGoogle?: string | null
}

export function RotaMapa({ origem, paradas, polylineGoogle }: RotaMapaProps) {
  const browserKey = import.meta.env.VITE_GOOGLE_MAPS_BROWSER_KEY
  const pontosPolyline = useMemo(() => {
    if (!polylineGoogle) return []
    try {
      return decodeGooglePolyline(polylineGoogle)
    } catch {
      return []
    }
  }, [polylineGoogle])

  if (!browserKey) {
    return <div className="p-4 border border-amber-200 bg-amber-50 rounded-lg text-amber-800 text-sm">Configure VITE_GOOGLE_MAPS_BROWSER_KEY para visualizar o mapa.</div>
  }

  return (
    <APIProvider apiKey={browserKey}>
      <div className="space-y-2">
        {!polylineGoogle && (
          <div className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-3 py-2">
            Traçado visual aproximado; rota Google ainda não calculada.
          </div>
        )}
        <div className="h-[420px] w-full rounded-xl overflow-hidden border border-gray-200">
          <Map defaultZoom={6} defaultCenter={{ lat: origem.latitude, lng: origem.longitude }} mapId="rotas-manifestos-map" gestureHandling="greedy">
            <AdvancedMarker position={{ lat: origem.latitude, lng: origem.longitude }} title="Filial" />
            {paradas.map((parada) => (
              <AdvancedMarker
                key={`${parada.ordem}-${parada.latitude}-${parada.longitude}`}
                position={{ lat: parada.latitude, lng: parada.longitude }}
                title={`Parada ${parada.ordem}`}
              />
            ))}
            <OverlayRota pontos={pontosPolyline} origem={{ lat: origem.latitude, lng: origem.longitude }} paradas={paradas} />
          </Map>
        </div>
      </div>
    </APIProvider>
  )
}
