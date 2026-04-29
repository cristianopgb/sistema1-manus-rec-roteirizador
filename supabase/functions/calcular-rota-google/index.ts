import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

type GoogleStatus = 'pendente' | 'processando' | 'ok' | 'erro' | 'sem_coordenadas' | 'sem_paradas' | 'excede_limite_waypoints' | 'reutilizada'

type Parada = {
  ordem?: number
  latitude?: number
  longitude?: number
  cidade?: string | null
  uf?: string | null
  destinatarios?: string[]
}

const corsHeaders = {
  'Content-Type': 'application/json',
}

const jsonResponse = (status: number, body: Record<string, unknown>) => (
  new Response(JSON.stringify(body), { status, headers: corsHeaders })
)

const toNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string') {
    const parsed = Number(value.replace(',', '.'))
    if (Number.isFinite(parsed)) return parsed
  }
  return null
}

const toDurationSeconds = (duration?: string): number | null => {
  if (!duration || typeof duration !== 'string') return null
  const parsed = Number(duration.replace('s', ''))
  return Number.isFinite(parsed) ? Math.round(parsed) : null
}

const formatDurationText = (seconds: number | null): string | null => {
  if (!seconds || seconds <= 0) return null
  const h = Math.floor(seconds / 3600)
  const m = Math.floor((seconds % 3600) / 60)
  if (h <= 0) return `${m} min`
  return `${h}h ${String(m).padStart(2, '0')}min`
}

const formatParadaLabel = (parada: Parada & { cidade?: string | null; uf?: string | null }, ordem: number): string => {
  const destinatario = Array.isArray(parada.destinatarios) ? parada.destinatarios.filter(Boolean)[0] : null
  const cidadeUf = [parada.cidade, parada.uf].filter(Boolean).join(' / ')
  if (destinatario) return cidadeUf ? `${destinatario} - ${cidadeUf}` : destinatario
  return cidadeUf ? `Parada ${ordem} - ${cidadeUf}` : `Parada ${ordem}`
}

Deno.serve(async (req) => {
  try {
    const authHeader = req.headers.get('Authorization')
    if (!authHeader) {
      return jsonResponse(401, { status: 'erro', message: 'Usuário não autenticado' })
    }

    const supabaseUrl = Deno.env.get('SUPABASE_URL')
    const supabaseAnonKey = Deno.env.get('SUPABASE_ANON_KEY')
    if (!supabaseUrl || !supabaseAnonKey) {
      return jsonResponse(500, { status: 'erro', message: 'Configuração Supabase inválida' })
    }

    const supabase = createClient(supabaseUrl, supabaseAnonKey, {
      global: {
        headers: {
          Authorization: authHeader,
        },
      },
    })

    const { data: userData, error: userError } = await supabase.auth.getUser()
    if (userError || !userData.user) {
      return jsonResponse(401, { status: 'erro', message: 'Usuário não autenticado' })
    }

    const body = await req.json().catch(() => null) as { rodada_id?: string; manifesto_id?: string } | null
    const rodadaId = body?.rodada_id
    const manifestoId = body?.manifesto_id

    if (!rodadaId || !manifestoId) {
      return jsonResponse(400, { status: 'erro', message: 'rodada_id e manifesto_id são obrigatórios' })
    }

    const { data: rota, error: rotaError } = await supabase
      .from('rotas_manifestos_google')
      .select('*')
      .eq('rodada_id', rodadaId)
      .eq('manifesto_id', manifestoId)
      .maybeSingle()

    if (rotaError) {
      return jsonResponse(400, { status: 'erro', message: 'Erro ao buscar rota pendente' })
    }

    if (!rota) {
      return jsonResponse(404, { status: 'erro', message: 'Rota não encontrada para rodada/manifesto informados' })
    }

    const origemLat = toNumber(rota.origem_latitude)
    const origemLng = toNumber(rota.origem_longitude)

    const atualizarStatus = async (status: GoogleStatus, googleErro: string | null, requestJson?: Record<string, unknown> | null, responseJson?: Record<string, unknown> | null) => {
      const { data: rotaAtualizada } = await supabase
        .from('rotas_manifestos_google')
        .update({
          google_status: status,
          google_erro: googleErro,
          request_json: requestJson ?? null,
          response_json: responseJson ?? null,
        })
        .eq('id', rota.id)
        .select('*')
        .single()

      return rotaAtualizada
    }

    if (origemLat === null || origemLng === null) {
      const rotaAtualizada = await atualizarStatus('sem_coordenadas', 'Origem da filial sem coordenadas válidas')
      return jsonResponse(200, { status: 'sem_coordenadas', rota: rotaAtualizada })
    }

    const paradasRaw = Array.isArray(rota.paradas_json) ? rota.paradas_json as Parada[] : []
    if (!paradasRaw.length) {
      const rotaAtualizada = await atualizarStatus('sem_paradas', 'Manifesto sem paradas válidas para cálculo')
      return jsonResponse(200, { status: 'sem_paradas', rota: rotaAtualizada })
    }

    const paradasValidas = paradasRaw
      .map((parada) => ({
        ...parada,
        latitude: toNumber(parada.latitude),
        longitude: toNumber(parada.longitude),
      }))
      .filter((parada) => parada.latitude !== null && parada.longitude !== null)
      .reduce<Array<Parada & { latitude: number; longitude: number }>>((acc, parada) => {
        const ultima = acc[acc.length - 1]
        if (ultima && ultima.latitude === parada.latitude && ultima.longitude === parada.longitude) {
          return acc
        }
        acc.push(parada as Parada & { latitude: number; longitude: number })
        return acc
      }, [])

    if (!paradasValidas.length) {
      const rotaAtualizada = await atualizarStatus('sem_paradas', 'Manifesto sem paradas válidas para cálculo')
      return jsonResponse(200, { status: 'sem_paradas', rota: rotaAtualizada })
    }

    if (paradasValidas.length > 26) {
      const rotaAtualizada = await atualizarStatus('excede_limite_waypoints', 'Quantidade de paradas excede o limite de 26 pontos')
      return jsonResponse(200, { status: 'excede_limite_waypoints', rota: rotaAtualizada })
    }

    await supabase
      .from('rotas_manifestos_google')
      .update({ google_status: 'processando', google_erro: null })
      .eq('id', rota.id)

    const ultimaParada = paradasValidas[paradasValidas.length - 1]
    const intermediates = paradasValidas.slice(0, -1).map((parada) => ({
      location: {
        latLng: {
          latitude: parada.latitude,
          longitude: parada.longitude,
        },
      },
    }))

    const requestJson = {
      origin: {
        location: {
          latLng: {
            latitude: origemLat,
            longitude: origemLng,
          },
        },
      },
      destination: {
        location: {
          latLng: {
            latitude: ultimaParada.latitude,
            longitude: ultimaParada.longitude,
          },
        },
      },
      intermediates,
      travelMode: 'DRIVE',
      routingPreference: 'TRAFFIC_UNAWARE',
      computeAlternativeRoutes: false,
      units: 'METRIC',
      polylineQuality: 'OVERVIEW',
    }

    const apiKey = Deno.env.get('GOOGLE_ROUTES_API_KEY')
    if (!apiKey) {
      const rotaAtualizada = await atualizarStatus('erro', 'Secret GOOGLE_ROUTES_API_KEY não configurado', requestJson)
      return jsonResponse(500, { status: 'erro', rota: rotaAtualizada })
    }

    const googleResponse = await fetch('https://routes.googleapis.com/directions/v2:computeRoutes', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'X-Goog-Api-Key': apiKey,
        'X-Goog-FieldMask': 'routes.duration,routes.distanceMeters,routes.polyline.encodedPolyline,routes.legs.distanceMeters,routes.legs.duration',
      },
      body: JSON.stringify(requestJson),
    })

    const googleJson = await googleResponse.json().catch(() => null) as Record<string, unknown> | null

    if (!googleResponse.ok) {
      const rotaAtualizada = await atualizarStatus('erro', 'Não foi possível calcular a rota no Google Routes', requestJson, googleJson)
      return jsonResponse(200, { status: 'erro', rota: rotaAtualizada })
    }

    const routes = Array.isArray(googleJson?.routes) ? googleJson.routes as Array<Record<string, unknown>> : []
    const route0 = routes[0]
    if (!route0) {
      const rotaAtualizada = await atualizarStatus('erro', 'Google Routes retornou rota vazia', requestJson, googleJson)
      return jsonResponse(200, { status: 'erro', rota: rotaAtualizada })
    }

    const distanceMeters = toNumber(route0.distanceMeters)
    const durationSeconds = toDurationSeconds(typeof route0.duration === 'string' ? route0.duration : undefined)
    const encodedPolyline = (route0.polyline as Record<string, unknown> | undefined)?.encodedPolyline
    const routeLegs = Array.isArray(route0.legs) ? route0.legs as Array<Record<string, unknown>> : []
    const legsJson = routeLegs.length
      ? routeLegs.map((leg, idx) => {
        const legDistanceMeters = toNumber(leg.distanceMeters)
        const legDurationSeconds = toDurationSeconds(typeof leg.duration === 'string' ? leg.duration : undefined)
        const origemParada = idx === 0 ? null : paradasValidas[idx - 1]
        const destinoParada = paradasValidas[idx]
        return {
          ordem: idx + 1,
          origem_tipo: idx === 0 ? 'filial' : 'parada',
          origem_label: idx === 0 ? 'Filial' : formatParadaLabel(origemParada!, idx),
          destino_tipo: 'parada',
          destino_label: destinoParada ? formatParadaLabel(destinoParada, idx + 1) : `Parada ${idx + 1}`,
          distance_meters: legDistanceMeters ? Math.round(legDistanceMeters) : 0,
          distance_km: legDistanceMeters ? Number((legDistanceMeters / 1000).toFixed(3)) : 0,
          duration_seconds: legDurationSeconds,
          duration_text: formatDurationText(legDurationSeconds),
        }
      })
      : null

    const { data: rotaAtualizada, error: updateError } = await supabase
      .from('rotas_manifestos_google')
      .update({
        google_status: 'ok',
        distancia_metros_google: distanceMeters ? Math.round(distanceMeters) : null,
        km_google_maps: distanceMeters ? Number((distanceMeters / 1000).toFixed(3)) : null,
        duracao_segundos_google: durationSeconds,
        polyline_google: typeof encodedPolyline === 'string' ? encodedPolyline : null,
        legs_json: legsJson,
        request_json: requestJson,
        response_json: googleJson,
        google_erro: null,
      })
      .eq('id', rota.id)
      .select('*')
      .single()

    if (updateError) {
      return jsonResponse(400, { status: 'erro', message: 'Erro ao salvar retorno do Google' })
    }

    return jsonResponse(200, { status: 'ok', rota: rotaAtualizada })
  } catch {
    return jsonResponse(500, { status: 'erro', message: 'Falha inesperada ao processar rota Google' })
  }
})
