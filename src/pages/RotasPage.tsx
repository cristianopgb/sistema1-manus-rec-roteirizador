import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import toast from 'react-hot-toast'
import { roteirizacaoService } from '@/services/roteirizacao.service'
import { ManifestoRoteirizacaoDetalhe, RodadaRoteirizacao, RotaManifestoGoogle } from '@/types'
import { supabase } from '@/lib/supabase'
import { RotaMapa } from '@/components/rotas/RotaMapa'

const formatarDuracao = (segundos?: number | null): string => {
  if (!segundos || segundos <= 0) return '—'
  const h = Math.floor(segundos / 3600)
  const m = Math.floor((segundos % 3600) / 60)
  return `${h}h ${m}min`
}

export function RotasPage() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [rodadas, setRodadas] = useState<RodadaRoteirizacao[]>([])
  const [manifestos, setManifestos] = useState<ManifestoRoteirizacaoDetalhe[]>([])
  const [rota, setRota] = useState<RotaManifestoGoogle | null>(null)
  const [loading, setLoading] = useState(false)

  const rodadaId = searchParams.get('rodada_id') ?? ''
  const manifestoId = searchParams.get('manifesto_id') ?? ''

  useEffect(() => {
    const carregar = async () => {
      const data = await roteirizacaoService.listarRodadas()
      setRodadas(data)
    }
    void carregar()
  }, [])

  useEffect(() => {
    const carregarManifestos = async () => {
      if (!rodadaId) return
      const { data, error } = await supabase
        .from('manifestos_roteirizacao')
        .select('*')
        .eq('rodada_id', rodadaId)
        .order('manifesto_id')
      if (error) {
        toast.error('Erro ao carregar manifestos da rodada')
        return
      }
      setManifestos((data ?? []) as ManifestoRoteirizacaoDetalhe[])
    }
    void carregarManifestos()
  }, [rodadaId])

  const carregarRota = async () => {
    if (!rodadaId || !manifestoId) return
    setLoading(true)
    try {
      const rotaData = await roteirizacaoService.buscarRotaManifestoGoogle(rodadaId, manifestoId)
      setRota(rotaData)
    } catch {
      toast.error('Erro ao buscar rota Google')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { void carregarRota() }, [rodadaId, manifestoId])

  const manifestoSelecionado = useMemo(
    () => manifestos.find((m) => m.manifesto_id === manifestoId) ?? null,
    [manifestos, manifestoId],
  )

  const diferencaKm = (rota?.km_google_maps ?? 0) - (rota?.km_estimado_motor ?? manifestoSelecionado?.km_total ?? 0)

  return (
    <div className="space-y-4">
      <div className="bg-white border border-gray-200 rounded-xl p-4 grid md:grid-cols-3 gap-3">
        <select className="input" value={rodadaId} onChange={(e) => setSearchParams({ rodada_id: e.target.value, manifesto_id: '' })}>
          <option value="">Selecione a rodada</option>
          {rodadas.map((rodada) => (
            <option key={rodada.id} value={rodada.id}>{rodada.id.slice(0, 8)} - {rodada.filial_nome ?? 'Filial'}</option>
          ))}
        </select>
        <select className="input" value={manifestoId} onChange={(e) => setSearchParams({ rodada_id: rodadaId, manifesto_id: e.target.value })} disabled={!rodadaId}>
          <option value="">Selecione o manifesto</option>
          {manifestos.map((manifesto) => (
            <option key={manifesto.id} value={manifesto.manifesto_id}>{manifesto.manifesto_id}</option>
          ))}
        </select>
        <div className="flex gap-2">
          <button className="btn-secondary" onClick={() => void carregarRota()} disabled={!rodadaId || !manifestoId || loading}>Atualizar</button>
          <button
            className="btn-primary"
            onClick={async () => {
              if (!rodadaId || !manifestoId) return
              await roteirizacaoService.calcularRotaGoogleManifesto(rodadaId, manifestoId)
              await carregarRota()
            }}
            disabled={!rodadaId || !manifestoId || loading}
          >
            Recalcular rota
          </button>
        </div>
      </div>

      <div className="grid md:grid-cols-6 gap-3 text-sm">
        <div className="bg-white border rounded-lg p-3"><div className="text-gray-500">KM Motor</div><strong>{(rota?.km_estimado_motor ?? manifestoSelecionado?.km_total ?? 0).toFixed(2)} km</strong></div>
        <div className="bg-white border rounded-lg p-3"><div className="text-gray-500">KM Google</div><strong>{(rota?.km_google_maps ?? 0).toFixed(2)} km</strong></div>
        <div className="bg-white border rounded-lg p-3"><div className="text-gray-500">Diferença KM</div><strong>{diferencaKm.toFixed(2)} km</strong></div>
        <div className="bg-white border rounded-lg p-3"><div className="text-gray-500">Frete mínimo atual</div><strong>R$ {(manifestoSelecionado?.frete_minimo ?? 0).toFixed(2)}</strong></div>
        <div className="bg-white border rounded-lg p-3"><div className="text-gray-500">Status Google</div><strong>{rota?.google_status ?? 'pendente'}</strong></div>
        <div className="bg-white border rounded-lg p-3"><div className="text-gray-500">Duração Google</div><strong>{formatarDuracao(rota?.duracao_segundos_google)}</strong></div>
      </div>

      {rota ? (
        <div className="grid lg:grid-cols-3 gap-4">
          <div className="lg:col-span-2 bg-white border border-gray-200 rounded-xl p-3">
            <RotaMapa
              origem={{ latitude: rota.origem_latitude, longitude: rota.origem_longitude }}
              paradas={rota.paradas_json ?? []}
              polylineGoogle={rota.polyline_google}
            />
          </div>
          <div className="bg-white border border-gray-200 rounded-xl p-3">
            <h3 className="font-semibold mb-2">Paradas</h3>
            <ol className="space-y-2 text-sm">
              <li className="border rounded p-2">1. Filial</li>
              {(rota.paradas_json ?? []).map((parada) => (
                <li key={`p-${parada.ordem}`} className="border rounded p-2">
                  {parada.ordem + 1}. {(parada.destinatarios ?? []).join(', ') || 'Cliente'} - {parada.cidade ?? '-'} / {parada.uf ?? '-'}<br />
                  <span className="text-xs text-gray-600">Docs: {(parada.documentos ?? []).join(', ') || '—'}</span>
                </li>
              ))}
            </ol>
          </div>
        </div>
      ) : (
        <div className="bg-white border border-gray-200 rounded-xl p-4 text-sm text-gray-600">
          Selecione rodada e manifesto para visualizar a rota.
        </div>
      )}
    </div>
  )
}
