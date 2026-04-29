import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import toast from 'react-hot-toast'
import { roteirizacaoService } from '@/services/roteirizacao.service'
import { ManifestoItemRoteirizacao, ManifestoRoteirizacaoDetalhe, RodadaRoteirizacao, RotaManifestoGoogle, RotaManifestoParadaGoogle } from '@/types'
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
  const [isEditingSequence, setIsEditingSequence] = useState(false)
  const [editParadas, setEditParadas] = useState<RotaManifestoParadaGoogle[]>([])
  const [isSavingSequence, setIsSavingSequence] = useState(false)
  const [savingStep, setSavingStep] = useState<string | null>(null)

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
  useEffect(() => {
    setIsEditingSequence(false)
    setEditParadas(rota?.paradas_json ?? [])
  }, [rota?.id, rota?.updated_at])

  const manifestoSelecionado = useMemo(
    () => manifestos.find((m) => m.manifesto_id === manifestoId) ?? null,
    [manifestos, manifestoId],
  )

  const diferencaKm = (rota?.km_google_maps ?? 0) - (rota?.km_estimado_motor ?? manifestoSelecionado?.km_total ?? 0)
  const paradasAtuais = isEditingSequence ? editParadas : (rota?.paradas_json ?? [])
  const sequenceChanged = isEditingSequence && JSON.stringify(editParadas) !== JSON.stringify(rota?.paradas_json ?? [])
  const canShowLegs = !sequenceChanged && rota?.google_status === 'ok' && (rota.legs_json?.length ?? 0) > 0

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
              paradas={paradasAtuais}
              polylineGoogle={rota.polyline_google}
              usePolylineGoogle={!sequenceChanged}
              isPreviewRoute={sequenceChanged}
            />
          </div>
          <div className="bg-white border border-gray-200 rounded-xl p-3">
            <div className="flex items-center justify-between mb-2">
              <h3 className="font-semibold">Paradas</h3>
              {!isEditingSequence ? (
                <button className="btn-secondary" onClick={() => { console.log('[ROTAS] edição de sequência iniciada', { rodada_id: rodadaId, manifesto_id: manifestoId, qtd_paradas: rota.qtd_paradas }); setIsEditingSequence(true); setEditParadas(rota.paradas_json ?? []) }}>Editar sequência</button>
              ) : (
                <div className="flex gap-2">
                  <button className="btn-secondary" disabled={isSavingSequence} onClick={() => { setIsEditingSequence(false); setEditParadas(rota.paradas_json ?? []) }}>Cancelar</button>
                  <button className="btn-primary" disabled={isSavingSequence || !sequenceChanged} onClick={async () => {
                    if (!rodadaId || !manifestoId || !sequenceChanged) return
                    setIsSavingSequence(true)
                    try {
                      console.log('[ROTAS] salvando nova sequência', { rodada_id: rodadaId, manifesto_id: manifestoId, qtd_paradas: editParadas.length })
                      setSavingStep('Salvando nova sequência...')
                      const { data } = await supabase.from('manifestos_itens').select('*').eq('rodada_id', rodadaId).eq('manifesto_id', manifestoId).order('sequencia')
                      const itens = (data ?? []) as ManifestoItemRoteirizacao[]
                      const ordemDocs = editParadas.flatMap((p) => p.documentos ?? [])
                      const byDoc = new Map<string, ManifestoItemRoteirizacao[]>([])
                      for (const item of itens) { const key = String(item.nro_documento ?? ''); byDoc.set(key, [...(byDoc.get(key) ?? []), item]) }
                      const itensOrdenados = ordemDocs.flatMap((doc) => byDoc.get(String(doc)) ?? [])
                      await roteirizacaoService.salvarOrdemManifestoItens(rodadaId, manifestoId, itensOrdenados)
                      setSavingStep('Atualizando visualização...')
                      await carregarRota()
                      setIsEditingSequence(false)
                      console.log('[ROTAS] sequência, rota e frete atualizados', { rodada_id: rodadaId, manifesto_id: manifestoId, qtd_paradas: editParadas.length, status_google: rota?.google_status, km_google_maps: rota?.km_google_maps, frete_status: manifestoSelecionado?.frete_status })
                      toast.success('Sequência atualizada, rota e frete recalculados.')
                    } catch {
                      toast.error('Não foi possível recalcular a rota Google. O frete mínimo deverá ser calculado manualmente.')
                    } finally { setSavingStep(null); setIsSavingSequence(false) }
                  }}>Salvar e recalcular rota</button>
                </div>
              )}
            </div>
            {savingStep ? <div className="text-xs text-blue-700 bg-blue-50 border border-blue-200 rounded px-2 py-1 mb-2">{savingStep}</div> : null}
            {sequenceChanged ? <div className="text-xs text-amber-700 bg-amber-50 border border-amber-200 rounded px-2 py-1 mb-2">Sequência alterada. Clique em Salvar e recalcular rota para atualizar KM Google, mapa e frete mínimo.</div> : null}
            {!canShowLegs ? <div className="text-xs text-gray-600 bg-gray-50 border border-gray-200 rounded px-2 py-1 mb-2">{sequenceChanged ? 'Distâncias por trecho serão atualizadas após salvar e recalcular a rota.' : 'Trechos entre paradas indisponíveis. Recalcule a rota Google.'}</div> : null}
            <ol className="space-y-2 text-sm">
              <li className="border rounded p-2"><span className="inline-block h-2 w-2 rounded-full bg-blue-600 mr-2" />Filial</li>
              {paradasAtuais.map((parada, idx) => (
                <li key={`p-${parada.ordem}-${idx}`} className="border rounded p-2">
                  <div className="flex items-start justify-between gap-2">
                    <div>
                      <span className="inline-block h-2 w-2 rounded-full bg-red-500 mr-2" />
                      {idx + 1}. {(parada.destinatarios ?? []).join(', ') || 'Cliente'} - {parada.cidade ?? '-'} / {parada.uf ?? '-'}<br />
                    </div>
                    {isEditingSequence ? <div className="flex gap-1"><button className="btn-secondary px-2 py-0.5" disabled={idx===0||isSavingSequence} onClick={()=>{const arr=[...editParadas];[arr[idx-1],arr[idx]]=[arr[idx],arr[idx-1]];setEditParadas(arr);console.log('[ROTAS] sequência alterada localmente',{rodada_id:rodadaId,manifesto_id:manifestoId,qtd_paradas:arr.length})}}>↑</button><button className="btn-secondary px-2 py-0.5" disabled={idx===editParadas.length-1||isSavingSequence} onClick={()=>{const arr=[...editParadas];[arr[idx+1],arr[idx]]=[arr[idx],arr[idx+1]];setEditParadas(arr);console.log('[ROTAS] sequência alterada localmente',{rodada_id:rodadaId,manifesto_id:manifestoId,qtd_paradas:arr.length})}}>↓</button></div> : null}
                  </div>
                  <span className="text-xs text-gray-600">Docs: {(parada.documentos ?? []).join(', ') || '—'}</span>
                  {canShowLegs && rota.legs_json?.[idx] ? <div className="mt-1 text-xs text-blue-700">│ {rota.legs_json[idx].distance_km.toLocaleString('pt-BR', { minimumFractionDigits: 1, maximumFractionDigits: 1 })} km{rota.legs_json[idx].duration_text ? ` • ${rota.legs_json[idx].duration_text}` : ''}</div> : null}
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
