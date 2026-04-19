import { useState, useRef, useCallback, useEffect, useMemo } from 'react'
import {
  Upload, FileSpreadsheet, Play, Loader2,
  CheckCircle, XCircle, AlertTriangle,
  RotateCcw, Package, Truck, MapPin, Clock
} from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import { useUploadCarteira } from '@/hooks/useUploadCarteira'
import { roteirizacaoService } from '@/services/roteirizacao.service'
import { veiculosService } from '@/services/veiculos.service'
import { filiaisService } from '@/services/filiais.service'
import { ManifestoComFrete, RodadaRoteirizacao, FiltrosRoteirizacao, Filial } from '@/types'
import { ManifestoCard } from '@/components/roteirizacao/ManifestoCard'
import { EncadeamentoPanel } from '@/components/roteirizacao/EncadeamentoPanel'
import { NaoRoteirizadosPanel } from '@/components/roteirizacao/NaoRoteirizadosPanel'
import { getErrorMessage } from '@/lib/async'
import toast from 'react-hot-toast'

type Etapa = 'upload' | 'preview' | 'filtros' | 'processando' | 'resultado'

const TIPOS_ROTEIRIZACAO = [
  { value: 'padrao', label: 'Padrão', desc: 'Roteirização completa com todos os algoritmos' },
  { value: 'expressa', label: 'Expressa', desc: 'Foco em entregas urgentes e com agenda próxima' },
  { value: 'economica', label: 'Econômica', desc: 'Maximiza ocupação, reduz número de veículos' },
]

export function RoteirizacaoPage() {
  const {
    user,
    filialAtiva,
    isMaster,
    profileLoading,
    profileError,
    filialLoading,
    filialError,
    reloadAuthContext,
  } = useAuth()

  const { state: upload, processar, limpar } = useUploadCarteira()
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [etapa, setEtapa] = useState<Etapa>('upload')
  const [filtros, setFiltros] = useState<FiltrosRoteirizacao>({
    data_base: new Date().toISOString().slice(0, 16),
    tipo_roteirizacao: 'padrao',
    filial_id: filialAtiva?.id || '',
    considerar_agendados: true,
    apenas_agendados: false,
    veiculos_ids: [],
  })
  const [processando, setProcessando] = useState(false)
  const [progressoMsg, setProgressoMsg] = useState('')
  const [rodada, setRodada] = useState<RodadaRoteirizacao | null>(null)
  const [manifestos, setManifestos] = useState<ManifestoComFrete[]>([])
  const [abaAtiva, setAbaAtiva] = useState<'manifestos' | 'encadeamento' | 'nao_roteirizados'>('manifestos')

  const [filiaisMaster, setFiliaisMaster] = useState<Filial[]>([])
  const [filiaisMasterLoading, setFiliaisMasterLoading] = useState(false)
  const [filiaisMasterError, setFiliaisMasterError] = useState<string | null>(null)
  const [filialSelecionadaMaster, setFilialSelecionadaMaster] = useState('')

  const filialOperacionalId = isMaster ? filialSelecionadaMaster : (filialAtiva?.id ?? '')
  const filialOperacional = useMemo(() => {
    if (isMaster) {
      return filiaisMaster.find((f) => f.id === filialSelecionadaMaster) ?? null
    }
    return filialAtiva ?? null
  }, [isMaster, filialAtiva, filiaisMaster, filialSelecionadaMaster])

  useEffect(() => {
    setFiltros((prev) => (
      prev.filial_id === filialOperacionalId
        ? prev
        : { ...prev, filial_id: filialOperacionalId }
    ))
  }, [filialOperacionalId])

  useEffect(() => {
    const carregarFiliaisMaster = async () => {
      if (!isMaster) {
        setFiliaisMaster([])
        setFilialSelecionadaMaster('')
        setFiliaisMasterError(null)
        return
      }

      setFiliaisMasterLoading(true)
      setFiliaisMasterError(null)

      try {
        const data = await filiaisService.buscarAtivas()
        setFiliaisMaster(data)
        setFilialSelecionadaMaster((prev) => {
          if (prev && data.some((f) => f.id === prev)) return prev
          return data[0]?.id ?? ''
        })
      } catch (error) {
        console.error('[RoteirizacaoPage] Falha ao carregar filiais para master', error)
        setFiliaisMasterError(getErrorMessage(error, 'Não foi possível carregar as filiais ativas.'))
      } finally {
        setFiliaisMasterLoading(false)
      }
    }

    carregarFiliaisMaster()
  }, [isMaster])

  const handleArquivo = async (file: File) => {
    if (!file.name.match(/\.(xlsx|xls|csv)$/i)) {
      toast.error('Formato inválido. Use .xlsx, .xls ou .csv')
      return
    }

    if (!user?.id) {
      toast.error('Usuário não identificado para importar carteira')
      return
    }

    if (!filialOperacionalId) {
      toast.error('Selecione uma filial para continuar')
      return
    }

    await processar(file, user.id, filialOperacionalId)
    setEtapa('preview')
  }

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      const file = e.dataTransfer.files[0]
      if (file) {
        void handleArquivo(file)
      }
    },
    [handleArquivo]
  )

  const confirmarPreview = () => {
    if (upload.totalLinhas === 0 || !upload.uploadId) {
      toast.error('Arquivo sem dados')
      return
    }
    setEtapa('filtros')
  }

  const roteirizar = async () => {
    if (!filialOperacionalId || !filialOperacional) {
      toast.error('Selecione uma filial válida para roteirizar')
      return
    }
    if (!filtros.data_base) {
      toast.error('Informe a data base da roteirização')
      return
    }
    if (!upload.uploadId) {
      toast.error('Upload não encontrado. Reimporte a carteira.')
      return
    }
    if (!user?.id) {
      toast.error('Usuário não autenticado')
      return
    }

    setProcessando(true)
    setEtapa('processando')

    try {
      setProgressoMsg('Carregando frota da filial...')
      const veiculos = await veiculosService.listarAtivos(filialOperacionalId)

      if (veiculos.length === 0) {
        toast.error('Nenhum veículo ativo cadastrado para esta filial')
        setEtapa('filtros')
        setProcessando(false)
        return
      }

      setProgressoMsg(`${veiculos.length} veículo(s) carregado(s). Enviando para o Motor...`)

      const resultado = await roteirizacaoService.roteirizar(
        filialOperacional,
        veiculos,
        upload.uploadId,
        { ...filtros, filial_id: filialOperacionalId },
        user.id
      )

      setRodada(resultado.rodada)
      setManifestos(resultado.manifestos)
      setEtapa('resultado')
      toast.success(`Roteirização concluída! ${resultado.manifestos.length} manifesto(s) gerado(s)`)
    } catch (err) {
      const msg = err instanceof Error ? err.message : 'Erro desconhecido'
      toast.error(msg)
      setEtapa('filtros')
    } finally {
      setProcessando(false)
      setProgressoMsg('')
    }
  }

  const reiniciar = () => {
    limpar()
    setEtapa('upload')
    setRodada(null)
    setManifestos([])
  }

  const excluirManifesto = (id: string) => {
    setManifestos((prev) => prev.filter((m) => m.id_manifesto !== id))
    toast.success('Manifesto removido')
  }

  const aprovarManifesto = (id: string) => {
    setManifestos((prev) =>
      prev.map((m) => m.id_manifesto === id ? { ...m, aprovado: !m.aprovado } : m)
    )
  }

  const estadoAuthBloqueante = (profileLoading || filialLoading) && !profileError && !filialError && !filialOperacional

  if (estadoAuthBloqueante) {
    return (
      <div className="fade-in max-w-2xl mx-auto">
        <div className="card p-8 text-center">
          <Loader2 className="mx-auto mb-3 animate-spin text-brand-600" />
          <h2 className="font-semibold text-gray-900">Carregando perfil...</h2>
          <p className="text-sm text-gray-500 mt-2">Aguarde enquanto validamos perfil e filial.</p>
        </div>
      </div>
    )
  }

  if (profileError) {
    return (
      <div className="fade-in max-w-2xl mx-auto">
        <div className="card p-6 border border-red-200 bg-red-50">
          <div className="flex items-start gap-3">
            <AlertTriangle className="text-red-600 mt-0.5" size={18} />
            <div>
              <h2 className="font-semibold text-red-800">Não foi possível carregar o perfil.</h2>
              <p className="text-sm text-red-700 mt-1">{profileError}</p>
              <button className="btn-primary mt-4" onClick={() => void reloadAuthContext()}>
                <RotateCcw size={14} /> Tentar novamente
              </button>
            </div>
          </div>
        </div>
      </div>
    )
  }

  // ─── ETAPA: UPLOAD ────────────────────────────────────────────────────────
  if (etapa === 'upload') {
    return (
      <div className="fade-in max-w-2xl mx-auto">
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-gray-900">Nova Roteirização</h1>
          <p className="text-gray-500 mt-1">
            Filial: <strong>{filialOperacional?.nome || '—'}</strong>
          </p>
        </div>

        {isMaster && (
          <div className="card p-4 mb-4 space-y-2">
            <label className="label">Filial operacional *</label>
            <select
              className="input"
              value={filialSelecionadaMaster}
              onChange={(e) => setFilialSelecionadaMaster(e.target.value)}
              disabled={filiaisMasterLoading}
            >
              <option value="">Selecione uma filial...</option>
              {filiaisMaster.map((f) => <option key={f.id} value={f.id}>{f.nome}</option>)}
            </select>
            {filiaisMasterError && <p className="text-sm text-red-600">{filiaisMasterError}</p>}
          </div>
        )}

        {filialError && (
          <div className="mt-4 mb-4 bg-amber-50 border border-amber-200 rounded-xl p-4 flex gap-3">
            <AlertTriangle size={18} className="text-amber-600 flex-shrink-0" />
            <p className="text-amber-700 text-sm">{filialError}</p>
          </div>
        )}

        <div
          className="border-2 border-dashed border-brand-300 rounded-2xl p-12 text-center cursor-pointer hover:border-brand-500 hover:bg-brand-50 transition-all"
          onDrop={onDrop}
          onDragOver={(e) => e.preventDefault()}
          onClick={() => fileInputRef.current?.click()}
        >
          <Upload size={48} className="mx-auto text-brand-400 mb-4" />
          <h3 className="text-lg font-semibold text-gray-700 mb-2">
            Arraste a carteira de cargas aqui
          </h3>
          <p className="text-gray-500 text-sm mb-4">ou clique para selecionar o arquivo</p>
          <span className="badge-gray">XLSX · XLS · CSV</span>
          <input
            ref={fileInputRef}
            type="file"
            accept=".xlsx,.xls,.csv"
            className="hidden"
            onChange={(e) => e.target.files?.[0] && handleArquivo(e.target.files[0])}
          />
        </div>

        {!filialOperacionalId && (
          <p className="text-sm text-amber-700 mt-3">Selecione uma filial para continuar.</p>
        )}

        {upload.carregando && (
          <div className="flex items-center justify-center gap-2 mt-6 text-brand-600">
            <Loader2 size={20} className="animate-spin" />
            <span>Processando arquivo...</span>
          </div>
        )}

        {upload.erro && (
          <div className="mt-4 bg-red-50 border border-red-200 rounded-xl p-4 flex gap-3">
            <XCircle size={18} className="text-red-500 flex-shrink-0" />
            <p className="text-red-700 text-sm">{upload.erro}</p>
          </div>
        )}
      </div>
    )
  }

  if (etapa === 'preview') {
    return (
      <div className="fade-in">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1>Preview da Carteira</h1>
            <p className="text-gray-500 text-sm">Confirme os dados antes de roteirizar</p>
          </div>
          <button className="btn-ghost" onClick={reiniciar}>
            <RotateCcw size={16} /> Novo Upload
          </button>
        </div>

        <div className="grid grid-cols-3 gap-4 mb-6">
          <div className="card p-4 text-center">
            <FileSpreadsheet size={24} className="mx-auto text-brand-600 mb-2" />
            <div className="text-2xl font-bold text-gray-900">{upload.totalLinhas}</div>
            <div className="text-sm text-gray-500">Linhas carregadas</div>
          </div>
          <div className="card p-4 text-center">
            <Package size={24} className="mx-auto text-brand-600 mb-2" />
            <div className="text-2xl font-bold text-gray-900">{upload.totalColunas}</div>
            <div className="text-sm text-gray-500">Colunas detectadas</div>
          </div>
          <div className="card p-4 text-center">
            <CheckCircle size={24} className="mx-auto text-green-600 mb-2" />
            <div className="text-lg font-bold text-gray-900 truncate">{upload.nomeArquivo || upload.arquivo?.name}</div>
            <div className="text-sm text-gray-500">Arquivo</div>
          </div>
        </div>

        <div className="card mb-6">
          <div className="card-header">
            <h3>Primeiras 5 linhas</h3>
            <span className="text-sm text-gray-500">Linha de cabeçalho detectada: {upload.linhaCabecalhoDetectada ?? '—'}</span>
          </div>
          <div className="table-container">
            <table className="table text-xs">
              <thead>
                <tr>
                  {upload.colunasDetectadas.slice(0, 12).map((col) => (
                    <th key={col} className="whitespace-nowrap">{col}</th>
                  ))}
                  {upload.colunasDetectadas.length > 12 && <th>+{upload.colunasDetectadas.length - 12} cols</th>}
                </tr>
              </thead>
              <tbody>
                {upload.preview.map((row, i) => (
                  <tr key={i}>
                    {upload.colunasDetectadas.slice(0, 12).map((col) => (
                      <td key={col} className="max-w-[120px] truncate">{String(row[col] ?? '—')}</td>
                    ))}
                    {upload.colunasDetectadas.length > 12 && <td className="text-gray-400">...</td>}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="flex justify-end gap-3">
          <button className="btn-secondary" onClick={reiniciar}>Cancelar</button>
          <button className="btn-primary" onClick={confirmarPreview}>
            <CheckCircle size={16} /> Confirmar e Configurar Filtros
          </button>
        </div>
      </div>
    )
  }

  if (etapa === 'filtros') {
    return (
      <div className="fade-in max-w-2xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1>Configurar Roteirização</h1>
            <p className="text-gray-500 text-sm">{upload.totalLinhas} cargas · {filialOperacional?.nome || 'Filial não selecionada'}</p>
          </div>
          <button className="btn-ghost" onClick={() => setEtapa('preview')}>Voltar</button>
        </div>

        <div className="card p-6 space-y-6">
          {isMaster && (
            <div>
              <label className="label">Filial operacional *</label>
              <select className="input" value={filialSelecionadaMaster} onChange={(e) => setFilialSelecionadaMaster(e.target.value)}>
                <option value="">Selecione a filial...</option>
                {filiaisMaster.map((f) => <option key={f.id} value={f.id}>{f.nome}</option>)}
              </select>
            </div>
          )}

          <div>
            <label className="label">Data Base da Roteirização *</label>
            <input type="datetime-local" className="input" value={filtros.data_base} onChange={(e) => setFiltros({ ...filtros, data_base: e.target.value })} />
          </div>

          <div>
            <label className="label">Tipo de Roteirização *</label>
            <div className="space-y-3 mt-2">
              {TIPOS_ROTEIRIZACAO.map((tipo) => (
                <label
                  key={tipo.value}
                  className={`flex items-start gap-3 p-4 rounded-xl border-2 cursor-pointer transition-all ${
                    filtros.tipo_roteirizacao === tipo.value ? 'border-brand-500 bg-brand-50' : 'border-gray-200 hover:border-gray-300'
                  }`}
                >
                  <input
                    type="radio"
                    name="tipo_roteirizacao"
                    value={tipo.value}
                    checked={filtros.tipo_roteirizacao === tipo.value}
                    onChange={() => setFiltros({ ...filtros, tipo_roteirizacao: tipo.value as FiltrosRoteirizacao['tipo_roteirizacao'] })}
                    className="mt-0.5"
                  />
                  <div>
                    <div className="font-semibold text-gray-900">{tipo.label}</div>
                    <div className="text-sm text-gray-500">{tipo.desc}</div>
                  </div>
                </label>
              ))}
            </div>
          </div>
        </div>

        <div className="flex justify-end gap-3 mt-6">
          <button className="btn-secondary" onClick={() => setEtapa('preview')}>Voltar</button>
          <button className="btn-primary text-base px-8 py-3" onClick={roteirizar} disabled={!filialOperacionalId}>
            <Play size={18} /> Roteirizar Agora
          </button>
        </div>
      </div>
    )
  }

  if (etapa === 'processando') {
    return (
      <div className="fade-in flex flex-col items-center justify-center min-h-[60vh]">
        <div className="text-center">
          <div className="relative w-24 h-24 mx-auto mb-6">
            <div className="absolute inset-0 rounded-full border-4 border-brand-100" />
            <div className="absolute inset-0 rounded-full border-4 border-brand-600 border-t-transparent animate-spin" />
            <Truck size={32} className="absolute inset-0 m-auto text-brand-600" />
          </div>
          <h2 className="text-xl font-bold text-gray-900 mb-2">Processando Roteirização</h2>
          <p className="text-gray-500 mb-4">{progressoMsg || 'Aguarde enquanto o Motor otimiza as rotas...'}</p>
          <div className="flex items-center justify-center gap-2 text-sm text-gray-400">
            <Clock size={14} />
            <span>Isso pode levar até 3 minutos para carteiras grandes</span>
          </div>
        </div>
      </div>
    )
  }

  if (etapa === 'resultado' && rodada) {
    const manifestosAtivos = manifestos.filter((m) => !m.excluido)
    const totalFreteMinimo = manifestosAtivos.reduce((sum, m) => sum + (m.frete_minimo_antt || 0), 0)

    return (
      <div className="fade-in">
        <div className="flex items-center justify-between mb-6">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <CheckCircle size={20} className="text-green-600" />
              <h1>Roteirização Concluída</h1>
            </div>
            <p className="text-gray-500 text-sm">
              {filialOperacional?.nome} · {new Date(rodada.created_at).toLocaleString('pt-BR')} ·
              {(rodada.tempo_processamento_ms / 1000).toFixed(1)}s
            </p>
          </div>
          <button className="btn-ghost" onClick={reiniciar}>
            <RotateCcw size={16} /> Nova Roteirização
          </button>
        </div>

        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
          {[
            { label: 'Manifestos', value: manifestosAtivos.length, icon: Package, color: 'text-brand-600' },
            { label: 'Itens Roteirizados', value: rodada.total_itens_manifestados, icon: CheckCircle, color: 'text-green-600' },
            { label: 'Não Roteirizados', value: rodada.total_nao_roteirizados, icon: AlertTriangle, color: 'text-amber-600' },
            { label: 'KM Total', value: `${rodada.km_total_frota?.toLocaleString('pt-BR')} km`, icon: MapPin, color: 'text-blue-600' },
            { label: 'Frete Mínimo Total', value: `R$ ${totalFreteMinimo.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`, icon: Truck, color: 'text-purple-600' },
          ].map(({ label, value, icon: Icon, color }) => (
            <div key={label} className="card p-4">
              <Icon size={18} className={`${color} mb-2`} />
              <div className="text-xl font-bold text-gray-900">{value}</div>
              <div className="text-xs text-gray-500">{label}</div>
            </div>
          ))}
        </div>

        <div className="flex gap-1 mb-4 bg-gray-100 rounded-xl p-1 w-fit">
          {[
            { key: 'manifestos', label: `Manifestos (${manifestosAtivos.length})` },
            { key: 'encadeamento', label: 'Encadeamento' },
            { key: 'nao_roteirizados', label: `Não Roteirizados (${rodada.total_nao_roteirizados})` },
          ].map(({ key, label }) => (
            <button
              key={key}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${abaAtiva === key ? 'bg-white shadow text-brand-700' : 'text-gray-600 hover:text-gray-900'}`}
              onClick={() => setAbaAtiva(key as typeof abaAtiva)}
            >
              {label}
            </button>
          ))}
        </div>

        {abaAtiva === 'manifestos' && (
          <div className="space-y-4">
            {manifestosAtivos.length === 0 ? (
              <div className="card p-12 text-center text-gray-400">
                <Package size={40} className="mx-auto mb-3 opacity-30" />
                <p>Todos os manifestos foram removidos</p>
              </div>
            ) : (
              manifestosAtivos.map((manifesto) => (
                <ManifestoCard
                  key={manifesto.id_manifesto}
                  manifesto={manifesto}
                  onAprovar={() => aprovarManifesto(manifesto.id_manifesto)}
                  onExcluir={() => excluirManifesto(manifesto.id_manifesto)}
                />
              ))
            )}
          </div>
        )}

        {abaAtiva === 'encadeamento' && rodada.resposta_motor && (
          <EncadeamentoPanel encadeamento={rodada.resposta_motor.encadeamento ?? []} />
        )}

        {abaAtiva === 'nao_roteirizados' && rodada.resposta_motor && (
          <NaoRoteirizadosPanel resposta={rodada.resposta_motor} />
        )}
      </div>
    )
  }

  return null
}
