import { useState, useRef, useCallback } from 'react'
import {
  Upload, FileSpreadsheet, Filter, Play, Loader2,
  CheckCircle, XCircle, AlertTriangle, ChevronDown,
  ChevronUp, Trash2, ArrowUpDown, Printer, Eye,
  RotateCcw, Package, Truck, MapPin, Clock
} from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import { useUploadCarteira } from '@/hooks/useUploadCarteira'
import { roteirizacaoService } from '@/services/roteirizacao.service'
import { veiculosService } from '@/services/veiculos.service'
import { ManifestoComFrete, RodadaRoteirizacao, FiltrosRoteirizacao } from '@/types'
import { ManifestoCard } from '@/components/roteirizacao/ManifestoCard'
import { EncadeamentoPanel } from '@/components/roteirizacao/EncadeamentoPanel'
import { NaoRoteirizadosPanel } from '@/components/roteirizacao/NaoRoteirizadosPanel'
import toast from 'react-hot-toast'

type Etapa = 'upload' | 'preview' | 'filtros' | 'processando' | 'resultado'

const TIPOS_ROTEIRIZACAO = [
  { value: 'padrao', label: 'Padrão', desc: 'Roteirização completa com todos os algoritmos' },
  { value: 'expressa', label: 'Expressa', desc: 'Foco em entregas urgentes e com agenda próxima' },
  { value: 'economica', label: 'Econômica', desc: 'Maximiza ocupação, reduz número de veículos' },
]

export function RoteirizacaoPage() {
  const { user, filialAtiva } = useAuth()
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

  const onDrop = useCallback(
    (e: React.DragEvent) => {
      e.preventDefault()
      const file = e.dataTransfer.files[0]
      if (file) handleArquivo(file)
    },
    []
  )

  const handleArquivo = async (file: File) => {
    if (!file.name.match(/\.(xlsx|xls|csv)$/i)) {
      toast.error('Formato inválido. Use .xlsx, .xls ou .csv')
      return
    }
    if (!user?.id || !filialAtiva?.id) {
      toast.error('Usuário ou filial não identificados para importar carteira')
      return
    }

    await processar(file, user.id, filialAtiva.id)
    setEtapa('preview')
  }

  const confirmarPreview = () => {
    if (upload.totalLinhas === 0 || !upload.uploadId) {
      toast.error('Arquivo sem dados')
      return
    }
    setEtapa('filtros')
  }

  const roteirizar = async () => {
    if (!filialAtiva) {
      toast.error('Filial não identificada')
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

    setProcessando(true)
    setEtapa('processando')

    try {
      // Buscar veículos ativos da filial
      setProgressoMsg('Carregando frota da filial...')
      const veiculos = await veiculosService.listarAtivos(filialAtiva.id)

      if (veiculos.length === 0) {
        toast.error('Nenhum veículo ativo cadastrado para esta filial')
        setEtapa('filtros')
        setProcessando(false)
        return
      }

      setProgressoMsg(`${veiculos.length} veículo(s) carregado(s). Enviando para o Motor...`)

      // Disparar roteirização
      const resultado = await roteirizacaoService.roteirizar(
        filialAtiva,
        veiculos,
        upload.uploadId,
        filtros,
        user!.id
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

  // ─── ETAPA: UPLOAD ────────────────────────────────────────────────────────
  if (etapa === 'upload') {
    return (
      <div className="fade-in max-w-2xl mx-auto">
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-gray-900">Nova Roteirização</h1>
          <p className="text-gray-500 mt-1">
            Filial: <strong>{filialAtiva?.nome || '—'}</strong>
          </p>
        </div>

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

  // ─── ETAPA: PREVIEW ───────────────────────────────────────────────────────
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

        {/* Resumo */}
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

        {/* Tabela preview */}
        <div className="card mb-6">
          <div className="card-header">
            <h3>Primeiras 5 linhas</h3>
            <span className="text-sm text-gray-500">Linha de cabeçalho detectada: {upload.linhaCabecalhoDetectada ?? "—"}</span>
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
                      <td key={col} className="max-w-[120px] truncate">
                        {String(row[col] ?? '—')}
                      </td>
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

  // ─── ETAPA: FILTROS ───────────────────────────────────────────────────────
  if (etapa === 'filtros') {
    return (
      <div className="fade-in max-w-2xl mx-auto">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1>Configurar Roteirização</h1>
            <p className="text-gray-500 text-sm">{upload.totalLinhas} cargas · {filialAtiva?.nome}</p>
          </div>
          <button className="btn-ghost" onClick={() => setEtapa('preview')}>
            Voltar
          </button>
        </div>

        <div className="card p-6 space-y-6">
          <div>
            <label className="label">Data Base da Roteirização *</label>
            <input
              type="datetime-local"
              className="input"
              value={filtros.data_base}
              onChange={(e) => setFiltros({ ...filtros, data_base: e.target.value })}
            />
            <p className="text-xs text-gray-500 mt-1">
              Data de referência para calcular folga de agendamentos e prioridades
            </p>
          </div>

          <div>
            <label className="label">Tipo de Roteirização *</label>
            <div className="space-y-3 mt-2">
              {TIPOS_ROTEIRIZACAO.map((tipo) => (
                <label
                  key={tipo.value}
                  className={`flex items-start gap-3 p-4 rounded-xl border-2 cursor-pointer transition-all ${
                    filtros.tipo_roteirizacao === tipo.value
                      ? 'border-brand-500 bg-brand-50'
                      : 'border-gray-200 hover:border-gray-300'
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

          {/* Resumo da frota */}
          <div className="bg-gray-50 rounded-xl p-4">
            <div className="flex items-center gap-2 mb-2">
              <Truck size={16} className="text-gray-600" />
              <span className="text-sm font-semibold text-gray-700">Frota da filial será carregada automaticamente</span>
            </div>
            <p className="text-xs text-gray-500">
              Todos os veículos ativos da filial {filialAtiva?.nome} serão utilizados na roteirização.
              Gerencie a frota em Cadastros → Veículos.
            </p>
          </div>
        </div>

        <div className="flex justify-end gap-3 mt-6">
          <button className="btn-secondary" onClick={() => setEtapa('preview')}>Voltar</button>
          <button className="btn-primary text-base px-8 py-3" onClick={roteirizar}>
            <Play size={18} /> Roteirizar Agora
          </button>
        </div>
      </div>
    )
  }

  // ─── ETAPA: PROCESSANDO ───────────────────────────────────────────────────
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

        {/* Pipeline visual */}
        <div className="mt-10 flex items-center gap-3">
          {['M1 Padronização', 'M2 Enriquecimento', 'M3 Triagem', 'M4 Manifestos', 'M5 Composição', 'M6 Consolidação', 'M7 Sequenciamento'].map((m, i) => (
            <div key={m} className="flex items-center gap-3">
              <div className="flex flex-col items-center">
                <div className="w-8 h-8 rounded-full bg-brand-100 flex items-center justify-center text-xs font-bold text-brand-700 animate-pulse">
                  {i + 1}
                </div>
                <span className="text-xs text-gray-400 mt-1 whitespace-nowrap hidden lg:block">{m.split(' ')[0]}</span>
              </div>
              {i < 6 && <div className="w-4 h-0.5 bg-brand-200" />}
            </div>
          ))}
        </div>
      </div>
    )
  }

  // ─── ETAPA: RESULTADO ─────────────────────────────────────────────────────
  if (etapa === 'resultado' && rodada) {
    const manifestosAtivos = manifestos.filter((m) => !m.excluido)
    const totalFreteMinimo = manifestosAtivos.reduce((sum, m) => sum + (m.frete_minimo_antt || 0), 0)

    return (
      <div className="fade-in">
        {/* Header resultado */}
        <div className="flex items-center justify-between mb-6">
          <div>
            <div className="flex items-center gap-2 mb-1">
              <CheckCircle size={20} className="text-green-600" />
              <h1>Roteirização Concluída</h1>
            </div>
            <p className="text-gray-500 text-sm">
              {filialAtiva?.nome} · {new Date(rodada.created_at).toLocaleString('pt-BR')} ·
              {(rodada.tempo_processamento_ms / 1000).toFixed(1)}s
            </p>
          </div>
          <button className="btn-ghost" onClick={reiniciar}>
            <RotateCcw size={16} /> Nova Roteirização
          </button>
        </div>

        {/* KPIs */}
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

        {/* Abas */}
        <div className="flex gap-1 mb-4 bg-gray-100 rounded-xl p-1 w-fit">
          {[
            { key: 'manifestos', label: `Manifestos (${manifestosAtivos.length})` },
            { key: 'encadeamento', label: 'Encadeamento' },
            { key: 'nao_roteirizados', label: `Não Roteirizados (${rodada.total_nao_roteirizados})` },
          ].map(({ key, label }) => (
            <button
              key={key}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                abaAtiva === key ? 'bg-white shadow text-brand-700' : 'text-gray-600 hover:text-gray-900'
              }`}
              onClick={() => setAbaAtiva(key as typeof abaAtiva)}
            >
              {label}
            </button>
          ))}
        </div>

        {/* Conteúdo das abas */}
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
