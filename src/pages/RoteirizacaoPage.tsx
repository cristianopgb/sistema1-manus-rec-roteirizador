import { useState, useRef, useCallback, useEffect, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import {
  Upload, FileSpreadsheet, Play, Loader2,
  CheckCircle, XCircle, AlertTriangle,
  RotateCcw, Package, Truck, MapPin, Clock,
} from 'lucide-react'
import { useAuth } from '@/contexts/AuthContext'
import { useUploadCarteira } from '@/hooks/useUploadCarteira'
import { roteirizacaoService } from '@/services/roteirizacao.service'
import { veiculosService } from '@/services/veiculos.service'
import { filiaisService } from '@/services/filiais.service'
import { carteiraUploadService } from '@/services/carteira-upload.service'
import {
  ManifestoComFrete,
  RodadaRoteirizacao,
  FiltrosRoteirizacao,
  Filial,
  FiltrosCarteira,
  CarteiraCarga,
  ConfiguracaoFrotaItem,
} from '@/types'
import { ManifestoCard } from '@/components/roteirizacao/ManifestoCard'
import { EncadeamentoPanel } from '@/components/roteirizacao/EncadeamentoPanel'
import { NaoRoteirizadosPanel } from '@/components/roteirizacao/NaoRoteirizadosPanel'
import { UploadHistoryList, UploadHistoricoItem } from '@/components/roteirizacao/UploadHistoryList'
import { CarteiraPreviewTable } from '@/components/roteirizacao/CarteiraPreviewTable'
import { CarteiraFiltersPanel } from '@/components/roteirizacao/CarteiraFiltersPanel'
import { RealtimeSummaryCard } from '@/components/roteirizacao/RealtimeSummaryCard'
import { getErrorMessage } from '@/lib/async'
import toast from 'react-hot-toast'

type Etapa = 'upload' | 'preview' | 'filtros' | 'processando' | 'resultado'
type MultiSelectField = keyof Pick<FiltrosCarteira, 'filial_r' | 'uf' | 'destin' | 'cidade' | 'tomad' | 'mesoregiao' | 'prioridade' | 'restricao_veiculo'>

const TIPOS_ROTEIRIZACAO: Array<{ value: FiltrosRoteirizacao['tipo_roteirizacao']; label: string; desc: string }> = [
  { value: 'carteira', label: 'Carteira', desc: 'Roteiriza o máximo possível da carteira filtrada' },
  { value: 'frota', label: 'Frota', desc: 'Usa configuração manual de quantidade por perfil' },
]

const FILTROS_CARTEIRA_INICIAIS: FiltrosCarteira = {
  filial_r: [], uf: [], destin: [], cidade: [], tomad: [], mesoregiao: [], prioridade: [], restricao_veiculo: [],
  carro_dedicado: 'todos',
  agendam_de: '', agendam_ate: '', dle_de: '', dle_ate: '', data_des_de: '', data_des_ate: '', data_nf_de: '', data_nf_ate: '',
}

const TABELA_COLUNAS_PADRAO = [
  { key: 'linha_numero', label: 'Linha' },
  { key: 'filial_r', label: 'Filial R' },
  { key: 'romane', label: 'Romane' },
  { key: 'filial_d', label: 'Filial D' },
  { key: 'serie', label: 'Série' },
  { key: 'nro_doc', label: 'Nro Doc' },
  { key: 'data_des', label: 'Data Des' },
  { key: 'data_nf', label: 'Data NF' },
  { key: 'dle', label: 'D.L.E.' },
  { key: 'agendam', label: 'Agendam' },
  { key: 'palet', label: 'Palet' },
  { key: 'conf', label: 'Conf' },
  { key: 'peso', label: 'Peso' },
  { key: 'vlr_merc', label: 'Vlr Merc' },
  { key: 'qtd', label: 'Qtd' },
  { key: 'peso_cubico', label: 'Peso Cub' },
  { key: 'classif', label: 'Classif' },
  { key: 'tomad', label: 'Tomad' },
  { key: 'destin', label: 'Destin' },
  { key: 'bairro', label: 'Bairro' },
  { key: 'cidade', label: 'Cidade' },
  { key: 'uf', label: 'UF' },
]

const COLUNAS_TABELA_ORDENADAS = TABELA_COLUNAS_PADRAO.map((coluna) => coluna.key)

const formatColumnLabel = (coluna: string) => coluna
  .replace(/_/g, ' ')
  .replace(/\s+/g, ' ')
  .trim()
  .replace(/\b\w/g, (l) => l.toUpperCase())

export function RoteirizacaoPage() {
  const navigate = useNavigate()
  const { user, filialAtiva, isMaster, profileLoading, profileError, filialLoading, filialError, reloadAuthContext } = useAuth()
  const { state: upload, processar, limpar, carregarUploadExistente } = useUploadCarteira()
  const fileInputRef = useRef<HTMLInputElement>(null)

  const [etapa, setEtapa] = useState<Etapa>('upload')
  const [filtrosCarteira, setFiltrosCarteira] = useState<FiltrosCarteira>(FILTROS_CARTEIRA_INICIAIS)
  const [filtrosAplicadosCarteira, setFiltrosAplicadosCarteira] = useState<FiltrosCarteira>(FILTROS_CARTEIRA_INICIAIS)
  const [filtrosExpandidos, setFiltrosExpandidos] = useState(false)
  const [filtros, setFiltros] = useState<FiltrosRoteirizacao>({ data_base: new Date().toISOString().slice(0, 16), tipo_roteirizacao: 'carteira', filial_id: filialAtiva?.id || '', filtros_aplicados: FILTROS_CARTEIRA_INICIAIS, configuracao_frota: [] })

  const [processando, setProcessando] = useState(false)
  const [progressoMsg, setProgressoMsg] = useState('')
  const [rodada, setRodada] = useState<RodadaRoteirizacao | null>(null)
  const [manifestos, setManifestos] = useState<ManifestoComFrete[]>([])
  const [abaAtiva, setAbaAtiva] = useState<'manifestos' | 'encadeamento' | 'nao_roteirizados'>('manifestos')

  const [historicoUploads, setHistoricoUploads] = useState<UploadHistoricoItem[]>([])
  const [historicoLoading, setHistoricoLoading] = useState(false)
  const [deletingUploadId, setDeletingUploadId] = useState<string | null>(null)

  const [filiaisMaster, setFiliaisMaster] = useState<Filial[]>([])
  const [filiaisMasterLoading, setFiliaisMasterLoading] = useState(false)
  const [filiaisMasterError, setFiliaisMasterError] = useState<string | null>(null)
  const [filialSelecionadaMaster, setFilialSelecionadaMaster] = useState('')

  const [totalValidas, setTotalValidas] = useState(0)
  const [carteiraFiltrada, setCarteiraFiltrada] = useState<CarteiraCarga[]>([])
  const [previewRows, setPreviewRows] = useState<CarteiraCarga[]>([])
  const [resumoLoading, setResumoLoading] = useState(false)
  const [resumoErro, setResumoErro] = useState<string | null>(null)
  const [opcoesFiltro, setOpcoesFiltro] = useState<Record<MultiSelectField, string[]>>({ filial_r: [], uf: [], destin: [], cidade: [], tomad: [], mesoregiao: [], prioridade: [], restricao_veiculo: [] })

  const [quantidadePorPerfil, setQuantidadePorPerfil] = useState<Record<string, number>>({})
  const [perfisFrota, setPerfisFrota] = useState<string[]>([])
  const [frotaLoading, setFrotaLoading] = useState(false)
  const [veiculosAtivosQtd, setVeiculosAtivosQtd] = useState(0)

  const filialOperacionalId = isMaster ? filialSelecionadaMaster : (filialAtiva?.id ?? '')
  const filialOperacional = useMemo(() => (isMaster ? filiaisMaster.find((f) => f.id === filialSelecionadaMaster) ?? null : filialAtiva ?? null), [isMaster, filialAtiva, filiaisMaster, filialSelecionadaMaster])
  const configuracaoFrota = useMemo<ConfiguracaoFrotaItem[]>(() => Object.entries(quantidadePorPerfil).filter(([, qtd]) => qtd > 0).map(([perfil, quantidade]) => ({ perfil, quantidade })), [quantidadePorPerfil])
  const totalColunasResumo = useMemo(() => {
    const primeira = carteiraFiltrada[0] as Record<string, unknown> | undefined
    return primeira ? Object.keys(primeira).filter((k) => !k.startsWith('_')).length : upload.totalColunas
  }, [carteiraFiltrada, upload.totalColunas])
  const colunasCarteira = useMemo(() => {
    const linhas = [...previewRows, ...carteiraFiltrada] as Array<Record<string, unknown>>
    const chavesComConteudo = new Set<string>()

    linhas.forEach((linha) => {
      Object.entries(linha ?? {}).forEach(([key, value]) => {
        if (key.startsWith('_')) return
        if (value === null || value === undefined) return
        if (String(value).trim() === '') return
        chavesComConteudo.add(key)
      })
    })

    const ordenadas = COLUNAS_TABELA_ORDENADAS.filter((key) => chavesComConteudo.has(key))
    const extras = Array.from(chavesComConteudo).filter((key) => !COLUNAS_TABELA_ORDENADAS.includes(key))
    const chavesFinais = [...ordenadas, ...extras]

    if (!chavesFinais.length) return TABELA_COLUNAS_PADRAO

    return chavesFinais.map((key) => ({ key, label: formatColumnLabel(key) }))
  }, [carteiraFiltrada, previewRows])

  const carregarHistorico = useCallback(async () => {
    if (!filialOperacionalId && !isMaster) return
    try {
      setHistoricoLoading(true)
      const uploads = await carteiraUploadService.listarUploadsRecentes(filialOperacionalId || undefined, 12)
      const mapped: UploadHistoricoItem[] = uploads.map((u) => ({
        id: u.id,
        nomeArquivo: u.nome_arquivo,
        createdAt: u.created_at,
        totalValidas: u.total_linhas_validas,
        totalImportadas: u.total_linhas_importadas,
        status: u.status,
      }))
      if (import.meta.env.DEV) console.log('[UPLOADS] carregados:', mapped.length)
      setHistoricoUploads(mapped)
    } catch (error) {
      toast.error(getErrorMessage(error, 'Não foi possível carregar uploads recentes.'))
    } finally {
      setHistoricoLoading(false)
    }
  }, [filialOperacionalId, isMaster])

  const carregarUploadById = useCallback(async (uploadId: string) => {
    try {
      setResumoErro(null)
      const uploadData = await carteiraUploadService.buscarUpload(uploadId)
      const preview = await carteiraUploadService.buscarPreviewUpload(uploadId, 5)

      if (import.meta.env.DEV) {
        console.log('[UPLOAD] carregado:', uploadId)
        console.log('[PREVIEW] linhas carregadas:', preview.length)
      }

      carregarUploadExistente({
        uploadId: uploadData.id,
        nomeArquivo: uploadData.nome_arquivo,
        totalLinhas: uploadData.total_linhas_importadas,
        totalColunas: uploadData.total_colunas_detectadas,
        linhaCabecalhoDetectada: uploadData.linha_cabecalho_detectada,
        colunasDetectadas: uploadData.colunas_detectadas_json ?? [],
        preview: preview,
      })
      setPreviewRows(preview)
      setEtapa('preview')
    } catch (error) {
      toast.error(getErrorMessage(error, 'Não foi possível abrir esse upload.'))
    }
  }, [carregarUploadExistente])

  useEffect(() => {
    void carregarHistorico()
  }, [carregarHistorico])

  useEffect(() => {
    setFiltros((prev) => (prev.filial_id === filialOperacionalId ? prev : { ...prev, filial_id: filialOperacionalId }))
  }, [filialOperacionalId])

  useEffect(() => {
    setFiltros((prev) => ({ ...prev, filtros_aplicados: filtrosAplicadosCarteira }))
  }, [filtrosAplicadosCarteira])

  useEffect(() => {
    setFiltros((prev) => ({ ...prev, configuracao_frota: prev.tipo_roteirizacao === 'frota' ? configuracaoFrota : [] }))
  }, [configuracaoFrota])

  useEffect(() => {
    if (import.meta.env.DEV) {
      console.log('[ROTEIRIZACAO] tipo_roteirizacao na UI:', filtros.tipo_roteirizacao)
    }
  }, [filtros.tipo_roteirizacao])

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
        setFilialSelecionadaMaster((prev) => prev && data.some((f) => f.id === prev) ? prev : data[0]?.id ?? '')
      } catch (error) {
        setFiliaisMasterError(getErrorMessage(error, 'Não foi possível carregar as filiais ativas.'))
      } finally {
        setFiliaisMasterLoading(false)
      }
    }
    void carregarFiliaisMaster()
  }, [isMaster])

  useEffect(() => {
    const carregarResumo = async () => {
      if (!upload.uploadId) return
      setResumoLoading(true)
      setResumoErro(null)

      try {
        const validas = await roteirizacaoService.buscarCarteiraFiltrada(upload.uploadId)
        setTotalValidas(validas.length)
        const filtradas = await roteirizacaoService.buscarCarteiraFiltrada(upload.uploadId, filtrosAplicadosCarteira)
        setCarteiraFiltrada(filtradas)

        const opcoes: Record<MultiSelectField, string[]> = { filial_r: [], uf: [], destin: [], cidade: [], tomad: [], mesoregiao: [], prioridade: [], restricao_veiculo: [] }
        ;(Object.keys(opcoes) as MultiSelectField[]).forEach((campo) => {
          const valores = new Set(validas.map((l) => String((l as Record<string, unknown>)[campo] ?? '').trim()).filter(Boolean))
          opcoes[campo] = Array.from(valores).sort((a, b) => a.localeCompare(b, 'pt-BR'))
        })
        setOpcoesFiltro(opcoes)

        const preview = await carteiraUploadService.buscarPreviewUpload(upload.uploadId, 5)
        setPreviewRows(preview)
        if (import.meta.env.DEV) console.log('[FILTROS] valores carregados para upload:', upload.uploadId)
      } catch (error) {
        setResumoErro(getErrorMessage(error, 'Não foi possível atualizar resumo da carteira.'))
      } finally {
        setResumoLoading(false)
      }
    }

    void carregarResumo()
  }, [upload.uploadId, filtrosAplicadosCarteira])

  useEffect(() => {
    const carregarVeiculosAtivos = async () => {
      if (!filialOperacionalId) {
        setVeiculosAtivosQtd(0)
        return
      }
      try {
        const veiculos = await veiculosService.listarAtivos(filialOperacionalId)
        setVeiculosAtivosQtd(veiculos.length)
      } catch {
        setVeiculosAtivosQtd(0)
      }
    }
    void carregarVeiculosAtivos()
  }, [filialOperacionalId])

  useEffect(() => {
    const carregarPerfisFrota = async () => {
      if (filtros.tipo_roteirizacao !== 'frota' || !filialOperacionalId) {
        setPerfisFrota([])
        setQuantidadePorPerfil({})
        return
      }
      setFrotaLoading(true)
      try {
        const veiculos = await veiculosService.listarAtivos(filialOperacionalId)
        const perfis = Array.from(new Set(veiculos.map((v) => v.tipo))).sort((a, b) => a.localeCompare(b, 'pt-BR'))
        setPerfisFrota(perfis)
        setQuantidadePorPerfil((prev) => Object.fromEntries(perfis.map((p) => [p, prev[p] ?? 0])))
      } catch (error) {
        toast.error(getErrorMessage(error, 'Não foi possível carregar perfis de frota.'))
      } finally {
        setFrotaLoading(false)
      }
    }

    void carregarPerfisFrota()
  }, [filtros.tipo_roteirizacao, filialOperacionalId])

  const handleArquivo = useCallback(async (file: File) => {
    if (!file.name.match(/\.(xlsx|xls|csv)$/i)) {
      toast.error('Formato inválido. Use .xlsx, .xls ou .csv')
      return
    }
    if (!user?.id) return toast.error('Usuário não identificado para importar carteira')
    if (!filialOperacionalId) return toast.error('Selecione uma filial para continuar')

    await processar(file, user.id, filialOperacionalId)
    await carregarHistorico()
    setFiltrosCarteira(FILTROS_CARTEIRA_INICIAIS)
    setFiltrosAplicadosCarteira(FILTROS_CARTEIRA_INICIAIS)
    setEtapa('preview')
  }, [user?.id, filialOperacionalId, processar, carregarHistorico])

  const onDrop = useCallback((e: React.DragEvent) => {
    e.preventDefault()
    const file = e.dataTransfer.files[0]
    if (file) void handleArquivo(file)
  }, [handleArquivo])

  const aplicarFiltros = () => {
    setFiltrosAplicadosCarteira(filtrosCarteira)
    if (import.meta.env.DEV) console.log('[FILTROS] aplicados:', filtrosCarteira)
  }

  const limparFiltros = () => {
    setFiltrosCarteira(FILTROS_CARTEIRA_INICIAIS)
    setFiltrosAplicadosCarteira(FILTROS_CARTEIRA_INICIAIS)
    setQuantidadePorPerfil((prev) => Object.fromEntries(Object.keys(prev).map((k) => [k, 0])))
  }

  const confirmarPreview = () => {
    if (upload.totalLinhas === 0 || !upload.uploadId) return toast.error('Arquivo sem dados')
    setFiltrosExpandidos(true)
    setEtapa('filtros')
  }

  const roteirizar = async () => {
    if (!upload.uploadId) return toast.error('Upload não encontrado. Reimporte a carteira.')
    if (!filialOperacionalId || !filialOperacional) return toast.error('Selecione uma filial válida para roteirizar')
    if (!filtros.data_base) return toast.error('Informe a data base da roteirização')
    if (!user?.id) return toast.error('Usuário não autenticado')
    if (!carteiraFiltrada.length) return toast.error('Nenhuma carga encontrada após aplicação dos filtros')
    if (veiculosAtivosQtd === 0) return toast.error('Nenhum veículo ativo encontrado para a filial operacional selecionada.')

    const frotaParaPayload = filtros.tipo_roteirizacao === 'frota' ? configuracaoFrota : []
    if (filtros.tipo_roteirizacao === 'frota' && frotaParaPayload.length === 0) {
      return toast.error('Informe quantidades válidas (> 0) para pelo menos um perfil da frota.')
    }

    setProcessando(true)
    setEtapa('processando')

    try {
      if (import.meta.env.DEV) {
        console.log('[ROTEIRIZACAO] tipo_roteirizacao salvo/enviado:', filtros.tipo_roteirizacao)
      }
      setProgressoMsg('Enviando carteira filtrada para o Motor...')
      const resultado = await roteirizacaoService.roteirizar(
        filialOperacional,
        upload.uploadId,
        {
          ...filtros,
          filial_id: filialOperacionalId,
          tipo_roteirizacao: filtros.tipo_roteirizacao,
          filtros_aplicados: filtrosAplicadosCarteira,
          configuracao_frota: frotaParaPayload,
        },
        user.id,
        carteiraFiltrada,
        frotaParaPayload,
      )

      toast.success(`Roteirização concluída! ${resultado.manifestos.length} manifesto(s) gerado(s)`)
      navigate(`/historico?rodada=${resultado.rodada.id}`, { replace: true })
    } catch (err) {
      toast.error(err instanceof Error ? err.message : 'Erro desconhecido')
      setEtapa('filtros')
    } finally {
      setProcessando(false)
      setProgressoMsg('')
    }
  }

  const reiniciar = () => {
    limpar()
    setFiltrosCarteira(FILTROS_CARTEIRA_INICIAIS)
    setFiltrosAplicadosCarteira(FILTROS_CARTEIRA_INICIAIS)
    setQuantidadePorPerfil({})
    setPerfisFrota([])
    setCarteiraFiltrada([])
    setPreviewRows([])
    setTotalValidas(0)
    setEtapa('upload')
    setRodada(null)
    setManifestos([])
  }

  const excluirUpload = useCallback(async (uploadItem: UploadHistoricoItem) => {
    const confirmado = window.confirm('Tem certeza que deseja excluir este upload/rodada?')
    if (!confirmado) return

    try {
      setDeletingUploadId(uploadItem.id)
      await carteiraUploadService.excluirUpload(uploadItem.id)
      toast.success('Upload excluído com sucesso.')

      if (upload.uploadId === uploadItem.id) {
        reiniciar()
      }

      await carregarHistorico()
    } catch (error) {
      toast.error(getErrorMessage(error, 'Não foi possível excluir o upload selecionado.'))
    } finally {
      setDeletingUploadId(null)
    }
  }, [carregarHistorico, reiniciar, upload.uploadId])

  const estadoAuthBloqueante = (profileLoading || filialLoading) && !profileError && !filialError && !filialOperacional

  if (estadoAuthBloqueante) {
    return <div className="fade-in max-w-2xl mx-auto"><div className="card p-8 text-center"><Loader2 className="mx-auto mb-3 animate-spin text-brand-600" /><h2 className="font-semibold text-gray-900">Carregando perfil...</h2></div></div>
  }

  if (profileError) {
    return <div className="fade-in max-w-2xl mx-auto"><div className="card p-6 border border-red-200 bg-red-50"><button className="btn-primary" onClick={() => void reloadAuthContext()}><RotateCcw size={14} /> Tentar novamente</button><p className="text-sm text-red-700 mt-1">{profileError}</p></div></div>
  }

  if (etapa === 'upload') {
    return (
      <div className="fade-in max-w-3xl mx-auto">
        <div className="text-center mb-8">
          <h1 className="text-2xl font-bold text-gray-900">Nova Roteirização</h1>
          <p className="text-gray-500 mt-1">Filial: <strong>{filialOperacional?.nome || '—'}</strong></p>
        </div>

        {isMaster && (
          <div className="card p-4 mb-4 space-y-2">
            <label className="label">Filial operacional *</label>
            <select className="input" value={filialSelecionadaMaster} onChange={(e) => setFilialSelecionadaMaster(e.target.value)} disabled={filiaisMasterLoading}>
              <option value="">Selecione uma filial...</option>
              {filiaisMaster.map((f) => <option key={f.id} value={f.id}>{f.nome}</option>)}
            </select>
            {filiaisMasterError && <p className="text-sm text-red-600">{filiaisMasterError}</p>}
          </div>
        )}

        <div className="border-2 border-dashed border-brand-300 rounded-2xl p-12 text-center cursor-pointer hover:border-brand-500 hover:bg-brand-50 transition-all" onDrop={onDrop} onDragOver={(e) => e.preventDefault()} onClick={() => fileInputRef.current?.click()}>
          <Upload size={48} className="mx-auto text-brand-400 mb-4" />
          <h3 className="text-lg font-semibold text-gray-700 mb-2">Arraste a carteira de cargas aqui</h3>
          <p className="text-gray-500 text-sm mb-4">ou clique para selecionar o arquivo</p>
          <span className="badge-gray">XLSX · XLS · CSV</span>
          <input ref={fileInputRef} type="file" accept=".xlsx,.xls,.csv" className="hidden" onChange={(e) => e.target.files?.[0] && handleArquivo(e.target.files[0])} />
        </div>

        <UploadHistoryList
          uploads={historicoUploads}
          selectedUploadId={upload.uploadId}
          loading={historicoLoading}
          deletingUploadId={deletingUploadId}
          onSelect={(id) => void carregarUploadById(id)}
          onDelete={(item) => void excluirUpload(item)}
        />

        {upload.carregando && <div className="flex items-center justify-center gap-2 mt-6 text-brand-600"><Loader2 size={20} className="animate-spin" /><span>Processando arquivo...</span></div>}
        {upload.erro && <div className="mt-4 bg-red-50 border border-red-200 rounded-xl p-4 flex gap-3"><XCircle size={18} className="text-red-500" /><p className="text-red-700 text-sm">{upload.erro}</p></div>}
      </div>
    )
  }

  if (etapa === 'preview') {
    return (
      <div className="fade-in space-y-5">
        <div className="flex items-center justify-between">
          <div>
            <h1>Preview da Carteira</h1>
            <p className="text-gray-500 text-sm">Resumo do processamento e amostra real da carteira</p>
          </div>
          <button className="btn-ghost" onClick={reiniciar}><RotateCcw size={16} /> Novo Upload</button>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <div className="card p-4 text-center"><FileSpreadsheet size={24} className="mx-auto text-brand-600 mb-2" /><div className="text-2xl font-bold">{upload.totalLinhas}</div><div className="text-sm text-gray-500">Linhas válidas</div></div>
          <div className="card p-4 text-center"><Package size={24} className="mx-auto text-brand-600 mb-2" /><div className="text-2xl font-bold">{upload.totalColunas}</div><div className="text-sm text-gray-500">Colunas detectadas</div></div>
          <div className="card p-4 text-center"><CheckCircle size={24} className="mx-auto text-green-600 mb-2" /><div className="text-sm font-bold truncate">{upload.nomeArquivo || upload.arquivo?.name}</div><div className="text-sm text-gray-500">Arquivo</div></div>
        </div>

        <CarteiraPreviewTable
          rows={previewRows}
          columns={colunasCarteira}
          title="Primeiras linhas reais da carteira"
          total={upload.totalLinhas}
          maxHeightClassName="max-h-[360px]"
        />

        <div className="flex justify-end gap-3"><button className="btn-secondary" onClick={reiniciar}>Cancelar</button><button className="btn-primary" onClick={confirmarPreview}><CheckCircle size={16} /> Ir para Roteirização</button></div>
      </div>
    )
  }

  if (etapa === 'filtros') {
    return (
      <div className="fade-in">
        <div className="flex items-center justify-between mb-6">
          <div>
            <h1>Configurar Roteirização</h1>
            <p className="text-gray-500 text-sm">{carteiraFiltrada.length} cargas após filtros · {filialOperacional?.nome || 'Filial não selecionada'}</p>
          </div>
          <button className="btn-ghost" onClick={() => setEtapa('preview')}>Voltar</button>
        </div>

        <div className="space-y-6">
          <div className="card p-6 space-y-5">
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
              <div>
                <label className="label">Filial operacional *</label>
                {isMaster ? (
                  <select className="input" value={filialSelecionadaMaster} onChange={(e) => setFilialSelecionadaMaster(e.target.value)}>
                    <option value="">Selecione a filial...</option>
                    {filiaisMaster.map((f) => <option key={f.id} value={f.id}>{f.nome}</option>)}
                  </select>
                ) : (
                  <input className="input" value={filialOperacional?.nome || ''} readOnly />
                )}
              </div>
              <div>
                <label className="label">Data Base da Roteirização *</label>
                <input type="datetime-local" className="input" value={filtros.data_base} onChange={(e) => setFiltros({ ...filtros, data_base: e.target.value })} />
              </div>
              <div>
                <label className="label">Tipo de Roteirização *</label>
                <div className="grid grid-cols-1 sm:grid-cols-2 gap-3 mt-2">
                  {TIPOS_ROTEIRIZACAO.map((tipo) => (
                    <label key={tipo.value} className={`flex items-start gap-3 p-4 rounded-xl border-2 min-h-[96px] cursor-pointer transition-all ${filtros.tipo_roteirizacao === tipo.value ? 'border-brand-500 bg-brand-50' : 'border-gray-200 hover:border-gray-300'}`}>
                      <input type="radio" name="tipo_roteirizacao" value={tipo.value} checked={filtros.tipo_roteirizacao === tipo.value} onChange={() => setFiltros({ ...filtros, tipo_roteirizacao: tipo.value })} className="mt-0.5 shrink-0" />
                      <div className="min-w-0">
                        <div className="font-semibold leading-5">{tipo.label}</div>
                        <div className="text-sm text-gray-500 leading-5 mt-1">{tipo.desc}</div>
                      </div>
                    </label>
                  ))}
                </div>
              </div>
              <div className="hidden md:block" aria-hidden />
            </div>
          </div>

          <CarteiraFiltersPanel filtros={filtrosCarteira} opcoesFiltro={opcoesFiltro} expanded={filtrosExpandidos} onToggleExpanded={() => setFiltrosExpandidos((p) => !p)} onChange={setFiltrosCarteira} onClear={limparFiltros} onApply={aplicarFiltros} />

          {filtros.tipo_roteirizacao === 'frota' && (
            <div className="card p-6 space-y-3">
              <h3 className="font-semibold text-gray-900">Configuração de Frota por Perfil</h3>
              {frotaLoading && <p className="text-sm text-gray-500">Carregando perfis...</p>}
              {!frotaLoading && perfisFrota.length === 0 && <p className="text-sm text-amber-700">Nenhum perfil de veículo ativo encontrado para a filial.</p>}
              {!frotaLoading && perfisFrota.length > 0 && configuracaoFrota.length === 0 && (
                <p className="text-sm text-amber-700">Informe quantidades válidas (&gt; 0) para pelo menos um perfil antes de roteirizar.</p>
              )}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
                {perfisFrota.map((perfil) => (
                  <div key={perfil} className="flex items-center justify-between border rounded-lg p-3"><span className="font-medium">{perfil}</span><input type="number" min={0} className="input w-28" value={quantidadePorPerfil[perfil] ?? 0} onChange={(e) => setQuantidadePorPerfil((prev) => ({ ...prev, [perfil]: Number(e.target.value || 0) }))} /></div>
                ))}
              </div>
            </div>
          )}

          {resumoErro && <div className="card p-4 text-sm text-red-600">{resumoErro}</div>}
          {resumoLoading && <div className="card p-4 text-sm text-gray-500">Atualizando resumo...</div>}
          <RealtimeSummaryCard totalValidas={totalValidas} totalFiltradas={carteiraFiltrada.length} arquivo={upload.nomeArquivo || upload.arquivo?.name || ''} totalColunas={totalColunasResumo} filial={filialOperacional?.nome || ''} tipo={filtros.tipo_roteirizacao} />

          <CarteiraPreviewTable rows={carteiraFiltrada} columns={colunasCarteira} title="Carteira filtrada" total={carteiraFiltrada.length} maxHeightClassName="max-h-[65vh]" />
        </div>

        <div className="flex justify-end gap-3 mt-6">
          <button className="btn-secondary" onClick={() => setEtapa('preview')}>Voltar</button>
          <button className="btn-primary text-base px-8 py-3" onClick={roteirizar} disabled={!filialOperacionalId || processando}><Play size={18} /> Roteirizar Agora</button>
        </div>
      </div>
    )
  }

  if (etapa === 'processando') {
    return <div className="fade-in flex flex-col items-center justify-center min-h-[60vh]"><div className="text-center"><div className="relative w-24 h-24 mx-auto mb-6"><div className="absolute inset-0 rounded-full border-4 border-brand-100" /><div className="absolute inset-0 rounded-full border-4 border-brand-600 border-t-transparent animate-spin" /><Truck size={32} className="absolute inset-0 m-auto text-brand-600" /></div><h2 className="text-xl font-bold text-gray-900 mb-2">Processando Roteirização</h2><p className="text-gray-500 mb-4">{progressoMsg || 'Aguarde enquanto o Motor otimiza as rotas...'}</p><div className="flex items-center justify-center gap-2 text-sm text-gray-400"><Clock size={14} /><span>Isso pode levar até 3 minutos para carteiras grandes</span></div></div></div>
  }

  if (etapa === 'resultado' && rodada) {
    const manifestosAtivos = manifestos.filter((m) => !m.excluido)
    const totalFreteMinimo = manifestosAtivos.reduce((sum, m) => sum + (m.frete_minimo_antt || 0), 0)
    return (
      <div className="fade-in">
        <div className="flex items-center justify-between mb-6">
          <div><div className="flex items-center gap-2 mb-1"><CheckCircle size={20} className="text-green-600" /><h1>Roteirização Concluída</h1></div><p className="text-gray-500 text-sm">{filialOperacional?.nome} · {new Date(rodada.created_at).toLocaleString('pt-BR')} · {(rodada.tempo_processamento_ms / 1000).toFixed(1)}s</p></div>
          <button className="btn-ghost" onClick={reiniciar}><RotateCcw size={16} /> Nova Roteirização</button>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">{[{ label: 'Manifestos', value: manifestosAtivos.length, icon: Package, color: 'text-brand-600' }, { label: 'Itens Roteirizados', value: rodada.total_itens_manifestados, icon: CheckCircle, color: 'text-green-600' }, { label: 'Não Roteirizados', value: rodada.total_nao_roteirizados, icon: AlertTriangle, color: 'text-amber-600' }, { label: 'KM Total', value: `${rodada.km_total_frota?.toLocaleString('pt-BR')} km`, icon: MapPin, color: 'text-blue-600' }, { label: 'Frete Mínimo Total', value: `R$ ${totalFreteMinimo.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`, icon: Truck, color: 'text-purple-600' }].map(({ label, value, icon: Icon, color }) => <div key={label} className="card p-4"><Icon size={18} className={`${color} mb-2`} /><div className="text-xl font-bold text-gray-900">{value}</div><div className="text-xs text-gray-500">{label}</div></div>)}</div>
        <div className="flex gap-1 mb-4 bg-gray-100 rounded-xl p-1 w-fit">{[{ key: 'manifestos', label: `Manifestos (${manifestosAtivos.length})` }, { key: 'encadeamento', label: 'Encadeamento' }, { key: 'nao_roteirizados', label: `Não Roteirizados (${rodada.total_nao_roteirizados})` }].map(({ key, label }) => <button key={key} className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${abaAtiva === key ? 'bg-white shadow text-brand-700' : 'text-gray-600 hover:text-gray-900'}`} onClick={() => setAbaAtiva(key as typeof abaAtiva)}>{label}</button>)}</div>
        {abaAtiva === 'manifestos' && <div className="space-y-4">{manifestosAtivos.length === 0 ? <div className="card p-12 text-center text-gray-400"><Package size={40} className="mx-auto mb-3 opacity-30" /><p>Todos os manifestos foram removidos</p></div> : manifestosAtivos.map((manifesto) => <ManifestoCard key={manifesto.id_manifesto} manifesto={manifesto} onAprovar={() => setManifestos((prev) => prev.map((m) => m.id_manifesto === manifesto.id_manifesto ? { ...m, aprovado: !m.aprovado } : m))} onExcluir={() => setManifestos((prev) => prev.filter((m) => m.id_manifesto !== manifesto.id_manifesto))} />)}</div>}
        {abaAtiva === 'encadeamento' && rodada.resposta_motor && <EncadeamentoPanel encadeamento={rodada.resposta_motor.encadeamento ?? []} />}
        {abaAtiva === 'nao_roteirizados' && rodada.resposta_motor && <NaoRoteirizadosPanel resposta={rodada.resposta_motor} />}
      </div>
    )
  }

  return null
}
