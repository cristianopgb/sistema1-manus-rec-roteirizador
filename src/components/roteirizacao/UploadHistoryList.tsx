import { CheckCircle2, AlertCircle, Loader2, Trash2 } from 'lucide-react'

export interface UploadHistoricoItem {
  id: string
  nomeArquivo: string
  createdAt: string
  totalValidas: number
  totalImportadas: number
  status: string
}

interface UploadHistoryListProps {
  uploads: UploadHistoricoItem[]
  selectedUploadId?: string | null
  loading?: boolean
  onSelect: (uploadId: string) => void
  deletingUploadId?: string | null
  onDelete: (upload: UploadHistoricoItem) => void
}

const statusInfo = (status: string) => {
  if (status === 'erro') return { label: 'Erro', icon: AlertCircle, cls: 'text-red-600 bg-red-50' }
  if (status === 'processando') return { label: 'Processando', icon: Loader2, cls: 'text-amber-700 bg-amber-50' }
  return { label: 'Concluído', icon: CheckCircle2, cls: 'text-green-700 bg-green-50' }
}

export function UploadHistoryList({
  uploads,
  selectedUploadId,
  loading,
  onSelect,
  deletingUploadId,
  onDelete,
}: UploadHistoryListProps) {
  return (
    <div className="card p-4 mt-6">
      <h3 className="font-semibold text-gray-900 mb-3">Uploads Recentes</h3>
      {loading && <p className="text-sm text-gray-500">Carregando histórico...</p>}
      {!loading && uploads.length === 0 && <p className="text-sm text-gray-500">Nenhum upload recente encontrado.</p>}
      <div className="space-y-1.5 max-h-[500px] overflow-y-auto pr-1">
        {uploads.map((upload) => {
          const info = statusInfo(upload.status)
          const Icon = info.icon
          const deletando = deletingUploadId === upload.id
          return (
            <div
              key={upload.id}
              className={`w-full text-left border rounded-xl px-3 py-2.5 hover:border-brand-300 transition-colors ${selectedUploadId === upload.id ? 'border-brand-500 bg-brand-50' : 'border-gray-200'}`}
            >
              <div className="flex items-center justify-between gap-2">
                <button type="button" onClick={() => onSelect(upload.id)} className="min-w-0 flex-1 text-left">
                  <p className="font-medium text-sm text-gray-900 truncate">{upload.nomeArquivo}</p>
                  <p className="text-xs text-gray-500">{new Date(upload.createdAt).toLocaleString('pt-BR')}</p>
                  <p className="text-[11px] text-gray-600 mt-0.5">{upload.totalValidas} válidas / {upload.totalImportadas} total</p>
                </button>
                <button
                  type="button"
                  onClick={() => onDelete(upload)}
                  disabled={deletando}
                  title="Excluir upload"
                  className="p-1.5 rounded-md text-gray-500 hover:text-red-600 hover:bg-red-50 disabled:opacity-60 disabled:cursor-not-allowed"
                >
                  {deletando ? <Loader2 size={14} className="animate-spin" /> : <Trash2 size={14} />}
                </button>
                <span className={`text-xs px-2 py-1 rounded-full inline-flex items-center gap-1 ${info.cls}`}>
                  <Icon size={12} className={upload.status === 'processando' ? 'animate-spin' : ''} />
                  {info.label}
                </span>
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}
