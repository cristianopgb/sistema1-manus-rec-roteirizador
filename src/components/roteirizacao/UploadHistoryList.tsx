import { CheckCircle2, AlertCircle, Loader2 } from 'lucide-react'

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
}

const statusInfo = (status: string) => {
  if (status === 'erro') return { label: 'Erro', icon: AlertCircle, cls: 'text-red-600 bg-red-50' }
  if (status === 'processando') return { label: 'Processando', icon: Loader2, cls: 'text-amber-700 bg-amber-50' }
  return { label: 'Concluído', icon: CheckCircle2, cls: 'text-green-700 bg-green-50' }
}

export function UploadHistoryList({ uploads, selectedUploadId, loading, onSelect }: UploadHistoryListProps) {
  return (
    <div className="card p-4 mt-6">
      <h3 className="font-semibold text-gray-900 mb-3">Uploads Recentes</h3>
      {loading && <p className="text-sm text-gray-500">Carregando histórico...</p>}
      {!loading && uploads.length === 0 && <p className="text-sm text-gray-500">Nenhum upload recente encontrado.</p>}
      <div className="space-y-2">
        {uploads.map((upload) => {
          const info = statusInfo(upload.status)
          const Icon = info.icon
          return (
            <button
              key={upload.id}
              type="button"
              onClick={() => onSelect(upload.id)}
              className={`w-full text-left border rounded-xl p-3 hover:border-brand-300 transition-colors ${selectedUploadId === upload.id ? 'border-brand-500 bg-brand-50' : 'border-gray-200'}`}
            >
              <div className="flex items-start justify-between gap-2">
                <div>
                  <p className="font-medium text-gray-900 truncate">{upload.nomeArquivo}</p>
                  <p className="text-xs text-gray-500">{new Date(upload.createdAt).toLocaleString('pt-BR')}</p>
                  <p className="text-xs text-gray-600 mt-1">{upload.totalValidas} válidas / {upload.totalImportadas} total</p>
                </div>
                <span className={`text-xs px-2 py-1 rounded-full inline-flex items-center gap-1 ${info.cls}`}>
                  <Icon size={12} className={upload.status === 'processando' ? 'animate-spin' : ''} />
                  {info.label}
                </span>
              </div>
            </button>
          )
        })}
      </div>
    </div>
  )
}
