import { CarteiraCarga, TipoRoteirizacao } from '@/types'

interface RealtimeSummaryCardProps {
  totalValidas: number
  totalFiltradas: number
  arquivo: string
  totalColunas: number
  filial: string
  tipo: TipoRoteirizacao
  previewRows: CarteiraCarga[]
}

const SUMMARY_COLUMNS = [
  { key: 'filial_r', label: 'Filial R' },
  { key: 'romane', label: 'Romane' },
  { key: 'nro_doc', label: 'Nro Doc' },
  { key: 'destin', label: 'Destino' },
  { key: 'cidade', label: 'Cidade' },
  { key: 'uf', label: 'UF' },
]

export function RealtimeSummaryCard({
  totalValidas,
  totalFiltradas,
  arquivo,
  totalColunas,
  filial,
  tipo,
  previewRows,
}: RealtimeSummaryCardProps) {
  return (
    <div className="card p-5 space-y-4 h-fit">
      <h3 className="font-semibold text-gray-900">Resumo em tempo real</h3>
      <div className="text-sm text-gray-600 space-y-1.5">
        <p><strong>Total linhas válidas:</strong> {totalValidas}</p>
        <p><strong>Total após filtros:</strong> {totalFiltradas}</p>
        <p><strong>Arquivo:</strong> {arquivo || '—'}</p>
        <p><strong>Quantidade de colunas:</strong> {totalColunas}</p>
        <p><strong>Filial operacional:</strong> {filial || '—'}</p>
        <p><strong>Tipo selecionado:</strong> {tipo}</p>
      </div>

      <div>
        <h4 className="text-sm font-semibold text-gray-900 mb-2">Amostra filtrada</h4>
        <div className="border rounded-lg overflow-auto max-h-72">
          <table className="table text-xs min-w-[520px]">
            <thead>
              <tr>
                {SUMMARY_COLUMNS.map((column) => <th key={column.key}>{column.label}</th>)}
              </tr>
            </thead>
            <tbody>
              {previewRows.map((row, idx) => (
                <tr key={`${String(row._carteira_item_id ?? idx)}-${idx}`}>
                  {SUMMARY_COLUMNS.map((column) => (
                    <td key={column.key}>{String(row[column.key] ?? '—')}</td>
                  ))}
                </tr>
              ))}
              {!previewRows.length && (
                <tr>
                  <td className="text-gray-400" colSpan={SUMMARY_COLUMNS.length}>Sem registros após filtros.</td>
                </tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
