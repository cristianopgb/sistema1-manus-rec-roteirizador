import { CarteiraCarga } from '@/types'

interface ColumnDef {
  key: string
  label: string
}

interface CarteiraPreviewTableProps {
  rows: CarteiraCarga[]
  columns: ColumnDef[]
  title?: string
  total?: number
}

const formatValue = (value: unknown) => {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}

export function CarteiraPreviewTable({ rows, columns, title, total }: CarteiraPreviewTableProps) {
  return (
    <div className="card">
      <div className="card-header">
        <h3>{title || 'Preview da carteira'}</h3>
        {typeof total === 'number' && <span className="text-sm text-gray-500">Total: {total}</span>}
      </div>
      <div className="overflow-auto">
        <table className="table text-xs min-w-[1200px]">
          <thead>
            <tr>
              {columns.map((column) => <th key={column.key} className="whitespace-nowrap">{column.label}</th>)}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={`${String(row._carteira_item_id ?? idx)}-${idx}`}>
                {columns.map((column) => (
                  <td key={column.key} className="whitespace-nowrap">{formatValue(row[column.key])}</td>
                ))}
              </tr>
            ))}
            {!rows.length && (
              <tr>
                <td className="text-gray-400" colSpan={columns.length}>Sem registros para exibir.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
