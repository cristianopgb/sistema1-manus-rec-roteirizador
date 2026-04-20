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
  maxHeightClassName?: string
}

const formatValue = (value: unknown) => {
  if (value === null || value === undefined || value === '') return '—'
  return String(value)
}

export function CarteiraPreviewTable({
  rows,
  columns,
  title,
  total,
  maxHeightClassName = 'max-h-[70vh]',
}: CarteiraPreviewTableProps) {
  const minWidthPx = Math.max(columns.length * 150, 1200)

  return (
    <div className="card">
      <div className="card-header">
        <h3>{title || 'Preview da carteira'}</h3>
        {typeof total === 'number' && <span className="text-sm text-gray-500">Total: {total}</span>}
      </div>
      <div className={`overflow-x-auto overflow-y-auto border-t border-gray-100 ${maxHeightClassName}`}>
        <table className="table text-xs" style={{ minWidth: `${minWidthPx}px` }}>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.key} className="whitespace-nowrap sticky top-0 z-10 bg-gray-50">
                  {column.label}
                </th>
              ))}
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
