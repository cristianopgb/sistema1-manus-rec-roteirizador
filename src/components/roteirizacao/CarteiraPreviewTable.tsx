import { format } from 'date-fns'
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

const SERIAL_MIN_EXCEL_DATE = 25569

const normalizarNomeColuna = (coluna: string): string => coluna
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .toLowerCase()
  .replace(/[^a-z0-9]/g, '')

const excelSerialToDate = (serial: number): Date => {
  const utcDays = Math.floor(serial - SERIAL_MIN_EXCEL_DATE)
  const utcValue = utcDays * 86400
  const dateInfo = new Date(utcValue * 1000)

  const fractionalDay = serial - Math.floor(serial)
  const totalSeconds = Math.round(86400 * fractionalDay)
  const seconds = totalSeconds % 60
  const minutes = Math.floor(totalSeconds / 60) % 60
  const hours = Math.floor(totalSeconds / 3600)

  dateInfo.setUTCHours(hours, minutes, seconds)
  return dateInfo
}

const parseNumero = (valor: unknown): number | null => {
  if (typeof valor === 'number' && Number.isFinite(valor)) return valor
  if (typeof valor !== 'string') return null
  const limpo = valor.trim().replace(/\s/g, '')
  if (!limpo) return null
  const normalizado = limpo.includes(',') && limpo.includes('.')
    ? limpo.replace(/\./g, '').replace(',', '.')
    : limpo.replace(',', '.')
  const numero = Number(normalizado)
  return Number.isFinite(numero) ? numero : null
}

const isColunaDocumentoOuCodigo = (colunaNorm: string): boolean => {
  return ['documento', 'doc', 'romane', 'filial', 'serie', 'codigo', 'id', 'nro', 'nfserie'].some((token) => colunaNorm.includes(token))
}

const formatarValorPreviewCarteira = (colunaOriginal: string, valor: unknown): string => {
  if (valor === null || valor === undefined || String(valor).trim() === '') return '-'

  const colunaNorm = normalizarNomeColuna(colunaOriginal)
  const numero = parseNumero(valor)

  const isData = colunaNorm.includes('data') || colunaNorm.includes('dle') || colunaNorm.includes('agendam')
  if (isData) {
    if (numero !== null && numero > SERIAL_MIN_EXCEL_DATE) {
      const dataExcel = excelSerialToDate(numero)
      if (!Number.isNaN(dataExcel.getTime())) {
        const temHora = Math.abs(numero - Math.trunc(numero)) > 0.000001
        return format(dataExcel, temHora ? 'dd/MM/yyyy HH:mm' : 'dd/MM/yyyy')
      }
    }

    if (typeof valor === 'string') {
      const dataIso = new Date(valor)
      if (!Number.isNaN(dataIso.getTime())) {
        const temHora = /\d{2}:\d{2}/.test(valor)
        return format(dataIso, temHora ? 'dd/MM/yyyy HH:mm' : 'dd/MM/yyyy')
      }
    }
  }

  if (numero !== null && !isColunaDocumentoOuCodigo(colunaNorm)) {
    if (colunaNorm.includes('latitude') || colunaNorm.includes('longitude')) {
      return numero.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 6 })
    }

    const colunaPeso = colunaNorm.includes('peso')
    if (colunaPeso) {
      return numero.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 1 })
    }

    const colunaQuantidade = colunaNorm.includes('qtd') || colunaNorm.includes('volumes')
    if (colunaQuantidade) {
      return Math.trunc(numero).toLocaleString('pt-BR')
    }

    const colunaMonetaria = colunaNorm.includes('vlrmerc') || colunaNorm.includes('valor') || colunaNorm.includes('frete')
    if (colunaMonetaria) {
      return numero.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL', minimumFractionDigits: 2, maximumFractionDigits: 2 })
    }

    if (!Number.isInteger(numero)) {
      return numero.toLocaleString('pt-BR', { minimumFractionDigits: 0, maximumFractionDigits: 1 })
    }

    return numero.toLocaleString('pt-BR')
  }

  return String(valor).trim() || '-'
}

export function CarteiraPreviewTable({
  rows,
  columns,
  title,
  total,
  maxHeightClassName = 'max-h-[360px]',
}: CarteiraPreviewTableProps) {
  const minWidthPx = Math.max(columns.length * 160, 1600)

  return (
    <div className="card">
      <div className="card-header">
        <h3>{title || 'Preview da carteira'}</h3>
        {typeof total === 'number' && <span className="text-sm text-gray-500">Total: {total}</span>}
      </div>
      <div className={`max-h-[360px] overflow-y-auto overflow-x-auto rounded-lg border border-gray-100 ${maxHeightClassName}`}>
        <table className="text-[11px] min-w-[1600px]" style={{ minWidth: `${minWidthPx}px` }}>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.key} className="whitespace-nowrap sticky top-0 z-10 bg-white px-2 py-1.5 text-left font-semibold text-gray-700 border-b border-gray-200">
                  {column.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((row, idx) => (
              <tr key={`${String(row._carteira_item_id ?? idx)}-${idx}`} className="border-b border-gray-100">
                {columns.map((column) => {
                  const valorFormatado = formatarValorPreviewCarteira(column.label || column.key, row[column.key])
                  return (
                    <td key={column.key} className="whitespace-nowrap px-2 py-1.5 max-w-[180px] truncate" title={valorFormatado}>
                      {valorFormatado}
                    </td>
                  )
                })}
              </tr>
            ))}
            {!rows.length && (
              <tr>
                <td className="text-gray-400 px-2 py-1.5" colSpan={columns.length}>Sem registros para exibir.</td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
