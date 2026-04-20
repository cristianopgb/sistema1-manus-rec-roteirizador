import { TipoRoteirizacao } from '@/types'

interface RealtimeSummaryCardProps {
  totalValidas: number
  totalFiltradas: number
  arquivo: string
  totalColunas: number
  filial: string
  tipo: TipoRoteirizacao
}

export function RealtimeSummaryCard({
  totalValidas,
  totalFiltradas,
  arquivo,
  totalColunas,
  filial,
  tipo,
}: RealtimeSummaryCardProps) {
  const items = [
    { label: 'Total linhas válidas', value: totalValidas.toLocaleString('pt-BR') },
    { label: 'Total após filtros', value: totalFiltradas.toLocaleString('pt-BR') },
    { label: 'Arquivo', value: arquivo || '—' },
    { label: 'Quantidade de colunas', value: totalColunas.toLocaleString('pt-BR') },
    { label: 'Filial operacional', value: filial || '—' },
    { label: 'Tipo selecionado', value: tipo },
  ]

  return (
    <div className="card p-4">
      <h3 className="font-semibold text-gray-900 mb-3">Resumo em tempo real</h3>
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
        {items.map((item) => (
          <div key={item.label} className="rounded-lg border border-gray-200 bg-gray-50 px-3 py-2">
            <p className="text-xs uppercase tracking-wide text-gray-500">{item.label}</p>
            <p className="text-sm font-semibold text-gray-900 truncate" title={item.value}>{item.value}</p>
          </div>
        ))}
      </div>
    </div>
  )
}
