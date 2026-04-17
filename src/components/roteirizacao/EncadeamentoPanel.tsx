import { ArrowRight, CheckCircle, AlertCircle } from 'lucide-react'
import { EtapaPipeline } from '@/types'

interface Props {
  encadeamento: EtapaPipeline[]
}

export function EncadeamentoPanel({ encadeamento }: Props) {
  if (!encadeamento || encadeamento.length === 0) {
    return (
      <div className="card p-8 text-center text-gray-400">
        Dados de encadeamento não disponíveis
      </div>
    )
  }

  return (
    <div className="card">
      <div className="card-header">
        <h3>Encadeamento do Pipeline</h3>
        <span className="text-sm text-gray-500">{encadeamento.length} etapas executadas</span>
      </div>
      <div className="table-container">
        <table className="table">
          <thead>
            <tr>
              <th>Módulo</th>
              <th>Etapa</th>
              <th className="text-right">Entrada</th>
              <th className="text-right">Saída</th>
              <th className="text-right">Remanescente</th>
              <th>Status</th>
              <th>Detalhes</th>
            </tr>
          </thead>
          <tbody>
            {encadeamento.map((etapa, idx) => (
              <tr key={idx}>
                <td>
                  <span className="badge-blue font-mono text-xs">{etapa.etapa.split('—')[0]?.trim()}</span>
                </td>
                <td className="font-medium">{etapa.etapa}</td>
                <td className="text-right font-mono">{etapa.entrada?.toLocaleString('pt-BR')}</td>
                <td className="text-right font-mono font-semibold text-brand-700">
                  {etapa.saida_principal?.toLocaleString('pt-BR')}
                </td>
                <td className="text-right font-mono text-gray-500">
                  {etapa.remanescente?.toLocaleString('pt-BR')}
                </td>
                <td>
                  <span className="flex items-center gap-1 text-green-600 text-xs">
                      <CheckCircle size={12} /> OK
                    </span>
                </td>
                <td className="text-xs text-gray-500 max-w-[200px] truncate">
                  {etapa.detalhes ? JSON.stringify(etapa.detalhes).slice(0, 80) : '—'}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
