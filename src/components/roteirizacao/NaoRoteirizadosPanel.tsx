import { useState } from 'react'
import { AlertTriangle, Clock, Calendar, XCircle, HelpCircle } from 'lucide-react'
import { RespostaMotor } from '@/types'
import { formatDateBR } from '@/lib/date-normalizers'

interface Props {
  resposta: RespostaMotor
}

const CATEGORIAS = [
  { key: 'nao_roteirizados', label: 'Não Roteirizados', icon: XCircle, color: 'text-red-600', bg: 'bg-red-50' },
  { key: 'cargas_agendamento_futuro', label: 'Agendamento Futuro', icon: Clock, color: 'text-blue-600', bg: 'bg-blue-50' },
  { key: 'cargas_agenda_vencida', label: 'Agenda Vencida', icon: Calendar, color: 'text-amber-600', bg: 'bg-amber-50' },
  { key: 'cargas_excecao_triagem', label: 'Exceção de Triagem', icon: HelpCircle, color: 'text-purple-600', bg: 'bg-purple-50' },
  { key: 'cargas_nao_alocadas', label: 'Não Alocadas', icon: AlertTriangle, color: 'text-orange-600', bg: 'bg-orange-50' },
] as const

export function NaoRoteirizadosPanel({ resposta }: Props) {
  const [categoriaAtiva, setCategoriaAtiva] = useState<string>('nao_roteirizados')

  const categorias = CATEGORIAS.filter((c) => {
    const dados = resposta[c.key as keyof RespostaMotor]
    return Array.isArray(dados) && dados.length > 0
  })

  if (categorias.length === 0) {
    return (
      <div className="card p-8 text-center text-gray-400">
        <AlertTriangle size={32} className="mx-auto mb-3 opacity-30" />
        <p>Nenhuma carga não roteirizada</p>
      </div>
    )
  }

  const dadosAtivos = (resposta[categoriaAtiva as keyof RespostaMotor] as unknown[]) as Record<string, unknown>[]
  const categoriaInfo = CATEGORIAS.find((c) => c.key === categoriaAtiva)

  return (
    <div className="space-y-4">
      {/* Tabs de categoria */}
      <div className="flex flex-wrap gap-2">
        {categorias.map(({ key, label, icon: Icon, color, bg }) => {
          const dados = resposta[key as keyof RespostaMotor] as unknown[]
          return (
            <button
              key={key}
              onClick={() => setCategoriaAtiva(key)}
              className={`flex items-center gap-2 px-4 py-2 rounded-xl text-sm font-medium transition-all border-2 ${
                categoriaAtiva === key
                  ? `${bg} border-current ${color}`
                  : 'bg-white border-gray-200 text-gray-600 hover:border-gray-300'
              }`}
            >
              <Icon size={14} />
              {label}
              <span className="font-bold">{dados?.length || 0}</span>
            </button>
          )
        })}
      </div>

      {/* Tabela de dados */}
      {dadosAtivos && dadosAtivos.length > 0 && (
        <div className="card">
          <div className={`card-header ${categoriaInfo?.bg}`}>
            <div className={`flex items-center gap-2 ${categoriaInfo?.color}`}>
              {categoriaInfo && <categoriaInfo.icon size={16} />}
              <h3>{categoriaInfo?.label} — {dadosAtivos.length} carga(s)</h3>
            </div>
          </div>
          <div className="table-container">
            <table className="table text-sm">
              <thead>
                <tr>
                  <th>Documento</th>
                  <th>Destinatário</th>
                  <th>Cidade / UF</th>
                  <th>Peso (kg)</th>
                  <th>Motivo</th>
                  <th>Data Limite</th>
                </tr>
              </thead>
              <tbody>
                {dadosAtivos.slice(0, 100).map((carga, idx) => (
                  <tr key={idx}>
                    <td className="font-mono text-xs">{String(carga.nro_documento || carga.documento || '—')}</td>
                    <td className="max-w-[160px] truncate">{String(carga.destinatario || carga.nome_destinatario || '—')}</td>
                    <td className="whitespace-nowrap">
                      {String(carga.cidade || carga.cidade_dest || '—')} / {String(carga.uf || '—')}
                    </td>
                    <td className="text-right">
                      {typeof carga.peso_kg === 'number'
                        ? carga.peso_kg.toLocaleString('pt-BR', { minimumFractionDigits: 1 })
                        : '—'}
                    </td>
                    <td>
                      {carga.motivo_nao_roteirizavel || carga.status_triagem ? (
                        <span className="badge-red text-xs">
                          {String(carga.motivo_nao_roteirizavel || carga.status_triagem)}
                        </span>
                      ) : '—'}
                    </td>
                    <td className="text-xs text-gray-500">
                      {carga.data_limite_entrega
                        ? formatDateBR(carga.data_limite_entrega)
                        : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
            {dadosAtivos.length > 100 && (
              <div className="p-3 text-center text-sm text-gray-500 border-t">
                Mostrando 100 de {dadosAtivos.length} registros
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
