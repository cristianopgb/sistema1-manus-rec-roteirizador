import { useState } from 'react'
import {
  ChevronDown, ChevronUp, Truck, MapPin, Package,
  CheckCircle, Trash2, Printer, GripVertical, Calendar,
  DollarSign, BarChart3
} from 'lucide-react'
import { ManifestoComFrete } from '@/types'
import { gerarPdfManifesto } from '@/services/pdf.service'
import toast from 'react-hot-toast'

interface Props {
  manifesto: ManifestoComFrete
  onAprovar: () => void
  onExcluir: () => void
}

export function ManifestoCard({ manifesto, onAprovar, onExcluir }: Props) {
  const [expandido, setExpandido] = useState(false)
  const [gerandoPdf, setGerandoPdf] = useState(false)
  const [entregas, setEntregas] = useState(manifesto.entregas)

  const ocupacaoColor =
    manifesto.ocupacao_percentual >= 90 ? 'text-green-600' :
    manifesto.ocupacao_percentual >= 70 ? 'text-amber-600' : 'text-red-600'

  const moverEntrega = (idx: number, direcao: 'up' | 'down') => {
    const novas = [...entregas]
    const troca = direcao === 'up' ? idx - 1 : idx + 1
    if (troca < 0 || troca >= novas.length) return
    ;[novas[idx], novas[troca]] = [novas[troca], novas[idx]]
    // Resequenciar
    novas.forEach((e, i) => { e.sequencia = i + 1 })
    setEntregas(novas)
  }

  const handlePdf = async () => {
    setGerandoPdf(true)
    try {
      await gerarPdfManifesto({ ...manifesto, entregas })
      toast.success('PDF gerado com sucesso')
    } catch {
      toast.error('Erro ao gerar PDF')
    } finally {
      setGerandoPdf(false)
    }
  }

  return (
    <div className={`card border-l-4 ${manifesto.aprovado ? 'border-l-green-500' : 'border-l-brand-500'}`}>
      {/* Cabeçalho do manifesto */}
      <div className="p-4">
        <div className="flex items-start justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 bg-brand-100 rounded-xl flex items-center justify-center flex-shrink-0">
              <Truck size={18} className="text-brand-700" />
            </div>
            <div>
              <div className="flex items-center gap-2">
                <h3 className="font-bold text-gray-900">{manifesto.numero_manifesto}</h3>
                <span className="badge-blue">{manifesto.veiculo_tipo}</span>
                {manifesto.aprovado && <span className="badge-green">Aprovado</span>}
              </div>
              <div className="flex items-center gap-3 text-sm text-gray-500 mt-0.5">
                <span className="flex items-center gap-1">
                  <MapPin size={12} /> {manifesto.regiao || 'Região não definida'}
                </span>
                <span className="flex items-center gap-1">
                  <Package size={12} /> {manifesto.total_entregas} entregas
                </span>
                <span className="flex items-center gap-1">
                  <MapPin size={12} /> {manifesto.km_estimado?.toLocaleString('pt-BR')} km
                </span>
              </div>
            </div>
          </div>

          {/* KPIs rápidos */}
          <div className="flex items-center gap-6 text-right">
            <div>
              <div className={`text-lg font-bold ${ocupacaoColor}`}>
                {manifesto.ocupacao_percentual?.toFixed(1)}%
              </div>
              <div className="text-xs text-gray-500">Ocupação</div>
            </div>
            <div>
              <div className="text-lg font-bold text-gray-900">
                {manifesto.total_peso_kg?.toLocaleString('pt-BR')} kg
              </div>
              <div className="text-xs text-gray-500">Peso total</div>
            </div>
            {manifesto.frete_minimo_antt != null && (
              <div>
                <div className="text-lg font-bold text-purple-700">
                  R$ {manifesto.frete_minimo_antt.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}
                </div>
                <div className="text-xs text-gray-500">Frete mínimo ANTT</div>
              </div>
            )}
          </div>
        </div>

        {/* Barra de ocupação */}
        <div className="mt-3">
          <div className="h-2 bg-gray-100 rounded-full overflow-hidden">
            <div
              className={`h-full rounded-full transition-all ${
                manifesto.ocupacao_percentual >= 90 ? 'bg-green-500' :
                manifesto.ocupacao_percentual >= 70 ? 'bg-amber-500' : 'bg-red-500'
              }`}
              style={{ width: `${Math.min(manifesto.ocupacao_percentual, 100)}%` }}
            />
          </div>
          <div className="flex justify-between text-xs text-gray-400 mt-1">
            <span>0 kg</span>
            <span>{manifesto.capacidade_peso_kg?.toLocaleString('pt-BR')} kg (capacidade)</span>
          </div>
        </div>

        {/* Ações */}
        <div className="flex items-center justify-between mt-4 pt-3 border-t">
          <button
            className="flex items-center gap-1.5 text-sm text-gray-500 hover:text-gray-700"
            onClick={() => setExpandido(!expandido)}
          >
            {expandido ? <ChevronUp size={16} /> : <ChevronDown size={16} />}
            {expandido ? 'Ocultar entregas' : `Ver ${entregas.length} entregas`}
          </button>

          <div className="flex items-center gap-2">
            <button
              className={`btn-sm ${manifesto.aprovado ? 'btn-secondary' : 'btn-success'}`}
              onClick={onAprovar}
            >
              <CheckCircle size={14} />
              {manifesto.aprovado ? 'Aprovado' : 'Aprovar'}
            </button>
            <button className="btn-sm btn-ghost" onClick={handlePdf} disabled={gerandoPdf}>
              <Printer size={14} />
              {gerandoPdf ? 'Gerando...' : 'PDF'}
            </button>
            <button className="btn-sm btn-ghost text-red-500 hover:bg-red-50" onClick={onExcluir}>
              <Trash2 size={14} />
            </button>
          </div>
        </div>
      </div>

      {/* Lista de entregas expandida */}
      {expandido && (
        <div className="border-t">
          <div className="table-container">
            <table className="table text-sm">
              <thead>
                <tr>
                  <th className="w-8">Seq.</th>
                  <th>Destinatário</th>
                  <th>Cidade / UF</th>
                  <th>Documentos</th>
                  <th>Peso (kg)</th>
                  <th>Valor</th>
                  <th>Data Limite</th>
                  <th>Agenda</th>
                  <th>Folga</th>
                  <th className="w-16">Ordem</th>
                </tr>
              </thead>
              <tbody>
                {entregas.map((entrega, idx) => (
                  <tr key={`${entrega.nro_documento}-${idx}`}>
                    <td>
                      <span className="w-6 h-6 rounded-full bg-brand-100 text-brand-700 text-xs font-bold flex items-center justify-center">
                        {entrega.sequencia}
                      </span>
                    </td>
                    <td className="font-medium max-w-[160px] truncate" title={entrega.destinatario}>
                      {entrega.destinatario || '—'}
                    </td>
                    <td className="whitespace-nowrap">
                      {entrega.cidade || '—'} / {entrega.uf || '—'}
                    </td>
                    <td className="font-mono text-xs text-gray-600">
                      {entrega.lista_nfs?.join(', ') || entrega.nro_documento || '—'}
                    </td>
                    <td className="text-right">
                      {entrega.peso_kg?.toLocaleString('pt-BR', { minimumFractionDigits: 1 })}
                    </td>
                    <td className="text-right text-gray-600">
                      {entrega.valor_mercadoria
                        ? `R$ ${entrega.valor_mercadoria.toLocaleString('pt-BR', { minimumFractionDigits: 2 })}`
                        : '—'}
                    </td>
                    <td className="whitespace-nowrap text-xs">
                      {entrega.data_limite_entrega
                        ? new Date(entrega.data_limite_entrega).toLocaleDateString('pt-BR')
                        : '—'}
                    </td>
                    <td>
                      {entrega.agendada ? (
                        <span className="badge-amber text-xs flex items-center gap-1">
                          <Calendar size={10} />
                          {entrega.hora_agenda || 'Agendado'}
                        </span>
                      ) : (
                        <span className="text-gray-400 text-xs">—</span>
                      )}
                    </td>
                    <td>
                      {entrega.folga_dias != null ? (
                        <span className={`text-xs font-medium ${
                          entrega.status_folga === 'urgente' ? 'text-red-600' :
                          entrega.status_folga === 'atencao' ? 'text-amber-600' : 'text-green-600'
                        }`}>
                          {entrega.folga_dias.toFixed(0)}d
                        </span>
                      ) : '—'}
                    </td>
                    <td>
                      <div className="flex flex-col gap-0.5">
                        <button
                          className="p-0.5 hover:bg-gray-100 rounded disabled:opacity-30"
                          onClick={() => moverEntrega(idx, 'up')}
                          disabled={idx === 0}
                        >
                          <ChevronUp size={12} />
                        </button>
                        <button
                          className="p-0.5 hover:bg-gray-100 rounded disabled:opacity-30"
                          onClick={() => moverEntrega(idx, 'down')}
                          disabled={idx === entregas.length - 1}
                        >
                          <ChevronDown size={12} />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}
