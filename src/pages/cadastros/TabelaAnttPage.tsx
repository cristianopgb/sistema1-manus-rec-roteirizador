import { useState, useEffect } from 'react'
import { FileSpreadsheet, Pencil, Loader2, Check, X, Info } from 'lucide-react'
import { anttService } from '@/services/antt.service'
import { TabelaAntt, TIPOS_CARGA_ANTT, EIXOS_VALIDOS } from '@/types'
import toast from 'react-hot-toast'

export function TabelaAnttPage() {
  const [tabela, setTabela] = useState<TabelaAntt[]>([])
  const [loading, setLoading] = useState(true)
  const [editando, setEditando] = useState<TabelaAntt | null>(null)
  const [formDeslocamento, setFormDeslocamento] = useState('')
  const [formCargaDescarga, setFormCargaDescarga] = useState('')
  const [salvando, setSalvando] = useState(false)

  const carregar = async () => {
    setLoading(true)
    try {
      const data = await anttService.listar()
      setTabela(data)
    } catch {
      toast.error('Erro ao carregar tabela ANTT')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { carregar() }, [])

  const abrirEditar = (item: TabelaAntt) => {
    setEditando(item)
    setFormDeslocamento(String(item.coeficiente_deslocamento))
    setFormCargaDescarga(String(item.coeficiente_carga_descarga))
  }

  const salvar = async () => {
    if (!editando) return
    setSalvando(true)
    try {
      await anttService.atualizar(editando.id, {
        coeficiente_deslocamento: parseFloat(formDeslocamento),
        coeficiente_carga_descarga: parseFloat(formCargaDescarga),
      })
      toast.success('Coeficiente atualizado')
      setEditando(null)
      carregar()
    } catch {
      toast.error('Erro ao atualizar coeficiente')
    } finally {
      setSalvando(false)
    }
  }

  // Agrupar por tipo de carga
  const tiposCarga = [...new Set(tabela.map((t) => t.tipo_carga_id))].sort((a, b) => a - b)

  return (
    <div className="fade-in">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-brand-100 rounded-xl flex items-center justify-center">
            <FileSpreadsheet size={20} className="text-brand-700" />
          </div>
          <div>
            <h1>Tabela ANTT — Frete Mínimo</h1>
            <p className="text-sm text-gray-500">Coeficientes de custo por tipo de carga e número de eixos</p>
          </div>
        </div>
      </div>

      {/* Info box */}
      <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-6 flex gap-3">
        <Info size={18} className="text-blue-600 flex-shrink-0 mt-0.5" />
        <div className="text-sm text-blue-700">
          <strong>Fórmula do Frete Mínimo:</strong> (KM estimado × Coeficiente de Deslocamento) + Coeficiente de Carga e Descarga.
          O frete é calculado automaticamente pelo Sistema 1 após o retorno do Motor, antes de exibir os manifestos.
        </div>
      </div>

      {loading ? (
        <div className="flex items-center justify-center py-16">
          <Loader2 size={24} className="animate-spin text-brand-600" />
        </div>
      ) : (
        <div className="space-y-6">
          {tiposCarga.map((tipoCargaId) => {
            const itens = tabela.filter((t) => t.tipo_carga_id === tipoCargaId)
            const nomeTipo = TIPOS_CARGA_ANTT[tipoCargaId] || `Tipo ${tipoCargaId}`

            return (
              <div key={tipoCargaId} className="card">
                <div className="card-header">
                  <div className="flex items-center gap-2">
                    <span className="badge-blue">{tipoCargaId}</span>
                    <h3 className="text-base">{nomeTipo}</h3>
                  </div>
                </div>
                <div className="table-container">
                  <table className="table">
                    <thead>
                      <tr>
                        <th>Nº Eixos</th>
                        <th>Deslocamento (R$/km)</th>
                        <th>Carga e Descarga (R$)</th>
                        <th>Vigência</th>
                        <th>Ações</th>
                      </tr>
                    </thead>
                    <tbody>
                      {EIXOS_VALIDOS.map((eixo) => {
                        const item = itens.find((i) => i.num_eixos === eixo)
                        if (!item) return null

                        const isEditando = editando?.id === item.id

                        return (
                          <tr key={item.id}>
                            <td>
                              <span className="badge-gray font-mono font-bold">{eixo} eixos</span>
                            </td>
                            <td>
                              {isEditando ? (
                                <input
                                  className="input w-32 text-sm"
                                  type="number"
                                  step="0.0001"
                                  value={formDeslocamento}
                                  onChange={(e) => setFormDeslocamento(e.target.value)}
                                />
                              ) : (
                                <span className="font-mono text-emerald-700 font-semibold">
                                  R$ {item.coeficiente_deslocamento.toFixed(4)}
                                </span>
                              )}
                            </td>
                            <td>
                              {isEditando ? (
                                <input
                                  className="input w-32 text-sm"
                                  type="number"
                                  step="0.01"
                                  value={formCargaDescarga}
                                  onChange={(e) => setFormCargaDescarga(e.target.value)}
                                />
                              ) : (
                                <span className="font-mono text-blue-700 font-semibold">
                                  R$ {item.coeficiente_carga_descarga.toFixed(2)}
                                </span>
                              )}
                            </td>
                            <td className="text-gray-500 text-xs">
                              {new Date(item.vigencia_inicio).toLocaleDateString('pt-BR')}
                              {item.vigencia_fim && ` → ${new Date(item.vigencia_fim).toLocaleDateString('pt-BR')}`}
                            </td>
                            <td>
                              {isEditando ? (
                                <div className="flex items-center gap-1">
                                  <button className="btn-success btn-sm" onClick={salvar} disabled={salvando}>
                                    {salvando ? <Loader2 size={12} className="animate-spin" /> : <Check size={12} />}
                                  </button>
                                  <button className="btn-ghost btn-sm" onClick={() => setEditando(null)}>
                                    <X size={12} />
                                  </button>
                                </div>
                              ) : (
                                <button className="btn-ghost btn-sm" onClick={() => abrirEditar(item)}>
                                  <Pencil size={14} />
                                </button>
                              )}
                            </td>
                          </tr>
                        )
                      })}
                    </tbody>
                  </table>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
