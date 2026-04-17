import { useState, useEffect } from 'react'
import { Truck, Plus, Pencil, Power, Loader2, X, Check } from 'lucide-react'
import { veiculosService } from '@/services/veiculos.service'
import { filiaisService } from '@/services/filiais.service'
import { Veiculo, Filial } from '@/types'
import toast from 'react-hot-toast'

const TIPOS_VEICULO = ['VUC', '3/4', 'TOCO', 'TRUCK', 'CARRETA', 'BITRUCK'] as const

interface FormVeiculo {
  filial_id: string
  codigo: string
  placa: string
  tipo: string
  capacidade_peso_kg: string
  capacidade_volume_m3: string
  num_eixos: string
  max_km_distancia: string
  max_entregas: string
  ocupacao_minima_perc: string
  ocupacao_maxima_perc: string
  motorista: string
}

const FORM_VAZIO: FormVeiculo = {
  filial_id: '', codigo: '', placa: '', tipo: 'TOCO',
  capacidade_peso_kg: '', capacidade_volume_m3: '',
  num_eixos: '2', max_km_distancia: '', max_entregas: '',
  ocupacao_minima_perc: '70', ocupacao_maxima_perc: '100',
  motorista: '',
}

// Defaults por tipo de veículo
const DEFAULTS_TIPO: Record<string, Partial<FormVeiculo>> = {
  'VUC':     { capacidade_peso_kg: '3000',  capacidade_volume_m3: '16',  num_eixos: '2', max_km_distancia: '120',  max_entregas: '10' },
  '3/4':     { capacidade_peso_kg: '4500',  capacidade_volume_m3: '28',  num_eixos: '2', max_km_distancia: '320',  max_entregas: '8'  },
  'TOCO':    { capacidade_peso_kg: '8000',  capacidade_volume_m3: '45',  num_eixos: '2', max_km_distancia: '450',  max_entregas: '5'  },
  'TRUCK':   { capacidade_peso_kg: '14000', capacidade_volume_m3: '60',  num_eixos: '3', max_km_distancia: '1200', max_entregas: '4'  },
  'CARRETA': { capacidade_peso_kg: '25000', capacidade_volume_m3: '90',  num_eixos: '5', max_km_distancia: '2500', max_entregas: '2'  },
  'BITRUCK': { capacidade_peso_kg: '18000', capacidade_volume_m3: '70',  num_eixos: '4', max_km_distancia: '1500', max_entregas: '3'  },
}

export function VeiculoPage() {
  const [veiculos, setVeiculos] = useState<Veiculo[]>([])
  const [filiais, setFiliais] = useState<Filial[]>([])
  const [loading, setLoading] = useState(true)
  const [showModal, setShowModal] = useState(false)
  const [editando, setEditando] = useState<Veiculo | null>(null)
  const [form, setForm] = useState<FormVeiculo>(FORM_VAZIO)
  const [salvando, setSalvando] = useState(false)
  const [filtroFilial, setFiltroFilial] = useState('')

  const carregar = async () => {
    setLoading(true)
    try {
      const [v, f] = await Promise.all([
        veiculosService.listar(filtroFilial || undefined),
        filiaisService.buscarAtivas(),
      ])
      setVeiculos(v)
      setFiliais(f)
    } catch {
      toast.error('Erro ao carregar veículos')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { carregar() }, [filtroFilial])

  const abrirNovo = () => {
    setEditando(null)
    setForm({ ...FORM_VAZIO, filial_id: filiais[0]?.id || '' })
    setShowModal(true)
  }

  const abrirEditar = (v: Veiculo) => {
    setEditando(v)
    setForm({
      filial_id: v.filial_id,
      codigo: v.codigo,
      placa: v.placa,
      tipo: v.tipo,
      capacidade_peso_kg: String(v.capacidade_peso_kg),
      capacidade_volume_m3: String(v.capacidade_volume_m3),
      num_eixos: String(v.num_eixos),
      max_km_distancia: String(v.max_km_distancia),
      max_entregas: String(v.max_entregas),
      ocupacao_minima_perc: String(v.ocupacao_minima_perc),
      ocupacao_maxima_perc: String(v.ocupacao_maxima_perc),
      motorista: v.motorista || '',
    })
    setShowModal(true)
  }

  const aplicarDefaultTipo = (tipo: string) => {
    const def = DEFAULTS_TIPO[tipo] || {}
    setForm((prev) => ({ ...prev, tipo, ...def }))
  }

  const salvar = async () => {
    if (!form.filial_id || !form.codigo || !form.placa || !form.tipo) {
      toast.error('Preencha todos os campos obrigatórios')
      return
    }
    setSalvando(true)
    try {
      const payload = {
        filial_id: form.filial_id,
        codigo: form.codigo.toUpperCase(),
        placa: form.placa.toUpperCase(),
        tipo: form.tipo as Veiculo['tipo'],
        capacidade_peso_kg: parseFloat(form.capacidade_peso_kg),
        capacidade_volume_m3: parseFloat(form.capacidade_volume_m3),
        num_eixos: parseInt(form.num_eixos),
        max_km_distancia: parseFloat(form.max_km_distancia),
        max_entregas: parseInt(form.max_entregas),
        ocupacao_minima_perc: parseFloat(form.ocupacao_minima_perc),
        ocupacao_maxima_perc: parseFloat(form.ocupacao_maxima_perc),
        motorista: form.motorista || null,
        ativo: true,
      }
      if (editando) {
        await veiculosService.atualizar(editando.id, payload as Partial<import('@/types').Veiculo>)
        toast.success('Veículo atualizado')
      } else {
        await veiculosService.criar(payload as Omit<import('@/types').Veiculo, 'id' | 'created_at' | 'filial_nome'>)
        toast.success('Veículo criado')
      }
      setShowModal(false)
      carregar()
    } catch {
      toast.error('Erro ao salvar veículo')
    } finally {
      setSalvando(false)
    }
  }

  const alternarAtivo = async (v: Veiculo) => {
    try {
      await veiculosService.alternarAtivo(v.id, !v.ativo)
      toast.success(`Veículo ${v.ativo ? 'desativado' : 'ativado'}`)
      carregar()
    } catch {
      toast.error('Erro ao alterar status')
    }
  }

  return (
    <div className="fade-in">
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-brand-100 rounded-xl flex items-center justify-center">
            <Truck size={20} className="text-brand-700" />
          </div>
          <div>
            <h1>Veículos</h1>
            <p className="text-sm text-gray-500">{veiculos.length} veículo(s) cadastrado(s)</p>
          </div>
        </div>
        <div className="flex items-center gap-3">
          <select className="input w-48" value={filtroFilial} onChange={(e) => setFiltroFilial(e.target.value)}>
            <option value="">Todas as filiais</option>
            {filiais.map((f) => <option key={f.id} value={f.id}>{f.nome}</option>)}
          </select>
          <button className="btn-primary" onClick={abrirNovo}>
            <Plus size={16} /> Novo Veículo
          </button>
        </div>
      </div>

      <div className="card">
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 size={24} className="animate-spin text-brand-600" />
          </div>
        ) : veiculos.length === 0 ? (
          <div className="text-center py-16 text-gray-400">
            <Truck size={40} className="mx-auto mb-3 opacity-30" />
            <p>Nenhum veículo cadastrado</p>
          </div>
        ) : (
          <div className="table-container">
            <table className="table">
              <thead>
                <tr>
                  <th>Código / Placa</th>
                  <th>Tipo</th>
                  <th>Filial</th>
                  <th>Cap. Peso</th>
                  <th>Cap. Vol.</th>
                  <th>Eixos</th>
                  <th>Max KM</th>
                  <th>Ocup. Mín</th>
                  <th>Motorista</th>
                  <th>Status</th>
                  <th>Ações</th>
                </tr>
              </thead>
              <tbody>
                {veiculos.map((v) => (
                  <tr key={v.id}>
                    <td>
                      <div className="font-semibold text-brand-700">{v.codigo}</div>
                      <div className="text-xs text-gray-500 font-mono">{v.placa}</div>
                    </td>
                    <td><span className="badge-blue">{v.tipo}</span></td>
                    <td className="text-gray-600">{v.filial_nome}</td>
                    <td>{v.capacidade_peso_kg.toLocaleString('pt-BR')} kg</td>
                    <td>{v.capacidade_volume_m3} m³</td>
                    <td className="text-center">{v.num_eixos}</td>
                    <td>{v.max_km_distancia.toLocaleString('pt-BR')} km</td>
                    <td>{v.ocupacao_minima_perc}%</td>
                    <td className="text-gray-600">{v.motorista || '—'}</td>
                    <td>
                      <span className={v.ativo ? 'badge-green' : 'badge-red'}>
                        {v.ativo ? 'Ativo' : 'Inativo'}
                      </span>
                    </td>
                    <td>
                      <div className="flex items-center gap-2">
                        <button className="btn-ghost btn-sm" onClick={() => abrirEditar(v)}><Pencil size={14} /></button>
                        <button
                          className={`btn-ghost btn-sm ${v.ativo ? 'text-red-500' : 'text-green-600'}`}
                          onClick={() => alternarAtivo(v)}
                        ><Power size={14} /></button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-2xl max-h-[90vh] overflow-y-auto">
            <div className="flex items-center justify-between p-6 border-b sticky top-0 bg-white">
              <h2>{editando ? 'Editar Veículo' : 'Novo Veículo'}</h2>
              <button className="btn-ghost btn-sm" onClick={() => setShowModal(false)}><X size={18} /></button>
            </div>
            <div className="p-6 space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="label">Filial *</label>
                  <select className="input" value={form.filial_id}
                    onChange={(e) => setForm({ ...form, filial_id: e.target.value })}>
                    <option value="">Selecione...</option>
                    {filiais.map((f) => <option key={f.id} value={f.id}>{f.nome}</option>)}
                  </select>
                </div>
                <div>
                  <label className="label">Tipo de Veículo *</label>
                  <select className="input" value={form.tipo}
                    onChange={(e) => aplicarDefaultTipo(e.target.value)}>
                    {TIPOS_VEICULO.map((t) => <option key={t}>{t}</option>)}
                  </select>
                </div>
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="label">Código *</label>
                  <input className="input uppercase" value={form.codigo}
                    onChange={(e) => setForm({ ...form, codigo: e.target.value })} placeholder="VUC-01" />
                </div>
                <div>
                  <label className="label">Placa *</label>
                  <input className="input uppercase" value={form.placa}
                    onChange={(e) => setForm({ ...form, placa: e.target.value })} placeholder="ABC-1234" />
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="label">Cap. Peso (kg) *</label>
                  <input className="input" type="number" value={form.capacidade_peso_kg}
                    onChange={(e) => setForm({ ...form, capacidade_peso_kg: e.target.value })} />
                </div>
                <div>
                  <label className="label">Cap. Volume (m³) *</label>
                  <input className="input" type="number" value={form.capacidade_volume_m3}
                    onChange={(e) => setForm({ ...form, capacidade_volume_m3: e.target.value })} />
                </div>
                <div>
                  <label className="label">Nº de Eixos *</label>
                  <select className="input" value={form.num_eixos}
                    onChange={(e) => setForm({ ...form, num_eixos: e.target.value })}>
                    {[2,3,4,5,6,7,9].map((n) => <option key={n}>{n}</option>)}
                  </select>
                </div>
              </div>
              <div className="grid grid-cols-3 gap-4">
                <div>
                  <label className="label">Max KM *</label>
                  <input className="input" type="number" value={form.max_km_distancia}
                    onChange={(e) => setForm({ ...form, max_km_distancia: e.target.value })} />
                </div>
                <div>
                  <label className="label">Max Entregas *</label>
                  <input className="input" type="number" value={form.max_entregas}
                    onChange={(e) => setForm({ ...form, max_entregas: e.target.value })} />
                </div>
                <div>
                  <label className="label">Ocup. Mínima (%) *</label>
                  <input className="input" type="number" min="0" max="100" value={form.ocupacao_minima_perc}
                    onChange={(e) => setForm({ ...form, ocupacao_minima_perc: e.target.value })} />
                </div>
              </div>
              <div>
                <label className="label">Motorista</label>
                <input className="input" value={form.motorista}
                  onChange={(e) => setForm({ ...form, motorista: e.target.value })} placeholder="Nome do motorista" />
              </div>
            </div>
            <div className="flex justify-end gap-3 px-6 py-4 border-t sticky bottom-0 bg-white">
              <button className="btn-secondary" onClick={() => setShowModal(false)}>Cancelar</button>
              <button className="btn-primary" onClick={salvar} disabled={salvando}>
                {salvando ? <Loader2 size={16} className="animate-spin" /> : <Check size={16} />}
                {editando ? 'Salvar Alterações' : 'Criar Veículo'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
