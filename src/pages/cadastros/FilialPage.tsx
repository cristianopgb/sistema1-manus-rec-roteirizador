import { useState, useEffect } from 'react'
import { Building2, Plus, Pencil, Power, Loader2, X, Check } from 'lucide-react'
import { filiaisService } from '@/services/filiais.service'
import { getErrorMessage } from '@/lib/async'
import { Filial } from '@/types'
import toast from 'react-hot-toast'

const ESTADOS_BR = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB','PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']

interface FormFilial {
  codigo: string
  nome: string
  cidade: string
  uf: string
  cep: string
  endereco: string
  latitude: string
  longitude: string
}

const FORM_VAZIO: FormFilial = {
  codigo: '', nome: '', cidade: '', uf: 'MG',
  cep: '', endereco: '', latitude: '', longitude: '',
}

export function FilialPage() {
  const [filiais, setFiliais] = useState<Filial[]>([])
  const [loading, setLoading] = useState(true)
  const [showModal, setShowModal] = useState(false)
  const [editando, setEditando] = useState<Filial | null>(null)
  const [form, setForm] = useState<FormFilial>(FORM_VAZIO)
  const [salvando, setSalvando] = useState(false)

  const carregar = async () => {
    setLoading(true)
    try {
      const data = await filiaisService.listar()
      setFiliais(data)
    } catch (error) {
      console.error('Erro ao carregar filiais:', error)
      toast.error(getErrorMessage(error, 'Erro ao carregar filiais'))
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { carregar() }, [])

  const abrirNovo = () => {
    setEditando(null)
    setForm(FORM_VAZIO)
    setShowModal(true)
  }

  const abrirEditar = (filial: Filial) => {
    setEditando(filial)
    setForm({
      codigo: filial.codigo,
      nome: filial.nome,
      cidade: filial.cidade,
      uf: filial.uf,
      cep: filial.cep || '',
      endereco: filial.endereco || '',
      latitude: String(filial.latitude),
      longitude: String(filial.longitude),
    })
    setShowModal(true)
  }

  const salvar = async () => {
    if (!form.codigo || !form.nome || !form.cidade || !form.latitude || !form.longitude) {
      toast.error('Preencha todos os campos obrigatórios')
      return
    }
    setSalvando(true)
    try {
      const payload = {
        codigo: form.codigo.toUpperCase(),
        nome: form.nome,
        cidade: form.cidade,
        uf: form.uf,
        cep: form.cep || null,
        endereco: form.endereco || null,
        latitude: parseFloat(form.latitude),
        longitude: parseFloat(form.longitude),
        ativo: true,
      }
      if (editando) {
        await filiaisService.atualizar(editando.id, payload as Partial<import('@/types').Filial>)
        toast.success('Filial atualizada com sucesso')
      } else {
        await filiaisService.criar(payload as Omit<import('@/types').Filial, 'id' | 'created_at'>)
        toast.success('Filial criada com sucesso')
      }
      setShowModal(false)
      carregar()
    } catch (error) {
      console.error('Erro ao salvar filial:', error)
      toast.error(getErrorMessage(error, 'Erro ao salvar filial'))
    } finally {
      setSalvando(false)
    }
  }

  const alternarAtivo = async (filial: Filial) => {
    try {
      await filiaisService.alternarAtivo(filial.id, !filial.ativo)
      toast.success(`Filial ${filial.ativo ? 'desativada' : 'ativada'}`)
      carregar()
    } catch (error) {
      console.error('Erro ao alterar status da filial:', error)
      toast.error(getErrorMessage(error, 'Erro ao alterar status'))
    }
  }

  return (
    <div className="fade-in">
      {/* Header */}
      <div className="flex items-center justify-between mb-6">
        <div className="flex items-center gap-3">
          <div className="w-10 h-10 bg-brand-100 rounded-xl flex items-center justify-center">
            <Building2 size={20} className="text-brand-700" />
          </div>
          <div>
            <h1>Filiais</h1>
            <p className="text-sm text-gray-500">{filiais.length} filial(is) cadastrada(s)</p>
          </div>
        </div>
        <button className="btn-primary" onClick={abrirNovo}>
          <Plus size={16} /> Nova Filial
        </button>
      </div>

      {/* Tabela */}
      <div className="card">
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 size={24} className="animate-spin text-brand-600" />
          </div>
        ) : filiais.length === 0 ? (
          <div className="text-center py-16 text-gray-400">
            <Building2 size={40} className="mx-auto mb-3 opacity-30" />
            <p>Nenhuma filial cadastrada</p>
          </div>
        ) : (
          <div className="table-container">
            <table className="table">
              <thead>
                <tr>
                  <th>Código</th>
                  <th>Nome</th>
                  <th>Cidade / UF</th>
                  <th>Coordenadas</th>
                  <th>Status</th>
                  <th>Ações</th>
                </tr>
              </thead>
              <tbody>
                {filiais.map((f) => (
                  <tr key={f.id}>
                    <td><span className="font-mono font-semibold text-brand-700">{f.codigo}</span></td>
                    <td className="font-medium">{f.nome}</td>
                    <td>{f.cidade} / {f.uf}</td>
                    <td className="font-mono text-xs text-gray-500">
                      {f.latitude.toFixed(4)}, {f.longitude.toFixed(4)}
                    </td>
                    <td>
                      <span className={f.ativo ? 'badge-green' : 'badge-red'}>
                        {f.ativo ? 'Ativa' : 'Inativa'}
                      </span>
                    </td>
                    <td>
                      <div className="flex items-center gap-2">
                        <button className="btn-ghost btn-sm" onClick={() => abrirEditar(f)} title="Editar">
                          <Pencil size={14} />
                        </button>
                        <button
                          className={`btn-ghost btn-sm ${f.ativo ? 'text-red-500' : 'text-green-600'}`}
                          onClick={() => alternarAtivo(f)}
                          title={f.ativo ? 'Desativar' : 'Ativar'}
                        >
                          <Power size={14} />
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {/* Modal */}
      {showModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center z-50 p-4">
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-lg">
            <div className="flex items-center justify-between p-6 border-b">
              <h2>{editando ? 'Editar Filial' : 'Nova Filial'}</h2>
              <button className="btn-ghost btn-sm" onClick={() => setShowModal(false)}>
                <X size={18} />
              </button>
            </div>
            <div className="p-6 space-y-4">
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="label">Código *</label>
                  <input className="input uppercase" value={form.codigo}
                    onChange={(e) => setForm({ ...form, codigo: e.target.value })} placeholder="CTG" />
                </div>
                <div>
                  <label className="label">UF *</label>
                  <select className="input" value={form.uf}
                    onChange={(e) => setForm({ ...form, uf: e.target.value })}>
                    {ESTADOS_BR.map((uf) => <option key={uf}>{uf}</option>)}
                  </select>
                </div>
              </div>
              <div>
                <label className="label">Nome da Filial *</label>
                <input className="input" value={form.nome}
                  onChange={(e) => setForm({ ...form, nome: e.target.value })} placeholder="REC Contagem" />
              </div>
              <div>
                <label className="label">Cidade *</label>
                <input className="input" value={form.cidade}
                  onChange={(e) => setForm({ ...form, cidade: e.target.value })} placeholder="Contagem" />
              </div>
              <div>
                <label className="label">Endereço</label>
                <input className="input" value={form.endereco}
                  onChange={(e) => setForm({ ...form, endereco: e.target.value })} placeholder="Rua, número, bairro" />
              </div>
              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="label">Latitude *</label>
                  <input className="input" type="number" step="0.000001" value={form.latitude}
                    onChange={(e) => setForm({ ...form, latitude: e.target.value })} placeholder="-19.9245" />
                </div>
                <div>
                  <label className="label">Longitude *</label>
                  <input className="input" type="number" step="0.000001" value={form.longitude}
                    onChange={(e) => setForm({ ...form, longitude: e.target.value })} placeholder="-44.0536" />
                </div>
              </div>
            </div>
            <div className="flex justify-end gap-3 px-6 py-4 border-t">
              <button className="btn-secondary" onClick={() => setShowModal(false)}>Cancelar</button>
              <button className="btn-primary" onClick={salvar} disabled={salvando}>
                {salvando ? <Loader2 size={16} className="animate-spin" /> : <Check size={16} />}
                {editando ? 'Salvar Alterações' : 'Criar Filial'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
