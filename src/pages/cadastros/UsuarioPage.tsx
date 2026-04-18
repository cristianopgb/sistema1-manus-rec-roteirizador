import { useState, useEffect } from 'react'
import { Users, Plus, Pencil, Power, Loader2, X, Check } from 'lucide-react'
import { usuariosService } from '@/services/usuarios.service'
import { filiaisService } from '@/services/filiais.service'
import { UserProfile, Filial } from '@/types'
import toast from 'react-hot-toast'

interface FormUsuario {
  email: string
  nome: string
  perfil: 'master' | 'roteirizador'
  filial_id: string
  password: string
}

const FORM_VAZIO: FormUsuario = {
  email: '', nome: '', perfil: 'roteirizador', filial_id: '', password: '',
}

export function UsuarioPage() {
  const [usuarios, setUsuarios] = useState<UserProfile[]>([])
  const [filiais, setFiliais] = useState<Filial[]>([])
  const [loading, setLoading] = useState(true)
  const [showModal, setShowModal] = useState(false)
  const [editando, setEditando] = useState<UserProfile | null>(null)
  const [form, setForm] = useState<FormUsuario>(FORM_VAZIO)
  const [salvando, setSalvando] = useState(false)

  const carregar = async () => {
    setLoading(true)
    try {
      const [u, f] = await Promise.all([
        usuariosService.listar(),
        filiaisService.buscarAtivas(),
      ])
      setUsuarios(u)
      setFiliais(f)
    } catch {
      toast.error('Erro ao carregar usuários')
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

  const abrirEditar = (u: UserProfile) => {
    setEditando(u)
    setForm({
      email: u.email,
      nome: u.nome,
      perfil: u.perfil,
      filial_id: u.filial_id || '',
      password: '',
    })
    setShowModal(true)
  }

  const salvar = async () => {
    if (!form.email || !form.nome) {
      toast.error('Preencha e-mail e nome')
      return
    }
    if (form.perfil === 'roteirizador' && !form.filial_id) {
      toast.error('Roteirizador precisa de uma filial')
      return
    }
    setSalvando(true)
    try {
      if (editando) {
        await usuariosService.atualizar(editando.id, {
          nome: form.nome,
          perfil: form.perfil,
          filial_id: form.perfil === 'master' ? null : form.filial_id || null,
        })
        toast.success('Usuário atualizado')
      } else {
        const { error } = await usuariosService.criar({
          email: form.email,
          nome: form.nome,
          perfil: form.perfil,
          filial_id: form.perfil === 'master' ? null : form.filial_id || null,
          password: form.password,
        })
        if (error) throw error
        toast.success('Usuário criado. Configure a senha no Supabase Dashboard.')
      }
      setShowModal(false)
      carregar()
    } catch {
      toast.error('Erro ao salvar usuário')
    } finally {
      setSalvando(false)
    }
  }

  const alternarAtivo = async (u: UserProfile) => {
    try {
      await usuariosService.alternarAtivo(u.id, !u.ativo)
      toast.success(`Usuário ${u.ativo ? 'desativado' : 'ativado'}`)
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
            <Users size={20} className="text-brand-700" />
          </div>
          <div>
            <h1>Usuários</h1>
            <p className="text-sm text-gray-500">{usuarios.length} usuário(s) cadastrado(s)</p>
          </div>
        </div>
        <button className="btn-primary" onClick={abrirNovo}>
          <Plus size={16} /> Novo Usuário
        </button>
      </div>

      <div className="card">
        {loading ? (
          <div className="flex items-center justify-center py-16">
            <Loader2 size={24} className="animate-spin text-brand-600" />
          </div>
        ) : (
          <div className="table-container">
            <table className="table">
              <thead>
                <tr>
                  <th>Nome</th>
                  <th>E-mail</th>
                  <th>Perfil</th>
                  <th>Filial</th>
                  <th>Status</th>
                  <th>Ações</th>
                </tr>
              </thead>
              <tbody>
                {usuarios.map((u) => (
                  <tr key={u.id}>
                    <td className="font-medium">{u.nome}</td>
                    <td className="text-gray-600">{u.email}</td>
                    <td>
                      <span className={u.perfil === 'master' ? 'badge-blue' : 'badge-green'}>
                        {u.perfil === 'master' ? 'Master' : 'Roteirizador'}
                      </span>
                    </td>
                    <td className="text-gray-600">{u.filial_nome || '—'}</td>
                    <td>
                      <span className={u.ativo ? 'badge-green' : 'badge-red'}>
                        {u.ativo ? 'Ativo' : 'Inativo'}
                      </span>
                    </td>
                    <td>
                      <div className="flex items-center gap-2">
                        <button className="btn-ghost btn-sm" onClick={() => abrirEditar(u)}><Pencil size={14} /></button>
                        <button
                          className={`btn-ghost btn-sm ${u.ativo ? 'text-red-500' : 'text-green-600'}`}
                          onClick={() => alternarAtivo(u)}
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
          <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md">
            <div className="flex items-center justify-between p-6 border-b">
              <h2>{editando ? 'Editar Usuário' : 'Novo Usuário'}</h2>
              <button className="btn-ghost btn-sm" onClick={() => setShowModal(false)}><X size={18} /></button>
            </div>
            <div className="p-6 space-y-4">
              <div>
                <label className="label">Nome *</label>
                <input className="input" value={form.nome}
                  onChange={(e) => setForm({ ...form, nome: e.target.value })} placeholder="Nome completo" />
              </div>
              <div>
                <label className="label">E-mail *</label>
                <input className="input" type="email" value={form.email} disabled={!!editando}
                  onChange={(e) => setForm({ ...form, email: e.target.value })} placeholder="email@rec.com.br" />
              </div>
              <div>
                <label className="label">Perfil *</label>
                <select className="input" value={form.perfil}
                  onChange={(e) => setForm({ ...form, perfil: e.target.value as 'master' | 'roteirizador', filial_id: '' })}>
                  <option value="roteirizador">Roteirizador</option>
                  <option value="master">Master</option>
                </select>
              </div>
              {form.perfil === 'roteirizador' && (
                <div>
                  <label className="label">Filial *</label>
                  <select className="input" value={form.filial_id}
                    onChange={(e) => setForm({ ...form, filial_id: e.target.value })}>
                    <option value="">Selecione a filial...</option>
                    {filiais.map((f) => <option key={f.id} value={f.id}>{f.nome}</option>)}
                  </select>
                </div>
              )}
              {!editando && (
                <div className="bg-amber-50 border border-amber-200 rounded-lg p-3 text-sm text-amber-700">
                  <strong>Atenção:</strong> Após criar o usuário aqui, acesse o Supabase Dashboard → Authentication → Users para definir a senha de acesso.
                </div>
              )}
            </div>
            <div className="flex justify-end gap-3 px-6 py-4 border-t">
              <button className="btn-secondary" onClick={() => setShowModal(false)}>Cancelar</button>
              <button className="btn-primary" onClick={salvar} disabled={salvando}>
                {salvando ? <Loader2 size={16} className="animate-spin" /> : <Check size={16} />}
                {editando ? 'Salvar' : 'Criar Usuário'}
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
