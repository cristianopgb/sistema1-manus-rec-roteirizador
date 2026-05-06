import { useEffect, useMemo, useState } from 'react'
import toast from 'react-hot-toast'
import { supabase } from '@/lib/supabase'

type Item = { id: string; codigo: string; nome: string; cnpj: string | null; observacao: string | null; ativo: boolean; filial_id: string | null }

export function TransportadorasRedespachoPage() {
  const [itens, setItens] = useState<Item[]>([])
  const [filtro, setFiltro] = useState('')
  const [form, setForm] = useState({ id: '', codigo: '', nome: '', cnpj: '', observacao: '', ativo: true })

  const carregar = async () => {
    const { data, error } = await supabase.from('transportadoras_redespacho').select('id,codigo,nome,cnpj,observacao,ativo,filial_id').order('codigo')
    if (error) return toast.error(error.message)
    setItens((data ?? []) as Item[])
  }
  useEffect(() => { void carregar() }, [])

  const filtrados = useMemo(() => itens.filter((i) => `${i.codigo} ${i.nome}`.toLowerCase().includes(filtro.toLowerCase())), [itens, filtro])

  const salvar = async () => {
    if (!form.codigo.trim() || !form.nome.trim()) return toast.error('Código e nome são obrigatórios')
    const payload = { codigo: form.codigo.trim(), nome: form.nome.trim(), cnpj: form.cnpj.trim() || null, observacao: form.observacao.trim() || null, ativo: form.ativo }
    const query = form.id
      ? supabase.from('transportadoras_redespacho').update(payload).eq('id', form.id)
      : supabase.from('transportadoras_redespacho').insert(payload)
    const { error } = await query
    if (error) return toast.error(error.message)
    toast.success('Salvo com sucesso')
    setForm({ id: '', codigo: '', nome: '', cnpj: '', observacao: '', ativo: true })
    await carregar()
  }

  return <div className="space-y-4">
    <h1 className="text-xl font-semibold">Transportadoras Redespacho</h1>
    <div className="card p-4 grid grid-cols-1 md:grid-cols-5 gap-3">
      <input className="input" placeholder="Código" value={form.codigo} onChange={(e) => setForm({ ...form, codigo: e.target.value })} />
      <input className="input" placeholder="Nome" value={form.nome} onChange={(e) => setForm({ ...form, nome: e.target.value })} />
      <input className="input" placeholder="CNPJ (opcional)" value={form.cnpj} onChange={(e) => setForm({ ...form, cnpj: e.target.value })} />
      <input className="input" placeholder="Observação" value={form.observacao} onChange={(e) => setForm({ ...form, observacao: e.target.value })} />
      <label className="flex items-center gap-2"><input type="checkbox" checked={form.ativo} onChange={(e) => setForm({ ...form, ativo: e.target.checked })} />Ativo</label>
      <button className="btn-primary md:col-span-5" onClick={() => void salvar()}>Salvar</button>
    </div>
    <div className="card p-4">
      <input className="input mb-3" placeholder="Filtrar por código ou nome" value={filtro} onChange={(e) => setFiltro(e.target.value)} />
      <table className="w-full text-sm"><thead><tr><th>Código</th><th>Nome</th><th>CNPJ</th><th>Status</th><th>Observação</th><th /></tr></thead><tbody>
        {filtrados.map((i) => <tr key={i.id} className="border-t"><td>{i.codigo}</td><td>{i.nome}</td><td>{i.cnpj || '-'}</td><td>{i.ativo ? 'Ativo' : 'Inativo'}</td><td>{i.observacao || '-'}</td><td><button className="btn-ghost btn-sm" onClick={() => setForm({ id: i.id, codigo: i.codigo, nome: i.nome, cnpj: i.cnpj || '', observacao: i.observacao || '', ativo: i.ativo })}>Editar</button></td></tr>)}
      </tbody></table>
    </div>
  </div>
}
