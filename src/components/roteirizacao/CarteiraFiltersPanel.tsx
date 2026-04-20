import { ChevronDown, FilterX } from 'lucide-react'
import { FiltrosCarteira } from '@/types'
import { MultiSelectFilter } from './MultiSelectFilter'

type MultiSelectField = keyof Pick<FiltrosCarteira, 'filial_r' | 'uf' | 'destin' | 'cidade' | 'tomad' | 'mesoregiao' | 'prioridade' | 'restricao_veiculo'>

const FILTROS_MULTI: Array<{ key: MultiSelectField; label: string; placeholder?: string }> = [
  { key: 'filial_r', label: 'Filial', placeholder: 'Todas' },
  { key: 'uf', label: 'UF', placeholder: 'Todas' },
  { key: 'mesoregiao', label: 'Mesorregião', placeholder: 'Todas' },
  { key: 'destin', label: 'Destinatário', placeholder: 'Selecione' },
  { key: 'cidade', label: 'Cidade', placeholder: 'Selecione' },
  { key: 'tomad', label: 'Tomador', placeholder: 'Selecione' },
  { key: 'prioridade', label: 'Prioridade', placeholder: 'Todas' },
  { key: 'restricao_veiculo', label: 'Restrição Veículo', placeholder: 'Todas' },
]

interface CarteiraFiltersPanelProps {
  filtros: FiltrosCarteira
  opcoesFiltro: Record<MultiSelectField, string[]>
  expanded: boolean
  onToggleExpanded: () => void
  onChange: (next: FiltrosCarteira) => void
  onClear: () => void
  onApply: () => void
}

export function CarteiraFiltersPanel({
  filtros,
  opcoesFiltro,
  expanded,
  onToggleExpanded,
  onChange,
  onClear,
  onApply,
}: CarteiraFiltersPanelProps) {
  const setField = <T extends keyof FiltrosCarteira>(field: T, value: FiltrosCarteira[T]) => {
    onChange({ ...filtros, [field]: value })
  }

  return (
    <div className="card p-4">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold text-gray-900">Filtros avançados</h3>
        <button type="button" className="btn-ghost" onClick={onToggleExpanded}>
          {expanded ? 'Recolher' : 'Expandir'}
          <ChevronDown size={14} className={expanded ? 'rotate-180 transition-transform' : 'transition-transform'} />
        </button>
      </div>

      {expanded && (
        <div className="space-y-4 mt-4">
          <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-4 gap-3">
            <div>
              <label className="label">Status validação</label>
              <select className="input" disabled value="valida">
                <option value="valida">Válidas</option>
              </select>
            </div>
            {FILTROS_MULTI.slice(0, 3).map(({ key, label, placeholder }) => (
              <MultiSelectFilter
                key={key}
                label={label}
                options={opcoesFiltro[key]}
                value={filtros[key]}
                placeholder={placeholder}
                onChange={(next) => setField(key, next)}
              />
            ))}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            {FILTROS_MULTI.slice(3, 6).map(({ key, label, placeholder }) => (
              <MultiSelectFilter
                key={key}
                label={label}
                options={opcoesFiltro[key]}
                value={filtros[key]}
                placeholder={placeholder}
                onChange={(next) => setField(key, next)}
              />
            ))}
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
            <div>
              <label className="label">Agendam. DE / ATÉ</label>
              <div className="grid grid-cols-2 gap-2">
                <input type="date" className="input" value={filtros.agendam_de} onChange={(e) => setField('agendam_de', e.target.value)} />
                <input type="date" className="input" value={filtros.agendam_ate} onChange={(e) => setField('agendam_ate', e.target.value)} />
              </div>
            </div>
            <div>
              <label className="label">D.L.E. DE / ATÉ</label>
              <div className="grid grid-cols-2 gap-2">
                <input type="date" className="input" value={filtros.dle_de} onChange={(e) => setField('dle_de', e.target.value)} />
                <input type="date" className="input" value={filtros.dle_ate} onChange={(e) => setField('dle_ate', e.target.value)} />
              </div>
            </div>
            <div>
              <label className="label">Data Des. DE / ATÉ</label>
              <div className="grid grid-cols-2 gap-2">
                <input type="date" className="input" value={filtros.data_des_de} onChange={(e) => setField('data_des_de', e.target.value)} />
                <input type="date" className="input" value={filtros.data_des_ate} onChange={(e) => setField('data_des_ate', e.target.value)} />
              </div>
            </div>
            <div>
              <label className="label">Data NF DE / ATÉ</label>
              <div className="grid grid-cols-2 gap-2">
                <input type="date" className="input" value={filtros.data_nf_de} onChange={(e) => setField('data_nf_de', e.target.value)} />
                <input type="date" className="input" value={filtros.data_nf_ate} onChange={(e) => setField('data_nf_ate', e.target.value)} />
              </div>
            </div>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-3">
            <div>
              <label className="label">Carro Dedicado</label>
              <select className="input" value={filtros.carro_dedicado} onChange={(e) => setField('carro_dedicado', e.target.value as FiltrosCarteira['carro_dedicado'])}>
                <option value="todos">Todos</option>
                <option value="sim">Sim</option>
                <option value="nao">Não</option>
              </select>
            </div>
            {FILTROS_MULTI.slice(6).map(({ key, label, placeholder }) => (
              <MultiSelectFilter
                key={key}
                label={label}
                options={opcoesFiltro[key]}
                value={filtros[key]}
                placeholder={placeholder}
                onChange={(next) => setField(key, next)}
              />
            ))}
          </div>

          <div className="flex justify-end gap-2 border-t pt-4">
            <button type="button" className="btn-ghost" onClick={onClear}><FilterX size={14} /> Limpar Filtros</button>
            <button type="button" className="btn-primary" onClick={onApply}>Aplicar Filtros</button>
          </div>
        </div>
      )}
    </div>
  )
}
