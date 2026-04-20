import { useEffect, useMemo, useRef, useState } from 'react'
import { CheckSquare, ChevronDown, Square, X } from 'lucide-react'

interface MultiSelectFilterProps {
  label: string
  options: string[]
  value: string[]
  onChange: (next: string[]) => void
  placeholder?: string
}

const resumirSelecao = (selecionados: string[], placeholder: string) => {
  if (!selecionados.length) return placeholder
  if (selecionados.length <= 2) return selecionados.join(', ')
  return `${selecionados.slice(0, 2).join(', ')} +${selecionados.length - 2}`
}

export function MultiSelectFilter({
  label,
  options,
  value,
  onChange,
  placeholder = 'Selecione',
}: MultiSelectFilterProps) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)

  useEffect(() => {
    const onClickOutside = (event: MouseEvent) => {
      if (!ref.current?.contains(event.target as Node)) {
        setOpen(false)
      }
    }

    document.addEventListener('mousedown', onClickOutside)
    return () => document.removeEventListener('mousedown', onClickOutside)
  }, [])

  const todosSelecionados = options.length > 0 && value.length === options.length

  const textoResumo = useMemo(() => resumirSelecao(value, placeholder), [value, placeholder])

  const toggle = (item: string) => {
    if (value.includes(item)) {
      onChange(value.filter((v) => v !== item))
      return
    }
    onChange([...value, item])
  }

  return (
    <div className="space-y-1" ref={ref}>
      <label className="label">{label}</label>
      <button
        type="button"
        className="input flex items-center justify-between text-left"
        onClick={() => setOpen((prev) => !prev)}
      >
        <span className="truncate">{textoResumo || 'Todas'}</span>
        <ChevronDown size={16} className={`${open ? 'rotate-180' : ''} transition-transform`} />
      </button>

      {open && (
        <div className="border border-gray-200 rounded-xl bg-white shadow-lg p-3 mt-1 z-20 relative">
          <div className="flex items-center justify-between gap-2 mb-2">
            <button
              type="button"
              className="text-xs text-brand-700 hover:underline"
              onClick={() => onChange(todosSelecionados ? [] : options)}
            >
              {todosSelecionados ? 'Desmarcar todos' : 'Selecionar todos'}
            </button>
            <button
              type="button"
              className="text-xs text-gray-600 hover:underline inline-flex items-center gap-1"
              onClick={() => onChange([])}
            >
              <X size={12} /> Limpar
            </button>
          </div>

          <div className="max-h-44 overflow-auto space-y-1 pr-1">
            {options.map((option) => {
              const checked = value.includes(option)
              return (
                <button
                  key={option}
                  type="button"
                  className="w-full text-left text-sm px-2 py-1.5 rounded hover:bg-gray-50 flex items-center gap-2"
                  onClick={() => toggle(option)}
                >
                  {checked ? <CheckSquare size={14} className="text-brand-600" /> : <Square size={14} className="text-gray-400" />}
                  <span className="truncate">{option}</span>
                </button>
              )
            })}
            {!options.length && <p className="text-xs text-gray-400">Sem opções para este upload.</p>}
          </div>
        </div>
      )}
    </div>
  )
}
