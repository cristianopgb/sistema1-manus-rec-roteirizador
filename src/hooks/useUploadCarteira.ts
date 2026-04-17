import { useState, useCallback } from 'react'
import * as XLSX from 'xlsx'
import { CarteiraCarga } from '@/types'

export interface UploadState {
  arquivo: File | null
  linhas: number
  colunas: string[]
  preview: Record<string, unknown>[]
  carregando: boolean
  erro: string | null
  carteira: CarteiraCarga[]
}

const ESTADO_INICIAL: UploadState = {
  arquivo: null,
  linhas: 0,
  colunas: [],
  preview: [],
  carregando: false,
  erro: null,
  carteira: [],
}

export function useUploadCarteira() {
  const [state, setState] = useState<UploadState>(ESTADO_INICIAL)

  const processar = useCallback(async (file: File) => {
    setState((prev) => ({ ...prev, carregando: true, erro: null, arquivo: file }))

    try {
      const buffer = await file.arrayBuffer()
      const workbook = XLSX.read(buffer, { type: 'array', cellDates: true })
      const sheetName = workbook.SheetNames[0]
      const sheet = workbook.Sheets[sheetName]

      // Converter para JSON preservando cabeçalhos originais
      const dados = XLSX.utils.sheet_to_json<Record<string, unknown>>(sheet, {
        raw: false,
        dateNF: 'dd/mm/yyyy hh:mm:ss',
        defval: null,
      })

      if (dados.length === 0) {
        setState((prev) => ({
          ...prev,
          carregando: false,
          erro: 'O arquivo está vazio ou não possui dados na primeira aba',
        }))
        return
      }

      const colunas = Object.keys(dados[0])
      const preview = dados.slice(0, 5)

      // Converter para o formato CarteiraCarga (passthrough — o M1 faz a padronização)
      const carteira: CarteiraCarga[] = dados.map((row, idx) => ({
        _linha_origem: idx + 2, // +2 porque linha 1 é cabeçalho
        ...row,
      }))

      setState({
        arquivo: file,
        linhas: dados.length,
        colunas,
        preview,
        carregando: false,
        erro: null,
        carteira,
      })
    } catch (err) {
      setState((prev) => ({
        ...prev,
        carregando: false,
        erro: `Erro ao processar arquivo: ${err instanceof Error ? err.message : 'Formato inválido'}`,
      }))
    }
  }, [])

  const limpar = useCallback(() => {
    setState(ESTADO_INICIAL)
  }, [])

  return { state, processar, limpar }
}
