import { useState, useCallback } from 'react'
import { carteiraUploadService } from '@/services/carteira-upload.service'

export interface UploadState {
  arquivo: File | null
  uploadId: string | null
  nomeArquivo: string | null
  totalLinhas: number
  totalColunas: number
  colunasDetectadas: string[]
  linhaCabecalhoDetectada: number | null
  preview: Record<string, unknown>[]
  carregando: boolean
  erro: string | null
}

const ESTADO_INICIAL: UploadState = {
  arquivo: null,
  uploadId: null,
  nomeArquivo: null,
  totalLinhas: 0,
  totalColunas: 0,
  colunasDetectadas: [],
  linhaCabecalhoDetectada: null,
  preview: [],
  carregando: false,
  erro: null,
}

export function useUploadCarteira() {
  const [state, setState] = useState<UploadState>(ESTADO_INICIAL)

  const processar = useCallback(async (file: File, usuarioId: string, filialId: string) => {
    setState((prev) => ({ ...prev, carregando: true, erro: null, arquivo: file }))

    try {
      const resultado = await carteiraUploadService.importarCarteiraExcel({
        file,
        usuarioId,
        filialId,
      })

      setState({
        arquivo: file,
        uploadId: resultado.uploadId,
        nomeArquivo: resultado.nomeArquivo,
        totalLinhas: resultado.totalLinhas,
        totalColunas: resultado.totalColunas,
        colunasDetectadas: resultado.colunasDetectadas,
        linhaCabecalhoDetectada: resultado.linhaCabecalho,
        preview: resultado.preview,
        carregando: false,
        erro: null,
      })
    } catch (err) {
      setState((prev) => ({
        ...prev,
        carregando: false,
        erro: `Erro ao processar arquivo: ${err instanceof Error ? err.message : 'Formato inválido'}`,
      }))
    }
  }, [])

  const carregarUploadExistente = useCallback((payload: {
    uploadId: string
    nomeArquivo: string
    totalLinhas: number
    totalColunas: number
    colunasDetectadas: string[]
    linhaCabecalhoDetectada: number | null
    preview: Record<string, unknown>[]
  }) => {
    setState((prev) => ({
      ...prev,
      uploadId: payload.uploadId,
      nomeArquivo: payload.nomeArquivo,
      totalLinhas: payload.totalLinhas,
      totalColunas: payload.totalColunas,
      colunasDetectadas: payload.colunasDetectadas,
      linhaCabecalhoDetectada: payload.linhaCabecalhoDetectada,
      preview: payload.preview,
      erro: null,
    }))
  }, [])

  const limpar = useCallback(() => {
    setState(ESTADO_INICIAL)
  }, [])

  return { state, processar, limpar, carregarUploadExistente }
}
