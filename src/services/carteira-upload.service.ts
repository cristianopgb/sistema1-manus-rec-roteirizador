import * as XLSX from 'xlsx'
import { supabase } from '@/lib/supabase'
import { CarteiraCarga } from '@/types'
import { normalizeAgendam, normalizeDataDesDataNF, normalizeDle } from '@/lib/date-normalizers'

type DatasetColumnDef = {
  slot: string
  labels: string[]
  target?: string
  required?: boolean
  ignore?: boolean
}

const DATASET_LAYOUT_DEFINITIVO: DatasetColumnDef[] = [
  { slot: 'filial_romaneio', labels: ['Filial Romaneio'], target: 'filial_r', required: true },
  { slot: 'romaneio', labels: ['Romaneio'], target: 'romane', required: true },
  { slot: 'filial_doc', labels: ['Filial Doc'], target: 'filial_d', required: true },
  { slot: 'serie_doc', labels: ['Serie Doc', 'Série Doc'], target: 'serie', required: true },
  { slot: 'nro_doc', labels: ['Nro Doc'], target: 'nro_doc', required: true },
  { slot: 'data_desc', labels: ['Data Desc'], target: 'data_des', required: true },
  { slot: 'data_nf', labels: ['Data NF'], target: 'data_nf', required: true },
  { slot: 'dle', labels: ['D.L.E.'], target: 'dle', required: true },
  { slot: 'agendam', labels: ['Agendam.'], target: 'agendam', required: true },
  { slot: 'conf', labels: ['Conf'], target: 'conf', required: true },
  { slot: 'peso', labels: ['Peso'], target: 'peso', required: true },
  { slot: 'valor_merc', labels: ['Vlr.Merc.'], target: 'vlr_merc', required: true },
  { slot: 'qtd', labels: ['Qtd.'], target: 'qtd', required: true },
  { slot: 'peso_cub', labels: ['Peso Cub.'], target: 'peso_cubico', required: true },
  { slot: 'classificacao', labels: ['Classificação', 'Classificacao'], target: 'classif', required: true },
  { slot: 'tomador', labels: ['Tomador'], target: 'tomad', required: true },
  { slot: 'destinatario', labels: ['Destinatário', 'Destinatario'], target: 'destin', required: true },
  { slot: 'bairro', labels: ['Bairro'], target: 'bairro', required: true },
  { slot: 'cidade', labels: ['Cidade'], target: 'cidade', required: true },
  { slot: 'uf', labels: ['UF'], target: 'uf', required: true },
  { slot: 'nf_s', labels: ['NF/S'], target: 'nf_serie', required: true },
  { slot: 'tipo_carga_1', labels: ['Tipo Carga'], target: 'tipo_carga', required: true },
  { slot: 'qtd_nf', labels: ['Qtd.NF'], target: 'qtd_nf', required: true },
  { slot: 'mesoregiao', labels: ['Mesoregião', 'Mesoregiao'], target: 'mesoregiao', required: true },
  { slot: 'sub_regiao', labels: ['Sub-Região', 'Sub-Regiao'], target: 'sub_regiao', required: true },
  { slot: 'ocorrencias', labels: ['Ocorrências', 'Ocorrencias'], target: 'ocorrencias_nf', required: true },
  { slot: 'remetente', labels: ['Remetente'], target: 'remetente', required: true },
  { slot: 'observacao', labels: ['Observação', 'Observacao'], target: 'observacao', required: true },
  { slot: 'ref_cliente', labels: ['Ref Cliente'], target: 'ref_cliente', required: true },
  { slot: 'cidade_dest', labels: ['Cidade Dest.'], target: 'cidade_dest', required: true },
  { slot: 'agenda', labels: ['Agenda'], target: 'agenda', required: true },
  { slot: 'tipo_carga_2', labels: ['Tipo Carga'], target: 'tipo_ca', required: true },
  { slot: 'ultima_ocorrencia', labels: ['Ultima Ocorrencia', 'Última Ocorrência'], target: 'ultima_ocorrencia', required: true },
  { slot: 'latitude', labels: ['Latitude'], target: 'latitude', required: true },
  { slot: 'longitude', labels: ['Longitude'], target: 'longitude', required: true },
  { slot: 'peso_calculo', labels: ['Peso Calculo'], target: 'peso_calculo', required: true },
  { slot: 'prioridade', labels: ['Prioridade'], target: 'prioridade', required: true },
  { slot: 'restricao_veiculo', labels: ['Restrição Veiculo', 'Restrição Veículo'], target: 'restricao_veiculo', required: true },
  { slot: 'carro_dedicado', labels: ['Carro Dedicado'], target: 'carro_dedicado', required: true },
  { slot: 'restricao_horario', labels: ['Restrição Horario', 'Restrição Horário'], ignore: true, required: true },
  { slot: 'redespacho', labels: ['Redespacho'], target: 'redespacho_codigo', required: true },
]

const CAMPOS_NUMERICOS = new Set([
  'peso',
  'vlr_merc',
  'qtd',
  'peso_cubico',
  'qtd_nf',
  'latitude',
  'longitude',
  'peso_calculo',
])

const PLACEHOLDER_HEADER_RE = /^(__empty(?:_\d+)?)$/i

const isPlaceholderHeader = (value: unknown): boolean => {
  if (value == null) return true
  const text = String(value).replace(/[\u200B-\u200D\uFEFF]/g, '').trim()
  if (!text) return true
  if (PLACEHOLDER_HEADER_RE.test(text)) return true
  if (/^-+$/.test(text)) return true
  return false
}

export const normalizeForComparison = (value: unknown): string => String(value ?? '')
  .replace(/[\u200B-\u200D\uFEFF]/g, '')
  .normalize('NFD')
  .replace(/[\u0300-\u036f]/g, '')
  .replace(/[‐‑‒–—−]/g, '-')
  .replace(/[“”]/g, '"')
  .replace(/[‘’]/g, "'")
  .replace(/[\s\u00A0]+/g, ' ')
  .replace(/\s*([./,-])\s*/g, '$1')
  .trim()
  .toLowerCase()

const getValidHeaderCells = (row: unknown[]) => row
  .map((cell, index) => ({ cell, index }))
  .filter(({ cell }) => !isPlaceholderHeader(cell))

const isRowFullyEmpty = (row: Record<string, unknown>): boolean =>
  Object.values(row).every((value) => {
    if (value === null || value === undefined) return true
    return String(value).trim() === ''
  })

const isLinhaCarteiraSemConteudo = (row: Record<string, unknown>): boolean => {
  const campos = Object.entries(row).filter(([key]) => !key.startsWith('_') && key !== 'linha_numero')
  if (!campos.length) return true
  return campos.every(([, value]) => {
    if (value === null || value === undefined) return true
    return String(value).trim() === ''
  })
}

const parseNumeric = (value: unknown): number | null => {
  if (value === null || value === undefined) return null
  if (typeof value === 'number') {
    return Number.isFinite(value) ? value : null
  }
  const text = String(value).trim()
  if (!text) return null
  const sanitized = text.replace(/[^\d,.-]/g, '')
  if (!sanitized) return null

  const lastComma = sanitized.lastIndexOf(',')
  const lastDot = sanitized.lastIndexOf('.')

  let normalized = sanitized

  if (lastComma >= 0 && lastDot >= 0) {
    if (lastComma > lastDot) {
      normalized = sanitized.replace(/\./g, '').replace(',', '.')
    } else {
      normalized = sanitized.replace(/,/g, '')
    }
  } else if (lastComma >= 0) {
    const digitsAfterComma = sanitized.length - lastComma - 1
    const digitsBeforeComma = sanitized.slice(0, lastComma).replace('-', '').length
    if (digitsAfterComma === 3 && digitsBeforeComma > 3 && sanitized.indexOf(',') === lastComma) {
      normalized = sanitized.replace(',', '')
    } else {
      normalized = sanitized.replace(',', '.')
    }
  }

  if (!normalized) return null
  const numeric = Number(normalized)
  return Number.isFinite(numeric) ? numeric : null
}

const parseBoolean = (value: unknown): boolean | null => {
  if (value === null || value === undefined) return null
  const text = String(value).trim().toLowerCase()
  if (!text) return null
  if (['sim', 's', 'true', '1', 'yes', 'y'].includes(text)) return true
  if (['nao', 'não', 'n', 'false', '0', 'no'].includes(text)) return false
  return null
}

const REDESPACHO_PLACEHOLDERS = new Set([
  '',
  '-',
  '—',
  'null',
  'nan',
  'undefined',
  'na',
  'n/a',
  'sem redespacho',
  'sem-redespacho',
  'redespacho',
])

export const isCodigoRedespachoValido = (value: unknown): value is string => {
  if (typeof value !== 'string') return false
  const text = value.trim()
  if (!text) return false
  const normalized = normalizeForComparison(text)
  return !REDESPACHO_PLACEHOLDERS.has(normalized)
}

const normalizarCodigoRedespacho = (value: unknown): string | null => {
  if (value === null || value === undefined) return null

  const text = String(value).trim()
  if (!text) return null

  if (!isCodigoRedespachoValido(text)) return null

  return text
}

export function detectHeaderRow(rows: unknown[][]): number {
  const expected = DATASET_LAYOUT_DEFINITIVO.map((col) => normalizeForComparison(col.labels[0]))
  let bestIndex = -1
  let bestScore = 0

  const maxScan = Math.min(rows.length, 80)

  for (let rowIndex = 0; rowIndex < maxScan; rowIndex += 1) {
    const candidate = getValidHeaderCells(rows[rowIndex] ?? []).map(({ cell }) => normalizeForComparison(cell))
    if (candidate.length < expected.length) continue

    let cursor = 0
    let matched = 0

    for (const token of candidate) {
      if (token === expected[cursor]) {
        matched += 1
        cursor += 1
        if (cursor === expected.length) break
      }
    }

    const score = matched / expected.length
    if (score > bestScore) {
      bestScore = score
      bestIndex = rowIndex
    }

    if (matched === expected.length) return rowIndex
  }

  if (bestIndex >= 0 && bestScore >= 0.9) {
    return bestIndex
  }

  throw new Error('Não foi possível detectar uma linha de cabeçalho compatível com o layout esperado da carteira.')
}

const validarCabecalho = (headerRow: unknown[]): string[] => {
  const validHeaderCells = getValidHeaderCells(headerRow)
  if (validHeaderCells.length < DATASET_LAYOUT_DEFINITIVO.length) {
    throw new Error('Cabeçalho inválido: quantidade de colunas menor que o layout esperado.')
  }

  for (let i = 0; i < DATASET_LAYOUT_DEFINITIVO.length; i += 1) {
    const cell = validHeaderCells[i]?.cell
    const actual = normalizeForComparison(cell)
    const expectedLabels = DATASET_LAYOUT_DEFINITIVO[i].labels.map(normalizeForComparison)
    if (!expectedLabels.includes(actual)) {
      throw new Error(
        `Cabeçalho incompatível na posição lógica ${i + 1}. Esperado: [${DATASET_LAYOUT_DEFINITIVO[i].labels.join(', ')}]. Encontrado: "${String(cell ?? '')}".`,
      )
    }
  }

  return validHeaderCells.map(({ cell }) => String(cell ?? ''))
}



export interface UploadCarteiraHistorico {
  id: string
  nome_arquivo: string
  status: string
  created_at: string
  total_linhas_importadas: number
  total_linhas_validas: number
  total_colunas_detectadas: number
  linha_cabecalho_detectada: number | null
  colunas_detectadas_json: string[]
}

export interface ImportarCarteiraResult {
  uploadId: string
  nomeArquivo: string
  totalLinhas: number
  totalColunas: number
  linhaCabecalho: number
  colunasDetectadas: string[]
  preview: Array<Record<string, unknown>>
}

interface ImportarParams {
  file: File
  usuarioId: string
  filialId: string
}

export const carteiraUploadService = {
  async importarCarteiraExcel({ file, usuarioId, filialId }: ImportarParams): Promise<ImportarCarteiraResult> {
    const buffer = await file.arrayBuffer()
    const workbook = XLSX.read(buffer, { type: 'array', cellDates: false })
    const sheetName = workbook.SheetNames[0]

    if (!sheetName) {
      throw new Error('Arquivo sem aba de dados para importação.')
    }

    const worksheet = workbook.Sheets[sheetName]
    const rows = XLSX.utils.sheet_to_json(worksheet, {
      header: 1,
      raw: true,
      defval: null,
      blankrows: false,
    }) as unknown[][]

    if (!rows.length) {
      throw new Error('Planilha vazia: não há conteúdo para importar.')
    }

    const headerRowIndex = detectHeaderRow(rows)
    const headerRow = rows[headerRowIndex] ?? []
    const headerRaw = validarCabecalho(headerRow)
    const validHeaderCells = getValidHeaderCells(headerRow)

    let linhasIgnoradasCabecalhoRedespacho = 0

    const mappedRows = rows
      .slice(headerRowIndex + 1)
      .map((row, index) => {
        const rawObject: Record<string, unknown> = {}

        DATASET_LAYOUT_DEFINITIVO.forEach((slotDef, slotIndex) => {
          const cellIndex = validHeaderCells[slotIndex]?.index ?? slotIndex
          rawObject[slotDef.slot] = row[cellIndex] ?? null
        })

        const mapped: Record<string, unknown> = {
          linha_numero: headerRowIndex + index + 2,
          status_validacao: 'valida',
          dados_originais_json: rawObject,
        }

        const rawCellsBySlot: Record<string, { v: unknown; w: unknown } | null> = {}

        DATASET_LAYOUT_DEFINITIVO.forEach((slotDef, slotIndex) => {
          if (!slotDef.target) return
          const target = slotDef.target
          const rawValue = rawObject[slotDef.slot]
          const cellIndex = validHeaderCells[slotIndex]?.index ?? slotIndex
          const worksheetRowIndex = headerRowIndex + index + 1
          const cellRef = XLSX.utils.encode_cell({ c: cellIndex, r: worksheetRowIndex })
          const sheetCell = worksheet[cellRef]
          rawCellsBySlot[slotDef.slot] = sheetCell ? { v: sheetCell.v, w: sheetCell.w } : null

          if (CAMPOS_NUMERICOS.has(target)) {
            mapped[target] = parseNumeric(rawValue)
          } else if (target === 'carro_dedicado') {
            mapped[target] = parseBoolean(rawValue)
          } else {
            const text = rawValue == null ? null : String(rawValue).trim()
            mapped[target] = text ? text : null
          }
        })

        mapped.data_des = normalizeDataDesDataNF(mapped.data_des)
        mapped.data_nf = normalizeDataDesDataNF(mapped.data_nf)
        mapped.dle = normalizeDle(mapped.dle, { dataDes: mapped.data_des, dataNf: mapped.data_nf })
        mapped.agendam = normalizeAgendam(mapped.agendam)

        mapped.palet = null
        mapped.status_r = null
        mapped.inicio_entrega = null
        mapped.fim_entrega = null

        const redespachoTexto = normalizarCodigoRedespacho(rawObject.redespacho)
        mapped.redespacho_codigo = redespachoTexto || null
        mapped.redespacho_flag = Boolean(redespachoTexto)
        mapped.redespacho_transportadora_id = null
        mapped.redespacho_transportadora_nome = null

        mapped._date_diag = {
          slot_dle: rawObject.dle,
          target_dle: mapped.dle,
          row_dle: rawObject.dle,
          worksheet_dle_v: rawCellsBySlot.dle?.v ?? null,
          worksheet_dle_w: rawCellsBySlot.dle?.w ?? null,
        }

        return mapped
      })
      .filter((row) => {
        const redespachoMapped = normalizeForComparison(row.redespacho_codigo)
        const rawObject = (row.dados_originais_json ?? {}) as Record<string, unknown>
        const redespachoRaw = normalizeForComparison(rawObject.redespacho)
        if (redespachoMapped === 'redespacho' || redespachoRaw === 'redespacho') {
          linhasIgnoradasCabecalhoRedespacho += 1
          return false
        }

        const cleanRow = { ...row }
        delete cleanRow.linha_numero
        delete cleanRow.status_validacao
        delete cleanRow.dados_originais_json
        delete (cleanRow as any)._date_diag
        return !isRowFullyEmpty(cleanRow)
      })

    console.log('[UPLOAD DATE RAW DIAG]', mappedRows.slice(0, 10).map((row) => ({
      linha_numero: row.linha_numero,
      nro_doc: row.nro_doc,
      slot_dle: (row as any)._date_diag?.slot_dle ?? null,
      target_dle: (row as any)._date_diag?.target_dle ?? null,
      valor_row_dle: (row as any)._date_diag?.row_dle ?? null,
      worksheet_dle_v: (row as any)._date_diag?.worksheet_dle_v ?? null,
      worksheet_dle_w: (row as any)._date_diag?.worksheet_dle_w ?? null,
      data_des: row.data_des,
      data_nf: row.data_nf,
      agendam: row.agendam,
      conf: row.conf,
      peso_calculo: row.peso_calculo,
    })))

    mappedRows.forEach((row) => { delete (row as any)._date_diag })

    console.log('[UPLOAD REDESPACHO] linhas ignoradas por cabeçalho redespacho:', linhasIgnoradasCabecalhoRedespacho)
    const redespachosPreview = mappedRows
      .filter((row) => row.redespacho_flag === true
        && typeof row.redespacho_codigo === 'string'
        && row.redespacho_codigo.trim()
        && normalizarCodigoRedespacho(row.redespacho_codigo) !== null)
      .map((row) => ({
        linha_numero: row.linha_numero,
        nro_doc: row.nro_doc,
        destin: row.destin,
        cidade: row.cidade,
        redespacho_codigo: row.redespacho_codigo,
      }))

    console.log('[UPLOAD REDESPACHO] total linhas marcadas:', redespachosPreview.length)
    console.table(redespachosPreview.slice(0, 50))

    const totalLinhasImportadas = mappedRows.length
    const preview = mappedRows.slice(0, 5).map((item) => {
      const { dados_originais_json, ...rest } = item
      return rest
    })

    const { data: uploadData, error: uploadError } = await supabase
      .from('uploads_carteira')
      .insert({
        usuario_id: usuarioId,
        filial_id: filialId,
        nome_arquivo: file.name,
        nome_aba: sheetName,
        status: 'importado',
        total_linhas_brutas: rows.length,
        total_linhas_importadas: totalLinhasImportadas,
        total_linhas_validas: totalLinhasImportadas,
        total_linhas_invalidas: 0,
        total_colunas_detectadas: DATASET_LAYOUT_DEFINITIVO.length,
        linha_cabecalho_detectada: headerRowIndex + 1,
        colunas_detectadas_json: headerRaw,
        metadados_json: {
          parser: 'xlsx.utils.sheet_to_json(header:1)',
          uploaded_at: new Date().toISOString(),
        },
      })
      .select('id')
      .single()

    if (uploadError || !uploadData) {
      throw new Error(`Falha ao persistir upload: ${uploadError?.message || 'erro desconhecido'}`)
    }

    const uploadId = uploadData.id
    const batchSize = 500

    for (let start = 0; start < mappedRows.length; start += batchSize) {
      const batch = mappedRows.slice(start, start + batchSize).map((item) => ({
        upload_id: uploadId,
        ...item,
      }))

      if (!batch.length) continue

      const { error } = await supabase
        .from('carteira_itens')
        .insert(batch)

      if (error) {
        await supabase
          .from('uploads_carteira')
          .update({
            status: 'erro',
            erro_importacao: error.message,
          })
          .eq('id', uploadId)

        throw new Error(`Falha ao persistir itens da carteira: ${error.message}`)
      }
    }

    return {
      uploadId,
      nomeArquivo: file.name,
      totalLinhas: totalLinhasImportadas,
      totalColunas: DATASET_LAYOUT_DEFINITIVO.length,
      linhaCabecalho: headerRowIndex + 1,
      colunasDetectadas: DATASET_LAYOUT_DEFINITIVO.map((slot) => slot.labels[0]),
      preview,
    }
  },


  async listarUploadsRecentes(filialId?: string, limite = 10): Promise<UploadCarteiraHistorico[]> {
    let query = supabase
      .from('uploads_carteira')
      .select('id, nome_arquivo, status, created_at, total_linhas_importadas, total_linhas_validas, total_colunas_detectadas, linha_cabecalho_detectada, colunas_detectadas_json')
      .order('created_at', { ascending: false })
      .limit(limite)

    if (filialId) {
      query = query.eq('filial_id', filialId)
    }

    const { data, error } = await query
    if (error) throw error

    return (data ?? []) as UploadCarteiraHistorico[]
  },

  async buscarUpload(uploadId: string): Promise<UploadCarteiraHistorico> {
    const { data, error } = await supabase
      .from('uploads_carteira')
      .select('id, nome_arquivo, status, created_at, total_linhas_importadas, total_linhas_validas, total_colunas_detectadas, linha_cabecalho_detectada, colunas_detectadas_json')
      .eq('id', uploadId)
      .single()

    if (error || !data) {
      throw new Error(error?.message || 'Upload não encontrado')
    }

    return data as UploadCarteiraHistorico
  },

  async buscarPreviewUpload(uploadId: string, limite = 5): Promise<CarteiraCarga[]> {
    const limiteConsulta = Math.max(limite * 5, limite)
    const { data, error } = await supabase
      .from('carteira_itens')
      .select('*')
      .eq('upload_id', uploadId)
      .eq('status_validacao', 'valida')
      .order('linha_numero', { ascending: true })
      .limit(limiteConsulta)

    if (error) throw error

    return (data ?? [])
      .map((row) => {
        const { id, upload_id, status_validacao, erro_validacao, created_at, dados_originais_json, ...rest } = row
        return {
          ...rest,
          _carteira_item_id: id,
          _upload_id: upload_id,
          _status_validacao: status_validacao,
          _erro_validacao: erro_validacao,
          _created_at: created_at,
          _dados_originais: dados_originais_json,
        } as CarteiraCarga
      })
      .filter((row) => !isLinhaCarteiraSemConteudo(row as Record<string, unknown>))
      .slice(0, limite)
  },

  async excluirUpload(uploadId: string): Promise<void> {
    const { error: rodadasError } = await supabase
      .from('rodadas_roteirizacao')
      .delete()
      .eq('upload_id', uploadId)

    if (rodadasError) {
      throw new Error(`Falha ao remover rodadas vinculadas ao upload: ${rodadasError.message}`)
    }

    const { error: uploadError } = await supabase
      .from('uploads_carteira')
      .delete()
      .eq('id', uploadId)

    if (uploadError) {
      throw new Error(`Falha ao remover upload: ${uploadError.message}`)
    }
  },

  expectedRawColumns: DATASET_LAYOUT_DEFINITIVO.map((slot) => slot.labels[0]),
}

export type CarteiraImportada = CarteiraCarga
