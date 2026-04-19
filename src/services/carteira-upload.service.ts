import * as XLSX from 'xlsx'
import { supabase } from '@/lib/supabase'
import { CarteiraCarga } from '@/types'

const DATASET_COLUNAS_BRUTAS = [
  'Filial R',
  'Romanei',
  'Filial',
  'Série D',
  'Nro Do',
  'Data D',
  'Data N',
  'D.L.E.',
  'Agendam.',
  'Palet',
  'Conf',
  'Peso',
  'Vlr.Merc.',
  'Qtd.',
  'Peso Cub',
  'Classifica',
  'Tomad',
  'Destina',
  'Bairro',
  'Cida',
  'UF',
  'NF/Ser',
  'Tipo Carg',
  'Qtd.NF',
  'Mesoregião',
  'Sub-Região',
  'Ocorrências N',
  'Remetente',
  'Observação R',
  'Ref Cliente',
  'Cidade Dest.',
  'Agenda',
  'Tipo Carga',
  'Última Ocorrê',
  'Status Rom. O',
  'Latitude',
  'Longitude',
  'Peso Calculo',
  'Prioridade',
  'Restrição Veíc',
  'Carro Dedicado',
  'Inicio Ent.',
  'Fim En',
] as const

const MAPEAMENTO_FIXO: Record<(typeof DATASET_COLUNAS_BRUTAS)[number], string> = {
  'Filial R': 'filial_r',
  Romanei: 'romane',
  Filial: 'filial_d',
  'Série D': 'serie',
  'Nro Do': 'nro_doc',
  'Data D': 'data_des',
  'Data N': 'data_nf',
  'D.L.E.': 'dle',
  'Agendam.': 'agendam',
  Palet: 'palet',
  Conf: 'conf',
  Peso: 'peso',
  'Vlr.Merc.': 'vlr_merc',
  'Qtd.': 'qtd',
  'Peso Cub': 'peso_cubico',
  Classifica: 'classif',
  Tomad: 'tomad',
  Destina: 'destin',
  Bairro: 'bairro',
  Cida: 'cidade',
  UF: 'uf',
  'NF/Ser': 'nf_serie',
  'Tipo Carg': 'tipo_carga',
  'Qtd.NF': 'qtd_nf',
  Mesoregião: 'mesoregiao',
  'Sub-Região': 'sub_regiao',
  'Ocorrências N': 'ocorrencias_nf',
  Remetente: 'remetente',
  'Observação R': 'observacao',
  'Ref Cliente': 'ref_cliente',
  'Cidade Dest.': 'cidade_dest',
  Agenda: 'agenda',
  'Tipo Carga': 'tipo_ca',
  'Última Ocorrê': 'ultima_ocorrencia',
  'Status Rom. O': 'status_r',
  Latitude: 'latitude',
  Longitude: 'longitude',
  'Peso Calculo': 'peso_calculo',
  Prioridade: 'prioridade',
  'Restrição Veíc': 'restricao_veiculo',
  'Carro Dedicado': 'carro_dedicado',
  'Inicio Ent.': 'inicio_entrega',
  'Fim En': 'fim_entrega',
}

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

const removePlaceholderHeaders = (row: unknown[]): unknown[] => row.filter((cell) => !isPlaceholderHeader(cell))

const isRowFullyEmpty = (row: Record<string, unknown>): boolean =>
  Object.values(row).every((value) => {
    if (value === null || value === undefined) return true
    return String(value).trim() === ''
  })

const parseNumeric = (value: unknown): number | null => {
  if (value === null || value === undefined) return null
  const text = String(value).trim()
  if (!text) return null
  const normalized = text
    .replace(/\./g, '')
    .replace(',', '.')
    .replace(/[^\d.-]/g, '')
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

export function detectHeaderRow(rows: unknown[][]): number {
  const expected = DATASET_COLUNAS_BRUTAS.map(normalizeForComparison)
  let bestIndex = -1
  let bestScore = 0

  const maxScan = Math.min(rows.length, 80)

  for (let rowIndex = 0; rowIndex < maxScan; rowIndex += 1) {
    const candidate = removePlaceholderHeaders(rows[rowIndex] ?? []).map(normalizeForComparison)
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
  const cleaned = removePlaceholderHeaders(headerRow)
  const normalized = cleaned.map(normalizeForComparison)
  const expected = DATASET_COLUNAS_BRUTAS.map(normalizeForComparison)

  if (normalized.length < expected.length) {
    throw new Error('Cabeçalho inválido: quantidade de colunas menor que o layout esperado.')
  }

  for (let i = 0; i < expected.length; i += 1) {
    if (normalized[i] !== expected[i]) {
      throw new Error(`Cabeçalho incompatível na coluna ${i + 1}. Esperado "${DATASET_COLUNAS_BRUTAS[i]}".`)
    }
  }

  return DATASET_COLUNAS_BRUTAS.map((_, i) => String(cleaned[i] ?? DATASET_COLUNAS_BRUTAS[i]))
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
      raw: false,
      defval: null,
      blankrows: false,
    }) as unknown[][]

    if (!rows.length) {
      throw new Error('Planilha vazia: não há conteúdo para importar.')
    }

    const headerRowIndex = detectHeaderRow(rows)
    const headerRaw = validarCabecalho(rows[headerRowIndex] ?? [])

    const mappedRows = rows
      .slice(headerRowIndex + 1)
      .map((row, index) => {
        const rawObject: Record<string, unknown> = {}

        DATASET_COLUNAS_BRUTAS.forEach((coluna, colIndex) => {
          rawObject[coluna] = row[colIndex] ?? null
        })

        const mapped: Record<string, unknown> = {
          linha_numero: headerRowIndex + index + 2,
          status_validacao: 'valida',
          dados_originais_json: rawObject,
        }

        DATASET_COLUNAS_BRUTAS.forEach((coluna) => {
          const target = MAPEAMENTO_FIXO[coluna]
          const rawValue = rawObject[coluna]

          if (CAMPOS_NUMERICOS.has(target)) {
            mapped[target] = parseNumeric(rawValue)
          } else if (target === 'carro_dedicado') {
            mapped[target] = parseBoolean(rawValue)
          } else {
            const text = rawValue == null ? null : String(rawValue).trim()
            mapped[target] = text ? text : null
          }
        })

        return mapped
      })
      .filter((row) => {
        const cleanRow = { ...row }
        delete cleanRow.linha_numero
        delete cleanRow.status_validacao
        delete cleanRow.dados_originais_json
        return !isRowFullyEmpty(cleanRow)
      })

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
        total_colunas_detectadas: DATASET_COLUNAS_BRUTAS.length,
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
      totalColunas: DATASET_COLUNAS_BRUTAS.length,
      linhaCabecalho: headerRowIndex + 1,
      colunasDetectadas: [...DATASET_COLUNAS_BRUTAS],
      preview,
    }
  },

  expectedRawColumns: DATASET_COLUNAS_BRUTAS,
}

export type CarteiraImportada = CarteiraCarga
