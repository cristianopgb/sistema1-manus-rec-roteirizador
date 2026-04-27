// ============================================================
// TIPOS DO SISTEMA 1 — REC ROTEIRIZADOR
// ============================================================

// --- Usuário e Autenticação ---
export type UserRole = 'master' | 'roteirizador'

export interface UserProfile {
  id: string
  email: string
  nome: string
  perfil: UserRole
  filial_id: string | null
  filial_nome?: string
  ativo: boolean
  created_at: string
}

// --- Filial ---
export interface Filial {
  id: string
  codigo: string
  nome: string
  cidade: string
  uf: string
  cep?: string
  endereco?: string
  latitude: number
  longitude: number
  ativo: boolean
  created_at: string
}

// --- Veículo ---
export interface Veiculo {
  id: string
  filial_id: string
  filial_nome?: string
  codigo?: string | null
  placa?: string | null
  tipo: 'VUC' | '3/4' | 'TOCO' | 'TRUCK' | 'CARRETA' | 'BITRUCK'
  capacidade_peso_kg: number
  capacidade_volume_m3: number
  num_eixos: number
  max_km_distancia: number
  max_entregas: number
  ocupacao_minima_perc: number
  ocupacao_maxima_perc: number
  motorista?: string | null
  ativo: boolean
  created_at: string
}

// --- Tabela ANTT ---
export interface TabelaAntt {
  id: string
  codigo_tipo: number
  nome_tipo: string
  num_eixos: number
  coef_ccd: number  // R$/km
  coef_cc: number  // R$
  vigencia_inicio: string
  vigencia_fim?: string
  ativa: boolean
  created_at: string
  updated_at: string
}

// Tipos de carga ANTT
export const TIPOS_CARGA_ANTT: Record<number, string> = {
  1: 'Granel Sólido',
  2: 'Granel Líquido',
  3: 'Frigorificada ou Aquecida',
  4: 'Conteinerizada',
  5: 'Carga Geral',
  6: 'Neogranel',
  7: 'Perigosa (Granel Sólido)',
  8: 'Perigosa (Granel Líquido)',
  9: 'Perigosa (Frigorificada ou Aquecida)',
  10: 'Perigosa (Conteinerizada)',
  11: 'Perigosa (Carga Geral)',
  12: 'Carga Granel Pressurizada',
}

// Número de eixos válidos
export const EIXOS_VALIDOS = [2, 3, 4, 5, 6, 7, 9]

// --- Rodada de Roteirização ---
export type StatusRodada = 'processando' | 'sucesso' | 'erro' | 'parcial'

export interface RodadaRoteirizacao {
  id: string
  filial_id: string
  filial_nome?: string
  usuario_id: string
  upload_id?: string | null
  usuario_nome?: string
  status: StatusRodada
  tipo_roteirizacao: TipoRoteirizacao
  total_cargas_entrada: number
  total_manifestos: number
  total_itens_manifestados: number
  total_nao_roteirizados: number
  km_total_frota: number
  ocupacao_media_percentual: number
  tempo_processamento_ms: number
  payload_enviado?: Record<string, unknown>
  resposta_motor?: RespostaMotor
  created_at: string
  aprovada_em?: string
  aprovada_por?: string
}

// --- Tipos de Roteirização ---
export type TipoRoteirizacao = 'carteira' | 'frota'

export type CarroDedicadoFiltro = 'todos' | 'sim' | 'nao'

export interface FiltrosCarteira {
  filial_r: string[]
  uf: string[]
  destin: string[]
  cidade: string[]
  tomad: string[]
  mesoregiao: string[]
  prioridade: string[]
  restricao_veiculo: string[]
  carro_dedicado: CarroDedicadoFiltro
  agendam_de: string
  agendam_ate: string
  dle_de: string
  dle_ate: string
  data_des_de: string
  data_des_ate: string
  data_nf_de: string
  data_nf_ate: string
}

export interface ConfiguracaoFrotaItem {
  perfil: string
  quantidade: number
}

// --- Filtros de Roteirização ---
export interface FiltrosRoteirizacao {
  filial_id: string
  tipo_roteirizacao: TipoRoteirizacao
  data_base: string
  filtros_aplicados: FiltrosCarteira
  configuracao_frota: ConfiguracaoFrotaItem[]
}

// --- Payload para o Motor (Sistema 2) ---
export interface PayloadMotor {
  rodada_id: string
  upload_id: string
  usuario_id: string
  filial_id: string
  data_base_roteirizacao: string
  tipo_roteirizacao: TipoRoteirizacao
  filtros_aplicados: FiltrosCarteira
  configuracao_frota: ConfiguracaoFrotaItem[]
  veiculos: Array<{
    id: string
    filial_id: string
    tipo?: string | null
    perfil?: string | null
    placa?: string | null
    capacidade_peso_kg?: number | null
    capacidade_vol_m3?: number | null
    qtd_eixos?: number | null
    max_km_distancia?: number | null
    max_entregas?: number | null
    ocupacao_minima_perc?: number | null
    ocupacao_maxima_perc?: number | null
    ativo: boolean
  }>
  filial: {
    id: string
    nome: string
    cidade: string
    uf: string
    latitude: number
    longitude: number
  }
  parametros: {
    usuario_id: string
    usuario_nome: string
    filial_id: string
    filial_nome: string
    upload_id: string
    rodada_id: string
    data_execucao: string
    data_base_roteirizacao: string
    origem_sistema: 'sistema1'
    modelo_roteirizacao: 'roteirizador_rec'
    tipo_roteirizacao: TipoRoteirizacao
    filtros_aplicados: FiltrosCarteira
  }
  carteira: CarteiraCargaContratoMotor[]
}

export interface CarteiraCarga {
  [key: string]: unknown
}

export interface CarteiraCargaContratoMotor {
  [key: string]: unknown
  'Filial R': unknown
  Romane: unknown
  'Filial D': unknown
  'Série': unknown
  'Nro Doc.': unknown
  'Data Des': unknown
  'Data NF': unknown
  'D.L.E.': unknown
  'Agendam.': unknown
  Palet: unknown
  Conf: unknown
  Peso: unknown
  'Vlr.Merc.': unknown
  'Qtd.': unknown
  'Peso Cub.': unknown
  Classif: unknown
  Tomad: unknown
  Destin: unknown
  Bairro: unknown
  Cidad: unknown
  UF: unknown
  'NF / Serie': unknown
  'Tipo Ca': unknown
  'Qtd.NF': unknown
  Mesoregião: unknown
  'Sub-Região': unknown
  'Ocorrências NF': unknown
  Remetente: unknown
  Observação: unknown
  'Ref Cliente': unknown
  'Cidade Dest.': unknown
  Agenda: unknown
  'Tipo Carga': unknown
  'Última Ocorrência': unknown
  'Status R': unknown
  Latitude: unknown
  Longitude: unknown
  'Peso Calculo': unknown
  Prioridade: unknown
  'Restrição Veículo': unknown
  'Carro Dedicado': unknown
  'Inicio Ent.': unknown
  'Fim En': unknown
}

// --- Resposta do Motor (Sistema 2) ---
export interface RespostaMotor {
  status: 'ok' | 'sucesso' | 'erro' | 'parcial'
  mensagem?: string
  pipeline_real_ate?: string
  modo_resposta?: string
  resposta_truncada?: boolean
  erro?: { codigo: string; mensagem: string } | null
  resumo?: ResumoMotor
  resultado_roteirizacao?: Record<string, unknown>[]
  itens_manifestos?: Record<string, unknown>[]
  manifestos_fechados?: Record<string, unknown>[]
  manifestos_compostos?: Record<string, unknown>[]
  paradas_m7?: Record<string, unknown>[]
  manifestos_sequenciamento_resumo_m7?: Record<string, unknown>[]
  tentativas_sequenciamento_m7?: Record<string, unknown>[]
  diagnostico_recuperacao_coordenadas_m7?: Record<string, unknown>
  remanescentes?: Record<string, unknown>
  auditoria_serializacao?: Record<string, unknown>
  auditoria_m7?: Record<string, unknown>
  logs?: Record<string, unknown>[]
  logs_pipeline?: Record<string, unknown>[]
  resumo_execucao?: Record<string, unknown>
  resumo_negocio?: Record<string, unknown>
  contexto_rodada?: Record<string, unknown>
  encadeamento?: EtapaPipeline[]
  manifestos?: ManifestoMotor[]
  manifestos_m7?: ManifestoMotorEstruturado[]
  itens_manifestos_sequenciados_m7?: ItemManifestoEstruturado[]
  nao_roteirizados?: CargaNaoRoteirizada[]
  cargas_agendamento_futuro?: CargaNaoRoteirizada[]
  cargas_agenda_vencida?: CargaNaoRoteirizada[]
  cargas_excecao_triagem?: CargaNaoRoteirizada[]
  cargas_nao_alocadas?: CargaNaoRoteirizada[]
}

export interface ManifestoMotorEstruturado {
  manifesto_id?: string
  id_manifesto?: string
  origem_modulo?: string
  tipo_manifesto?: string
  veiculo_perfil?: string
  veiculo_tipo?: string
  veiculo_id?: string | null
  qtd_eixos?: number | null
  exclusivo_flag?: boolean
  peso_total?: number
  km_total?: number
  ocupacao?: number
  qtd_entregas?: number
  qtd_clientes?: number
  [key: string]: unknown
}

export interface ItemManifestoEstruturado {
  manifesto_id?: string
  id_manifesto?: string
  sequencia?: number
  nro_documento?: string
  destinatario?: string
  cidade?: string
  uf?: string
  peso?: number
  distancia_km?: number
  inicio_entrega?: string | null
  fim_entrega?: string | null
  latitude?: number | null
  longitude?: number | null
  [key: string]: unknown
}

export interface ResumoMotor {
  data_base_roteirizacao: string
  total_cargas_entrada: number
  total_manifestos: number
  total_itens_manifestados: number
  total_nao_roteirizados: number
  total_roteirizaveis: number
  total_agendamento_futuro: number
  total_agenda_vencida: number
  total_excecao_triagem: number
  km_total_frota: number
  ocupacao_media_percentual: number
  tempo_processamento_ms: number
}

export interface EtapaPipeline {
  etapa: string
  entrada: number
  saida_principal: number
  remanescente: number
  detalhes?: Record<string, unknown>
}

// Alias para compatibilidade com EncadeamentoPanel
export interface EtapaEncadeamento {
  etapa: string
  modulo?: string
  entrada: number
  saida_principal: number
  saida?: number
  remanescente: number
  status?: string
  detalhes?: Record<string, unknown>
}

export interface ManifestoMotor {
  id_manifesto: string
  numero_manifesto: number
  veiculo_tipo: string
  veiculo_codigo: string
  placa: string
  num_eixos: number | null
  capacidade_peso_kg: number
  motorista: string
  filial_origem: string
  linha_operacao: string
  regiao: string
  data_roteirizacao: string | null
  total_entregas: number
  total_peso_kg: number
  total_valor_mercadoria: number
  total_volumes: number
  km_estimado: number
  ocupacao_percentual: number
  frete_minimo_antt: number | null
  agendamentos: AgendamentoManifesto[]
  entregas: EntregaManifesto[]
}

export interface AgendamentoManifesto {
  nro_documento: string
  destinatario: string
  cidade: string
  uf: string
  data_agenda: string
  hora_agenda: string
  peso_kg: number
}

export interface EntregaManifesto {
  sequencia: number
  nro_documento: string
  lista_nfs: string[]
  doc_ctrc: string
  remetente: string
  destinatario: string
  cidade: string
  uf: string
  cep: string
  endereco: string
  tipo_carga: string
  peso_kg: number
  peso_bruto_kg: number
  qtd_volumes: number | null
  valor_mercadoria: number | null
  data_limite_entrega: string | null
  agendada: boolean
  data_agenda: string | null
  hora_agenda: string | null
  info_agendamento: string | null
  distancia_km: number
  status_folga: string
  folga_dias: number | null
  latitude_destinatario: number | null
  longitude_destinatario: number | null
}

export interface CargaNaoRoteirizada {
  nro_documento: string
  destinatario: string
  cidade: string
  uf: string
  peso_kg: number
  valor_mercadoria: number | null
  motivo: string
  status_triagem: string
  data_limite_entrega: string | null
  data_agenda: string | null
  agendada: boolean
  folga_dias: number | null
  status_folga: string
  mesorregiao: string
  sub_regiao: string
}

// --- Manifesto com Frete Calculado (para exibição) ---
export interface ManifestoComFrete extends ManifestoMotor {
  tipo_carga_antt?: string
  coef_ccd?: number
  coef_cc?: number
  coeficiente_deslocamento?: number
  coeficiente_carga_descarga?: number
  aprovado: boolean
  excluido: boolean
}

export interface ManifestoRoteirizacaoDetalhe {
  id: string
  rodada_id: string
  manifesto_id: string
  origem_modulo: string | null
  tipo_manifesto: string | null
  veiculo_perfil: string | null
  veiculo_tipo: string | null
  veiculo_id: string | null
  qtd_eixos: number | null
  exclusivo_flag: boolean
  peso_total: number
  km_total: number
  ocupacao: number
  qtd_entregas: number
  qtd_clientes: number
  frete_minimo: number
  created_at: string
  updated_at: string
}

export interface ManifestoItemRoteirizacao {
  id: string
  rodada_id: string
  manifesto_id: string
  sequencia: number
  nro_documento: string | null
  destinatario: string | null
  cidade: string | null
  uf: string | null
  peso: number | null
  distancia_km: number | null
  inicio_entrega: string | null
  fim_entrega: string | null
  latitude: number | null
  longitude: number | null
  created_at: string
  updated_at: string
}

export interface RemanescenteRoteirizacao {
  id: string
  rodada_id: string
  nro_documento: string | null
  destinatario: string | null
  cidade: string | null
  uf: string | null
  motivo: string | null
  etapa_origem: string | null
  created_at: string
}

export interface EstatisticasRoteirizacao {
  rodada_id: string
  total_carteira: number
  total_roteirizado: number
  total_remanescente: number
  total_manifestos: number
  km_total: number
  ocupacao_media: number
  tempo_execucao_ms: number
  created_at: string
  updated_at: string
}

// --- Estado da Tela de Roteirização ---
export type EtapaRoteirizacao =
  | 'upload'
  | 'preview'
  | 'configuracao'
  | 'processando'
  | 'resultado'

export interface EstadoRoteirizacao {
  etapa: EtapaRoteirizacao
  arquivo: File | null
  dadosArquivo: CarteiraCarga[]
  totalLinhas: number
  totalColunas: number
  colunas: string[]
  filtros: FiltrosRoteirizacao
  rodada_id: string | null
  resposta: RespostaMotor | null
  manifestos: ManifestoComFrete[]
  erro: string | null
}

// --- KPIs do Dashboard ---
export interface KpisDashboard {
  total_rodadas: number
  total_manifestos: number
  total_cargas_roteirizadas: number
  total_cargas_nao_roteirizadas: number
  km_total: number
  ocupacao_media: number
  taxa_roteirizacao: number
  rodadas_por_filial: { filial: string; total: number }[]
  rodadas_por_dia: { data: string; total: number }[]
  ocupacao_por_veiculo: { tipo: string; media: number }[]
}
