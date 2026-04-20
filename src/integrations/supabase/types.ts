export type Json = string | number | boolean | null | { [key: string]: Json | undefined } | Json[]

export type Database = {
  public: {
    Tables: {
      usuarios_perfil: {
        Row: {
          id: string
          email: string
          nome: string
          perfil: 'master' | 'roteirizador'
          filial_id: string | null
          ativo: boolean
          created_at: string
        }
      }
      filiais: {
        Row: {
          id: string
          codigo: string
          nome: string
          cidade: string
          uf: string
          cep: string | null
          endereco: string | null
          latitude: number
          longitude: number
          ativo: boolean
          created_at: string
        }
      }
      veiculos: {
        Row: {
          id: string
          filial_id: string
          codigo: string
          placa: string
          tipo: string
          capacidade_peso_kg: number
          capacidade_volume_m3: number
          num_eixos: number
          max_km_distancia: number
          max_entregas: number
          ocupacao_minima_perc: number
          ocupacao_maxima_perc: number
          motorista: string | null
          ativo: boolean
          created_at: string
        }
      }
      tabela_antt: {
        Row: {
          id: string
          codigo_tipo: number
          nome_tipo: string
          num_eixos: number
          coef_ccd: number
          coef_cc: number
          vigencia_inicio: string
          vigencia_fim: string | null
          ativa: boolean
          created_at: string
          updated_at: string
        }
      }
      uploads_carteira: {
        Row: {
          id: string
          usuario_id: string
          filial_id: string
          nome_arquivo: string
          nome_aba: string | null
          status: string
          total_linhas_brutas: number
          total_linhas_importadas: number
          total_linhas_validas: number
          total_linhas_invalidas: number
          total_colunas_detectadas: number
          linha_cabecalho_detectada: number | null
          colunas_detectadas_json: Json
          metadados_json: Json
          observacoes_importacao: string | null
          erro_importacao: string | null
          created_at: string
          updated_at: string
        }
      }
      carteira_itens: {
        Row: {
          id: string
          upload_id: string
          linha_numero: number
          status_validacao: string
          erro_validacao: string | null
          filial_r: string | null
          romane: string | null
          filial_d: string | null
          serie: string | null
          nro_doc: string | null
          data_des: string | null
          data_nf: string | null
          dle: string | null
          agendam: string | null
          palet: string | null
          conf: string | null
          peso: number | null
          vlr_merc: number | null
          qtd: number | null
          peso_cubico: number | null
          classif: string | null
          tomad: string | null
          destin: string | null
          bairro: string | null
          cidade: string | null
          uf: string | null
          nf_serie: string | null
          tipo_carga: string | null
          qtd_nf: number | null
          mesoregiao: string | null
          sub_regiao: string | null
          ocorrencias_nf: string | null
          remetente: string | null
          observacao: string | null
          ref_cliente: string | null
          cidade_dest: string | null
          agenda: string | null
          tipo_ca: string | null
          ultima_ocorrencia: string | null
          status_r: string | null
          latitude: number | null
          longitude: number | null
          peso_calculo: number | null
          prioridade: string | null
          restricao_veiculo: string | null
          carro_dedicado: boolean | null
          inicio_entrega: string | null
          fim_entrega: string | null
          dados_originais_json: Json
          created_at: string
        }
      }
      rodadas_roteirizacao: {
        Row: {
          id: string
          filial_id: string
          usuario_id: string
          upload_id: string | null
          status: string
          tipo_roteirizacao: string
          total_cargas_entrada: number
          total_manifestos: number
          total_itens_manifestados: number
          total_nao_roteirizados: number
          km_total_frota: number
          ocupacao_media_percentual: number
          tempo_processamento_ms: number
          payload_enviado: Record<string, unknown> | null
          resposta_motor: Record<string, unknown> | null
          created_at: string
          aprovada_em: string | null
          aprovada_por: string | null
        }
      }
    }
  }
}
