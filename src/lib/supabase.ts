import { createClient } from '@supabase/supabase-js'

const supabaseUrl = import.meta.env.VITE_SUPABASE_URL as string
const supabaseAnonKey = import.meta.env.VITE_SUPABASE_ANON_KEY as string

if (!supabaseUrl || !supabaseAnonKey) {
  console.error('⚠️  Variáveis de ambiente do Supabase não configuradas.')
  console.error('Crie um arquivo .env com VITE_SUPABASE_URL e VITE_SUPABASE_ANON_KEY')
}

export const supabase = createClient(supabaseUrl || '', supabaseAnonKey || '', {
  auth: {
    autoRefreshToken: true,
    persistSession: true,
    detectSessionInUrl: true,
  },
})

export type Database = {
  public: {
    Tables: {
      perfis_usuarios: {
        Row: {
          id: string
          email: string
          nome: string
          role: 'master' | 'roteirizador'
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
          tipo_carga_id: number
          tipo_carga_nome: string
          num_eixos: number
          coeficiente_deslocamento: number
          coeficiente_carga_descarga: number
          vigencia_inicio: string
          vigencia_fim: string | null
          ativo: boolean
          updated_at: string
          updated_by: string | null
        }
      }
      rodadas_roteirizacao: {
        Row: {
          id: string
          filial_id: string
          usuario_id: string
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
