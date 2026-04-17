-- ============================================================
-- REC Roteirizador — Schema Completo
-- Migration: 001_schema_completo.sql
-- ============================================================

-- ─── EXTENSÕES ───────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─── ENUM: PERFIL DE USUÁRIO ─────────────────────────────────
CREATE TYPE perfil_usuario AS ENUM ('master', 'roteirizador');
CREATE TYPE status_rodada AS ENUM ('processando', 'sucesso', 'erro');
CREATE TYPE tipo_roteirizacao AS ENUM ('padrao', 'expressa', 'economica');

-- ============================================================
-- TABELA: filiais
-- ============================================================
CREATE TABLE filiais (
  id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  nome          TEXT NOT NULL,
  codigo        TEXT UNIQUE NOT NULL,
  cnpj          TEXT,
  cidade        TEXT,
  uf            CHAR(2),
  latitude      NUMERIC(10, 7),
  longitude     NUMERIC(10, 7),
  ativa         BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE filiais IS 'Filiais da REC Transportes';

-- ─── TRIGGER: updated_at automático ─────────────────────────
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_filiais_updated_at
  BEFORE UPDATE ON filiais
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- TABELA: usuarios_perfil
-- Extensão da tabela auth.users do Supabase
-- ============================================================
CREATE TABLE usuarios_perfil (
  id            UUID PRIMARY KEY REFERENCES auth.users(id) ON DELETE CASCADE,
  nome          TEXT NOT NULL,
  email         TEXT NOT NULL,
  perfil        perfil_usuario NOT NULL DEFAULT 'roteirizador',
  filial_id     UUID REFERENCES filiais(id) ON DELETE SET NULL,
  ativo         BOOLEAN NOT NULL DEFAULT TRUE,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE usuarios_perfil IS 'Perfil estendido dos usuários (Master e Roteirizador)';

CREATE TRIGGER trg_usuarios_perfil_updated_at
  BEFORE UPDATE ON usuarios_perfil
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- TABELA: veiculos
-- ============================================================
CREATE TABLE veiculos (
  id                        UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  filial_id                 UUID NOT NULL REFERENCES filiais(id) ON DELETE CASCADE,
  tipo                      TEXT NOT NULL,  -- VUC, 3/4, TOCO, TRUCK, CARRETA, etc.
  placa                     TEXT,
  motorista                 TEXT,
  capacidade_peso_kg        NUMERIC(10, 2) NOT NULL,
  capacidade_volume_m3      NUMERIC(10, 2),
  num_eixos                 INTEGER NOT NULL DEFAULT 2,
  max_km_distancia          NUMERIC(10, 2),
  max_entregas              INTEGER,
  ocupacao_minima_perc      NUMERIC(5, 2) NOT NULL DEFAULT 70.0,
  ocupacao_maxima_perc      NUMERIC(5, 2) NOT NULL DEFAULT 100.0,
  restricao_tipo_carga      TEXT[],  -- tipos de carga que este veículo NÃO aceita
  ativo                     BOOLEAN NOT NULL DEFAULT TRUE,
  created_at                TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE veiculos IS 'Frota de veículos por filial';

CREATE TRIGGER trg_veiculos_updated_at
  BEFORE UPDATE ON veiculos
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ============================================================
-- TABELA: tabela_antt
-- Tabela de frete mínimo ANTT por tipo de carga e número de eixos
-- ============================================================
CREATE TABLE tabela_antt (
  id                UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  codigo_tipo       INTEGER NOT NULL,  -- 1-12 conforme tabela ANTT
  nome_tipo         TEXT NOT NULL,     -- ex: "Granel sólido"
  num_eixos         INTEGER NOT NULL,  -- 2, 3, 4, 5, 6, 7, 9
  coef_ccd          NUMERIC(10, 4) NOT NULL,  -- R$/km (Deslocamento)
  coef_cc           NUMERIC(10, 2) NOT NULL,  -- R$ (Carga e Descarga)
  vigencia_inicio   DATE NOT NULL DEFAULT CURRENT_DATE,
  vigencia_fim      DATE,
  ativa             BOOLEAN NOT NULL DEFAULT TRUE,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (codigo_tipo, num_eixos, vigencia_inicio)
);

COMMENT ON TABLE tabela_antt IS 'Tabela de coeficientes de frete mínimo ANTT';
COMMENT ON COLUMN tabela_antt.coef_ccd IS 'Coeficiente de Custo de Deslocamento (R$/km)';
COMMENT ON COLUMN tabela_antt.coef_cc IS 'Coeficiente de Carga e Descarga (R$ fixo)';

CREATE TRIGGER trg_tabela_antt_updated_at
  BEFORE UPDATE ON tabela_antt
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─── SEED: Tabela ANTT vigente ───────────────────────────────
INSERT INTO tabela_antt (codigo_tipo, nome_tipo, num_eixos, coef_ccd, coef_cc) VALUES
-- Tipo 1: Granel sólido
(1,'Granel sólido',2,3.9173,444.84),(1,'Granel sólido',3,5.0127,533.36),
(1,'Granel sólido',4,5.6728,576.59),(1,'Granel sólido',5,6.5381,642.10),
(1,'Granel sólido',6,7.2108,656.76),(1,'Granel sólido',7,7.8555,792.30),
(1,'Granel sólido',9,8.9995,877.83),
-- Tipo 2: Granel líquido
(2,'Granel líquido',2,4.0780,462.64),(2,'Granel líquido',3,5.2218,556.79),
(2,'Granel líquido',4,5.9163,601.94),(2,'Granel líquido',5,6.7991,669.93),
(2,'Granel líquido',6,7.4858,685.34),(2,'Granel líquido',7,8.1493,824.14),
(2,'Granel líquido',9,9.3395,913.34),
-- Tipo 3: Frigorificada ou aquecida
(3,'Frigorificada ou aquecida',2,4.4737,507.60),(3,'Frigorificada ou aquecida',3,5.5861,611.22),
(3,'Frigorificada ou aquecida',4,6.3109,670.21),(3,'Frigorificada ou aquecida',5,7.1540,745.22),
(3,'Frigorificada ou aquecida',6,7.8218,760.63),(3,'Frigorificada ou aquecida',7,8.6088,918.53),
(3,'Frigorificada ou aquecida',9,9.7122,1009.38),
-- Tipo 4: Conteinerizada
(4,'Conteinerizada',2,4.5394,515.21),(4,'Conteinerizada',3,5.0885,556.79),
(4,'Conteinerizada',4,5.8285,601.94),(4,'Conteinerizada',5,6.3939,616.35),
(4,'Conteinerizada',6,7.0083,769.17),(4,'Conteinerizada',9,8.0140,826.64),
-- Tipo 5: Carga geral
(5,'Carga geral',2,3.8866,436.39),(5,'Carga geral',3,4.9762,523.33),
(5,'Carga geral',4,5.6443,568.72),(5,'Carga geral',5,6.5126,635.08),
(5,'Carga geral',6,7.1824,648.95),(5,'Carga geral',7,7.8952,803.22),
(5,'Carga geral',9,8.9799,872.44),
-- Tipo 6: Neogranel
(6,'Neogranel',2,3.5108,436.39),(6,'Neogranel',3,4.9748,522.93),
(6,'Neogranel',4,5.6706,575.96),(6,'Neogranel',5,6.5126,635.08),
(6,'Neogranel',6,7.1824,648.95),(6,'Neogranel',7,7.8952,803.22),
(6,'Neogranel',9,8.9799,872.44),
-- Tipo 7: Perigosa granel sólido
(7,'Perigosa (granel sólido)',2,4.6610,587.98),(7,'Perigosa (granel sólido)',3,5.7660,679.12),
(7,'Perigosa (granel sólido)',4,6.4616,727.28),(7,'Perigosa (granel sólido)',5,7.3269,792.80),
(7,'Perigosa (granel sólido)',6,7.9996,807.45),(7,'Perigosa (granel sólido)',7,8.6619,947.84),
(7,'Perigosa (granel sólido)',9,9.8137,1035.49),
-- Tipo 8: Perigosa granel líquido
(8,'Perigosa (granel líquido)',2,4.7446,610.96),(8,'Perigosa (granel líquido)',3,5.8704,707.85),
(8,'Perigosa (granel líquido)',4,6.5913,762.95),(8,'Perigosa (granel líquido)',5,7.4697,832.06),
(8,'Perigosa (granel líquido)',6,8.1475,848.13),(8,'Perigosa (granel líquido)',7,8.7763,979.29),
(8,'Perigosa (granel líquido)',9,9.9480,1072.44),
-- Tipo 9: Perigosa frigorificada
(9,'Perigosa (frigorificada)',2,5.1859,609.31),(9,'Perigosa (frigorificada)',3,6.4760,712.41),
(9,'Perigosa (frigorificada)',4,7.3202,780.02),(9,'Perigosa (frigorificada)',5,8.2992,848.93),
(9,'Perigosa (frigorificada)',6,9.0843,862.80),(9,'Perigosa (frigorificada)',7,9.9980,1072.32),
(9,'Perigosa (frigorificada)',9,11.2780,1156.49),
-- Tipo 10: Perigosa conteinerizada
(10,'Perigosa (conteinerizada)',2,5.3576,623.38),(10,'Perigosa (conteinerizada)',3,6.0099,659.60),
(10,'Perigosa (conteinerizada)',4,6.8832,727.35),(10,'Perigosa (conteinerizada)',5,7.5543,741.56),
(10,'Perigosa (conteinerizada)',6,8.2776,898.70),(10,'Perigosa (conteinerizada)',9,9.3513,964.90),
-- Tipo 11: Perigosa carga geral
(11,'Perigosa (carga geral)',2,4.2483,531.01),(11,'Perigosa (carga geral)',3,5.3474,620.58),
(11,'Perigosa (carga geral)',4,6.0510,670.91),(11,'Perigosa (carga geral)',5,6.9193,737.27),
(11,'Perigosa (carga geral)',6,7.5891,751.14),(11,'Perigosa (carga geral)',7,8.3196,910.26),
(11,'Perigosa (carga geral)',9,9.4120,981.58),
-- Tipo 12: Carga Granel Pressurizada
(12,'Carga Granel Pressurizada',2,6.8646,731.90),(12,'Carga Granel Pressurizada',3,7.5789,757.99),
(12,'Carga Granel Pressurizada',9,9.5030,1016.29);

-- ============================================================
-- TABELA: rodadas_roteirizacao
-- Ledger de todas as execuções do Motor
-- ============================================================
CREATE TABLE rodadas_roteirizacao (
  id                          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  filial_id                   UUID NOT NULL REFERENCES filiais(id),
  filial_nome                 TEXT,
  usuario_id                  UUID NOT NULL REFERENCES auth.users(id),
  usuario_nome                TEXT,
  tipo_roteirizacao           tipo_roteirizacao NOT NULL DEFAULT 'padrao',
  data_base_roteirizacao      TIMESTAMPTZ NOT NULL,
  status                      status_rodada NOT NULL DEFAULT 'processando',
  -- Métricas de entrada
  total_cargas_entrada        INTEGER,
  -- Métricas de saída
  total_manifestos            INTEGER,
  total_itens_manifestados    INTEGER,
  total_nao_roteirizados      INTEGER,
  km_total_frota              NUMERIC(12, 2),
  ocupacao_media_percentual   NUMERIC(5, 2),
  tempo_processamento_ms      INTEGER,
  -- Payloads completos (ledger)
  payload_enviado             JSONB,  -- o que foi enviado ao motor
  resposta_motor              JSONB,  -- resposta bruta do motor
  manifestos_aprovados        JSONB,  -- manifestos após ajustes do usuário
  -- Erro
  erro_mensagem               TEXT,
  created_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at                  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE rodadas_roteirizacao IS 'Ledger de todas as execuções de roteirização';
COMMENT ON COLUMN rodadas_roteirizacao.payload_enviado IS 'Payload completo enviado ao Motor Python';
COMMENT ON COLUMN rodadas_roteirizacao.resposta_motor IS 'Resposta bruta do Motor Python';
COMMENT ON COLUMN rodadas_roteirizacao.manifestos_aprovados IS 'Manifestos após ajustes e aprovação do usuário';

CREATE TRIGGER trg_rodadas_updated_at
  BEFORE UPDATE ON rodadas_roteirizacao
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ─── ÍNDICES ─────────────────────────────────────────────────
CREATE INDEX idx_rodadas_filial_id ON rodadas_roteirizacao(filial_id);
CREATE INDEX idx_rodadas_usuario_id ON rodadas_roteirizacao(usuario_id);
CREATE INDEX idx_rodadas_created_at ON rodadas_roteirizacao(created_at DESC);
CREATE INDEX idx_rodadas_status ON rodadas_roteirizacao(status);
CREATE INDEX idx_veiculos_filial_id ON veiculos(filial_id);
CREATE INDEX idx_usuarios_perfil_filial_id ON usuarios_perfil(filial_id);
CREATE INDEX idx_tabela_antt_tipo_eixos ON tabela_antt(codigo_tipo, num_eixos);

-- ============================================================
-- ROW LEVEL SECURITY (RLS)
-- ============================================================
ALTER TABLE filiais ENABLE ROW LEVEL SECURITY;
ALTER TABLE usuarios_perfil ENABLE ROW LEVEL SECURITY;
ALTER TABLE veiculos ENABLE ROW LEVEL SECURITY;
ALTER TABLE tabela_antt ENABLE ROW LEVEL SECURITY;
ALTER TABLE rodadas_roteirizacao ENABLE ROW LEVEL SECURITY;

-- ─── FUNÇÃO AUXILIAR: obter perfil do usuário logado ────────
CREATE OR REPLACE FUNCTION get_user_perfil()
RETURNS perfil_usuario AS $$
  SELECT perfil FROM usuarios_perfil WHERE id = auth.uid();
$$ LANGUAGE sql SECURITY DEFINER STABLE;

CREATE OR REPLACE FUNCTION get_user_filial_id()
RETURNS UUID AS $$
  SELECT filial_id FROM usuarios_perfil WHERE id = auth.uid();
$$ LANGUAGE sql SECURITY DEFINER STABLE;

-- ─── POLICIES: filiais ───────────────────────────────────────
-- Master vê todas; Roteirizador vê apenas a sua
CREATE POLICY "filiais_select" ON filiais FOR SELECT
  USING (
    get_user_perfil() = 'master'
    OR id = get_user_filial_id()
  );

CREATE POLICY "filiais_insert" ON filiais FOR INSERT
  WITH CHECK (get_user_perfil() = 'master');

CREATE POLICY "filiais_update" ON filiais FOR UPDATE
  USING (get_user_perfil() = 'master');

CREATE POLICY "filiais_delete" ON filiais FOR DELETE
  USING (get_user_perfil() = 'master');

-- ─── POLICIES: usuarios_perfil ───────────────────────────────
-- Master vê todos; Roteirizador vê apenas o próprio
CREATE POLICY "usuarios_select" ON usuarios_perfil FOR SELECT
  USING (
    get_user_perfil() = 'master'
    OR id = auth.uid()
  );

CREATE POLICY "usuarios_insert" ON usuarios_perfil FOR INSERT
  WITH CHECK (get_user_perfil() = 'master');

CREATE POLICY "usuarios_update" ON usuarios_perfil FOR UPDATE
  USING (
    get_user_perfil() = 'master'
    OR id = auth.uid()
  );

CREATE POLICY "usuarios_delete" ON usuarios_perfil FOR DELETE
  USING (get_user_perfil() = 'master');

-- ─── POLICIES: veiculos ──────────────────────────────────────
-- Master vê todos; Roteirizador vê apenas da sua filial
CREATE POLICY "veiculos_select" ON veiculos FOR SELECT
  USING (
    get_user_perfil() = 'master'
    OR filial_id = get_user_filial_id()
  );

CREATE POLICY "veiculos_insert" ON veiculos FOR INSERT
  WITH CHECK (get_user_perfil() = 'master');

CREATE POLICY "veiculos_update" ON veiculos FOR UPDATE
  USING (get_user_perfil() = 'master');

CREATE POLICY "veiculos_delete" ON veiculos FOR DELETE
  USING (get_user_perfil() = 'master');

-- ─── POLICIES: tabela_antt ───────────────────────────────────
-- Todos leem; apenas Master escreve
CREATE POLICY "antt_select" ON tabela_antt FOR SELECT
  USING (TRUE);

CREATE POLICY "antt_insert" ON tabela_antt FOR INSERT
  WITH CHECK (get_user_perfil() = 'master');

CREATE POLICY "antt_update" ON tabela_antt FOR UPDATE
  USING (get_user_perfil() = 'master');

CREATE POLICY "antt_delete" ON tabela_antt FOR DELETE
  USING (get_user_perfil() = 'master');

-- ─── POLICIES: rodadas_roteirizacao ──────────────────────────
-- Master vê todas; Roteirizador vê apenas da sua filial
CREATE POLICY "rodadas_select" ON rodadas_roteirizacao FOR SELECT
  USING (
    get_user_perfil() = 'master'
    OR filial_id = get_user_filial_id()
  );

CREATE POLICY "rodadas_insert" ON rodadas_roteirizacao FOR INSERT
  WITH CHECK (
    get_user_perfil() = 'master'
    OR filial_id = get_user_filial_id()
  );

CREATE POLICY "rodadas_update" ON rodadas_roteirizacao FOR UPDATE
  USING (
    get_user_perfil() = 'master'
    OR (filial_id = get_user_filial_id() AND usuario_id = auth.uid())
  );

-- ─── TRIGGER: auto-criar perfil ao registrar usuário ────────
CREATE OR REPLACE FUNCTION handle_new_user()
RETURNS TRIGGER AS $$
BEGIN
  INSERT INTO usuarios_perfil (id, nome, email, perfil)
  VALUES (
    NEW.id,
    COALESCE(NEW.raw_user_meta_data->>'nome', NEW.email),
    NEW.email,
    COALESCE((NEW.raw_user_meta_data->>'perfil')::perfil_usuario, 'roteirizador')
  );
  RETURN NEW;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;

CREATE TRIGGER on_auth_user_created
  AFTER INSERT ON auth.users
  FOR EACH ROW EXECUTE FUNCTION handle_new_user();
