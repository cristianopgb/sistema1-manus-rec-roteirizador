-- ============================================================
-- Estrutura operacional para aprovação da roteirização
-- Migration: 004_aprovacao_roteirizacao_estruturada.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS public.manifestos_roteirizacao (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rodada_id uuid NOT NULL REFERENCES public.rodadas_roteirizacao(id) ON DELETE CASCADE,
  manifesto_id text NOT NULL,
  origem_modulo text,
  tipo_manifesto text,
  veiculo_perfil text,
  veiculo_tipo text,
  veiculo_id uuid NULL REFERENCES public.veiculos(id) ON DELETE SET NULL,
  qtd_eixos integer,
  exclusivo_flag boolean NOT NULL DEFAULT false,
  peso_total numeric(14, 3) NOT NULL DEFAULT 0,
  km_total numeric(14, 3) NOT NULL DEFAULT 0,
  ocupacao numeric(8, 3) NOT NULL DEFAULT 0,
  qtd_entregas integer NOT NULL DEFAULT 0,
  qtd_clientes integer NOT NULL DEFAULT 0,
  frete_minimo numeric(14, 2) NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (rodada_id, manifesto_id)
);

CREATE INDEX IF NOT EXISTS idx_manifestos_roteirizacao_rodada_id ON public.manifestos_roteirizacao(rodada_id);
CREATE INDEX IF NOT EXISTS idx_manifestos_roteirizacao_veiculo_id ON public.manifestos_roteirizacao(veiculo_id);

DROP TRIGGER IF EXISTS trg_manifestos_roteirizacao_updated_at ON public.manifestos_roteirizacao;
CREATE TRIGGER trg_manifestos_roteirizacao_updated_at
  BEFORE UPDATE ON public.manifestos_roteirizacao
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.manifestos_itens (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rodada_id uuid NOT NULL REFERENCES public.rodadas_roteirizacao(id) ON DELETE CASCADE,
  manifesto_id text NOT NULL,
  sequencia integer NOT NULL,
  nro_documento text,
  destinatario text,
  cidade text,
  uf text,
  peso numeric(14, 3),
  distancia_km numeric(14, 3),
  inicio_entrega text,
  fim_entrega text,
  latitude numeric(12, 8),
  longitude numeric(12, 8),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (rodada_id, manifesto_id, sequencia)
);

CREATE INDEX IF NOT EXISTS idx_manifestos_itens_rodada_manifesto ON public.manifestos_itens(rodada_id, manifesto_id);
CREATE INDEX IF NOT EXISTS idx_manifestos_itens_documento ON public.manifestos_itens(nro_documento);

DROP TRIGGER IF EXISTS trg_manifestos_itens_updated_at ON public.manifestos_itens;
CREATE TRIGGER trg_manifestos_itens_updated_at
  BEFORE UPDATE ON public.manifestos_itens
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.remanescentes_roteirizacao (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rodada_id uuid NOT NULL REFERENCES public.rodadas_roteirizacao(id) ON DELETE CASCADE,
  nro_documento text,
  destinatario text,
  cidade text,
  uf text,
  motivo text,
  etapa_origem text,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_remanescentes_roteirizacao_rodada_id ON public.remanescentes_roteirizacao(rodada_id);

CREATE TABLE IF NOT EXISTS public.estatisticas_roteirizacao (
  rodada_id uuid PRIMARY KEY REFERENCES public.rodadas_roteirizacao(id) ON DELETE CASCADE,
  total_carteira integer NOT NULL DEFAULT 0,
  total_roteirizado integer NOT NULL DEFAULT 0,
  total_remanescente integer NOT NULL DEFAULT 0,
  total_manifestos integer NOT NULL DEFAULT 0,
  km_total numeric(14, 3) NOT NULL DEFAULT 0,
  ocupacao_media numeric(8, 3) NOT NULL DEFAULT 0,
  tempo_execucao_ms integer NOT NULL DEFAULT 0,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

DROP TRIGGER IF EXISTS trg_estatisticas_roteirizacao_updated_at ON public.estatisticas_roteirizacao;
CREATE TRIGGER trg_estatisticas_roteirizacao_updated_at
  BEFORE UPDATE ON public.estatisticas_roteirizacao
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE public.manifestos_roteirizacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.manifestos_itens ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.remanescentes_roteirizacao ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.estatisticas_roteirizacao ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "manifestos_roteirizacao_select" ON public.manifestos_roteirizacao;
CREATE POLICY "manifestos_roteirizacao_select" ON public.manifestos_roteirizacao FOR SELECT
  USING (
    EXISTS (
      SELECT 1
      FROM public.rodadas_roteirizacao rr
      WHERE rr.id = manifestos_roteirizacao.rodada_id
      AND (get_user_perfil() = 'master' OR rr.filial_id = get_user_filial_id())
    )
  );

DROP POLICY IF EXISTS "manifestos_roteirizacao_modify" ON public.manifestos_roteirizacao;
CREATE POLICY "manifestos_roteirizacao_modify" ON public.manifestos_roteirizacao FOR ALL
  USING (TRUE)
  WITH CHECK (TRUE);

DROP POLICY IF EXISTS "manifestos_itens_select" ON public.manifestos_itens;
CREATE POLICY "manifestos_itens_select" ON public.manifestos_itens FOR SELECT
  USING (
    EXISTS (
      SELECT 1
      FROM public.rodadas_roteirizacao rr
      WHERE rr.id = manifestos_itens.rodada_id
      AND (get_user_perfil() = 'master' OR rr.filial_id = get_user_filial_id())
    )
  );

DROP POLICY IF EXISTS "manifestos_itens_modify" ON public.manifestos_itens;
CREATE POLICY "manifestos_itens_modify" ON public.manifestos_itens FOR ALL
  USING (TRUE)
  WITH CHECK (TRUE);

DROP POLICY IF EXISTS "remanescentes_roteirizacao_select" ON public.remanescentes_roteirizacao;
CREATE POLICY "remanescentes_roteirizacao_select" ON public.remanescentes_roteirizacao FOR SELECT
  USING (
    EXISTS (
      SELECT 1
      FROM public.rodadas_roteirizacao rr
      WHERE rr.id = remanescentes_roteirizacao.rodada_id
      AND (get_user_perfil() = 'master' OR rr.filial_id = get_user_filial_id())
    )
  );

DROP POLICY IF EXISTS "remanescentes_roteirizacao_modify" ON public.remanescentes_roteirizacao;
CREATE POLICY "remanescentes_roteirizacao_modify" ON public.remanescentes_roteirizacao FOR ALL
  USING (TRUE)
  WITH CHECK (TRUE);

DROP POLICY IF EXISTS "estatisticas_roteirizacao_select" ON public.estatisticas_roteirizacao;
CREATE POLICY "estatisticas_roteirizacao_select" ON public.estatisticas_roteirizacao FOR SELECT
  USING (
    EXISTS (
      SELECT 1
      FROM public.rodadas_roteirizacao rr
      WHERE rr.id = estatisticas_roteirizacao.rodada_id
      AND (get_user_perfil() = 'master' OR rr.filial_id = get_user_filial_id())
    )
  );

DROP POLICY IF EXISTS "estatisticas_roteirizacao_modify" ON public.estatisticas_roteirizacao;
CREATE POLICY "estatisticas_roteirizacao_modify" ON public.estatisticas_roteirizacao FOR ALL
  USING (TRUE)
  WITH CHECK (TRUE);
