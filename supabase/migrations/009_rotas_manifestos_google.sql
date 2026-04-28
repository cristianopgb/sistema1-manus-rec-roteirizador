-- ============================================================
-- Rotas Google por manifesto
-- Migration: 009_rotas_manifestos_google.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS public.rotas_manifestos_google (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  rodada_id uuid NOT NULL REFERENCES public.rodadas_roteirizacao(id) ON DELETE CASCADE,
  manifesto_id text NOT NULL,
  manifesto_db_id uuid NULL REFERENCES public.manifestos_roteirizacao(id) ON DELETE SET NULL,
  rota_hash text NOT NULL,
  origem_latitude numeric(12,8) NOT NULL,
  origem_longitude numeric(12,8) NOT NULL,
  destino_latitude numeric(12,8) NULL,
  destino_longitude numeric(12,8) NULL,
  paradas_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  qtd_paradas integer NOT NULL DEFAULT 0,
  km_estimado_motor numeric(12,3) NULL,
  distancia_metros_google integer NULL,
  km_google_maps numeric(12,3) NULL,
  duracao_segundos_google integer NULL,
  polyline_google text NULL,
  google_status text NOT NULL DEFAULT 'pendente',
  google_erro text NULL,
  request_json jsonb NULL,
  response_json jsonb NULL,
  fonte text NOT NULL DEFAULT 'google_routes_api',
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT rotas_manifestos_google_status_chk CHECK (
    google_status IN (
      'pendente',
      'processando',
      'ok',
      'erro',
      'sem_coordenadas',
      'sem_paradas',
      'excede_limite_waypoints',
      'reutilizada'
    )
  ),
  CONSTRAINT rotas_manifestos_google_unq_manifesto UNIQUE (rodada_id, manifesto_id)
);

CREATE INDEX IF NOT EXISTS idx_rotas_manifestos_google_rodada_id ON public.rotas_manifestos_google(rodada_id);
CREATE INDEX IF NOT EXISTS idx_rotas_manifestos_google_manifesto_id ON public.rotas_manifestos_google(manifesto_id);
CREATE INDEX IF NOT EXISTS idx_rotas_manifestos_google_rota_hash ON public.rotas_manifestos_google(rota_hash);
CREATE INDEX IF NOT EXISTS idx_rotas_manifestos_google_google_status ON public.rotas_manifestos_google(google_status);

DROP TRIGGER IF EXISTS trg_rotas_manifestos_google_updated_at ON public.rotas_manifestos_google;
CREATE TRIGGER trg_rotas_manifestos_google_updated_at
  BEFORE UPDATE ON public.rotas_manifestos_google
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE public.rotas_manifestos_google ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "rotas_manifestos_google_select" ON public.rotas_manifestos_google;
CREATE POLICY "rotas_manifestos_google_select" ON public.rotas_manifestos_google FOR SELECT
  USING (
    EXISTS (
      SELECT 1
      FROM public.rodadas_roteirizacao rr
      WHERE rr.id = rotas_manifestos_google.rodada_id
      AND (get_user_perfil() = 'master' OR rr.filial_id = get_user_filial_id())
    )
  );

DROP POLICY IF EXISTS "rotas_manifestos_google_modify" ON public.rotas_manifestos_google;
CREATE POLICY "rotas_manifestos_google_modify" ON public.rotas_manifestos_google FOR ALL
  USING (TRUE)
  WITH CHECK (TRUE);
