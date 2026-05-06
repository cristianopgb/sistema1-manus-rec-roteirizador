-- Base de redespacho - Sistema 1

CREATE TABLE IF NOT EXISTS public.transportadoras_redespacho (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  filial_id uuid NULL REFERENCES public.filiais(id) ON DELETE SET NULL,
  codigo text NOT NULL,
  nome text NOT NULL,
  cnpj text NULL,
  ativo boolean NOT NULL DEFAULT true,
  observacao text NULL,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  CONSTRAINT transportadoras_redespacho_filial_codigo_key UNIQUE (filial_id, codigo)
);

CREATE INDEX IF NOT EXISTS idx_transportadoras_redespacho_filial_id ON public.transportadoras_redespacho(filial_id);
CREATE INDEX IF NOT EXISTS idx_transportadoras_redespacho_codigo ON public.transportadoras_redespacho(codigo);

DROP TRIGGER IF EXISTS trg_transportadoras_redespacho_updated_at ON public.transportadoras_redespacho;
CREATE TRIGGER trg_transportadoras_redespacho_updated_at
  BEFORE UPDATE ON public.transportadoras_redespacho
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

ALTER TABLE public.transportadoras_redespacho ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "transportadoras_redespacho_select" ON public.transportadoras_redespacho;
CREATE POLICY "transportadoras_redespacho_select" ON public.transportadoras_redespacho FOR SELECT
  USING (get_user_perfil() = 'master' OR filial_id IS NULL OR filial_id = get_user_filial_id());

DROP POLICY IF EXISTS "transportadoras_redespacho_insert" ON public.transportadoras_redespacho;
CREATE POLICY "transportadoras_redespacho_insert" ON public.transportadoras_redespacho FOR INSERT
  WITH CHECK (get_user_perfil() = 'master');

DROP POLICY IF EXISTS "transportadoras_redespacho_update" ON public.transportadoras_redespacho;
CREATE POLICY "transportadoras_redespacho_update" ON public.transportadoras_redespacho FOR UPDATE
  USING (get_user_perfil() = 'master');

DROP POLICY IF EXISTS "transportadoras_redespacho_delete" ON public.transportadoras_redespacho;
CREATE POLICY "transportadoras_redespacho_delete" ON public.transportadoras_redespacho FOR DELETE
  USING (get_user_perfil() = 'master');

ALTER TABLE public.carteira_itens
  ADD COLUMN IF NOT EXISTS redespacho_flag boolean NOT NULL DEFAULT false,
  ADD COLUMN IF NOT EXISTS redespacho_codigo text NULL,
  ADD COLUMN IF NOT EXISTS redespacho_transportadora_id uuid NULL REFERENCES public.transportadoras_redespacho(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS redespacho_transportadora_nome text NULL;

CREATE INDEX IF NOT EXISTS idx_carteira_itens_redespacho_codigo ON public.carteira_itens(redespacho_codigo);
