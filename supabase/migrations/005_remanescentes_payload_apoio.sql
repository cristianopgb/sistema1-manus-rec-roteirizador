-- Campos adicionais para suportar contrato completo de remanescentes do Sistema 2
ALTER TABLE public.remanescentes_roteirizacao
  ADD COLUMN IF NOT EXISTS grupo_remanescente text,
  ADD COLUMN IF NOT EXISTS payload_apoio_json jsonb;

CREATE INDEX IF NOT EXISTS idx_remanescentes_roteirizacao_grupo
  ON public.remanescentes_roteirizacao(grupo_remanescente);
