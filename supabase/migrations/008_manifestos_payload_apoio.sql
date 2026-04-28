-- Enriquecimento de payload para sinalização visual em manifestos e itens
ALTER TABLE public.manifestos_roteirizacao
  ADD COLUMN IF NOT EXISTS payload_apoio_json jsonb;

ALTER TABLE public.manifestos_itens
  ADD COLUMN IF NOT EXISTS payload_apoio_json jsonb;
