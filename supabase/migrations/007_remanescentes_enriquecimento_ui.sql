-- Enriquecimento aditivo da tabela de remanescentes para nova UI de aprovação
ALTER TABLE public.remanescentes_roteirizacao
  ADD COLUMN IF NOT EXISTS tipo_remanescente text,
  ADD COLUMN IF NOT EXISTS id_linha_pipeline text,
  ADD COLUMN IF NOT EXISTS peso_calculado numeric,
  ADD COLUMN IF NOT EXISTS distancia_rodoviaria_est_km numeric,
  ADD COLUMN IF NOT EXISTS mesorregiao text,
  ADD COLUMN IF NOT EXISTS subregiao text,
  ADD COLUMN IF NOT EXISTS corredor_30g text,
  ADD COLUMN IF NOT EXISTS corredor_30g_idx integer,
  ADD COLUMN IF NOT EXISTS status_triagem text,
  ADD COLUMN IF NOT EXISTS motivo_triagem text,
  ADD COLUMN IF NOT EXISTS motivo_detalhado_m6_2 text,
  ADD COLUMN IF NOT EXISTS motivo_final_remanescente_m6_2 text,
  ADD COLUMN IF NOT EXISTS motivo_final_remanescente_m5_4 text,
  ADD COLUMN IF NOT EXISTS motivo_final_remanescente_m5_3 text,
  ADD COLUMN IF NOT EXISTS payload_apoio_json jsonb;
