-- ============================================================
-- Adiciona vínculo de rodada com upload de carteira
-- Migration: 003_rodadas_upload_id.sql
-- ============================================================

ALTER TABLE public.rodadas_roteirizacao
  ADD COLUMN IF NOT EXISTS upload_id uuid REFERENCES public.uploads_carteira(id);

CREATE INDEX IF NOT EXISTS idx_rodadas_upload_id ON public.rodadas_roteirizacao(upload_id);
