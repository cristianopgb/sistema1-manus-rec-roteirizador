ALTER TABLE public.manifestos_roteirizacao
  ADD COLUMN IF NOT EXISTS frete_minimo_valor numeric(14,2) NULL,
  ADD COLUMN IF NOT EXISTS km_frete numeric(12,3) NULL,
  ADD COLUMN IF NOT EXISTS fonte_km_frete text NULL,
  ADD COLUMN IF NOT EXISTS frete_status text NOT NULL DEFAULT 'pendente',
  ADD COLUMN IF NOT EXISTS frete_erro text NULL,
  ADD COLUMN IF NOT EXISTS frete_calculado_em timestamptz NULL,
  ADD COLUMN IF NOT EXISTS rota_google_id uuid NULL REFERENCES public.rotas_manifestos_google(id) ON DELETE SET NULL,
  ADD COLUMN IF NOT EXISTS frete_minimo_detalhes_json jsonb NULL;

ALTER TABLE public.manifestos_roteirizacao
  DROP CONSTRAINT IF EXISTS manifestos_roteirizacao_frete_status_chk;

ALTER TABLE public.manifestos_roteirizacao
  ADD CONSTRAINT manifestos_roteirizacao_frete_status_chk CHECK (
    frete_status IN (
      'pendente',
      'calculado',
      'erro',
      'sem_tabela_antt',
      'sem_qtd_eixos',
      'sem_km_google',
      'calculo_manual_necessario'
    )
  );
