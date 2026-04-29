alter table if exists public.rotas_manifestos_google
  add column if not exists legs_json jsonb null;
