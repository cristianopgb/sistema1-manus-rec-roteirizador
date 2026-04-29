create table if not exists public.roteirizacao_persistencia_rejeicoes (
  id uuid primary key default gen_random_uuid(),
  rodada_id uuid null references public.rodadas_roteirizacao(id) on delete cascade,
  grupo text not null,
  indice integer null,
  motivo text not null,
  severidade text not null default 'warning' check (severidade in ('info','warning','erro_nao_fatal','erro_fatal')),
  item_json jsonb null,
  contexto_json jsonb null,
  created_at timestamptz not null default now()
);
create index if not exists idx_persist_rej_rodada on public.roteirizacao_persistencia_rejeicoes(rodada_id);
create index if not exists idx_persist_rej_grupo on public.roteirizacao_persistencia_rejeicoes(grupo);
create index if not exists idx_persist_rej_severidade on public.roteirizacao_persistencia_rejeicoes(severidade);
create index if not exists idx_persist_rej_created_at on public.roteirizacao_persistencia_rejeicoes(created_at);
alter table public.rodadas_roteirizacao add column if not exists resumo_persistencia_json jsonb null;
alter table public.roteirizacao_persistencia_rejeicoes enable row level security;
-- TODO: alinhar policies ao mesmo padrão operacional por filial/master usado nas tabelas de rodadas/manifestos.
do $$ begin
if not exists (select 1 from pg_policies where schemaname='public' and tablename='roteirizacao_persistencia_rejeicoes' and policyname='rejeicoes_select_authenticated') then
create policy rejeicoes_select_authenticated on public.roteirizacao_persistencia_rejeicoes for select using (auth.role() = 'authenticated');
end if;
if not exists (select 1 from pg_policies where schemaname='public' and tablename='roteirizacao_persistencia_rejeicoes' and policyname='rejeicoes_insert_authenticated') then
create policy rejeicoes_insert_authenticated on public.roteirizacao_persistencia_rejeicoes for insert with check (auth.role() = 'authenticated');
end if;
end $$;
