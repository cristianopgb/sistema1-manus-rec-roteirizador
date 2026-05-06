-- Repescagem de remanescentes por linha única

alter table if exists public.rodadas_roteirizacao
  add column if not exists tipo_execucao text not null default 'principal',
  add column if not exists rodada_origem_id uuid null references public.rodadas_roteirizacao(id),
  add column if not exists repescagem_numero integer null;

create index if not exists idx_rodadas_roteirizacao_rodada_origem_id
  on public.rodadas_roteirizacao(rodada_origem_id);

create index if not exists idx_rodadas_roteirizacao_tipo_execucao
  on public.rodadas_roteirizacao(tipo_execucao);

alter table if exists public.remanescentes_roteirizacao
  add column if not exists carteira_item_id uuid null references public.carteira_itens(id);

create index if not exists idx_remanescentes_roteirizacao_carteira_item_id
  on public.remanescentes_roteirizacao(carteira_item_id);

create table if not exists public.rodadas_repescagem_itens (
  id uuid primary key default gen_random_uuid(),
  rodada_origem_id uuid not null references public.rodadas_roteirizacao(id),
  rodada_repescagem_id uuid null references public.rodadas_roteirizacao(id),
  remanescente_id uuid not null references public.remanescentes_roteirizacao(id),
  carteira_item_id uuid null references public.carteira_itens(id),
  status_repescagem text not null default 'selecionado',
  motivo_rejeicao text null,
  created_at timestamptz not null default now()
);

create index if not exists idx_rodadas_repescagem_itens_rodada_origem_id on public.rodadas_repescagem_itens(rodada_origem_id);
create index if not exists idx_rodadas_repescagem_itens_rodada_repescagem_id on public.rodadas_repescagem_itens(rodada_repescagem_id);
create index if not exists idx_rodadas_repescagem_itens_remanescente_id on public.rodadas_repescagem_itens(remanescente_id);
create index if not exists idx_rodadas_repescagem_itens_carteira_item_id on public.rodadas_repescagem_itens(carteira_item_id);
create index if not exists idx_rodadas_repescagem_itens_status_repescagem on public.rodadas_repescagem_itens(status_repescagem);
