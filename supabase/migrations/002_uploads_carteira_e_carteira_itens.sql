-- ============================================================
-- Upload de carteira e itens por upload_id
-- Migration: 002_uploads_carteira_e_carteira_itens.sql
-- ============================================================

CREATE TABLE IF NOT EXISTS public.uploads_carteira (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  usuario_id uuid NOT NULL REFERENCES auth.users(id),
  filial_id uuid NOT NULL REFERENCES public.filiais(id),
  nome_arquivo text NOT NULL,
  nome_aba text,
  status text NOT NULL DEFAULT 'importado',
  total_linhas_brutas integer NOT NULL DEFAULT 0,
  total_linhas_importadas integer NOT NULL DEFAULT 0,
  total_linhas_validas integer NOT NULL DEFAULT 0,
  total_linhas_invalidas integer NOT NULL DEFAULT 0,
  total_colunas_detectadas integer NOT NULL DEFAULT 0,
  linha_cabecalho_detectada integer,
  colunas_detectadas_json jsonb NOT NULL DEFAULT '[]'::jsonb,
  metadados_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  observacoes_importacao text,
  erro_importacao text,
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_uploads_carteira_usuario_id ON public.uploads_carteira(usuario_id);
CREATE INDEX IF NOT EXISTS idx_uploads_carteira_filial_id ON public.uploads_carteira(filial_id);
CREATE INDEX IF NOT EXISTS idx_uploads_carteira_created_at ON public.uploads_carteira(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_uploads_carteira_status ON public.uploads_carteira(status);

DROP TRIGGER IF EXISTS trg_uploads_carteira_updated_at ON public.uploads_carteira;
CREATE TRIGGER trg_uploads_carteira_updated_at
  BEFORE UPDATE ON public.uploads_carteira
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE TABLE IF NOT EXISTS public.carteira_itens (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  upload_id uuid NOT NULL REFERENCES public.uploads_carteira(id) ON DELETE CASCADE,
  linha_numero integer NOT NULL,
  status_validacao text NOT NULL DEFAULT 'valida',
  erro_validacao text,

  filial_r text,
  romane text,
  filial_d text,
  serie text,
  nro_doc text,
  data_des text,
  data_nf text,
  dle text,
  agendam text,
  palet text,
  conf text,
  peso numeric,
  vlr_merc numeric,
  qtd numeric,
  peso_cubico numeric,
  classif text,
  tomad text,
  destin text,
  bairro text,
  cidade text,
  uf text,
  nf_serie text,
  tipo_carga text,
  qtd_nf numeric,
  mesoregiao text,
  sub_regiao text,
  ocorrencias_nf text,
  remetente text,
  observacao text,
  ref_cliente text,
  cidade_dest text,
  agenda text,
  tipo_ca text,
  ultima_ocorrencia text,
  status_r text,
  latitude numeric,
  longitude numeric,
  peso_calculo numeric,
  prioridade text,
  restricao_veiculo text,
  carro_dedicado boolean,
  inicio_entrega text,
  fim_entrega text,

  dados_originais_json jsonb NOT NULL DEFAULT '{}'::jsonb,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_carteira_itens_upload_id ON public.carteira_itens(upload_id);
CREATE INDEX IF NOT EXISTS idx_carteira_itens_status_validacao ON public.carteira_itens(status_validacao);
CREATE INDEX IF NOT EXISTS idx_carteira_itens_cidade ON public.carteira_itens(cidade);
CREATE INDEX IF NOT EXISTS idx_carteira_itens_destin ON public.carteira_itens(destin);
CREATE INDEX IF NOT EXISTS idx_carteira_itens_tomad ON public.carteira_itens(tomad);
CREATE INDEX IF NOT EXISTS idx_carteira_itens_uf ON public.carteira_itens(uf);
CREATE INDEX IF NOT EXISTS idx_carteira_itens_mesoregiao ON public.carteira_itens(mesoregiao);
CREATE INDEX IF NOT EXISTS idx_carteira_itens_sub_regiao ON public.carteira_itens(sub_regiao);

ALTER TABLE public.uploads_carteira ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.carteira_itens ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "uploads_carteira_select" ON public.uploads_carteira;
CREATE POLICY "uploads_carteira_select" ON public.uploads_carteira FOR SELECT
  USING (get_user_perfil() = 'master' OR filial_id = get_user_filial_id());

DROP POLICY IF EXISTS "uploads_carteira_insert" ON public.uploads_carteira;
CREATE POLICY "uploads_carteira_insert" ON public.uploads_carteira FOR INSERT
  WITH CHECK (auth.uid() = usuario_id);

DROP POLICY IF EXISTS "uploads_carteira_update" ON public.uploads_carteira;
CREATE POLICY "uploads_carteira_update" ON public.uploads_carteira FOR UPDATE
  USING (get_user_perfil() = 'master' OR usuario_id = auth.uid());

DROP POLICY IF EXISTS "carteira_itens_select" ON public.carteira_itens;
CREATE POLICY "carteira_itens_select" ON public.carteira_itens FOR SELECT
  USING (
    EXISTS (
      SELECT 1 FROM public.uploads_carteira uc
      WHERE uc.id = carteira_itens.upload_id
      AND (get_user_perfil() = 'master' OR uc.filial_id = get_user_filial_id())
    )
  );

DROP POLICY IF EXISTS "carteira_itens_insert" ON public.carteira_itens;
CREATE POLICY "carteira_itens_insert" ON public.carteira_itens FOR INSERT
  WITH CHECK (
    EXISTS (
      SELECT 1 FROM public.uploads_carteira uc
      WHERE uc.id = carteira_itens.upload_id
      AND uc.usuario_id = auth.uid()
    )
  );

DROP POLICY IF EXISTS "carteira_itens_update" ON public.carteira_itens;
CREATE POLICY "carteira_itens_update" ON public.carteira_itens FOR UPDATE
  USING (
    EXISTS (
      SELECT 1 FROM public.uploads_carteira uc
      WHERE uc.id = carteira_itens.upload_id
      AND (get_user_perfil() = 'master' OR uc.usuario_id = auth.uid())
    )
  );
