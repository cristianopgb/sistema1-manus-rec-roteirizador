-- ============================================================
-- Permissões de exclusão para uploads/rodadas vinculadas
-- Migration: 006_uploads_delete_rls.sql
-- ============================================================

DROP POLICY IF EXISTS "rodadas_delete" ON public.rodadas_roteirizacao;
CREATE POLICY "rodadas_delete" ON public.rodadas_roteirizacao FOR DELETE
  USING (
    get_user_perfil() = 'master'
    OR (filial_id = get_user_filial_id() AND usuario_id = auth.uid())
  );

DROP POLICY IF EXISTS "uploads_carteira_delete" ON public.uploads_carteira;
CREATE POLICY "uploads_carteira_delete" ON public.uploads_carteira FOR DELETE
  USING (
    get_user_perfil() = 'master'
    OR (filial_id = get_user_filial_id() AND usuario_id = auth.uid())
  );

DROP POLICY IF EXISTS "carteira_itens_delete" ON public.carteira_itens;
CREATE POLICY "carteira_itens_delete" ON public.carteira_itens FOR DELETE
  USING (
    EXISTS (
      SELECT 1
      FROM public.uploads_carteira uc
      WHERE uc.id = carteira_itens.upload_id
        AND (
          get_user_perfil() = 'master'
          OR (uc.filial_id = get_user_filial_id() AND uc.usuario_id = auth.uid())
        )
    )
  );
