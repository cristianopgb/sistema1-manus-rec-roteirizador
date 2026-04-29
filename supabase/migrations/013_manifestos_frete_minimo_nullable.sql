-- Alinha persistência inicial de frete ao novo fluxo:
-- frete_minimo pode permanecer nulo até existir rota Google válida.
ALTER TABLE public.manifestos_roteirizacao
  ALTER COLUMN frete_minimo DROP NOT NULL;

ALTER TABLE public.manifestos_roteirizacao
  ALTER COLUMN frete_minimo DROP DEFAULT;
