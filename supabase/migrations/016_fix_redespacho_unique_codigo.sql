-- Corrige unicidade de código em transportadoras_redespacho para casos globais (filial_id null)

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'transportadoras_redespacho_filial_codigo_key'
      AND conrelid = 'public.transportadoras_redespacho'::regclass
  ) THEN
    ALTER TABLE public.transportadoras_redespacho
      DROP CONSTRAINT transportadoras_redespacho_filial_codigo_key;
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS transportadoras_redespacho_codigo_global_unique
  ON public.transportadoras_redespacho (codigo)
  WHERE filial_id IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS transportadoras_redespacho_filial_codigo_unique
  ON public.transportadoras_redespacho (filial_id, codigo)
  WHERE filial_id IS NOT NULL;
