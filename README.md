# sistema1-manus-rec-roteirizador
repositório do sistema 1 feito no manus

## Google Routes API (Edge Function)

Para configurar o secret usado no cálculo de rotas dentro da Supabase Edge Function:

```bash
supabase secrets set GOOGLE_ROUTES_API_KEY=...
```

> A chave `GOOGLE_ROUTES_API_KEY` deve ficar apenas no ambiente da Edge Function (não no React/browser).
