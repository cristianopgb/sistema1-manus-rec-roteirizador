# sistema1-manus-rec-roteirizador

Repositório do Sistema 1 feito no Manus.

## Deploy operacional (Vercel)

Para publicar o front-end na Vercel:

1. Importe este repositório na Vercel.
2. Mantenha o preset de framework como **Vite**.
3. Configure as variáveis de ambiente no projeto (Settings → Environment Variables):
   - `VITE_SUPABASE_URL`
   - `VITE_SUPABASE_ANON_KEY`
   - `VITE_MOTOR_2_URL` (ex.: `https://motor-manus-roteirizador.onrender.com`)
4. Faça o deploy.

> Observação: o nome da variável do motor foi atualizado para `VITE_MOTOR_2_URL`.
