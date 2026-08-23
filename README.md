# Fofoqueiro do Mercado — Setup da automação (custo zero)

Pipeline que faz o painel **Fofoqueiro do Mercado** consumir dados atualizados
diariamente sem servidor próprio, sem API paga e sem manutenção.

## Arquitetura

```
GitHub Actions (cron 09h UTC)
        │
        ▼
scripts/ingest.mjs  ──> RSS: Google News + G1 + O Povo + Diário do Nordeste
        │
        ▼
data/oncologia-fortaleza.json  (commitado no repositório)
        │
        ▼
https://raw.githubusercontent.com/<usuario>/<repo>/main/data/oncologia-fortaleza.json
        │
        ▼
Painel HTML  ──> Configuração → Fonte de dados remota
```

## Passo a passo

1. **Criar repositório GitHub** (público ou privado com Actions habilitado — plano grátis já basta).
2. **Copiar os arquivos** deste pacote para a raiz do repositório, preservando as pastas:
   - `.github/workflows/ingest.yml`
   - `scripts/ingest.mjs`
3. **Fazer o primeiro commit e push** — o workflow já rodará (também pode ser disparado
   manualmente em *Actions → Fofoqueiro → Run workflow*).
4. **Verificar o arquivo gerado** em `data/oncologia-fortaleza.json`.
5. **Copiar a URL raw**:
   `https://raw.githubusercontent.com/<usuario>/<repo>/main/data/oncologia-fortaleza.json`
6. **No painel**: aba *⚙️ Configuração → Fonte de dados remota* → colar → **Salvar & Recarregar**.

A partir daí, todo dia às 06h de Fortaleza o pipeline atualiza o JSON e o painel
carrega os dados novos sempre que for aberto.

## Custos

- **GitHub Actions:** 2.000 minutos/mês grátis; cada rodada consome ~1 min → sobra muito.
- **GitHub Pages/raw:** ilimitado para arquivos pequenos.
- **RSS Google News:** gratuito, sem autenticação.
- **RSS de veículos brasileiros:** gratuito.

Total: **R$ 0,00/mês**.

## Adicionar novos mercados

Edite `scripts/ingest.mjs`, seção `MERCADOS`: duplique o objeto e ajuste `id`, `nome`,
`termosGoogleNews` e `feedsImprensa`. O workflow gerará um JSON separado para cada
mercado, e você adiciona a URL correspondente na *Configuração* daquele mercado no painel.

## Fase 2 — extensões previstas

Já mapeadas para incorporar sem custo:

- **Google Trends** via biblioteca `google-trends-api` (npm, grátis) — popula
  `termosBusca[i].serie` para os sparklines.
- **Reclame Aqui** — fetch + regex simples, uma vez ao dia.
- **ANS** — RSS oficial de atualizações do Rol.
- **LexML API** — projetos de lei federais e estaduais.
- **Portal da Transparência** — compras públicas oncológicas.

Todas essas fontes têm exemplos prontos para copiar dentro do painel, na aba
*🔧 Setup Automação*.
