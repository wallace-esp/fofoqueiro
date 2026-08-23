// scripts/ingest.mjs — coleta diária Fofoqueiro do Mercado
// Roda no GitHub Actions (cron), custo zero.
// Consome RSS de imprensa + Google News por termo, deduplica,
// classifica tom heurístico e grava data/<mercado>.json

import Parser from "rss-parser";
import { writeFile, mkdir, readFile } from "node:fs/promises";
import { createHash } from "node:crypto";

const parser = new Parser({ timeout: 15000 });
const hash = s => createHash("sha1").update(s).digest("hex").slice(0, 12);
const norm = s => (s || "").toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();

// ============ CONFIGURE POR MERCADO ============
const MERCADOS = [
  {
    id: "oncologia-fortaleza",
    nome: "Oncologia · Fortaleza (CE)",
    termosGoogleNews: [
      "oncologia Fortaleza",
      "tratamento câncer Fortaleza",
      "ICC Fortaleza Instituto do Câncer",
      "Oncoclínicas",
      "Rede D'Or oncologia",
      "CRIO Fortaleza",
      "ATO Oncologia",
      "quimioterapia Ceará",
      "radioterapia Fortaleza",
      "ANS Rol oncologia"
    ],
    feedsImprensa: [
      { veiculo: "G1 Ceará",           url: "https://g1.globo.com/rss/g1/ce/ceara/" },
      { veiculo: "Diário do Nordeste", url: "https://diariodonordeste.verdesmares.com.br/rss" },
      { veiculo: "O Povo",             url: "https://www.opovo.com.br/rss.xml" },
      { veiculo: "G1 Saúde",           url: "https://g1.globo.com/rss/g1/ciencia-e-saude/" }
    ],
    feedsLegais: [
      { fonte: "DOU", url: "https://www.in.gov.br/leiturajornal?data=hoje" }
    ]
  }
  // adicione outros mercados aqui — cada um gera seu próprio JSON
];

// heurística leve de tom — substitua por LLM local (llama.cpp) para maior precisão
const NEG = ["recuperação judicial","recuperação extrajudicial","dívida","fraude","denúncia","queda","risco","alerta","escassez","greve","processo","suspende","cassação","multa","irregularidade"];
const POS = ["expansão","parceria","inauguração","cresce","recorde","aprovação","liderança","investimento","inovação","pioneir","conquista","reconhecida","abre inscrições","selo","premiada"];
function classificarTom(t) {
  const s = norm(t);
  let np = 0, nn = 0;
  POS.forEach(p => { if (s.includes(p)) np++; });
  NEG.forEach(p => { if (s.includes(p)) nn++; });
  if (np > nn) return "Positivo";
  if (nn > np) return "Negativo";
  return "Neutro";
}

async function coletarGoogleNews(termos) {
  const out = [];
  for (const t of termos) {
    const url = "https://news.google.com/rss/search?q=" + encodeURIComponent(t)
              + "&hl=pt-BR&gl=BR&ceid=BR:pt-419";
    try {
      const feed = await parser.parseURL(url);
      for (const it of feed.items || []) {
        out.push({
          veiculo:  (it.source && it.source._) || "Google News",
          titulo:   it.title,
          url:      it.link,
          data:     (it.isoDate || it.pubDate || new Date().toISOString()).slice(0, 10),
          tom:      classificarTom(it.title),
          origem:   "auto",
          termo:    t
        });
      }
    } catch (e) {
      console.error("GNews fail:", t, e.message);
    }
  }
  return out;
}

async function coletarImprensa(feeds) {
  const out = [];
  for (const f of feeds) {
    try {
      const feed = await parser.parseURL(f.url);
      for (const it of feed.items || []) {
        out.push({
          veiculo: f.veiculo,
          titulo:  it.title,
          url:     it.link,
          data:    (it.isoDate || it.pubDate || new Date().toISOString()).slice(0, 10),
          tom:     classificarTom(it.title),
          origem:  "auto"
        });
      }
    } catch (e) {
      console.error("RSS fail:", f.veiculo, e.message);
    }
  }
  return out;
}

// Deduplicação por hash de título normalizado (primeiros 60 chars)
// Uma matéria replicada em N veículos vira uma linha com veiculos[]
function dedup(noticias) {
  const grupos = new Map();
  for (const n of noticias) {
    const key = hash(norm(n.titulo).slice(0, 60));
    if (!grupos.has(key)) {
      grupos.set(key, { ...n, veiculos: [n.veiculo], incluir: true });
    } else {
      const g = grupos.get(key);
      if (!g.veiculos.includes(n.veiculo)) g.veiculos.push(n.veiculo);
    }
  }
  return [...grupos.values()].sort((a, b) => b.data.localeCompare(a.data));
}

async function processarMercado(m) {
  console.log("→ processando:", m.id);
  const gnews    = await coletarGoogleNews(m.termosGoogleNews);
  const imprensa = await coletarImprensa(m.feedsImprensa);
  const noticias = dedup([...gnews, ...imprensa]).slice(0, 50);

  // preserva overrides manuais (curadoria feita pelo analista dentro do painel)
  let previo = null;
  try {
    previo = JSON.parse(await readFile("data/" + m.id + ".json", "utf8"));
  } catch {}
  const curados = (previo && previo.noticiasCuradas) || [];

  const payload = {
    id: m.id,
    nome: m.nome,
    atualizadoEm: new Date().toISOString(),
    proximaAtualizacao: "diária, 09h UTC",
    noticias,
    noticiasCuradas: curados,
    origem: { motor: "ingest.mjs", versao: "1.0" }
  };

  await mkdir("data", { recursive: true });
  await writeFile("data/" + m.id + ".json", JSON.stringify(payload, null, 2), "utf8");
  console.log("  ", m.id, "→", noticias.length, "notícias após dedup");
}

// ============ RUN ============
for (const m of MERCADOS) {
  await processarMercado(m);
}
console.log("✔ ingestão concluída");
