// scripts/ingest.mjs — Fase 2 do Fofoqueiro do Mercado
// Motor de ingestão diária, custo zero, sem API keys.
//
// Painéis alimentados: É NOTÍCIA, É LEI, É NEGÓCIO, É BUSCA,
// É CONCORRÊNCIA, OPORTUNIDADES.
//
// Regra de ouro: nenhuma fonte individual pode derrubar o pipeline.
// Cada fonte roda em try/catch com timeout. Falha vira [WARN] no log
// e o último valor válido é preservado a partir do JSON anterior.

import Parser from "rss-parser";
import googleTrends from "google-trends-api";
import { writeFile, mkdir, readFile } from "node:fs/promises";
import { createHash } from "node:crypto";

// ============================================================
// CONFIG POR MERCADO
// ============================================================
const MERCADOS = [
  {
    id: "oncologia-fortaleza",
    nome: "Oncologia · Fortaleza (CE)",
    setor: "Saúde",
    segmento: "Oncologia",
    cidade: "Fortaleza",
    estado: "CE",
    geoTrends: "BR-CE",

    // Termos passados para Google News (por termo → uma query)
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

    // Termos monitorados no Google Trends
    termosBuscaTrends: [
      "tratamento de câncer Fortaleza",
      "oncologia Fortaleza",
      "quimioterapia Fortaleza",
      "radioterapia Fortaleza"
    ],

    // RSS de imprensa regional/nacional
    feedsImprensa: [
      { veiculo: "G1 Ceará",           url: "https://g1.globo.com/rss/g1/ce/ceara/" },
      { veiculo: "Diário do Nordeste", url: "https://diariodonordeste.verdesmares.com.br/rss" },
      { veiculo: "O Povo",             url: "https://www.opovo.com.br/rss.xml" },
      { veiculo: "G1 Saúde",           url: "https://g1.globo.com/rss/g1/ciencia-e-saude/" }
    ],

    // Fontes legislativas / regulatórias
    // Câmara e Senado: APIs oficiais JSON/XML, sem key.
    // ANS/DOU/STF: via Google News (não há RSS público estável).
    fontesLei: {
      camaraKeywords: ["oncologia", "câncer", "saúde suplementar"],
      senadoKeywords: ["oncologia", "câncer"],
      newsRegulatorio: [
        "ANS Rol procedimentos oncologia",
        "Anvisa medicamento oncológico",
        "Ministério da Saúde oncologia",
        "STF medicamentos oncológicos SUS",
        "DOU portaria oncologia"
      ]
    },

    // Seed inicial de concorrentes — usado se o JSON anterior não existir.
    // Em runs subsequentes, valores como gmn/seg/eng são preservados.
    concorrentesSeed: [
      { nome:"Instituto do Câncer do Ceará (ICC)", grupo:"Rede ICC Saúde",     instagram:"@haroldojuacaba",       gmn:4.6, ra:"—",       seg:12000, eng:2.1, url:"https://www.icc.org.br/" },
      { nome:"Oncoclínicas Fortaleza",             grupo:"Grupo Oncoclínicas", instagram:"@grupooncoclinicas",    gmn:4.4, ra:"—",       seg:45000, eng:1.8, url:"https://oncoclinicas.com/" },
      { nome:"Oncologia D'Or Fujiday",             grupo:"Rede D'Or",          instagram:"@oncologiadorfujiday",  gmn:4.5, ra:"7.0",     seg:1800,  eng:3.2, url:"https://www.rededor.com.br/" },
      { nome:"Centro de Oncologia CRIO",           grupo:"CRIO",               instagram:"@centrodeoncologiacrio",gmn:4.7, ra:"—",       seg:29400, eng:2.8, url:"https://www.crio.med.br/" },
      { nome:"Quimioclinic",                       grupo:"Quimioclinic",       instagram:"—",                     gmn:4.3, ra:"—",       seg:0,     eng:0,   url:"https://quimioclinic.com.br/" },
      { nome:"Hospital Cura d'Ars",                grupo:"Rede São Camilo",    instagram:"@saocamilofortaleza",   gmn:4.2, ra:"—",       seg:8000,  eng:1.5, url:"https://www.saocamilo.com/" },
      { nome:"ATO Oncologia",                      grupo:"ATO Terapias",       instagram:"@atooncologia",         gmn:4.8, ra:"—",       seg:3700,  eng:4.1, url:"https://atooncologia.com.br/" },
      { nome:"Pronutrir Oncologia",                grupo:"Pronutrir",          instagram:"@pronutriroficial",     gmn:4.5, ra:"—",       seg:5200,  eng:2.4, url:"https://pronutrir.com.br/" },
      { nome:"Oncovie",                            grupo:"Oncovie",            instagram:"@clinicaoncovie",       gmn:4.4, ra:"sem selo",seg:2900,  eng:3.0, url:"https://oncovie.com.br/" },
      { nome:"OTO Oncologia",                      grupo:"Rede Oto / Kora Saúde", instagram:"via Rede Oto",       gmn:4.3, ra:"—",       seg:11000, eng:1.9, url:"https://www.korasaude.com.br/" }
    ]
  }
];

// ============================================================
// DICIONÁRIOS DE CLASSIFICAÇÃO
// ============================================================

// Palavras que aumentam relevância para o mercado alvo
const RELEVANCIA_KEYWORDS = {
  core: ["oncolog", "câncer", "cancer", "quimioterap", "radioterap", "imunoterap", "tumor", "metástas", "biópsi", "mama", "próstata"],
  saudeAmpla: ["saúde", "hospital", "clínica", "clinica", "medicamento", "farmacêut", "diagnóstico", "diagnostico", "paciente", "sus", "ans", "anvisa", "plano de saúde"],
  local: ["fortaleza", "ceará", "ceara", "nordeste"],
  players: ["icc", "oncoclínicas", "oncoclinicas", "rede d'or", "rede dor", "crio", "ato oncologia", "pronutrir", "oncovie", "kora saúde", "kora saude", "cura d'ars", "quimioclinic", "haroldo juaçaba", "hapvida", "unimed"],
  negocio: ["aquisição", "aquisicao", "fusão", "fusao", "investimento", "aporte", "recuperação", "recuperacao", "joint venture", "parceria estratégica", "expansão", "expansao", "inauguração", "inauguracao", "nova unidade", "abre unidade", "opa "]
};

// Palavras para classificação de tom (heurística leve)
const TOM_POS = ["expansão","expansao","parceria","inauguração","inauguracao","cresce","recorde","aprovação","aprovacao","lidera","liderança","lideranca","investimento","inovação","inovacao","pioneir","conquista","reconhecida","abre inscrições","abre inscricoes","selo","premiada","avanço","avanco","descoberta","cura","salva vidas"];
const TOM_NEG = ["recuperação judicial","recuperacao judicial","recuperação extrajudicial","recuperacao extrajudicial","dívida","divida","fraude","denúncia","denuncia","queda","risco","alerta","escassez","greve","processo","suspende","cassação","cassacao","multa","irregularidade","morre","morte","falência","falencia","crise"];

// Regex para movimentações de negócio
const NEGOCIO_TIPOS = [
  { tipo: "Aquisição",           regex: /\baquisi[cç][aã]o|adquire|comprada|comprou\b/i },
  { tipo: "Fusão",               regex: /\bfus[aã]o|se une|se uniu|incorpora|incorporou\b/i },
  { tipo: "Investimento",        regex: /\binvestimento|aporte|rodada|capta[a-z]*|s[eé]rie [a-e]\b/i },
  { tipo: "Recuperação Judicial",regex: /\brecupera[cç][aã]o\s+(judicial|extrajudicial)|em recupera[cç][aã]o\b/i },
  { tipo: "Joint Venture",       regex: /\bjoint venture\b/i },
  { tipo: "Parceria Estratégica",regex: /\bparceria estrat[eé]gica\b/i },
  { tipo: "Expansão",            regex: /\bexpans[aã]o|inaugura[a-z]*|abre (nova|unidade)|nova unidade\b/i },
  { tipo: "OPA",                 regex: /\bopa\b|oferta p[uú]blica de aquisi[cç][aã]o/i },
  { tipo: "Venda de Ativos",     regex: /\bvenda de ativos|desinveste|desinvestimento\b/i }
];

// Regex para impacto de atos regulatórios
const IMPACTO_REGRAS = [
  { impacto: "ALTO",  regex: /\b(obrigat[oó]ri[oa]|obriga|determina|sanciona|promulg|publicad[oa]|entra em vigor|vigor|lei n[º°o]|portaria n[º°o]|resolu[cç][aã]o normativa|inclus[aã]o no rol)\b/i },
  { impacto: "MÉDIO", regex: /\b(regulamenta|prop[oõ]e|apresenta projeto|audi[eê]ncia p[uú]blica|consulta p[uú]blica|em an[aá]lise)\b/i }
];

// ============================================================
// UTILITÁRIOS
// ============================================================
const parser = new Parser({ timeout: 15000 });
const hash = s => createHash("sha1").update(s || "").digest("hex").slice(0, 12);
const norm = s => (s || "").toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
const hojeISO = () => new Date().toISOString();
const diasAtras = (n) => new Date(Date.now() - n * 24 * 60 * 60 * 1000);

function log(nivel, fonte, msg) {
  const tag = { OK: "[OK]   ", WARN: "[WARN] ", ERROR: "[ERROR]", INFO: "[INFO] " }[nivel] || "[INFO] ";
  console.log(`${tag} ${fonte} — ${msg}`);
}

// Executa uma fonte de forma resiliente: try/catch + timeout + log estruturado.
// Se falhar, retorna o valor fallback e loga [WARN].
async function safeRun(nome, fn, fallback = []) {
  try {
    const r = await fn();
    log("OK", nome, `${Array.isArray(r) ? r.length + " itens" : "ok"}`);
    return r;
  } catch (e) {
    log("WARN", nome, `indisponível — ${e.message}`);
    return fallback;
  }
}

// fetch com timeout (padrão 15s)
async function fetchTimeout(url, ms = 15000, opts = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try {
    return await fetch(url, { ...opts, signal: ctrl.signal });
  } finally {
    clearTimeout(t);
  }
}

async function lerJsonAnterior(id) {
  try {
    return JSON.parse(await readFile(`data/${id}.json`, "utf8"));
  } catch {
    return null;
  }
}

function classificarTom(titulo) {
  const s = norm(titulo);
  let np = 0, nn = 0;
  TOM_POS.forEach(p => { if (s.includes(norm(p))) np++; });
  TOM_NEG.forEach(p => { if (s.includes(norm(p))) nn++; });
  if (np > nn) return "Positivo";
  if (nn > np) return "Negativo";
  return "Neutro";
}

// Score de relevância transparente:
//   +3 por keyword "core" (oncologia/câncer/etc)
//   +2 por menção de player conhecido
//   +2 por menção local (Fortaleza/CE)
//   +1 por keyword "saúde ampla"
//   +2 por keyword de "negócio" quando combinada com saúde
function scoreRelevancia(titulo) {
  const s = norm(titulo);
  let score = 0;
  const hits = { core: 0, saude: 0, local: 0, players: 0, negocio: 0 };
  RELEVANCIA_KEYWORDS.core.forEach(k => { if (s.includes(k)) { score += 3; hits.core++; } });
  RELEVANCIA_KEYWORDS.players.forEach(k => { if (s.includes(k)) { score += 2; hits.players++; } });
  RELEVANCIA_KEYWORDS.local.forEach(k => { if (s.includes(k)) { score += 2; hits.local++; } });
  RELEVANCIA_KEYWORDS.saudeAmpla.forEach(k => { if (s.includes(k)) { score += 1; hits.saude++; } });
  RELEVANCIA_KEYWORDS.negocio.forEach(k => { if (s.includes(k)) { score += hits.saude || hits.core ? 2 : 0; hits.negocio++; } });
  return { score, hits };
}

function classificarTema(titulo) {
  const { hits } = scoreRelevancia(titulo);
  if (hits.core) return "oncologia";
  if (hits.negocio) return "negócio";
  if (hits.saude) return "saúde";
  if (hits.local) return "local";
  return "outro";
}

// ============================================================
// FONTE 1 — NOTÍCIAS (Google News + RSS de imprensa)
// ============================================================
async function coletarGoogleNews(termos) {
  const out = [];
  for (const t of termos) {
    const url = "https://news.google.com/rss/search?q=" + encodeURIComponent(t)
              + "&hl=pt-BR&gl=BR&ceid=BR:pt-419";
    await safeRun(`GNews:${t}`, async () => {
      const feed = await parser.parseURL(url);
      for (const it of feed.items || []) {
        out.push({
          veiculo:  (it.source && it.source._) || "Google News",
          titulo:   it.title,
          url:      it.link,
          data:     (it.isoDate || it.pubDate || hojeISO()).slice(0, 10),
          termo:    t
        });
      }
      return feed.items || [];
    });
  }
  return out;
}

async function coletarImprensa(feeds) {
  const out = [];
  for (const f of feeds) {
    await safeRun(`RSS:${f.veiculo}`, async () => {
      const feed = await parser.parseURL(f.url);
      for (const it of feed.items || []) {
        out.push({
          veiculo: f.veiculo,
          titulo:  it.title,
          url:     it.link,
          data:    (it.isoDate || it.pubDate || hojeISO()).slice(0, 10)
        });
      }
      return feed.items || [];
    });
  }
  return out;
}

// Deduplica por hash de título normalizado. Uma matéria replicada em
// N veículos vira uma linha só com veiculos[] listando todos.
function dedupNoticias(itens) {
  const grupos = new Map();
  for (const n of itens) {
    const key = hash(norm(n.titulo).slice(0, 60));
    if (!grupos.has(key)) {
      grupos.set(key, {
        ...n,
        veiculos: [n.veiculo],
        origem: "auto",
        incluir: false
      });
    } else {
      const g = grupos.get(key);
      if (!g.veiculos.includes(n.veiculo)) g.veiculos.push(n.veiculo);
    }
  }
  return [...grupos.values()];
}

// Aplica score/tema/tom + define 'incluir' com base em threshold.
function curarNoticias(noticias) {
  const THRESHOLD = 3; // ver scoreRelevancia — pelo menos uma keyword core
  return noticias
    .map(n => {
      const { score } = scoreRelevancia(n.titulo);
      const tema = classificarTema(n.titulo);
      const tom = classificarTom(n.titulo);
      // pontinho extra se aparece em >1 veículo (relevância revelada)
      const scoreFinal = score + Math.min(3, (n.veiculos.length - 1));
      return { ...n, score: scoreFinal, tema, tom, incluir: scoreFinal >= THRESHOLD };
    })
    .sort((a, b) => b.data.localeCompare(a.data) || b.score - a.score)
    .slice(0, 60);
}

// ============================================================
// FONTE 2 — LEI (Câmara + Senado + Google News regulatório)
// ============================================================
async function coletarCamara(keywords) {
  const out = [];
  // API oficial dadosabertos.camara.leg.br — retorna JSON, sem key
  for (const kw of keywords) {
    const url = "https://dadosabertos.camara.leg.br/api/v2/proposicoes"
              + "?keywords=" + encodeURIComponent(kw)
              + "&dataApresentacaoInicio=" + diasAtras(60).toISOString().slice(0, 10)
              + "&itens=15&ordem=DESC&ordenarPor=id";
    await safeRun(`Câmara:${kw}`, async () => {
      const r = await fetchTimeout(url, 20000, { headers: { accept: "application/json" } });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const j = await r.json();
      for (const p of j.dados || []) {
        out.push({
          titulo: (p.ementa || p.descricaoTipo || "Proposição") + (p.siglaTipo ? ` (${p.siglaTipo} ${p.numero}/${p.ano})` : ""),
          fonte:  "Câmara dos Deputados",
          data:   (p.dataApresentacao || hojeISO()).slice(0, 10),
          url:    p.uri || `https://www.camara.leg.br/propostas-legislativas/${p.id}`,
          tema:   "legislativo-federal"
        });
      }
      return j.dados || [];
    });
  }
  return out;
}

async function coletarSenado(keywords) {
  const out = [];
  for (const kw of keywords) {
    // API pública do Senado — retorna XML por padrão, mas suporta JSON via header
    const url = "https://legis.senado.leg.br/dadosabertos/materia/pesquisa/lista"
              + "?palavraChave=" + encodeURIComponent(kw)
              + "&ano=" + new Date().getFullYear();
    await safeRun(`Senado:${kw}`, async () => {
      const r = await fetchTimeout(url, 20000, { headers: { accept: "application/json" } });
      if (!r.ok) throw new Error("HTTP " + r.status);
      const j = await r.json();
      const lista = j?.PesquisaBasicaMateria?.Materias?.Materia || [];
      const items = Array.isArray(lista) ? lista : [lista];
      for (const m of items) {
        const ident = m.IdentificacaoMateria || {};
        const codigo = ident.CodigoMateria;
        out.push({
          titulo: (m.EmentaMateria || ident.DescricaoObjetivoProcesso || "Matéria legislativa") +
                  (ident.SiglaSubtipoMateria ? ` (${ident.SiglaSubtipoMateria} ${ident.NumeroMateria}/${ident.AnoMateria})` : ""),
          fonte:  "Senado Federal",
          data:   (m.DataApresentacao || hojeISO()).slice(0, 10),
          url:    codigo ? `https://www25.senado.leg.br/web/atividade/materias/-/materia/${codigo}` : "https://www25.senado.leg.br/",
          tema:   "legislativo-federal"
        });
      }
      return items;
    });
  }
  return out;
}

async function coletarNewsRegulatorio(termos) {
  // reutiliza o Google News como proxy para atos/notícias regulatórias
  // que não têm RSS público estável (ANS, DOU, STF, Anvisa)
  const out = [];
  for (const t of termos) {
    const url = "https://news.google.com/rss/search?q=" + encodeURIComponent(t)
              + "&hl=pt-BR&gl=BR&ceid=BR:pt-419";
    await safeRun(`RegNews:${t}`, async () => {
      const feed = await parser.parseURL(url);
      for (const it of feed.items || []) {
        // tenta identificar a fonte real pela ementa/link/veículo
        const src = (it.source && it.source._) || "";
        let fonte = "Regulatório (via imprensa)";
        const lower = (it.title + " " + src).toLowerCase();
        if (lower.includes("ans")) fonte = "ANS";
        else if (lower.includes("anvisa")) fonte = "Anvisa";
        else if (lower.includes("stf")) fonte = "STF";
        else if (lower.includes("dou") || lower.includes("diário oficial")) fonte = "DOU";
        else if (lower.includes("minist")) fonte = "Ministério da Saúde";
        out.push({
          titulo: it.title,
          fonte,
          data: (it.isoDate || it.pubDate || hojeISO()).slice(0, 10),
          url:  it.link,
          tema: "regulatório-noticia"
        });
      }
      return feed.items || [];
    });
  }
  return out;
}

function classificarImpacto(titulo) {
  for (const r of IMPACTO_REGRAS) {
    if (r.regex.test(titulo)) return r.impacto;
  }
  return "BAIXO";
}

async function coletarAtosLei(m) {
  const cam = await coletarCamara(m.fontesLei.camaraKeywords);
  const sen = await coletarSenado(m.fontesLei.senadoKeywords);
  const reg = await coletarNewsRegulatorio(m.fontesLei.newsRegulatorio);

  const todos = [...cam, ...sen, ...reg]
    .map(a => ({
      ...a,
      impacto: classificarImpacto(a.titulo),
      origem: "auto"
    }));

  // dedup por (titulo normalizado + fonte)
  const grupos = new Map();
  for (const a of todos) {
    const k = hash(norm(a.titulo).slice(0, 60) + "|" + a.fonte);
    if (!grupos.has(k)) grupos.set(k, a);
  }
  return [...grupos.values()]
    .sort((a, b) => b.data.localeCompare(a.data))
    .slice(0, 30);
}

// ============================================================
// FONTE 3 — NEGÓCIO (extraído das notícias)
// ============================================================
function extrairMovimentacoes(noticias) {
  const out = [];
  const vistos = new Set();
  for (const n of noticias) {
    for (const t of NEGOCIO_TIPOS) {
      if (t.regex.test(n.titulo)) {
        const key = hash(t.tipo + "|" + norm(n.titulo).slice(0, 40));
        if (vistos.has(key)) continue;
        vistos.add(key);
        // extrai "empresas" heuristicamente: tira o veículo do título e limita comprimento
        const empresas = n.titulo.replace(/\s*[-—|]\s*[^-—|]+$/, "").trim().slice(0, 140);
        out.push({
          tipo: t.tipo,
          empresas,
          texto: n.veiculo,
          data: n.data,
          link: n.url,
          origem: "auto"
        });
        break; // um tipo por notícia
      }
    }
  }
  return out.sort((a, b) => b.data.localeCompare(a.data)).slice(0, 20);
}

// ============================================================
// FONTE 4 — BUSCA (Google Trends)
// ============================================================
async function coletarTrends(termos, geo) {
  const out = [];
  const start = diasAtras(90);

  // Google Trends permite comparar até 5 termos por consulta.
  // Fazemos lotes para reduzir throttling e manter o pipeline resiliente.
  const lotes = [];

  for (let i = 0; i < termos.length; i += 5) {
    lotes.push(termos.slice(i, i + 5));
  }

  for (const lote of lotes) {
    await safeRun(`Trends:${lote.join(" | ")}`, async () => {
      const raw = await googleTrends.interestOverTime({
        keyword: lote,
        startTime: start,
        geo: geo || "BR-CE",
        hl: "pt-BR"
      });

      const json = JSON.parse(raw);
      const pts = json?.default?.timelineData || [];

      if (!pts.length) {
        throw new Error("Google Trends retornou série vazia");
      }

      /*
       * Quando vários termos são consultados juntos,
       * cada ponto contém um valor para cada termo.
       */
      const seriesPorTermo = lote.map(() => []);

      for (const p of pts) {
        const values = Array.isArray(p.value) ? p.value : [];

        lote.forEach((termo, index) => {
          const valor = Number(values[index] ?? 0);
          seriesPorTermo[index].push(valor);
        });
      }

      lote.forEach((termo, index) => {
        const serie = seriesPorTermo[index];

        if (!serie.length) return;

        // Reduz para aproximadamente 12 pontos representativos.
        const stride = Math.max(1, Math.floor(serie.length / 12));
        const serieAmostrada = [];

        for (let i = 0; i < serie.length; i += stride) {
          serieAmostrada.push(serie[i]);
        }

        if (serieAmostrada.length > 12) {
          serieAmostrada.length = 12;
        }

        const volume =
          serieAmostrada[serieAmostrada.length - 1] || 0;

        const anterior =
          serieAmostrada[serieAmostrada.length - 2] || 0;

        const incremento =
          anterior > 0
            ? Number((((volume - anterior) / anterior) * 100).toFixed(1))
            : 0;

        let tendencia = "estável";

        if (incremento > 5) {
          tendencia = "alta";
        } else if (incremento < -5) {
          tendencia = "queda";
        }

        out.push({
          termo,
          volume,
          incremento,
          tendencia,
          serie: serieAmostrada,
          url:
            "https://trends.google.com/trends/explore?q=" +
            encodeURIComponent(termo) +
            "&geo=" +
            (geo || "BR"),
          origem: "auto",
          atualizadoEm: hojeISO()
        });
      });
    });

    // Pequena pausa entre lotes para reduzir risco de throttling.
    await new Promise(resolve => setTimeout(resolve, 1500));
  }

  return out;
}

// Fallback: se Trends falhar totalmente, preserva os dados anteriores.
function mergeTermosBusca(novos, anteriores) {
  if (!novos.length) return anteriores || [];
  // por termo: preferir o novo se veio; senão manter o antigo
  const porTermo = new Map();
  for (const a of (anteriores || [])) porTermo.set(a.termo, a);
  for (const n of novos) porTermo.set(n.termo, n);
  return [...porTermo.values()];
}

// ============================================================
// FONTE 5 — CONCORRÊNCIA (menções + preserva indicadores)
// ============================================================
function atualizarConcorrentes(seed, anteriores, noticias) {
  const base = (anteriores && anteriores.length) ? anteriores : seed;
  const out = [];
  const trintaDiasAtras = diasAtras(30).toISOString().slice(0, 10);

  for (const c of base) {
    // conta menções nas notícias dos últimos 30d
    const nomeNorm = norm(c.nome);
    // separa em variantes para melhorar match
    const chaves = new Set([nomeNorm]);
    if (c.nome.includes("(")) {
      const abreviatura = c.nome.match(/\(([^)]+)\)/);
      if (abreviatura) chaves.add(norm(abreviatura[1]));
    }
    if (c.grupo) chaves.add(norm(c.grupo));
    // primeira palavra do nome (ex: "Oncoclínicas Fortaleza" → "oncoclínicas")
    chaves.add(norm(c.nome.split(/\s|—/)[0]));

    let mencoes = 0;
    for (const n of noticias) {
      if (n.data < trintaDiasAtras) continue;
      const tn = norm(n.titulo);
      for (const k of chaves) {
        if (k.length >= 4 && tn.includes(k)) { mencoes++; break; }
      }
    }

    out.push({
      // preserva valores conhecidos; nunca zera indicadores por falta de dado novo
      nome: c.nome,
      grupo: c.grupo,
      url: c.url,
      instagram: c.instagram ?? "—",
      facebook: c.facebook ?? "—",
      gmn: c.gmn ?? null,
      ra: c.ra ?? "—",
      seg: c.seg ?? 0,
      eng: c.eng ?? 0,
      mencoes30d: mencoes,
      dataAtualizacao: hojeISO(),
      origem: c.origem || "curado"
    });
  }
  return out;
}

// ============================================================
// FONTE 6 — OPORTUNIDADES (cruzamento de sinais)
// ============================================================
function gerarOportunidades({ noticias, atosLei, movimentacoes, termosBusca, concorrentes }) {
  const ops = [];

  // Regra 1: termos com incremento >= 15% → capturar demanda
  for (const t of termosBusca || []) {
    if (t.incremento >= 15) {
      ops.push({
        titulo: `Capturar demanda em alta — "${t.termo}"`,
        descricao: `Busca com incremento de ${t.incremento}% e volume ${t.volume}. Recomenda-se ativar campanha SEO + Google Ads e revisar a landing page usando "${t.termo}" como âncora H1.`,
        tipo: "conteudo-conversao",
        prioridade: t.incremento >= 30 ? "ALTA" : "MÉDIA",
        sinais: [{ fonte: "Google Trends", ref: t.termo }],
        recomendacao: "Ativar campanha de conversão e atualizar SEO on-page.",
        origem: "auto"
      });
    }
  }

  // Regra 2: atos com impacto ALTO → adequação
  for (const a of atosLei || []) {
    if (a.impacto === "ALTO") {
      ops.push({
        titulo: `Adequação regulatória — ${a.titulo.slice(0, 80)}`,
        descricao: `Ato de impacto ALTO publicado por ${a.fonte}. Recomenda-se rodada com jurídico e comunicação para adaptar materiais, contratos e comunicação com pacientes/beneficiários.`,
        tipo: "adequacao-regulatoria",
        prioridade: "ALTA",
        sinais: [{ fonte: a.fonte, ref: a.url, data: a.data }],
        recomendacao: "Convocar jurídico + comunicação para revisão de conformidade.",
        origem: "auto"
      });
    }
  }

  // Regra 3: movimentações relevantes → janela de posicionamento
  for (const m of (movimentacoes || []).slice(0, 5)) {
    ops.push({
      titulo: `Janela de posicionamento — ${m.tipo}: ${m.empresas.slice(0, 60)}`,
      descricao: `Movimentação do tipo "${m.tipo}" em ${m.data}. Momento de reforçar mensagens de solidez, continuidade assistencial e diferenciação do cliente MEATO.`,
      tipo: "posicionamento",
      prioridade: "MÉDIA",
      sinais: [{ fonte: m.texto, ref: m.link, data: m.data }],
      recomendacao: "Pauta de conteúdo reforçando atributos comparativos.",
      origem: "auto"
    });
  }

  // Regra 4: concorrente ativo (>= 3 menções em 30d) → monitorar
  const ativos = (concorrentes || []).filter(c => (c.mencoes30d || 0) >= 3);
  for (const c of ativos.slice(0, 3)) {
    ops.push({
      titulo: `Concorrente ativo — ${c.nome}`,
      descricao: `${c.nome} (${c.grupo}) foi mencionado ${c.mencoes30d} vezes na imprensa nos últimos 30 dias. Recomenda-se aprofundar análise de conteúdo e monitorar movimentos.`,
      tipo: "monitoramento-competitivo",
      prioridade: c.mencoes30d >= 6 ? "ALTA" : "MÉDIA",
      sinais: [{ fonte: "clipping-30d", ref: c.url, mencoes: c.mencoes30d }],
      recomendacao: "Battlecard atualizado + monitorar tom e frequência das menções.",
      origem: "auto"
    });
  }

  // Regra 5: cruzamento — termo em alta + concorrente citado nesse termo
  const termosAlta = (termosBusca || []).filter(t => t.incremento >= 10);
  const noticiasRecentes = (noticias || []).filter(n => n.data >= diasAtras(15).toISOString().slice(0, 10));
  for (const t of termosAlta) {
    const kwT = norm(t.termo);
    for (const c of (concorrentes || [])) {
      const kwC = norm(c.nome.split(/\s|—/)[0]);
      const combinou = noticiasRecentes.some(n => {
        const tn = norm(n.titulo);
        return kwT.split(" ").some(w => w.length >= 5 && tn.includes(w)) && kwC.length >= 4 && tn.includes(kwC);
      });
      if (combinou) {
        ops.push({
          titulo: `Concorrente ocupando espaço no tema "${t.termo}"`,
          descricao: `${c.nome} apareceu em pelo menos uma matéria recente relacionada ao termo em alta "${t.termo}" (+${t.incremento}%). Se o cliente MEATO atua no mesmo tema, é urgente ativar comunicação.`,
          tipo: "gap-competitivo",
          prioridade: "ALTA",
          sinais: [{ fonte: "cruzamento", termo: t.termo, concorrente: c.nome }],
          recomendacao: "Produzir 1 peça de autoridade + 1 campanha no termo em alta.",
          origem: "auto"
        });
        break;
      }
    }
  }

  return ops.slice(0, 12);
}

// ============================================================
// MERGE PRESERVANDO HISTÓRICO
// ============================================================
// Se a nova coleta veio vazia (ou fonte caiu), preserva o valor anterior.
// Nunca sobrescreve silenciosamente com [].
function preservar(novo, anterior) {
  if (Array.isArray(novo) && novo.length > 0) return novo;
  return anterior || novo || [];
}

// ============================================================
// ORQUESTRADOR POR MERCADO
// ============================================================
async function processarMercado(m) {
  console.log("\n════════════════════════════════════════");
  console.log(` MERCADO: ${m.nome} (${m.id})`);
  console.log("════════════════════════════════════════\n");

  const previo = await lerJsonAnterior(m.id);

  // -------- NOTÍCIAS --------
  const gnews    = await coletarGoogleNews(m.termosGoogleNews);
  const imprensa = await coletarImprensa(m.feedsImprensa);
  const noticiasBrutas = dedupNoticias([...gnews, ...imprensa]);
  let noticias = curarNoticias(noticiasBrutas);
  noticias = preservar(noticias, previo?.noticias);

  // -------- É LEI --------
  let atosLei = await coletarAtosLei(m);
  atosLei = preservar(atosLei, previo?.atosLei);

  // -------- É NEGÓCIO --------
  let movimentacoes = extrairMovimentacoes(noticias);
  movimentacoes = preservar(movimentacoes, previo?.movimentacoes);

  // -------- É BUSCA --------
  const trendsNovos = await coletarTrends(m.termosBuscaTrends, m.geoTrends);
  const termosBusca = mergeTermosBusca(trendsNovos, previo?.termosBusca);

  // -------- É CONCORRÊNCIA --------
  const concorrentes = atualizarConcorrentes(m.concorrentesSeed, previo?.concorrentes, noticias);

  // -------- OPORTUNIDADES --------
  const oportunidades = gerarOportunidades({ noticias, atosLei, movimentacoes, termosBusca, concorrentes });

  // -------- CANAIS / FONTES LEGAIS (preservado do prev + defaults) --------
  const canais = preservar([], previo?.canais);
  const fontesLegais = preservar([], previo?.fontesLegais);

  const payload = {
    id: m.id,
    nome: m.nome,
    setor: m.setor,
    segmento: m.segmento,
    cidade: m.cidade,
    estado: m.estado,
    atualizadoEm: hojeISO(),
    proximaAtualizacao: "diária, 09h UTC",
    noticias,
    atosLei,
    movimentacoes,
    termosBusca,
    concorrentes,
    canais,
    fontesLegais,
    oportunidades,
    noticiasCuradas: (previo && previo.noticiasCuradas) || [],
    origem: { motor: "ingest.mjs", versao: "2.0" }
  };

  // Validação mínima antes de gravar — se algo essencial falhou, aborta a gravação
  // (preserva o JSON anterior no repositório)
  if (!payload.noticias || !Array.isArray(payload.noticias)) {
    throw new Error("payload inválido: noticias ausente");
  }

  await mkdir("data", { recursive: true });
  await writeFile(`data/${m.id}.json`, JSON.stringify(payload, null, 2), "utf8");

  console.log("\n── RESUMO ─────────────────────────────");
  console.log(`  notícias:      ${noticias.length}  (incluídas: ${noticias.filter(n => n.incluir).length})`);
  console.log(`  atos lei:      ${atosLei.length}`);
  console.log(`  movimentações: ${movimentacoes.length}`);
  console.log(`  termos busca:  ${termosBusca.length}`);
  console.log(`  concorrentes:  ${concorrentes.length}`);
  console.log(`  oportunidades: ${oportunidades.length}`);
  console.log("───────────────────────────────────────\n");
}

// ============================================================
// RUN
// ============================================================
let houveErro = false;
for (const m of MERCADOS) {
  try {
    await processarMercado(m);
  } catch (e) {
    houveErro = true;
    console.error(`[ERROR] mercado ${m.id} falhou: ${e.message}`);
    console.error(e.stack);
  }
}
if (houveErro) {
  console.error("\n✖ ingestão concluída com erros (JSON anterior preservado nos mercados que falharam)");
  process.exit(1);
}
console.log("✔ ingestão Fase 2 concluída com sucesso");
