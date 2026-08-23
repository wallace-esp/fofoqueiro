// scripts/ingest.mjs — Fofoqueiro do Mercado (Fase 2.1)
//
// Motor mode-aware. Dois modos:
//   node scripts/ingest.mjs --mode=daily
//     → notícias, atos legais, movimentações (roda todo dia)
//   node scripts/ingest.mjs --mode=weekly
//     → concorrência (com snapshot), Trends batched, oportunidades
//
// Sem --mode ou --mode=all: roda os dois (útil para dispatch manual).
//
// Princípios:
//  1. Nenhuma fonte individual derruba o pipeline (try/catch + timeout).
//  2. Falha NÃO vira zero — preserva último valor válido e marca stale.
//  3. Dado sempre tem proveniência (fonte, coletadoEm, confiabilidade).
//  4. Config editável em data/<id>-config.json (fonte de verdade dos termos).

import Parser from "rss-parser";
import googleTrends from "google-trends-api";
import { writeFile, mkdir, readFile } from "node:fs/promises";
import { createHash } from "node:crypto";

// ============================================================
// PARSE ARGS
// ============================================================
function parseArgs() {
  const args = process.argv.slice(2);
  let mode = "all";
  for (const a of args) {
    const m = a.match(/^--mode=(.+)$/);
    if (m) mode = m[1];
  }
  if (!["daily", "weekly", "all"].includes(mode)) mode = "all";
  return { mode };
}
const { mode: RUN_MODE } = parseArgs();

// ============================================================
// MERCADOS — apenas identidade e feeds. Termos + concorrentes
// vêm do arquivo data/<id>-config.json (editável).
// ============================================================
const MERCADOS = [
  {
    id: "oncologia-fortaleza",
    nome: "Oncologia · Fortaleza (CE)",
    setor: "Saúde",
    segmento: "Oncologia",
    cidade: "Fortaleza",
    estado: "CE",
    // feeds de imprensa (não dependem de config editável)
    feedsImprensa: [
      { veiculo: "G1 Ceará",           url: "https://g1.globo.com/rss/g1/ce/ceara/" },
      { veiculo: "Diário do Nordeste", url: "https://diariodonordeste.verdesmares.com.br/rss" },
      { veiculo: "O Povo",             url: "https://www.opovo.com.br/rss.xml" },
      { veiculo: "G1 Saúde",           url: "https://g1.globo.com/rss/g1/ciencia-e-saude/" }
    ],
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
    }
  }
];

// ============================================================
// DICIONÁRIOS DE CLASSIFICAÇÃO
// ============================================================
const RELEVANCIA_KEYWORDS = {
  core: ["oncolog","câncer","cancer","quimioterap","radioterap","imunoterap","tumor","metástas","biópsi","biopsia","mama","próstata","prostata"],
  saudeAmpla: ["saúde","saude","hospital","clínica","clinica","medicamento","farmacêut","farmaceut","diagnóstico","diagnostico","paciente","sus","ans","anvisa","plano de saúde"],
  local: ["fortaleza","ceará","ceara","nordeste"],
  players: ["icc","oncoclínicas","oncoclinicas","rede d'or","rede dor","crio","ato oncologia","pronutrir","oncovie","kora saúde","kora saude","cura d'ars","quimioclinic","haroldo juaçaba","haroldo juacaba","hapvida","unimed"],
  negocio: ["aquisição","aquisicao","fusão","fusao","investimento","aporte","recuperação","recuperacao","joint venture","parceria estratégica","expansão","expansao","inauguração","inauguracao","nova unidade","abre unidade","opa "]
};
const TOM_POS = ["expansão","expansao","parceria","inauguração","inauguracao","cresce","recorde","aprovação","aprovacao","lidera","liderança","lideranca","investimento","inovação","inovacao","pioneir","conquista","reconhecida","abre inscrições","abre inscricoes","selo","premiada","avanço","avanco","descoberta","cura","salva vidas"];
const TOM_NEG = ["recuperação judicial","recuperacao judicial","recuperação extrajudicial","recuperacao extrajudicial","dívida","divida","fraude","denúncia","denuncia","queda","risco","alerta","escassez","greve","processo","suspende","cassação","cassacao","multa","irregularidade","morre","morte","falência","falencia","crise"];
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
const IMPACTO_REGRAS = [
  { impacto: "ALTO",  regex: /\b(obrigat[oó]ri[oa]|obriga|determina|sanciona|promulg|publicad[oa]|entra em vigor|vigor|lei n[º°o]|portaria n[º°o]|resolu[cç][aã]o normativa|inclus[aã]o no rol)\b/i },
  { impacto: "MÉDIO", regex: /\b(regulamenta|prop[oõ]e|apresenta projeto|audi[eê]ncia p[uú]blica|consulta p[uú]blica|em an[aá]lise)\b/i }
];

// ============================================================
// UTILITÁRIOS
// ============================================================
const parser = new Parser({ timeout: 15000 });
const hash = s => createHash("sha1").update(s || "").digest("hex").slice(0, 12);
const norm = s => (s || "").toString().toLowerCase().replace(/[^\p{L}\p{N}]+/gu, " ").trim();
const hojeISO = () => new Date().toISOString();
const hojeYMD = () => hojeISO().slice(0, 10);
const diasAtras = (n) => new Date(Date.now() - n * 24 * 60 * 60 * 1000);
const sleep = (ms) => new Promise(r => setTimeout(r, ms));

function log(nivel, fonte, msg) {
  const tag = { OK: "[OK]   ", WARN: "[WARN] ", ERROR: "[ERROR]", INFO: "[INFO] " }[nivel] || "[INFO] ";
  console.log(`${tag} ${fonte} — ${msg}`);
}

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

async function fetchTimeout(url, ms = 15000, opts = {}) {
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), ms);
  try { return await fetch(url, { ...opts, signal: ctrl.signal }); }
  finally { clearTimeout(t); }
}

async function lerJson(path) {
  try { return JSON.parse(await readFile(path, "utf8")); } catch { return null; }
}

async function lerConfig(id) {
  const cfg = await lerJson(`data/${id}-config.json`);
  return cfg;
}
async function lerAnterior(id) {
  return await lerJson(`data/${id}.json`);
}
async function lerHistorico(id) {
  const h = await lerJson(`data/${id}-history.json`);
  return h || { id, snapshots: [] };
}

function preservar(novo, anterior) {
  if (Array.isArray(novo) && novo.length > 0) return novo;
  return anterior || novo || [];
}

// Helper: produz um objeto de dado com proveniência.
function comProveniencia(valor, fonte, confiabilidade = "ALTA") {
  return {
    valor: valor,
    fonte: fonte,
    coletadoEm: hojeISO(),
    confiabilidade: confiabilidade,
    evidencia: valor !== null && valor !== undefined
  };
}
function semDado(motivo = "não coletado") {
  return {
    valor: null,
    fonte: null,
    coletadoEm: null,
    confiabilidade: "SEM_DADO",
    evidencia: false,
    motivo
  };
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
function classificarImpacto(titulo) {
  for (const r of IMPACTO_REGRAS) if (r.regex.test(titulo)) return r.impacto;
  return "BAIXO";
}

// ============================================================
// COLETORES — NOTÍCIAS
// ============================================================
async function coletarGoogleNews(termos) {
  const out = [];
  for (const t of termos) {
    const url = "https://news.google.com/rss/search?q=" + encodeURIComponent(t)
              + "&hl=pt-BR&gl=BR&ceid=BR:pt-419";
    await safeRun(`GNews:${t.slice(0,30)}`, async () => {
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
function dedupNoticias(itens) {
  const g = new Map();
  for (const n of itens) {
    const key = hash(norm(n.titulo).slice(0, 60));
    if (!g.has(key)) g.set(key, { ...n, veiculos: [n.veiculo], origem: "auto", incluir: false });
    else {
      const gv = g.get(key);
      if (!gv.veiculos.includes(n.veiculo)) gv.veiculos.push(n.veiculo);
    }
  }
  return [...g.values()];
}
function curarNoticias(noticias) {
  const THRESHOLD = 3;
  return noticias.map(n => {
    const { score } = scoreRelevancia(n.titulo);
    const tema = classificarTema(n.titulo);
    const tom = classificarTom(n.titulo);
    const scoreFinal = score + Math.min(3, (n.veiculos.length - 1));
    return { ...n, score: scoreFinal, tema, tom, incluir: scoreFinal >= THRESHOLD };
  })
  .sort((a, b) => b.data.localeCompare(a.data) || b.score - a.score)
  .slice(0, 80);
}

// ============================================================
// COLETORES — LEIS
// ============================================================
async function coletarCamara(keywords) {
  const out = [];
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
          fonte: "Câmara dos Deputados",
          data: (p.dataApresentacao || hojeISO()).slice(0, 10),
          url: p.uri || `https://www.camara.leg.br/propostas-legislativas/${p.id}`,
          tema: "legislativo-federal"
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
          fonte: "Senado Federal",
          data: (m.DataApresentacao || hojeISO()).slice(0, 10),
          url: codigo ? `https://www25.senado.leg.br/web/atividade/materias/-/materia/${codigo}` : "https://www25.senado.leg.br/",
          tema: "legislativo-federal"
        });
      }
      return items;
    });
  }
  return out;
}
async function coletarNewsRegulatorio(termos) {
  const out = [];
  for (const t of termos) {
    const url = "https://news.google.com/rss/search?q=" + encodeURIComponent(t) + "&hl=pt-BR&gl=BR&ceid=BR:pt-419";
    await safeRun(`RegNews:${t.slice(0,30)}`, async () => {
      const feed = await parser.parseURL(url);
      for (const it of feed.items || []) {
        const src = (it.source && it.source._) || "";
        let fonte = "Regulatório (via imprensa)";
        const lower = (it.title + " " + src).toLowerCase();
        if (lower.includes("ans")) fonte = "ANS";
        else if (lower.includes("anvisa")) fonte = "Anvisa";
        else if (lower.includes("stf")) fonte = "STF";
        else if (lower.includes("dou") || lower.includes("diário oficial")) fonte = "DOU";
        else if (lower.includes("minist")) fonte = "Ministério da Saúde";
        out.push({ titulo: it.title, fonte, data: (it.isoDate || it.pubDate || hojeISO()).slice(0, 10), url: it.link, tema: "regulatório-noticia" });
      }
      return feed.items || [];
    });
  }
  return out;
}
async function coletarAtosLei(m) {
  const cam = await coletarCamara(m.fontesLei.camaraKeywords);
  const sen = await coletarSenado(m.fontesLei.senadoKeywords);
  const reg = await coletarNewsRegulatorio(m.fontesLei.newsRegulatorio);
  const todos = [...cam, ...sen, ...reg].map(a => ({ ...a, impacto: classificarImpacto(a.titulo), origem: "auto" }));
  const g = new Map();
  for (const a of todos) {
    const k = hash(norm(a.titulo).slice(0, 60) + "|" + a.fonte);
    if (!g.has(k)) g.set(k, a);
  }
  return [...g.values()].sort((a, b) => b.data.localeCompare(a.data)).slice(0, 30);
}

// ============================================================
// NEGÓCIO — extração
// ============================================================
function extrairMovimentacoes(noticias) {
  const out = [];
  const vis = new Set();
  for (const n of noticias) {
    for (const t of NEGOCIO_TIPOS) {
      if (t.regex.test(n.titulo)) {
        const k = hash(t.tipo + "|" + norm(n.titulo).slice(0, 40));
        if (vis.has(k)) continue;
        vis.add(k);
        out.push({
          tipo: t.tipo,
          empresas: n.titulo.replace(/\s*[-—|]\s*[^-—|]+$/, "").trim().slice(0, 140),
          texto: n.veiculo,
          data: n.data,
          link: n.url,
          origem: "auto"
        });
        break;
      }
    }
  }
  return out.sort((a, b) => b.data.localeCompare(a.data)).slice(0, 25);
}

// ============================================================
// TRENDS — batched, resiliente, com quality metadata
// ============================================================
// A biblioteca google-trends-api aceita array em `keyword` para comparação
// (até 5 termos). Batching reduz drasticamente o número de chamadas.
//
// Retorna { termos, debug } — debug gravado em data/<id>-trends-debug.json
// para você poder inspecionar o que o wrapper realmente devolveu.
async function coletarTrendsLote(termos, geo, fallbackGeo, mercadoId) {
  const inicio = diasAtras(90);
  const debug = {
    executadoEm: hojeISO(),
    geoPrincipal: geo,
    geoFallback: fallbackGeo,
    termosCarregados: termos.length,
    lotes: [],
    canary: null,
    resumo: null
  };

  const chamadaTrends = async (tArr, geoUso) => {
    const raw = await googleTrends.interestOverTime({
      keyword: tArr,
      startTime: inicio,
      geo: geoUso
    });
    // Google às vezes devolve HTML de erro em vez de JSON — detectar cedo.
    if (raw.trim().startsWith("<")) {
      throw new Error("resposta não-JSON (HTML/erro): " + raw.slice(0, 80));
    }
    const j = JSON.parse(raw);
    return j?.default?.timelineData || [];
  };

  // ---- CANARY: termo notoriamente popular no Brasil ----
  // Se o canary funcionar mas os seus termos falharem = seus termos
  // realmente não têm volume detectável no Trends (esperado para
  // termos hiper-nichados como "biópsia Fortaleza"). Se o canary
  // falhar = o wrapper está bloqueado/quebrado e a causa é externa.
  log("INFO", "Trends[canary]", "testando 'instagram' em BR para validar wrapper…");
  try {
    const canaryPts = await chamadaTrends(["instagram"], "BR");
    const canaryValor = canaryPts.length ? (canaryPts[canaryPts.length - 1].value?.[0] ?? 0) : 0;
    debug.canary = { termo: "instagram", geo: "BR", pontos: canaryPts.length, ultimoValor: canaryValor, status: canaryValor > 0 ? "wrapper-ok" : "wrapper-suspeito" };
    log(canaryValor > 0 ? "OK" : "WARN", "Trends[canary]", `wrapper ${canaryValor > 0 ? "OK" : "SUSPEITO"} — 'instagram' devolveu último valor ${canaryValor} em ${canaryPts.length} pontos`);
  } catch (e) {
    debug.canary = { termo: "instagram", geo: "BR", erro: e.message, status: "wrapper-quebrado" };
    log("ERROR", "Trends[canary]", `wrapper QUEBRADO — ${e.message}`);
  }
  await sleep(2000);

  // helper para transformar série do Trends em ponto do painel
  function processarSerie(termo, pontosBrutos, indice, geoUsado) {
    if (!pontosBrutos.length) return null;
    // no formato comparativo, value é array [termo0, termo1, ...]
    const serie = pontosBrutos.map(p => (p.value && p.value[indice]) || 0);
    // amostra ~12 pontos
    const stride = Math.max(1, Math.floor(serie.length / 12));
    const amostra = [];
    for (let i = 0; i < serie.length; i += stride) amostra.push(serie[i]);
    if (amostra.length > 12) amostra.length = 12;
    const ultimo = amostra[amostra.length - 1] || 0;
    const anterior = amostra[amostra.length - 2] || 0;
    const incremento = anterior > 0 ? +(((ultimo - anterior) / anterior) * 100).toFixed(1) : 0;
    let tendencia = "estável";
    if (incremento > 5) tendencia = "alta";
    else if (incremento < -5) tendencia = "queda";
    // se todos os pontos foram 0, tratar como sem interesse detectado
    const totalNonZero = amostra.reduce((s, v) => s + (v > 0 ? 1 : 0), 0);
    const status = totalNonZero === 0 ? "sem-interesse-detectado" : "ok";
    return {
      termo,
      indiceInteresse: ultimo,      // 0-100 (Google Trends é relativo)
      volume: ultimo,                // legado — mantido para compat com frontend
      incremento,
      tendencia,
      serie: amostra,
      geo: geoUsado,
      status,
      fonte: "Google Trends",
      coletadoEm: hojeISO(),
      confiabilidade: totalNonZero === 0 ? "BAIXA" : "ALTA",
      url: "https://trends.google.com/trends/explore?q=" + encodeURIComponent(termo) + "&geo=" + geoUsado
    };
  }

  const out = [];
  const lotes = [];
  for (let i = 0; i < termos.length; i += 5) lotes.push(termos.slice(i, i + 5));

  log("INFO", "Trends", `${termos.length} termos → ${lotes.length} lote(s) de até 5`);

  for (let li = 0; li < lotes.length; li++) {
    const lote = lotes[li];
    const loteInfo = { indice: li + 1, termos: lote, tentativas: [] };
    let pontos = null;
    let geoUsado = geo;

    // Tentativa 1: geo principal
    try {
      pontos = await chamadaTrends(lote, geo);
      loteInfo.tentativas.push({ geo, status: "ok", pontos: pontos.length });
      log("OK", `Trends[${geo}] lote ${li + 1}/${lotes.length}`, `${lote.length} termos, ${pontos.length} pontos`);
    } catch (e) {
      loteInfo.tentativas.push({ geo, status: "erro", erro: e.message });
      log("WARN", `Trends[${geo}] lote ${li + 1}`, `falhou — ${e.message}`);
    }

    // Se veio vazio ou tudo zero, tenta fallback geo (BR)
    const todoZero = pontos && pontos.length && pontos.every(p => (p.value || []).every(v => v === 0));
    if (fallbackGeo && (!pontos || pontos.length === 0 || todoZero)) {
      await sleep(1500);
      try {
        pontos = await chamadaTrends(lote, fallbackGeo);
        geoUsado = fallbackGeo;
        loteInfo.tentativas.push({ geo: fallbackGeo, status: "ok-fallback", pontos: pontos.length });
        log("OK", `Trends[${fallbackGeo}] fallback lote ${li + 1}`, `${pontos.length} pontos`);
      } catch (e) {
        loteInfo.tentativas.push({ geo: fallbackGeo, status: "erro-fallback", erro: e.message });
        log("WARN", `Trends[${fallbackGeo}] fallback lote ${li + 1}`, `falhou — ${e.message}`);
      }
    }

    loteInfo.geoUsado = geoUsado;
    loteInfo.pontos = pontos ? pontos.length : 0;
    loteInfo.resultadosPorTermo = [];

    for (let i = 0; i < lote.length; i++) {
      const p = pontos ? processarSerie(lote[i], pontos, i, geoUsado) : null;
      if (p) {
        out.push(p);
        loteInfo.resultadosPorTermo.push({ termo: lote[i], status: p.status, ultimo: p.indiceInteresse });
      } else {
        const semDado = {
          termo: lote[i], indiceInteresse: null, volume: null, incremento: null,
          tendencia: "indisponível", serie: [], geo, status: "indisponivel",
          fonte: "Google Trends", coletadoEm: null, confiabilidade: "SEM_DADO",
          url: "https://trends.google.com/trends/explore?q=" + encodeURIComponent(lote[i]) + "&geo=" + geo
        };
        out.push(semDado);
        loteInfo.resultadosPorTermo.push({ termo: lote[i], status: "indisponivel" });
      }
    }

    debug.lotes.push(loteInfo);
    await sleep(2500);
  }

  const comDados   = out.filter(t => t.status === "ok").length;
  const semDados   = out.filter(t => t.confiabilidade === "SEM_DADO").length;
  const semInteresse = out.filter(t => t.status === "sem-interesse-detectado").length;
  debug.resumo = { comDados, semDados, semInteresse, total: out.length };
  log("INFO", "Trends[resumo]", `${comDados}/${out.length} com dados · ${semInteresse} sem interesse detectado · ${semDados} sem dado (erro)`);

  // Escreve arquivo de diagnóstico para você inspecionar no repo depois
  if (mercadoId) {
    try {
      await writeFile(`data/${mercadoId}-trends-debug.json`, JSON.stringify(debug, null, 2), "utf8");
      log("OK", "Trends[debug]", `escrito em data/${mercadoId}-trends-debug.json`);
    } catch (e) {
      log("WARN", "Trends[debug]", `falha ao gravar debug — ${e.message}`);
    }
  }

  return out;
}

// Merge preserva o último valor válido por termo se o novo veio indisponível.
function mergeTermosBusca(novos, anteriores) {
  const porTermo = new Map();
  for (const a of (anteriores || [])) porTermo.set(a.termo, a);
  for (const n of novos) {
    const prev = porTermo.get(n.termo);
    if (n.status === "ok") {
      porTermo.set(n.termo, n);
    } else if (prev && (prev.status === "ok" || prev.confiabilidade === "ALTA")) {
      // preserva o antigo mas marca stale
      porTermo.set(n.termo, { ...prev, stale: true, ultimaTentativa: hojeISO(), motivoFalha: n.status });
    } else {
      porTermo.set(n.termo, n);
    }
  }
  return [...porTermo.values()];
}

// ============================================================
// CONCORRÊNCIA — coleta com qualidade + snapshot semanal
// ============================================================
// Matcher robusto para menções: normaliza, lida com parênteses e alias.
function chavesConcorrente(c) {
  const chaves = new Set();
  // nome principal, sem parênteses
  const semPar = (c.nome || "").replace(/\([^)]*\)/g, "").trim();
  if (semPar) chaves.add(norm(semPar));
  // nome completo original
  chaves.add(norm(c.nome));
  // conteúdo dos parênteses (sigla)
  const par = (c.nome || "").match(/\(([^)]+)\)/);
  if (par) chaves.add(norm(par[1]));
  // grupo
  if (c.grupo) chaves.add(norm(c.grupo));
  // aliases
  for (const a of (c.alias || [])) if (a) chaves.add(norm(a));
  // primeira palavra significativa (fallback fraco)
  const primeiro = norm((c.nome || "").split(/[\s—]/)[0]);
  if (primeiro && primeiro.length >= 4) chaves.add(primeiro);
  return [...chaves].filter(k => k.length >= 3);
}

async function coletarTrendsMarca(nomeMarca, geo, fallbackGeo) {
  // 1 chamada por marca (poderia ser batched, mas nomes de marca geram
  // muitos zeros em BR-CE — melhor fazer individual com fallback rápido)
  try {
    let raw = await googleTrends.interestOverTime({
      keyword: nomeMarca,
      startTime: diasAtras(90),
      geo: fallbackGeo || geo
    });
    const j = JSON.parse(raw);
    const pts = j?.default?.timelineData || [];
    if (!pts.length) return semDado("Trends vazio para marca");
    const serie = pts.map(p => (p.value && p.value[0]) || 0);
    const stride = Math.max(1, Math.floor(serie.length / 8));
    const amostra = [];
    for (let i = 0; i < serie.length; i += stride) amostra.push(serie[i]);
    if (amostra.length > 8) amostra.length = 8;
    const totalNonZero = amostra.reduce((s, v) => s + (v > 0 ? 1 : 0), 0);
    if (totalNonZero === 0) return semDado("Sem interesse detectado no período");
    return {
      valor: amostra[amostra.length - 1],
      serie: amostra,
      fonte: "Google Trends",
      coletadoEm: hojeISO(),
      confiabilidade: totalNonZero >= 4 ? "ALTA" : "MEDIA",
      evidencia: true
    };
  } catch (e) {
    return semDado(`Trends falhou: ${e.message}`);
  }
}

function contarMencoesReais(concorrente, noticias, dias = 30) {
  const cutoff = diasAtras(dias).toISOString().slice(0, 10);
  const chaves = chavesConcorrente(concorrente);
  let count = 0;
  const evidencias = [];
  for (const n of noticias) {
    if (n.data < cutoff) continue;
    const tn = norm(n.titulo);
    for (const k of chaves) {
      if (tn.includes(k)) { count++; evidencias.push({ titulo: n.titulo, url: n.url, data: n.data }); break; }
    }
  }
  return { count, evidencias: evidencias.slice(0, 5) };
}

// ============================================================
// APIFY — Google Maps + Instagram (opt-in via APIFY_API_TOKEN)
// ============================================================
// Regras (custo controlado):
//  - só roda se APIFY_API_TOKEN estiver definido no ambiente
//  - roda no workflow SEMANAL (10 concorrentes × 4 semanas = 40 items/mês)
//  - cada Actor limitado ao mínimo (1 item por concorrente)
//  - token NUNCA aparece em log
//  - falha silenciosa → SEM_DADO (não derruba pipeline)
//
// Actors (verificados na store Apify — free tier ~$5 USD/mês):
//   Google Maps: compass/crawler-google-places
//   Instagram:   apify/instagram-profile-scraper
//
// Consumo estimado por semana:
//   Google Maps: ~10 chamadas = ~0.5 USD (dentro do free tier)
//   Instagram:   ~10 chamadas = ~0.3 USD (dentro do free tier)

const APIFY_TOKEN = process.env.APIFY_API_TOKEN || "";
const APIFY_ATIVO = !!APIFY_TOKEN;

async function apifyRun(actorId, input, timeoutMs = 90000) {
  // roda o actor de forma síncrona (run-sync-get-dataset-items) — retorna array já pronto
  const url = `https://api.apify.com/v2/acts/${actorId.replace("/", "~")}/run-sync-get-dataset-items?token=${APIFY_TOKEN}&timeout=${Math.floor(timeoutMs/1000)}`;
  const r = await fetchTimeout(url, timeoutMs + 5000, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(input)
  });
  if (!r.ok) throw new Error(`Apify HTTP ${r.status}`);
  return await r.json();
}

async function apifyGoogleMaps(concorrente) {
  if (!APIFY_ATIVO) return semDado("APIFY_API_TOKEN ausente — Google Maps não coletado");
  try {
    // Actor compass/crawler-google-places aceita URLs OU query textual.
    // Preferimos URL direta (mais estável); fallback: query "nome + cidade".
    const input = concorrente.googleMapsUrl
      ? { startUrls: [{ url: concorrente.googleMapsUrl }], maxCrawledPlacesPerSearch: 1, language: "pt-BR", scrapeReviewsCount: 0, includeWebResults: false }
      : { searchStringsArray: [`${concorrente.nome} Fortaleza`], maxCrawledPlacesPerSearch: 1, language: "pt-BR", scrapeReviewsCount: 0, includeWebResults: false };
    const items = await apifyRun("compass/crawler-google-places", input, 90000);
    if (!items || !items.length) return semDado("Google Maps: nenhum resultado retornado");
    const p = items[0];
    return {
      valor: {
        avaliacao: p.totalScore ?? p.rating ?? null,
        reviews: p.reviewsCount ?? p.reviews_count ?? null,
        endereco: p.address ?? null,
        categoria: p.categoryName ?? p.category ?? null,
        placeId: p.placeId ?? p.place_id ?? null,
        website: p.website ?? null
      },
      fonte: "Google Maps (via Apify compass/crawler-google-places)",
      coletadoEm: hojeISO(),
      confiabilidade: (p.totalScore || p.rating) ? "ALTA" : "MEDIA",
      evidencia: true,
      urlReferencia: p.url || concorrente.googleMapsUrl || null
    };
  } catch (e) {
    return semDado(`Google Maps (Apify) falhou: ${e.message}`);
  }
}

async function apifyInstagram(concorrente) {
  if (!APIFY_ATIVO) return semDado("APIFY_API_TOKEN ausente — Instagram não coletado");
  if (!concorrente.instagramHandle) return semDado("Handle Instagram não configurado");
  try {
    const handle = concorrente.instagramHandle.replace(/^@/, "");
    const input = {
      usernames: [handle],
      resultsLimit: 1,           // só o perfil, sem posts
      scrapePostsUntilDate: null // não puxa histórico
    };
    const items = await apifyRun("apify/instagram-profile-scraper", input, 60000);
    if (!items || !items.length) return semDado("Instagram: perfil não encontrado");
    const p = items[0];
    return {
      valor: {
        seguidores: p.followersCount ?? null,
        seguindo: p.followsCount ?? null,
        publicacoes: p.postsCount ?? null,
        nome: p.fullName ?? null,
        verificado: p.verified ?? false
      },
      fonte: "Instagram (via Apify apify/instagram-profile-scraper)",
      coletadoEm: hojeISO(),
      confiabilidade: p.followersCount != null ? "ALTA" : "MEDIA",
      evidencia: true,
      handle: "@" + handle,
      urlReferencia: `https://instagram.com/${handle}`
    };
  } catch (e) {
    return semDado(`Instagram (Apify) falhou: ${e.message}`);
  }
}

// ============================================================
async function coletarConcorrentesSemanal(cfg, noticias, previo) {
  const concs = cfg.concorrentesMonitorados || [];
  const seed = (cfg.seedHistorico && cfg.seedHistorico.concorrentes) || [];
  const mapSeed = new Map(seed.map(s => [norm(s.nome), s]));

  const out = [];
  for (const c of concs) {
    log("INFO", `Concorrência:${c.nome}`, "iniciando");

    // 1. Menções na imprensa (30d) — dado REAL
    const men = contarMencoesReais(c, noticias, 30);
    const mencoes30d = {
      valor: men.count,
      fonte: "Google News + RSS imprensa (dedup)",
      coletadoEm: hojeISO(),
      confiabilidade: "ALTA",
      evidencia: men.count > 0,
      exemplos: men.evidencias
    };
    log("OK", `Mencoes:${c.nome}`, `${men.count} nos últimos 30d`);

    // 2. Interesse relativo (Google Trends por marca) — dado REAL quando possível
    const interesseTrends = await coletarTrendsMarca(c.nome, cfg.geoTrendsPrincipal, cfg.geoTrendsFallback);
    if (interesseTrends.confiabilidade !== "SEM_DADO") log("OK", `TrendsMarca:${c.nome}`, `valor ${interesseTrends.valor}`);
    else log("WARN", `TrendsMarca:${c.nome}`, interesseTrends.motivo || "sem dado");
    await sleep(1000);

    // 3. Google Maps (nota + reviews) — via Apify (opt-in via APIFY_API_TOKEN)
    const googleMaps = await apifyGoogleMaps(c);
    if (googleMaps.confiabilidade !== "SEM_DADO") log("OK", `Maps:${c.nome}`, `avaliação ${googleMaps.valor.avaliacao} · ${googleMaps.valor.reviews} reviews`);
    else log(APIFY_ATIVO ? "WARN" : "INFO", `Maps:${c.nome}`, googleMaps.motivo);
    await sleep(1500);

    // 4. Instagram — via Apify (opt-in via APIFY_API_TOKEN)
    const instagramFollowers = await apifyInstagram(c);
    if (instagramFollowers.confiabilidade !== "SEM_DADO") log("OK", `Instagram:${c.nome}`, `${instagramFollowers.valor.seguidores} seguidores`);
    else log(APIFY_ATIVO ? "WARN" : "INFO", `Instagram:${c.nome}`, instagramFollowers.motivo);
    await sleep(1500);

    // 5. Seed histórico (valores manuais originais) — preservado com etiqueta clara
    const seedC = mapSeed.get(norm(c.nome));
    const historicoManual = seedC ? {
      gmn:  { valor: seedC.gmn, fonte: "seed-manual",  coletadoEm: "histórico", confiabilidade: "BAIXA", evidencia: false },
      seg:  { valor: seedC.seg, fonte: "seed-manual",  coletadoEm: "histórico", confiabilidade: "BAIXA", evidencia: false },
      eng:  { valor: seedC.eng, fonte: "seed-manual",  coletadoEm: "histórico", confiabilidade: "BAIXA", evidencia: false }
    } : null;

    out.push({
      nome: c.nome,
      grupo: c.grupo,
      url: c.url,
      instagramHandle: c.instagramHandle || null,
      googleMapsUrl: c.googleMapsUrl || null,
      indicadores: {
        mencoes30d,
        interesseTrends,
        googleMaps,          // {avaliacao, reviews} — SEM_DADO até integrar
        instagramFollowers,  // SEM_DADO até integrar
        historicoManual      // valores originais preservados, marcados como BAIXA
      }
    });
  }
  return out;
}

// snapshot compacto para history (só o essencial)
function snapshotConcorrentes(concorrentes) {
  return {
    data: hojeYMD(),
    coletadoEm: hojeISO(),
    concorrentes: concorrentes.map(c => ({
      nome: c.nome,
      mencoes30d: c.indicadores?.mencoes30d?.valor ?? null,
      interesseTrends: c.indicadores?.interesseTrends?.valor ?? null,
      gmnAvaliacao: c.indicadores?.googleMaps?.valor?.avaliacao ?? null,
      gmnReviews: c.indicadores?.googleMaps?.valor?.reviews ?? null,
      instagramFollowers: c.indicadores?.instagramFollowers?.valor ?? null
    }))
  };
}

async function atualizarHistorico(id, novoSnapshot) {
  const hist = await lerHistorico(id);
  // se já tem snapshot da mesma semana, substitui
  const semanaAtual = novoSnapshot.data.slice(0, 10);
  hist.snapshots = hist.snapshots.filter(s => s.data !== semanaAtual);
  hist.snapshots.push(novoSnapshot);
  // mantém apenas os últimos 5 snapshots (~30 dias)
  hist.snapshots.sort((a, b) => b.data.localeCompare(a.data));
  hist.snapshots = hist.snapshots.slice(0, 5);
  await writeFile(`data/${id}-history.json`, JSON.stringify(hist, null, 2), "utf8");
  log("OK", "History", `${hist.snapshots.length} snapshots mantidos`);
  return hist;
}

// Calcula evolução 30d por indicador comparando snapshot atual com o mais antigo
// disponível na janela de 30 dias. Se histórico for insuficiente, marca claramente.
function calcularEvolucao30d(concorrentes, historico) {
  if (!historico || !historico.snapshots || historico.snapshots.length < 2) {
    for (const c of concorrentes) {
      c.evolucao30d = { status: "historico-insuficiente", motivo: "Mínimo 2 snapshots necessários" };
    }
    return;
  }
  const snapshots = historico.snapshots.slice().sort((a, b) => b.data.localeCompare(a.data));
  const atual = snapshots[0];
  const antigo = snapshots[snapshots.length - 1];
  const diasEntre = Math.round((new Date(atual.data) - new Date(antigo.data)) / (86400 * 1000));

  function delta(vAtual, vAntigo) {
    if (vAtual == null || vAntigo == null) return null;
    const diff = vAtual - vAntigo;
    const pct = vAntigo > 0 ? +((diff / vAntigo) * 100).toFixed(1) : null;
    return { diff, pct, diasEntre };
  }

  for (const c of concorrentes) {
    const snapA = atual.concorrentes.find(x => x.nome === c.nome);
    const snapB = antigo.concorrentes.find(x => x.nome === c.nome);
    if (!snapA || !snapB) { c.evolucao30d = { status: "concorrente-novo-no-monitoramento" }; continue; }
    c.evolucao30d = {
      status: "ok",
      periodo: `${antigo.data} → ${atual.data}`,
      diasEntre,
      mencoes:    delta(snapA.mencoes30d, snapB.mencoes30d),
      interesseTrends: delta(snapA.interesseTrends, snapB.interesseTrends),
      gmnAvaliacao:   delta(snapA.gmnAvaliacao, snapB.gmnAvaliacao),
      gmnReviews:     delta(snapA.gmnReviews, snapB.gmnReviews),
      instagramFollowers: delta(snapA.instagramFollowers, snapB.instagramFollowers)
    };
  }
}

// ============================================================
// OPORTUNIDADES — cruzamento com sinais reais
// ============================================================
function gerarOportunidades({ noticias, atosLei, movimentacoes, termosBusca, concorrentes, historico }) {
  const ops = [];

  // 1. Termos com incremento significativo → conteúdo
  for (const t of (termosBusca || [])) {
    if (t.status === "ok" && t.incremento >= 15) {
      ops.push({
        titulo: `Capturar demanda em alta — "${t.termo}"`,
        descricao: `Índice de interesse (Google Trends) subiu ${t.incremento}%. Recomenda-se ativar SEO on-page + campanha e revisar landing usando "${t.termo}" como H1.`,
        tipo: "conteudo-conversao",
        prioridade: t.incremento >= 30 ? "ALTA" : "MÉDIA",
        sinais: [{ fonte: "Google Trends", termo: t.termo, incremento: t.incremento, coletadoEm: t.coletadoEm }],
        recomendacao: "Ativar campanha + atualizar SEO on-page.",
        origem: "auto",
        confianca: t.confiabilidade === "ALTA" ? "ALTA" : "MEDIA"
      });
    }
  }

  // 2. Ato ALTO impacto → conformidade
  for (const a of (atosLei || [])) {
    if (a.impacto === "ALTO") {
      ops.push({
        titulo: `Adequação regulatória — ${a.titulo.slice(0, 80)}`,
        descricao: `Ato de impacto ALTO por ${a.fonte}. Rodada com jurídico e comunicação para adaptar materiais e contratos.`,
        tipo: "adequacao-regulatoria",
        prioridade: "ALTA",
        sinais: [{ fonte: a.fonte, url: a.url, data: a.data }],
        recomendacao: "Convocar jurídico + comunicação.",
        origem: "auto",
        confianca: "ALTA"
      });
    }
  }

  // 3. Concorrente com muitas menções → monitoramento
  for (const c of (concorrentes || [])) {
    const m = c.indicadores?.mencoes30d?.valor || 0;
    if (m >= 3) {
      ops.push({
        titulo: `Concorrente ativo — ${c.nome}`,
        descricao: `${c.nome} foi mencionado ${m} vezes na imprensa nos últimos 30 dias. Recomenda-se aprofundar análise de conteúdo e monitorar movimentos.`,
        tipo: "monitoramento-competitivo",
        prioridade: m >= 6 ? "ALTA" : "MÉDIA",
        sinais: [{ fonte: "clipping-30d", concorrente: c.nome, mencoes: m, exemplos: c.indicadores.mencoes30d.exemplos }],
        recomendacao: "Battlecard atualizado + acompanhar tom e frequência.",
        origem: "auto",
        confianca: "ALTA"
      });
    }
  }

  // 4. Concorrente com crescimento de menções (comparando snapshots)
  if (historico && historico.snapshots && historico.snapshots.length >= 2) {
    const atual = historico.snapshots[0];
    const antigo = historico.snapshots[historico.snapshots.length - 1];
    const diasEntre = (new Date(atual.data) - new Date(antigo.data)) / (1000 * 60 * 60 * 24);
    for (const cA of atual.concorrentes) {
      const cB = antigo.concorrentes.find(x => x.nome === cA.nome);
      if (!cB) continue;
      const delta = (cA.mencoes30d || 0) - (cB.mencoes30d || 0);
      if (delta >= 3) {
        ops.push({
          titulo: `Crescimento de mídia — ${cA.nome}`,
          descricao: `Menções à ${cA.nome} passaram de ${cB.mencoes30d} para ${cA.mencoes30d} nos últimos ~${Math.round(diasEntre)} dias. Sinal de campanha ou acontecimento relevante.`,
          tipo: "sinal-competitivo",
          prioridade: delta >= 6 ? "ALTA" : "MÉDIA",
          sinais: [{ fonte: "snapshot-comparativo", inicio: antigo.data, fim: atual.data, delta }],
          recomendacao: "Investigar causa e considerar resposta comunicacional.",
          origem: "auto",
          confianca: "ALTA"
        });
      }
    }
  }

  // 5. Movimentação estratégica → posicionamento
  for (const m of (movimentacoes || []).slice(0, 5)) {
    ops.push({
      titulo: `Janela de posicionamento — ${m.tipo}: ${m.empresas.slice(0, 60)}`,
      descricao: `Movimentação "${m.tipo}" em ${m.data}. Momento de reforçar mensagens de solidez e diferenciação.`,
      tipo: "posicionamento",
      prioridade: "MÉDIA",
      sinais: [{ fonte: m.texto, url: m.link, data: m.data }],
      recomendacao: "Pauta de conteúdo reforçando atributos comparativos.",
      origem: "auto",
      confianca: "MEDIA"
    });
  }

  return ops.slice(0, 15);
}

// ============================================================
// ORQUESTRADOR
// ============================================================
async function processarMercado(m) {
  console.log("\n════════════════════════════════════════");
  console.log(` ${m.nome}  (mode: ${RUN_MODE})`);
  console.log("════════════════════════════════════════\n");

  const cfg = await lerConfig(m.id) || {};
  const previo = await lerAnterior(m.id) || {};

  let noticias      = previo.noticias      || [];
  let atosLei       = previo.atosLei       || [];
  let movimentacoes = previo.movimentacoes || [];
  let termosBusca   = previo.termosBusca   || [];
  let concorrentes  = previo.concorrentes  || [];
  let oportunidades = previo.oportunidades || [];

  // -------- DAILY: notícias + leis + negócios --------
  if (RUN_MODE === "daily" || RUN_MODE === "all") {
    const gnews    = await coletarGoogleNews(cfg.termosBusca || []);
    const imprensa = await coletarImprensa(m.feedsImprensa);
    const brutos = dedupNoticias([...gnews, ...imprensa]);
    const noticiasNovas = curarNoticias(brutos);
    noticias = preservar(noticiasNovas, previo.noticias);

    const atosLeiNovos = await coletarAtosLei(m);
    atosLei = preservar(atosLeiNovos, previo.atosLei);

    const movsNovas = extrairMovimentacoes(noticias);
    movimentacoes = preservar(movsNovas, previo.movimentacoes);
  }

  // -------- WEEKLY: concorrência + trends + oportunidades --------
  let historico = await lerHistorico(m.id);
  if (RUN_MODE === "weekly" || RUN_MODE === "all") {
    // Trends dos termos
    const termos = cfg.termosBusca || [];
    log("INFO", "Trends", `${termos.length} termos em ${Math.ceil(termos.length/5)} lote(s) de 5`);
    const trendsNovos = await coletarTrendsLote(termos, cfg.geoTrendsPrincipal || "BR-CE", cfg.geoTrendsFallback || "BR", m.id);
    termosBusca = mergeTermosBusca(trendsNovos, previo.termosBusca);

    // Concorrentes (usa notícias já atualizadas)
    concorrentes = await coletarConcorrentesSemanal(cfg, noticias, previo);

    // Snapshot semanal → history
    const snap = snapshotConcorrentes(concorrentes);
    historico = await atualizarHistorico(m.id, snap);

    // Enriquece cada concorrente com evolução 30d comparando snapshots
    calcularEvolucao30d(concorrentes, historico);

    // Oportunidades (usa todos os sinais + evoluções)
    oportunidades = gerarOportunidades({ noticias, atosLei, movimentacoes, termosBusca, concorrentes, historico });
  } else {
    // No modo daily preservamos os campos "weekly" já existentes.
    concorrentes  = previo.concorrentes  || [];
    termosBusca   = previo.termosBusca   || [];
    oportunidades = previo.oportunidades || [];
  }

  const payload = {
    id: m.id,
    nome: m.nome,
    setor: m.setor,
    segmento: m.segmento,
    cidade: m.cidade,
    estado: m.estado,
    atualizadoEm: hojeISO(),
    ultimaExecucao: { mode: RUN_MODE, em: hojeISO() },
    proximaAtualizacao: RUN_MODE === "weekly" ? "concorrência semanal" : "notícias/leis diárias",
    noticias,
    atosLei,
    movimentacoes,
    termosBusca,
    concorrentes,
    canais: preservar([], previo.canais),
    fontesLegais: preservar([], previo.fontesLegais),
    oportunidades,
    noticiasCuradas: (previo && previo.noticiasCuradas) || [],
    origem: { motor: "ingest.mjs", versao: "2.1", mode: RUN_MODE }
  };

  if (!Array.isArray(payload.noticias)) throw new Error("payload inválido: noticias ausente");

  await mkdir("data", { recursive: true });
  await writeFile(`data/${m.id}.json`, JSON.stringify(payload, null, 2), "utf8");

  console.log("\n── RESUMO ─────────────────────────────");
  console.log(`  modo:          ${RUN_MODE}`);
  console.log(`  notícias:      ${noticias.length} (incluídas: ${noticias.filter(n => n.incluir).length})`);
  console.log(`  atos lei:      ${atosLei.length}`);
  console.log(`  movimentações: ${movimentacoes.length}`);
  console.log(`  termos busca:  ${termosBusca.length} (ok: ${termosBusca.filter(t => t.status === 'ok').length}, stale: ${termosBusca.filter(t => t.stale).length}, sem-dado: ${termosBusca.filter(t => t.confiabilidade === 'SEM_DADO').length})`);
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
  console.error("\n✖ ingestão concluída com erros");
  process.exit(1);
}
console.log(`✔ ingestão (mode=${RUN_MODE}) concluída`);
