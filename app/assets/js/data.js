// Carregamento dos artefatos publicados e funções de domínio sobre eles.
// Cada arquivo traz { meta, data } conforme app/DATA_CONTRACT.md.
import { CONFIG } from "./config.js";
import { resolvePeriod } from "./store.js";
import { diffDays, addDays } from "./util.js";

const cache = new Map();
let manifest = null;

export async function loadManifest() {
  if (manifest) return manifest;
  // Só o 404 (pasta ainda não publicada) cai para a próxima; qualquer outro erro é mostrado, para não
  // trocar dados reais por demonstração em silêncio durante uma falha. O banner de demonstração é permanente.
  let ultimo = null;
  for (const root of CONFIG.DATA_ROOTS) {
    const res = await fetch(`${root}/manifest.json`, { cache: "no-cache" });
    if (res.status === 404) { ultimo = `${root}/manifest.json não existe`; continue; }
    if (!res.ok) throw new Error(`${root}/manifest.json respondeu ${res.status}`);
    CONFIG.DATA_ROOT = root;
    manifest = await res.json();
    return manifest;
  }
  throw new Error(ultimo || "nenhum manifest.json encontrado");
}
export const getManifest = () => manifest;
export const isDemo = () => manifest?.modo === "demo";

// Retorna sempre um objeto de estado: { status: "ok" | "vazio" | "erro" | "nao_publicado", meta, data, error }
export function load(id) {
  if (cache.has(id)) return cache.get(id);
  const ds = manifest?.datasets.find((d) => d.id === id);
  const p = (async () => {
    if (!ds) return { status: "nao_publicado", id };
    try {
      const res = await fetch(`${CONFIG.DATA_ROOT}/${ds.arquivo}`);
      if (!res.ok) throw new Error(`${ds.arquivo} respondeu ${res.status}`);
      const json = await res.json();
      const vazio = Array.isArray(json.data) ? json.data.length === 0 : json.meta?.registros === 0;
      return { status: vazio ? "vazio" : "ok", meta: json.meta, data: json.data, id };
    } catch (error) {
      cache.delete(id); // permite nova tentativa
      return { status: "erro", error, id, meta: ds };
    }
  })();
  cache.set(id, p);
  return p;
}
// Só os metadados: usa a cópia do manifesto quando existe (evita baixar setores e vias, de vários MB).
export function loadMeta(id) {
  const ds = manifest?.datasets.find((d) => d.id === id);
  return ds?.meta ? Promise.resolve({ status: "ok", meta: ds.meta, id }) : load(id);
}
export const loadMany = (ids) => Promise.all(ids.map(load)).then((arr) => Object.fromEntries(arr.map((r) => [r.id, r])));

// Atualização: compara a referência do dado com o relógio do usuário.
export function freshness(meta, key = meta?.id) {
  if (!meta) return { estado: "desconhecido" };
  if (meta.data_referencia && !meta.referencia) return { estado: "estatico", ref: meta.data_referencia };
  const refIso = meta.referencia || (meta.periodo?.fim ? `${meta.periodo.fim}T23:59:00-03:00` : meta.publicado_em);
  const horas = (Date.now() - Date.parse(refIso)) / 3600000;
  const limite = CONFIG.STALE_HOURS[key] ?? CONFIG.STALE_HOURS.default;
  return { estado: horas > limite ? "desatualizado" : "atual", ref: refIso, horas, limite };
}

/* ------------------------------------------------------------------ ocorrências */
let occCache = null;
export async function getOccurrences() {
  const r = await load("ocorrencias");
  if (r.status !== "ok") return { ...r, rows: [] };
  if (!occCache) {
    const c = r.data;
    occCache = c.id.map((_, i) => ({
      key: `${c.ano_origem[i]}-${c.id[i]}`,
      ano_origem: c.ano_origem[i],
      id: c.id[i],
      data: c.data[i],
      tipo: c.tipo[i],
      subprefeitura: c.subprefeitura[i],
      lat: c.lat[i],
      lon: c.lon[i],
      grau: c.grau_susc_inundacao[i],
    }));
  }
  return { ...r, rows: occCache };
}

export function filterOccurrences(rows, f, period = resolvePeriod(f)) {
  const tipos = f.tipos?.length ? new Set(f.tipos) : null;
  return rows.filter(
    (o) =>
      o.data >= period.inicio &&
      o.data <= period.fim &&
      (!tipos || tipos.has(o.tipo)) &&
      (!f.subprefeitura || o.subprefeitura === f.subprefeitura),
  );
}

/* ---------------------------------------------------------------------- estações */
export async function getStations() {
  const [est, diaria, horaria] = await Promise.all([load("estacoes"), load("chuva_diaria"), load("chuva_horaria")]);
  return { est, diaria, horaria };
}

export function filterStations(list, f) {
  return list.filter((s) => (!f.rede || s.rede === f.rede) && (!f.subprefeitura || s.subprefeitura === f.subprefeitura));
}

// Soma da chuva diária no período; cobertura = dias com leitura / dias do período.
export function rainInPeriod(diaria, id, inicio, fim) {
  const serie = diaria?.data?.series?.[id];
  if (!serie) return { mm: null, cobertura: 0 };
  const base = diaria.data.inicio;
  const i0 = Math.max(0, diffDays(base, inicio));
  const i1 = Math.min(serie.length - 1, diffDays(base, fim));
  let mm = 0, n = 0;
  for (let i = i0; i <= i1; i++) {
    if (serie[i] !== null) {
      mm += serie[i];
      n++;
    }
  }
  const total = i1 - i0 + 1;
  return { mm: n ? mm : null, cobertura: total > 0 ? n / total : 0, dias: total };
}

// Janela móvel nas últimas `horas` da série horária (termina na referência).
export function rainLastHours(horaria, id, horas) {
  const serie = horaria?.data?.series?.[id];
  if (!serie) return { mm: null, cobertura: 0 };
  const fatia = serie.slice(-horas);
  const validos = fatia.filter((v) => v !== null);
  return { mm: validos.length ? validos.reduce((a, b) => a + b, 0) : null, cobertura: validos.length / horas };
}

// Sensor cadastrado sem leitura no período publicado: série só de nulos, do mesmo tamanho das demais.
export function serieNivel(nivel, tipo, id) {
  const series = nivel.data[tipo];
  return series[id] || new Array(Object.values(series)[0]?.length || 0).fill(null);
}

export function hourlyTimes(horaria) {
  const start = Date.parse(horaria.data.inicio);
  const n = Object.values(horaria.data.series)[0]?.length || 0;
  return Array.from({ length: n }, (_, i) => new Date(start + i * 3600000).toISOString());
}

// Média diária das estações (chuva "da cidade") — base para séries e análises.
let cityDailyCache = null;
export function cityDaily(diaria, ids) {
  const key = ids ? ids.join(",") : "all";
  if (cityDailyCache?.key === key) return cityDailyCache.value;
  const series = Object.entries(diaria.data.series).filter(([id]) => !ids || ids.includes(id)).map(([, s]) => s);
  const n = series[0]?.length || 0;
  const media = new Array(n), max = new Array(n);
  for (let i = 0; i < n; i++) {
    let s = 0, c = 0, m = null;
    for (const serie of series) {
      const v = serie[i];
      if (v !== null) {
        s += v;
        c++;
        if (m === null || v > m) m = v;
      }
    }
    media[i] = c ? s / c : null;
    max[i] = m;
  }
  const value = { inicio: diaria.data.inicio, media, max };
  cityDailyCache = { key, value };
  return value;
}

export const dayIndex = (base, day) => diffDays(base, day);
export const dayAt = (base, i) => addDays(base, i);

// Lista de meses AAAA-MM entre duas datas.
export function monthsBetween(inicio, fim) {
  const out = [];
  let [y, m] = inicio.slice(0, 7).split("-").map(Number);
  const [y1, m1] = fim.slice(0, 7).split("-").map(Number);
  while (y < y1 || (y === y1 && m <= m1)) {
    out.push(`${y}-${String(m).padStart(2, "0")}`);
    m++;
    if (m > 12) { m = 1; y++; }
  }
  return out;
}

export function bboxOf(points) {
  if (!points.length) return null;
  let a = Infinity, b = Infinity, c = -Infinity, d = -Infinity;
  for (const p of points) {
    if (p.lon < a) a = p.lon;
    if (p.lat < b) b = p.lat;
    if (p.lon > c) c = p.lon;
    if (p.lat > d) d = p.lat;
  }
  return [[a, b], [c, d]];
}
