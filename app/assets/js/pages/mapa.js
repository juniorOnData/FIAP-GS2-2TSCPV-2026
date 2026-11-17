// Mapa principal (F01–F05, F10, F12): camadas, filtros, legenda, detalhe e insights.
import { CONFIG } from "../config.js";
import { icon } from "../icons.js";
import { esc, fmtInt, fmt1, fmt2, fmtDay, fmtDateTime, groupCount, downloadText, $ } from "../util.js";
import { store, resolvePeriod } from "../store.js";
import { getOccurrences, filterOccurrences, getStations, filterStations, rainInPeriod, rainLastHours, load, bboxOf, isDemo, getManifest, hourlyTimes } from "../data.js";
import { filterBar, bindFilterBar, subprefNome, stateBlock, resultState, natureBadge, freshnessBadge, badge, provenance, DEMO_MSG } from "../ui.js";
import { sparkline, TYPE_COLORS } from "../charts.js";

// Rampa de chuva para o mapa: começa num azul visível sobre o mapa base claro (o branco fica para "sem leitura").
const RAIN_RAMP = ["#DCEBFC", "#B7D3F6", "#86B6EF", "#5598E7", "#2A78D6", "#1C5CAB", "#104281", "#0D366B"];
import { computeInsights, insightCard } from "../insights.js";

const SUSC_COLORS = { 1: "#E4DEFB", 2: "#B4A7EF", 3: "#6250D6" };
const FONT = ["Montserrat SemiBold", "Open Sans Bold"];
const EMPTY = { type: "FeatureCollection", features: [] };

let map = null;
let ctx = null; // dados carregados
let root = null;
let lastSubpref = null;
const view = {
  layers: { ocorrencias: true, chuva: true, nivel: true, suscetibilidade: false },
  rainWindow: "24h",
  basemapOk: true,
};

export default {
  subtitle: "Exploração territorial do município de São Paulo",
  async render(r, { params }) {
    root = r;
    lastSubpref = null;
    if (params.get("camada")) view.layers[params.get("camada")] = true;
    root.innerHTML = `<div class="page page--full"><div class="mapview" id="mapview">
      <aside class="mapside" id="mapside" aria-label="Camadas, filtros e insights">
        <button type="button" class="mapside__toggle btn btn--neutral" id="side-toggle" aria-expanded="false" aria-controls="mapside-body">${icon("layers")}Camadas, filtros e insights</button>
        <div class="mapside__body" id="mapside-body">
          <section class="mapside__sec"><h3 class="mapside__h">${icon("filter")}Filtros</h3><div id="m-filters"></div></section>
          <section class="mapside__sec"><h3 class="mapside__h">${icon("layers")}Camadas</h3><div id="m-layers"></div></section>
          <section class="mapside__sec"><h3 class="mapside__h">${icon("sparkle")}Insights do recorte</h3><p class="note">Padrões descritivos dos dados filtrados. Não são previsões nem alertas.</p><div id="m-insights">${stateBlock("loading", "Calculando…")}</div></section>
          <section class="mapside__sec"><h3 class="mapside__h">${icon("table")}Registros no recorte</h3><div id="m-list"></div></section>
        </div>
      </aside>
      <div class="mapwrap">
        <div id="map" class="map" role="region" aria-label="Mapa do município de São Paulo. Use as setas para mover e + ou − para aproximar."></div>
        <div class="map-overlay map-overlay--tl" id="m-scope" aria-live="polite"></div>
        <div class="map-overlay map-overlay--bl" id="m-notice"></div>
        ${isDemo() ? `<div class="map-demo">${icon("flask")}Demonstração · dados sintéticos</div>` : ""}
      </div>
      <aside class="detail" id="detail" aria-label="Detalhe do elemento selecionado" hidden>
        <div class="detail__head"><h3 id="detail-title">Detalhe</h3><button type="button" class="btn btn--neutral btn--icon btn--sm" id="detail-close" aria-label="Fechar detalhe">${icon("close")}</button></div>
        <div class="detail__body" id="detail-body"></div>
      </aside>
    </div></div>`;

    $("#side-toggle").addEventListener("click", (e) => {
      const open = $("#mapside").classList.toggle("is-open");
      e.currentTarget.setAttribute("aria-expanded", String(open));
    });
    $("#detail-close").addEventListener("click", closeDetail);
    root.addEventListener("keydown", (e) => e.key === "Escape" && !$("#detail")?.hidden && closeDetail());

    const [occ, st, susc, nivel] = await Promise.all([getOccurrences(), getStations(), load("suscetibilidade"), load("nivel")]);
    ctx = { occ, st, susc, nivel };
    renderFilters();
    renderLayersPanel();
    renderSideContent();

    if (!window.maplibregl) {
      $("#map").innerHTML = `<div style="padding:24px;max-width:520px">${stateBlock("error", "O componente de mapa não carregou", "A biblioteca MapLibre não respondeu. Os indicadores, insights e a lista de registros continuam disponíveis ao lado.")}</div>`;
      return () => {};
    }
    const style = await loadBasemap();
    map = new maplibregl.Map({
      container: "map",
      style,
      center: CONFIG.MAP_CENTER,
      zoom: CONFIG.MAP_ZOOM,
      minZoom: 8.5,
      maxZoom: 18,
      maxBounds: CONFIG.MAP_BOUNDS,
      attributionControl: false,
      cooperativeGestures: false,
    });
    map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
    map.addControl(new maplibregl.ScaleControl({ unit: "metric" }), "bottom-right");
    map.addControl(new maplibregl.AttributionControl({ compact: true, customAttribution: view.basemapOk ? "" : "Mapa base indisponível" }), "bottom-right");
    map.on("load", () => {
      addLayers();
      refreshData();
      applyParams(params);
    });
    return () => {
      map?.remove();
      map = null;
    };
  },
  onFilters() {
    const focusId = document.activeElement?.id;
    renderFilters();
    renderLayersPanel();
    renderSideContent();
    if (map?.isStyleLoaded()) refreshData();
    if (focusId) document.getElementById(focusId)?.focus();
  },
};

/* ------------------------------------------------------------------ mapa base */
async function loadBasemap() {
  try {
    const ctrl = new AbortController();
    const t = setTimeout(() => ctrl.abort(), 8000);
    const res = await fetch(CONFIG.BASEMAP_STYLE, { signal: ctrl.signal });
    clearTimeout(t);
    if (!res.ok) throw new Error(res.status);
    view.basemapOk = true;
    return await res.json();
  } catch {
    view.basemapOk = false;
    $("#m-notice").innerHTML = stateBlock("stale", "Mapa base indisponível", "As camadas de dados continuam visíveis sobre fundo neutro.");
    return {
      version: 8,
      glyphs: "https://tiles.basemaps.cartocdn.com/fonts/{fontstack}/{range}.pbf",
      sources: {},
      layers: [{ id: "bg", type: "background", paint: { "background-color": "#EEF2F7" } }],
    };
  }
}

function makeDiamond(color, size = 22) {
  const c = document.createElement("canvas");
  c.width = c.height = size * 2;
  const g = c.getContext("2d");
  g.scale(2, 2);
  g.translate(size / 2, size / 2);
  g.rotate(Math.PI / 4);
  const s = size * 0.48;
  g.fillStyle = "#fff";
  g.fillRect(-s / 2 - 2, -s / 2 - 2, s + 4, s + 4);
  g.fillStyle = color;
  g.fillRect(-s / 2, -s / 2, s, s);
  return { width: size * 2, height: size * 2, data: g.getImageData(0, 0, size * 2, size * 2).data };
}

function addLayers() {
  const firstSymbol = map.getStyle().layers.find((l) => l.type === "symbol")?.id;
  map.addImage("diamond", makeDiamond("#0F8A73"), { pixelRatio: 2 });
  map.addImage("diamond-off", makeDiamond("#94A3B8"), { pixelRatio: 2 });

  map.addSource("susc", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "susc-fill", type: "fill", source: "susc", paint: { "fill-color": ["match", ["get", "grau"], 3, SUSC_COLORS[3], 2, SUSC_COLORS[2], SUSC_COLORS[1]], "fill-opacity": ["match", ["get", "grau"], 3, 0.5, 2, 0.42, 0.38] } }, firstSymbol);
  map.addLayer({ id: "susc-line-media", type: "line", source: "susc", filter: ["==", ["get", "grau"], 2], paint: { "line-color": "#7B6AE0", "line-width": 1, "line-dasharray": [3, 2] } }, firstSymbol);
  map.addLayer({ id: "susc-line-alta", type: "line", source: "susc", filter: ["==", ["get", "grau"], 3], paint: { "line-color": "#3F2FB0", "line-width": 1.6 } }, firstSymbol);

  map.addSource("chuva", { type: "geojson", data: EMPTY });
  map.addLayer({
    id: "chuva-circle", type: "circle", source: "chuva",
    paint: {
      "circle-radius": ["interpolate", ["linear"], ["zoom"], 9, 6, 14, 10],
      "circle-color": ["case", ["==", ["get", "sem"], 1], "#FFFFFF", ["to-color", ["get", "cor"]]],
      "circle-stroke-color": ["case", ["==", ["get", "sem"], 1], "#64748B", "#1E3A8A"],
      "circle-stroke-width": ["case", ["==", ["get", "sem"], 1], 2, 1.2],
    },
  });
  map.addLayer({ id: "chuva-label", type: "symbol", source: "chuva", minzoom: 11.5, layout: { "text-field": ["get", "rotulo"], "text-font": FONT, "text-size": 11, "text-offset": [0, 1.4], "text-anchor": "top" }, paint: { "text-color": "#0F172A", "text-halo-color": "#fff", "text-halo-width": 1.5 } });

  map.addSource("nivel", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "nivel-icon", type: "symbol", source: "nivel", layout: { "icon-image": ["case", ["==", ["get", "sem"], 1], "diamond-off", "diamond"], "icon-allow-overlap": true, "icon-size": ["interpolate", ["linear"], ["zoom"], 9, 0.8, 14, 1.1] } });
  map.addLayer({ id: "nivel-label", type: "symbol", source: "nivel", minzoom: 11.5, layout: { "text-field": ["get", "rotulo"], "text-font": FONT, "text-size": 11, "text-offset": [0, 1.3], "text-anchor": "top" }, paint: { "text-color": "#0B5F50", "text-halo-color": "#fff", "text-halo-width": 1.5 } });

  map.addSource("oc", { type: "geojson", data: EMPTY, cluster: true, clusterRadius: 44, clusterMaxZoom: 15 });
  map.addLayer({
    id: "oc-cluster", type: "circle", source: "oc", filter: ["has", "point_count"],
    paint: {
      "circle-color": "#1E293B", "circle-opacity": 0.88, "circle-stroke-color": "#fff", "circle-stroke-width": 2,
      "circle-radius": ["step", ["get", "point_count"], 13, 10, 17, 50, 22, 200, 28, 1000, 34],
    },
  });
  map.addLayer({ id: "oc-count", type: "symbol", source: "oc", filter: ["has", "point_count"], layout: { "text-field": ["get", "point_count_abbreviated"], "text-font": FONT, "text-size": 12, "text-allow-overlap": true }, paint: { "text-color": "#fff" } });
  map.addLayer({ id: "oc-point", type: "circle", source: "oc", filter: ["!", ["has", "point_count"]], paint: { "circle-color": "#1E293B", "circle-radius": ["interpolate", ["linear"], ["zoom"], 12, 4, 16, 7], "circle-stroke-color": "#fff", "circle-stroke-width": 1.5 } });

  map.addSource("sel", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "sel", type: "circle", source: "sel", paint: { "circle-radius": 13, "circle-color": "rgba(21,94,239,0.12)", "circle-stroke-color": "#155EEF", "circle-stroke-width": 2.5 } });

  for (const id of ["oc-cluster", "oc-point", "chuva-circle", "nivel-icon", "susc-fill"]) {
    map.on("mouseenter", id, () => (map.getCanvas().style.cursor = "pointer"));
    map.on("mouseleave", id, () => (map.getCanvas().style.cursor = ""));
  }
  // Prioridade de clique: ponto > estação > sensor > cluster > polígono
  map.on("click", (e) => {
    const order = ["oc-point", "chuva-circle", "nivel-icon", "oc-cluster", "susc-fill"].filter((l) => map.getLayer(l) && map.getLayoutProperty(l, "visibility") !== "none");
    const hits = map.queryRenderedFeatures(e.point, { layers: order });
    if (!hits.length) return;
    hits.sort((a, b) => order.indexOf(a.layer.id) - order.indexOf(b.layer.id));
    const h = hits[0];
    if (h.layer.id === "oc-point") showOccurrence(h.properties.key);
    else if (h.layer.id === "chuva-circle") showStation(h.properties.id);
    else if (h.layer.id === "nivel-icon") showLevel(h.properties.id);
    else if (h.layer.id === "oc-cluster") showCluster(h);
    else if (h.layer.id === "susc-fill") showSusc(h.properties.id, e.lngLat);
  });
}

/* ------------------------------------------------------------------ dados → mapa */
function currentOcc() {
  return ctx.occ.status === "ok" ? filterOccurrences(ctx.occ.rows, store.filters) : [];
}
function currentStations() {
  return ctx.st.est.status === "ok" ? filterStations(ctx.st.est.data.chuva, store.filters) : [];
}
function currentLevel() {
  return ctx.st.est.status === "ok" ? filterStations(ctx.st.est.data.nivel, store.filters) : [];
}

function rainValue(s) {
  const per = resolvePeriod();
  if (view.rainWindow === "periodo") return rainInPeriod(ctx.st.diaria, s.id, per.inicio, per.fim);
  const h = { "1h": 1, "24h": 24, "72h": 72 }[view.rainWindow];
  return rainLastHours(ctx.st.horaria, s.id, h);
}

function rainBreaks(values) {
  const fixed = { "1h": [0.2, 2, 5, 10, 20, 30, 50], "24h": [1, 5, 10, 20, 40, 60, 80], "72h": [2, 10, 25, 50, 80, 120, 160] }[view.rainWindow];
  if (fixed) return fixed;
  const max = Math.max(...values, 1);
  const step = Math.ceil(max / 7 / 10) * 10 || 1;
  return Array.from({ length: 7 }, (_, i) => step * (i + 1) * (i === 0 ? 0.5 : 1));
}
const rainColor = (mm, br) => RAIN_RAMP[Math.min(RAIN_RAMP.length - 1, br.filter((b) => mm >= b).length)];

function refreshData() {
  if (!map) return;
  const vis = (ids, on) => ids.forEach((id) => map.getLayer(id) && map.setLayoutProperty(id, "visibility", on ? "visible" : "none"));

  const occ = currentOcc();
  map.getSource("oc").setData({ type: "FeatureCollection", features: occ.map((o) => ({ type: "Feature", geometry: { type: "Point", coordinates: [o.lon, o.lat] }, properties: { key: o.key, tipo: o.tipo } })) });
  vis(["oc-cluster", "oc-count", "oc-point"], view.layers.ocorrencias);

  const st = currentStations();
  const vals = st.map((s) => ({ s, r: rainValue(s) }));
  const br = rainBreaks(vals.map((v) => v.r.mm).filter((v) => v !== null));
  view.rainBreaks = br;
  map.getSource("chuva").setData({
    type: "FeatureCollection",
    features: vals.map(({ s, r }) => ({
      type: "Feature", geometry: { type: "Point", coordinates: [s.lon, s.lat] },
      properties: { id: s.id, sem: r.mm === null ? 1 : 0, cor: r.mm === null ? "#fff" : rainColor(r.mm, br), rotulo: r.mm === null ? "sem dado" : `${fmt1(r.mm)} mm` },
    })),
  });
  vis(["chuva-circle", "chuva-label"], view.layers.chuva);

  const nv = currentLevel();
  map.getSource("nivel").setData({
    type: "FeatureCollection",
    features: nv.map((s) => {
      const serie = ctx.nivel.status === "ok" ? ctx.nivel.data.horario[s.id] : null;
      const ult = serie ? serie[serie.length - 1] : null;
      return { type: "Feature", geometry: { type: "Point", coordinates: [s.lon, s.lat] }, properties: { id: s.id, sem: ult === null ? 1 : 0, rotulo: ult === null ? "sem dado" : `${fmt2(ult)} m` } };
    }),
  });
  vis(["nivel-icon", "nivel-label"], view.layers.nivel);

  if (ctx.susc.status === "ok" && !map.getSource("susc")._loadedOnce) {
    map.getSource("susc").setData({
      type: "FeatureCollection",
      features: [...ctx.susc.data.areas].sort((a, b) => a.grau - b.grau).map((a) => ({ type: "Feature", geometry: { type: "Polygon", coordinates: [[...a.poligono, a.poligono[0]]] }, properties: { id: a.id, grau: a.grau } })),
    });
    map.getSource("susc")._loadedOnce = true;
  }
  vis(["susc-fill", "susc-line-media", "susc-line-alta"], view.layers.suscetibilidade);

  // recorte territorial: aproxima a subprefeitura selecionada
  const sp = store.filters.subprefeitura;
  if (sp !== lastSubpref) {
    const prev = lastSubpref;
    lastSubpref = sp;
    if (sp) {
      const bb = bboxOf([...occ, ...st, ...nv]);
      if (bb) map.fitBounds(bb, { padding: 60, maxZoom: 14, duration: 600 });
    } else if (prev) map.easeTo({ center: CONFIG.MAP_CENTER, zoom: CONFIG.MAP_ZOOM, duration: 600 });
  }
  renderScope(occ.length, st.length);
  renderLayersPanel();
}

function renderScope(nOcc, nSt) {
  const f = store.filters;
  const per = resolvePeriod();
  $("#m-scope").innerHTML = `<div class="scope">
    <strong>${icon("pin")}${esc(f.subprefeitura ? subprefNome(f.subprefeitura) : "Município de São Paulo")}</strong>
    <span>${esc(per.rotulo)} · ${esc(per.texto)}</span>
    <span>${view.layers.ocorrencias ? `${fmtInt(nOcc)} ocorrências` : "ocorrências ocultas"} · ${fmtInt(nSt)} estações${f.rede ? ` ${esc(f.rede)}` : ""}</span>
  </div>`;
}

/* ------------------------------------------------------------------ painel lateral */
function renderFilters() {
  $("#m-filters").innerHTML = filterBar({ id: "mfb" });
  bindFilterBar($("#m-filters"), "mfb");
}

const WINDOWS = [["1h", "1 h"], ["24h", "24 h"], ["72h", "72 h"], ["periodo", "Período do filtro"]];

function layerRow(id, title, hint, legend, { disabled = false, reason = "" } = {}) {
  const on = !disabled && view.layers[id];
  return `<div class="layer ${disabled ? "is-disabled" : ""}">
    <label class="check"><input type="checkbox" data-layer="${id}" ${on ? "checked" : ""} ${disabled ? "disabled" : ""} aria-describedby="lh-${id}">
      <span class="check__text"><span>${esc(title)}</span><span class="check__hint" id="lh-${id}">${disabled ? `${icon("info")} ${esc(reason)}` : hint}</span></span></label>
    ${on && legend ? `<div class="layer__legend">${legend}</div>` : ""}
  </div>`;
}

function renderLayersPanel() {
  const host = $("#m-layers");
  if (!host) return;
  const per = resolvePeriod();
  const occMeta = ctx.occ.meta, susc = ctx.susc;
  const br = view.rainBreaks || [1, 5, 10, 20, 40, 60, 80];
  const winLabel = view.rainWindow === "periodo" ? `${per.texto}` : `últimas ${WINDOWS.find((w) => w[0] === view.rainWindow)[1]} até ${fmtDateTime(ctx.st.horaria?.meta?.referencia)}`;
  const m = getManifest();
  const naoPub = Object.fromEntries((m.nao_publicados || []).map((n) => [n.id, n.motivo]));

  host.innerHTML = [
    layerRow("ocorrencias", "Ocorrências históricas", `${ctx.occ.status === "ok" ? "GeoSampa · registros por dia, sem hora" : "Indisponível"}`,
      `<div class="legend">
        <div class="legend__row"><span class="legend__dot" style="background:#1E293B;width:18px;height:18px"></span>Agrupamento — o número é a contagem de registros</div>
        <div class="legend__row"><span class="legend__dot" style="background:#1E293B"></span>Ocorrência individual (zoom maior)</div>
        <div class="note">${natureBadge(occMeta)} ${occMeta ? freshnessBadge(occMeta, "ocorrencias") : ""}</div>
        <div class="note">Período: ${esc(per.texto)}${occMeta && per.fim > occMeta.periodo.fim ? ` · publicados até ${esc(fmtDay(occMeta.periodo.fim))}` : ""}</div>
      </div>`),
    layerRow("chuva", "Chuva observada", "Pluviômetros CEMADEN, SIBH e INMET · mm",
      `<div class="seg" role="group" aria-label="Janela da chuva acumulada">${WINDOWS.map(([k, t]) => `<button type="button" data-win="${k}" aria-pressed="${view.rainWindow === k}">${t}</button>`).join("")}</div>
      <div class="legend" style="margin-top:8px">
        <div>Chuva acumulada (mm) · ${esc(winLabel)}</div>
        <div class="legend__row">${RAIN_RAMP.map((c) => `<span class="legend__ramp" style="background:${c}"></span>`).join("")}</div>
        <div class="legend__scale"><span>0</span>${br.filter((_, i) => i % 2 === 1).map((b) => `<span>${fmtInt(b)}</span>`).join("")}<span>${fmtInt(br[br.length - 1])}+</span></div>
        <div class="legend__row"><span class="legend__dot" style="background:#fff;border-color:#64748B;box-shadow:none"></span>Sem leitura na janela</div>
        <div class="note">A escala é de chuva, não de risco. ${ctx.st.horaria?.meta ? freshnessBadge(ctx.st.horaria.meta, "chuva_horaria") : ""}</div>
      </div>`, { disabled: ctx.st.est.status !== "ok", reason: "Dados de chuva não publicados nesta versão." }),
    layerRow("nivel", "Nível de rios e córregos", "Sensores fluviométricos do SIBH · último valor horário",
      `<div class="legend">
        <div class="legend__row"><span class="legend__diamond" style="background:#0F8A73"></span>Sensor com leitura (m, unidade da fonte)</div>
        <div class="legend__row"><span class="legend__diamond" style="background:#94A3B8"></span>Sensor sem leitura recente</div>
        <div class="note">${esc(ctx.nivel.meta?.limiares || "")}</div>
      </div>`, { disabled: ctx.nivel.status !== "ok", reason: "Nível não publicado nesta versão." }),
    layerRow("suscetibilidade", "Suscetibilidade a inundação", "SGB/CPRM, carta de 2015 · não é previsão",
      `<div class="legend">
        ${[3, 2, 1].map((g) => `<div class="legend__row"><span class="legend__sw" style="background:${SUSC_COLORS[g]};${g === 3 ? "outline:1.6px solid #3F2FB0" : g === 2 ? "outline:1px dashed #7B6AE0" : ""}"></span>Suscetibilidade ${["", "baixa", "média", "alta"][g]}${g === 3 ? " (contorno contínuo)" : g === 2 ? " (contorno tracejado)" : " (sem contorno)"}</div>`).join("")}
        <div class="note">Predisposição do terreno a inundar, sem considerar a chuva. ${susc.meta ? natureBadge(susc.meta) : ""}</div>
      </div>`, { disabled: susc.status !== "ok", reason: "Suscetibilidade não publicada nesta versão." }),
    layerRow("limites", "Limites administrativos", "", "", { disabled: true, reason: naoPub.limites || "Não publicado." }),
    layerRow("setores", "Setores censitários (IBGE)", "", "", { disabled: true, reason: naoPub.setores || "Não publicado." }),
    layerRow("vias", "Rede viária (OpenStreetMap)", "", "", { disabled: true, reason: naoPub.vias || "Não publicado." }),
  ].join("");

  host.querySelectorAll("[data-layer]").forEach((cb) =>
    cb.addEventListener("change", () => {
      view.layers[cb.dataset.layer] = cb.checked;
      refreshData();
      renderLayersPanel();
      host.querySelector(`[data-layer="${cb.dataset.layer}"]`)?.focus();
    }),
  );
  host.querySelectorAll("[data-win]").forEach((b) =>
    b.addEventListener("click", () => {
      view.rainWindow = b.dataset.win;
      refreshData();
      host.querySelector(`[data-win="${b.dataset.win}"]`)?.focus();
    }),
  );
}

function renderSideContent() {
  const f = store.filters;
  const per = resolvePeriod();
  const occ = currentOcc();
  const insEl = $("#m-insights");
  if (ctx.occ.status !== "ok") insEl.innerHTML = resultState(ctx.occ, "Ocorrências");
  else {
    const occHist = filterOccurrences(ctx.occ.rows, { ...f, periodo: "todo" });
    const ins = computeInsights({ occ, occHist, per, f, susc: ctx.susc, stations: currentStations(), diaria: ctx.st.diaria, occMeta: ctx.occ.meta });
    insEl.innerHTML = ins.length ? ins.map((i) => insightCard(i, { compact: true })).join("") : stateBlock("empty", "Poucos registros no recorte", "São necessários ao menos 10 registros para descrever padrões. Amplie o período ou o território.");
    insEl.querySelectorAll("[data-acao]").forEach((b) => b.addEventListener("click", () => runAction(ins.find((i) => i.id === b.dataset.acao).acao)));
  }

  const list = $("#m-list");
  if (ctx.occ.status !== "ok") {
    list.innerHTML = "";
    return;
  }
  const recent = [...occ].sort((a, b) => (a.data < b.data ? 1 : -1)).slice(0, 12);
  list.innerHTML = recent.length
    ? `<p class="note">${fmtInt(occ.length)} ocorrências no recorte. As 12 mais recentes:</p>
      <ul class="reclist">${recent.map((o) => `<li><button type="button" data-oc="${esc(o.key)}"><span class="reclist__dot" style="background:${TYPE_COLORS[o.tipo]}"></span><span><strong>${esc(o.tipo)}</strong> · ${esc(fmtDay(o.data))}<small>${esc(subprefNome(o.subprefeitura))}</small></span></button></li>`).join("")}</ul>
      <button type="button" class="btn btn--neutral btn--sm" id="m-export">${icon("download")}Exportar recorte (CSV)</button>
      <p class="note">${isDemo() ? "O arquivo exportado contém dados sintéticos e é marcado como demonstração." : "Exportação sujeita às licenças das fontes."}</p>`
    : stateBlock("empty", "Nenhuma ocorrência no recorte", "Amplie o período ou remova filtros.");
  list.querySelectorAll("[data-oc]").forEach((b) => b.addEventListener("click", () => {
    const o = ctx.occ.rows.find((x) => x.key === b.dataset.oc);
    map?.flyTo({ center: [o.lon, o.lat], zoom: 16 });
    showOccurrence(o.key);
  }));
  $("#m-export")?.addEventListener("click", () => exportCsv(occ));
}

function exportCsv(occ) {
  const per = resolvePeriod();
  const head = ["ano_origem", "id_ocorrencia", "data", "tipo", "subprefeitura", "latitude", "longitude", "grau_suscetibilidade_inundacao"];
  const lines = [
    `# ChuvaSegura-SP — ocorrências históricas (${isDemo() ? "DEMONSTRAÇÃO: dados sintéticos, sem conexão com dados reais" : "fonte: GeoSampa"})`,
    `# Período: ${per.texto}; exportado em ${new Date().toISOString()}`,
    head.join(";"),
    ...occ.map((o) => [o.ano_origem, o.id, o.data, o.tipo, subprefNome(o.subprefeitura), o.lat, o.lon, o.grau ?? ""].join(";")),
  ];
  downloadText(`chuvasegura_ocorrencias${isDemo() ? "_DEMO" : ""}_${per.inicio}_${per.fim}.csv`, lines.join("\n"));
}

function runAction(a) {
  if (a.filtro) store.setFilters(a.filtro);
  if (a.camada) {
    view.layers[a.camada] = true;
    refreshData();
  }
  if (a.rota) location.hash = a.rota;
  if (a.center && map) map.flyTo({ center: a.center, zoom: a.zoom || 14 });
  if (a.estacao) showStation(a.estacao);
}

function applyParams(params) {
  const c = params.get("center");
  if (c) {
    const [lon, lat] = c.split(",").map(Number);
    map.jumpTo({ center: [lon, lat], zoom: Number(params.get("zoom")) || 14 });
  }
  const est = params.get("estacao");
  if (est) {
    const s = ctx.st.est.data?.chuva.find((x) => x.id === est);
    if (s && !c) map.jumpTo({ center: [s.lon, s.lat], zoom: 13 });
    showStation(est);
  }
}

/* ------------------------------------------------------------------ detalhe */
function openDetail(title, html, point) {
  const d = $("#detail");
  $("#detail-title").textContent = title;
  $("#detail-body").innerHTML = html + (isDemo() ? `<div class="detail__demo">${icon("flask")}${DEMO_MSG}.</div>` : "");
  d.hidden = false;
  if (map && point) map.getSource("sel").setData({ type: "FeatureCollection", features: [{ type: "Feature", geometry: { type: "Point", coordinates: point }, properties: {} }] });
  $("#detail-close").focus();
}
function closeDetail() {
  $("#detail").hidden = true;
  map?.getSource("sel")?.setData(EMPTY);
}

function showOccurrence(key) {
  const o = ctx.occ.rows.find((x) => x.key === key);
  if (!o) return;
  const g = Number.isInteger(o.grau)
    ? o.grau ? `Dentro de área de suscetibilidade ${["", "baixa", "média", "alta"][o.grau]} a inundação (SGB/CPRM)` : "Fora das áreas mapeadas de suscetibilidade a inundação"
    : ctx.susc.status === "ok" ? "Sem classe de suscetibilidade associada a este registro" : "Cruzamento com suscetibilidade não publicado nesta versão";
  openDetail(o.tipo, `
    <div class="pill-list">${natureBadge(ctx.occ.meta)}</div>
    <dl class="dl" style="margin-top:12px">
      <dt>Data</dt><dd>${esc(fmtDay(o.data))} <span class="muted">(sem hora no registro)</span></dd>
      <dt>Tipo</dt><dd>${esc(o.tipo)}</dd>
      <dt>Subprefeitura</dt><dd>${esc(subprefNome(o.subprefeitura))}</dd>
      <dt>Coordenadas</dt><dd class="mono">${o.lat.toFixed(5)}, ${o.lon.toFixed(5)} (SIRGAS 2000)</dd>
      <dt>Território</dt><dd>${esc(g)}</dd>
      <dt>Identificação</dt><dd class="mono">arquivo ${o.ano_origem} · nº ${o.id}</dd>
      <dt>Tabela</dt><dd class="mono">gold.fact_geosampa_flooding_events</dd>
    </dl>
    <hr class="divider">${provenance(ctx.occ.meta)}
    <p class="note" style="margin-top:12px">Registro histórico: descreve algo que aconteceu, não a condição atual da via.</p>`, [o.lon, o.lat]);
}

async function showCluster(h) {
  const src = map.getSource("oc");
  const id = h.properties.cluster_id;
  const n = h.properties.point_count;
  let leaves = [];
  try {
    leaves = await src.getClusterLeaves(id, Math.min(n, 2000), 0);
  } catch { /* segue sem composição */ }
  const tipos = [...groupCount(leaves.map((l) => l.properties.tipo))].sort((a, b) => b[1] - a[1]);
  openDetail(`${fmtInt(n)} ocorrências agrupadas`, `
    <p>Agrupamento de registros próximos na escala atual. Aproxime para ver os pontos individuais.</p>
    <ul class="mini-bars">${tipos.map(([t, c]) => `<li><span>${esc(t)}</span><span class="mini-bars__bar"><i style="width:${(c / n) * 100}%;background:${TYPE_COLORS[t]}"></i></span><b>${fmtInt(c)}</b></li>`).join("")}</ul>
    <button type="button" class="btn btn--primary btn--sm" id="zoom-cluster">${icon("crosshair")}Aproximar o agrupamento</button>
    <hr class="divider">${provenance(ctx.occ.meta, { periodo: resolvePeriod().texto })}`, h.geometry.coordinates);
  $("#zoom-cluster").addEventListener("click", async () => {
    const z = await src.getClusterExpansionZoom(id);
    map.easeTo({ center: h.geometry.coordinates, zoom: z + 0.5 });
  });
}

function showStation(id) {
  const s = ctx.st.est.data.chuva.find((x) => x.id === id);
  if (!s) return;
  const per = resolvePeriod();
  const w = (h) => rainLastHours(ctx.st.horaria, id, h);
  const p = rainInPeriod(ctx.st.diaria, id, per.inicio, per.fim);
  const serie = ctx.st.diaria.data.series[id].slice(-30);
  const cell = (label, r) => `<div class="mini-kpi"><span>${label}</span><b>${r.mm === null ? "sem leitura" : `${fmt1(r.mm)} mm`}</b>${r.cobertura < 1 && r.mm !== null ? `<small>${Math.round(r.cobertura * 100)}% com leitura</small>` : ""}</div>`;
  openDetail(s.nome, `
    <div class="pill-list">${natureBadge(ctx.st.horaria.meta)} ${freshnessBadge(ctx.st.horaria.meta, "chuva_horaria")} ${s.ativa ? "" : badge("neutral", "Estação inativa", "x")}</div>
    <div class="mini-kpis">${cell("1 h", w(1))}${cell("24 h", w(24))}${cell("72 h", w(72))}${cell("Período do filtro", p)}</div>
    <p class="note">Janelas móveis até ${esc(fmtDateTime(ctx.st.horaria.meta.referencia))}, ${esc(CONFIG.TZ_LABEL)}. Período do filtro: ${esc(per.texto)}.</p>
    <div class="spark"><span class="note">Chuva diária, últimos 30 dias (mm)</span>${sparkline(serie, { width: 300, height: 44 })}</div>
    <dl class="dl">
      <dt>Rede</dt><dd>${esc(s.rede)}${s.instituicao !== s.rede ? ` · ${esc(s.instituicao)}` : ""}</dd>
      <dt>Código</dt><dd class="mono">${esc(s.codigo)}</dd>
      <dt>Subprefeitura</dt><dd>${esc(subprefNome(s.subprefeitura))}</dd>
      <dt>Coordenadas</dt><dd class="mono">${s.lat.toFixed(5)}, ${s.lon.toFixed(5)}</dd>
      <dt>Tabela</dt><dd class="mono">gold.${esc(s.tabela)}</dd>
    </dl>
    <a class="btn btn--secondary btn--sm" href="#/monitoramento?estacao=${encodeURIComponent(id)}">${icon("activity")}Abrir série no monitoramento</a>
    <hr class="divider">${provenance(ctx.st.horaria.meta)}`, [s.lon, s.lat]);
}

function showLevel(id) {
  const s = ctx.st.est.data.nivel.find((x) => x.id === id);
  if (!s) return;
  const serie = ctx.nivel.data.horario[id];
  const ult = serie[serie.length - 1];
  const valid = serie.filter((v) => v !== null);
  const t = hourlyTimes({ data: { inicio: ctx.nivel.data.horario_inicio, series: ctx.nivel.data.horario } });
  openDetail(s.nome, `
    <div class="pill-list">${natureBadge(ctx.nivel.meta)} ${freshnessBadge(ctx.nivel.meta, "nivel")}</div>
    <div class="mini-kpis">
      <div class="mini-kpi"><span>Último nível</span><b>${ult === null ? "sem leitura" : `${fmt2(ult)} m`}</b><small>${esc(fmtDateTime(t[t.length - 1]))}</small></div>
      <div class="mini-kpi"><span>Máximo em 7 dias</span><b>${valid.length ? `${fmt2(Math.max(...valid))} m` : "—"}</b></div>
      <div class="mini-kpi"><span>Mínimo em 7 dias</span><b>${valid.length ? `${fmt2(Math.min(...valid))} m` : "—"}</b></div>
    </div>
    ${valid.length ? `<div class="spark"><span class="note">Nível horário, últimos 7 dias (m)</span>${sparkline(serie, { width: 300, height: 44, bars: false, color: "#0F8A73" })}</div>` : ""}
    ${stateBlock("info", "Sem limiar oficial integrado", "Não há cota de transbordamento publicada para este sensor nesta versão; o nível não é classificado como normal ou crítico.")}
    <dl class="dl" style="margin-top:12px">
      <dt>Curso d'água</dt><dd>${esc(s.curso_dagua)}</dd>
      <dt>Instituição</dt><dd>${esc(s.instituicao)} (via SIBH)</dd>
      <dt>Código</dt><dd class="mono">${esc(s.codigo)}</dd>
      <dt>Tabela</dt><dd class="mono">gold.fact_sibh_level_hourly</dd>
    </dl>
    <a class="btn btn--secondary btn--sm" href="#/monitoramento?nivel=${encodeURIComponent(id)}">${icon("activity")}Abrir série no monitoramento</a>
    <hr class="divider">${provenance(ctx.nivel.meta)}`, [s.lon, s.lat]);
}

function showSusc(id, lngLat) {
  const a = ctx.susc.data.areas.find((x) => x.id === Number(id));
  if (!a) return;
  const cls = ctx.susc.data.classes.find((c) => c.grau === a.grau);
  openDetail(`Suscetibilidade ${a.classe.toLowerCase()} a inundação`, `
    <div class="pill-list">${natureBadge(ctx.susc.meta)} ${badge("neutral", "Não é previsão", "info")}</div>
    <dl class="dl" style="margin-top:12px">
      <dt>Camada</dt><dd>Inundação</dd>
      <dt>Classe</dt><dd>${esc(a.classe)} (grau ${a.grau} de 3)</dd>
      <dt>Características</dt><dd>${esc(cls?.caracteristicas || "—")}</dd>
      <dt>Área do polígono</dt><dd>${fmt2(a.area_km2)} km²</dd>
      <dt>Referência</dt><dd>Carta IPT/CPRM de 2015, escala 1:75.000</dd>
      <dt>Tabela</dt><dd class="mono">gold.dim_sgb_susceptibility_areas</dd>
    </dl>
    <p class="note" style="margin-top:12px">Suscetibilidade indica a predisposição do terreno, sem considerar chuva, drenagem ou obras recentes. ${esc(ctx.susc.meta.observacao || "")}</p>
    <hr class="divider">${provenance(ctx.susc.meta)}`, [lngLat.lng, lngLat.lat]);
}
