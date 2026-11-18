// Mapa principal (F01–F05, F10, F12): camadas, filtros, legenda, detalhe e insights.
import { CONFIG } from "../config.js";
import { icon, glyphPaths, TIPO_ICONE } from "../icons.js";
import { esc, fmtInt, fmt1, fmt2, fmtDay, fmtDateTime, groupCount, downloadText, $ } from "../util.js";
import { store, resolvePeriod } from "../store.js";
import { getOccurrences, filterOccurrences, getStations, filterStations, rainInPeriod, rainLastHours, load, bboxOf, isDemo, getManifest, hourlyTimes, serieNivel } from "../data.js";
import { filterBar, bindFilterBar, subprefNome, stateBlock, resultState, natureBadge, freshnessBadge, badge, provenance, DEMO_MSG } from "../ui.js";
import { sparkline, TYPE_COLORS } from "../charts.js";
import { hexbin, quebras, raioPorZoom, ZOOM_PONTOS } from "../hexbin.js";
import { REDES_EM_REVISAO } from "../regras-alerta.js";

// Rampa de chuva para o mapa: começa num azul visível sobre o mapa base claro (o branco fica para "sem leitura").
const RAIN_RAMP = ["#DCEBFC", "#B7D3F6", "#86B6EF", "#5598E7", "#2A78D6", "#1C5CAB", "#104281", "#0D366B"];
import { computeInsights, insightCard } from "../insights.js";

const SUSC_COLORS = { 1: "#E4DEFB", 2: "#B4A7EF", 3: "#6250D6" };
// Densidade (hab/km²): rampa sequencial de um só tom, distinta da chuva (azul), do nível (verde) e da suscetibilidade (violeta).
const DENS_BREAKS = [1000, 5000, 10000, 15000, 20000, 30000];
const DENS_RAMP = ["#F6EFE4", "#EADBC2", "#DCC39E", "#CBA77A", "#B48A58", "#946B3E", "#6F4E2A"];
const VIA_COR = "#334155";
// Ocorrências agregadas em hexágonos: rampa sequencial de um só tom (ardósia), a mesma cor-base das ocorrências.
const HEX_RAMP = ["#DDE3EC", "#B3BED0", "#8494AF", "#56668A", "#2B3A5C"];
// Camadas pesadas (vários MB): só são baixadas quando o usuário liga a camada.
const SOB_DEMANDA = { setores: "setores", vias: "vias", hidrografia: "hidrografia" };
// Hidrografia: azul de linha (cursos) e preenchimento claro (corpos), distintos dos círculos de chuva pelo formato.
const AGUA_LINHA = "#2C7BD0";
const AGUA_AREA = "#B9D8F5";
const LARGURA_AGUA = { Rio: 2.6, Canal: 2, Córrego: 1.3, "Dreno ou vala": 0.8 };
const FONT = ["Montserrat SemiBold", "Open Sans Bold"];
const EMPTY = { type: "FeatureCollection", features: [] };

let map = null;
let ctx = null; // dados carregados
let root = null;
let lastSubpref = null;
const view = {
  layers: { ocorrencias: true, chuva: true, nivel: false, suscetibilidade: false, limites: true, hidrografia: true, setores: false, vias: false },
  coropletico: false,
  subBreaks: [],
  hexRaio: null,
  hexBreaks: [],
  hexCells: new Map(),
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
          <details class="mapside__sec" open><summary class="mapside__h">${icon("filter")}Filtros</summary><div id="m-filters"></div></details>
          <details class="mapside__sec" open><summary class="mapside__h">${icon("layers")}Camadas</summary><div id="m-layers"></div></details>
          <details class="mapside__sec" open><summary class="mapside__h">${icon("sparkle")}Insights do recorte</summary><p class="note">Padrões descritivos dos dados filtrados. Não são previsões nem alertas.</p><div id="m-insights">${stateBlock("loading", "Calculando…")}</div></details>
          <details class="mapside__sec"><summary class="mapside__h">${icon("table")}Registros no recorte</summary><div id="m-list"></div></details>
        </div>
      </aside>
      <div class="mapwrap">
        <div id="map" class="map" role="region" aria-label="Mapa do município de São Paulo. Use as setas para mover e + ou − para aproximar."></div>
        <div class="map-overlay map-overlay--tl"><div id="m-scope" aria-live="polite"></div><div id="m-notice"></div></div>
        <div class="map-overlay map-overlay--bl" id="m-legend"></div>
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

    const [occ, st, susc, nivel, limites] = await Promise.all([getOccurrences(), getStations(), load("suscetibilidade"), load("nivel"), load("limites")]);
    ctx = { occ, st, susc, nivel, limites, setores: null, vias: null, hidrografia: null };
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

// Marcador da ocorrência: círculo na cor do tipo com o ícone branco por cima (cor nunca sozinha: o ícone diz o tipo).
function makeMarker(color, glyph, size = 30) {
  const c = document.createElement("canvas");
  c.width = c.height = size * 2;
  const g = c.getContext("2d");
  g.scale(2, 2);
  g.beginPath();
  g.arc(size / 2, size / 2, size / 2 - 1.5, 0, Math.PI * 2);
  g.fillStyle = color;
  g.fill();
  g.lineWidth = 2;
  g.strokeStyle = "#fff";
  g.stroke();
  const k = (size * 0.6) / 24;
  g.translate(size / 2 - 12 * k, size / 2 - 12 * k);
  g.scale(k, k);
  g.lineWidth = 2.2;
  g.lineCap = g.lineJoin = "round";
  for (const d of glyphPaths(glyph)) g.stroke(new Path2D(d));
  return { width: size * 2, height: size * 2, data: g.getImageData(0, 0, size * 2, size * 2).data };
}

function addLayers() {
  const firstSymbol = map.getStyle().layers.find((l) => l.type === "symbol")?.id;
  map.addImage("diamond", makeDiamond("#0F8A73"), { pixelRatio: 2 });
  map.addImage("diamond-off", makeDiamond("#94A3B8"), { pixelRatio: 2 });
  for (const [tipo, glifo] of Object.entries(TIPO_ICONE)) map.addImage(`oc-${glifo}`, makeMarker(TYPE_COLORS[tipo], glifo), { pixelRatio: 2 });

  map.addSource("setores", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "setores-fill", type: "fill", source: "setores", paint: { "fill-color": ["case", ["==", ["get", "d"], null], "#E2E8F0", ["step", ["get", "d"], DENS_RAMP[0], ...DENS_BREAKS.flatMap((b, i) => [b, DENS_RAMP[i + 1]])]], "fill-opacity": 0.6 } }, firstSymbol);
  map.addLayer({ id: "setores-line", type: "line", source: "setores", minzoom: 13, paint: { "line-color": "#FFFFFF", "line-width": 0.6 } }, firstSymbol);

  map.addSource("susc", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "susc-fill", type: "fill", source: "susc", paint: { "fill-color": ["match", ["get", "grau"], 3, SUSC_COLORS[3], 2, SUSC_COLORS[2], SUSC_COLORS[1]], "fill-opacity": ["match", ["get", "grau"], 3, 0.5, 2, 0.42, 0.38] } }, firstSymbol);
  map.addLayer({ id: "susc-line-media", type: "line", source: "susc", filter: ["==", ["get", "grau"], 2], paint: { "line-color": "#7B6AE0", "line-width": 1, "line-dasharray": [3, 2] } }, firstSymbol);
  map.addLayer({ id: "susc-line-alta", type: "line", source: "susc", filter: ["==", ["get", "grau"], 3], paint: { "line-color": "#3F2FB0", "line-width": 1.6 } }, firstSymbol);

  map.addSource("agua-corpos", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "agua-corpos-fill", type: "fill", source: "agua-corpos", paint: { "fill-color": AGUA_AREA, "fill-opacity": 0.9, "fill-outline-color": AGUA_LINHA } }, firstSymbol);
  map.addSource("agua-cursos", { type: "geojson", data: EMPTY });
  map.addLayer({
    id: "agua-cursos-line", type: "line", source: "agua-cursos", layout: { "line-cap": "round", "line-join": "round" },
    paint: {
      "line-color": AGUA_LINHA, "line-opacity": 0.85,
      "line-width": ["interpolate", ["linear"], ["zoom"], 10, ["*", 0.6, ["get", "w"]], 15, ["*", 1.8, ["get", "w"]]],
    },
  }, firstSymbol);
  map.addLayer({ id: "agua-label", type: "symbol", source: "agua-cursos", minzoom: 12.5, filter: ["!=", ["get", "nome"], ""],
    layout: { "symbol-placement": "line", "text-field": ["get", "nome"], "text-font": FONT, "text-size": 11, "text-letter-spacing": 0.03 },
    paint: { "text-color": "#1D5A9E", "text-halo-color": "#fff", "text-halo-width": 1.5 } });

  map.addSource("limites", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "limites-fill", type: "fill", source: "limites", paint: { "fill-color": "#FFFFFF", "fill-opacity": 0 } }, firstSymbol);
  map.addLayer({ id: "limites-line", type: "line", source: "limites", paint: { "line-color": "#475569", "line-width": ["interpolate", ["linear"], ["zoom"], 9, 0.8, 14, 1.6], "line-opacity": 0.8 } }, firstSymbol);
  map.addLayer({ id: "limites-sel", type: "line", source: "limites", filter: ["==", ["get", "sigla"], ""], paint: { "line-color": "#155EEF", "line-width": 3 } }, firstSymbol);
  map.addLayer({ id: "limites-label", type: "symbol", source: "limites", minzoom: 10.3, layout: { "text-field": ["get", "nome"], "text-font": FONT, "text-size": 11, "text-transform": "uppercase", "text-letter-spacing": 0.05, "text-max-width": 8 }, paint: { "text-color": "#334155", "text-halo-color": "#fff", "text-halo-width": 1.6 } });

  map.addSource("vias", { type: "geojson", data: EMPTY });
  map.addLayer({
    id: "vias-line", type: "line", source: "vias", layout: { "line-cap": "round", "line-join": "round" },
    paint: {
      "line-color": VIA_COR, "line-opacity": 0.75,
      "line-width": ["interpolate", ["linear"], ["zoom"], 10, ["match", ["get", "h"], [1, 2], 1.6, [3, 4], 0.9, 0.5], 15, ["match", ["get", "h"], [1, 2], 5, [3, 4], 3.2, 2]],
    },
  }, firstSymbol);

  map.addSource("ochex", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "oc-hex", type: "fill", source: "ochex", maxzoom: ZOOM_PONTOS, paint: { "fill-color": HEX_RAMP[0], "fill-opacity": 0.8 } }, firstSymbol);
  map.addLayer({ id: "oc-hex-line", type: "line", source: "ochex", maxzoom: ZOOM_PONTOS, paint: { "line-color": "#FFFFFF", "line-width": 0.8 } }, firstSymbol);

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

  map.addSource("oc", { type: "geojson", data: EMPTY });
  map.addLayer({
    id: "oc-icon", type: "symbol", source: "oc", minzoom: ZOOM_PONTOS,
    layout: { "icon-image": ["concat", "oc-", ["get", "ic"]], "icon-allow-overlap": true, "icon-size": ["interpolate", ["linear"], ["zoom"], ZOOM_PONTOS, 0.75, 17, 1] },
  });

  map.addSource("sel", { type: "geojson", data: EMPTY });
  map.addLayer({ id: "sel", type: "circle", source: "sel", paint: { "circle-radius": 13, "circle-color": "rgba(21,94,239,0.12)", "circle-stroke-color": "#155EEF", "circle-stroke-width": 2.5 } });

  for (const id of ["oc-icon", "oc-hex", "chuva-circle", "nivel-icon", "vias-line", "agua-cursos-line", "agua-corpos-fill", "susc-fill", "setores-fill", "limites-fill"]) {
    map.on("mouseenter", id, () => (map.getCanvas().style.cursor = "pointer"));
    map.on("mouseleave", id, () => (map.getCanvas().style.cursor = ""));
  }
  const dica = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 8, className: "hexdica" });
  map.on("mousemove", "oc-hex", (e) => {
    const c = view.hexCells.get(e.features[0].properties.id);
    if (!c) return;
    const [tipo, n] = Object.entries(c.tipos).sort((a, b) => b[1] - a[1])[0];
    dica.setLngLat(e.lngLat).setHTML(`<strong>${fmtInt(c.n)} ocorrência${c.n > 1 ? "s" : ""}</strong><br>Mais comum: ${esc(tipo)} (${fmtInt(n)})`).addTo(map);
  });
  map.on("mouseleave", "oc-hex", () => dica.remove());
  map.on("zoomend", () => {
    if (raioPorZoom(map.getZoom()) !== view.hexRaio) atualizarHex();
    renderLegend();
  });

  // Prioridade de clique: ocorrência > estação > sensor > hexágono > via > suscetibilidade > setor > subprefeitura
  map.on("click", (e) => {
    const order = ["oc-icon", "chuva-circle", "nivel-icon", "oc-hex", "agua-cursos-line", "vias-line", "agua-corpos-fill", "susc-fill", "setores-fill", "limites-fill"].filter((l) => map.getLayer(l) && map.getLayoutProperty(l, "visibility") !== "none");
    const hits = map.queryRenderedFeatures(e.point, { layers: order });
    if (!hits.length) return;
    hits.sort((a, b) => order.indexOf(a.layer.id) - order.indexOf(b.layer.id));
    const h = hits[0];
    if (h.layer.id === "oc-icon") showOccurrence(h.properties.key);
    else if (h.layer.id === "chuva-circle") showStation(h.properties.id);
    else if (h.layer.id === "nivel-icon") showLevel(h.properties.id);
    else if (h.layer.id === "oc-hex") showHex(h.properties.id);
    else if (h.layer.id === "susc-fill") showSusc(h.properties.id, e.lngLat);
    else if (h.layer.id === "vias-line") showVia(h.properties.i, e.lngLat);
    else if (h.layer.id === "setores-fill") showSetor(h.properties.i, e.lngLat);
    else if (h.layer.id === "limites-fill") showSubpref(h.properties.sigla, e.lngLat);
    else if (h.layer.id === "agua-cursos-line") showAgua("cursos", h.properties.i, e.lngLat);
    else if (h.layer.id === "agua-corpos-fill") showAgua("corpos", h.properties.i, e.lngLat);
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
  map.getSource("oc").setData({ type: "FeatureCollection", features: occ.map((o) => ({ type: "Feature", geometry: { type: "Point", coordinates: [o.lon, o.lat] }, properties: { key: o.key, ic: TIPO_ICONE[o.tipo] } })) });
  atualizarHex(occ);
  const comCoropleta = view.coropletico && view.layers.limites && ctx.limites.status === "ok";
  vis(["oc-hex", "oc-hex-line"], view.layers.ocorrencias && !comCoropleta);
  vis(["oc-icon"], view.layers.ocorrencias);

  if (ctx.limites.status === "ok") {
    const porSub = groupCount(occ.map((o) => o.subprefeitura).filter(Boolean));
    if (!map.getSource("limites")._loadedOnce) {
      map.getSource("limites").setData({ type: "FeatureCollection", features: ctx.limites.data.subprefeituras.map((sp) => ({ type: "Feature", geometry: { type: "MultiPolygon", coordinates: sp.geometria }, properties: { sigla: sp.sigla, nome: sp.nome } })) });
      map.getSource("limites")._loadedOnce = true;
    }
    view.subContagem = porSub;
    view.subBreaks = quebras(ctx.limites.data.subprefeituras.map((sp) => porSub.get(sp.sigla) || 0).filter((n) => n > 0), HEX_RAMP.length);
    const cor = ["match", ["get", "sigla"], ...ctx.limites.data.subprefeituras.flatMap((sp) => [sp.sigla, corDaContagem(porSub.get(sp.sigla) || 0)]), "#FFFFFF"];
    map.setPaintProperty("limites-fill", "fill-color", comCoropleta ? cor : "#FFFFFF");
    map.setPaintProperty("limites-fill", "fill-opacity", comCoropleta ? 0.72 : 0);
    map.setFilter("limites-sel", ["==", ["get", "sigla"], store.filters.subprefeitura || ""]);
  }
  vis(["limites-fill", "limites-line", "limites-sel", "limites-label"], view.layers.limites && ctx.limites.status === "ok");

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
      const serie = ctx.nivel.status === "ok" ? serieNivel(ctx.nivel, "horario", s.id) : null;
      const ult = serie?.length ? serie[serie.length - 1] : null;
      return { type: "Feature", geometry: { type: "Point", coordinates: [s.lon, s.lat] }, properties: { id: s.id, sem: ult === null ? 1 : 0, rotulo: ult === null ? "sem dado" : `${fmt2(ult)} m` } };
    }),
  });
  vis(["nivel-icon", "nivel-label"], view.layers.nivel);

  if (ctx.susc.status === "ok" && !map.getSource("susc")._loadedOnce) {
    map.getSource("susc").setData({
      type: "FeatureCollection",
      features: [...ctx.susc.data.areas].sort((a, b) => a.grau - b.grau).map((a) => ({ type: "Feature", geometry: { type: "MultiPolygon", coordinates: a.geometria }, properties: { id: a.id, grau: a.grau } })),
    });
    map.getSource("susc")._loadedOnce = true;
  }
  vis(["susc-fill", "susc-line-media", "susc-line-alta"], view.layers.suscetibilidade);

  for (const id of Object.keys(SOB_DEMANDA)) {
    if (view.layers[id]) carregarSobDemanda(id);
  }
  vis(["setores-fill", "setores-line"], view.layers.setores && ctx.setores?.status === "ok");
  vis(["vias-line"], view.layers.vias && ctx.vias?.status === "ok");
  vis(["agua-cursos-line", "agua-corpos-fill", "agua-label"], view.layers.hidrografia && ctx.hidrografia?.status === "ok");

  // recorte territorial: aproxima a subprefeitura selecionada
  const sp = store.filters.subprefeitura;
  if (sp !== lastSubpref) {
    const prev = lastSubpref;
    lastSubpref = sp;
    const lim = ctx.limites.status === "ok" ? ctx.limites.data.subprefeituras.find((x) => x.sigla === sp) : null;
    if (lim) map.fitBounds([[lim.envelope[0], lim.envelope[1]], [lim.envelope[2], lim.envelope[3]]], { padding: 60, duration: 600 });
    else if (sp) {
      const bb = bboxOf([...occ, ...st, ...nv]);
      if (bb) map.fitBounds(bb, { padding: 60, maxZoom: 14, duration: 600 });
    } else if (prev) map.easeTo({ center: CONFIG.MAP_CENTER, zoom: CONFIG.MAP_ZOOM, duration: 600 });
  }
  renderScope(occ, vals);
  renderLayersPanel();
  renderLegend();
}

function corDaContagem(n) {
  if (!n) return "#F8FAFC";
  return HEX_RAMP[Math.min(HEX_RAMP.length - 1, view.subBreaks.filter((b) => n >= b).length)];
}

function atualizarHex(occ = currentOcc()) {
  if (!map) return;
  view.hexRaio = raioPorZoom(map.getZoom());
  const cells = hexbin(occ, view.hexRaio);
  view.hexCells = new Map(cells.map((c) => [c.id, c]));
  view.hexBreaks = quebras(cells.map((c) => c.n), HEX_RAMP.length);
  map.getSource("ochex").setData({
    type: "FeatureCollection",
    features: cells.map((c) => ({ type: "Feature", geometry: { type: "Polygon", coordinates: [c.anel] }, properties: { id: c.id, n: c.n } })),
  });
  map.setPaintProperty("oc-hex", "fill-color", view.hexBreaks.length
    ? ["step", ["get", "n"], HEX_RAMP[0], ...view.hexBreaks.flatMap((b, i) => [b, HEX_RAMP[i + 1]])]
    : HEX_RAMP[HEX_RAMP.length - 1]);
}

const publicado = (id) => Boolean(getManifest()?.datasets.some((d) => d.id === id));

async function carregarSobDemanda(id) {
  if (ctx[id] || !publicado(id)) return;
  ctx[id] = { status: "carregando" };
  renderLayersPanel();
  const r = await load(SOB_DEMANDA[id]);
  ctx[id] = r;
  if (!map) return;
  if (r.status === "ok" && id === "hidrografia") {
    const { cursos, corpos } = r.data;
    map.getSource("agua-corpos").setData({ type: "FeatureCollection", features: corpos.geometria.map((g, i) => ({ type: "Feature", geometry: { type: "MultiPolygon", coordinates: g }, properties: { i } })) });
    map.getSource("agua-cursos").setData({
      type: "FeatureCollection",
      features: cursos.geometria.map((g, i) => ({ type: "Feature", geometry: { type: "MultiLineString", coordinates: g }, properties: { i, w: LARGURA_AGUA[cursos.classe[i]] || 1, nome: cursos.nome[i] || "" } })),
    });
  } else if (r.status === "ok") {
    const c = r.data;
    const features = id === "setores"
      ? c.geometria.map((g, i) => ({ type: "Feature", geometry: { type: "MultiPolygon", coordinates: g }, properties: { i, d: c.densidade[i] } }))
      : c.geometria.map((g, i) => ({ type: "Feature", geometry: { type: "MultiLineString", coordinates: g }, properties: { i, h: c.hierarquia[i] } }))
          .sort((a, b) => b.properties.h - a.properties.h);
    map.getSource(id).setData({ type: "FeatureCollection", features });
  }
  refreshData();
}

function renderScope(occ, vals) {
  const f = store.filters;
  const per = resolvePeriod();
  const kpi = (rotulo, valor, sub = "") => `<div class="kpistrip__item"><span>${rotulo}</span><b>${valor}</b>${sub ? `<small>${sub}</small>` : ""}</div>`;
  const itens = [];
  if (ctx.occ.status === "ok" && view.layers.ocorrencias) {
    itens.push(kpi("Ocorrências", fmtInt(occ.length), esc(per.rotulo.toLowerCase())));
    const tipos = [...groupCount(occ.map((o) => o.tipo))].sort((a, b) => b[1] - a[1]);
    if (tipos.length) itens.push(kpi("Mais frequente", `${icon(TIPO_ICONE[tipos[0][0]])}${esc(tipos[0][0])}`, `${fmtInt(Math.round((tipos[0][1] / occ.length) * 100))}% do recorte`));
    const sps = [...groupCount(occ.map((o) => o.subprefeitura).filter(Boolean))].sort((a, b) => b[1] - a[1]);
    if (sps.length && !f.subprefeitura) itens.push(kpi("Mais registros", esc(subprefNome(sps[0][0])), `${fmtInt(sps[0][1])} ocorrências`));
  }
  if (ctx.st.horaria?.status === "ok" && view.layers.chuva) {
    const validas = vals.filter(({ s, r }) => r.mm !== null && !REDES_EM_REVISAO[s.rede]);
    const media = validas.length ? validas.reduce((a, { r }) => a + r.mm, 0) / validas.length : null;
    const janela = view.rainWindow === "periodo" ? "no período" : `em ${WINDOWS.find((w) => w[0] === view.rainWindow)[1]}`;
    itens.push(kpi(`Chuva média ${janela}`, media === null ? "sem leitura" : `${fmt1(media)} mm`,
      `${fmtInt(validas.length)} estações${Object.keys(REDES_EM_REVISAO).length ? `, sem ${Object.keys(REDES_EM_REVISAO).join(", ")} (em revisão)` : ""}`));
  }
  $("#m-scope").innerHTML = `<div class="kpistrip">
    <div class="kpistrip__lugar">${icon("pin")}<strong>${esc(f.subprefeitura ? subprefNome(f.subprefeitura) : "Município de São Paulo")}</strong><span>${esc(per.texto)}</span></div>
    ${itens.join("")}
  </div>`;
}

/* ------------------------------------------------------------------ legenda fixa */
function renderLegend() {
  const host = $("#m-legend");
  if (!host || !ctx) return;
  const aberta = host.querySelector("details")?.open ?? window.innerWidth > 700;
  const zoom = map?.getZoom() ?? CONFIG.MAP_ZOOM;
  const blocos = [];
  const comCoropleta = view.coropletico && view.layers.limites && ctx.limites.status === "ok";
  if (comCoropleta) {
    const br = view.subBreaks;
    const faixas = HEX_RAMP.slice(0, br.length + 1).map((c, i) => {
      const de = i === 0 ? 1 : br[i - 1], ate = i < br.length ? br[i] - 1 : null;
      return `<span class="legend__faixa"><i style="background:${c}"></i>${ate === null ? `${fmtInt(de)}+` : de === ate ? fmtInt(de) : `${fmtInt(de)}–${fmtInt(ate)}`}</span>`;
    }).join("");
    blocos.push(`<div class="legend__bloco"><strong>Ocorrências por subprefeitura</strong><div class="legend__faixas">${faixas}</div><small>Contagem no recorte; subprefeituras maiores tendem a somar mais registros.</small></div>`);
  } else if (view.layers.limites && ctx.limites.status === "ok")
    blocos.push(`<div class="legend__bloco"><strong>Subprefeituras</strong><div class="legend__row"><span class="legend__line" style="height:2px;background:#475569"></span>limite <span class="legend__line" style="height:3px;background:#155EEF"></span>selecionada</div></div>`);
  if (view.layers.ocorrencias && ctx.occ.status === "ok") {
    const tipos = Object.entries(TIPO_ICONE).map(([t, g]) => `<span class="legend__tipo"><span class="legend__icone" style="background:${TYPE_COLORS[t]}">${icon(g)}</span>${esc(t)}</span>`).join("");
    if (zoom < ZOOM_PONTOS && !comCoropleta) {
      const br = view.hexBreaks;
      const faixas = HEX_RAMP.slice(0, br.length + 1).map((c, i) => {
        const de = i === 0 ? 1 : br[i - 1], ate = i < br.length ? br[i] - 1 : null;
        return `<span class="legend__faixa"><i style="background:${c}"></i>${ate === null ? `${fmtInt(de)}+` : de === ate ? fmtInt(de) : `${fmtInt(de)}–${fmtInt(ate)}`}</span>`;
      }).join("");
      blocos.push(`<div class="legend__bloco"><strong>Ocorrências por hexágono (${fmtInt(view.hexRaio * 2)} m)</strong><div class="legend__faixas">${faixas}</div><small>Aproxime para ver cada ocorrência pelo tipo:</small><div class="legend__tipos">${tipos}</div></div>`);
    } else blocos.push(`<div class="legend__bloco"><strong>Ocorrências por tipo</strong><div class="legend__tipos">${tipos}</div></div>`);
  }
  if (view.layers.chuva && ctx.st.est.status === "ok") {
    const br = view.rainBreaks || [1, 5, 10, 20, 40, 60, 80];
    blocos.push(`<div class="legend__bloco"><strong>Chuva acumulada (mm)</strong><div class="legend__row">${RAIN_RAMP.map((c) => `<span class="legend__ramp" style="background:${c}"></span>`).join("")}</div><div class="legend__scale"><span>0</span><span>${fmtInt(br[br.length - 1])}+</span></div><small><span class="legend__dot legend__dot--sem"></span> sem leitura · escala de chuva, não de risco</small></div>`);
  }
  if (view.layers.nivel && ctx.nivel.status === "ok")
    blocos.push(`<div class="legend__bloco"><strong>Nível de rios</strong><div class="legend__row"><span class="legend__diamond" style="background:#0F8A73"></span>com leitura <span class="legend__diamond" style="background:#94A3B8"></span>sem leitura</div></div>`);
  if (view.layers.suscetibilidade && ctx.susc.status === "ok")
    blocos.push(`<div class="legend__bloco"><strong>Suscetibilidade a inundação</strong><div class="legend__faixas">${[3, 2, 1].map((g) => `<span class="legend__faixa"><i style="background:${SUSC_COLORS[g]}"></i>${["", "Baixa", "Média", "Alta"][g]}</span>`).join("")}</div></div>`);
  if (view.layers.hidrografia && ctx.hidrografia?.status === "ok")
    blocos.push(`<div class="legend__bloco"><strong>Hidrografia (OpenStreetMap)</strong><div class="legend__row"><span class="legend__line" style="height:3px;background:${AGUA_LINHA}"></span>rio <span class="legend__line" style="height:1.5px;background:${AGUA_LINHA}"></span>córrego <span class="legend__sw" style="background:${AGUA_AREA};outline:1px solid ${AGUA_LINHA}"></span>represa ou lago</div><small>Córregos canalizados podem não aparecer.</small></div>`);
  if (view.layers.setores && ctx.setores?.status === "ok")
    blocos.push(`<div class="legend__bloco"><strong>Pessoas por km²</strong><div class="legend__row">${DENS_RAMP.map((c) => `<span class="legend__ramp" style="background:${c}"></span>`).join("")}</div><div class="legend__scale"><span>0</span><span>${fmtInt(DENS_BREAKS[DENS_BREAKS.length - 1])}+</span></div></div>`);
  if (view.layers.vias && ctx.vias?.status === "ok")
    blocos.push(`<div class="legend__bloco"><strong>Vias principais</strong><div class="legend__row"><span class="legend__line" style="height:4px;background:${VIA_COR}"></span>expressas <span class="legend__line" style="height:2px;background:${VIA_COR}"></span>coletoras</div></div>`);
  host.innerHTML = blocos.length ? `<details class="maplegend" ${aberta ? "open" : ""}><summary>${icon("layers")}Legenda</summary>${blocos.join("")}</details>` : "";
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
  const winLabel = view.rainWindow === "periodo" ? `${per.texto}` : `últimas ${WINDOWS.find((w) => w[0] === view.rainWindow)[1]} até ${fmtDateTime(ctx.st.horaria?.meta?.referencia)}`;
  const m = getManifest();
  const naoPub = Object.fromEntries((m.nao_publicados || []).map((n) => [n.id, n.motivo]));

  host.innerHTML = [
    layerRow("ocorrencias", "Ocorrências históricas", `${ctx.occ.status === "ok" ? "GeoSampa · hexágonos no zoom afastado, ícones por tipo no zoom próximo" : "Indisponível"}`,
      `<div class="note">${natureBadge(occMeta)} ${occMeta ? freshnessBadge(occMeta, "ocorrencias") : ""}</div>
      <div class="note">Período: ${esc(per.texto)}${occMeta && per.fim > occMeta.periodo.fim ? ` · publicados até ${esc(fmtDay(occMeta.periodo.fim))}` : ""}</div>`),
    layerRow("chuva", "Chuva observada", "Pluviômetros CEMADEN, SIBH e INMET · mm",
      `<div class="seg" role="group" aria-label="Janela da chuva acumulada">${WINDOWS.map(([k, t]) => `<button type="button" data-win="${k}" aria-pressed="${view.rainWindow === k}">${t}</button>`).join("")}</div>
      <div class="note" style="margin-top:8px">${esc(winLabel)}. ${ctx.st.horaria?.meta ? freshnessBadge(ctx.st.horaria.meta, "chuva_horaria") : ""}</div>`,
      { disabled: ctx.st.est.status !== "ok", reason: "Dados de chuva não publicados nesta versão." }),
    layerRow("nivel", "Nível de rios e córregos", "Sensores fluviométricos do SIBH · cota em m",
      `<div class="note">${esc(ctx.nivel.meta?.limiares || "")}</div>`, { disabled: ctx.nivel.status !== "ok", reason: "Nível não publicado nesta versão." }),
    layerRow("suscetibilidade", "Suscetibilidade a inundação", "SGB/CPRM, carta de 2015 · não é previsão",
      `<div class="note">Predisposição do terreno a inundar, sem considerar a chuva. ${susc.meta ? natureBadge(susc.meta) : ""}</div>`,
      { disabled: susc.status !== "ok", reason: "Suscetibilidade não publicada nesta versão." }),
    layerRow("limites", "Subprefeituras", "Limites do GeoSampa · clique para ver o resumo",
      `<label class="check"><input type="checkbox" data-coropletico ${view.coropletico ? "checked" : ""}><span class="check__text"><span>Colorir pelo número de ocorrências</span><span class="check__hint">Substitui os hexágonos enquanto estiver ligado.</span></span></label>`,
      { disabled: ctx.limites.status !== "ok", reason: naoPub.limites || "Limites não publicados nesta versão." }),
    layerRow("hidrografia", "Rios, córregos e represas", "OpenStreetMap · cursos e corpos d'água", legendaGeo("hidrografia"),
      { disabled: !publicado("hidrografia"), reason: naoPub.hidrografia || "Hidrografia não publicada nesta versão." }),
    layerRow("setores", "Densidade populacional (IBGE)", "Setores censitários do Censo 2022 · hab/km²", legendaGeo("setores"),
      { disabled: !publicado("setores"), reason: naoPub.setores || "Não publicado." }),
    layerRow("vias", "Vias principais (OpenStreetMap)", "Expressas, arteriais, coletoras e acessos", legendaGeo("vias"),
      { disabled: !publicado("vias"), reason: naoPub.vias || "Não publicado." }),
  ].join("");

  host.querySelectorAll("[data-layer]").forEach((cb) =>
    cb.addEventListener("change", () => {
      view.layers[cb.dataset.layer] = cb.checked;
      refreshData();
      renderLayersPanel();
      host.querySelector(`[data-layer="${cb.dataset.layer}"]`)?.focus();
    }),
  );
  host.querySelector("[data-coropletico]")?.addEventListener("change", (e) => {
    view.coropletico = e.target.checked;
    refreshData();
    host.querySelector("[data-coropletico]")?.focus();
  });
  host.querySelectorAll("[data-win]").forEach((b) =>
    b.addEventListener("click", () => {
      view.rainWindow = b.dataset.win;
      refreshData();
      host.querySelector(`[data-win="${b.dataset.win}"]`)?.focus();
    }),
  );
}

function legendaGeo(id) {
  const r = ctx[id];
  const nome = { setores: "Setores censitários", vias: "Vias", hidrografia: "Hidrografia" }[id];
  if (!r || r.status === "carregando") return stateBlock("loading", `Baixando: ${nome.toLowerCase()}…`);
  if (r.status !== "ok") return resultState(r, nome);
  if (id === "hidrografia") return `<div class="note">${esc(r.meta.observacao || "")} Dados © colaboradores do OpenStreetMap, ODbL.</div>`;
  if (id === "setores") return `<div class="note">Dado agregado por setor: não identifica pessoas. ${natureBadge(r.meta)}</div>`;
  return `<div class="note">${esc(r.meta.observacao || "")} Dados © colaboradores do OpenStreetMap, ODbL.</div>`;
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
      <ul class="reclist">${recent.map((o) => `<li><button type="button" data-oc="${esc(o.key)}"><span class="legend__icone legend__icone--sm" style="background:${TYPE_COLORS[o.tipo]}">${icon(TIPO_ICONE[o.tipo])}</span><span><strong>${esc(o.tipo)}</strong> · ${esc(fmtDay(o.data))}<small>${esc(subprefNome(o.subprefeitura))}</small></span></button></li>`).join("")}</ul>
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
    <div class="pill-list"><span class="legend__icone" style="background:${TYPE_COLORS[o.tipo]}">${icon(TIPO_ICONE[o.tipo])}</span>${natureBadge(ctx.occ.meta)}</div>
    <dl class="dl" style="margin-top:12px">
      <dt>Data</dt><dd>${esc(fmtDay(o.data))} <span class="muted">(sem hora no registro)</span></dd>
      <dt>Tipo</dt><dd>${esc(o.tipo)}</dd>
      <dt>Subprefeitura</dt><dd>${esc(subprefNome(o.subprefeitura))}</dd>
      <dt>Coordenadas</dt><dd class="mono">${o.lat.toFixed(5)}, ${o.lon.toFixed(5)} (SIRGAS 2000)</dd>
      <dt>Território</dt><dd>${esc(g)}</dd>
      ${ctx.occ.meta?.dist_agua_max_m ? `<dt>Rede hídrica</dt><dd>${o.dist === null ? `Nenhum curso ou corpo d'água mapeado a até ${fmtInt(ctx.occ.meta.dist_agua_max_m)} m` : o.dist === 0 ? "Dentro de um corpo d'água mapeado" : `Curso ou corpo d'água mapeado a ${fmtInt(o.dist)} m (OpenStreetMap)`}</dd>` : ""}
      <dt>Identificação</dt><dd class="mono">arquivo ${o.ano_origem} · nº ${o.id}</dd>
      <dt>Tabela</dt><dd class="mono">gold.fact_geosampa_flooding_events</dd>
    </dl>
    <hr class="divider">${provenance(ctx.occ.meta)}
    <p class="note" style="margin-top:12px">Registro histórico: descreve algo que aconteceu, não a condição atual da via.</p>`, [o.lon, o.lat]);
}

function showHex(id) {
  const c = view.hexCells.get(id);
  if (!c) return;
  const tipos = Object.entries(c.tipos).sort((a, b) => b[1] - a[1]);
  openDetail(`${fmtInt(c.n)} ocorrência${c.n > 1 ? "s" : ""} nesta área`, `
    <p>Hexágono de ${fmtInt(view.hexRaio * 2)} m de largura, no recorte de ${esc(resolvePeriod().texto)}. Aproxime para ver cada ocorrência.</p>
    <ul class="mini-bars">${tipos.map(([t, n]) => `<li><span class="mini-bars__tipo"><span class="legend__icone legend__icone--sm" style="background:${TYPE_COLORS[t]}">${icon(TIPO_ICONE[t])}</span>${esc(t)}</span><span class="mini-bars__bar"><i style="width:${(n / c.n) * 100}%;background:${TYPE_COLORS[t]}"></i></span><b>${fmtInt(n)}</b></li>`).join("")}</ul>
    <button type="button" class="btn btn--primary btn--sm" id="zoom-hex">${icon("crosshair")}Aproximar esta área</button>
    <hr class="divider">${provenance(ctx.occ.meta, { periodo: resolvePeriod().texto })}`, c.centro);
  $("#zoom-hex").addEventListener("click", () => map.easeTo({ center: c.centro, zoom: ZOOM_PONTOS + 0.5 }));
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
  const serie = serieNivel(ctx.nivel, "horario", id);
  const ult = serie.length ? serie[serie.length - 1] : null;
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

function showAgua(tipo, i, lngLat) {
  const c = ctx.hidrografia.data[tipo];
  openDetail(c.nome[i] || (tipo === "cursos" ? "Curso d'água sem nome no OpenStreetMap" : "Corpo d'água sem nome no OpenStreetMap"), `
    <div class="pill-list"><span class="legend__icone" style="background:${AGUA_LINHA}">${icon("rio")}</span>${natureBadge(ctx.hidrografia.meta)}</div>
    <dl class="dl" style="margin-top:12px">
      <dt>Classe</dt><dd>${esc(c.classe[i] || "—")}</dd>
      ${tipo === "cursos" ? `<dt>Comprimento</dt><dd>${fmt2(c.km[i])} km</dd>` : `<dt>Área</dt><dd>${fmt2(c.area_km2[i])} km²</dd>`}
      <dt>Identificador OSM</dt><dd class="mono">${esc(c.id[i])}</dd>
    </dl>
    <p class="note" style="margin-top:12px">${esc(ctx.hidrografia.meta.observacao || "")} Dados © colaboradores do OpenStreetMap, ODbL 1.0.</p>
    <hr class="divider">${provenance(ctx.hidrografia.meta)}`, [lngLat.lng, lngLat.lat]);
}

function showSubpref(sigla, lngLat) {
  const sp = ctx.limites.data.subprefeituras.find((x) => x.sigla === sigla);
  if (!sp) return;
  const occ = currentOcc().filter((o) => o.subprefeitura === sigla);
  const tipos = [...groupCount(occ.map((o) => o.tipo))].sort((a, b) => b[1] - a[1]);
  const estacoes = ctx.st.est.status === "ok" ? ctx.st.est.data.chuva.filter((s) => s.subprefeitura === sigla).length : 0;
  const per = resolvePeriod();
  openDetail(`Subprefeitura ${sp.nome}`, `
    <div class="mini-kpis">
      <div class="mini-kpi"><span>Ocorrências</span><b>${fmtInt(occ.length)}</b><small>${esc(per.texto)}</small></div>
      <div class="mini-kpi"><span>Por km²</span><b>${fmt1(occ.length / sp.area_km2)}</b><small>${fmt1(sp.area_km2)} km² de área</small></div>
    </div>
    ${tipos.length ? `<ul class="mini-bars">${tipos.map(([t, n]) => `<li><span class="mini-bars__tipo"><span class="legend__icone legend__icone--sm" style="background:${TYPE_COLORS[t]}">${icon(TIPO_ICONE[t])}</span>${esc(t)}</span><span class="mini-bars__bar"><i style="width:${(n / occ.length) * 100}%;background:${TYPE_COLORS[t]}"></i></span><b>${fmtInt(n)}</b></li>`).join("")}</ul>` : stateBlock("empty", "Nenhuma ocorrência no recorte")}
    <dl class="dl"><dt>Estações de chuva</dt><dd>${fmtInt(estacoes)}</dd><dt>Sigla</dt><dd class="mono">${esc(sp.sigla)}</dd></dl>
    <div class="row">${store.filters.subprefeitura === sigla
      ? `<button type="button" class="btn btn--secondary btn--sm" id="sub-filtro" data-sigla="">${icon("reset")}Voltar ao município</button>`
      : `<button type="button" class="btn btn--primary btn--sm" id="sub-filtro" data-sigla="${esc(sigla)}">${icon("filter")}Filtrar por esta subprefeitura</button>`}</div>
    <hr class="divider">${provenance(ctx.limites.meta)}`, [lngLat.lng, lngLat.lat]);
  $("#sub-filtro").addEventListener("click", (e) => store.setFilters({ subprefeitura: e.currentTarget.dataset.sigla }));
}

function showSetor(i, lngLat) {
  const c = ctx.setores.data;
  openDetail(`Setor censitário ${c.id[i]}`, `
    <div class="pill-list">${natureBadge(ctx.setores.meta)} ${c.favela[i] ? badge("neutral", "Favela e comunidade urbana", "home") : ""}</div>
    <div class="mini-kpis" style="margin-top:12px">
      <div class="mini-kpi"><span>Pessoas</span><b>${fmtInt(c.pessoas[i])}</b><small>Censo 2022</small></div>
      <div class="mini-kpi"><span>Densidade</span><b>${fmtInt(c.densidade[i])}</b><small>hab/km²</small></div>
    </div>
    <dl class="dl" style="margin-top:12px">
      <dt>Distrito</dt><dd>${esc(c.distrito[i] || "—")}</dd>
      <dt>Situação</dt><dd>${esc(c.situacao[i] || "—")}</dd>
      <dt>Domicílios</dt><dd>${fmtInt(c.domicilios[i])}</dd>
      <dt>Área</dt><dd>${fmt2(c.area_km2[i])} km²</dd>
      <dt>Tabela</dt><dd class="mono">gold.fact_ibge_census_tract_population</dd>
    </dl>
    <p class="note" style="margin-top:12px">${esc(ctx.setores.meta.observacao || "")}</p>
    <hr class="divider">${provenance(ctx.setores.meta)}`, [lngLat.lng, lngLat.lat]);
}

function showVia(i, lngLat) {
  const c = ctx.vias.data;
  const tipo = [c.ponte[i] && "ponte ou viaduto", c.tunel[i] && "túnel"].filter(Boolean).join(", ");
  openDetail(c.nome[i] || "Via sem nome no OpenStreetMap", `
    <div class="pill-list">${natureBadge(ctx.vias.meta)}</div>
    <dl class="dl" style="margin-top:12px">
      <dt>Classe</dt><dd>${esc(c.classe[i] || "—")}</dd>
      ${c.ref[i] ? `<dt>Referência</dt><dd>${esc(c.ref[i])}</dd>` : ""}
      ${tipo ? `<dt>Trecho</dt><dd>${esc(tipo)}</dd>` : ""}
      <dt>Comprimento</dt><dd>${fmt2(c.km[i])} km</dd>
      <dt>Identificador OSM</dt><dd class="mono">way ${esc(c.id[i])}</dd>
    </dl>
    <p class="note" style="margin-top:12px">Dados © colaboradores do OpenStreetMap, ODbL 1.0. Sem informação de trânsito nem de interdição.</p>
    <hr class="divider">${provenance(ctx.vias.meta)}`, [lngLat.lng, lngLat.lat]);
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
