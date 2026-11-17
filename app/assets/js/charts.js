// Gráficos em SVG sem dependências. Regras: um único eixo Y, barras ≤ 24 px
// com topo arredondado de 4 px, linhas de 2 px, grade discreta, tooltip no hover
// e alternância para tabela (acessibilidade). Texto nunca usa a cor da série.
import { esc, fmtInt, fmt1 } from "./util.js";
import { icon } from "./icons.js";

const PAD = { top: 22, right: 12, bottom: 28, left: 44 };

function niceTicks(max, count = 4) {
  if (!max || max <= 0) return [0, 1];
  const raw = max / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw) || raw;
  const ticks = [];
  for (let v = 0; v <= max + step * 0.001; v += step) ticks.push(+v.toFixed(6));
  if (ticks[ticks.length - 1] < max) ticks.push(+(ticks[ticks.length - 1] + step).toFixed(6));
  return ticks;
}
const tickFmt = (v) => (v >= 10000 ? `${fmt1(v / 1000)} mil` : Number.isInteger(v) ? fmtInt(v) : fmt1(v));

// Coluna com topo arredondado e base reta.
function colPath(x, y, w, h, r = 4) {
  if (h <= 0) return "";
  const rr = Math.min(r, w / 2, h);
  return `M${x},${y + h}V${y + rr}Q${x},${y} ${x + rr},${y}H${x + w - rr}Q${x + w},${y} ${x + w},${y + rr}V${y + h}Z`;
}
function rowPath(x, y, w, h, r = 4) {
  if (w <= 0) return "";
  const rr = Math.min(r, h / 2, w);
  return `M${x},${y}H${x + w - rr}Q${x + w},${y} ${x + w},${y + rr}V${y + h - rr}Q${x + w},${y + h} ${x + w - rr},${y + h}H${x}Z`;
}

function mount(el, draw) {
  const render = () => {
    const width = Math.max(240, el.clientWidth || 600);
    el.innerHTML = draw(width);
    bindTooltip(el);
  };
  render();
  if (el._ro) el._ro.disconnect();
  let last = el.clientWidth;
  el._ro = new ResizeObserver(() => {
    if (Math.abs(el.clientWidth - last) > 8) {
      last = el.clientWidth;
      render();
    }
  });
  el._ro.observe(el);
}

function bindTooltip(el) {
  const tip = document.createElement("div");
  tip.className = "tooltip";
  tip.hidden = true;
  el.appendChild(tip);
  const tips = el._tips || [];
  const hover = el.querySelector(".hover-line");
  el.querySelectorAll("[data-tip]").forEach((node) => {
    const show = (evt) => {
      const t = tips[Number(node.dataset.tip)];
      if (!t) return;
      tip.innerHTML = t.html;
      tip.hidden = false;
      const box = el.getBoundingClientRect();
      const x = evt?.clientX ? evt.clientX - box.left : t.x;
      tip.style.left = `${Math.min(Math.max(x, 70), box.width - 70)}px`;
      tip.style.top = `${t.y}px`;
      if (hover && t.cx !== undefined) {
        hover.setAttribute("x1", t.cx);
        hover.setAttribute("x2", t.cx);
        hover.style.display = "";
      }
    };
    node.addEventListener("pointermove", show);
    node.addEventListener("focus", () => show(null));
    const hide = () => {
      tip.hidden = true;
      if (hover) hover.style.display = "none";
    };
    node.addEventListener("pointerleave", hide);
    node.addEventListener("blur", hide);
  });
}

function axisY(ticks, y, width, unit) {
  return ticks
    .map(
      (t) => `<line class="gridline" x1="${PAD.left}" x2="${width - PAD.right}" y1="${y(t)}" y2="${y(t)}"/>
      <text class="lbl" x="${PAD.left - 8}" y="${y(t) + 4}" text-anchor="end">${tickFmt(t)}</text>`,
    )
    .join("") + (unit ? `<text class="lbl" x="0" y="${PAD.top - 6}" text-anchor="start">${esc(unit)}</text>` : "");
}

// Rótulos do eixo X espaçados para não colidirem.
function axisX(labels, xCenter, height, width) {
  // rótulos esparsos (vazios intencionais) aparecem todos; senão, espaça para não colidir
  const every = labels.some((l) => l === "") ? 1 : Math.max(1, Math.ceil((labels.length * 46) / (width - PAD.left - PAD.right)));
  return labels
    .map((l, i) => (l !== "" && i % every === 0 ? `<text class="lbl" x="${xCenter(i)}" y="${height - 8}" text-anchor="middle">${esc(l)}</text>` : ""))
    .join("");
}

/**
 * Colunas (uma série) ou colunas empilhadas (várias séries).
 * opts: { labels, series:[{name, color, values}], unit, height, tipTitle(i), fmt }
 */
export function columnChart(el, { labels, series, unit = "", height = 240, tipTitle = (i) => labels[i], fmt = fmtInt, ariaLabel = "" }) {
  mount(el, (width) => {
    const n = labels.length;
    const totals = labels.map((_, i) => series.reduce((a, s) => a + (s.values[i] || 0), 0));
    const ticks = niceTicks(Math.max(...totals, 0));
    const maxY = ticks[ticks.length - 1] || 1;
    const plotW = width - PAD.left - PAD.right;
    const plotH = height - PAD.top - PAD.bottom;
    const band = plotW / n;
    const bw = Math.min(24, Math.max(2, band - 2));
    const y = (v) => PAD.top + plotH - (v / maxY) * plotH;
    const xC = (i) => PAD.left + band * i + band / 2;
    const tips = [];
    let marks = "";
    labels.forEach((_, i) => {
      let acc = 0;
      const x = xC(i) - bw / 2;
      series.forEach((s, si) => {
        const v = s.values[i] || 0;
        if (v <= 0) return;
        const h = (v / maxY) * plotH;
        const top = y(acc + v);
        const isTop = series.slice(si + 1).every((t) => !(t.values[i] > 0));
        const gap = acc > 0 ? 2 : 0; // fresta de 2 px entre segmentos
        marks += isTop
          ? `<path d="${colPath(x, top, bw, h - gap)}" fill="${s.color}"/>`
          : `<rect x="${x}" y="${top}" width="${bw}" height="${Math.max(0, h - gap)}" fill="${s.color}"/>`;
        acc += v;
      });
      const rows = series.length > 1
        ? series.map((s) => `<div class="t-row"><i style="background:${s.color}"></i>${esc(s.name)}: <b>${fmt(s.values[i] || 0)}</b></div>`).join("") +
          `<div class="t-row">Total: <b>${fmt(totals[i])}${unit ? " " + esc(unit) : ""}</b></div>`
        : `<div class="t-row"><b>${fmt(totals[i])}${unit ? " " + esc(unit) : ""}</b></div>`;
      tips.push({ html: `<div><b>${esc(tipTitle(i))}</b></div>${rows}`, y: Math.max(y(totals[i]), PAD.top + 10) });
      marks += `<rect class="hit" data-tip="${i}" x="${PAD.left + band * i}" y="${PAD.top}" width="${band}" height="${plotH}"/>`;
    });
    el._tips = tips;
    return `<svg width="${width}" height="${height}" role="img" aria-label="${esc(ariaLabel)}">
      ${axisY(ticks, y, width, unit)}
      ${marks}
      <line class="baseline" x1="${PAD.left}" x2="${width - PAD.right}" y1="${y(0)}" y2="${y(0)}"/>
      ${axisX(labels, xC, height, width)}
    </svg>`;
  });
}

/**
 * Linha(s) com crosshair. opts: { labels, series:[{name,color,values}], unit, area, height, tipTitle, fmt }
 */
export function lineChart(el, { labels, series, unit = "", area = false, height = 240, tipTitle = (i) => labels[i], fmt = fmt1, ariaLabel = "", minZero = true }) {
  mount(el, (width) => {
    const n = labels.length;
    const all = series.flatMap((s) => s.values.filter((v) => v !== null && v !== undefined));
    const vmax = Math.max(...all, 0);
    const vmin = minZero ? 0 : Math.min(...all);
    const ticks = minZero ? niceTicks(vmax) : niceRange(vmin, vmax);
    const lo = ticks[0], hi = ticks[ticks.length - 1] || 1;
    const plotW = width - PAD.left - PAD.right;
    const plotH = height - PAD.top - PAD.bottom;
    const x = (i) => PAD.left + (n <= 1 ? plotW / 2 : (i / (n - 1)) * plotW);
    const y = (v) => PAD.top + plotH - ((v - lo) / (hi - lo || 1)) * plotH;
    let paths = "";
    series.forEach((s) => {
      let d = "", started = false;
      s.values.forEach((v, i) => {
        if (v === null || v === undefined) { started = false; return; }
        d += `${started ? "L" : "M"}${x(i).toFixed(1)},${y(v).toFixed(1)}`;
        started = true;
      });
      if (area && series.length === 1) {
        const idx = s.values.map((v, i) => (v === null ? null : i)).filter((i) => i !== null);
        if (idx.length) paths += `<path d="${d}L${x(idx[idx.length - 1])},${y(lo)}L${x(idx[0])},${y(lo)}Z" fill="${s.color}" fill-opacity="0.1"/>`;
      }
      paths += `<path d="${d}" fill="none" stroke="${s.color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round"/>`;
      const lastI = s.values.map((v, i) => (v === null ? -1 : i)).reduce((a, b) => Math.max(a, b), -1);
      if (lastI >= 0) paths += `<circle cx="${x(lastI)}" cy="${y(s.values[lastI])}" r="4" fill="${s.color}" stroke="#fff" stroke-width="2"/>`;
    });
    const tips = [];
    const step = n > 1 ? plotW / (n - 1) : plotW;
    let hits = "";
    for (let i = 0; i < n; i++) {
      const rows = series
        .map((s) => `<div class="t-row">${series.length > 1 ? `<i style="background:${s.color}"></i>${esc(s.name)}: ` : ""}<b>${s.values[i] === null || s.values[i] === undefined ? "sem dado" : fmt(s.values[i]) + (unit ? " " + esc(unit) : "")}</b></div>`)
        .join("");
      const top = Math.min(...series.map((s) => (s.values[i] === null || s.values[i] === undefined ? Infinity : y(s.values[i]))));
      tips.push({ html: `<div><b>${esc(tipTitle(i))}</b></div>${rows}`, y: Number.isFinite(top) ? top : PAD.top + 20, cx: x(i) });
      hits += `<rect class="hit" data-tip="${i}" x="${x(i) - step / 2}" y="${PAD.top}" width="${step}" height="${plotH}"/>`;
    }
    el._tips = tips;
    return `<svg width="${width}" height="${height}" role="img" aria-label="${esc(ariaLabel)}">
      ${axisY(ticks, y, width, unit)}
      <line class="hover-line" x1="0" x2="0" y1="${PAD.top}" y2="${PAD.top + plotH}" style="display:none"/>
      ${paths}
      <line class="baseline" x1="${PAD.left}" x2="${width - PAD.right}" y1="${y(lo)}" y2="${y(lo)}"/>
      ${axisX(labels, x, height, width)}
      ${hits}
    </svg>`;
  });
}

function niceRange(min, max) {
  const span = max - min || 1;
  const raw = span / 4;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw);
  const lo = Math.floor(min / step) * step;
  const out = [];
  for (let v = lo; v <= max + step; v += step) {
    out.push(+v.toFixed(6));
    if (v >= max) break;
  }
  return out;
}

/**
 * Barras horizontais para rankings. opts: { rows:[{label, value, sub, color}], unit, fmt, onClick }
 */
export function barList(el, { rows, unit = "", fmt = fmtInt, color = "var(--series-1)", ariaLabel = "", max: forcedMax }) {
  mount(el, (width) => {
    const labelW = Math.min(190, Math.round(width * 0.38));
    const valueW = 64;
    const rowH = 28;
    const height = rows.length * rowH + 4;
    const max = forcedMax || Math.max(...rows.map((r) => r.value), 1);
    const plotW = width - labelW - valueW - 8;
    const tips = [];
    const out = rows
      .map((r, i) => {
        const y = i * rowH + 4;
        const w = Math.max(r.value > 0 ? 2 : 0, (r.value / max) * plotW);
        tips.push({ html: `<b>${esc(r.label)}</b><div class="t-row">${fmt(r.value)}${unit ? " " + esc(unit) : ""}${r.sub ? ` · ${esc(r.sub)}` : ""}</div>`, y: y + 4, x: labelW + w });
        const maxChars = Math.max(6, Math.floor((labelW - 10) / 6.6));
        const label = r.label.length > maxChars ? r.label.slice(0, maxChars - 1) + "…" : r.label;
        return `<text class="lbl lbl--strong" x="0" y="${y + 15}"><title>${esc(r.label)}</title>${esc(label)}</text>
          <path d="${rowPath(labelW, y + 4, w, 16)}" fill="${r.color || color}"/>
          <text class="lbl" x="${labelW + w + 6}" y="${y + 16}">${fmt(r.value)}${unit ? " " + esc(unit) : ""}</text>
          <rect class="hit" data-tip="${i}" x="0" y="${y}" width="${width}" height="${rowH}"/>`;
      })
      .join("");
    el._tips = tips;
    return `<svg width="${width}" height="${height}" role="img" aria-label="${esc(ariaLabel)}">${out}</svg>`;
  });
}

// Sparkline estática (sem eixos), usada em cards e painéis de detalhe.
export function sparkline(values, { width = 160, height = 36, color = "var(--rain-5)", bars = true } = {}) {
  const max = Math.max(...values.map((v) => v || 0), 0.1);
  const n = values.length;
  if (bars) {
    const bw = Math.max(1, width / n - 1);
    return `<svg width="${width}" height="${height}" aria-hidden="true">${values
      .map((v, i) => {
        const h = ((v || 0) / max) * (height - 2);
        return v === null ? `<rect x="${i * (width / n)}" y="${height - 1}" width="${bw}" height="1" fill="var(--border-strong)"/>` : `<rect x="${i * (width / n)}" y="${height - h}" width="${bw}" height="${Math.max(h, 0.5)}" fill="${color}" rx="1"/>`;
      })
      .join("")}</svg>`;
  }
  const valid = values.filter((v) => v !== null);
  const lo = Math.min(...valid), hi = Math.max(...valid);
  let d = "";
  values.forEach((v, i) => {
    if (v === null) return;
    d += `${d ? "L" : "M"}${((i / (n - 1)) * width).toFixed(1)},${(height - 2 - ((v - lo) / (hi - lo || 1)) * (height - 4)).toFixed(1)}`;
  });
  return `<svg width="${width}" height="${height}" aria-hidden="true"><path d="${d}" fill="none" stroke="${color}" stroke-width="2" stroke-linejoin="round"/></svg>`;
}

// Contêiner com alternância gráfico ⇄ tabela. Retorna o elemento do gráfico.
export function chartBlock(host, { tableHtml, legend = "" }) {
  host.innerHTML = `${legend ? `<div class="series-keys" style="margin-bottom:8px">${legend}</div>` : ""}
    <div class="chart" data-chart></div>
    <div data-table hidden></div>
    <div class="chart-tools"><button type="button" class="linkbtn" data-toggle aria-pressed="false">${icon("table")}Ver como tabela</button></div>`;
  const chartEl = host.querySelector("[data-chart]");
  const tableEl = host.querySelector("[data-table]");
  tableEl.innerHTML = tableHtml;
  const btn = host.querySelector("[data-toggle]");
  btn.addEventListener("click", () => {
    const showTable = tableEl.hidden;
    tableEl.hidden = !showTable;
    chartEl.hidden = showTable;
    btn.setAttribute("aria-pressed", String(showTable));
    btn.innerHTML = showTable ? `${icon("chart")}Ver como gráfico` : `${icon("table")}Ver como tabela`;
  });
  return chartEl;
}

export const legendKeys = (series) => series.map((s) => `<span class="series-key"><i style="background:${s.color}"></i>${esc(s.name)}</span>`).join("");

export const TYPE_COLORS = {
  Alagamento: "#2A78D6",
  "Queda de Árvore": "#EB6834",
  Inundação: "#1BAF7A",
  Deslizamento: "#EDA100",
};
export const RAIN_RAMP = ["#EAF2FD", "#CDE2FB", "#9EC5F4", "#6DA7EC", "#3987E5", "#256ABF", "#184F95", "#0D366B"];
