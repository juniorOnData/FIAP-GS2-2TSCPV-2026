// Monitoramento de estações (F07): chuva acumulada por janela e nível hidrológico.
import { CONFIG } from "../config.js";
import { icon } from "../icons.js";
import { esc, fmtInt, fmt1, fmt2, fmtDateTime, fmtHourShort, fmtAge, fmtDay, addDays, $ } from "../util.js";
import { store } from "../store.js";
import { getStations, filterStations, rainLastHours, rainInPeriod, load, freshness, hourlyTimes } from "../data.js";
import { filterBar, bindFilterBar, subprefNome, stateBlock, resultState, natureBadge, freshnessBadge, provenance, card, table, badge } from "../ui.js";
import { columnChart, lineChart, chartBlock, sparkline, RAIN_RAMP } from "../charts.js";

const WINDOWS = [
  { k: "1h", t: "1 hora", h: 1 },
  { k: "24h", t: "24 horas", h: 24 },
  { k: "72h", t: "72 horas", h: 72 },
  { k: "7d", t: "7 dias", h: 168 },
  { k: "30d", t: "30 dias", d: 30 },
];
let win = "24h";
let selected = null;
let selectedLevel = null;

export default {
  subtitle: "Chuva acumulada por estação e nível de rios e córregos",
  async render(root, { params }) {
    if (params.get("estacao")) selected = params.get("estacao");
    if (params.get("nivel")) selectedLevel = params.get("nivel");
    root.innerHTML = `<div class="page">
      ${filterBar({ id: "fb", show: ["subprefeitura", "rede"] })}
      <div id="fresh"></div>
      <div class="grid grid--12">
        <div class="span-7" id="rain-table"></div>
        <div class="span-5" id="rain-detail"></div>
      </div>
      <h3 class="section-title">Nível de rios e córregos</h3>
      <div class="grid grid--12">
        <div class="span-7" id="lvl-table"></div>
        <div class="span-5" id="lvl-detail"></div>
      </div>
    </div>`;
    bindFilterBar(root);

    const st = await getStations();
    const nivel = await load("nivel");
    const bad = resultState(st.est, "Cadastro de estações") || resultState(st.horaria, "Chuva horária") || resultState(st.diaria, "Chuva diária");
    if (bad) {
      root.querySelector("#rain-table").innerHTML = bad;
      return;
    }

    // estado de atualização (F10)
    const fr = freshness(st.horaria.meta, "chuva_horaria");
    root.querySelector("#fresh").innerHTML = fr.estado === "desatualizado"
      ? `<div style="margin-bottom:16px">${stateBlock("stale", `Leituras desatualizadas: a última hora publicada é de ${fmtDateTime(fr.ref)} (${fmtAge(fr.ref)})`, `As janelas abaixo terminam nessa hora, não no horário atual. Limite para considerar atual: ${fr.limite} h.`)}</div>`
      : "";

    const drawRain = () => {
      const f = store.filters;
      const est = filterStations(st.est.data.chuva, f);
      const w = WINDOWS.find((x) => x.k === win);
      const ref = st.horaria.meta.referencia;
      const rows = est.map((s) => {
        const r = w.h ? rainLastHours(st.horaria, s.id, w.h) : rainInPeriod(st.diaria, s.id, addDays(ref.slice(0, 10), -(w.d - 1)), ref.slice(0, 10));
        return { s, ...r };
      }).sort((a, b) => (b.mm ?? -1) - (a.mm ?? -1));
      if (!selected || !est.find((s) => s.id === selected)) selected = rows[0]?.s.id || null;

      const seg = `<div class="seg" role="group" aria-label="Janela de acumulação">${WINDOWS.map((x) => `<button type="button" data-win="${x.k}" aria-pressed="${x.k === win}">${x.t}</button>`).join("")}</div>`;
      const max = Math.max(...rows.map((r) => r.mm || 0), 1);
      const host = root.querySelector("#rain-table");
      host.innerHTML = card({
        title: "Chuva acumulada por estação",
        sub: `Janela móvel de ${w.t} até ${esc(fmtDateTime(ref))} · ${esc(CONFIG.TZ_LABEL)}`,
        actions: `<div class="pill-list">${natureBadge(st.horaria.meta)} ${freshnessBadge(st.horaria.meta, "chuva_horaria")}</div>`,
        body: `<div style="margin-bottom:12px">${seg}</div>` + table(
          [
            { label: "Estação", fmt: (r) => `<strong>${esc(r.s.nome)}</strong>${r.s.ativa ? "" : ` ${badge("neutral", "Inativa")}`}<br><small class="muted">${esc(subprefNome(r.s.subprefeitura))}</small>` },
            { label: "Rede", fmt: (r) => esc(r.s.rede) },
            { label: "Acumulado", num: true, fmt: (r) => (r.mm === null ? '<span class="muted">sem leitura</span>' : `<span class="rain-cell"><i style="width:${(r.mm / max) * 60}px;background:${RAIN_RAMP[5]}"></i><b>${fmt1(r.mm)}</b> mm</span>`) },
            { label: "Leituras", num: true, fmt: (r) => (r.mm === null ? "—" : `${Math.round(r.cobertura * 100)}%`) },
          ],
          rows,
          { caption: "Leituras = proporção de horas (ou dias, na janela de 30 dias) com leitura. Selecione uma estação para ver a série.", rowAttr: (r) => `class="is-clickable ${r.s.id === selected ? "is-selected" : ""}" data-st="${esc(r.s.id)}" tabindex="0" aria-selected="${r.s.id === selected}"`, empty: "Nenhuma estação no recorte." },
        ),
        foot: provenance(st.horaria.meta),
      });
      host.querySelectorAll("[data-win]").forEach((b) => b.addEventListener("click", () => { win = b.dataset.win; drawRain(); root.querySelector(`[data-win="${win}"]`).focus(); }));
      host.querySelectorAll("[data-st]").forEach((tr) => {
        const pick = () => { selected = tr.dataset.st; drawRain(); root.querySelector(`[data-st="${CSS.escape(selected)}"]`)?.focus(); };
        tr.addEventListener("click", pick);
        tr.addEventListener("keydown", (e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), pick()));
      });
      drawStation();
    };

    const drawStation = () => {
      const host = root.querySelector("#rain-detail");
      const s = st.est.data.chuva.find((x) => x.id === selected);
      if (!s) { host.innerHTML = card({ title: "Série da estação", body: stateBlock("empty", "Selecione uma estação") }); return; }
      const times = hourlyTimes(st.horaria);
      const hv = st.horaria.data.series[s.id];
      const daily = st.diaria.data.series[s.id].slice(-30);
      const days = Array.from({ length: 30 }, (_, i) => addDays(st.diaria.meta.periodo.fim, i - 29));
      host.innerHTML = card({
        title: s.nome,
        sub: `${esc(s.rede)}${s.instituicao !== s.rede ? ` · ${esc(s.instituicao)}` : ""} · código ${esc(s.codigo)}`,
        actions: `<a class="btn btn--ghost btn--sm" href="#/mapa?estacao=${encodeURIComponent(s.id)}&center=${s.lon},${s.lat}&zoom=14">${icon("map")}Ver no mapa</a>`,
        body: `<h4 style="margin-bottom:4px">Chuva horária — últimos 7 dias</h4><p class="note">mm por hora; a hora marca o início do intervalo</p><div id="h-chart"></div>
          <h4 style="margin:16px 0 4px">Chuva diária — últimos 30 dias</h4><p class="note">mm por dia (soma das horas, fuso de São Paulo)</p><div id="d-chart"></div>`,
      });
      const hEl = chartBlock(host.querySelector("#h-chart"), { tableHtml: table([{ key: "t", label: "Hora" }, { key: "v", label: "mm", num: true }], times.map((t, i) => ({ t: fmtHourShort(t), v: hv[i] === null ? "sem leitura" : fmt1(hv[i]) })).reverse()) });
      columnChart(hEl, { labels: times.map((t) => (fmtHourShort(t).endsWith("00:00") ? fmtHourShort(t).slice(0, 5) : "")), series: [{ name: "Chuva", color: RAIN_RAMP[5], values: hv.map((v) => v ?? 0) }], unit: "mm", height: 180, fmt: fmt1, tipTitle: (i) => `${fmtHourShort(times[i])}${hv[i] === null ? " · sem leitura" : ""}`, ariaLabel: "Chuva horária, últimos 7 dias" });
      const dEl = chartBlock(host.querySelector("#d-chart"), { tableHtml: table([{ key: "d", label: "Dia" }, { key: "v", label: "mm", num: true }], days.map((d, i) => ({ d: fmtDay(d), v: daily[i] === null ? "sem leitura" : fmt1(daily[i]) })).reverse()) });
      columnChart(dEl, { labels: days.map((d) => d.slice(8, 10) + "/" + d.slice(5, 7)), series: [{ name: "Chuva", color: RAIN_RAMP[4], values: daily.map((v) => v ?? 0) }], unit: "mm", height: 180, fmt: fmt1, tipTitle: (i) => `${fmtDay(days[i])}${daily[i] === null ? " · sem leitura" : ""}`, ariaLabel: "Chuva diária, últimos 30 dias" });
    };

    const drawLevel = () => {
      const host = root.querySelector("#lvl-table");
      const det = root.querySelector("#lvl-detail");
      const bad = resultState(nivel, "Nível hidrológico");
      if (bad) { host.innerHTML = bad; det.innerHTML = ""; return; }
      const sens = filterStations(st.est.data.nivel, store.filters);
      const rows = sens.map((s) => {
        const h = nivel.data.horario[s.id];
        const ult = h[h.length - 1];
        const ant = h[h.length - 25];
        const v = h.filter((x) => x !== null);
        return { s, ult, delta: ult !== null && ant !== null ? ult - ant : null, max: v.length ? Math.max(...v) : null, h };
      });
      if (!selectedLevel || !sens.find((s) => s.id === selectedLevel)) selectedLevel = rows[0]?.s.id || null;
      host.innerHTML = card({
        title: "Sensores de nível",
        sub: `Último valor horário até ${esc(fmtDateTime(nivel.meta.referencia))} · metros, na unidade informada pela fonte`,
        actions: `<div class="pill-list">${natureBadge(nivel.meta)} ${freshnessBadge(nivel.meta, "nivel")}</div>`,
        body: `${stateBlock("info", "Sem classificação de nível", esc(nivel.meta.limiares) + " Por isso nenhum sensor é marcado como normal, atenção ou crítico.")}<div style="height:12px"></div>` + table(
          [
            { label: "Sensor", fmt: (r) => `<strong>${esc(r.s.curso_dagua)}</strong><br><small class="muted">${esc(r.s.nome.split(" — ")[1] || "")} · ${esc(subprefNome(r.s.subprefeitura))}</small>` },
            { label: "Último", num: true, fmt: (r) => (r.ult === null ? '<span class="muted">sem leitura</span>' : `${fmt2(r.ult)} m`) },
            { label: "Variação 24 h", num: true, fmt: (r) => (r.delta === null ? "—" : `${r.delta >= 0 ? "+" : "−"}${fmt2(Math.abs(r.delta))} m`) },
            { label: "7 dias", fmt: (r) => (r.h.some((x) => x !== null) ? sparkline(r.h, { width: 96, height: 24, bars: false, color: "#0F8A73" }) : "—") },
          ],
          rows,
          { rowAttr: (r) => `class="is-clickable ${r.s.id === selectedLevel ? "is-selected" : ""}" data-lv="${esc(r.s.id)}" tabindex="0" aria-selected="${r.s.id === selectedLevel}"`, empty: "Nenhum sensor de nível no recorte." },
        ),
        foot: provenance(nivel.meta),
      });
      host.querySelectorAll("[data-lv]").forEach((tr) => {
        const pick = () => { selectedLevel = tr.dataset.lv; drawLevel(); root.querySelector(`[data-lv="${CSS.escape(selectedLevel)}"]`)?.focus(); };
        tr.addEventListener("click", pick);
        tr.addEventListener("keydown", (e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), pick()));
      });
      const s = st.est.data.nivel.find((x) => x.id === selectedLevel);
      if (!s) { det.innerHTML = ""; return; }
      const times = hourlyTimes({ data: { inicio: nivel.data.horario_inicio, series: nivel.data.horario } });
      const hv = nivel.data.horario[s.id];
      const dv = nivel.data.diario[s.id];
      const days = Array.from({ length: dv.length }, (_, i) => addDays(nivel.data.diario_inicio, i));
      det.innerHTML = card({
        title: s.curso_dagua, sub: `${esc(s.nome)} · ${esc(s.instituicao)}`,
        actions: `<a class="btn btn--ghost btn--sm" href="#/mapa?center=${s.lon},${s.lat}&zoom=14">${icon("map")}Ver no mapa</a>`,
        body: `<h4 style="margin-bottom:4px">Nível horário — 7 dias</h4><p class="note">metros (unidade da fonte), média da hora</p><div id="lh"></div>
          <h4 style="margin:16px 0 4px">Nível médio diário — 12 meses</h4><div id="ld"></div>`,
      });
      const a = chartBlock(det.querySelector("#lh"), { tableHtml: table([{ key: "t", label: "Hora" }, { key: "v", label: "m", num: true }], times.map((t, i) => ({ t: fmtHourShort(t), v: hv[i] === null ? "sem leitura" : fmt2(hv[i]) })).reverse()) });
      if (hv.some((v) => v !== null)) lineChart(a, { labels: times.map((t) => (fmtHourShort(t).endsWith("00:00") ? fmtHourShort(t).slice(0, 5) : "")), series: [{ name: "Nível", color: "#0F8A73", values: hv }], unit: "m", height: 180, fmt: fmt2, minZero: false, tipTitle: (i) => fmtHourShort(times[i]), ariaLabel: "Nível horário" });
      else a.innerHTML = stateBlock("empty", "Sem leituras nos últimos 7 dias", "O sensor pode estar fora de operação.");
      const b = chartBlock(det.querySelector("#ld"), { tableHtml: table([{ key: "d", label: "Dia" }, { key: "v", label: "m", num: true }], days.map((d, i) => ({ d: fmtDay(d), v: dv[i] === null ? "sem leitura" : fmt2(dv[i]) })).reverse()) });
      lineChart(b, { labels: days.map((d) => d.slice(5, 7) + "/" + d.slice(2, 4)), series: [{ name: "Nível", color: "#0F8A73", values: dv }], unit: "m", height: 180, fmt: fmt2, minZero: false, tipTitle: (i) => fmtDay(days[i]), ariaLabel: "Nível médio diário" });
    };

    drawRain();
    drawLevel();
  },
};
