// Histórico (F08): séries de ocorrências, chuva e nível, respeitando a granularidade de cada fonte.
import { esc, fmtInt, fmt1, fmt2, fmtDay, fmtMonth, fmtMonthLong, groupCount, addDays, diffDays, MESES_LONGOS } from "../util.js";
import { store, resolvePeriod } from "../store.js";
import { getOccurrences, filterOccurrences, getStations, filterStations, load, monthsBetween, cityDaily } from "../data.js";
import { filterBar, bindFilterBar, subprefNome, stateBlock, resultState, natureBadge, provenance, card, table } from "../ui.js";
import { columnChart, lineChart, barList, chartBlock, legendKeys, TYPE_COLORS, RAIN_RAMP } from "../charts.js";

let levelSel = null;

export default {
  subtitle: "Séries de ocorrências, chuva e nível no recorte",
  async render(root) {
    const f = store.filters;
    const per = resolvePeriod();
    const nDias = diffDays(per.inicio, per.fim) + 1;
    const daily = nDias <= 92;
    const gran = daily ? "dia" : "mês";
    root.innerHTML = `<div class="page">
      ${filterBar({ id: "fb" })}
      <div class="grid grid--12">
        <div class="span-12" id="h-occ"></div>
        <div class="span-12" id="h-rain"></div>
        <div class="span-6" id="h-seas-occ"></div>
        <div class="span-6" id="h-seas-rain"></div>
        <div class="span-6" id="h-tipo"></div>
        <div class="span-6" id="h-ano"></div>
        <div class="span-12" id="h-lvl"></div>
      </div>
    </div>`;
    bindFilterBar(root);

    const [occR, st, nivel] = await Promise.all([getOccurrences(), getStations(), load("nivel")]);
    const buckets = daily ? Array.from({ length: nDias }, (_, i) => addDays(per.inicio, i)) : monthsBetween(per.inicio, per.fim);
    const keyOf = (d) => (daily ? d : d.slice(0, 7));
    const lbl = (b) => (daily ? `${b.slice(8, 10)}/${b.slice(5, 7)}` : fmtMonth(b));
    const longLbl = (b) => (daily ? fmtDay(b) : fmtMonthLong(b));

    // ---------------- ocorrências
    if (occR.status === "ok") {
      const occ = filterOccurrences(occR.rows, f, per);
      const tipos = Object.keys(TYPE_COLORS).filter((t) => !f.tipos.length || f.tipos.includes(t));
      const series = tipos.map((t) => {
        const c = groupCount(occ.filter((o) => o.tipo === t).map((o) => keyOf(o.data)));
        return { name: t, color: TYPE_COLORS[t], values: buckets.map((b) => c.get(b) || 0) };
      });
      const host = root.querySelector("#h-occ");
      const fimPub = occR.meta.periodo.fim;
      host.innerHTML = card({
        title: `Ocorrências por ${gran}`, sub: `${esc(per.texto)} · ${fmtInt(occ.length)} registros${per.fim > fimPub ? ` · publicados até ${esc(fmtDay(fimPub))}` : ""}`,
        actions: natureBadge(occR.meta), body: '<div id="b1"></div>', foot: provenance(occR.meta, { periodo: per.texto }),
      });
      const el = chartBlock(host.querySelector("#b1"), {
        legend: series.length > 1 ? legendKeys(series) : "",
        tableHtml: table([{ key: "b", label: daily ? "Dia" : "Mês" }, ...series.map((s) => ({ label: s.name, num: true, fmt: (r) => fmtInt(r[s.name]) }))], buckets.map((b, i) => Object.fromEntries([["b", longLbl(b)], ...series.map((s) => [s.name, s.values[i]])]))),
      });
      occ.length ? columnChart(el, { labels: buckets.map(lbl), series, unit: "ocorrências", tipTitle: (i) => longLbl(buckets[i]), ariaLabel: `Ocorrências por ${gran}` }) : (el.innerHTML = stateBlock("empty", "Nenhuma ocorrência no recorte"));

      // sazonalidade (todo o histórico com os mesmos filtros de tipo e território)
      const hist = filterOccurrences(occR.rows, { ...f, periodo: "todo" });
      const anos = new Set(hist.map((o) => o.data.slice(0, 4))).size || 1;
      const porMes = groupCount(hist.map((o) => Number(o.data.slice(5, 7))));
      const sv = Array.from({ length: 12 }, (_, i) => (porMes.get(i + 1) || 0) / anos);
      const h2 = root.querySelector("#h-seas-occ");
      h2.innerHTML = card({ title: "Sazonalidade das ocorrências", sub: `Média de registros por mês do ano · ${anos} anos do histórico`, body: '<div id="b2"></div>', foot: "O período chuvoso de São Paulo vai de outubro a março." });
      const el2 = chartBlock(h2.querySelector("#b2"), { tableHtml: table([{ key: "m", label: "Mês" }, { key: "v", label: "Média", num: true }], sv.map((v, i) => ({ m: MESES_LONGOS[i], v: fmt1(v) }))) });
      columnChart(el2, { labels: MESES_LONGOS.map((m) => m.slice(0, 3)), series: [{ name: "Média", color: "#2A78D6", values: sv }], unit: "ocorrências/mês", fmt: fmt1, height: 200, tipTitle: (i) => MESES_LONGOS[i], ariaLabel: "Sazonalidade das ocorrências" });

      const porTipo = [...groupCount(occ.map((o) => o.tipo))].sort((a, b) => b[1] - a[1]);
      const h3 = root.querySelector("#h-tipo");
      h3.innerHTML = card({ title: "Por tipo de ocorrência", sub: esc(per.texto), body: '<div id="b3"></div>' });
      const rowsT = porTipo.map(([t, n]) => ({ label: t, value: n, color: TYPE_COLORS[t] }));
      const el3 = chartBlock(h3.querySelector("#b3"), { tableHtml: table([{ key: "label", label: "Tipo" }, { key: "value", label: "Ocorrências", num: true, fmt: (r) => fmtInt(r.value) }], rowsT) });
      rowsT.length ? barList(el3, { rows: rowsT, ariaLabel: "Ocorrências por tipo" }) : (el3.innerHTML = stateBlock("empty", "Sem registros"));

      const porAno = groupCount(hist.map((o) => o.data.slice(0, 4)));
      const anosL = [...porAno.keys()].sort();
      const h4 = root.querySelector("#h-ano");
      h4.innerHTML = card({ title: "Por ano", sub: "Todo o histórico com os filtros de tipo e território", body: '<div id="b4"></div>', foot: `O ano corrente está incompleto. ${f.subprefeitura ? esc(subprefNome(f.subprefeitura)) : "Município inteiro"}.` });
      const el4 = chartBlock(h4.querySelector("#b4"), { tableHtml: table([{ key: "a", label: "Ano" }, { key: "v", label: "Ocorrências", num: true }], anosL.map((a) => ({ a, v: fmtInt(porAno.get(a)) }))) });
      columnChart(el4, { labels: anosL, series: [{ name: "Ocorrências", color: "#2A78D6", values: anosL.map((a) => porAno.get(a)) }], unit: "ocorrências", height: 200, ariaLabel: "Ocorrências por ano" });
    } else root.querySelector("#h-occ").innerHTML = resultState(occR, "Ocorrências");

    // ---------------- chuva
    const bad = resultState(st.diaria, "Chuva diária") || resultState(st.est, "Estações");
    if (!bad) {
      const ids = filterStations(st.est.data.chuva, f).map((s) => s.id);
      const city = cityDaily(st.diaria, ids);
      const sums = new Map();
      for (let d = per.inicio; d <= per.fim; d = addDays(d, 1)) {
        const v = city.media[diffDays(city.inicio, d)];
        if (v === null || v === undefined) continue;
        sums.set(keyOf(d), (sums.get(keyOf(d)) || 0) + v);
      }
      const values = buckets.map((b) => sums.get(b) ?? null);
      const host = root.querySelector("#h-rain");
      host.innerHTML = card({
        title: `Chuva por ${gran} — média das estações`, sub: `${esc(per.texto)} · mm · média de ${fmtInt(ids.length)} estações${f.rede ? ` da rede ${esc(f.rede)}` : ""}`,
        actions: natureBadge(st.diaria.meta), body: '<div id="b5"></div>', foot: provenance(st.diaria.meta, { periodo: per.texto }),
      });
      const el = chartBlock(host.querySelector("#b5"), { tableHtml: table([{ key: "b", label: daily ? "Dia" : "Mês" }, { key: "v", label: "mm", num: true }], buckets.map((b, i) => ({ b: longLbl(b), v: fmt1(values[i]) }))) });
      columnChart(el, { labels: buckets.map(lbl), series: [{ name: "Chuva", color: RAIN_RAMP[5], values: values.map((v) => v ?? 0) }], unit: "mm", fmt: fmt1, tipTitle: (i) => longLbl(buckets[i]), ariaLabel: `Chuva por ${gran}` });

      // climatologia mensal
      const acc = Array.from({ length: 12 }, () => ({ s: 0, anos: new Set() }));
      city.media.forEach((v, i) => {
        if (v === null) return;
        const d = addDays(city.inicio, i);
        const m = Number(d.slice(5, 7)) - 1;
        acc[m].s += v;
        acc[m].anos.add(d.slice(0, 4));
      });
      const clim = acc.map((a) => (a.anos.size ? a.s / a.anos.size : null));
      const h2 = root.querySelector("#h-seas-rain");
      h2.innerHTML = card({ title: "Chuva média por mês do ano", sub: "mm · média das estações, todo o histórico", body: '<div id="b6"></div>', foot: "Meses do ano corrente ainda incompletos entram parcialmente." });
      const el2 = chartBlock(h2.querySelector("#b6"), { tableHtml: table([{ key: "m", label: "Mês" }, { key: "v", label: "mm", num: true }], clim.map((v, i) => ({ m: MESES_LONGOS[i], v: fmt1(v) }))) });
      columnChart(el2, { labels: MESES_LONGOS.map((m) => m.slice(0, 3)), series: [{ name: "Chuva", color: RAIN_RAMP[5], values: clim.map((v) => v ?? 0) }], unit: "mm", fmt: fmt1, height: 200, tipTitle: (i) => MESES_LONGOS[i], ariaLabel: "Chuva média por mês do ano" });
    } else root.querySelector("#h-rain").innerHTML = bad;

    // ---------------- nível
    const hl = root.querySelector("#h-lvl");
    const badL = resultState(nivel, "Nível hidrológico");
    if (badL || st.est.status !== "ok") { hl.innerHTML = badL || ""; return; }
    const sens = filterStations(st.est.data.nivel, f);
    if (!sens.length) { hl.innerHTML = card({ title: "Nível médio diário", body: stateBlock("empty", "Nenhum sensor de nível no recorte") }); return; }
    if (!levelSel || !sens.find((s) => s.id === levelSel)) levelSel = sens[0].id;
    const drawL = () => {
      const s = sens.find((x) => x.id === levelSel);
      const dv = nivel.data.diario[s.id];
      const days = dv.map((_, i) => addDays(nivel.data.diario_inicio, i));
      hl.innerHTML = card({
        title: "Nível médio diário", sub: `Últimos 12 meses (cobertura publicada para a web) · metros, unidade da fonte`,
        actions: `<div class="field" style="min-width:260px"><label for="lvl-sel">Sensor</label><select class="select" id="lvl-sel">${sens.map((x) => `<option value="${esc(x.id)}" ${x.id === levelSel ? "selected" : ""}>${esc(x.curso_dagua)} · ${esc(x.codigo)}</option>`).join("")}</select></div>`,
        body: '<div id="b7"></div>', foot: provenance(nivel.meta),
      });
      const el = chartBlock(hl.querySelector("#b7"), { tableHtml: table([{ key: "d", label: "Dia" }, { key: "v", label: "m", num: true }], days.map((d, i) => ({ d: fmtDay(d), v: dv[i] === null ? "sem leitura" : fmt2(dv[i]) })).reverse()) });
      lineChart(el, { labels: days.map((d) => `${d.slice(8, 10)}/${d.slice(5, 7)}`), series: [{ name: s.curso_dagua, color: "#0F8A73", values: dv }], unit: "m", fmt: fmt2, minZero: false, height: 220, tipTitle: (i) => fmtDay(days[i]), ariaLabel: "Nível médio diário" });
      hl.querySelector("#lvl-sel").addEventListener("change", (e) => { levelSel = e.target.value; drawL(); hl.querySelector("#lvl-sel").focus(); });
    };
    drawL();
  },
};
