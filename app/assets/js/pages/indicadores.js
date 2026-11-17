// Indicadores (F06): indicadores da cidade no recorte ativo e atalhos.
import { icon } from "../icons.js";
import { esc, fmtInt, fmt1, fmtPct, fmtDateTime, groupCount, mean, fmtMonth } from "../util.js";
import { store, resolvePeriod, previousPeriod } from "../store.js";
import { getOccurrences, filterOccurrences, getStations, filterStations, rainInPeriod, rainLastHours, load, getManifest, monthsBetween } from "../data.js";
import { kpiCard, filterBar, bindFilterBar, freshnessBadge, natureBadge, subprefNome, stateBlock, resultState, provenance, card, table, badge } from "../ui.js";
import { columnChart, barList, chartBlock, legendKeys, TYPE_COLORS } from "../charts.js";
import { ROUTE_META } from "../routes.js";

export default {
  subtitle: "Indicadores do município no recorte selecionado",
  async render(root) {
    const perfil = store.perfil;
    root.innerHTML = `<div class="page">
      ${filterBar({ id: "fb" })}
      <div class="grid grid--kpi" id="kpis">${Array.from({ length: 5 }, () => '<div class="card kpi"><div class="skeleton" style="height:100%"></div></div>').join("")}</div>
      <div class="grid grid--12" style="margin-top:16px">
        <div class="span-8" id="c-mes"></div>
        <div class="span-4" id="c-sp"></div>
      </div>
      <h3 class="section-title">Atalhos para ${esc(perfil.nome.toLowerCase())}</h3>
      <div class="grid grid--kpi" id="atalhos">${perfil.atalhos.map(shortcut).join("")}</div>
      <div class="grid grid--12" style="margin-top:16px">
        <div class="span-7" id="c-status"></div>
        <div class="span-5" id="c-avisos"></div>
      </div>
    </div>`;
    bindFilterBar(root);

    const f = store.filters;
    const per = resolvePeriod();
    const [occR, st, susc, avisos] = await Promise.all([getOccurrences(), getStations(), load("suscetibilidade"), load("avisos_oficiais")]);

    // ---------- indicadores
    const kpis = [];
    if (occR.status === "ok") {
      const occ = filterOccurrences(occR.rows, f, per);
      const prev = filterOccurrences(occR.rows, f, previousPeriod(per));
      const delta = prev.length ? (occ.length - prev.length) / prev.length : null;
      const fimOc = occR.meta.periodo.fim;
      kpis.push(kpiCard({
        label: "Ocorrências registradas", ic: "pin", value: fmtInt(occ.length),
        ctx: `${esc(per.texto)} · registros históricos do GeoSampa${delta !== null ? `<br>${delta >= 0 ? "+" : ""}${fmtPct(delta, 0)} vs. período anterior de mesma duração (${fmtInt(prev.length)})` : ""}${per.fim > fimOc ? `<br>Registros publicados até ${esc(fimOc.split("-").reverse().join("/"))}` : ""}`,
      }));
      const cruzadas = occ.filter((o) => Number.isInteger(o.grau));
      if (susc.status === "ok" && cruzadas.length) {
        const altas = cruzadas.filter((o) => o.grau === 3).length;
        const areaAlta = susc.meta.area_uniao_km2["3"] / susc.meta.area_municipio_km2;
        kpis.push(kpiCard({
          label: "Em área de suscetibilidade alta", ic: "terrain", value: fmtPct(altas / cruzadas.length, 0),
          ctx: `${fmtInt(altas)} de ${fmtInt(cruzadas.length)} ocorrências cruzadas dentro de polígonos de inundação classe Alta${areaAlta !== null ? `, que cobrem ${fmtPct(areaAlta, 1)} do município` : ""}. Associação espacial, não causalidade.`,
        }));
      }
    } else kpis.push(`<div class="card kpi">${resultState(occR, "Ocorrências")}</div>`);

    if (st.est.status === "ok" && st.diaria.status === "ok") {
      const est = filterStations(st.est.data.chuva, f);
      const somas = est.map((s) => rainInPeriod(st.diaria, s.id, per.inicio, per.fim)).filter((r) => r.mm !== null && r.cobertura >= 0.8);
      kpis.push(kpiCard({
        label: "Chuva acumulada média", ic: "drop", value: fmt1(mean(somas.map((r) => r.mm))), unit: "mm",
        ctx: `${esc(per.texto)} · média de ${fmtInt(somas.length)} estações com ≥ 80% de dias com leitura`,
      }));
      if (st.horaria.status === "ok") {
        const r24 = est.map((s) => ({ s, ...rainLastHours(st.horaria, s.id, 24) })).filter((r) => r.mm !== null).sort((a, b) => b.mm - a.mm);
        const top = r24[0];
        kpis.push(kpiCard({
          label: "Maior acumulado em 24 h", ic: "activity", value: top ? fmt1(top.mm) : "—", unit: "mm",
          ctx: top ? `${esc(top.s.nome)} (${esc(top.s.rede)})<br>Até ${esc(fmtDateTime(st.horaria.meta.referencia))}` : "Sem leituras nas últimas 24 h",
          foot: `<div style="margin-top:4px">${freshnessBadge(st.horaria.meta, "chuva_horaria")}</div>`,
        }));
        const comLeitura = est.filter((s) => rainLastHours(st.horaria, s.id, 1).mm !== null).length;
        kpis.push(kpiCard({
          label: "Estações com leitura na última hora", ic: "wave", value: `${fmtInt(comLeitura)}<span class="kpi__unit">de ${fmtInt(est.length)}</span>`,
          ctx: f.rede ? `Rede ${esc(f.rede)}` : "CEMADEN, SIBH e INMET · pluviômetros",
        }));
      }
    } else kpis.push(`<div class="card kpi">${resultState(st.est.status !== "ok" ? st.est : st.diaria, "Chuva")}</div>`);
    root.querySelector("#kpis").innerHTML = kpis.join("");

    // ---------- ocorrências por mês e por subprefeitura
    if (occR.status === "ok") {
      const occ = filterOccurrences(occR.rows, f, per);
      const meses = monthsBetween(per.inicio, per.fim);
      const tipos = Object.keys(TYPE_COLORS).filter((t) => !f.tipos.length || f.tipos.includes(t));
      const series = tipos.map((t) => {
        const c = groupCount(occ.filter((o) => o.tipo === t).map((o) => o.data.slice(0, 7)));
        return { name: t, color: TYPE_COLORS[t], values: meses.map((m) => c.get(m) || 0) };
      });
      const host = root.querySelector("#c-mes");
      host.innerHTML = card({
        title: "Ocorrências por mês", sub: `${esc(per.texto)} · contagem de registros por tipo`,
        body: '<div id="mes-body"></div>', foot: provenance(occR.meta, { periodo: per.texto }),
        actions: `<div>${natureBadge(occR.meta)}</div>`,
      });
      const el = chartBlock(host.querySelector("#mes-body"), {
        legend: series.length > 1 ? legendKeys(series) : "",
        tableHtml: table([{ key: "m", label: "Mês" }, ...series.map((s) => ({ label: s.name, num: true, fmt: (r) => fmtInt(r[s.name]) }))],
          meses.map((m, i) => Object.fromEntries([["m", fmtMonth(m)], ...series.map((s) => [s.name, s.values[i]])]))),
      });
      if (occ.length) columnChart(el, { labels: meses.map(fmtMonth), series, unit: "ocorrências", ariaLabel: "Ocorrências por mês" });
      else el.innerHTML = stateBlock("empty", "Nenhuma ocorrência no recorte", "Amplie o período ou remova filtros.");

      const porSp = [...groupCount(occ.map((o) => o.subprefeitura))].sort((a, b) => b[1] - a[1]).slice(0, 8);
      const h2 = root.querySelector("#c-sp");
      h2.innerHTML = card({
        title: "Subprefeituras com mais registros", sub: "Top 8 no recorte",
        body: '<div id="sp-body"></div>', foot: `<a href="#/analises">Ver recorrência e suscetibilidade ${icon("arrowRight")}</a>`,
      });
      const rows = porSp.map(([sp, n]) => ({ label: subprefNome(sp), value: n, sub: fmtPct(n / occ.length, 0) + " do recorte" }));
      const el2 = chartBlock(h2.querySelector("#sp-body"), { tableHtml: table([{ key: "label", label: "Subprefeitura" }, { key: "value", label: "Ocorrências", num: true, fmt: (r) => fmtInt(r.value) }], rows) });
      if (rows.length) barList(el2, { rows, ariaLabel: "Ranking de subprefeituras" });
      else el2.innerHTML = stateBlock("empty", "Sem ocorrências no recorte");
    }

    // ---------- estado dos dados
    const m = getManifest();
    const sts = await Promise.all(m.datasets.map((d) => load(d.id)));
    root.querySelector("#c-status").innerHTML = card({
      title: "Estado dos dados publicados", sub: "Fonte, natureza e atualização de cada conjunto",
      body: table(
        [
          { label: "Conjunto", fmt: (r) => `<a href="#/dados?ds=${esc(r.id)}">${esc(r.titulo)}</a>` },
          { label: "Natureza", fmt: (r) => natureBadge(r.meta || r) },
          { label: "Atualização", fmt: (r) => (r.res.status === "ok" || r.res.status === "vazio" ? freshnessBadge(r.res.meta, r.id) : badge("error", "Falha ao carregar", "alert")) },
        ],
        m.datasets.filter((d) => d.id !== "avisos_oficiais").map((d, i) => ({ ...d, res: sts[i], meta: sts[i].meta })),
      ) + `<p class="note" style="margin-top:8px">Não publicados nesta versão: ${m.nao_publicados.map((n) => esc(n.titulo)).join(", ")}.</p>`,
    });

    root.querySelector("#c-avisos").innerHTML = card({
      title: "Avisos oficiais", sub: "Defesa Civil, CEMADEN, INMET",
      body: avisos.status === "nao_publicado"
        ? stateBlock("unavailable", "Nenhuma fonte de avisos oficiais integrada", "Esta área exibirá avisos com origem e validade quando a integração existir. Os indicadores desta plataforma não são avisos nem alertas. Em emergência, ligue 199 (Defesa Civil).")
        : avisos.status === "erro" ? resultState(avisos, "Avisos oficiais")
        : avisos.meta?.estado_integracao === "nao_integrado"
          ? stateBlock("unavailable", "Nenhuma fonte de avisos oficiais integrada", "Esta área exibirá avisos com origem e validade quando a integração existir. Os indicadores desta plataforma não são avisos nem alertas. Em emergência, ligue 199 (Defesa Civil).")
          : stateBlock("empty", "Nenhum aviso oficial vigente"),
    });
  },
};

function shortcut(k) {
  const r = ROUTE_META[k];
  const d = r.desc;
  return `<a class="card shortcut" href="#/${k}">${icon(r.icon)}<span><strong>${esc(r.label)}</strong><small>${esc(d)}</small></span>${icon("arrowRight")}</a>`;
}
