// Análises (F12 + insights 8.3 + gate de Data Science, seção 11).
import { icon } from "../icons.js";
import { esc, fmtInt, fmt1, fmt2, fmtPct, groupCount } from "../util.js";
import { store, resolvePeriod } from "../store.js";
import { getOccurrences, filterOccurrences, getStations, filterStations, load } from "../data.js";
import { filterBar, bindFilterBar, subprefNome, stateBlock, resultState, natureBadge, provenance, card, table, badge } from "../ui.js";
import { barList, columnChart, chartBlock } from "../charts.js";
import { computeInsights, insightCard, recurrenceCells } from "../insights.js";

const GATE = [
  ["Evento-alvo e unidade geográfica definidos", "pendente", "Não há definição formal de evento-alvo nem de unidade de previsão."],
  ["Cobertura, vieses e resolução documentados", "parcial", "As ocorrências do GeoSampa têm data, mas não hora; a chuva é horária. A sobreposição temporal fina não é possível."],
  ["Baseline e validação temporal", "pendente", "Nenhum modelo foi treinado ou comparado a baseline."],
  ["Métricas para falsos positivos e negativos", "pendente", "Custos de erro ainda não discutidos com as equipes."],
  ["Desempenho por área e período", "pendente", "Sem resultados de modelo."],
  ["Linguagem e visualização coerentes com a evidência", "atendido", "Painéis exibem apenas análise histórica e suscetibilidade, rotuladas como tal."],
];

export default {
  subtitle: "Recorrência, suscetibilidade territorial e padrões descritivos",
  async render(root) {
    const f = store.filters;
    const per = resolvePeriod();
    root.innerHTML = `<div class="page">
      ${filterBar({ id: "fb" })}
      <div id="gate"></div>
      <div class="grid grid--12" style="margin-top:16px">
        <div class="span-12" id="a-susc"></div>
        <div class="span-6" id="a-susc-chart"></div>
        <div class="span-6" id="a-rain"></div>
        <div class="span-12" id="a-rec"></div>
        <div class="span-12" id="a-ins"></div>
      </div>
    </div>`;
    bindFilterBar(root);
    root.querySelector("#gate").innerHTML = gateCard();

    const [occR, st, susc] = await Promise.all([getOccurrences(), getStations(), load("suscetibilidade")]);
    const bad = resultState(occR, "Ocorrências");
    if (bad) { root.querySelector("#a-susc").innerHTML = bad; return; }
    const occ = filterOccurrences(occR.rows, f, per);
    const occHist = filterOccurrences(occR.rows, { ...f, periodo: "todo" });

    // ---------------- suscetibilidade × ocorrências
    const sb = resultState(susc, "Suscetibilidade");
    if (sb) {
      root.querySelector("#a-susc").innerHTML = sb;
      // sem suscetibilidade, o card de chuva ocupa a linha inteira (não deixa um vazio ao lado)
      root.querySelector("#a-susc-chart").remove();
      root.querySelector("#a-rain").className = "span-12";
    } else {
      const A = susc.meta.area_municipio_km2;
      const u = susc.meta.area_uniao_km2;
      const cnt = groupCount(occ.map((o) => o.grau || 0));
      const fora = A - u["1"] - u["2"] - u["3"];
      const rows = [3, 2, 1, 0].map((g) => {
        const area = g ? u[String(g)] : fora;
        const n = cnt.get(g) || 0;
        return { g, classe: g ? ["", "Baixa", "Média", "Alta"][g] : "Fora das áreas mapeadas", area, pa: area / A, n, pn: occ.length ? n / occ.length : 0, dens: n / area };
      });
      root.querySelector("#a-susc").innerHTML = card({
        title: "Ocorrências por classe de suscetibilidade a inundação",
        sub: `${esc(per.texto)} · ${fmtInt(occ.length)} ocorrências · ${f.subprefeitura ? esc(subprefNome(f.subprefeitura)) : "município inteiro"}`,
        actions: `<div class="pill-list">${natureBadge(susc.meta)} ${badge("neutral", "Não é previsão", "info")}</div>`,
        body: table(
          [
            { label: "Classe", fmt: (r) => `<span class="row" style="gap:8px;flex-wrap:nowrap"><span class="legend__sw" style="background:${["#F1F5F9", "#E4DEFB", "#B4A7EF", "#6250D6"][r.g]}"></span>${esc(r.classe)}</span>` },
            { label: "Área (km²)", num: true, fmt: (r) => fmt1(r.area) },
            { label: "% do município", num: true, fmt: (r) => fmtPct(r.pa, 1) },
            { label: "Ocorrências", num: true, fmt: (r) => fmtInt(r.n) },
            { label: "% das ocorrências", num: true, fmt: (r) => fmtPct(r.pn, 1) },
            { label: "Por km²", num: true, fmt: (r) => fmt2(r.dens) },
          ],
          rows,
          { caption: "Cada ocorrência recebe o maior grau entre os polígonos que a contêm. Áreas por classe = união dos polígonos (sem dupla contagem). Área do município: 1.521 km² (IBGE)." },
        ) + (f.subprefeitura ? `<p class="note" style="margin-top:8px">${icon("info")} Com subprefeitura filtrada, as ocorrências são da subprefeitura, mas as áreas continuam sendo do município (a Gold não tem o limite das subprefeituras).</p>` : ""),
        foot: `${provenance(susc.meta)}<div style="margin-top:6px">Leitura: associação espacial entre registros e predisposição do terreno. Não indica causa nem probabilidade de alagamento.</div>`,
      });
      const h = root.querySelector("#a-susc-chart");
      h.innerHTML = card({ title: "Densidade de ocorrências", sub: "ocorrências por km² em cada classe", body: '<div id="c1"></div>' });
      const el = chartBlock(h.querySelector("#c1"), { tableHtml: table([{ key: "classe", label: "Classe" }, { key: "d", label: "Por km²", num: true }], rows.map((r) => ({ classe: r.classe, d: fmt2(r.dens) }))) });
      barList(el, { rows: rows.map((r) => ({ label: r.classe, value: r.dens, color: ["#94A3B8", "#B4A7EF", "#7B6AE0", "#6250D6"][r.g] })), fmt: fmt2, unit: "/km²", ariaLabel: "Densidade por classe" });
    }

    // ---------------- recorrência
    const cells = recurrenceCells(occHist).slice(0, 10);
    const anosHist = new Set(occHist.map((o) => o.data.slice(0, 4))).size;
    root.querySelector("#a-rec").innerHTML = card({
      title: "Pontos de maior recorrência",
      sub: `Células de ~500 m com registros no maior número de anos (de ${anosHist}) · todo o histórico, com filtros de tipo e território`,
      actions: natureBadge(occR.meta),
      body: table(
        [
          { label: "#", fmt: (r) => r.i },
          { label: "Subprefeitura", fmt: (r) => esc(subprefNome(r.subprefeitura)) },
          { label: "Centro aproximado", fmt: (r) => `<span class="mono">${r.lat.toFixed(4)}, ${r.lon.toFixed(4)}</span>` },
          { label: "Anos com registro", num: true, fmt: (r) => `${r.nAnos} de ${anosHist}` },
          { label: "Registros", num: true, fmt: (r) => fmtInt(r.n) },
          { label: "Tipo predominante", fmt: (r) => esc([...r.tipos].sort((a, b) => b[1] - a[1])[0][0]) },
          { label: "", fmt: (r) => `<a class="btn btn--ghost btn--sm" href="#/mapa?center=${r.lon},${r.lat}&zoom=15">${icon("map")}Mapa</a>` },
        ],
        cells.map((c, i) => ({ ...c, i: i + 1 })),
        { caption: "Recorrência = número de anos distintos com pelo menos um registro na célula. Registros dependem de chamados e vistorias.", empty: "Sem registros no recorte." },
      ),
      foot: provenance(occR.meta, { periodo: "Todo o histórico publicado" }),
    });

    // ---------------- insights e chuva × ocorrências
    const stations = st.est.status === "ok" ? filterStations(st.est.data.chuva, f) : [];
    const ins = computeInsights({ occ, occHist, per, f, susc, stations, diaria: st.diaria, occMeta: occR.meta });
    const ch = ins.find((i) => i.id === "chuva");
    const hr = root.querySelector("#a-rain");
    if (ch) {
      hr.innerHTML = card({ title: "Ocorrências por dia, por faixa de chuva", sub: `${esc(per.texto)} · média de registros por dia, conforme a maior chuva diária entre as estações`, body: '<div id="c2"></div>', foot: esc(ch.limitacoes) });
      const t = ch.tabela;
      const el = chartBlock(hr.querySelector("#c2"), { tableHtml: table([{ key: "nome", label: "Maior chuva do dia" }, { key: "dias", label: "Dias", num: true, fmt: (r) => fmtInt(r.dias) }, { key: "oc", label: "Ocorrências", num: true, fmt: (r) => fmtInt(r.oc) }, { label: "Por dia", num: true, fmt: (r) => (r.dias ? fmt1(r.oc / r.dias) : "—") }], t) });
      columnChart(el, { labels: t.map((x) => x.nome), series: [{ name: "Ocorrências por dia", color: "#2A78D6", values: t.map((x) => (x.dias ? x.oc / x.dias : 0)) }], unit: "por dia", fmt: fmt1, height: 220, tipTitle: (i) => `${t[i].nome} · ${fmtInt(t[i].dias)} dias`, ariaLabel: "Ocorrências por faixa de chuva" });
    } else hr.innerHTML = card({ title: "Ocorrências por faixa de chuva", body: stateBlock("empty", "Dados insuficientes no recorte", "São necessários ao menos 10 registros e dias com chuva acima de 10 mm.") });

    const hi = root.querySelector("#a-ins");
    hi.innerHTML = card({
      title: "Insights do recorte", sub: "Padrões descritivos com critério, fonte e limitação",
      body: ins.length ? `<div class="insight-grid">${ins.filter((i) => i.id !== "chuva").map((i) => insightCard(i)).join("")}</div>` : stateBlock("empty", "Poucos registros para descrever padrões"),
    });
    hi.querySelectorAll("[data-acao]").forEach((b) => b.addEventListener("click", () => {
      const a = ins.find((i) => i.id === b.dataset.acao).acao;
      if (a.filtro) return store.setFilters(a.filtro);
      if (a.camada) return (location.hash = `#/mapa?camada=${a.camada}`);
      if (a.center) return (location.hash = `#/mapa?center=${a.center.join(",")}&zoom=${a.zoom || 14}${a.estacao ? `&estacao=${encodeURIComponent(a.estacao)}` : ""}`);
      if (a.rota) location.hash = a.rota;
    }));
  },
};

function gateCard() {
  const b = { atendido: badge("ok", "Atendido", "check"), parcial: badge("stale", "Parcial", "clock"), pendente: badge("neutral", "Pendente", "info") };
  return `<section class="card gate">
    <div class="card__body">
      <div class="gate__head">
        <div>${icon("flask")}</div>
        <div><h3>Gate de Data Science: não aprovado</h3>
        <p>Enquanto os critérios da seção 11 do PDR não forem atendidos, a plataforma mostra análises históricas e, quando publicada, suscetibilidade territorial. Nenhuma classe ou score de risco, previsão ou alerta é exibido.</p></div>
      </div>
      <details><summary>Ver os 6 critérios e a situação de cada um</summary>
        <ul class="gate__list">${GATE.map(([t, s, d]) => `<li>${b[s]}<span><strong>${esc(t)}</strong><small>${esc(d)}</small></span></li>`).join("")}</ul>
      </details>
    </div>
  </section>`;
}
