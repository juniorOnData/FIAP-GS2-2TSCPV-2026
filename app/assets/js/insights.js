// Insights descritivos (PDR 8.3). Cada insight traz padrão, recorte, método,
// fontes, limitações, nível de evidência e uma ação de exploração.
// Nenhum insight é previsão ou recomendação de conduta.
import { esc, fmtInt, fmt1, fmtPct, groupCount, diffDays, addDays } from "./util.js";
import { subprefNome } from "./ui.js";
import { icon } from "./icons.js";
import { cityDaily, rainInPeriod } from "./data.js";

const CELL = 0.005; // ~500 m

export function recurrenceCells(rows) {
  const cells = new Map();
  for (const o of rows) {
    const k = `${Math.floor(o.lon / CELL)}:${Math.floor(o.lat / CELL)}`;
    let c = cells.get(k);
    if (!c) cells.set(k, (c = { k, n: 0, anos: new Set(), lat: 0, lon: 0, sp: groupCount([]), tipos: groupCount([]) }));
    c.n++;
    c.anos.add(o.data.slice(0, 4));
    c.lat += o.lat;
    c.lon += o.lon;
    c.sp.set(o.subprefeitura, (c.sp.get(o.subprefeitura) || 0) + 1);
    c.tipos.set(o.tipo, (c.tipos.get(o.tipo) || 0) + 1);
  }
  return [...cells.values()]
    .map((c) => ({ ...c, lat: c.lat / c.n, lon: c.lon / c.n, nAnos: c.anos.size, subprefeitura: [...c.sp].sort((a, b) => b[1] - a[1])[0][0] }))
    .sort((a, b) => b.nAnos - a.nAnos || b.n - a.n);
}

export function computeInsights({ occ, occHist, per, f, susc, stations, diaria, occMeta }) {
  const out = [];
  const fontesOc = `GeoSampa (ocorrências), publicado ${occMeta ? new Date(occMeta.publicado_em).toLocaleDateString("pt-BR") : "—"}`;
  const recorte = f.subprefeitura ? subprefNome(f.subprefeitura) : "Município de São Paulo";

  if (occ.length >= 10) {
    // 1. recorrência espacial (anos distintos com registro, em todo o histórico com os mesmos filtros de tipo/território)
    const cells = recurrenceCells(occHist);
    const top = cells[0];
    if (top && top.nAnos >= 2) {
      const anosHist = new Set(occHist.map((o) => o.data.slice(0, 4))).size;
      out.push({
        id: "recorrencia",
        titulo: "Ponto com registros recorrentes",
        descricao: `Uma célula de cerca de 500 m em ${esc(subprefNome(top.subprefeitura))} teve ocorrências em ${top.nAnos} dos ${anosHist} anos do histórico (${fmtInt(top.n)} registros).`,
        area: `${esc(subprefNome(top.subprefeitura))}, perto de ${top.lat.toFixed(4)}, ${top.lon.toFixed(4)}`,
        periodo: "Todo o histórico publicado (o filtro de período não se aplica à recorrência)",
        metodo: "Grade regular de 0,005° (~500 m). Conta anos distintos com pelo menos um registro e o total de registros por célula.",
        fontes: fontesOc,
        limitacoes: "Registros dependem de chamados e vistorias; ausência de registro não significa ausência de evento. A grade não segue limites de bairro.",
        evidencia: "Descritiva (contagem de registros)",
        acao: { label: "Ver no mapa", center: [top.lon, top.lat], zoom: 15 },
      });
    }

    // 2. concentração territorial
    if (!f.subprefeitura) {
      const porSp = [...groupCount(occ.map((o) => o.subprefeitura))].sort((a, b) => b[1] - a[1]);
      const [sp, n] = porSp[0];
      const top3 = porSp.slice(0, 3).reduce((a, [, v]) => a + v, 0);
      out.push({
        id: "concentracao",
        titulo: "Concentração por subprefeitura",
        descricao: `${esc(subprefNome(sp))} reúne ${fmtPct(n / occ.length, 0)} das ocorrências do recorte; as três primeiras somam ${fmtPct(top3 / occ.length, 0)}.`,
        area: recorte,
        periodo: per.texto,
        metodo: "Contagem de ocorrências por subprefeitura informada no registro (32 subprefeituras).",
        fontes: fontesOc,
        limitacoes: "As subprefeituras têm áreas e populações diferentes; a contagem bruta não é taxa.",
        evidencia: "Descritiva (contagem de registros)",
        acao: { label: "Filtrar esta subprefeitura", filtro: { subprefeitura: sp } },
      });
    }

    // 3. sobreposição com suscetibilidade
    if (susc?.status === "ok") {
      const alta = occ.filter((o) => o.grau === 3).length;
      const qualquer = occ.filter((o) => o.grau).length;
      const areaAlta = susc.meta.area_uniao_km2["3"] / susc.meta.area_municipio_km2;
      const lift = alta / occ.length / areaAlta;
      out.push({
        id: "suscetibilidade",
        titulo: "Ocorrências em áreas suscetíveis a inundação",
        descricao: `${fmtPct(alta / occ.length, 0)} das ocorrências caem em áreas de suscetibilidade Alta, que cobrem ${fmtPct(areaAlta, 1)} do município (${fmt1(lift)}× a proporção da área). Em qualquer classe: ${fmtPct(qualquer / occ.length, 0)}.`,
        area: recorte,
        periodo: per.texto,
        metodo: "Ponto em polígono: cada ocorrência recebe o maior grau entre as áreas de inundação que a contêm. A área por classe é a união dos polígonos.",
        fontes: `${fontesOc}; SGB/CPRM (carta de 2015)`,
        limitacoes: "Associação espacial, não relação causal. A carta é de 2015 (escala 1:75.000) e não considera obras de drenagem posteriores. Suscetibilidade não é previsão.",
        evidencia: "Descritiva (sobreposição espacial)",
        acao: { label: "Ligar a camada de suscetibilidade", camada: "suscetibilidade" },
      });
    }

    // 3b. proximidade da rede hídrica (OSM): tipos ligados à água comparados com quedas de árvore
    if (occMeta?.dist_agua_max_m) {
      const perto = (lista) => lista.filter((o) => o.dist !== null && o.dist <= 100).length / lista.length;
      const agua = occ.filter((o) => o.tipo === "Alagamento" || o.tipo === "Inundação");
      const arvores = occ.filter((o) => o.tipo === "Queda de Árvore");
      if (agua.length >= 20 && arvores.length >= 20) {
        out.push({
          id: "hidrografia",
          titulo: "Proximidade da rede hídrica",
          descricao: `${fmtPct(perto(agua), 0)} dos alagamentos e inundações do recorte estão a até 100 m de um curso ou corpo d'água mapeado, contra ${fmtPct(perto(arvores), 0)} das quedas de árvore.`,
          area: recorte,
          periodo: per.texto,
          metodo: "Distância de cada ocorrência ao curso ou corpo d'água do OpenStreetMap mais próximo; as quedas de árvore servem de comparação, por não dependerem da água.",
          fontes: `${fontesOc}; OpenStreetMap (hidrografia)`,
          limitacoes: "Muitos córregos de São Paulo são canalizados ou correm sob avenidas, e o OpenStreetMap os mapeia de forma desigual. Associação espacial, não relação causal.",
          evidencia: "Descritiva (proximidade espacial)",
          acao: { label: "Ligar a camada de hidrografia", camada: "hidrografia" },
        });
      }
    }
  }

  // 4. chuva × ocorrências por faixa de chuva diária
  if (diaria?.status === "ok" && occ.length >= 10) {
    const city = cityDaily(diaria);
    const porDia = groupCount(occ.map((o) => o.data));
    const faixas = [
      { nome: "até 10 mm", min: 0, max: 10 },
      { nome: "10 a 30 mm", min: 10, max: 30 },
      { nome: "30 a 50 mm", min: 30, max: 50 },
      { nome: "50 mm ou mais", min: 50, max: Infinity },
    ].map((fx) => ({ ...fx, dias: 0, oc: 0 }));
    const fim = occMeta?.periodo?.fim && occMeta.periodo.fim < per.fim ? occMeta.periodo.fim : per.fim;
    for (let d = per.inicio; d <= fim; d = addDays(d, 1)) {
      const v = city.max[diffDays(city.inicio, d)];
      if (v === null || v === undefined) continue;
      const fx = faixas.find((x) => v >= x.min && v < x.max);
      fx.dias++;
      fx.oc += porDia.get(d) || 0;
    }
    const lo = faixas[0], hi = [...faixas].reverse().find((x) => x.dias >= 3);
    if (hi && hi !== lo && lo.dias) {
      out.push({
        id: "chuva",
        titulo: "Mais registros em dias de chuva forte",
        descricao: `Em dias em que alguma estação passou de ${hi.nome.replace(" ou mais", "")}, houve em média ${fmt1(hi.oc / hi.dias)} ocorrências por dia, contra ${fmt1(lo.oc / lo.dias)} nos dias com até 10 mm (${fmtInt(hi.dias)} e ${fmtInt(lo.dias)} dias).`,
        area: "Município (maior valor diário entre as estações)",
        periodo: per.texto,
        metodo: "Para cada dia, maior chuva diária entre as estações; ocorrências do dia agrupadas pela faixa de chuva.",
        fontes: `${fontesOc}; CEMADEN, SIBH e INMET (chuva diária)`,
        limitacoes: "As ocorrências do GeoSampa têm data, mas não hora; a chuva máxima pode ter ocorrido longe dos registros. Não é modelo preditivo.",
        evidencia: "Descritiva (associação temporal)",
        acao: { label: "Abrir o histórico", rota: "#/historico" },
        tabela: faixas,
      });
    }
  }

  // 5. estação com maior acumulado no período
  if (diaria?.status === "ok" && stations?.length) {
    const somas = stations.map((s) => ({ s, ...rainInPeriod(diaria, s.id, per.inicio, per.fim) })).filter((r) => r.mm !== null && r.cobertura >= 0.8).sort((a, b) => b.mm - a.mm);
    if (somas.length >= 3) {
      const t = somas[0];
      const med = somas[Math.floor(somas.length / 2)].mm;
      out.push({
        id: "estacao",
        titulo: "Maior chuva acumulada no período",
        descricao: `${esc(t.s.nome)} (${esc(t.s.rede)}) acumulou ${fmt1(t.mm)} mm, ${fmt1(t.mm / (med || 1))}× a mediana das ${fmtInt(somas.length)} estações com dados suficientes (${fmt1(med)} mm).`,
        area: recorte,
        periodo: per.texto,
        metodo: "Soma da chuva diária por estação; entram só estações com leitura em pelo menos 80% dos dias.",
        fontes: "CEMADEN, SIBH e INMET",
        limitacoes: "Pluviômetros medem um ponto; falhas de leitura reduzem o acumulado. Redes diferentes têm equipamentos e manutenção diferentes.",
        evidencia: "Observação direta (medição)",
        acao: { label: "Ver estação", center: [t.s.lon, t.s.lat], zoom: 13, estacao: t.s.id },
      });
    }
  }
  return out;
}

export function insightCard(ins, { compact = false } = {}) {
  return `<article class="insight" data-insight="${esc(ins.id)}">
    <div class="insight__head"><h4>${esc(ins.titulo)}</h4><span class="badge badge--neutral">${esc(ins.evidencia)}</span></div>
    <p class="insight__desc">${ins.descricao}</p>
    <details ${compact ? "" : "open"}>
      <summary>Critérios, fontes e limitações</summary>
      <dl class="dl">
        <dt>Área</dt><dd>${ins.area}</dd>
        <dt>Período</dt><dd>${esc(ins.periodo)}</dd>
        <dt>Método</dt><dd>${esc(ins.metodo)}</dd>
        <dt>Fontes</dt><dd>${esc(ins.fontes)}</dd>
        <dt>Limitações</dt><dd>${esc(ins.limitacoes)}</dd>
      </dl>
    </details>
    ${ins.acao ? `<button type="button" class="btn btn--secondary btn--sm" data-acao="${esc(ins.id)}">${icon("arrowRight")}${esc(ins.acao.label)}</button>` : ""}
  </article>`;
}
