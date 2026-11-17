// Sobre os dados (F09) e qualidade/linhagem (F11).
import { icon } from "../icons.js";
import { esc, fmtInt, fmtDateTime, fmtDay } from "../util.js";
import { getManifest, isDemo, load } from "../data.js";
import { natureBadge, freshnessBadge, badge, stateBlock, table, card, pageHead } from "../ui.js";
import { FONTES, METODOLOGIA } from "../content.js";

const TABS = [["fontes", "Fontes"], ["conjuntos", "Conjuntos publicados"], ["linhagem", "Qualidade e linhagem"], ["metodologia", "Metodologia"], ["licencas", "Licenças e limites"]];
let tab = "fontes";

export default {
  subtitle: "Fontes, metodologia, cobertura, qualidade e licenças",
  async render(root, { params }) {
    if (params.get("ds")) tab = "conjuntos";
    if (params.get("tab")) tab = params.get("tab");
    const m = getManifest();
    root.innerHTML = `<div class="page">
      ${pageHead({ eyebrow: "Transparência", title: "Sobre os dados", lead: "De onde vem cada informação, como foi processada, quando foi publicada e o que ela não diz." })}
      ${isDemo() ? stateBlock("demo", "Esta publicação usa o pacote de demonstração", `Todos os valores exibidos são sintéticos, gerados por <code>${esc(m.gerador)}</code> (semente ${esc(m.semente)}), e mantidos em pasta separada dos exports reais (<code>data/demo/</code>). Os arquivos seguem o mesmo contrato dos exports da Gold, mas não representam medições, ocorrências ou áreas reais.`) : ""}
      <div class="tabs" role="tablist" aria-label="Seções" style="margin-top:16px">${TABS.map(([k, t]) => `<button type="button" role="tab" id="tab-${k}" aria-selected="${k === tab}" aria-controls="panel" data-tab="${k}" tabindex="${k === tab ? 0 : -1}">${t}</button>`).join("")}</div>
      <div id="panel" role="tabpanel" aria-labelledby="tab-${tab}"></div>
    </div>`;
    const tablist = root.querySelector(".tabs");
    tablist.addEventListener("click", (e) => {
      const b = e.target.closest("[data-tab]");
      if (!b) return;
      tab = b.dataset.tab;
      this.render(root, { params: new URLSearchParams() }).then(() => root.querySelector(`#tab-${tab}`).focus());
    });
    tablist.addEventListener("keydown", (e) => {
      if (!["ArrowRight", "ArrowLeft"].includes(e.key)) return;
      const i = TABS.findIndex(([k]) => k === tab);
      tab = TABS[(i + (e.key === "ArrowRight" ? 1 : TABS.length - 1)) % TABS.length][0];
      this.render(root, { params: new URLSearchParams() }).then(() => root.querySelector(`#tab-${tab}`).focus());
    });
    const panel = root.querySelector("#panel");
    if (tab === "fontes") panel.innerHTML = fontes(m);
    if (tab === "metodologia") panel.innerHTML = `<div class="grid grid--2">${METODOLOGIA.map((x) => `<article class="card"><div class="card__body"><h3 style="margin-bottom:8px">${esc(x.t)}</h3><p style="margin:0;color:var(--text-2)">${esc(x.d)}</p></div></article>`).join("")}</div>`;
    if (tab === "licencas") panel.innerHTML = licencas();
    if (tab === "linhagem") panel.innerHTML = linhagem(m);
    if (tab === "conjuntos") {
      const res = await Promise.all(m.datasets.map((d) => load(d.id)));
      panel.innerHTML = res.map((r, i) => conjunto(m.datasets[i], r, params.get("ds"))).join("") +
        card({ title: "Não publicados nesta versão", body: table([{ key: "titulo", label: "Conjunto" }, { key: "motivo", label: "Motivo" }], m.nao_publicados) });
      const alvo = params.get("ds") && root.querySelector(`#ds-${CSS.escape(params.get("ds"))}`);
      if (alvo) alvo.scrollIntoView({ block: "start" });
    }
  },
};

function fontes(manifest) {
  const publishedSources = new Set();
  const tableSources = {
    geosampa: ["geosampa_"], cemaden: ["cemaden_"], sibh: ["sibh_"],
    sgb: ["sgb_"], ibge: ["ibge_"], osm: ["osm_", "openstreetmap"], inmet: ["inmet_"],
  };
  for (const d of manifest?.datasets || []) {
    const source = d.fonte?.id || d.fonte;
    if (typeof source === "string" && source !== "multiplas") publishedSources.add(source.toLowerCase());
    const tables = (d.tabelas_origem || []).map((t) => t.toLowerCase());
    for (const [id, prefixes] of Object.entries(tableSources)) {
      if (tables.some((t) => prefixes.some((p) => t.includes(p)))) publishedSources.add(id);
    }
  }
  return `<div class="stack">${FONTES.map((f) => {
    const published = publishedSources.has(f.id.toLowerCase());
    return `<article class="card source" id="src-${f.id}">
    <div class="card__head"><div><h3>${esc(f.nome)}</h3><p>${esc(f.uso)}</p></div>
      <div class="pill-list">${badge("neutral", `Prioridade ${f.prioridade}`)} ${published ? badge("ok", "Publicado na web", "check") : badge("neutral", "Só no pipeline", "database")}</div></div>
    <div class="card__body">
      <dl class="dl">
        <dt>Acesso</dt><dd>${esc(f.acesso)}</dd>
        <dt>Frequência</dt><dd>${esc(f.frequencia)}</dd>
        <dt>Cobertura</dt><dd>${esc(f.cobertura)}</dd>
        <dt>Licença</dt><dd>${esc(f.licenca)}</dd>
        <dt>Tabelas Gold</dt><dd>${f.tabelas.map((t) => `<code>${esc(t)}</code>`).join(" ")}</dd>
        <dt>Limitações</dt><dd><ul class="plain">${f.limitacoes.map((l) => `<li>${esc(l)}</li>`).join("")}</ul></dd>
        <dt>Site</dt><dd><a href="${esc(f.url)}" target="_blank" rel="noopener">${esc(f.url)} ${icon("external")}</a></dd>
      </dl>
    </div></article>`;
  }).join("")}</div>`;
}

function conjunto(d, r, alvo) {
  const meta = r.meta || {};
  const ok = r.status === "ok" || r.status === "vazio";
  const per = meta.periodo ? `${fmtDay(meta.periodo.inicio?.slice(0, 10))} a ${fmtDay(meta.periodo.fim?.slice(0, 10))}` : meta.data_referencia || "—";
  const gran = meta.granularidade ? [meta.granularidade.espacial, meta.granularidade.temporal].filter(Boolean).join(" · ") : "—";
  return `<article class="card ${alvo === d.id ? "is-target" : ""}" id="ds-${esc(d.id)}" style="margin-bottom:16px">
    <div class="card__head"><div><h3>${esc(d.titulo)}</h3><p><code>${esc(d.arquivo)}</code> · ${fmtInt(d.registros || 0)} registros · ${(d.bytes / 1024).toFixed(0)} KB</p></div>
      <div class="pill-list">${natureBadge(meta.natureza ? meta : d)} ${ok ? freshnessBadge(meta, d.id) : badge("error", "Falha ao carregar", "alert")}</div></div>
    <div class="card__body">
      ${!ok ? stateBlock("error", "O arquivo não respondeu", "Os metadados abaixo vêm do manifesto.") : ""}
      <dl class="dl">
        <dt>Fonte</dt><dd>${esc(meta.fonte?.nome || d.fonte)}</dd>
        <dt>Natureza</dt><dd>${esc(meta.natureza || d.natureza)}${meta.natureza_representada ? ` (representa dado ${esc(meta.natureza_representada)})` : ""}</dd>
        <dt>Período / referência</dt><dd>${esc(per)}${meta.referencia ? ` · última hora ${esc(fmtDateTime(meta.referencia))}` : ""}</dd>
        <dt>Unidade</dt><dd>${esc(meta.unidade || "—")}</dd>
        <dt>Granularidade</dt><dd>${esc(gran)}</dd>
        <dt>Tabelas de origem</dt><dd>${(d.tabelas_origem || []).map((t) => `<code>${esc(t)}</code>`).join(" ") || "—"}</dd>
        <dt>Processado</dt><dd>${esc(fmtDateTime(d.processado_em))}</dd>
        <dt>Publicado</dt><dd>${esc(fmtDateTime(d.publicado_em))}</dd>
        <dt>Versão</dt><dd>${esc(meta.versao || "—")}</dd>
        <dt>Licença</dt><dd>${esc(meta.licenca || "—")}</dd>
        <dt>Qualidade</dt><dd>${esc(meta.qualidade?.descricao || d.qualidade)}</dd>
        ${meta.observacao ? `<dt>Observação</dt><dd>${esc(meta.observacao)}</dd>` : ""}
        ${meta.limiares ? `<dt>Limiares</dt><dd>${esc(meta.limiares)}</dd>` : ""}
      </dl>
    </div></article>`;
}

function linhagem(m) {
  const steps = ["Fonte", "Landing (volume)", "Bronze", "Silver", "Gold", "Export web"];
  return `<p class="muted" style="max-width:760px">Cada fonte passa pelas mesmas etapas. As regras de qualidade da Silver barram a Gold quando falham. ${isDemo() ? "<strong>Em demonstração, os horários e estados de qualidade abaixo são simulados.</strong>" : ""}</p>
    <div class="lineage" aria-hidden="true">${steps.map((s, i) => `<span>${esc(s)}</span>${i < steps.length - 1 ? icon("arrowRight") : ""}`).join("")}</div>
    ${table(
      [
        { label: "Fonte", fmt: (r) => `<strong>${esc(r.fonte)}</strong>` },
        { label: "Tabelas (Bronze → Silver → Gold)", fmt: (r) => r.tabelas.map((t) => `<code>${esc(t)}</code>`).join(` ${icon("arrowRight", "inline-ic")} `) },
        { label: "Última execução", fmt: (r) => esc(fmtDateTime(r.processado_em)) },
        { label: "Qualidade", fmt: (r) => (r.qualidade === "simulado" ? badge("demo", "Simulada", "flask") : r.qualidade === "aprovado" ? badge("ok", "Aprovada", "check") : r.qualidade === "aprovado_com_avisos" ? badge("stale", "Aprovada com avisos", "info") : r.qualidade === "reprovado" ? badge("error", "Reprovada", "alert") : badge("neutral", "Não verificada")) },
        { label: "Na web", fmt: (r) => (r.publicado_na_web ? badge("ok", "Publicado", "check") : badge("neutral", "Não publicado")) },
      ],
      m.linhagem,
      { caption: "Catálogo: fiap_analytics (Unity Catalog). Cada job grava log por origem com contagem e SHA256 dos arquivos." },
    )}`;
}

function licencas() {
  return `<div class="grid grid--2">
    ${card({ title: "Atribuições obrigatórias", body: `<ul class="plain">
      <li><strong>OpenStreetMap:</strong> © colaboradores do OpenStreetMap, sob a <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">Open Database License (ODbL 1.0)</a>. O mapa base também usa dados do OSM.</li>
      <li><strong>Mapa base:</strong> © CARTO (estilo Positron).</li>
      <li><strong>Suscetibilidade:</strong> IPT/CPRM (2015), Serviço Geológico do Brasil, registro no RIGEO.</li>
      <li><strong>Demais fontes:</strong> GeoSampa (Prefeitura de São Paulo), CEMADEN, SIBH/SP Águas, INMET e IBGE, conforme os termos de cada portal.</li></ul>` })}
    ${card({ title: "O que a plataforma não faz", body: `<ul class="plain">
      <li>Não é sistema oficial de alerta nem substitui Defesa Civil, CEMADEN, INMET, SAISP ou órgãos municipais.</li>
      <li>Não prevê alagamento por rua nem garante que uma via esteja transitável.</li>
      <li>Não oferece roteamento ou navegação em tempo real.</li>
      <li>Não exibe score ou classe de risco enquanto o gate de Data Science não for aprovado.</li>
      <li>Não infere exposição individual a partir de dados populacionais agregados.</li>
      <li>Não coleta dados pessoais: preferências (perfil e filtros) ficam só no seu navegador.</li></ul>` })}
  </div>`;
}
