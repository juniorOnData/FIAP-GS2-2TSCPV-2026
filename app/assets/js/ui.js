// Componentes reutilizáveis (cards, badges, estados, proveniência, tabelas, filtros).
import { icon } from "./icons.js";
import { esc, fmtDateTime, fmtAge, fmtDay } from "./util.js";
import { freshness, isDemo } from "./data.js";
import { store, resolvePeriod, TIPOS_OCORRENCIA, REDES } from "./store.js";

export const DEMO_MSG = "Valor demonstrativo, sem conexão com dados reais";

/* ---------------------------------------------------------------- badges */
export const badge = (kind, text, ic) => `<span class="badge badge--${kind}">${ic ? icon(ic) : ""}${esc(text)}</span>`;

// Cor semântica sempre acompanhada de rótulo e ícone (PDR 12.1).
export const statusBadge = (nivel) =>
  ({
    normal: badge("normal", "Normal", "checkCircle"),
    atencao: badge("atencao", "Atenção", "info"),
    alerta: badge("alerta", "Alerta", "alert"),
    critico: badge("critico", "Crítico", "x"),
  })[nivel] || "";

const NATUREZA = {
  historico: ["neutral", "Registro histórico", "history"],
  observado: ["info", "Observado", "activity"],
  oficial: ["neutral", "Cadastro oficial", "shield"],
  derivado: ["neutral", "Derivado", "layers"],
  colaborativo: ["neutral", "Mapeamento colaborativo", "layers"],
  estimado: ["atencao", "Estimado", "flask"],
  sintetico: ["demo", "Sintético", "flask"],
};
export function natureBadge(meta) {
  const n = meta?.natureza_representada || meta?.natureza;
  const [k, t, i] = NATUREZA[n] || ["neutral", n || "—", "info"];
  let out = badge(k, t, i);
  if (meta?.natureza === "sintetico") out += " " + badge("demo", "Demonstração", "flask");
  return out;
}

export function freshnessBadge(meta, key) {
  const f = freshness(meta, key);
  if (f.estado === "estatico") return badge("neutral", `Base estática · ${f.ref}`, "layers");
  if (f.estado === "desatualizado") return badge("stale", `Desatualizado · ${fmtAge(f.ref)}`, "clock");
  if (f.estado === "atual") return badge("ok", `Atualizado · ${fmtAge(f.ref)}`, "check");
  return badge("neutral", "Atualização desconhecida", "info");
}

/* ---------------------------------------------------------------- estados */
const STATE_ICON = { loading: "clock", empty: "info", error: "alert", stale: "clock", unavailable: "x", demo: "flask", info: "info" };
export function stateBlock(kind, title, msg = "") {
  const cls = { error: "state--error", stale: "state--stale", loading: "state--loading", demo: "state--demo" }[kind] || "";
  const role = kind === "error" ? 'role="alert"' : kind === "loading" ? 'role="status"' : "";
  return `<div class="state ${cls}" ${role}>${icon(STATE_ICON[kind] || "info")}<div><strong>${esc(title)}</strong>${msg ? `<p>${msg}</p>` : ""}</div></div>`;
}
export const loadingBlock = (t = "Carregando dados…") => stateBlock("loading", t);

// Converte o resultado de data.load() em bloco de estado (ou null se ok).
export function resultState(r, nome) {
  if (!r) return stateBlock("error", `${nome}: falha de carregamento`);
  if (r.status === "erro")
    return stateBlock("error", `${nome}: não foi possível carregar`, `O arquivo publicado não respondeu (${esc(r.error?.message || "erro")}). Nenhum valor foi exibido no lugar.`);
  if (r.status === "nao_publicado") return stateBlock("unavailable", `${nome}: não publicado nesta versão`, "O conjunto não faz parte da publicação atual.");
  if (r.status === "vazio") return stateBlock("empty", `${nome}: sem registros publicados`);
  return null;
}

/* ---------------------------------------------------------------- KPI */
export function kpiCard({ label, value, unit = "", ctx = "", ic = "", demo = isDemo(), foot = "" }) {
  return `<article class="card kpi">
    <div class="kpi__label">${ic ? icon(ic) : ""}${esc(label)}</div>
    <div class="kpi__value">${value}${unit ? `<span class="kpi__unit">${esc(unit)}</span>` : ""}</div>
    ${ctx ? `<div class="kpi__ctx">${ctx}</div>` : ""}
    ${foot}
    ${demo ? `<div class="kpi__demo">${icon("flask")}${DEMO_MSG}</div>` : ""}
  </article>`;
}

/* ---------------------------------------------------------------- proveniência */
export function provenance(meta, { periodo, extra = "", metodo = true } = {}) {
  if (!meta) return "";
  const fonte = meta.fonte?.nome || meta.fonte || "—";
  const per = periodo || (meta.periodo ? `${fmtDay(meta.periodo.inicio?.slice(0, 10))} a ${fmtDay(meta.periodo.fim?.slice(0, 10))}` : meta.data_referencia || "");
  return `<div class="prov">
    <span>${icon("database")}Fonte: ${esc(fonte)}</span>
    ${per ? `<span>${icon("clock")}Período: ${esc(per)}</span>` : ""}
    <span>${icon("activity")}Publicado: ${esc(fmtDateTime(meta.publicado_em))}</span>
    ${meta.unidade ? `<span>Unidade: ${esc(meta.unidade)}</span>` : ""}
    ${extra}
    ${metodo ? `<span><a href="#/dados?ds=${esc(meta.id)}">Metodologia e linhagem</a></span>` : ""}
  </div>`;
}

/* ---------------------------------------------------------------- tabelas */
// columns: [{ key, label, num, fmt(row) }]
export function table(columns, rows, { caption = "", rowAttr = null, empty = "Nenhum registro no recorte." } = {}) {
  if (!rows.length) return stateBlock("empty", empty);
  const head = columns.map((c) => `<th scope="col" class="${c.num ? "num" : ""}">${esc(c.label)}</th>`).join("");
  const body = rows
    .map((r) => {
      const attr = rowAttr ? rowAttr(r) : "";
      return `<tr ${attr}>${columns.map((c) => `<td class="${c.num ? "num" : ""}">${c.fmt ? c.fmt(r) : esc(r[c.key])}</td>`).join("")}</tr>`;
    })
    .join("");
  return `<div class="table-wrap"><table class="table">${caption ? `<caption>${esc(caption)}</caption>` : ""}<thead><tr>${head}</tr></thead><tbody>${body}</tbody></table></div>`;
}

/* ---------------------------------------------------------------- filtros */
let subprefs = [];
export const setSubprefeituras = (list) => (subprefs = list || []);
export const subprefNome = (sig) => subprefs.find((s) => s.sigla === sig)?.nome || sig || "Não informada";
export const getSubprefeituras = () => subprefs;

// Barra de filtros compartilhada. `show` controla quais dimensões aparecem.
// compacto: rótulos só para leitores de tela e sem a linha de resumo (cabeçalho do mapa; as opções já dizem o filtro).
export function filterBar({ show = ["periodo", "tipo", "subprefeitura", "rede"], id = "fb", compacto = false } = {}) {
  const f = store.filters;
  const { ref, first } = store.bounds;
  const anos = [];
  for (let y = Number(ref.slice(0, 4)); y >= Number(first.slice(0, 4)); y--) anos.push(y);
  const per = resolvePeriod();
  const opt = (v, t, sel) => `<option value="${esc(v)}" ${sel ? "selected" : ""}>${esc(t)}</option>`;

  const parts = [];
  if (show.includes("periodo")) {
    parts.push(`<div class="field"><label for="${id}-periodo">Período</label>
      <select class="select" id="${id}-periodo" data-f="periodo">
        ${opt("30d", "Últimos 30 dias", f.periodo === "30d")}
        ${opt("90d", "Últimos 90 dias", f.periodo === "90d")}
        ${opt("12m", "Últimos 12 meses", f.periodo === "12m")}
        ${opt("ano", "Ano específico", f.periodo === "ano")}
        ${opt("todo", `Todo o período (desde ${first.slice(0, 4)})`, f.periodo === "todo")}
        ${opt("custom", "Intervalo personalizado", f.periodo === "custom")}
      </select></div>`);
    if (f.periodo === "ano")
      parts.push(`<div class="field" style="max-width:120px"><label for="${id}-ano">Ano</label>
        <select class="select" id="${id}-ano" data-f="ano">${anos.map((y) => opt(y, y, (f.ano || anos[0]) === y)).join("")}</select></div>`);
    if (f.periodo === "custom")
      parts.push(`<div class="field" style="max-width:170px"><label for="${id}-ini">De</label>
          <input class="input" type="date" id="${id}-ini" data-f="inicio" min="${first}" max="${ref}" value="${esc(f.inicio || per.inicio)}"></div>
        <div class="field" style="max-width:170px"><label for="${id}-fim">Até</label>
          <input class="input" type="date" id="${id}-fim" data-f="fim" min="${first}" max="${ref}" value="${esc(f.fim || per.fim)}"></div>`);
  }
  if (show.includes("tipo"))
    parts.push(`<div class="field"><label for="${id}-tipo">Tipo de ocorrência</label>
      <select class="select" id="${id}-tipo" data-f="tipos">
        ${opt("", "Todos os tipos", !f.tipos.length)}
        ${TIPOS_OCORRENCIA.map((t) => opt(t, t, f.tipos.length === 1 && f.tipos[0] === t)).join("")}
      </select></div>`);
  if (show.includes("subprefeitura"))
    parts.push(`<div class="field field--wide"><label for="${id}-sp">Subprefeitura</label>
      <select class="select" id="${id}-sp" data-f="subprefeitura">
        ${opt("", "Município inteiro", !f.subprefeitura)}
        ${[...subprefs].sort((a, b) => a.nome.localeCompare(b.nome, "pt-BR")).map((s) => opt(s.sigla, s.nome, f.subprefeitura === s.sigla)).join("")}
      </select></div>`);
  if (show.includes("rede"))
    parts.push(`<div class="field"><label for="${id}-rede">Rede de monitoramento</label>
      <select class="select" id="${id}-rede" data-f="rede">
        ${opt("", "Todas as redes", !f.rede)}
        ${REDES.map((r) => opt(r, r, f.rede === r)).join("")}
      </select></div>`);

  const chips = [];
  if (show.includes("periodo")) chips.push(`<span class="chip">${icon("clock")}${esc(per.rotulo)} · ${esc(per.texto)}</span>`);
  if (show.includes("tipo") && f.tipos.length) chips.push(`<span class="chip">${esc(f.tipos.join(", "))}</span>`);
  if (show.includes("subprefeitura")) chips.push(`<span class="chip">${icon("pin")}${esc(f.subprefeitura ? subprefNome(f.subprefeitura) : "Município inteiro")}</span>`);
  if (show.includes("rede") && f.rede) chips.push(`<span class="chip">Rede ${esc(f.rede)}</span>`);
  const dirty = f.periodo !== "12m" || f.tipos.length || f.subprefeitura || f.rede;

  const campos = compacto ? parts.join("").replaceAll("<label for=", '<label class="sr-only" for=') : parts.join("");
  return `<form class="filterbar ${compacto ? "filterbar--compacto" : ""}" id="${id}" role="search" aria-label="Filtros do recorte" onsubmit="return false">
    ${campos}
    ${dirty ? `<button type="button" class="btn btn--ghost btn--sm" data-reset>${icon("reset")}${compacto ? "Limpar" : "Limpar filtros"}</button>` : ""}
    ${compacto ? "" : `<div class="filterbar__summary" aria-live="polite">Recorte ativo: ${chips.join("")}<span>Os filtros valem para mapa, indicadores e séries.</span></div>`}
  </form>`;
}

export function bindFilterBar(root, id = "fb") {
  const form = root.querySelector(`#${id}`);
  if (!form) return;
  form.addEventListener("change", (e) => {
    const k = e.target.dataset.f;
    if (!k) return;
    const v = e.target.value;
    if (k === "tipos") store.setFilters({ tipos: v ? [v] : [] });
    else if (k === "ano") store.setFilters({ ano: Number(v) });
    else if (k === "periodo") {
      const patch = { periodo: v };
      if (v === "custom" && !store.filters.inicio) Object.assign(patch, resolvePeriod());
      store.setFilters(patch);
    } else store.setFilters({ [k]: v });
  });
  form.querySelector("[data-reset]")?.addEventListener("click", () => store.resetFilters());
}

/* ---------------------------------------------------------------- diversos */
export function pageHead({ eyebrow = "", title, lead = "", actions = "" }) {
  return `<header class="page-head"><div>${eyebrow ? `<span class="eyebrow">${esc(eyebrow)}</span>` : ""}<h2>${esc(title)}</h2>${lead ? `<p>${lead}</p>` : ""}</div>${actions ? `<div class="row">${actions}</div>` : ""}</header>`;
}

export function card({ title, sub = "", body, foot = "", actions = "", cls = "" }) {
  return `<section class="card ${cls}">
    <div class="card__head"><div><h3>${esc(title)}</h3>${sub ? `<p>${sub}</p>` : ""}</div>${actions ? `<div class="pill-list card__actions">${actions}</div>` : ""}</div>
    <div class="card__body">${body}</div>
    ${foot ? `<div class="card__foot">${foot}</div>` : ""}
  </section>`;
}
