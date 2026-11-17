// Shell da aplicação: carregamento inicial, navegação global e roteamento por hash.
import { icon, LOGO } from "./icons.js";
import { esc, fmtDateTime, $ } from "./util.js";
import { loadManifest, load, isDemo, getManifest } from "./data.js";
import { store } from "./store.js";
import { setSubprefeituras, stateBlock } from "./ui.js";
import { ROUTE_META } from "./routes.js";

import portal from "./pages/portal.js";
import overview from "./pages/overview.js";
import mapa from "./pages/mapa.js";
import monitoramento from "./pages/monitoramento.js";
import historico from "./pages/historico.js";
import analises from "./pages/analises.js";
import dados from "./pages/dados.js";
import conhecimento from "./pages/conhecimento.js";
import assistente from "./pages/assistente.js";
import perfil from "./pages/perfil.js";

const PAGES = { overview, mapa, monitoramento, historico, analises, dados, conhecimento, assistente, perfil };
const ROUTES = Object.fromEntries(Object.entries(ROUTE_META).map(([k, r]) => [k, { ...r, page: PAGES[k] }]));

const app = $("#app");
let current = null; // { name, page, cleanup }

function parseHash() {
  const raw = location.hash.replace(/^#\/?/, "");
  const [path, qs] = raw.split("?");
  return { name: path || "", params: new URLSearchParams(qs || "") };
}

function demoBanner() {
  if (!isDemo()) return "";
  return `<div class="demo-banner" role="note">${icon("flask")}<span><strong>Modo demonstração.</strong> Todos os valores são sintéticos, sem conexão com dados reais nem com órgãos oficiais.</span><a href="#/dados">Entenda</a></div>`;
}

function navHtml(active) {
  const groups = {};
  for (const [k, r] of Object.entries(ROUTES)) (groups[r.group] ||= []).push([k, r]);
  return Object.entries(groups)
    .map(
      ([g, items]) => `<div class="nav__group">${esc(g)}</div>` +
        items.map(([k, r]) => `<a class="nav__link" href="#/${k}" ${k === active ? 'aria-current="page"' : ""}>${icon(r.icon)}${esc(r.label)}</a>`).join(""),
    )
    .join("");
}

function renderShell(name) {
  const m = getManifest();
  const r = ROUTES[name];
  const p = store.perfil;
  app.innerHTML = `<div class="shell" id="shell">
    ${demoBanner()}
    <aside class="sidebar" id="sidebar" aria-label="Navegação principal">
      <a class="brand" href="#/">${LOGO}<span class="brand__name">Chuva<span>Segura</span>-SP<span class="brand__tag">Urban Intelligence</span></span></a>
      <nav class="nav">${navHtml(name)}</nav>
      <div class="sidebar__foot">
        <div>${isDemo() ? "Demonstração" : "Gold"} · ${esc(m?.versao || "")}</div>
        <div>Ref. ${esc(fmtDateTime(m?.referencia))}</div>
      </div>
    </aside>
    <div class="scrim" data-close-nav></div>
    <header class="topbar">
      <button class="btn btn--neutral btn--icon menu-btn" type="button" aria-label="Abrir navegação" aria-controls="sidebar" aria-expanded="false" data-open-nav>${icon("menu")}</button>
      <div class="topbar__title"><h1 id="page-title">${esc(r.label)}</h1><p id="page-sub">${esc(r.page.subtitle || "")}</p></div>
      <div class="topbar__actions">
        ${isDemo() ? `<span class="badge badge--demo hide-sm">${icon("flask")}Demonstração</span>` : ""}
        <a class="btn btn--neutral btn--sm" href="#/perfil" title="Perfil de uso (sem login)">${icon("user")}<span class="hide-sm">${esc(p.curto)}</span></a>
      </div>
    </header>
    <main class="main" id="conteudo" tabindex="-1"></main>
  </div>`;
  const shell = $("#shell");
  const btn = $("[data-open-nav]");
  const setNav = (open) => {
    shell.classList.toggle("nav-open", open);
    btn.setAttribute("aria-expanded", String(open));
    if (open) $("#sidebar .nav__link")?.focus();
  };
  btn.addEventListener("click", () => setNav(true));
  $("[data-close-nav]").addEventListener("click", () => setNav(false));
  shell.addEventListener("keydown", (e) => e.key === "Escape" && shell.classList.contains("nav-open") && setNav(false));
  const banner = $(".demo-banner");
  if (banner) document.documentElement.style.setProperty("--banner-h", `${banner.offsetHeight}px`);
}

async function route() {
  const { name, params } = parseHash();
  current?.cleanup?.();
  window.scrollTo(0, 0);

  if (!name) {
    current = { name: "", page: portal };
    app.innerHTML = "";
    current.cleanup = await portal.render(app, { params });
    document.title = "ChuvaSegura-SP — Urban Intelligence";
    return;
  }
  const r = ROUTES[name];
  if (!r) {
    location.replace("#/overview");
    return;
  }
  renderShell(name);
  const main = $("#conteudo");
  current = { name, page: r.page };
  document.title = `${r.label} · ChuvaSegura-SP`;
  try {
    current.cleanup = await r.page.render(main, { params });
  } catch (err) {
    console.error(err);
    main.innerHTML = `<div class="page">${stateBlock("error", "Esta tela não pôde ser exibida", esc(err.message))}</div>`;
  }
  main.focus({ preventScroll: true });
}

store.subscribe(async (kind) => {
  if (!current?.page) return;
  if (kind === "filters" && current.page.onFilters) return current.page.onFilters();
  if (kind === "perfil" && current.name) {
    const btn = document.querySelector('.topbar__actions a[href="#/perfil"] span');
    if (btn) btn.textContent = store.perfil.curto;
    if (current.page.onPerfil) return current.page.onPerfil();
  }
  const y = window.scrollY;
  const main = $("#conteudo") || app;
  const focusId = document.activeElement?.id;
  current.cleanup?.();
  current.cleanup = await current.page.render(main, { params: parseHash().params });
  window.scrollTo(0, y);
  if (focusId) document.getElementById(focusId)?.focus();
});

async function boot() {
  try {
    const m = await loadManifest();
    const ocorrencias = m.datasets?.find((d) => d.id === "ocorrencias");
    const primeiroDia = ocorrencias?.periodo?.inicio?.slice(0, 10) || "2020-01-01";
    store.setBounds(primeiroDia, m.referencia.slice(0, 10));
    const sp = await load("subprefeituras");
    setSubprefeituras(sp.status === "ok" ? sp.data : []);
  } catch (err) {
    app.innerHTML = `<div class="page" style="max-width:720px;margin:64px auto">${stateBlock(
      "error",
      "Não foi possível carregar a publicação de dados",
      `O manifesto <code>manifest.json</code> não respondeu (${esc(err.message)}). Nenhum dado é exibido sem ele. Se estiver rodando localmente, sirva a pasta <code>app/</code> por HTTP (veja app/README.md).`,
    )}</div>`;
    return;
  }
  window.addEventListener("hashchange", route);
  route();
}

boot();
