// Base de conhecimento (F14): glossário, camadas, perguntas frequentes e limitações.
import { icon } from "../icons.js";
import { esc, norm } from "../util.js";
import { pageHead } from "../ui.js";
import { KB } from "../content.js";

const GRUPOS = [["faq", "Perguntas frequentes"], ["glossario", "Glossário"], ["camada", "Camadas do mapa"], ["limitacao", "Limitações conhecidas"]];

export default {
  subtitle: "Conceitos, camadas, perguntas frequentes e limitações",
  async render(root) {
    root.innerHTML = `<div class="page">
      ${pageHead({ eyebrow: "Base de conhecimento", title: "Entenda os dados e a plataforma", lead: "Conteúdo curado pela equipe do projeto. É a mesma base que o assistente consulta.", actions: `<a class="btn btn--secondary" href="#/assistente">${icon("chat")}Perguntar ao assistente</a>` })}
      <div class="kb-search">
        <label for="kb-q" class="field__label">Buscar na base</label>
        <div class="kb-search__box">${icon("search")}<input class="input" id="kb-q" type="search" placeholder="Ex.: suscetibilidade, mm, alerta, fonte" autocomplete="off"></div>
        <div class="seg" role="group" aria-label="Filtrar por grupo" style="margin-top:12px"><button type="button" data-g="" aria-pressed="true">Tudo</button>${GRUPOS.map(([k, t]) => `<button type="button" data-g="${k}" aria-pressed="false">${t}</button>`).join("")}</div>
      </div>
      <p class="sr-only" aria-live="polite" id="kb-count"></p>
      <div id="kb-list"></div>
    </div>`;
    let grupo = "";
    const q = root.querySelector("#kb-q");
    const draw = () => {
      const termo = norm(q.value.trim());
      const itens = KB.filter((k) => (!grupo || k.tipo === grupo) && (!termo || norm(`${k.titulo} ${k.texto} ${k.tags}`).includes(termo)));
      root.querySelector("#kb-count").textContent = `${itens.length} itens encontrados`;
      root.querySelector("#kb-list").innerHTML = itens.length
        ? GRUPOS.filter(([k]) => itens.some((i) => i.tipo === k)).map(([k, t]) => `<h3 class="section-title">${t}</h3><div class="kb-grid">${itens.filter((i) => i.tipo === k).map((i) => `<article class="card kb-item" id="kb-${esc(i.id)}"><div class="card__body"><h4>${esc(i.titulo)}</h4><p>${esc(i.texto)}</p>${i.link ? `<a href="${esc(i.link)}">Ver na plataforma ${icon("arrowRight")}</a>` : ""}</div></article>`).join("")}</div>`).join("")
        : `<div class="state">${icon("info")}<div><strong>Nada encontrado</strong><p>Tente outro termo ou pergunte ao assistente.</p></div></div>`;
    };
    q.addEventListener("input", draw);
    root.querySelectorAll("[data-g]").forEach((b) => b.addEventListener("click", () => {
      grupo = b.dataset.g;
      root.querySelectorAll("[data-g]").forEach((x) => x.setAttribute("aria-pressed", String(x === b)));
      draw();
    }));
    draw();
  },
};
