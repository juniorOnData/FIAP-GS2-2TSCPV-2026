// Portal institucional (PDR 12.2): composição editorial e chamadas.
import { icon, LOGO } from "../icons.js";
import { isDemo, getManifest } from "../data.js";
import { esc, fmtDateTime } from "../util.js";

export default {
  async render(root) {
    const m = getManifest();
    root.innerHTML = `
    ${isDemo() ? `<div class="demo-banner" role="note">${icon("flask")}<span><strong>Modo demonstração.</strong> Os painéis usam dados sintéticos, sem conexão com dados reais nem com órgãos oficiais.</span></div>` : ""}
    <div class="portal">
      <header class="portal__nav">
        <a class="brand" href="#/" style="padding:0">${LOGO}<span class="brand__name">Chuva<span>Segura</span>-SP</span></a>
        <nav class="portal__links" aria-label="Portal">
          <a href="#/mapa">Mapa</a><a href="#/indicadores">Indicadores</a><a href="#/alertas">Alertas</a><a href="#/dados">Sobre os dados</a><a href="#/conhecimento">Base de conhecimento</a>
        </nav>
        <a class="btn btn--primary btn--sm" href="#/mapa">Acessar a plataforma</a>
      </header>

      <section class="hero" aria-labelledby="hero-title">
        <div class="hero__text">
          <span class="eyebrow">Município de São Paulo</span>
          <h1 id="hero-title" class="hero__title">Entender a chuva.<br>Antecipar riscos.<br><span>Proteger a cidade.</span></h1>
          <p class="hero__lead">Inteligência de dados para monitoramento hidrometeorológico e prevenção de alagamentos em São Paulo. Chuva, nível de rios, ocorrências históricas e suscetibilidade do território em um mesmo mapa, com fonte e período sempre visíveis.</p>
          <div class="row" style="gap:12px">
            <a class="btn btn--primary btn--lg" href="#/monitoramento">${icon("activity")}Explorar o monitoramento</a>
            <a class="btn btn--secondary btn--lg" href="#projeto">Conhecer o projeto</a>
          </div>
          <p class="hero__note">${icon("info")}Não é sistema oficial de alerta. Em emergência, siga a Defesa Civil (199) e os canais oficiais.</p>
        </div>
        <div class="hero__visual" aria-hidden="true">
          <img class="hero__foto" src="assets/img/portal-chuva-sp.webp" width="1400" height="788" alt="" fetchpriority="high">
          <figure class="hero__mapa">
            <img src="assets/img/portal-mapa-ilustrativo.webp" width="1000" height="750" alt="" loading="lazy">
            <figcaption>Ilustração</figcaption>
          </figure>
        </div>
      </section>

      <section class="portal__band" id="projeto" aria-labelledby="proj-title">
        <div class="portal__inner">
          <h2 id="proj-title" class="portal__h2">Mais do que saber quando vai chover, entender onde a chuva pode se transformar em risco para a população.</h2>
          <div class="grid grid--3" style="margin-top:32px">
            ${feature("map", "Mapa primeiro", "Ocorrências, estações, chuva, nível e suscetibilidade em camadas que você liga e desliga, com legenda e fonte.")}
            ${feature("database", "Evidência antes da conclusão", "Cada indicador mostra de onde vem, o período, a unidade e quando foi atualizado.")}
            ${feature("layers", "Observado não é estimado", "Registro histórico, medição, cadastro oficial e suscetibilidade têm rótulos e convenções visuais distintos.")}
          </div>
        </div>
      </section>

      <section class="portal__inner portal__sources" aria-labelledby="src-title">
        <h2 id="src-title" class="portal__h3">Fontes integradas no pipeline</h2>
        <p class="muted">Processadas no Databricks (Unity Catalog <code>fiap_analytics</code>) em camadas Bronze, Silver e Gold.</p>
        <ul class="portal__logos">
          ${["GeoSampa", "CEMADEN", "SIBH / SP Águas", "SGB/CPRM", "IBGE — Censo 2022", "OpenStreetMap", "INMET"].map((s) => `<li>${esc(s)}</li>`).join("")}
        </ul>
        <p class="note">Publicação de dados: ${esc(m?.versao || "—")} · referência ${esc(fmtDateTime(m?.referencia))}${isDemo() ? " · pacote de demonstração" : ""}.</p>
      </section>

      <footer class="portal__foot">
        <div class="portal__inner row" style="justify-content:space-between">
          <span>ChuvaSegura-SP · FIAP GS2 2TSCPV 2026</span>
          <span>Mapa base © CARTO · © colaboradores do OpenStreetMap (ODbL)</span>
        </div>
      </footer>
    </div>`;
  },
};

const feature = (ic, t, d) => `<article class="feature">${icon(ic)}<h3>${t}</h3><p>${d}</p></article>`;
