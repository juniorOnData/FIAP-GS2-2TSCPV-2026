// Portal institucional (PDR 12.2): composição editorial, conceito Urban Intelligence e chamadas.
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
        <a class="brand" href="#/" style="padding:0">${LOGO}<span class="brand__name">Chuva<span>Segura</span>-SP<span class="brand__tag">Urban Intelligence</span></span></a>
        <nav class="portal__links" aria-label="Portal">
          <a href="#/mapa">Mapa</a><a href="#/indicadores">Indicadores</a><a href="#/alertas">Alertas</a><a href="#/dados">Sobre os dados</a><a href="#/conhecimento">Base de conhecimento</a>
        </nav>
        <a class="btn btn--primary btn--sm" href="#/mapa">Acessar a plataforma</a>
      </header>

      <section class="hero" aria-labelledby="hero-title">
        <div class="hero__text">
          <span class="eyebrow">Urban Intelligence · Município de São Paulo</span>
          <h1 id="hero-title" class="hero__title">Entender a chuva.<br>Antecipar riscos.<br><span>Proteger a cidade.</span></h1>
          <p class="hero__lead">Inteligência de dados para monitoramento hidrometeorológico e prevenção de alagamentos em São Paulo. Chuva, nível de rios, ocorrências históricas e suscetibilidade do território em um mesmo mapa, com fonte e período sempre visíveis.</p>
          <div class="row" style="gap:12px">
            <a class="btn btn--primary btn--lg" href="#/monitoramento">${icon("activity")}Explorar o monitoramento</a>
            <a class="btn btn--secondary btn--lg" href="#projeto">Conhecer o projeto</a>
          </div>
          <p class="hero__note">${icon("info")}Não é sistema oficial de alerta. Em emergência, siga a Defesa Civil (199) e os canais oficiais.</p>
        </div>
        <div class="hero__visual" aria-hidden="true">${heroArt()}</div>
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

// Ilustração abstrata (não é mapa real): malha urbana, canal e pontos de medição.
function heroArt() {
  const dots = [[120, 90], [210, 140], [300, 70], [360, 190], [160, 230], [260, 260], [400, 110], [80, 180]];
  return `<svg viewBox="0 0 480 360" class="hero__svg">
    <defs><pattern id="g" width="24" height="24" patternUnits="userSpaceOnUse"><path d="M24 0H0V24" fill="none" stroke="#DCE6F5" stroke-width="1"/></pattern></defs>
    <rect width="480" height="360" rx="24" fill="#fff"/>
    <rect width="480" height="360" rx="24" fill="url(#g)"/>
    <path d="M-10 210C60 190 110 230 180 205S300 150 360 175 450 200 490 180" fill="none" stroke="#B4A7EF" stroke-width="34" stroke-linecap="round" opacity=".45"/>
    <path d="M-10 210C60 190 110 230 180 205S300 150 360 175 450 200 490 180" fill="none" stroke="#6DA7EC" stroke-width="6" stroke-linecap="round"/>
    <path d="M250 -10C240 60 270 110 255 170" fill="none" stroke="#6DA7EC" stroke-width="4" stroke-linecap="round"/>
    ${dots.map(([x, y], i) => `<circle cx="${x}" cy="${y}" r="${8 + (i % 3) * 4}" fill="${["#CDE2FB", "#6DA7EC", "#256ABF"][i % 3]}" stroke="#fff" stroke-width="3"/>`).join("")}
    <g transform="translate(300 250)"><circle r="22" fill="#1E293B"/><text y="5" text-anchor="middle" fill="#fff" font-family="Inter" font-weight="700" font-size="14">12</text></g>
    <g transform="translate(24 24)"><rect width="168" height="58" rx="12" fill="#fff" stroke="#E2E8F0"/><text x="14" y="24" font-family="Inter" font-size="11" fill="#64748B">Ilustração</text><text x="14" y="46" font-family="Manrope" font-weight="800" font-size="17" fill="#0F172A">Chuva · rios · território</text></g>
  </svg>`;
}
