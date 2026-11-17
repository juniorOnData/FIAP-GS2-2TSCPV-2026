// Assistente (F15) em modo demonstrativo: sem modelo de linguagem.
// Responde com a base de conhecimento curada e com cálculos sobre os dados
// publicados, sempre citando fontes e filtros. Nada sai do navegador.
import { icon } from "../icons.js";
import { esc, norm, fmtInt, fmt1, fmtPct, fmtDateTime, groupCount } from "../util.js";
import { store, resolvePeriod } from "../store.js";
import { getOccurrences, filterOccurrences, getStations, filterStations, rainLastHours, isDemo } from "../data.js";
import { subprefNome, badge } from "../ui.js";
import { KB } from "../content.js";

const SUGESTOES = {
  gestao: ["Quais subprefeituras têm mais ocorrências?", "O que significa suscetibilidade alta?", "Quantas ocorrências há no recorte?", "A plataforma calcula risco?"],
  tecnico: ["Por que as ocorrências não têm hora?", "Com que frequência os dados são atualizados?", "De onde vêm os dados?", "Onde choveu mais nas últimas 24 horas?"],
  publico: ["Esta plataforma emite alertas?", "A rua da minha casa está segura agora?", "O que é chuva acumulada em mm?", "O que é o modo demonstração?"],
};
const STOP = new Set("a o os as de da do das dos e em no na nos nas um uma para por com que qual quais como onde quando é ser são há tem ter mais menos isso esta este essa esse me meu minha sobre".split(" "));
const history = [];

const tokens = (s) => norm(s).replace(/[^a-z0-9 ]/g, " ").split(/\s+/).filter((t) => t.length > 2 && !STOP.has(t));

function retrieve(q) {
  const qt = tokens(q);
  if (!qt.length) return [];
  return KB.map((k) => {
    const title = tokens(k.titulo), body = tokens(`${k.texto} ${k.tags}`);
    let s = 0;
    for (const t of qt) {
      if (title.some((w) => w.startsWith(t) || t.startsWith(w))) s += 3;
      else if (body.some((w) => w.startsWith(t) || (t.length > 4 && w.startsWith(t.slice(0, 5))))) s += 1;
    }
    return { k, s: s / Math.sqrt(qt.length) };
  }).filter((x) => x.s >= 1.6).sort((a, b) => b.s - a.s).slice(0, 2);
}

function filtrosTexto() {
  const f = store.filters;
  const per = resolvePeriod();
  return [`período ${per.texto}`, f.tipos.length ? `tipo ${f.tipos.join(", ")}` : "todos os tipos", f.subprefeitura ? subprefNome(f.subprefeitura) : "município inteiro", f.rede ? `rede ${f.rede}` : null].filter(Boolean).join(" · ");
}

async function answer(q) {
  const n = norm(q);
  const demoNote = isDemo() ? " Valores do pacote de demonstração: sintéticos, sem conexão com dados reais." : "";

  // 1. Segurança de vias e situação atual: nunca afirmar que uma via está segura.
  if (/\b(rua|via|avenida|av|caminho|rota|trajeto|ponte|tunel|passar|atravessar)\b/.test(n) && /(segur|alagad|transit|agora|hoje|posso|da pra|risco)/.test(n)) {
    const k = KB.find((x) => x.id === "faq-rua");
    return { tipo: "Conteúdo curado", texto: `Não consigo dizer se uma rua está segura ou alagada agora. ${k.texto.replace(/^Não\.\s*/, "")} Se houver risco imediato, ligue 199 (Defesa Civil) ou 193 (Bombeiros).`, fontes: [kbRef(k)] };
  }
  // 2. Previsão
  if (/(vai chover|previs|vai alagar|amanha|proximos dias|vai encher)/.test(n)) {
    const k = KB.find((x) => x.id === "previsao");
    return { tipo: "Conteúdo curado", texto: `A plataforma não faz previsão. ${k.texto} Para previsão do tempo e avisos, consulte o INMET, o CEMADEN e a Defesa Civil.`, fontes: [kbRef(k), kbRef(KB.find((x) => x.id === "faq-risco"))] };
  }
  // 3. Perguntas sobre os dados publicados
  if (/(quant|numero|total)/.test(n) && /ocorr/.test(n)) {
    const r = await getOccurrences();
    if (r.status !== "ok") return indisponivel("ocorrências");
    const occ = filterOccurrences(r.rows, store.filters);
    const tipos = [...groupCount(occ.map((o) => o.tipo))].sort((a, b) => b[1] - a[1]).map(([t, c]) => `${t}: ${fmtInt(c)}`).join("; ");
    return { tipo: "Dado publicado", texto: `No recorte atual há ${fmtInt(occ.length)} ocorrências históricas registradas${tipos ? ` (${tipos})` : ""}.${demoNote}`, fontes: [dataRef("Ocorrências históricas (GeoSampa)", r.meta, filtrosTexto())] };
  }
  if (/subprefeitura|regiao|bairro/.test(n) && /(mais|maior|ranking|concentra)/.test(n)) {
    const r = await getOccurrences();
    if (r.status !== "ok") return indisponivel("ocorrências");
    const occ = filterOccurrences(r.rows, { ...store.filters, subprefeitura: "" });
    const top = [...groupCount(occ.map((o) => o.subprefeitura))].sort((a, b) => b[1] - a[1]).slice(0, 3);
    if (!top.length) return { tipo: "Dado publicado", texto: "Não há ocorrências no período e tipo selecionados.", fontes: [] };
    return { tipo: "Dado publicado", texto: `No período selecionado, as subprefeituras com mais registros são ${top.map(([s, c]) => `${subprefNome(s)} (${fmtInt(c)}, ${fmtPct(c / occ.length, 0)})`).join(", ")}. É contagem bruta, não taxa: as subprefeituras têm áreas e populações diferentes.${demoNote}`, fontes: [dataRef("Ocorrências históricas (GeoSampa)", r.meta, filtrosTexto().replace(/ · [^·]*$/, ""))] };
  }
  if (/(chov|chuva)/.test(n) && /(mais|maior|onde|qual estac)/.test(n)) {
    const st = await getStations();
    if (st.horaria.status !== "ok") return indisponivel("chuva horária");
    const est = filterStations(st.est.data.chuva, store.filters).map((s) => ({ s, ...rainLastHours(st.horaria, s.id, 24) })).filter((x) => x.mm !== null).sort((a, b) => b.mm - a.mm);
    if (!est.length) return { tipo: "Dado publicado", texto: "Nenhuma estação do recorte tem leitura nas últimas 24 horas.", fontes: [] };
    const t = est[0];
    return { tipo: "Dado publicado", texto: `Nas 24 horas até ${fmtDateTime(st.horaria.meta.referencia)}, o maior acumulado foi em ${t.s.nome} (${t.s.rede}): ${fmt1(t.mm)} mm. Isso é medição pontual, não aviso.${demoNote}`, fontes: [dataRef("Chuva horária (CEMADEN, SIBH, INMET)", st.horaria.meta, `${est.length} estações com leitura${store.filters.rede ? ` · rede ${store.filters.rede}` : ""}`)] };
  }
  // 4. Base de conhecimento
  const hits = retrieve(q);
  if (hits.length) {
    return { tipo: "Conteúdo curado", texto: hits.map((h) => h.k.texto).join(" "), fontes: hits.map((h) => kbRef(h.k)) };
  }
  return { tipo: "Sem base suficiente", texto: "Não encontrei base suficiente para responder isso com segurança. Posso explicar conceitos, fontes, camadas, atualização e limitações, ou contar ocorrências e chuva do recorte atual.", fontes: [] };
}

const kbRef = (k) => ({ titulo: k.titulo, detalhe: "Base de conhecimento", link: `#/conhecimento` });
const dataRef = (titulo, meta, filtros) => ({ titulo, detalhe: `publicado ${fmtDateTime(meta.publicado_em)} · ${filtros}`, link: `#/dados?ds=${meta.id}` });
const indisponivel = (n) => ({ tipo: "Indisponível", texto: `O conjunto de ${n} não está disponível nesta publicação, então não posso responder com dados.`, fontes: [] });

function bubble(m) {
  if (m.de === "user") return `<div class="msg msg--user"><p>${esc(m.texto)}</p></div>`;
  const kind = { "Dado publicado": isDemo() ? "demo" : "info", "Conteúdo curado": "neutral", "Sem base suficiente": "stale", Indisponível: "error" }[m.tipo] || "neutral";
  return `<div class="msg msg--bot">
    <div class="msg__meta">${badge(kind, m.tipo === "Dado publicado" && isDemo() ? "Dado sintético (demonstração)" : m.tipo)}</div>
    <p>${esc(m.texto)}</p>
    ${m.fontes?.length ? `<div class="msg__src"><strong>Consultado:</strong><ul>${m.fontes.map((f) => `<li><a href="${esc(f.link)}">${esc(f.titulo)}</a> <span class="muted">— ${esc(f.detalhe)}</span></li>`).join("")}</ul></div>` : ""}
  </div>`;
}

export default {
  subtitle: "Perguntas sobre a plataforma, os dados e a metodologia",
  async render(root) {
    const sug = SUGESTOES[store.perfil.id] || SUGESTOES.gestao;
    root.innerHTML = `<div class="page chat-page">
      <div class="state state--demo" role="note">${icon("flask")}<div><strong>Assistente em modo demonstrativo</strong><p>Nenhum modelo de linguagem está integrado. As respostas vêm da base de conhecimento curada e de cálculos sobre os dados publicados, com fontes indicadas. O que você digita não sai do seu navegador. Não emite alertas nem avalia a segurança de vias.</p></div></div>
      <div class="chat card">
        <div class="chat__log" id="log" role="log" aria-live="polite" aria-label="Conversa">
          ${history.length ? history.map(bubble).join("") : `<div class="msg msg--bot"><p>Olá! Posso explicar camadas, fontes, metodologia e limitações, ou contar ocorrências e chuva no recorte atual (${esc(filtrosTexto())}).</p></div>`}
        </div>
        <div class="chat__sug" aria-label="Perguntas sugeridas">${sug.map((s) => `<button type="button" class="btn btn--secondary btn--sm" data-sug="${esc(s)}">${esc(s)}</button>`).join("")}</div>
        <form class="chat__form" id="chat-form">
          <label for="chat-q" class="sr-only">Sua pergunta</label>
          <input class="input" id="chat-q" autocomplete="off" maxlength="300" placeholder="Pergunte sobre os dados ou a plataforma">
          <button class="btn btn--primary" type="submit">${icon("send")}Enviar</button>
        </form>
      </div>
    </div>`;
    const log = root.querySelector("#log");
    const input = root.querySelector("#chat-q");
    const ask = async (q) => {
      q = q.trim();
      if (!q) return;
      history.push({ de: "user", texto: q });
      const a = await answer(q);
      history.push({ de: "bot", ...a });
      log.innerHTML = history.map(bubble).join("");
      log.scrollTop = log.scrollHeight;
    };
    root.querySelector("#chat-form").addEventListener("submit", (e) => {
      e.preventDefault();
      const q = input.value;
      input.value = "";
      ask(q);
    });
    root.querySelectorAll("[data-sug]").forEach((b) => b.addEventListener("click", () => ask(b.dataset.sug)));
    log.scrollTop = log.scrollHeight;
  },
};
