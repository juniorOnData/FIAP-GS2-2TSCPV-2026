// Central de alertas da plataforma: episódios detectados por regras sobre chuva e nível observados,
// com reconhecimento e tratamento (registrados só neste navegador) e canais de notificação.
import { icon } from "../icons.js";
import { esc, fmtInt, fmt1, fmt2, fmtDateTime, fmtHourShort, fmtAge, storage } from "../util.js";
import { store } from "../store.js";
import { getStations, filterStations, load, freshness, hourlyTimes } from "../data.js";
import { filterBar, bindFilterBar, stateBlock, resultState, kpiCard, card, table, badge, statusBadge, subprefNome } from "../ui.js";
import { columnChart, lineChart, chartBlock, RAIN_RAMP } from "../charts.js";
import { REGRAS, REDES_EM_REVISAO, detectarAlertas, nivelComUnidadeConfirmada } from "../regras-alerta.js";

const CHAVE = "cs.alertas.tratamento";
const TRATAMENTO = {
  novo: ["info", "Novo", "bell"],
  reconhecido: ["neutral", "Reconhecido", "check"],
  em_tratamento: ["stale", "Em tratamento", "clock"],
  finalizado: ["ok", "Finalizado", "checkCircle"],
  cancelado: ["neutral", "Cancelado", "x"],
};
const ACOES = [
  { de: ["novo"], para: "reconhecido", rotulo: "Reconhecer", ic: "check", cls: "btn--primary" },
  { de: ["novo", "reconhecido"], para: "em_tratamento", rotulo: "Iniciar tratamento", ic: "clock", cls: "btn--secondary" },
  { de: ["novo", "reconhecido", "em_tratamento"], para: "finalizado", rotulo: "Finalizar", ic: "checkCircle", cls: "btn--secondary" },
  { de: ["novo", "reconhecido", "em_tratamento"], para: "cancelado", rotulo: "Cancelar", ic: "x", cls: "btn--ghost" },
];

let recorte = "todos";
let grandeza = "";
let selecionado = null;

const historico = (id) => storage.get(CHAVE, {})[id] || [];
const situacao = (id) => historico(id).at(-1)?.status || "novo";
function registrar(id, status) {
  const todos = storage.get(CHAVE, {});
  todos[id] = [...(todos[id] || []), { status, em: new Date().toISOString() }];
  storage.set(CHAVE, todos);
}

const tratamentoBadge = (s) => badge(...TRATAMENTO[s]);
const valorFmt = (a, v) => (a.regra.unidade === "mm" ? `${fmt1(v)} mm` : `${v >= 0 && a.regra.id === "nivel_subida" ? "+" : ""}${fmt2(v)} m`);
const tituloAlerta = (a) =>
  ({
    chuva_1h: `${fmt1(a.pico)} mm em 1 hora`,
    chuva_24h: `${fmt1(a.pico)} mm em 24 horas`,
    nivel_subida: `Nível subiu ${fmt2(a.pico)} m em poucas horas`,
    nivel_maximo: `Nível ${fmt2(a.pico)} m acima do maior dos 12 meses`,
  })[a.regra.id];
const nomeLocal = (s) => (s.curso_dagua && s.curso_dagua !== s.nome ? `${s.curso_dagua} · ${s.nome}` : s.nome);

export default {
  subtitle: "Alertas da plataforma por regras sobre chuva e nível observados",
  async render(root) {
    root.innerHTML = `<div class="page">
      <div id="al-aviso"></div>
      ${filterBar({ id: "fb", show: ["subprefeitura", "rede"] })}
      <div id="al-fresh"></div>
      <div class="grid grid--kpi" id="al-kpis">${Array.from({ length: 4 }, () => '<div class="card kpi"><div class="skeleton" style="height:100%"></div></div>').join("")}</div>
      <div class="grid grid--12" style="margin-top:16px">
        <div class="span-7" id="al-lista"></div>
        <div class="span-5" id="al-detalhe"></div>
        <div class="span-7" id="al-regras"></div>
        <div class="span-5" id="al-canais"></div>
      </div>
    </div>`;
    bindFilterBar(root);
    root.querySelector("#al-aviso").innerHTML = `<div style="margin-bottom:16px">${stateBlock(
      "info",
      "Alertas da plataforma, não oficiais",
      "São gerados por regras fixas sobre as leituras publicadas de chuva e nível, sem modelo preditivo. Não substituem a Defesa Civil, o CGE, o CEMADEN ou o INMET; em emergência, ligue 199. Avisos oficiais ainda não estão integrados.",
    )}</div>`;

    const [st, nivel] = await Promise.all([getStations(), load("nivel")]);
    const bad = resultState(st.est, "Cadastro de estações") || resultState(st.horaria, "Chuva horária");
    if (bad) {
      root.querySelector("#al-kpis").innerHTML = "";
      root.querySelector("#al-lista").innerHTML = bad;
      return;
    }

    const fr = freshness(st.horaria.meta, "chuva_horaria");
    if (fr.estado === "desatualizado")
      root.querySelector("#al-fresh").innerHTML = `<div style="margin-bottom:16px">${stateBlock(
        "stale",
        `Leituras até ${fmtDateTime(fr.ref)} (${fmtAge(fr.ref)})`,
        "\"Ativo\" quer dizer ativo na última leitura publicada, não agora. A publicação é diária.",
      )}</div>`;

    const f = store.filters;
    const todos = detectarAlertas({
      estacoes: { chuva: filterStations(st.est.data.chuva, f), nivel: filterStations(st.est.data.nivel, f) },
      horaria: st.horaria,
      temposChuva: hourlyTimes(st.horaria),
      nivel,
      temposNivel: nivel.status === "ok" ? hourlyTimes({ data: { inicio: nivel.data.horario_inicio, series: nivel.data.horario } }) : [],
    });

    const desenharKpis = () => {
      const ativos = todos.filter((a) => a.ativo);
      const abertos = todos.filter((a) => ["novo", "reconhecido", "em_tratamento"].includes(situacao(a.id)));
      root.querySelector("#al-kpis").innerHTML = [
        kpiCard({ label: "Ativos na última leitura", ic: "bell", value: fmtInt(ativos.length), ctx: `${fmtInt(ativos.filter((a) => a.severidade === "alerta").length)} com severidade alerta` }),
        kpiCard({ label: "Novos", ic: "info", value: fmtInt(todos.filter((a) => situacao(a.id) === "novo").length), ctx: "Ainda não reconhecidos" }),
        kpiCard({ label: "Em tratamento", ic: "clock", value: fmtInt(todos.filter((a) => situacao(a.id) === "em_tratamento").length), ctx: `${fmtInt(abertos.length)} em aberto no total` }),
        kpiCard({ label: "Últimos 7 dias", ic: "history", value: fmtInt(todos.length), ctx: `${fmtInt(todos.filter((a) => a.regra.grandeza === "chuva").length)} de chuva · ${fmtInt(todos.filter((a) => a.regra.grandeza === "nivel").length)} de nível` }),
      ].join("");
    };

    const desenharLista = () => {
      desenharKpis();
      const lista = todos.filter((a) => (recorte === "todos" || a.ativo) && (!grandeza || a.regra.grandeza === grandeza));
      if (!lista.find((a) => a.id === selecionado)) selecionado = lista[0]?.id || null;
      const seg = (nome, atual, opcoes) => `<div class="seg" role="group" aria-label="${esc(nome)}">${opcoes.map(([v, t]) => `<button type="button" data-${nome === "Situação" ? "rec" : "gr"}="${v}" aria-pressed="${v === atual}">${esc(t)}</button>`).join("")}</div>`;
      const host = root.querySelector("#al-lista");
      host.innerHTML = card({
        title: "Alertas",
        sub: `Leituras até ${esc(fmtDateTime(st.horaria.meta.referencia))}`,
        body: `<div class="row" style="margin-bottom:12px">${seg("Situação", recorte, [["todos", "Últimos 7 dias"], ["ativos", "Ativos"]])}${seg("Grandeza", grandeza, [["", "Chuva e nível"], ["chuva", "Chuva"], ["nivel", "Nível"]])}</div>` +
          table(
            [
              { label: "Severidade", fmt: (a) => statusBadge(a.severidade) },
              { label: "Alerta", fmt: (a) => `<strong>${esc(tituloAlerta(a))}</strong><br><small class="muted">${esc(nomeLocal(a.local))} · ${esc(a.local.rede)}</small>` },
              { label: "Início", fmt: (a) => `${esc(fmtHourShort(a.inicioEm))}${a.ativo ? `<br>${badge("alerta", "Ativo", "activity")}` : ""}` },
              { label: "Tratamento", fmt: (a) => tratamentoBadge(situacao(a.id)) },
            ],
            lista,
            {
              empty: recorte === "ativos" ? "Nenhum alerta ativo na última leitura." : "Nenhum alerta nos últimos 7 dias publicados.",
              rowAttr: (a) => `class="is-clickable ${a.id === selecionado ? "is-selected" : ""}" data-al="${esc(a.id)}" tabindex="0" aria-selected="${a.id === selecionado}"`,
            },
          ),
      });
      host.querySelectorAll("[data-rec]").forEach((b) => b.addEventListener("click", () => { recorte = b.dataset.rec; desenharLista(); root.querySelector(`[data-rec="${recorte}"]`).focus(); }));
      host.querySelectorAll("[data-gr]").forEach((b) => b.addEventListener("click", () => { grandeza = b.dataset.gr; desenharLista(); root.querySelector(`[data-gr="${grandeza}"]`).focus(); }));
      host.querySelectorAll("[data-al]").forEach((tr) => {
        const pick = () => { selecionado = tr.dataset.al; desenharLista(); root.querySelector(`[data-al="${CSS.escape(selecionado)}"]`)?.focus(); };
        tr.addEventListener("click", pick);
        tr.addEventListener("keydown", (e) => (e.key === "Enter" || e.key === " ") && (e.preventDefault(), pick()));
      });
      desenharDetalhe(lista.find((a) => a.id === selecionado));
    };

    const desenharDetalhe = (a) => {
      const host = root.querySelector("#al-detalhe");
      if (!a) { host.innerHTML = card({ title: "Detalhe do alerta", body: stateBlock("empty", "Selecione um alerta na lista") }); return; }
      const s = a.local;
      const status = situacao(a.id);
      const linha = [
        [`Detectado: ${a.regra.titulo.toLowerCase()}`, a.inicioEm],
        [`Pico: ${valorFmt(a, a.pico)}`, a.picoEm],
        ...(a.ativo || a.fim === a.iPico ? [] : [["Última hora acima do limiar", a.fimEm]]),
        ...historico(a.id).map((h) => [TRATAMENTO[h.status][1], h.em]),
      ];
      const monit = s.grandeza === "Nível" ? `nivel=${encodeURIComponent(s.id)}` : `estacao=${encodeURIComponent(s.id)}`;
      host.innerHTML = card({
        title: tituloAlerta(a),
        sub: `${esc(nomeLocal(s))} · ${esc(s.rede)} · código ${esc(s.codigo)} · ${esc(subprefNome(s.subprefeitura))}`,
        body: `<div class="pill-list" style="margin-bottom:12px">${statusBadge(a.severidade)} ${a.ativo ? badge("alerta", "Ativo", "activity") : badge("neutral", "Encerrado", "check")} ${tratamentoBadge(status)}</div>
          <div class="mini-kpis">
            <div class="mini-kpi"><span>Pico</span><b>${esc(valorFmt(a, a.pico))}</b><small>${esc(fmtHourShort(a.picoEm))}</small></div>
            <div class="mini-kpi"><span>Duração acima do limiar</span><b>${fmtInt(a.fim - a.inicio + 1)} h</b><small>${esc(fmtHourShort(a.inicioEm))}${a.fim > a.inicio ? ` a ${esc(fmtHourShort(a.fimEm))}` : ""}</small></div>
          </div>
          <p class="note" style="margin:12px 0 4px"><strong>Critério:</strong> ${esc(a.regra.criterio)}</p>
          <div id="al-graf"></div>
          <h4 style="margin:16px 0 8px">Linha do tempo</h4>
          <ol class="timeline">${linha.map(([t, em]) => `<li><strong>${esc(t)}</strong><small>${esc(fmtDateTime(em))}</small></li>`).join("")}</ol>
          <div class="row" style="margin-top:16px">${ACOES.filter((x) => x.de.includes(status)).map((x) => `<button type="button" class="btn ${x.cls} btn--sm" data-acao="${x.para}">${icon(x.ic)}${esc(x.rotulo)}</button>`).join("")}</div>
          <p class="note" style="margin-top:8px">${icon("info")}O tratamento fica salvo só neste navegador e não é compartilhado com a equipe nesta versão.</p>`,
        foot: `<div class="row"><a class="btn btn--ghost btn--sm" href="#/mapa?center=${s.lon},${s.lat}&zoom=14">${icon("map")}Ver no mapa</a><a class="btn btn--ghost btn--sm" href="#/monitoramento?${monit}">${icon("activity")}Abrir no monitoramento</a></div>`,
      });
      const titulo = { chuva_1h: "Chuva por hora (mm)", chuva_24h: "Chuva acumulada em 24 horas (mm)", nivel_subida: "Subida do nível sobre as horas anteriores (m)", nivel_maximo: "Nível acima da maior média diária de 12 meses (m)" }[a.regra.id];
      const el = chartBlock(host.querySelector("#al-graf"), {
        tableHtml: table([{ key: "t", label: "Hora" }, { key: "v", label: a.regra.unidade, num: true }], a.tempos.map((t, i) => ({ t: fmtHourShort(t), v: a.valores[i] === null ? "sem leitura" : valorFmt(a, a.valores[i]) })).reverse()),
      });
      const rotulos = a.tempos.map((t) => (fmtHourShort(t).endsWith("00:00") ? fmtHourShort(t).slice(0, 5) : ""));
      const comum = { labels: rotulos, height: 170, unit: a.regra.unidade, tipTitle: (i) => fmtHourShort(a.tempos[i]), ariaLabel: `${titulo}, últimos 7 dias` };
      el.insertAdjacentHTML("beforebegin", `<p class="note">${esc(titulo)}, últimos 7 dias · limiar de atenção ${esc(valorFmt(a, a.regra.atencao))}${a.regra.alerta !== null ? ` · alerta ${esc(valorFmt(a, a.regra.alerta))}` : ""}</p>`);
      if (a.regra.id === "chuva_1h") columnChart(el, { ...comum, series: [{ name: "Chuva", color: RAIN_RAMP[5], values: a.valores.map((v) => v ?? 0) }], fmt: fmt1 });
      else lineChart(el, { ...comum, series: [{ name: a.regra.titulo, color: a.regra.grandeza === "chuva" ? RAIN_RAMP[5] : "#0F8A73", values: a.valores }], fmt: a.regra.unidade === "mm" ? fmt1 : fmt2, minZero: a.regra.grandeza === "chuva" });
      host.querySelectorAll("[data-acao]").forEach((b) => b.addEventListener("click", () => {
        registrar(a.id, b.dataset.acao);
        desenharLista();
        root.querySelector(`[data-al="${CSS.escape(a.id)}"]`)?.focus();
      }));
    };

    const nivelOk = nivel.status === "ok" && nivelComUnidadeConfirmada(nivel.meta);
    const suspensas = [
      ...Object.entries(REDES_EM_REVISAO).map(([rede, motivo]) => [`${rede} fora das regras de chuva`, motivo]),
      ...(nivelOk ? [] : [["Regras de nível suspensas", nivel.status === "ok"
        ? "A publicação atual do nível não confirma a conversão para metros. As regras voltam na próxima publicação do pipeline, que converte a cota de centímetros para metros."
        : "O nível não está disponível nesta publicação."]]),
    ];
    root.querySelector("#al-regras").innerHTML = card({
      title: "Regras de alerta",
      sub: "Critérios fixos da plataforma, avaliados por estação ou sensor a cada hora publicada",
      body: `<ul class="gate__list">${REGRAS.map((r) => `<li>${r.grandeza === "chuva" ? icon("drop") : icon("wave")}<span><strong>${esc(r.titulo)}</strong><small>${esc(r.criterio)}</small></span></li>`).join("")}</ul>
        ${suspensas.map(([t, m]) => `<div style="margin-top:12px">${stateBlock("stale", t, esc(m))}</div>`).join("")}
        <p class="note" style="margin-top:12px">Horas consecutivas acima do limiar, com intervalos de até 3 horas, formam um único alerta. Ele fica ativo enquanto houver leitura acima do limiar nas 2 horas anteriores à última leitura publicada. Não há limiares oficiais integrados; os valores podem ser revistos com as equipes de operação.</p>`,
    });

    root.querySelector("#al-canais").innerHTML = card({
      title: "Canais de notificação",
      sub: "Como os alertas chegam às equipes",
      body: `<ul class="channel-list">
          <li>${icon("grid")}<span class="channel-list__txt"><strong>Painel</strong><small>Esta página, com os alertas da última publicação.</small></span>${badge("ok", "Ativo", "check")}</li>
          <li>${icon("chat")}<span class="channel-list__txt"><strong>SMS</strong><small>Envio planejado. Exige um serviço no servidor, com o provedor de SMS e login de operador, para que só pessoas autorizadas disparem mensagens. Os números dos destinatários ficarão no servidor, nunca no navegador.</small></span>${badge("neutral", "Não configurado", "info")}</li>
        </ul>`,
    });

    desenharLista();
  },
};
