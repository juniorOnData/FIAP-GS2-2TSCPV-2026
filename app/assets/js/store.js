// Estado global compartilhado: filtros (aplicados a mapa, indicadores e séries)
// e perfil de uso. Perfis NÃO são controle de acesso — só ajustam linguagem e atalhos.
import { storage, addDays, fmtDay } from "./util.js";

export const PERFIS = {
  gestao: {
    id: "gestao",
    nome: "Gestão pública / Defesa Civil",
    curto: "Gestão",
    descricao: "Atalhos para mapa, recorrência e suscetibilidade, e perguntas sugeridas sobre concentração territorial.",
    atalhos: ["mapa", "analises", "monitoramento", "historico"],
  },
  tecnico: {
    id: "tecnico",
    nome: "Análise técnica",
    curto: "Técnico",
    descricao: "Atalhos para séries, análises e fontes, qualidade e linhagem, e perguntas sugeridas sobre metodologia.",
    atalhos: ["historico", "analises", "dados", "monitoramento"],
  },
  publico: {
    id: "publico",
    nome: "Consulta pública",
    curto: "Público",
    descricao: "Atalhos para mapa, base de conhecimento e assistente, com perguntas em linguagem simples. Não é sistema de alerta.",
    atalhos: ["mapa", "conhecimento", "historico", "assistente"],
  },
};

export const TIPOS_OCORRENCIA = ["Alagamento", "Inundação", "Queda de Árvore", "Deslizamento"];
export const REDES = ["CEMADEN", "SIBH", "INMET"];

const DEFAULT_FILTERS = { periodo: "12m", ano: null, inicio: null, fim: null, tipos: [], subprefeitura: "", rede: "" };

const state = {
  filters: { ...DEFAULT_FILTERS, ...storage.get("cs.filters", {}) },
  perfil: storage.get("cs.perfil", "gestao"),
  referenceDay: null, // dia de referência do pacote de dados (definido no carregamento)
  firstDay: "2020-01-01",
};
const listeners = new Set();

export const store = {
  get filters() {
    return state.filters;
  },
  get perfil() {
    return PERFIS[state.perfil] || PERFIS.gestao;
  },
  setPerfil(id) {
    if (!PERFIS[id]) return;
    state.perfil = id;
    storage.set("cs.perfil", id);
    emit("perfil");
  },
  setFilters(patch) {
    state.filters = { ...state.filters, ...patch };
    storage.set("cs.filters", state.filters);
    emit("filters");
  },
  resetFilters() {
    state.filters = { ...DEFAULT_FILTERS };
    storage.set("cs.filters", state.filters);
    emit("filters");
  },
  setBounds(firstDay, referenceDay) {
    state.firstDay = firstDay;
    state.referenceDay = referenceDay;
  },
  get bounds() {
    return { first: state.firstDay, ref: state.referenceDay };
  },
  subscribe(fn) {
    listeners.add(fn);
    return () => listeners.delete(fn);
  },
};

function emit(kind) {
  for (const fn of listeners) fn(kind);
}

// Converte o filtro de período em datas inclusivas (dias civis de São Paulo).
export function resolvePeriod(f = state.filters) {
  const ref = state.referenceDay || new Date().toISOString().slice(0, 10);
  const first = state.firstDay;
  let inicio, fim, rotulo;
  switch (f.periodo) {
    case "30d":
      inicio = addDays(ref, -29); fim = ref; rotulo = "Últimos 30 dias";
      break;
    case "90d":
      inicio = addDays(ref, -89); fim = ref; rotulo = "Últimos 90 dias";
      break;
    case "12m":
      inicio = addDays(ref, -364); fim = ref; rotulo = "Últimos 12 meses";
      break;
    case "ano": {
      const ano = f.ano || Number(ref.slice(0, 4));
      inicio = `${ano}-01-01`; fim = `${ano}-12-31` > ref ? ref : `${ano}-12-31`; rotulo = `Ano de ${ano}`;
      break;
    }
    case "custom":
      inicio = f.inicio || first; fim = f.fim || ref;
      if (inicio > fim) [inicio, fim] = [fim, inicio];
      rotulo = `${fmtDay(inicio)} a ${fmtDay(fim)}`;
      break;
    default:
      inicio = first; fim = ref; rotulo = "Todo o período";
  }
  if (inicio < first) inicio = first;
  if (fim > ref) fim = ref;
  return { inicio, fim, rotulo, texto: `${fmtDay(inicio)} a ${fmtDay(fim)}` };
}

// Período anterior de mesma duração (comparações de indicadores).
export function previousPeriod(p) {
  const dias = Math.round((Date.parse(p.fim) - Date.parse(p.inicio)) / 86400000) + 1;
  return { inicio: addDays(p.inicio, -dias), fim: addDays(p.inicio, -1) };
}
