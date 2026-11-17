// Alertas da plataforma: regras fixas sobre chuva e nível observados nos últimos 7 dias publicados.
// Não são alertas oficiais nem previsões (PDR, seções 3 e 11): só sinalizam leituras acima de critérios documentados.

export const REGRAS = [
  {
    id: "chuva_1h", grandeza: "chuva", titulo: "Chuva intensa em 1 hora", unidade: "mm", atencao: 25, alerta: 50,
    criterio: "Chuva registrada em uma hora: 25 mm ou mais é atenção; 50 mm ou mais é alerta.",
  },
  {
    id: "chuva_24h", grandeza: "chuva", titulo: "Chuva acumulada em 24 horas", unidade: "mm", atencao: 50, alerta: 80,
    criterio: "Soma móvel das últimas 24 horas: 50 mm ou mais é atenção; 80 mm ou mais é alerta.",
  },
  {
    id: "nivel_subida", grandeza: "nivel", titulo: "Subida rápida do nível", unidade: "m", atencao: 1.0, alerta: 2.0,
    criterio: "Nível mantido por 2 horas seguidas acima da mediana de 3 a 6 horas antes: subida de 1 m ou mais é atenção; de 2 m ou mais é alerta. Leituras isoladas fora do padrão não disparam a regra.",
  },
  {
    id: "nivel_maximo", grandeza: "nivel", titulo: "Nível acima do maior dos últimos 12 meses", unidade: "m", atencao: 0.1, alerta: null,
    criterio: "Nível da hora 0,1 m ou mais acima da maior média diária do sensor nos 12 meses anteriores à última semana: atenção.",
  },
];

// Redes cujas leituras não entram nas regras até a revisão do pipeline. Remover a entrada reativa a rede.
export const REDES_EM_REVISAO = {
  CEMADEN: "A chuva horária publicada do CEMADEN tem valores incompatíveis com as estações vizinhas (até 314 mm em 1 hora, várias vezes o registrado pelo SIBH na mesma tempestade). A agregação está em revisão no pipeline.",
};

const GAP_HORAS = 3; // horas abaixo do limiar que ainda contam como o mesmo episódio
const ATIVO_HORAS = 2; // episódio com leitura acima do limiar até 2 h antes da referência segue ativo

// O nível só entra nas regras quando a publicação confirma a conversão para metros.
export const nivelComUnidadeConfirmada = (meta) => meta?.unidade === "m" && Boolean(meta?.unidade_origem);

function somaMovel(serie, horas) {
  return serie.map((_, i) => {
    if (i < horas - 1) return null;
    let s = 0, n = 0;
    for (let j = i - horas + 1; j <= i; j++) if (serie[j] !== null) { s += serie[j]; n++; }
    return n ? s : null;
  });
}

function mediana(vals) {
  const v = [...vals].sort((a, b) => a - b);
  const m = v.length >> 1;
  return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
}

// Duas horas seguidas acima da mediana de 3 a 6 horas antes: ignora quedas ou picos de uma hora só.
const subida = (serie) =>
  serie.map((v, i) => {
    if (i < 6 || v === null || serie[i - 1] === null) return null;
    const base = serie.slice(i - 6, i - 2).filter((x) => x !== null);
    return base.length >= 2 ? Math.min(v, serie[i - 1]) - mediana(base) : null;
  });

// Avalia na precisão exibida (1 casa em mm, 2 em m), para o valor mostrado bater com a severidade.
const arredonda = (valores, casas) => valores.map((v) => (v === null ? null : Number(v.toFixed(casas))));

function excessoSobreMaximo(serie, diario) {
  const base = diario ? diario.slice(0, -8).filter((v) => v !== null) : [];
  if (base.length < 180) return null; // sem meio ano de histórico, não há referência
  const max = Math.max(...base);
  return serie.map((v) => (v === null ? null : v - max));
}

function severidade(v, regra) {
  if (v === null) return null;
  if (regra.alerta !== null && v >= regra.alerta) return "alerta";
  return v >= regra.atencao ? "atencao" : null;
}

function episodios(valores, regra) {
  const out = [];
  let atual = null;
  valores.forEach((v, i) => {
    const sev = severidade(v, regra);
    if (!sev) return;
    if (atual && i - atual.fim <= GAP_HORAS) {
      atual.fim = i;
      if (v > atual.pico) Object.assign(atual, { pico: v, iPico: i });
      if (sev === "alerta") atual.severidade = "alerta";
    } else {
      atual = { inicio: i, fim: i, pico: v, iPico: i, severidade: sev };
      out.push(atual);
    }
  });
  return out;
}

// Devolve os episódios dos últimos 7 dias, do mais recente para o mais antigo.
// estacoes: { chuva: [], nivel: [] }; horaria e nivel: resultados de data.load(); tempos: ISO por índice.
export function detectarAlertas({ estacoes, horaria, temposChuva, nivel, temposNivel }) {
  const alertas = [];
  const add = (regra, local, brutos, tempos) => {
    const valores = arredonda(brutos, regra.unidade === "mm" ? 1 : 2);
    const n = valores.length;
    for (const e of episodios(valores, regra)) {
      alertas.push({
        id: `${regra.id}|${local.id}|${tempos[e.inicio]}`,
        regra, local, valores, tempos, ...e,
        inicioEm: tempos[e.inicio], fimEm: tempos[e.fim], picoEm: tempos[e.iPico],
        ativo: e.fim >= n - 1 - ATIVO_HORAS,
      });
    }
  };
  if (horaria?.status === "ok") {
    for (const s of estacoes.chuva) {
      if (REDES_EM_REVISAO[s.rede]) continue;
      const serie = horaria.data.series[s.id];
      if (!serie) continue;
      add(REGRAS[0], s, serie, temposChuva);
      add(REGRAS[1], s, somaMovel(serie, 24), temposChuva);
    }
  }
  if (nivel?.status === "ok" && nivelComUnidadeConfirmada(nivel.meta)) {
    for (const s of estacoes.nivel) {
      const serie = nivel.data.horario[s.id];
      if (!serie) continue;
      add(REGRAS[2], s, subida(serie), temposNivel);
      const exc = excessoSobreMaximo(serie, nivel.data.diario[s.id]);
      if (exc) add(REGRAS[3], s, exc, temposNivel);
    }
  }
  return alertas.sort((a, b) => b.fimEm.localeCompare(a.fimEm) || (b.severidade === "alerta") - (a.severidade === "alerta"));
}
