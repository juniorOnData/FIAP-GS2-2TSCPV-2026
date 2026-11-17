// Metadados de navegação (sem importar as páginas, para evitar dependência circular).
// A ordem define o menu; a primeira rota é a entrada da plataforma.
export const ROUTE_META = {
  mapa: { label: "Mapa", icon: "map", group: "Painéis", desc: "Ocorrências, estações, chuva, nível e suscetibilidade em camadas." },
  indicadores: { label: "Indicadores", icon: "grid", group: "Painéis", desc: "Indicadores do recorte e estado dos dados." },
  monitoramento: { label: "Monitoramento", icon: "activity", group: "Painéis", desc: "Chuva acumulada por estação e nível dos rios." },
  alertas: { label: "Alertas", icon: "bell", group: "Painéis", desc: "Alertas da plataforma por regras sobre chuva e nível observados." },
  historico: { label: "Histórico", icon: "history", group: "Painéis", desc: "Séries de ocorrências, chuva e nível." },
  analises: { label: "Previsões e Análises", icon: "chart", group: "Painéis", desc: "Recorrência, suscetibilidade, insights e, após o gate de Data Science, previsões." },
  dados: { label: "Sobre os dados", icon: "database", group: "Conhecimento", desc: "Fontes, metodologia, qualidade e linhagem." },
  conhecimento: { label: "Base de conhecimento", icon: "book", group: "Conhecimento", desc: "Glossário, camadas e perguntas frequentes." },
  assistente: { label: "Assistente", icon: "chat", group: "Conhecimento", desc: "Pergunte sobre a plataforma e os dados." },
  perfil: { label: "Perfil de uso", icon: "user", group: "Conta", desc: "Linguagem e atalhos por tipo de uso." },
};

export const ENTRY_ROUTE = "mapa";
