// Configuração da publicação. O app usa a primeira pasta de DATA_ROOTS que tiver um manifest.json:
// os exports reais da Gold (data/gold, publicados pelo job do Databricks) e, se ainda não existirem,
// o pacote sintético de demonstração (sempre sinalizado na interface). Mesmo contrato: app/DATA_CONTRACT.md.
export const CONFIG = {
  DATA_ROOTS: ["data/gold", "data/demo"],
  DATA_ROOT: "data/gold", // definido em loadManifest() com a pasta efetivamente usada
  TIMEZONE: "America/Sao_Paulo",
  TZ_LABEL: "horário de São Paulo (UTC−03:00)",
  // Idade máxima, em horas, antes de um dado ser marcado como desatualizado.
  // O job publica a cada 2 horas; somando a duração da execução e o atraso das fontes, 4 h ainda é o ritmo normal.
  STALE_HOURS: { chuva_horaria: 4, chuva_diaria: 26, nivel: 4, ocorrencias: 24 * 45, default: 24 * 7 },
  BASEMAP_STYLE: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  MAP_CENTER: [-46.633, -23.62],
  MAP_ZOOM: 9.6,
  MAP_BOUNDS: [[-47.05, -24.1], [-46.2, -23.25]],
};
