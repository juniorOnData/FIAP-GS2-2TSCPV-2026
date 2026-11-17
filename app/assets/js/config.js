// Configuração da publicação. Trocar DATA_ROOT para "data/gold" quando os
// exports reais da Gold estiverem publicados (mesmo contrato: app/DATA_CONTRACT.md).
export const CONFIG = {
  DATA_ROOT: "data/demo",
  TIMEZONE: "America/Sao_Paulo",
  TZ_LABEL: "horário de São Paulo (UTC−03:00)",
  // Idade máxima, em horas, antes de um dado ser marcado como desatualizado.
  STALE_HOURS: { chuva_horaria: 3, chuva_diaria: 26, nivel: 3, ocorrencias: 24 * 45, default: 24 * 7 },
  BASEMAP_STYLE: "https://basemaps.cartocdn.com/gl/positron-gl-style/style.json",
  MAP_CENTER: [-46.633, -23.62],
  MAP_ZOOM: 9.6,
  MAP_BOUNDS: [[-47.05, -24.1], [-46.2, -23.25]],
};
