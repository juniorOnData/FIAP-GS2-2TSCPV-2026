// Agregação de pontos em hexágonos regulares (raio em metros) para o zoom afastado do mapa.
// Usa a escala local de São Paulo (metros por grau), suficiente para contar pontos por célula.
const KX = 102050;
const KY = 110780;
const R3 = Math.sqrt(3);

// Raio do hexágono por faixa de zoom; a partir de ZOOM_PONTOS o mapa mostra cada ocorrência.
export const ZOOM_PONTOS = 14;
export function raioPorZoom(zoom) {
  if (zoom < 10.5) return 1200;
  if (zoom < 11.5) return 700;
  if (zoom < 12.5) return 400;
  return 220;
}

function arredondar(qf, rf) {
  const sf = -qf - rf;
  let q = Math.round(qf), r = Math.round(rf);
  const s = Math.round(sf);
  const dq = Math.abs(q - qf), dr = Math.abs(r - rf), ds = Math.abs(s - sf);
  if (dq > dr && dq > ds) q = -r - s;
  else if (dr > ds) r = -q - s;
  return [q, r];
}

function anel(q, r, raio) {
  const cx = raio * R3 * (q + r / 2);
  const cy = raio * 1.5 * r;
  const pts = [];
  for (let i = 0; i <= 6; i++) {
    const a = (Math.PI / 180) * (60 * (i % 6) - 30);
    pts.push([(cx + raio * Math.cos(a)) / KX, (cy + raio * Math.sin(a)) / KY]);
  }
  return { pts, centro: [cx / KX, cy / KY] };
}

// points: [{ lon, lat, tipo }]. Devolve as células ocupadas: { id, n, tipos: {tipo: n}, anel, centro }.
export function hexbin(points, raio) {
  const cells = new Map();
  for (const p of points) {
    const x = p.lon * KX, y = p.lat * KY;
    const [q, r] = arredondar((R3 / 3 * x - y / 3) / raio, (2 / 3 * y) / raio);
    const id = `${q},${r}`;
    let c = cells.get(id);
    if (!c) cells.set(id, (c = { id, q, r, n: 0, tipos: {} }));
    c.n++;
    c.tipos[p.tipo] = (c.tipos[p.tipo] || 0) + 1;
  }
  return [...cells.values()].map((c) => {
    const { pts, centro } = anel(c.q, c.r, raio);
    return { id: c.id, n: c.n, tipos: c.tipos, anel: pts, centro };
  });
}

// Quebras por quantis das contagens (até `classes` faixas, sem repetir limites).
export function quebras(valores, classes = 5) {
  const v = [...valores].sort((a, b) => a - b);
  if (!v.length) return [];
  const out = [];
  for (let i = 1; i < classes; i++) {
    const q = v[Math.min(v.length - 1, Math.floor((i / classes) * v.length))];
    if (q > (out.at(-1) ?? 1)) out.push(q);
  }
  return out;
}
