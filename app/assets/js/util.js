import { CONFIG } from "./config.js";

const ESC = { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" };
export const esc = (v) => (v === null || v === undefined ? "" : String(v).replace(/[&<>"']/g, (c) => ESC[c]));

export const $ = (sel, root = document) => root.querySelector(sel);
export const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

const nf0 = new Intl.NumberFormat("pt-BR", { maximumFractionDigits: 0 });
const nf1 = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 1, maximumFractionDigits: 1 });
const nf2 = new Intl.NumberFormat("pt-BR", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
export const fmtInt = (v) => (v === null || v === undefined || Number.isNaN(v) ? "—" : nf0.format(v));
export const fmt1 = (v) => (v === null || v === undefined || Number.isNaN(v) ? "—" : nf1.format(v));
export const fmt2 = (v) => (v === null || v === undefined || Number.isNaN(v) ? "—" : nf2.format(v));
export const fmtPct = (v, d = 1) =>
  v === null || v === undefined || Number.isNaN(v) ? "—" : `${new Intl.NumberFormat("pt-BR", { maximumFractionDigits: d, minimumFractionDigits: d }).format(v * 100)}%`;
export const fmtCompact = (v) => {
  if (v === null || v === undefined) return "—";
  if (Math.abs(v) >= 10000) return new Intl.NumberFormat("pt-BR", { notation: "compact", maximumFractionDigits: 1 }).format(v);
  return nf0.format(v);
};

const MESES = ["jan", "fev", "mar", "abr", "mai", "jun", "jul", "ago", "set", "out", "nov", "dez"];
export const MESES_LONGOS = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho", "agosto", "setembro", "outubro", "novembro", "dezembro"];

// Datas "AAAA-MM-DD" são tratadas como dias civis de São Paulo (sem hora).
export const parseDay = (s) => {
  const [y, m, d] = s.split("-").map(Number);
  return new Date(Date.UTC(y, m - 1, d));
};
export const dayStr = (dt) => dt.toISOString().slice(0, 10);
export const addDays = (s, n) => {
  const d = parseDay(s);
  d.setUTCDate(d.getUTCDate() + n);
  return dayStr(d);
};
export const diffDays = (a, b) => Math.round((parseDay(b) - parseDay(a)) / 86400000);
export const fmtDay = (s) => {
  if (!s) return "—";
  const [y, m, d] = s.split("-");
  return `${d}/${m}/${y}`;
};
export const fmtMonth = (ym) => {
  const [y, m] = ym.split("-");
  return `${MESES[Number(m) - 1]}/${y.slice(2)}`;
};
export const fmtMonthLong = (ym) => {
  const [y, m] = ym.split("-");
  return `${MESES_LONGOS[Number(m) - 1]} de ${y}`;
};

const dtf = new Intl.DateTimeFormat("pt-BR", {
  timeZone: CONFIG.TIMEZONE, day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit",
});
const hf = new Intl.DateTimeFormat("pt-BR", { timeZone: CONFIG.TIMEZONE, day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit" });
export const fmtDateTime = (iso) => (iso ? dtf.format(new Date(iso)).replace(",", " às") : "—");
export const fmtHourShort = (iso) => hf.format(new Date(iso)).replace(",", "");

export const fmtAge = (iso, now = new Date()) => {
  if (!iso) return "—";
  const min = Math.round((now - new Date(iso)) / 60000);
  if (min < 1) return "agora";
  if (min < 60) return `há ${min} min`;
  const h = Math.round(min / 60);
  if (h < 48) return `há ${h} h`;
  return `há ${Math.round(h / 24)} dias`;
};

export const sum = (arr) => arr.reduce((a, b) => a + (b ?? 0), 0);
export const mean = (arr) => {
  const v = arr.filter((x) => x !== null && x !== undefined);
  return v.length ? sum(v) / v.length : null;
};
export const groupCount = (arr) => {
  const m = new Map();
  for (const k of arr) m.set(k, (m.get(k) || 0) + 1);
  return m;
};
export const clamp = (v, a, b) => Math.min(b, Math.max(a, v));
export const debounce = (fn, ms = 150) => {
  let t;
  return (...a) => {
    clearTimeout(t);
    t = setTimeout(() => fn(...a), ms);
  };
};

export const storage = {
  get(key, fallback) {
    try {
      const v = localStorage.getItem(key);
      return v === null ? fallback : JSON.parse(v);
    } catch {
      return fallback;
    }
  },
  set(key, value) {
    try {
      localStorage.setItem(key, JSON.stringify(value));
    } catch {
      /* armazenamento indisponível: a preferência vale só nesta sessão */
    }
  },
};

export const slug = (s) =>
  s.normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase().replace(/[^a-z0-9]+/g, "-").replace(/(^-|-$)/g, "");
export const norm = (s) => s.normalize("NFD").replace(/[̀-ͯ]/g, "").toLowerCase();

export function downloadText(filename, text, type = "text/csv;charset=utf-8") {
  const blob = new Blob(["﻿" + text], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  setTimeout(() => URL.revokeObjectURL(url), 1000);
}
