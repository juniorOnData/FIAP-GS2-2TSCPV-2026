// Perfis de uso (F16): só ajustam linguagem e atalhos — não são controle de acesso.
import { icon } from "../icons.js";
import { esc } from "../util.js";
import { store, PERFIS } from "../store.js";
import { pageHead, stateBlock, statusBadge, badge } from "../ui.js";
import { ROUTE_META } from "../routes.js";

export default {
  subtitle: "Experiência ajustada ao tipo de uso",
  async render(root) {
    const atual = store.perfil.id;
    root.innerHTML = `<div class="page">
      ${pageHead({ eyebrow: "Perfil de uso", title: "Como você usa o ChuvaSegura-SP?", lead: "O perfil muda os atalhos dos indicadores e as perguntas sugeridas pelo assistente." })}
      ${stateBlock("info", "Sem login nem controle de acesso nesta versão", "Todos os perfis veem os mesmos dados. A escolha fica salva só neste navegador (quando o armazenamento local está disponível) e não é enviada a nenhum servidor.")}
      <fieldset class="profiles">
        <legend class="sr-only">Escolha o perfil</legend>
        ${Object.values(PERFIS).map((p) => `<label class="card profile ${p.id === atual ? "is-active" : ""}">
          <input type="radio" name="perfil" value="${p.id}" ${p.id === atual ? "checked" : ""}>
          <span class="profile__body"><strong>${esc(p.nome)}</strong><span>${esc(p.descricao)}</span>
          <span class="pill-list">${p.atalhos.map((a) => badge("neutral", ROUTE_META[a].label)).join(" ")}</span></span>
        </label>`).join("")}
      </fieldset>
      <h3 class="section-title">Biblioteca de componentes</h3>
      <p class="muted">Referência do Design System v1.0 aplicada na interface.</p>
      <div class="card"><div class="card__body stack">
        <div class="row"><button class="btn btn--primary" type="button">Ação principal</button><button class="btn btn--secondary" type="button">Ação secundária</button><button class="btn btn--ghost" type="button">Ação textual</button></div>
        <div class="row">${statusBadge("normal")} ${statusBadge("atencao")} ${statusBadge("alerta")} ${statusBadge("critico")} <span class="note">Badges semânticos: só com critério documentado, sempre com texto e ícone. Nenhum dado desta versão tem critério oficial, por isso não aparecem nos painéis.</span></div>
        <div class="row">${badge("ok", "Atualizado", "check")} ${badge("stale", "Desatualizado", "clock")} ${badge("error", "Falha ao carregar", "alert")} ${badge("demo", "Demonstração", "flask")} ${badge("neutral", "Registro histórico", "history")}</div>
      </div></div>
    </div>`;
    root.querySelectorAll('input[name="perfil"]').forEach((r) => r.addEventListener("change", () => store.setPerfil(r.value)));
  },
};
