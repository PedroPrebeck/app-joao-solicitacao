"""Form helpers shared across Streamlit pages."""
from __future__ import annotations

from typing import Iterable, Tuple

import streamlit as st

from app.state import session_keys as keys
from app.utils.validators import is_valid_email, is_valid_name


_SIDEBAR_LINKS: Iterable[tuple[str, str]] = (
    ("app/Home.py", "🏠 Início"),
    ("app/pages/1_Solicitar.py", "📝 Solicitar"),
    ("app/pages/2_Resumo.py", "📋 Resumo"),
    ("app/pages/3_Gestao.py", "🛠 Gestão"),
)


def _render_nav_link(target: str, label: str) -> None:
    """Render a sidebar navigation link with a graceful fallback."""

    if hasattr(st, "page_link"):
        st.page_link(target, label=label)
        return

    safe_target = target.replace("/", "_").replace(".", "_")
    button_key = f"nav_to_{safe_target}"
    if st.button(label, use_container_width=True, key=button_key) and hasattr(st, "switch_page"):
        st.switch_page(target)


def render_sidebar(*, show_instructions: bool = True) -> None:
    """Display the sidebar navigation and optional usage instructions."""

    with st.sidebar:
        st.subheader("Navegação")
        for target, label in _SIDEBAR_LINKS:
            _render_nav_link(target, label)

        st.divider()

        if show_instructions:
            st.subheader("Como usar o painel")
            st.markdown(
                """
1. **Preencha seu nome e e-mail corporativo** (@neoenergia.com).
2. **Escolha uma ou mais UTDs**.
3. **Selecione a TURMA** (EPS ou STC).
4. Para cada UTD, **escolha as BASEs** desejadas.
5. Em **Linhas por BASE**, clique em **“➕ Adicionar serviço”** para criar mais linhas se necessário.
6. No quadro, preencha:
   - **Serviço**, **Pacotes**, **Geração para** (HOJE/AMANHÃ/FIM DE SEMANA) e **Justificativa**.
   - **Comentário** (opcional).
7. Clique em **“Enviar Solicitação”**.
8. Use **“Limpar Tudo”** para zerar as seleções e a tabela.
                """
            )
            st.divider()

        st.caption(
            "Dúvidas ou falhas? João Paulo (`joao.almeida@neoenergia.com`) ou Luiz Felipe (`luiz.espozel@neoenergia.com`)."
        )
        st.divider()
        st.caption("Feito por: Pedro Azevedo (`pedro.azevedo@neoenergia.com`)")


def render_sidebar_instructions() -> None:
    """Backward compatible wrapper for the previous sidebar helper."""

    render_sidebar(show_instructions=True)


def requester_identification() -> Tuple[str, str]:
    """Render the requester identification fields and return their values."""

    col1, col2 = st.columns([1, 1])
    nome = col1.text_input(
        "Seu nome*",
        placeholder="Nome e sobrenome",
        help="Ex.: MARIA SILVA",
        key=keys.NAME_INPUT,
    )
    email = col2.text_input(
        "Seu e-mail*",
        placeholder="voce@neoenergia.com",
        help="Somente domínio @neoenergia.com",
        key=keys.EMAIL_INPUT,
    )
    return nome, email


def validate_requester(nome: str, email: str) -> None:
    """Raise a ``ValueError`` if the requester information is invalid."""

    if not is_valid_name(nome):
        raise ValueError("Informe ao menos um **Nome** e um **Sobrenome**.")
    if not is_valid_email(email):
        raise ValueError("Informe um **e-mail** válido do domínio **@neoenergia.com**.")
