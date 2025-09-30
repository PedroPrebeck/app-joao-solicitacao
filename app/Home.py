"""Streamlit entrypoint that configures the application shell."""
from __future__ import annotations

import streamlit as st

from app.components.forms import render_sidebar
from app.state import session_keys as keys
from app.utils.cache import load_dag40_cached
from app.utils.time_windows import current_time_window


def _handle_full_reset() -> None:
    if st.session_state.get(keys.FULL_RESET_FLAG, False):
        st.session_state.clear()
        st.session_state[keys.FULL_RESET_FLAG] = False
        st.rerun()


def main() -> None:
    st.set_page_config(
        page_title="Geração de Notas de Cobrança - Painel de Solicitações",
        page_icon="🧾",
        layout="wide",
    )

    _handle_full_reset()

    st.title("Geração de Notas de Cobrança - Painel de Solicitações")
    st.caption("Solicite a geração de notas por UTD, TURMA e BASE, com validações operacionais de horário.")

    window = current_time_window()
    if window.after_10:
        st.info(
            "⚠️ **Após 10:30 só serão aceitos pedidos para amanhã ou fim de semana. Pedidos para a cadeia noturna só serão aceitos até às 14:30**",
            icon="🕙",
        )

    render_sidebar()

    # Prime the DAG40 cache so the pages load quickly when the user navigates.
    try:
        load_dag40_cached()
    except Exception:
        st.warning("Não foi possível carregar o catálogo DAG40 automaticamente. Verifique as credenciais do HANA.")

    st.divider()
    st.markdown(
        "Escolha uma das páginas na barra lateral para **solicitar**, **acompanhar** ou **gerenciar** os pedidos."
    )


if __name__ == "__main__":
    main()
