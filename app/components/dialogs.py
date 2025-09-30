"""Reusable dialog components."""
from __future__ import annotations

import pandas as pd
import streamlit as st

from app.state import session_keys as keys


@st.dialog("Sua solicitação foi enviada com sucesso", width="large")
def show_submission_success() -> None:
    """Render the success dialog after a pedido submission."""

    qtd = st.session_state.get(keys.SUCCESS_QUANTITY, 0)
    nome = st.session_state.get(keys.SUCCESS_NAME, "")
    email = st.session_state.get(keys.SUCCESS_EMAIL, "")
    resumo_df = st.session_state.get(keys.SUCCESS_RESUMO)

    st.success(f"✅ Solicitações enviadas com sucesso ({qtd} linha(s)).")
    if nome or email:
        st.caption(f"**Solicitante:** {nome} • {email}")

    if isinstance(resumo_df, pd.DataFrame) and not resumo_df.empty:
        resumo_show = resumo_df.rename(
            columns={
                "UTD": "UTD",
                "BASE": "BASE",
                "TURMA": "TURMA",
                "GERACAO_PARA": "Geração para",
                "SERVIÇO": "Serviço",
                "PACOTES": "Pacotes",
                "JUSTIFICATIVA": "Justificativa",
                "COMENTARIO": "Comentário",
            }
        )
        st.dataframe(resumo_show, use_container_width=True, hide_index=True)
    else:
        st.info("Não foi possível montar o resumo da solicitação.")

    st.divider()
    if st.button("➕ Fazer outro pedido", use_container_width=True):
        st.session_state[keys.FULL_RESET_FLAG] = True
        st.rerun()
