from __future__ import annotations

from datetime import datetime, date

import pandas as pd
import streamlit as st

from app.services.pedidos_cache import fetch_all_pedidos_cached
from app.settings import TZ


def page() -> None:
    st.header("📋 Resumo de Pedidos", divider=True)

    try:
        resumo_df = fetch_all_pedidos_cached()
    except Exception as exc:  # pragma: no cover - Streamlit feedback
        st.error(f"Erro ao carregar pedidos: {exc}")
        st.stop()

    resumo_df = resumo_df.copy()
    resumo_df["TS_DT"] = pd.to_datetime(resumo_df["TIMESTAMP"], errors="coerce")
    resumo_df["DATA_HORA"] = resumo_df["TS_DT"].dt.strftime("%d/%m/%Y %H:%M")

    if st.session_state.get("reset_filters_resumo", False):
        for key in ["f_data_resumo", "f_utd_resumo", "f_base_resumo", "f_email_resumo"]:
            st.session_state.pop(key, None)
        st.session_state["reset_filters_resumo"] = False

    with st.expander("Filtros", expanded=True):
        row1c1, row1c2, row1c3 = st.columns([1.6, 1.6, 2.3])
        with row1c1:
            ts_valid = resumo_df["TS_DT"].dropna()
            default_date = ts_valid.max().date() if not ts_valid.empty else datetime.now(TZ).date()
            st.date_input(
                "Data do pedido",
                value=st.session_state.get("f_data_resumo", default_date),
                key="f_data_resumo",
            )
        with row1c2:
            utd_opts = sorted([u for u in resumo_df["UTD"].dropna().unique().tolist() if u], key=str.casefold)
            st.multiselect("UTD", options=utd_opts, key="f_utd_resumo")
        with row1c3:
            base_opts = sorted([b for b in resumo_df["BASE"].dropna().unique().tolist() if b], key=str.casefold)
            st.multiselect("BASE", options=base_opts, key="f_base_resumo")
        row2c1 = st.columns([1.6])[0]
        with row2c1:
            st.text_input("E-mail (contém)", placeholder="filtrar por parte do e-mail", key="f_email_resumo")
        reset_col, _ = st.columns([1, 3])
        with reset_col:
            if st.button("🧭 Limpar filtros", use_container_width=True, key="btn_reset_resumo"):
                st.session_state["reset_filters_resumo"] = True
                st.rerun()

    resumo_f = resumo_df.copy()
    try:
        selected_date = st.session_state.get("f_data_resumo")
        if isinstance(selected_date, date):
            resumo_f = resumo_f[resumo_f["TS_DT"].dt.date == selected_date]
    except Exception:
        pass
    if st.session_state.get("f_utd_resumo"):
        resumo_f = resumo_f[resumo_f["UTD"].isin(st.session_state["f_utd_resumo"])]
    if st.session_state.get("f_base_resumo"):
        resumo_f = resumo_f[resumo_f["BASE"].isin(st.session_state["f_base_resumo"])]
    email_contains = (st.session_state.get("f_email_resumo", "") or "").strip().lower()
    if email_contains:
        resumo_f = resumo_f[
            resumo_f["E-MAIL"].astype(str).str.lower().str.contains(email_contains, na=False)
        ]

    resumo_f = resumo_f.sort_values("TS_DT", ascending=True)

    show_cols = [
        "DATA_HORA",
        "NOME",
        "E-MAIL",
        "UTD",
        "BASE",
        "TURMA",
        "SERVICO",
        "PACOTES",
        "CADEIA",
        "JUSTIFICATIVA",
        "COMENTARIOS",
        "STATUS_LABEL",
    ]
    pretty_names = {
        "DATA_HORA": "Data e hora",
        "NOME": "Nome",
        "E-MAIL": "E-mail",
        "UTD": "UTD",
        "BASE": "BASE",
        "TURMA": "TURMA",
        "SERVICO": "Serviço",
        "PACOTES": "Pacotes",
        "CADEIA": "Geração para",
        "JUSTIFICATIVA": "Justificativa",
        "COMENTARIOS": "Comentário",
        "STATUS_LABEL": "Status",
    }

    if resumo_f.empty:
        st.info("Nenhum registro encontrado para os filtros selecionados.")
        return

    st.dataframe(
        resumo_f[show_cols].rename(columns=pretty_names),
        use_container_width=True,
        hide_index=True,
    )


if __name__ == "__main__":  # pragma: no cover - executed via Streamlit
    page()
