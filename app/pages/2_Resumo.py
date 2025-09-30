"""Página de resumo dos pedidos enviados."""
from __future__ import annotations

from datetime import datetime

import pandas as pd
import streamlit as st

from app.components.forms import render_sidebar
from app.state import session_keys as keys
from app.utils.cache import fetch_pedidos_cached
from app.utils.time_windows import TZ


def _reset_filters() -> None:
    for key in [
        keys.RESUMO_DATE_FILTER,
        keys.RESUMO_UTD_FILTER,
        keys.RESUMO_BASE_FILTER,
        keys.RESUMO_EMAIL_FILTER,
    ]:
        st.session_state.pop(key, None)
    st.session_state[keys.RESUMO_RESET] = False


def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()

    try:
        selected_date = st.session_state.get(keys.RESUMO_DATE_FILTER)
        if isinstance(selected_date, datetime):
            result = result[result["TS_DT"].dt.date == selected_date.date()]
        elif hasattr(selected_date, "year"):
            result = result[result["TS_DT"].dt.date == selected_date]
    except Exception:
        pass

    utd_filter = st.session_state.get(keys.RESUMO_UTD_FILTER) or []
    if utd_filter:
        result = result[result["UTD"].isin(utd_filter)]

    base_filter = st.session_state.get(keys.RESUMO_BASE_FILTER) or []
    if base_filter:
        result = result[result["BASE"].isin(base_filter)]

    email_contains = (st.session_state.get(keys.RESUMO_EMAIL_FILTER) or "").strip().lower()
    if email_contains:
        result = result[
            result["E-MAIL"].astype(str).str.lower().str.contains(email_contains, na=False)
        ]

    return result


def main() -> None:
    render_sidebar(show_instructions=False)

    st.subheader("Resumo de Pedidos", divider=True)

    try:
        resumo_df = fetch_pedidos_cached()
    except Exception as exc:  # noqa: BLE001 - show message to user
        st.error(f"Erro ao carregar pedidos: {exc}")
        st.stop()

    resumo_df["TS_DT"] = pd.to_datetime(resumo_df["TIMESTAMP"], errors="coerce")
    resumo_df["DATA_HORA"] = resumo_df["TS_DT"].dt.strftime("%d/%m/%Y %H:%M")

    if st.session_state.get(keys.RESUMO_RESET, False):
        _reset_filters()

    with st.expander("Filtros", expanded=True):
        col_dt, col_utd, col_base = st.columns([1.6, 1.6, 2.3])
        with col_dt:
            ts_valid = resumo_df["TS_DT"].dropna()
            default_date = ts_valid.max().date() if not ts_valid.empty else datetime.now(TZ).date()
            st.date_input(
                "Data do pedido",
                value=st.session_state.get(keys.RESUMO_DATE_FILTER, default_date),
                key=keys.RESUMO_DATE_FILTER,
            )
        with col_utd:
            utd_opts = sorted([u for u in resumo_df["UTD"].dropna().unique().tolist() if u], key=str.casefold)
            st.multiselect("UTD", options=utd_opts, key=keys.RESUMO_UTD_FILTER)
        with col_base:
            base_opts = sorted([b for b in resumo_df["BASE"].dropna().unique().tolist() if b], key=str.casefold)
            st.multiselect("BASE", options=base_opts, key=keys.RESUMO_BASE_FILTER)

        col_email = st.columns([1.6])[0]
        with col_email:
            st.text_input(
                "E-mail (contém)",
                placeholder="filtrar por parte do e-mail",
                key=keys.RESUMO_EMAIL_FILTER,
            )
        col_reset, _ = st.columns([1, 3])
        with col_reset:
            if st.button("🧭 Limpar filtros", use_container_width=True, key="btn_reset_resumo"):
                st.session_state[keys.RESUMO_RESET] = True
                st.rerun()

    filtered = _apply_filters(resumo_df)
    filtered = filtered.sort_values("TS_DT", ascending=True)

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
    }

    filtered = filtered.rename(columns=pretty_names)
    filtered = filtered[[pretty_names[col] for col in show_cols if col in filtered.columns]]

    if filtered.empty:
        st.info("Nenhum registro encontrado para os filtros selecionados.")
    st.dataframe(filtered, use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
