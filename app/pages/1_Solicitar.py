"""Página responsável pela criação de novos pedidos."""
from __future__ import annotations

from typing import Dict, List

import pandas as pd
import streamlit as st
from hdbcli import dbapi

from app.components.dialogs import show_submission_success
from app.components.editors import request_lines_editor
from app.components.forms import render_sidebar, requester_identification, validate_requester
from app.services.hana import HanaConfig
from app.services.pedidos_service import insert_pedidos_rows
from app.services.pedidos_submission import prepare_submission_dataframe
from app.state import session_keys as keys
from app.utils.cache import load_dag40_cached
from app.utils.constants import DEFAULT_SERVICOS
from app.utils.time_windows import current_time_window
from app.utils.validators import strip_accents_and_punct_name


def _base_options(dag40_df: pd.DataFrame, utd: str, turma: str) -> List[str]:
    subset = dag40_df[(dag40_df["UTD"] == utd) & (dag40_df["TURMA"] == turma)]["BASE"]
    options = [opt for opt in subset.dropna().unique().tolist() if opt]
    return sorted(options, key=str.casefold)


def _render_base_selection(
    dag40_df: pd.DataFrame,
    *,
    utds_sel: List[str],
    turma_sel: str | None,
) -> Dict[str, List[str]]:
    st.markdown("#### BASEs por UTD")
    cols = st.columns(2)
    base_selection: Dict[str, List[str]] = st.session_state.setdefault(keys.UTD_BASE_SELECTION, {})

    for utd_key in list(base_selection.keys()):
        if utd_key not in utds_sel:
            del base_selection[utd_key]

    if not (utds_sel and turma_sel):
        st.session_state[keys.UTD_BASE_SELECTION] = {}
        return {}

    base_options_by_utd = {utd: _base_options(dag40_df, utd, turma_sel) for utd in utds_sel}

    for i, utd in enumerate(utds_sel):
        with cols[i % 2]:
            current = [
                base
                for base in base_selection.get(utd, [])
                if base in base_options_by_utd.get(utd, [])
            ]
            new_selection = st.multiselect(
                f"BASE(s) para **{utd}** ({turma_sel})",
                options=base_options_by_utd.get(utd, []),
                default=current,
                key=f"bases_for_{utd}",
                help="Selecione apenas as BASEs desta UTD.",
            )
            base_selection[utd] = new_selection

    st.session_state[keys.UTD_BASE_SELECTION] = base_selection
    return base_selection


def main() -> None:
    render_sidebar(show_instructions=True)

    window = current_time_window()
    dag40_df = load_dag40_cached()

    servicos_opcoes = DEFAULT_SERVICOS

    nome_input, email_input = requester_identification()

    utd_options = sorted([u for u in dag40_df["UTD"].dropna().unique().tolist() if u], key=str.casefold)
    utds_sel = st.multiselect(
        "UTDs*",
        options=utd_options,
        placeholder="Escolha uma ou mais UTDs",
        key=keys.UTD_SELECTION,
    )
    turma_sel = st.radio(
        "TURMA*",
        options=["STC", "EPS"],
        horizontal=True,
        disabled=not utds_sel,
        key=keys.TURMA_SELECTION,
    )

    _render_base_selection(dag40_df, utds_sel=utds_sel, turma_sel=turma_sel)

    request_lines_editor(
        dag40_df,
        utds_sel=utds_sel,
        turma_sel=turma_sel,
        geracao_options=window.available_options,
        geracao_default=window.default_option,
        servicos_opcoes=servicos_opcoes,
    )

    st.divider()
    col_send, col_clear = st.columns([1, 1])

    can_send = bool(strip_accents_and_punct_name(nome_input)) and bool(email_input.strip())
    lines_df = st.session_state.get(keys.REQUEST_LINES, pd.DataFrame())
    can_send = can_send and not lines_df.empty

    if col_send.button(
        "📨 Enviar Solicitação",
        type="primary",
        use_container_width=True,
        disabled=not can_send,
    ):
        try:
            validate_requester(nome_input, email_input)
            out_df = prepare_submission_dataframe(
                lines_df,
                nome=nome_input,
                email=email_input,
                turma=turma_sel or "",
                after_1055=window.after_1055,
            )
            cfg = HanaConfig.from_env()
            inserted = insert_pedidos_rows(out_df, connector=dbapi.connect, config=cfg)
            resumo_cols = [
                "UTD",
                "BASE",
                "TURMA",
                "GERACAO_PARA",
                "SERVIÇO",
                "PACOTES",
                "JUSTIFICATIVA",
                "COMENTARIO",
            ]
            resumo_df = lines_df[resumo_cols].copy()
            resumo_df["PACOTES"] = pd.to_numeric(resumo_df["PACOTES"], errors="coerce").fillna(0).astype(int)

            st.session_state[keys.SUCCESS_QUANTITY] = inserted
            st.session_state[keys.SUCCESS_NAME] = strip_accents_and_punct_name(nome_input)
            st.session_state[keys.SUCCESS_EMAIL] = email_input.strip().lower()
            st.session_state[keys.SUCCESS_RESUMO] = resumo_df

            st.cache_data.clear()
            show_submission_success()
        except Exception as exc:  # noqa: BLE001 - show message to user
            st.error(f"Falha ao enviar: {exc}")

    if col_clear.button("🧹 Limpar Tudo", use_container_width=True):
        st.session_state[keys.FULL_RESET_FLAG] = True
        st.rerun()


if __name__ == "__main__":
    main()
