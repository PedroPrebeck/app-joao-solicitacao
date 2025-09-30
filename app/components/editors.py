"""Reusable editor components."""
from __future__ import annotations

from typing import Iterable, Mapping

import pandas as pd
import streamlit as st

from app.state import session_keys as keys

COLUMNS_ALL = [
    "UTD",
    "BASE",
    "TURMA",
    "GERACAO_PARA",
    "SERVIÇO",
    "PACOTES",
    "JUSTIFICATIVA",
    "COMENTARIO",
    "ZONA",
]
COLUMNS_SHOW = [
    "UTD",
    "BASE",
    "TURMA",
    "GERACAO_PARA",
    "SERVIÇO",
    "PACOTES",
    "JUSTIFICATIVA",
    "COMENTARIO",
]


def _empty_request_df() -> pd.DataFrame:
    df = pd.DataFrame(columns=COLUMNS_ALL)
    df["PACOTES"] = pd.Series(dtype="int")
    for column in COLUMNS_ALL:
        if column != "PACOTES":
            df[column] = pd.Series(dtype="string")
    return df


def ensure_request_dataframe() -> None:
    """Ensure the session state contains the base dataframe for the editor."""

    if keys.REQUEST_LINES not in st.session_state:
        st.session_state[keys.REQUEST_LINES] = _empty_request_df()


def request_lines_editor(
    dag40_df: pd.DataFrame,
    *,
    utds_sel: Iterable[str],
    turma_sel: str | None,
    geracao_options: list[str],
    geracao_default: str,
    servicos_opcoes: list[str],
) -> None:
    """Render the editable table for request lines."""

    ensure_request_dataframe()
    if not utds_sel or not turma_sel:
        st.info("Selecione UTD(s), a TURMA e ao menos uma BASE para cada UTD.")
        return

    base_selection: Mapping[str, list[str]] = st.session_state.get(keys.UTD_BASE_SELECTION, {})
    if not any(base_selection.values()):
        st.info("Selecione UTD(s), a TURMA e ao menos uma BASE para cada UTD.")
        return

    def _zona_for(utd: str, base: str, turma: str) -> str:
        subset = dag40_df[(dag40_df["UTD"] == utd) & (dag40_df["BASE"] == base) & (dag40_df["TURMA"] == turma)]
        return subset["ZONA"].iloc[0] if not subset.empty else ""

    def _ensure_rows_for_selected_pairs() -> None:
        df = st.session_state[keys.REQUEST_LINES].copy()
        wanted_keys = set()
        rows_to_add = []
        for utd, bases in base_selection.items():
            for base in bases:
                key = f"{utd}\n{base}\n{turma_sel}"
                wanted_keys.add(key)
                exists = df[(df["UTD"] == utd) & (df["BASE"] == base) & (df["TURMA"] == turma_sel)]
                if exists.empty:
                    zona = _zona_for(utd, base, turma_sel)
                    rows_to_add.append(
                        {
                            "UTD": utd,
                            "BASE": base,
                            "TURMA": turma_sel,
                            "GERACAO_PARA": geracao_default,
                            "SERVIÇO": "",
                            "PACOTES": 1,
                            "JUSTIFICATIVA": "",
                            "COMENTARIO": "",
                            "ZONA": zona,
                        }
                    )
        if not df.empty:
            df["KEY"] = df["UTD"].astype(str) + "\n" + df["BASE"].astype(str) + "\n" + df["TURMA"].astype(str)
            df = df[df["KEY"].isin(wanted_keys)].drop(columns=["KEY"])
        if rows_to_add:
            df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)
        st.session_state[keys.REQUEST_LINES] = df

    def _add_service_row_for_base(utd: str, base: str, turma: str, zona: str) -> None:
        df = st.session_state[keys.REQUEST_LINES].copy()
        new_row = {
            "UTD": utd,
            "BASE": base,
            "TURMA": turma,
            "GERACAO_PARA": geracao_default,
            "SERVIÇO": "",
            "PACOTES": 1,
            "JUSTIFICATIVA": "",
            "COMENTARIO": "",
            "ZONA": zona,
        }
        st.session_state[keys.REQUEST_LINES] = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
        if keys.REQUEST_EDITOR_KEY in st.session_state:
            del st.session_state[keys.REQUEST_EDITOR_KEY]

    _ensure_rows_for_selected_pairs()

    st.subheader("Linhas por BASE", divider=True)
    with st.container(border=True):
        cols = st.columns(3)
        i = 0
        for utd, bases in base_selection.items():
            for base in bases:
                zona_val = _zona_for(utd, base, turma_sel)
                if cols[i % 3].button(
                    f"➕ Adicionar serviço • {base} (UTD {utd}, {turma_sel})",
                    key=f"add_{utd}_{base}",
                    use_container_width=True,
                ):
                    _add_service_row_for_base(utd, base, turma_sel, zona_val)
                i += 1

    def _apply_editor_changes() -> None:
        ed_state = st.session_state.get(keys.REQUEST_EDITOR_KEY, {})
        df = st.session_state[keys.REQUEST_LINES].copy()
        deleted = ed_state.get("deleted_rows", [])
        if deleted:
            df = df.drop(df.index[deleted]).reset_index(drop=True)
        for row_idx, changes in ed_state.get("edited_rows", {}).items():
            for col, val in changes.items():
                if col in df.columns and col in COLUMNS_SHOW:
                    df.at[row_idx, col] = val
        for new in ed_state.get("added_rows", []):
            base_row = {column: "" for column in COLUMNS_ALL}
            base_row.update({
                "PACOTES": 1,
                "GERACAO_PARA": geracao_default,
            })
            base_row.update({k: v for k, v in new.items() if k in COLUMNS_ALL})
            df = pd.concat([df, pd.DataFrame([base_row])], ignore_index=True)
        df["PACOTES"] = pd.to_numeric(df["PACOTES"], errors="coerce").fillna(0).astype(int)
        st.session_state[keys.REQUEST_LINES] = df

    editor_df = st.session_state[keys.REQUEST_LINES][COLUMNS_SHOW].copy()
    editor_df["PACOTES"] = pd.to_numeric(editor_df["PACOTES"], errors="coerce").fillna(0).astype(int)

    st.data_editor(
        editor_df,
        key=keys.REQUEST_EDITOR_KEY,
        on_change=_apply_editor_changes,
        hide_index=True,
        num_rows="dynamic",
        column_order=COLUMNS_SHOW,
        column_config={
            "UTD": st.column_config.TextColumn("UTD", disabled=True),
            "BASE": st.column_config.TextColumn("BASE", disabled=True),
            "TURMA": st.column_config.TextColumn("TURMA", disabled=True),
            "GERACAO_PARA": st.column_config.SelectboxColumn(
                "Geração para",
                options=geracao_options,
                required=True,
            ),
            "SERVIÇO": st.column_config.SelectboxColumn(
                "Serviço",
                options=servicos_opcoes,
                required=True,
                help="• Corte Gavião ⇒ Disjuntor • Visita de Cobrança ⇒ COB.DOM",
            ),
            "PACOTES": st.column_config.NumberColumn(
                "Pacotes",
                min_value=1,
                step=1,
                required=True,
            ),
            "JUSTIFICATIVA": st.column_config.TextColumn("Justificativa", required=True),
            "COMENTARIO": st.column_config.TextColumn("Comentário"),
        },
    )
