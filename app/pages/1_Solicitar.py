from __future__ import annotations

from datetime import datetime, time as dtime

import pandas as pd
import streamlit as st

from app.services.pedidos_cache import clear_pedidos_cache
from app.services.pedidos_service import insert_pedidos_default
from app.settings import TZ
from app.state.data import get_dag40_dataframe
from app.state.session import trigger_full_reset
from app.utils.constants import BASE_GERACAO_OPCOES, DEFAULT_SERVICOS
from app.utils.validators import (
    is_valid_email,
    is_valid_name,
    strip_accents_and_punct_action,
    strip_accents_and_punct_name,
)


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
EDITOR_KEY = "editor_lines_v2"


def _empty_df() -> pd.DataFrame:
    df = pd.DataFrame(columns=COLUMNS_ALL)
    df["PACOTES"] = pd.Series(dtype="int")
    for column in [c for c in COLUMNS_ALL if c != "PACOTES"]:
        df[column] = pd.Series(dtype="string")
    return df


def _clear_editor_state() -> None:
    if EDITOR_KEY in st.session_state:
        del st.session_state[EDITOR_KEY]


@st.dialog("Sua solicitação foi enviada com sucesso", width="large")
def _show_success_dialog() -> None:
    qtd = st.session_state.get("success_qtd", 0)
    nome = st.session_state.get("success_nome", "")
    email = st.session_state.get("success_email", "")
    resumo_df = st.session_state.get("success_resumo")
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
        trigger_full_reset()
        st.rerun()


def _zona_for(dag40_df: pd.DataFrame, utd: str, base: str, turma: str) -> str:
    subset = dag40_df[
        (dag40_df["UTD"] == utd)
        & (dag40_df["BASE"] == base)
        & (dag40_df["TURMA"] == turma)
    ]
    return subset["ZONA"].iloc[0] if not subset.empty else ""


def page() -> None:
    st.header("🧾 Solicitar Geração", divider=True)

    dag40_df = get_dag40_dataframe()

    now = datetime.now(TZ)
    after_10h00 = now.time() >= dtime(10, 0)
    after_10h55 = now.time() >= dtime(10, 55)

    geracao_opcoes = [o for o in BASE_GERACAO_OPCOES if not (after_10h55 and o == "HOJE")]
    geracao_default = geracao_opcoes[0] if geracao_opcoes else "AMANHÃ"

    if after_10h00:
        st.info(
            "⚠️ **Após 10:30 só serão aceitos pedidos para amanhã ou fim de semana. Pedidos para a cadeia noturna só serão aceitos até às 14:30**",
            icon="🕙",
        )

    with st.container():
        col1, col2 = st.columns([1, 1])
        nome_input = col1.text_input("Seu nome*", placeholder="Nome e sobrenome", help="Ex.: MARIA SILVA")
        email_input = col2.text_input(
            "Seu e-mail*",
            placeholder="voce@neoenergia.com",
            help="Somente domínio @neoenergia.com",
        )

    st.subheader("Seleção de UTD, TURMA e BASE", divider=True)
    utd_options = sorted([u for u in dag40_df["UTD"].dropna().unique().tolist() if u], key=str.casefold)
    utds_sel = st.multiselect(
        "UTDs*",
        options=utd_options,
        placeholder="Escolha uma ou mais UTDs",
        key="utds_sel",
    )
    turma_sel = st.radio("TURMA*", options=["STC", "EPS"], horizontal=True, disabled=not utds_sel, key="turma_sel")

    st.session_state.setdefault("utd_base_sel", {})
    for utd_key in list(st.session_state["utd_base_sel"].keys()):
        if utd_key not in utds_sel:
            del st.session_state["utd_base_sel"][utd_key]

    base_options_by_utd: dict[str, list[str]] = {}
    if utds_sel and turma_sel:
        for utd in utds_sel:
            opts = (
                dag40_df[(dag40_df["UTD"] == utd) & (dag40_df["TURMA"] == turma_sel)]["BASE"].dropna().unique().tolist()
            )
            base_options_by_utd[utd] = sorted([o for o in opts if o], key=str.casefold)

    st.markdown("#### BASEs por UTD")
    cols = st.columns(2)
    for i, utd in enumerate(utds_sel):
        with cols[i % 2]:
            current = [
                b
                for b in st.session_state["utd_base_sel"].get(utd, [])
                if b in base_options_by_utd.get(utd, [])
            ]
            new_sel = st.multiselect(
                f"BASE(s) para **{utd}** ({turma_sel})",
                options=base_options_by_utd.get(utd, []),
                default=current,
                key=f"bases_for_{utd}",
                help="Selecione apenas as BASEs desta UTD.",
            )
            st.session_state["utd_base_sel"][utd] = new_sel
    else:
        if not (utds_sel and turma_sel):
            st.session_state["utd_base_sel"] = {}

    if "lines_df" not in st.session_state:
        st.session_state["lines_df"] = _empty_df()

    def _ensure_rows_for_selected_pairs() -> None:
        orig = st.session_state["lines_df"]
        df = orig.copy()
        wanted_keys = set()
        rows_to_add = []
        for utd, bases in st.session_state.get("utd_base_sel", {}).items():
            for base in bases:
                key = f"{utd}\n{base}\n{turma_sel}"
                wanted_keys.add(key)
                exists = df[(df["UTD"] == utd) & (df["BASE"] == base) & (df["TURMA"] == turma_sel)]
                if exists.empty:
                    zona = _zona_for(dag40_df, utd, base, turma_sel)
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
            df["KEY"] = (
                df["UTD"].astype(str)
                + "\n"
                + df["BASE"].astype(str)
                + "\n"
                + df["TURMA"].astype(str)
            )
            df = df[df["KEY"].isin(wanted_keys)].drop(columns=["KEY"], errors="ignore")
        if rows_to_add:
            df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)
        if not df.equals(orig):
            st.session_state["lines_df"] = df
            _clear_editor_state()
        else:
            st.session_state["lines_df"] = df

    def _add_service_row_for_base(utd: str, base: str, turma: str, zona: str) -> None:
        df = st.session_state["lines_df"].copy()
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
        st.session_state["lines_df"] = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    def _has_any_bases_selected() -> bool:
        sel = st.session_state.get("utd_base_sel", {})
        return any(len(v) > 0 for v in sel.values())

    if utds_sel and turma_sel and _has_any_bases_selected():
        _ensure_rows_for_selected_pairs()
        st.subheader("Linhas por BASE", divider=True)
        with st.container(border=True):
            cols = st.columns(3)
            i = 0
            for utd, bases in st.session_state["utd_base_sel"].items():
                for base in bases:
                    zona_val = _zona_for(dag40_df, utd, base, turma_sel)
                    if cols[i % 3].button(
                        f"➕ Adicionar serviço • {base} (UTD {utd}, {turma_sel})",
                        key=f"add_{utd}_{base}",
                        use_container_width=True,
                    ):
                        _add_service_row_for_base(utd, base, turma_sel, zona_val)
                    i += 1

        def _apply_editor_changes() -> None:
            ed_state = st.session_state.get(EDITOR_KEY, {})
            df = st.session_state["lines_df"].copy()
            deleted = ed_state.get("deleted_rows", [])
            if deleted:
                df = df.drop(df.index[deleted]).reset_index(drop=True)
            for row_idx, changes in ed_state.get("edited_rows", {}).items():
                for col, val in changes.items():
                    if col in df.columns and col in COLUMNS_SHOW:
                        df.at[row_idx, col] = val
            for new in ed_state.get("added_rows", []):
                base_row = {
                    "UTD": "",
                    "BASE": "",
                    "TURMA": "",
                    "GERACAO_PARA": geracao_default,
                    "SERVIÇO": "",
                    "PACOTES": 1,
                    "JUSTIFICATIVA": "",
                    "COMENTARIO": "",
                    "ZONA": "",
                }
                base_row.update({k: v for k, v in new.items() if k in COLUMNS_ALL})
                df = pd.concat([df, pd.DataFrame([base_row])], ignore_index=True)
            df["PACOTES"] = pd.to_numeric(df["PACOTES"], errors="coerce").fillna(0).astype(int)
            st.session_state["lines_df"] = df

        editor_df = st.session_state["lines_df"][COLUMNS_SHOW].copy()
        editor_df["PACOTES"] = pd.to_numeric(editor_df["PACOTES"], errors="coerce").fillna(0).astype(int)
        st.data_editor(
            editor_df,
            key=EDITOR_KEY,
            on_change=_apply_editor_changes,
            hide_index=True,
            num_rows="dynamic",
            column_order=COLUMNS_SHOW,
            column_config={
                "UTD": st.column_config.TextColumn("UTD", disabled=True),
                "BASE": st.column_config.TextColumn("BASE", disabled=True),
                "TURMA": st.column_config.TextColumn("TURMA", disabled=True),
                "GERACAO_PARA": st.column_config.SelectboxColumn("Geração para", options=geracao_opcoes, required=True),
                "SERVIÇO": st.column_config.SelectboxColumn(
                    "Serviço",
                    options=DEFAULT_SERVICOS,
                    required=True,
                    help="• Corte Gavião ⇒ Disjuntor • Visita de Cobrança ⇒ COB.DOM",
                ),
                "PACOTES": st.column_config.NumberColumn("Pacotes", min_value=1, step=1, required=True),
                "JUSTIFICATIVA": st.column_config.TextColumn("Justificativa", required=True),
                "COMENTARIO": st.column_config.TextColumn("Comentário"),
            },
        )
    else:
        st.info("Selecione UTD(s), a TURMA e ao menos uma BASE para cada UTD.")

    def _validate_and_prepare_output() -> pd.DataFrame:
        if not is_valid_name(nome_input):
            raise ValueError("Informe ao menos um **Nome** e um **Sobrenome**.")
        if not is_valid_email(email_input):
            raise ValueError("Informe um **e-mail** válido do domínio **@neoenergia.com**.")
        df = st.session_state["lines_df"].copy()
        if df.empty:
            raise ValueError("Nenhuma linha para enviar. Adicione ao menos uma BASE e configure os serviços.")
        req_cols = ["GERACAO_PARA", "SERVIÇO", "PACOTES", "JUSTIFICATIVA"]
        for col in req_cols:
            if df[col].isna().any() or (df[col].astype(str).str.strip() == "").any():
                raise ValueError(f"Há linhas com **{col}** vazio.")
        if after_10h55 and (df["GERACAO_PARA"].astype(str).str.upper().str.strip() == "HOJE").any():
            raise ValueError("Após 10:55, **HOJE** não é permitido. Altere para **AMANHÃ** ou **FIM DE SEMANA**.")
        df["PACOTES"] = pd.to_numeric(df["PACOTES"], errors="coerce").fillna(0).astype(int)
        if (df["PACOTES"] < 1).any():
            raise ValueError("Há linhas com **PACOTES** inválidos (mín. 1).")
        nome_norm = strip_accents_and_punct_name(nome_input)
        email_norm = email_input.strip().lower()

        df["SERVICO_CLEAN"] = df["SERVIÇO"].apply(strip_accents_and_punct_action)
        df["GERACAO_PARA"] = df["GERACAO_PARA"].astype(str).str.upper().str.strip()

        timestamp = datetime.now(TZ)
        out = df.copy()
        out.insert(0, "TIMESTAMP", timestamp)
        out.insert(1, "NOME", nome_norm)
        out.insert(2, "E-MAIL", email_norm)
        out.insert(3, "CADEIA", out["GERACAO_PARA"])
        if "TURMA" not in out.columns:
            out["TURMA"] = turma_sel
        out = out[
            [
                "TIMESTAMP",
                "NOME",
                "E-MAIL",
                "CADEIA",
                "UTD",
                "BASE",
                "TURMA",
                "ZONA",
                "SERVIÇO",
                "SERVICO_CLEAN",
                "PACOTES",
                "JUSTIFICATIVA",
                "COMENTARIO",
            ]
        ]
        return out

    st.divider()
    col_a, col_b = st.columns([1, 1])
    can_send = bool(strip_accents_and_punct_name(nome_input)) and bool(email_input.strip()) and not st.session_state[
        "lines_df"
    ].empty

    if col_a.button("📨 Enviar Solicitação", type="primary", use_container_width=True, disabled=not can_send):
        try:
            out_df = _validate_and_prepare_output()
            inserted = insert_pedidos_default(out_df)
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
            resumo_df = st.session_state["lines_df"][resumo_cols].copy()
            resumo_df["PACOTES"] = pd.to_numeric(resumo_df["PACOTES"], errors="coerce").fillna(0).astype(int)
            st.session_state["success_qtd"] = inserted
            st.session_state["success_nome"] = strip_accents_and_punct_name(nome_input)
            st.session_state["success_email"] = email_input.strip().lower()
            st.session_state["success_resumo"] = resumo_df
            st.session_state["lines_df"] = _empty_df()
            clear_pedidos_cache()
            _show_success_dialog()
        except Exception as exc:  # pragma: no cover - Streamlit feedback
            st.error(f"Falha ao enviar: {exc}")

    if col_b.button("🧹 Limpar Tudo", use_container_width=True):
        trigger_full_reset()
        st.rerun()


if __name__ == "__main__":  # pragma: no cover - executed via Streamlit
    page()
