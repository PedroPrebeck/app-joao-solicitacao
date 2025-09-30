from __future__ import annotations

from datetime import date, datetime
from typing import Dict, List

import pandas as pd
import streamlit as st
import streamlit_authenticator as stauth
import yaml
from hdbcli import dbapi
from yaml.loader import SafeLoader

from app.exporters.csv_exporter import generate_csv_payloads
from app.models.pedido import build_row_key_from_series
from app.services.cluster_config_service import fetch_cluster_config
from app.services.hana import HanaConfig
from app.services.pedidos_cache import clear_pedidos_cache, fetch_all_pedidos_cached
from app.services.pedidos_service import apply_status_changes, has_column, has_status_column
from app.settings import TZ
from app.utils.constants import ALLOWED_ADMINS, STATUS_LABEL_INV, STATUS_LABEL_MAP


ADMIN_EDITOR_KEY = "admin_editor_v2"


@st.cache_data(show_spinner=False)
def load_auth_config() -> Dict[str, Dict[str, Dict[str, str]]]:
    with open(".streamlit/auth_config.yaml", "r", encoding="utf-8") as file:
        return yaml.load(file, Loader=SafeLoader)


def _ensure_session_defaults() -> None:
    st.session_state.setdefault("admin_pending_changes", {})
    st.session_state.setdefault("admin_index_to_key", {})
    st.session_state.setdefault("csv_row_selection", {})


def _filter_admin_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    working = df.copy()
    working["STATUS_NORM"] = (
        working["STATUS"].fillna("").astype(str).str.upper().str.strip().replace({"": "EM ANALISE", "NAN": "EM ANALISE"})
    )
    working["TS_DT"] = pd.to_datetime(working["TIMESTAMP"], errors="coerce")
    working["DATA_HORA"] = working["TS_DT"].dt.strftime("%d/%m/%Y %H:%M")

    if st.session_state.get("reset_filters_admin", False):
        for key in ["f_data_admin", "f_utd_admin", "f_base_admin", "f_email_admin", "f_status_admin"]:
            st.session_state.pop(key, None)
        st.session_state["reset_filters_admin"] = False

    with st.expander("Filtros", expanded=True):
        row1c1, row1c2, row1c3, row1c4 = st.columns([1.4, 1.4, 2.2, 1.0])
        with row1c1:
            ts_valid = working["TS_DT"].dropna()
            default_date = ts_valid.max().date() if not ts_valid.empty else datetime.now(TZ).date()
            st.date_input(
                "Data do pedido",
                value=st.session_state.get("f_data_admin", default_date),
                key="f_data_admin",
            )
        with row1c2:
            utd_opts = sorted([u for u in working["UTD"].dropna().unique().tolist() if u], key=str.casefold)
            st.multiselect("UTD", options=utd_opts, key="f_utd_admin")
        with row1c3:
            base_opts = sorted([b for b in working["BASE"].dropna().unique().tolist() if b], key=str.casefold)
            st.multiselect("BASE", options=base_opts, key="f_base_admin")
        with row1c4:
            st.text_input("E-mail (contém)", placeholder="parte do e-mail", key="f_email_admin")
        row2c1, row2c2 = st.columns([1.4, 1.4])
        with row2c1:
            st.multiselect(
                "Status",
                options=list(STATUS_LABEL_MAP.values()),
                key="f_status_admin",
            )
        with row2c2:
            if st.button("🧭 Limpar filtros", use_container_width=True, key="btn_reset_admin"):
                st.session_state["reset_filters_admin"] = True
                st.rerun()

    filtered = working
    try:
        selected_date = st.session_state.get("f_data_admin")
        if isinstance(selected_date, date):
            filtered = filtered[filtered["TS_DT"].dt.date == selected_date]
    except Exception:
        pass
    if st.session_state.get("f_utd_admin"):
        filtered = filtered[filtered["UTD"].isin(st.session_state["f_utd_admin"])]
    if st.session_state.get("f_base_admin"):
        filtered = filtered[filtered["BASE"].isin(st.session_state["f_base_admin"])]
    email_contains = (st.session_state.get("f_email_admin", "") or "").strip().lower()
    if email_contains:
        filtered = filtered[
            filtered["E-MAIL"].astype(str).str.lower().str.contains(email_contains, na=False)
        ]
    if st.session_state.get("f_status_admin"):
        allowed_status = set(st.session_state["f_status_admin"])
        filtered = filtered[filtered["STATUS_LABEL"].isin(allowed_status)]

    filtered = filtered.sort_values("TS_DT", ascending=True)
    return filtered


def _render_admin_grid(filtered_df: pd.DataFrame) -> None:
    st.session_state["admin_index_to_key"] = {idx: rk for idx, rk in enumerate(filtered_df["_ROW_KEY"].tolist())}
    sel_map = st.session_state.get("csv_row_selection", {})

    def _label_from_db_or_pending(idx: int, db_value_norm: str) -> str:
        key = st.session_state["admin_index_to_key"].get(idx)
        if key and key in st.session_state["admin_pending_changes"]:
            return st.session_state["admin_pending_changes"][key]
        return STATUS_LABEL_MAP.get(db_value_norm, "🟡 Pendente")

    editor_df = filtered_df.copy()
    editor_df["STATUS"] = [
        _label_from_db_or_pending(i, v) for i, v in enumerate(filtered_df["STATUS_NORM"].tolist())
    ]
    editor_df["SELECIONAR"] = [bool(sel_map.get(rk, True)) for rk in filtered_df["_ROW_KEY"]]

    def _capture_admin_edits() -> None:
        ed_state = st.session_state.get(ADMIN_EDITOR_KEY, {})
        if not ed_state:
            return
        edited = ed_state.get("edited_rows", {})
        for idx, changes in edited.items():
            key = st.session_state["admin_index_to_key"].get(idx)
            if not key:
                continue
            if "STATUS" in changes:
                st.session_state["admin_pending_changes"][key] = changes["STATUS"]
            if "SELECIONAR" in changes:
                st.session_state["csv_row_selection"][key] = bool(changes["SELECIONAR"])

    st.data_editor(
        editor_df[
            [
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
                "STATUS",
                "SELECIONAR",
            ]
        ],
        key=ADMIN_EDITOR_KEY,
        on_change=_capture_admin_edits,
        hide_index=True,
        num_rows="fixed",
        use_container_width=True,
        column_config={
            "DATA_HORA": st.column_config.TextColumn("Data e hora", disabled=True),
            "NOME": st.column_config.TextColumn("Nome", disabled=True),
            "E-MAIL": st.column_config.TextColumn("E-mail", disabled=True),
            "UTD": st.column_config.TextColumn("UTD", disabled=True),
            "BASE": st.column_config.TextColumn("BASE", disabled=True),
            "TURMA": st.column_config.TextColumn("TURMA", disabled=True),
            "SERVICO": st.column_config.TextColumn("Serviço", disabled=True),
            "PACOTES": st.column_config.NumberColumn("Pacotes", disabled=True),
            "CADEIA": st.column_config.TextColumn("Geração para", disabled=True),
            "JUSTIFICATIVA": st.column_config.TextColumn("Justificativa", disabled=True),
            "COMENTARIOS": st.column_config.TextColumn("Comentário", disabled=True),
            "STATUS": st.column_config.SelectboxColumn(
                "Status",
                options=list(STATUS_LABEL_INV.keys()),
                help="As alterações só serão gravadas ao clicar em “Aplicar mudanças no HANA”.",
            ),
            "SELECIONAR": st.column_config.CheckboxColumn(
                "Selecionar p/ CSV",
                help="Marque apenas as linhas que devem ir para o CSV.",
            ),
        },
    )


def _apply_pending_changes(admin_email: str) -> None:
    pending_changes = st.session_state.get("admin_pending_changes", {})
    if not pending_changes:
        return

    cfg = HanaConfig.from_env()
    has_validado_por = has_column("VALIDADO_POR", connector=dbapi.connect, config=cfg)

    clear_pedidos_cache()
    current_df = fetch_all_pedidos_cached()
    current_df = current_df.copy()
    current_df["STATUS_NORM"] = (
        current_df["STATUS"].fillna("").astype(str).str.upper().str.strip().replace({"": "EM ANALISE", "NAN": "EM ANALISE"})
    )
    current_df["_ROW_KEY"] = current_df.apply(build_row_key_from_series, axis=1)

    updated = apply_status_changes(
        current_df,
        pending_changes,
        admin_email=admin_email,
        has_validado_por=has_validado_por,
        connector=dbapi.connect,
        config=cfg,
    )

    st.session_state["admin_pending_changes"].clear()
    if ADMIN_EDITOR_KEY in st.session_state:
        del st.session_state[ADMIN_EDITOR_KEY]
    clear_pedidos_cache()
    st.session_state["admin_last_apply_success"] = updated
    st.session_state["show_csv_tools"] = True
    st.rerun()


def _discard_pending_changes() -> None:
    st.session_state["admin_pending_changes"].clear()
    if ADMIN_EDITOR_KEY in st.session_state:
        del st.session_state[ADMIN_EDITOR_KEY]
    st.info("Alterações pendentes descartadas.")
    st.rerun()


def _render_csv_generation_tools() -> None:
    cfg = HanaConfig.from_env()
    st.subheader("Geração de arquivos de configuração", divider=True)

    with st.container(border=True):
        cols_top = st.columns([1.4, 1.4, 1.8])
        with cols_top[0]:
            gen_date = st.date_input("Data de geração", key="csv_gen_date", value=datetime.now(TZ).date())
        with cols_top[1]:
            turmas_sel = st.multiselect("Turma(s)", options=["STC", "EPS"], key="csv_gen_turmas", default=["STC"])
        with cols_top[2]:
            carteiras_ui = [
                ("Corte", "CONVENCIONAL"),
                ("Recorte", "RECORTE"),
                ("Baixa", "BAIXA"),
                ("Disjuntor (Corte Gavião)", "DISJUNTOR"),
                ("Cobrança domiciliar (COB.DOM)", "COB.DOM"),
            ]
            label_to_db = {label: db for label, db in carteiras_ui}
            default_labels = ["Corte"]
            carteiras_labels_sel = st.multiselect(
                "Carteira(s)",
                options=[label for label, _ in carteiras_ui],
                default=default_labels,
                key="csv_gen_carteiras_labels",
            )
            carteiras_db_sel: List[str] = [label_to_db[label] for label in carteiras_labels_sel]

        excluir_desmarcados = st.checkbox(
            "Excluir do arquivo as linhas desmarcadas (SELECIONAR=NAO)",
            value=True,
            key="csv_excluir_desmarcados",
        )

        disabled_gen = not isinstance(gen_date, date) or not turmas_sel or not carteiras_db_sel

        st.session_state.setdefault(
            "csv_gen_state",
            {"date": None, "turmas": [], "carteiras": [], "df_all": pd.DataFrame(), "ready": False},
        )

        def _store_csv_state(df_all: pd.DataFrame, dt: date, turmas: List[str], carteiras: List[str]) -> None:
            st.session_state["csv_gen_state"] = {
                "date": dt,
                "turmas": list(turmas),
                "carteiras": list(carteiras),
                "df_all": df_all.copy(),
                "ready": not df_all.empty,
            }

        def _state_matches_current() -> bool:
            state = st.session_state["csv_gen_state"]
            return (
                state.get("ready", False)
                and state.get("date") == gen_date
                and state.get("turmas") == list(turmas_sel)
                and state.get("carteiras") == list(carteiras_db_sel)
                and isinstance(state.get("df_all"), pd.DataFrame)
            )

        if st.button("🧾 Gerar arquivos de configuração", type="primary", disabled=disabled_gen):
            try:
                df_all = fetch_cluster_config(
                    gen_date,
                    turmas_sel,
                    carteiras_db_sel,
                    connector=dbapi.connect,
                    config=cfg,
                )
                if df_all.empty:
                    st.warning("Nenhum dado encontrado para os filtros selecionados.")
                    _store_csv_state(pd.DataFrame(), gen_date, turmas_sel, carteiras_db_sel)
                else:
                    df_for_key = pd.DataFrame(
                        {
                            "TIMESTAMP": df_all["TS"],
                            "NOME": df_all["NOME"],
                            "E-MAIL": df_all["EMAIL"],
                            "UTD": df_all["UTD"],
                            "BASE": df_all["BASE"],
                            "SERVICO": df_all["SERVICO"],
                            "PACOTES": pd.to_numeric(df_all["PACOTES"], errors="coerce").fillna(0).astype(int),
                        }
                    )
                    df_all["_ROW_KEY"] = df_for_key.apply(build_row_key_from_series, axis=1)
                    sel_map = st.session_state.get("csv_row_selection", {})
                    df_all["SELECIONAR"] = df_all["_ROW_KEY"].map(
                        lambda key: "SIM" if sel_map.get(key, True) else "NAO"
                    )
                    if excluir_desmarcados:
                        df_all = df_all[df_all["SELECIONAR"].str.upper() == "SIM"].copy()
                        if df_all.empty:
                            st.warning("Nenhuma linha marcada para exportação com os filtros atuais.")
                    _store_csv_state(df_all, gen_date, turmas_sel, carteiras_db_sel)
                    st.success("Arquivos prontos para download abaixo.")
            except Exception as exc:  # pragma: no cover - Streamlit feedback
                st.error(f"Falha ao gerar CSV: {exc}")

        if _state_matches_current():
            df_all = st.session_state["csv_gen_state"]["df_all"]
            turmas_presentes = sorted(df_all["TURMA"].dropna().unique().tolist())
            carteiras_presentes = sorted(df_all["CARTEIRA"].dropna().unique().tolist())

            with st.expander("Prévia de contagem por TURMA x CARTEIRA", expanded=False):
                for turma in turmas_presentes:
                    row_info = []
                    for carteira in carteiras_presentes:
                        count = int(((df_all["TURMA"] == turma) & (df_all["CARTEIRA"] == carteira)).sum())
                        row_info.append(f"{carteira}: {count}")
                    st.write(f"**{turma}** → " + " | ".join(row_info))

            payloads = generate_csv_payloads(
                df_all,
                exclude_unselected=excluir_desmarcados,
            )
            if not payloads:
                st.warning("Nenhuma linha marcada para exportação com os filtros atuais.")
            else:
                for payload in payloads:
                    st.download_button(
                        label=f"⬇️ Baixar {payload.file_name}",
                        data=payload.content,
                        file_name=payload.file_name,
                        mime="text/csv",
                        use_container_width=True,
                    )
        else:
            st.info("Defina os filtros e clique em **Gerar arquivos de configuração**.")


def page() -> None:
    st.header("🛡️ Gestão de Pedidos (restrito)", divider=True)

    cfg = load_auth_config()
    authenticator = stauth.Authenticate(
        credentials=cfg["credentials"],
        cookie_name=cfg["cookie"]["name"],
        cookie_key=cfg["cookie"]["key"],
        cookie_expiry_days=cfg["cookie"]["expiry_days"],
    )

    authenticator.login(
        location="main",
        fields={
            "Form name": "Acesso restrito",
            "Username": "E-mail",
            "Password": "Senha",
            "Login": "Entrar",
        },
        key="admin_login_form",
    )

    auth_status = st.session_state.get("authentication_status")
    if auth_status is False:
        st.error("Usuário ou senha incorretos.")
        st.stop()
    if auth_status is None:
        st.info("Faça login para gerenciar os pedidos.")
        st.stop()

    user_email = (st.session_state.get("username") or "").strip().lower()
    if user_email not in ALLOWED_ADMINS:
        st.error("Usuário sem permissão para acessar a Gestão.")
        authenticator.logout("Sair", "main", key="logout_no_perm")
        st.stop()

    st.session_state["admin_email"] = user_email

    with st.container():
        hdr_left, hdr_right = st.columns([2, 2])
        with hdr_left:
            st.caption(f"Logado como: **{st.session_state.get('admin_email', '')}**")
        with hdr_right:
            col_refresh, col_logout = st.columns(2)
            with col_refresh:
                if st.button("🔄 Atualizar dados", use_container_width=True):
                    clear_pedidos_cache()
                    if ADMIN_EDITOR_KEY in st.session_state:
                        del st.session_state[ADMIN_EDITOR_KEY]
                    st.rerun()
            with col_logout:
                authenticator.logout("Sair", "main", key="logout_admin")

    st.divider()

    if "admin_last_apply_success" in st.session_state:
        updated = st.session_state.pop("admin_last_apply_success", None)
        if updated is not None:
            st.success(f"✅ Mudanças aplicadas no HANA com sucesso ({updated} linha(s) atualizada(s)).")
            st.session_state["show_csv_tools"] = True

    _ensure_session_defaults()

    try:
        admin_df = fetch_all_pedidos_cached()
    except Exception as exc:  # pragma: no cover - Streamlit feedback
        st.error(f"Erro ao carregar pedidos: {exc}")
        st.stop()

    filtered_df = _filter_admin_dataframe(admin_df)
    filtered_df["_ROW_KEY"] = filtered_df.apply(build_row_key_from_series, axis=1)

    total_filtrados = len(filtered_df)
    st.caption(f"{total_filtrados} registro(s) encontrados.")

    if total_filtrados == 0:
        st.info("Nenhum pedido encontrado com os filtros atuais.")
        return

    st.session_state.setdefault("csv_row_selection", {})
    st.session_state.setdefault("admin_pending_changes", {})

    select_bar = st.columns([1.2, 5])[0]
    with select_bar:
        if st.button("🔁 Alternar seleção", use_container_width=True, disabled=(total_filtrados == 0)):
            for row_key in filtered_df["_ROW_KEY"]:
                st.session_state["csv_row_selection"][row_key] = not bool(
                    st.session_state["csv_row_selection"].get(row_key, True)
                )
            if ADMIN_EDITOR_KEY in st.session_state:
                del st.session_state[ADMIN_EDITOR_KEY]
            st.rerun()

    _render_admin_grid(filtered_df)

    pending_count = len(st.session_state["admin_pending_changes"])
    actions = st.columns([1.3, 1.6, 1.8, 2.5])
    with actions[0]:
        st.metric("Alterações pendentes", pending_count)
    with actions[1]:
        if st.button("🟢 Aprovar tudo", use_container_width=True, disabled=filtered_df.empty):
            for row_key in filtered_df["_ROW_KEY"].tolist():
                st.session_state["admin_pending_changes"][row_key] = "🟢 Aprovado"
            if ADMIN_EDITOR_KEY in st.session_state:
                del st.session_state[ADMIN_EDITOR_KEY]
            st.rerun()
    with actions[2]:
        if st.button(
            "💾 Aplicar mudanças no HANA",
            type="primary",
            use_container_width=True,
            disabled=(pending_count == 0),
        ):
            if not has_status_column(connector=dbapi.connect, config=HanaConfig.from_env()):
                st.warning("Coluna STATUS não existe no HANA. Crie a coluna para persistir as alterações.")
            else:
                _apply_pending_changes(st.session_state.get("admin_email", ""))
    with actions[3]:
        if st.button(
            "🗑️ Descartar mudanças pendentes",
            use_container_width=True,
            disabled=(pending_count == 0),
        ):
            _discard_pending_changes()

    if st.session_state.get("show_csv_tools", False):
        _render_csv_generation_tools()
    else:
        st.info("Aplique as mudanças no HANA para liberar a geração de CSV.")


if __name__ == "__main__":  # pragma: no cover - executed via Streamlit
    page()
