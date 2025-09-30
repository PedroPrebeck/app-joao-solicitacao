"""Página de gestão restrita dos pedidos."""
from __future__ import annotations

from datetime import date, datetime
from typing import Iterable, List

import pandas as pd
import streamlit as st
from hdbcli import dbapi

from app.exporters.csv_exporter import generate_csv_payloads
from app.models.pedido import build_row_key_from_series
from app.services.auth_service import authenticator_from_config, load_auth_config
from app.services.hana import HanaConfig, create_connection
from app.services.pedidos_service import apply_status_changes, pedidos_table_has_column
from app.state import session_keys as keys
from app.utils.cache import fetch_pedidos_cached
from app.utils.constants import ALLOWED_ADMINS, STATUS_LABEL_INV, STATUS_LABEL_MAP
from app.utils.time_windows import TZ


def _load_admin_df() -> pd.DataFrame:
    df = fetch_pedidos_cached()
    status_norm = df["STATUS"].fillna("").astype(str).str.upper().str.strip()
    df["STATUS_NORM"] = status_norm.where(~status_norm.isin(["", "NAN", "NONE", "NULL"]), "EM ANALISE")
    df["TS_DT"] = pd.to_datetime(df["TIMESTAMP"], errors="coerce")
    df["DATA_HORA"] = df["TS_DT"].dt.strftime("%d/%m/%Y %H:%M")
    df["_ROW_KEY"] = df.apply(build_row_key_from_series, axis=1)
    return df


def _reset_admin_filters() -> None:
    for key in [
        keys.ADMIN_DATE_FILTER,
        keys.ADMIN_UTD_FILTER,
        keys.ADMIN_BASE_FILTER,
        keys.ADMIN_EMAIL_FILTER,
        keys.ADMIN_STATUS_FILTER,
    ]:
        st.session_state.pop(key, None)
    st.session_state[keys.ADMIN_RESET] = False


def _apply_admin_filters(df: pd.DataFrame) -> pd.DataFrame:
    result = df.copy()
    try:
        selected_date = st.session_state.get(keys.ADMIN_DATE_FILTER)
        if isinstance(selected_date, datetime):
            result = result[result["TS_DT"].dt.date == selected_date.date()]
        elif hasattr(selected_date, "year"):
            result = result[result["TS_DT"].dt.date == selected_date]
    except Exception:
        pass

    utd_filter = st.session_state.get(keys.ADMIN_UTD_FILTER) or []
    if utd_filter:
        result = result[result["UTD"].isin(utd_filter)]

    base_filter = st.session_state.get(keys.ADMIN_BASE_FILTER) or []
    if base_filter:
        result = result[result["BASE"].isin(base_filter)]

    email_contains = (st.session_state.get(keys.ADMIN_EMAIL_FILTER) or "").strip().lower()
    if email_contains:
        result = result[result["E-MAIL"].astype(str).str.lower().str.contains(email_contains, na=False)]

    status_filter = st.session_state.get(keys.ADMIN_STATUS_FILTER) or []
    if status_filter:
        db_values = [STATUS_LABEL_INV.get(label, "EM ANALISE") for label in status_filter]
        result = result[result["STATUS_NORM"].isin(db_values)]

    return result


def _ensure_admin_state() -> None:
    st.session_state.setdefault(keys.ADMIN_PENDING_CHANGES, {})
    st.session_state.setdefault(keys.ADMIN_INDEX_TO_KEY, {})
    st.session_state.setdefault(keys.CSV_SELECTION, {})


def _capture_admin_edits(editor_key: str) -> None:
    ed_state = st.session_state.get(editor_key, {})
    if not ed_state:
        return

    edited = ed_state.get("edited_rows", {})
    for idx, changes in edited.items():
        key = st.session_state[keys.ADMIN_INDEX_TO_KEY].get(idx)
        if not key:
            continue
        if "STATUS" in changes:
            st.session_state[keys.ADMIN_PENDING_CHANGES][key] = changes["STATUS"]
        if "SELECIONAR" in changes:
            st.session_state[keys.CSV_SELECTION][key] = bool(changes["SELECIONAR"])


def _apply_pending_changes(df: pd.DataFrame, admin_email: str) -> int:
    if not st.session_state[keys.ADMIN_PENDING_CHANGES]:
        return 0

    cfg = HanaConfig.from_env()
    has_status = pedidos_table_has_column("STATUS", connector=dbapi.connect, config=cfg)
    if not has_status:
        st.warning("Coluna STATUS não existe no HANA. Crie a coluna para persistir as alterações.")
        return 0

    has_validado_por = pedidos_table_has_column("VALIDADO_POR", connector=dbapi.connect, config=cfg)
    try:
        updated = apply_status_changes(
            df,
            st.session_state[keys.ADMIN_PENDING_CHANGES],
            admin_email=admin_email,
            has_validado_por=has_validado_por,
            connector=dbapi.connect,
            config=cfg,
        )
    finally:
        st.cache_data.clear()

    st.session_state[keys.ADMIN_PENDING_CHANGES].clear()
    if keys.ADMIN_EDITOR_KEY in st.session_state:
        del st.session_state[keys.ADMIN_EDITOR_KEY]
    return updated


def _toggle_selection(filtered_df: pd.DataFrame) -> None:
    for rk in filtered_df["_ROW_KEY"]:
        current = bool(st.session_state[keys.CSV_SELECTION].get(rk, True))
        st.session_state[keys.CSV_SELECTION][rk] = not current
    if keys.ADMIN_EDITOR_KEY in st.session_state:
        del st.session_state[keys.ADMIN_EDITOR_KEY]


def _run_cluster_config_query(
    sel_date: date,
    turmas: Iterable[str],
    carteiras_db: Iterable[str],
) -> pd.DataFrame:
    turmas = [t.strip().upper() for t in turmas]
    carteiras_db = [c.strip().upper() for c in carteiras_db]
    turma_placeholders = ",".join(["?"] * len(turmas))
    cart_placeholders = ",".join(["?"] * len(carteiras_db))

    sql = f"""
WITH BASES AS (
  SELECT UTD40, ID_BASE_STC, MIN(BASE_STC) AS BASE_STC, NULL AS ID_BASE_EPS, NULL AS BASE_EPS
  FROM CLB142840.DAG40
  GROUP BY UTD40, ID_BASE_STC
  UNION ALL
  SELECT UTD40, NULL, NULL, ID_BASE_EPS, MIN(BASE_EPS)
  FROM CLB142840.DAG40
  GROUP BY UTD40, ID_BASE_EPS
),
CLUSTER_CONFIG AS (
  SELECT
    CASE WHEN UTD = 'ITAPUAN' THEN 'ITAPOAN' ELSE TRIM(UPPER(UTD)) END AS UTD,
    TRIM(UPPER(CARTEIRA)) AS CARTEIRA,
    TURMA, ZONA,
    QTD_MIN, QTD_MAX,
    RAIO_MIN * 1000 AS RAIO_MIN,
    RAIO_MAX * 1000 AS RAIO_MAX,
    RAIO_INC * 1000 AS RAIO_INC
  FROM CLB142840.CLUSTER_CONFIG5
),
INNER_Q AS (
  SELECT
    BP.UTD,
    'SIM' AS SELECIONAR,
    BP.ZONA,
    '' AS LOCALI,
    '' AS MUNICIPIO,
    '' AS BAIRRO,
    '' AS TIPO_LOCAL,
    BP.PACOTES AS CLUSTERS,
    BP.PACOTES AS PACOTES,
    COALESCE(CNF.QTD_MAX, '15') AS QTD_MAX,
    COALESCE(CNF.QTD_MIN, '10') AS QTD_MIN,
    COALESCE(CNF.RAIO_MIN, '4000') AS RAIO_IDEAL,
    COALESCE(CNF.RAIO_MAX, '5000') AS RAIO_MAX,
    COALESCE(CNF.RAIO_INC, '500') AS RAIO_STEP,
    CASE
      WHEN UPPER(BP.SERVICO) LIKE '%GAVIAO%' THEN 'DISJUNTOR'
      WHEN UPPER(BP.SERVICO) LIKE '%RECORTE%' THEN 'RECORTE'
      WHEN UPPER(BP.SERVICO) LIKE '%BAIXA%' THEN 'BAIXA'
      WHEN UPPER(BP.SERVICO) LIKE '%VISITA%' THEN 'COB.DOM'
      ELSE 'CONVENCIONAL'
    END AS CARTEIRA,
    CASE
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NULL THEN 'STC'
      WHEN E.ID_BASE_EPS IS NOT NULL AND S.ID_BASE_STC IS NULL THEN 'EPS'
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NOT NULL THEN 'AMBIGUO'
      ELSE NULL
    END AS TURMA,
    '1' AS PESO_MTVCOB,
    '2' AS PESO_PECLD,
    '0' AS PESO_QTDFTVE,
    '' AS PREENCHER,
    '' AS QTD_PREENCHER,
    BP."NOME" AS NOME,
    BP."E-MAIL" AS EMAIL,
    BP.BASE AS BASE,
    BP.SERVICO AS SERVICO,
    BP."TIMESTAMP" AS TS
  FROM "U618488"."BASE_PEDIDOS" BP
  LEFT JOIN BASES S
    ON BP.ZONA = S.ID_BASE_STC AND TRIM(UPPER(BP.UTD)) = TRIM(UPPER(S.UTD40))
  LEFT JOIN BASES E
    ON BP.ZONA = E.ID_BASE_EPS AND TRIM(UPPER(BP.UTD)) = TRIM(UPPER(E.UTD40))
  LEFT JOIN CLUSTER_CONFIG CNF
    ON CNF.UTD = TRIM(UPPER(BP.UTD))
   AND CNF.TURMA = CASE
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NULL THEN 'STC'
      WHEN E.ID_BASE_EPS IS NOT NULL AND S.ID_BASE_STC IS NULL THEN 'EPS'
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NOT NULL THEN 'AMBIGUO'
      ELSE NULL
    END
   AND (
      (UPPER(TRIM(COALESCE(BP.SERVICO, ''))) LIKE '%GAVIAO%' AND CNF.CARTEIRA = 'DISJUNTOR')
      OR (UPPER(TRIM(COALESCE(BP.SERVICO, ''))) NOT LIKE '%GAVIAO%' AND CNF.ZONA = BP.ZONA)
   )
  WHERE TO_DATE(BP."TIMESTAMP") = ?
)
SELECT *
FROM INNER_Q
WHERE TURMA IN ({turma_placeholders})
  AND CARTEIRA IN ({cart_placeholders})
ORDER BY TO_DATE(TS) ASC, TO_VARCHAR(TS, 'HH24:MI:SS') ASC
"""

    params = [sel_date] + list(turmas) + list(carteiras_db)
    cfg = HanaConfig.from_env()
    conn = None
    cur = None
    try:
        conn = create_connection(cfg, dbapi.connect)
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        return pd.DataFrame(rows, columns=cols).fillna("")
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()


def _store_csv_state(df_all: pd.DataFrame, dt: date, turmas: List[str], carteiras: List[str]) -> None:
    st.session_state.setdefault("csv_gen_state", {})
    st.session_state["csv_gen_state"] = {
        "date": dt,
        "turmas": list(turmas),
        "carteiras": list(carteiras),
        "df_all": df_all.copy(),
        "ready": not df_all.empty,
    }


def _state_matches_current(dt: date, turmas: List[str], carteiras: List[str]) -> bool:
    state = st.session_state.get("csv_gen_state", {})
    return (
        state.get("ready", False)
        and state.get("date") == dt
        and state.get("turmas") == list(turmas)
        and state.get("carteiras") == list(carteiras)
        and isinstance(state.get("df_all"), pd.DataFrame)
        and not state["df_all"].empty
    )


def _render_csv_generation_tools(admin_df: pd.DataFrame) -> None:
    st.subheader("Geração de CSV", divider=True)

    col_date, col_turma, col_cart = st.columns([1.1, 1.5, 1.7])
    with col_date:
        gen_date = st.date_input(
            "Data do pedido",
            value=st.session_state.get("csv_gen_date", datetime.now(TZ).date()),
            key="csv_gen_date",
        )
    with col_turma:
        turmas_opts = sorted(admin_df["TURMA"].dropna().unique().tolist())
        turmas_sel = st.multiselect(
            "Turma(s)",
            options=turmas_opts,
            default=turmas_opts,
            key="csv_gen_turmas",
        )
    with col_cart:
        carteiras_ui = [
            ("Corte", "CONVENCIONAL"),
            ("Recorte", "RECORTE"),
            ("Baixa", "BAIXA"),
            ("Disjuntor (Corte Gavião)", "DISJUNTOR"),
            ("Cobrança domiciliar (COB.DOM)", "COB.DOM"),
        ]
        label_to_db = {lbl: db for lbl, db in carteiras_ui}
        carteiras_sel = st.multiselect(
            "Carteira(s)",
            options=[lbl for lbl, _ in carteiras_ui],
            default=["Corte"],
            key="csv_gen_carteiras_labels",
        )
        carteiras_db_sel = [label_to_db[lbl] for lbl in carteiras_sel]

    excluir_desmarcados = st.checkbox(
        "Excluir do arquivo as linhas desmarcadas (SELECIONAR=NAO)",
        value=st.session_state.get("csv_excluir_desmarcados", True),
        key="csv_excluir_desmarcados",
    )

    disabled_gen = (not isinstance(gen_date, date)) or (not turmas_sel) or (not carteiras_db_sel)

    if st.button("🧾 Gerar arquivos de configuração", type="primary", disabled=disabled_gen):
        try:
            df_all = _run_cluster_config_query(gen_date, turmas_sel, carteiras_db_sel)
            if df_all.empty:
                st.warning("Nenhum dado encontrado para os filtros selecionados.")
                _store_csv_state(pd.DataFrame(), gen_date, turmas_sel, carteiras_db_sel)
            else:
                df_all_for_key = pd.DataFrame(
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
                df_all["_ROW_KEY"] = df_all_for_key.apply(build_row_key_from_series, axis=1)
                sel_map = st.session_state.get(keys.CSV_SELECTION, {})
                df_all["SELECIONAR"] = df_all["_ROW_KEY"].map(lambda k: "SIM" if sel_map.get(k, True) else "NAO")
                if excluir_desmarcados:
                    df_all = df_all[df_all["SELECIONAR"].str.upper() == "SIM"].copy()
                    if df_all.empty:
                        st.warning("Nenhuma linha marcada para exportação com os filtros atuais.")
                _store_csv_state(df_all, gen_date, turmas_sel, carteiras_db_sel)
                st.success("Arquivos prontos para download abaixo.")
        except Exception as exc:  # noqa: BLE001 - show message to user
            st.error(f"Falha ao gerar CSV: {exc}")

    if _state_matches_current(gen_date, turmas_sel, carteiras_db_sel):
        df_all = st.session_state["csv_gen_state"]["df_all"]
        turmas_presentes = sorted(df_all["TURMA"].dropna().unique().tolist())
        carteiras_presentes = sorted(df_all["CARTEIRA"].dropna().unique().tolist())
        with st.expander("Prévia de contagem por TURMA x CARTEIRA", expanded=False):
            for turma in turmas_presentes:
                row = []
                for cart in carteiras_presentes:
                    n = int(((df_all["TURMA"] == turma) & (df_all["CARTEIRA"] == cart)).sum())
                    row.append(f"{cart}: {n}")
                st.write(f"**{turma}** → " + " | ".join(row))

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


def main() -> None:
    st.subheader("Gestão de Pedidos", divider=True)

    try:
        cfg = load_auth_config()
    except Exception as exc:  # noqa: BLE001 - show message to user
        st.error(f"Erro ao carregar configuração de autenticação: {exc}")
        st.stop()

    authenticator = authenticator_from_config(cfg)
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

    auth_status = st.session_state.get(keys.AUTH_STATUS)
    if auth_status is False:
        st.error("Usuário ou senha incorretos.")
        st.stop()
    if auth_status is None:
        st.info("Faça login para gerenciar os pedidos.")
        st.stop()

    user_email = (st.session_state.get(keys.USERNAME) or "").strip().lower()
    if user_email not in ALLOWED_ADMINS:
        st.error("Usuário sem permissão para acessar a Gestão.")
        authenticator.logout("Sair", "main", key="logout_no_perm")
        st.stop()

    st.session_state[keys.ADMIN_EMAIL] = user_email

    with st.container():
        hdr_left, hdr_right = st.columns([2, 2])
        with hdr_left:
            st.caption(f"Logado como: **{st.session_state.get(keys.ADMIN_EMAIL, '')}**")
        with hdr_right:
            btn_refresh, btn_logout = st.columns(2)
            with btn_refresh:
                if st.button("🔄 Atualizar dados", use_container_width=True):
                    st.cache_data.clear()
                    st.rerun()
            with btn_logout:
                authenticator.logout("Sair", "main", key="logout_admin")

    st.divider()

    if keys.ADMIN_LAST_APPLY in st.session_state:
        upd_count = st.session_state.pop(keys.ADMIN_LAST_APPLY, None)
        if upd_count is not None:
            st.success(f"✅ Mudanças aplicadas no HANA com sucesso ({upd_count} linha(s) atualizada(s)).")
            st.session_state.setdefault("show_csv_tools", True)

    _ensure_admin_state()

    with st.expander("Filtros", expanded=True):
        try:
            admin_df = _load_admin_df()
        except Exception as exc:  # noqa: BLE001 - show message to user
            st.error(f"Erro ao carregar pedidos: {exc}")
            st.stop()

        if st.session_state.get(keys.ADMIN_RESET, False):
            _reset_admin_filters()

        row1c1, row1c2, row1c3, row1c4 = st.columns([1.4, 1.4, 2.2, 1.0])
        with row1c1:
            ts_valid = admin_df["TS_DT"].dropna()
            default_date = ts_valid.max().date() if not ts_valid.empty else datetime.now(TZ).date()
            st.date_input(
                "Data do pedido",
                value=st.session_state.get(keys.ADMIN_DATE_FILTER, default_date),
                key=keys.ADMIN_DATE_FILTER,
            )
        with row1c2:
            utd_opts = sorted([u for u in admin_df["UTD"].dropna().unique().tolist() if u], key=str.casefold)
            st.multiselect("UTD", options=utd_opts, key=keys.ADMIN_UTD_FILTER)
        with row1c3:
            base_opts = sorted([b for b in admin_df["BASE"].dropna().unique().tolist() if b], key=str.casefold)
            st.multiselect("BASE", options=base_opts, key=keys.ADMIN_BASE_FILTER)
        with row1c4:
            status_opts = list(STATUS_LABEL_MAP.values())
            st.multiselect("Status", options=status_opts, key=keys.ADMIN_STATUS_FILTER)

        row2c1 = st.columns([1.6])[0]
        with row2c1:
            st.text_input(
                "E-mail (contém)",
                placeholder="filtrar por parte do e-mail",
                key=keys.ADMIN_EMAIL_FILTER,
            )

        col_reset, _ = st.columns([1, 3])
        with col_reset:
            if st.button("🧭 Limpar filtros", use_container_width=True, key="btn_reset_admin"):
                st.session_state[keys.ADMIN_RESET] = True
                st.rerun()

    filtered_df = _apply_admin_filters(admin_df)
    filtered_df = filtered_df.sort_values("TS_DT", ascending=True)

    total_filtrados = len(filtered_df)
    st.caption(f"{total_filtrados} linha(s) após filtro.")

    sel_map = st.session_state[keys.CSV_SELECTION]
    st.session_state[keys.ADMIN_INDEX_TO_KEY] = {idx: rk for idx, rk in enumerate(filtered_df["_ROW_KEY"].tolist())}

    editor_df = filtered_df.copy()

    def _label_from_db_or_pending(idx: int, db_value_norm: str) -> str:
        key = st.session_state[keys.ADMIN_INDEX_TO_KEY].get(idx)
        if key and key in st.session_state[keys.ADMIN_PENDING_CHANGES]:
            return st.session_state[keys.ADMIN_PENDING_CHANGES][key]
        return STATUS_LABEL_MAP.get(str(db_value_norm).upper().strip(), "🟡 Pendente")

    editor_df["STATUS"] = [
        _label_from_db_or_pending(i, v) for i, v in enumerate(filtered_df["STATUS_NORM"].tolist())
    ]
    editor_df["DATA_HORA"] = filtered_df["DATA_HORA"]
    editor_df["SELECIONAR"] = [bool(sel_map.get(rk, True)) for rk in filtered_df["_ROW_KEY"]]

    cols_show = [
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

    st.data_editor(
        editor_df[cols_show],
        key=keys.ADMIN_EDITOR_KEY,
        on_change=lambda: _capture_admin_edits(keys.ADMIN_EDITOR_KEY),
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
                options=list(STATUS_LABEL_MAP.values()),
                help="As alterações só serão gravadas ao clicar em “Aplicar mudanças no HANA”.",
            ),
            "SELECIONAR": st.column_config.CheckboxColumn(
                "Selecionar p/ CSV",
                help="Marque apenas as linhas que devem ir para o CSV.",
            ),
        },
    )

    act1, act2, act3, act4 = st.columns([1.3, 1.6, 1.8, 2.5])
    with act1:
        pending_count = len(st.session_state[keys.ADMIN_PENDING_CHANGES])
        st.metric("Alterações pendentes", pending_count)
    with act2:
        approve_all_disabled = filtered_df.empty
        if st.button("🟢 Aprovar tudo", use_container_width=True, disabled=approve_all_disabled):
            for rk in filtered_df["_ROW_KEY"].tolist():
                st.session_state[keys.ADMIN_PENDING_CHANGES][rk] = "🟢 Aprovado"
            if keys.ADMIN_EDITOR_KEY in st.session_state:
                del st.session_state[keys.ADMIN_EDITOR_KEY]
            st.rerun()
    with act3:
        if st.button(
            "💾 Aplicar mudanças no HANA",
            type="primary",
            use_container_width=True,
            disabled=(pending_count == 0),
        ):
            try:
                current_df = _load_admin_df()
                updated = _apply_pending_changes(current_df, st.session_state.get(keys.ADMIN_EMAIL, ""))
                st.session_state[keys.ADMIN_LAST_APPLY] = updated
                st.session_state["show_csv_tools"] = True
                st.rerun()
            except Exception as exc:  # noqa: BLE001 - show message to user
                st.error(f"Falha ao atualizar status: {exc}")
    with act4:
        if st.button(
            "🗑️ Descartar mudanças pendentes",
            use_container_width=True,
            disabled=(pending_count == 0),
        ):
            st.session_state[keys.ADMIN_PENDING_CHANGES].clear()
            if keys.ADMIN_EDITOR_KEY in st.session_state:
                del st.session_state[keys.ADMIN_EDITOR_KEY]
            st.info("Alterações pendentes descartadas.")
            st.rerun()

    toggle_col = st.columns([1.2, 5])[0]
    with toggle_col:
        if st.button("🔁 Alternar seleção", use_container_width=True, disabled=(total_filtrados == 0)):
            _toggle_selection(filtered_df)
            st.rerun()

    if st.session_state.get("show_csv_tools", False):
        _render_csv_generation_tools(filtered_df)
    else:
        st.info("Aplique as mudanças no HANA para liberar a geração de CSV.")


if __name__ == "__main__":
    main()
