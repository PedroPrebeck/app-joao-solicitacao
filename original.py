from __future__ import annotations
import os
import re
import unicodedata
from datetime import datetime, date, time as dtime
from zoneinfo import ZoneInfo
from typing import Iterable, List, Dict, Any, Tuple
import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from hdbcli import dbapi

# --- AUTH imports ---
import yaml
from yaml.loader import SafeLoader
import streamlit_authenticator as stauth



# =============================== Config & Constantes ===============================
st.set_page_config(
    page_title="Geração de Notas de Cobrança - Painel de Solicitações",
    page_icon="🧾",
    layout="wide"
)

# Reset total (usado pelo botão "Limpar Tudo" e "Fazer outro pedido")
if st.session_state.get("_do_full_reset", False):
    st.session_state.pop("utds_sel", None)
    st.session_state.pop("utd_base_sel", None)
    st.session_state.clear()

st.title("Geração de Notas de Cobrança - Painel de Solicitações")
st.caption("Solicite a geração de notas por UTD, TURMA e BASE, com validações operacionais de horário.")
TZ = ZoneInfo("America/Bahia")
CACHE_PATH = os.getenv("DAG40_CACHE_PATH", "dag40_cache.csv")

DEFAULT_SERVICOS = ["Corte", "Corte Gavião", "Recorte", "Baixa", "Visita de Cobrança"]
servicos_opcoes = DEFAULT_SERVICOS

# Janela operacional
now = datetime.now(TZ)
after_10h00 = now.time() >= dtime(10, 0)
after_10h55 = now.time() >= dtime(10, 55)

# Opções para "Geração para"
BASE_GERACAO_OPCOES = ["HOJE", "AMANHÃ", "FIM DE SEMANA"]
geracao_opcoes = [o for o in BASE_GERACAO_OPCOES if not (after_10h55 and o == "HOJE")]
geracao_default = geracao_opcoes[0] if geracao_opcoes else "AMANHÃ"

# Banner informativo (após 10:00)
if after_10h00:
    st.info("⚠️ **Após 10:30 só serão aceitos pedidos para amanhã ou fim de semana. Pedidos para a cadeia noturna só serão aceitos até às 14:30**", icon="🕙")

# ================================ Sidebar (Instruções) ================================
with st.sidebar:
    st.header("Como usar o painel")
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
    st.caption("Dúvidas ou falhas? João Paulo (`joao.almeida@neoenergia.com`) ou Luiz Felipe (`luiz.espozel@neoenergia.com`).")
    st.divider()
    st.caption("Feito por: Pedro Azevedo (`pedro.azevedo@neoenergia.com`)")

# =================================== Utilidades ===================================
def strip_accents(s: str) -> str:
    if not isinstance(s, str):
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def strip_accents_and_punct_name(s: str) -> str:
    no_accents = strip_accents(s or "")
    cleaned = re.sub(r"[^A-Za-z\s]", " ", no_accents)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.upper()

def strip_accents_and_punct_action(s: str) -> str:
    no_accents = strip_accents(s or "")
    cleaned = re.sub(r"[^A-Za-z0-9\s]", " ", no_accents)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned.upper()

def is_valid_name(name: str) -> bool:
    cleaned = strip_accents_and_punct_name(name)
    return len([w for w in cleaned.split() if w]) >= 2

def is_valid_email(email: str) -> bool:
    if not isinstance(email, str):
        return False
    return bool(re.match(r"^[a-z0-9.\_%\+\-]+@neoenergia\.com$", email.strip().lower()))

def load_env():
    load_dotenv()
    return dict(
        host=os.getenv("HANA_HOST", ""),
        port=int(os.getenv("HANA_PORT", "30015")),
        user=os.getenv("HANA_USER", ""),
        password=os.getenv("HANA_PASS", ""),
    )

def get_hana_conn():
    cfg = load_env()
    if not (cfg["host"] and cfg["user"] and cfg["password"]):
        raise RuntimeError("Defina HANA_HOST, HANA_USER e HANA_PASS no .env.")
    return dbapi.connect(address=cfg["host"], port=cfg["port"], user=cfg["user"], password=cfg["password"])

# ============================== DAG40 (HANA + cache) ==============================
SQL_DAG40 = """
SELECT
  UTD40 AS UTD,
  BASE,
  ZONA,
  TURMA
FROM (
  SELECT DISTINCT
    UTD40,
    BASE_STC AS BASE,
    ID_BASE_STC AS ZONA,
    'STC' AS TURMA
  FROM CLB142840.DAG40
  WHERE BASE_STC NOT LIKE '%.%'
  UNION
  SELECT DISTINCT
    UTD40,
    BASE_EPS AS BASE,
    ID_BASE_EPS AS ZONA,
    'EPS' AS TURMA
  FROM CLB142840.DAG40
  WHERE BASE_EPS NOT LIKE '%.%'
)
WHERE UPPER(BASE) NOT LIKE UPPER('%MANUAL%')
  AND ZONA <> ''
ORDER BY ZONA
"""

@st.cache_data(show_spinner=False)
def fetch_dag40_from_hana_cached() -> pd.DataFrame:
    conn = None
    cur = None
    try:
        conn = get_hana_conn()
        cur = conn.cursor()
        cur.execute(SQL_DAG40)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        df = pd.DataFrame(rows, columns=cols).fillna("")
        return df.astype({"UTD": "string", "BASE": "string", "ZONA": "string", "TURMA": "string"})
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            if conn is not None:
                conn.close()

def ensure_cache(path: str) -> None:
    if os.path.exists(path):
        return
    df = fetch_dag40_from_hana_cached()
    df.to_csv(path, index=False, encoding="utf-8-sig")

@st.cache_data(show_spinner=False)
def load_dag40(path: str) -> pd.DataFrame:
    ensure_cache(path)
    if not os.path.exists(path):
        st.error("Não foi possível criar o cache DAG40. Verifique as credenciais do HANA (.env).")
        st.stop()
    df = pd.read_csv(path, dtype=str).fillna("")
    return df.astype({"UTD": "string", "BASE": "string", "ZONA": "string", "TURMA": "string"})

dag40_df = load_dag40(CACHE_PATH)

# ======================= Área restrita: constantes e helpers =======================
ALLOWED_ADMINS = {
    "joao.almeida@neoenergia.com",
    "luiz.espozel@neoenergia.com",
    "pedro.azevedo@neoenergia.com",
    "carlla.ventura@neoenergia.com",
    "ccsantos@neoenergia.com",
    "dsaraujo@neoenergia.com",
    "madson.melo@neoenergia.com",
    "jsbrito@neoenergia.com",
}

STATUS_DB_VALUES = ["EM ANALISE", "APROVADO", "RECUSADO"]
STATUS_LABEL_MAP = {
    "EM ANALISE": "🟡 Pendente",
    "APROVADO": "🟢 Aprovado",
    "RECUSADO": "🔴 Recusado",
}
STATUS_LABEL_INV = {v: k for k, v in STATUS_LABEL_MAP.items()}

STATUS_COLORS_BG = {
    "EM ANALISE": "#FFF6CC",
    "APROVADO": "#E8F5E9",
    "RECUSADO": "#FFEBEE",
}

# def _is_admin_authed():
#     return st.session_state.get("admin_authed", False)

# def _admin_login_ui():
#     st.subheader("Acesso restrito")
#     with st.form("admin_login_form", clear_on_submit=False):
#         email = st.text_input("E-mail corporativo", placeholder="voce@neoenergia.com")
#         pwd = st.text_input("Senha", type="password", placeholder="Senha de administrador")
#         submitted = st.form_submit_button("Entrar")
#         if submitted:
#             email_norm = email.strip().lower()
#             email_ok = (email_norm in ALLOWED_ADMINS)
#             pwd_ok = (pwd.strip() == ADMIN_PASSWORD)
#             if email_ok and pwd_ok:
#                 st.session_state["admin_authed"] = True
#                 st.session_state["admin_email"] = email_norm
#                 st.success("Acesso concedido.")
#                 st.rerun()
#             else:
#                 st.error("Credenciais inválidas. Verifique e tente novamente.")

# def _admin_logout():
#     st.session_state.pop("admin_authed", None)
#     st.session_state.pop("admin_email", None)
#     st.success("Sessão encerrada.")
#     st.rerun()

@st.cache_data(ttl=300, show_spinner=False)
def _table_has_col(colname: str) -> bool:
    conn = None
    cur = None
    try:
        conn = get_hana_conn()
        cur = conn.cursor()
        cur.execute('SELECT * FROM "U618488"."BASE_PEDIDOS" WHERE 1=0')
        cols = [c[0].upper() for c in cur.description]
        return colname.upper() in cols
    except Exception:
        return False
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            if conn is not None:
                conn.close()

def _table_has_status_col() -> bool:
    return _table_has_col("STATUS")

# =============================== Pedidos (HANA) ===============================
@st.cache_data(ttl=15, show_spinner=False)
def fetch_all_pedidos_cached() -> pd.DataFrame:
    has_status = _table_has_status_col()
    has_turma = _table_has_col("TURMA")
    has_validado_por = _table_has_col("VALIDADO_POR")
    select_cols = [
        '"TIMESTAMP"', '"NOME"', '"E-MAIL"', '"CADEIA"', '"UTD"',
        '"BASE"',  # [pos 5]
        '"ZONA"', '"SERVICO"', '"PACOTES"', '"JUSTIFICATIVA"', '"COMENTARIOS"'
    ]
    if has_turma:
        select_cols.insert(5, '"TURMA"')
    if has_status:
        select_cols.append('"STATUS"')
    if has_validado_por:
        select_cols.append('"VALIDADO_POR"')
    # [ALTERAÇÃO] Ordenação do mais antigo para o mais recente
    sql = f'SELECT {",".join(select_cols)} FROM "U618488"."BASE_PEDIDOS" ORDER BY "TIMESTAMP" ASC'
    conn = None
    cur = None
    try:
        conn = get_hana_conn()
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
        df = pd.DataFrame(rows, columns=cols)
        if "PACOTES" in df.columns:
            df["PACOTES"] = pd.to_numeric(df["PACOTES"], errors="coerce").fillna(0).astype(int)
        if "STATUS" not in df.columns:
            df["STATUS"] = "EM ANALISE"
        for c in ["TURMA", "VALIDADO_POR"]:
            if c not in df.columns:
                df[c] = ""
        return df
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            if conn is not None:
                conn.close()

def update_status_in_hana(changes: list[dict], admin_email: str | None = None) -> int:
    if not changes:
        return 0
    if not _table_has_status_col():
        return 0
    has_validado_por = _table_has_col("VALIDADO_POR")
    if has_validado_por:
        sql = """
        UPDATE "U618488"."BASE_PEDIDOS"
        SET "STATUS" = ?, "VALIDADO_POR" = ?
        WHERE "TIMESTAMP" = ?
          AND "NOME" = ?
          AND "E-MAIL" = ?
          AND "UTD" = ?
          AND "BASE" = ?
          AND "SERVICO" = ?
          AND "PACOTES" = ?
        """
        params = []
        for row in changes:
            new_status = row["STATUS"]
            validado_por = admin_email if new_status in ("APROVADO", "RECUSADO") else None
            params.append((
                new_status,
                validado_por,
                row["TIMESTAMP"], row["NOME"], row["E-MAIL"],
                row["UTD"], row["BASE"], row["SERVICO"], int(row["PACOTES"])
            ))
    else:
        sql = """
        UPDATE "U618488"."BASE_PEDIDOS"
        SET "STATUS" = ?
        WHERE "TIMESTAMP" = ?
          AND "NOME" = ?
          AND "E-MAIL" = ?
          AND "UTD" = ?
          AND "BASE" = ?
          AND "SERVICO" = ?
          AND "PACOTES" = ?
        """
        params = [
            (
                row["STATUS"],
                row["TIMESTAMP"], row["NOME"], row["E-MAIL"],
                row["UTD"], row["BASE"], row["SERVICO"], int(row["PACOTES"])
            )
            for row in changes
        ]
    conn = None
    cur = None
    try:
        conn = get_hana_conn()
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
        return len(params)
    finally:
        try:
            if cur is not None:
                cur.close()
        finally:
            if conn is not None:
                conn.close()

# ==================================== Abas ====================================
# [ALTERAÇÃO PEDIDA] ordem: Solicitar → Resumo → Gestão (restrito)
tab_solic, tab_resumo, tab_admin = st.tabs([
    "🧾 Solicitar Geração",
    "📋 Resumo de Pedidos",
    "🛡️ Gestão de Pedidos (restrito)"
])

# ==========================================================================================
# ABA: SOLICITAR
# ==========================================================================================
with tab_solic:
    # Identificação
    with st.container():
        col1, col2 = st.columns([1, 1])
        nome_input = col1.text_input("Seu nome*", placeholder="Nome e sobrenome", help="Ex.: MARIA SILVA")
        email_input = col2.text_input("Seu e-mail*", placeholder="voce@neoenergia.com", help="Somente domínio @neoenergia.com")

    # Seleções
    st.subheader("Seleção de UTD, TURMA e BASE", divider=True)
    utd_options = sorted([u for u in dag40_df["UTD"].dropna().unique().tolist() if u != ""], key=str.casefold)
    utds_sel = st.multiselect("UTDs*", options=utd_options, placeholder="Escolha uma ou mais UTDs", key="utds_sel")
    turma_sel = st.radio("TURMA*", options=["STC", "EPS"], horizontal=True, disabled=not utds_sel, key="turma_sel")

    # Mapa de BASEs por UTD
    if "utd_base_sel" not in st.session_state:
        st.session_state["utd_base_sel"] = {}
    for utd_key in list(st.session_state["utd_base_sel"].keys()):
        if utd_key not in utds_sel:
            del st.session_state["utd_base_sel"][utd_key]

    base_options_by_utd = {}
    if utds_sel and turma_sel:
        for utd in utds_sel:
            opts = (
                dag40_df[(dag40_df["UTD"] == utd) & (dag40_df["TURMA"] == turma_sel)]["BASE"]
                .dropna().unique().tolist()
            )
            base_options_by_utd[utd] = sorted([o for o in opts if o], key=str.casefold)

    st.markdown("#### BASEs por UTD")
    cols = st.columns(2)
    for i, utd in enumerate(utds_sel):
        with cols[i % 2]:
            current = [b for b in st.session_state["utd_base_sel"].get(utd, []) if b in base_options_by_utd.get(utd, [])]
            new_sel = st.multiselect(
                f"BASE(s) para **{utd}** ({turma_sel})",
                options=base_options_by_utd.get(utd, []),
                default=current,
                key=f"bases_for_{utd}",
                help="Selecione apenas as BASEs desta UTD."
            )
            st.session_state["utd_base_sel"][utd] = new_sel
    else:
        if not (utds_sel and turma_sel):
            st.session_state["utd_base_sel"] = {}

    # Helpers da Tabela
    COLUMNS_ALL = ["UTD","BASE","TURMA","GERACAO_PARA","SERVIÇO","PACOTES","JUSTIFICATIVA","COMENTARIO","ZONA"]
    COLUMNS_SHOW = ["UTD","BASE","TURMA","GERACAO_PARA","SERVIÇO","PACOTES","JUSTIFICATIVA","COMENTARIO"]
    EDITOR_KEY = "editor_lines_v2"

    def _empty_df():
        df = pd.DataFrame(columns=COLUMNS_ALL)
        df["PACOTES"] = pd.Series(dtype="int")
        for c in ["UTD","BASE","TURMA","GERACAO_PARA","SERVIÇO","JUSTIFICATIVA","COMENTARIO","ZONA"]:
            df[c] = pd.Series(dtype="string")
        return df

    if "lines_df" not in st.session_state:
        st.session_state["lines_df"] = _empty_df()

    def _clear_editor_state():
        if EDITOR_KEY in st.session_state:
            del st.session_state[EDITOR_KEY]

    def _zona_for(utd: str, base: str, turma: str) -> str:
        subset = dag40_df[(dag40_df["UTD"] == utd) & (dag40_df["BASE"] == base) & (dag40_df["TURMA"] == turma)]
        return subset["ZONA"].iloc[0] if not subset.empty else ""

    def _ensure_rows_for_selected_pairs():
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
                    zona = _zona_for(utd, base, turma_sel)
                    rows_to_add.append({
                        "UTD": utd, "BASE": base, "TURMA": turma_sel, "GERACAO_PARA": geracao_default,
                        "SERVIÇO": "", "PACOTES": 1, "JUSTIFICATIVA": "", "COMENTARIO": "", "ZONA": zona
                    })
        if not df.empty:
            df["KEY"] = df["UTD"].astype(str) + "\n" + df["BASE"].astype(str) + "\n" + df["TURMA"].astype(str)
            df = df[df["KEY"].isin(wanted_keys)].drop(columns=["KEY"])
        if rows_to_add:
            df = pd.concat([df, pd.DataFrame(rows_to_add)], ignore_index=True)
        if not df.equals(orig):
            st.session_state["lines_df"] = df
            _clear_editor_state()
        else:
            st.session_state["lines_df"] = df

    def _add_service_row_for_base(utd: str, base: str, turma: str, zona: str):
        df = st.session_state["lines_df"].copy()
        new_row = {
            "UTD": utd, "BASE": base, "TURMA": turma, "GERACAO_PARA": geracao_default,
            "SERVIÇO": "", "PACOTES": 1, "JUSTIFICATIVA": "", "COMENTARIO": "", "ZONA": zona
        }
        st.session_state["lines_df"] = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

    def _has_any_bases_selected() -> bool:
        sel = st.session_state.get("utd_base_sel", {})
        return any(len(v) > 0 for v in sel.values())

    # Renderização da tabela
    if utds_sel and turma_sel and _has_any_bases_selected():
        _ensure_rows_for_selected_pairs()
        st.subheader("Linhas por BASE", divider=True)
        with st.container(border=True):
            cols = st.columns(3)
            i = 0
            for utd, bases in st.session_state["utd_base_sel"].items():
                for base in bases:
                    zona_val = _zona_for(utd, base, turma_sel)
                    if cols[i % 3].button(
                        f"➕ Adicionar serviço • {base} (UTD {utd}, {turma_sel})",
                        key=f"add_{utd}_{base}",
                        use_container_width=True
                    ):
                        _add_service_row_for_base(utd, base, turma_sel, zona_val)
                    i += 1

        def _apply_editor_changes():
            ed_state = st.session_state.get(EDITOR_KEY, {})
            df = st.session_state["lines_df"].copy()
            deleted = ed_state.get("deleted_rows", [])
            if deleted:
                df = df.drop(df.index[deleted]).reset_index(drop=True)
            for r, changes in ed_state.get("edited_rows", {}).items():
                for col, val in changes.items():
                    if col in df.columns and col in COLUMNS_SHOW:
                        df.at[r, col] = val
            for new in ed_state.get("added_rows", []):
                base_row = {"UTD":"","BASE":"","TURMA":"","GERACAO_PARA":geracao_default,"SERVIÇO":"","PACOTES":1,"JUSTIFICATIVA":"","COMENTARIO":"","ZONA":""}
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
                "GERACAO_PARA": st.column_config.SelectboxColumn(
                    "Geração para", options=geracao_opcoes, required=True
                ),
                "SERVIÇO": st.column_config.SelectboxColumn(
                    "Serviço", options=servicos_opcoes, required=True,
                    help="• Corte Gavião ⇒ Disjuntor • Visita de Cobrança ⇒ COB.DOM"
                ),
                "PACOTES": st.column_config.NumberColumn("Pacotes", min_value=1, step=1, required=True),
                "JUSTIFICATIVA": st.column_config.TextColumn("Justificativa", required=True),
                "COMENTARIO": st.column_config.TextColumn("Comentário"),
            },
        )
    else:
        st.info("Selecione UTD(s), a TURMA e ao menos uma BASE para cada UTD.")

    # Validação e Insert
    def _validate_and_prepare_output() -> pd.DataFrame:
        if not is_valid_name(nome_input):
            raise ValueError("Informe ao menos um **Nome** e um **Sobrenome**.")
        if not is_valid_email(email_input):
            raise ValueError("Informe um **e-mail** válido do domínio **@neoenergia.com**.")
        df = st.session_state["lines_df"].copy()
        if df.empty:
            raise ValueError("Nenhuma linha para enviar. Adicione ao menos uma BASE e configure os serviços.")
        req_cols = ["GERACAO_PARA", "SERVIÇO", "PACOTES", "JUSTIFICATIVA"]
        for c in req_cols:
            if df[c].isna().any() or (df[c].astype(str).str.strip() == "").any():
                raise ValueError(f"Há linhas com **{c}** vazio.")
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
        out = out[["TIMESTAMP","NOME","E-MAIL","CADEIA","UTD","BASE","TURMA","ZONA","SERVIÇO","SERVICO_CLEAN","PACOTES","JUSTIFICATIVA","COMENTARIO"]]
        return out

    def insert_rows_hana(df_final: pd.DataFrame) -> int:
        has_turma = _table_has_col("TURMA")
        if has_turma:
            sql = """
            INSERT INTO "U618488"."BASE_PEDIDOS"
            ("TIMESTAMP","NOME","E-MAIL","CADEIA","UTD","BASE","TURMA","ZONA","SERVICO","PACOTES","NOTAS","JUSTIFICATIVA","COMENTARIOS")
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """
            params = [
                (
                    row["TIMESTAMP"], row["NOME"], row["E-MAIL"], row["CADEIA"],
                    row["UTD"], row["BASE"], row["TURMA"], row["ZONA"],
                    row["SERVICO_CLEAN"], int(row["PACOTES"]),
                    None,
                    (str(row["JUSTIFICATIVA"]).strip() or None) if pd.notna(row["JUSTIFICATIVA"]) else None,
                    (str(row["COMENTARIO"]).strip() or None) if pd.notna(row["COMENTARIO"]) else None,
                )
                for _, row in df_final.iterrows()
            ]
        else:
            sql = """
            INSERT INTO "U618488"."BASE_PEDIDOS"
            ("TIMESTAMP","NOME","E-MAIL","CADEIA","UTD","BASE","ZONA","SERVICO","PACOTES","NOTAS","JUSTIFICATIVA","COMENTARIOS")
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
            """
            params = [
                (
                    row["TIMESTAMP"], row["NOME"], row["E-MAIL"], row["CADEIA"],
                    row["UTD"], row["BASE"], row["ZONA"],
                    row["SERVICO_CLEAN"], int(row["PACOTES"]),
                    None,
                    (str(row["JUSTIFICATIVA"]).strip() or None) if pd.notna(row["JUSTIFICATIVA"]) else None,
                    (str(row["COMENTARIO"]).strip() or None) if pd.notna(row["COMENTARIO"]) else None,
                )
                for _, row in df_final.iterrows()
            ]
        conn = None
        cur = None
        try:
            conn = get_hana_conn()
            cur = conn.cursor()
            cur.executemany(sql, params)
            conn.commit()
            return len(params)
        finally:
            try:
                if cur is not None:
                    cur.close()
            finally:
                if conn is not None:
                    conn.close()

    @st.dialog("Sua solicitação foi enviada com sucesso", width="large")
    def _show_success_dialog():
        qtd = st.session_state.get("success_qtd", 0)
        nome = st.session_state.get("success_nome", "")
        email = st.session_state.get("success_email", "")
        resumo_df = st.session_state.get("success_resumo")
        st.success(f"✅ Solicitações enviadas com sucesso ({qtd} linha(s)).")
        if nome or email:
            st.caption(f"**Solicitante:** {nome} • {email}")
        if isinstance(resumo_df, pd.DataFrame) and not resumo_df.empty:
            resumo_show = resumo_df.rename(columns={
                "UTD": "UTD",
                "BASE": "BASE",
                "TURMA": "TURMA",
                "GERACAO_PARA": "Geração para",
                "SERVIÇO": "Serviço",
                "PACOTES": "Pacotes",
                "JUSTIFICATIVA": "Justificativa",
                "COMENTARIO": "Comentário",
            })
            st.dataframe(resumo_show, use_container_width=True, hide_index=True)
        else:
            st.info("Não foi possível montar o resumo da solicitação.")
        st.divider()
        # [ALTERAÇÃO PEDIDA] remover botão "Fechar" e a dica do "X"
        if st.button("➕ Fazer outro pedido", use_container_width=True):
            st.session_state["_do_full_reset"] = True
            st.rerun()

    # Ações (Enviar / Limpar)
    st.divider()
    col_a, col_b = st.columns([1, 1])
    can_send = (
        bool(strip_accents_and_punct_name(nome_input)) and
        bool(email_input.strip()) and
        not st.session_state["lines_df"].empty
    )
    if col_a.button("📨 Enviar Solicitação", type="primary", use_container_width=True, disabled=not can_send):
        try:
            out_df = _validate_and_prepare_output()
            inserted = insert_rows_hana(out_df)
            resumo_cols = ["UTD","BASE","TURMA","GERACAO_PARA","SERVIÇO","PACOTES","JUSTIFICATIVA","COMENTARIO"]
            resumo_df = st.session_state["lines_df"][resumo_cols].copy()
            resumo_df["PACOTES"] = pd.to_numeric(resumo_df["PACOTES"], errors="coerce").fillna(0).astype(int)
            st.session_state["success_qtd"] = inserted
            st.session_state["success_nome"] = strip_accents_and_punct_name(nome_input)
            st.session_state["success_email"] = email_input.strip().lower()
            st.session_state["success_resumo"] = resumo_df
            # limpar caches de leitura
            st.cache_data.clear()
            _show_success_dialog()
        except Exception as e:
            st.error(f"Falha ao enviar: {e}")

    if col_b.button("🧹 Limpar Tudo", use_container_width=True):
        st.session_state["_do_full_reset"] = True
        st.rerun()

# ==========================================================================================
# ABA: RESUMO (NÃO RESTRITO)
# ==========================================================================================
with tab_resumo:
    st.subheader("Resumo de Pedidos", divider=True)
    # [ALTERAÇÃO] atualização automática com cache TTL
    try:
        resumo_df = fetch_all_pedidos_cached()
    except Exception as e:
        st.error(f"Erro ao carregar pedidos: {e}")
        st.stop()

    resumo_df["TS_DT"] = pd.to_datetime(resumo_df["TIMESTAMP"], errors="coerce")
    resumo_df["DATA_HORA"] = resumo_df["TS_DT"].dt.strftime("%d/%m/%Y %H:%M")

    if st.session_state.get("reset_filters_resumo", False):
        for k in ["f_data_resumo", "f_utd_resumo", "f_base_resumo", "f_email_resumo"]:
            st.session_state.pop(k, None)
        st.session_state["reset_filters_resumo"] = False

    with st.expander("Filtros", expanded=True):
        rrow1c1, rrow1c2, rrow1c3 = st.columns([1.6, 1.6, 2.3])
        with rrow1c1:
            ts_valid_r = resumo_df["TS_DT"].dropna()
            default_date_r = (ts_valid_r.max().date() if not ts_valid_r.empty else datetime.now(TZ).date())
            st.date_input("Data do pedido", value=st.session_state.get("f_data_resumo", default_date_r), key="f_data_resumo")
        with rrow1c2:
            utd_opts_r = sorted([u for u in resumo_df["UTD"].dropna().unique().tolist() if u], key=str.casefold)
            st.multiselect("UTD", options=utd_opts_r, key="f_utd_resumo")
        with rrow1c3:
            base_opts_r = sorted([b for b in resumo_df["BASE"].dropna().unique().tolist() if b], key=str.casefold)
            st.multiselect("BASE", options=base_opts_r, key="f_base_resumo")
        rrow2c1 = st.columns([1.6])[0]
        with rrow2c1:
            st.text_input("E-mail (contém)", placeholder="filtrar por parte do e-mail", key="f_email_resumo")
        rcolx1, _ = st.columns([1, 3])
        with rcolx1:
            if st.button("🧭 Limpar filtros", use_container_width=True, key="btn_reset_resumo"):
                st.session_state["reset_filters_resumo"] = True
                st.rerun()

    resumo_f = resumo_df.copy()
    try:
        if isinstance(st.session_state.get("f_data_resumo", None), date):
            rd_sel = st.session_state["f_data_resumo"]
            resumo_f = resumo_f[resumo_f["TS_DT"].dt.date == rd_sel]
    except Exception:
        pass
    if st.session_state.get("f_utd_resumo"):
        resumo_f = resumo_f[resumo_f["UTD"].isin(st.session_state["f_utd_resumo"])]
    if st.session_state.get("f_base_resumo"):
        resumo_f = resumo_f[resumo_f["BASE"].isin(st.session_state["f_base_resumo"])]
    email_contains_r = (st.session_state.get("f_email_resumo", "") or "").strip().lower()
    if email_contains_r:
        resumo_f = resumo_f[resumo_f["E-MAIL"].astype(str).str.lower().str.contains(email_contains_r, na=False)]

    # [ALTERAÇÃO] ordenar do mais antigo para o mais recente
    resumo_f = resumo_f.sort_values("TS_DT", ascending=True)

    show_cols_resumo = ["DATA_HORA","NOME","E-MAIL","UTD","BASE","TURMA","SERVICO","PACOTES","CADEIA","JUSTIFICATIVA","COMENTARIOS"]
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
    resumo_show = resumo_f[show_cols_resumo].rename(columns=pretty_names)
    if resumo_show.empty:
        st.info("Nenhum registro encontrado para os filtros selecionados.")
    st.dataframe(resumo_show, use_container_width=True, hide_index=True)

# ==========================================================================================
# ABA: GESTÃO (RESTRITO) — versão final com correções de chave e UX
# ==========================================================================================





with tab_admin:
    st.subheader("Gestão de Pedidos", divider=True)

    # ✅ Cache APENAS do YAML (não cria widgets)
    @st.cache_data
    def load_auth_config():
        with open(".streamlit/auth_config.yaml", "r", encoding="utf-8") as f:
            return yaml.load(f, Loader=SafeLoader)

    cfg = load_auth_config()

    authenticator = stauth.Authenticate(
        credentials=cfg["credentials"],
        cookie_name=cfg["cookie"]["name"],
        cookie_key=cfg["cookie"]["key"],
        cookie_expiry_days=cfg["cookie"]["expiry_days"],
    )

    # --- LOGIN persistente (0.4.x) ---
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

    # ==========================
    # CABEÇALHO em container próprio
    # ==========================
    with st.container():
        hdr_left, hdr_right = st.columns([2, 2])
        with hdr_left:
            st.caption(f"Logado como: **{st.session_state.get('admin_email','')}**")
        with hdr_right:
            b1, b2 = st.columns(2)
            with b1:
                if st.button("🔄 Atualizar dados", use_container_width=True):
                    st.cache_data.clear()
                    st.rerun()
            with b2:
                authenticator.logout("Sair", "main", key="logout_admin")

    # >>> A PARTIR DAQUI, TUDO FORA DE 'with hdr_right' E FORA DE 'with st.columns(...)' <<<
    st.divider()

    # ... (seu código existente de mensagens pós-aplicar, estados, Filtros, Grid, Ações, CSV etc.)




    # Mensagem pós-aplicação (persistência no HANA)
    if "admin_last_apply_success" in st.session_state:
        upd_count = st.session_state.pop("admin_last_apply_success", None)
        if upd_count is not None:
            st.success(f"✅ Mudanças aplicadas no HANA com sucesso ({upd_count} linha(s) atualizada(s)).")
            st.session_state["show_csv_tools"] = True  # mantém o fluxo original

    # Estados básicos
    st.session_state.setdefault("admin_pending_changes", {})
    st.session_state.setdefault("admin_index_to_key", {})
    st.session_state.setdefault("csv_row_selection", {})  # mapa _ROW_KEY -> bool (selecionado p/ CSV)

    # ---------------------------------- Filtros ----------------------------------
    with st.expander("Filtros", expanded=True):
        try:
            admin_df = fetch_all_pedidos_cached()
        except Exception as e:
            st.error(f"Erro ao carregar pedidos: {e}")
            st.stop()

        status_norm = admin_df["STATUS"].fillna("").astype(str).str.upper().str.strip()
        admin_df["STATUS_NORM"] = status_norm.where(~status_norm.isin(["", "NAN", "NONE", "NULL"]), "EM ANALISE")
        admin_df["TS_DT"] = pd.to_datetime(admin_df["TIMESTAMP"], errors="coerce")
        admin_df["DATA_HORA"] = admin_df["TS_DT"].dt.strftime("%d/%m/%Y %H:%M")

        if st.session_state.get("reset_filters_admin", False):
            for k in ["f_data_admin", "f_utd_admin", "f_base_admin", "f_email_admin", "f_status_admin"]:
                st.session_state.pop(k, None)
            st.session_state["reset_filters_admin"] = False

        row1c1, row1c2, row1c3, row1c4 = st.columns([1.4, 1.4, 2.2, 1.0])
        with row1c1:
            ts_valid = admin_df["TS_DT"].dropna()
            default_date = (ts_valid.max().date() if not ts_valid.empty else datetime.now(TZ).date())
            st.date_input("Data do pedido", value=st.session_state.get("f_data_admin", default_date), key="f_data_admin")
        with row1c2:
            utd_opts = sorted([u for u in admin_df["UTD"].dropna().unique().tolist() if u], key=str.casefold)
            st.multiselect("UTD", options=utd_opts, key="f_utd_admin")
        with row1c3:
            base_opts = sorted([b for b in admin_df["BASE"].dropna().unique().tolist() if b], key=str.casefold)
            st.multiselect("BASE", options=base_opts, key="f_base_admin")
        with row1c4:
            st.text_input("E-mail (contém)", placeholder="parte do e-mail", key="f_email_admin")

        row2c1, row2c2 = st.columns([1.6, 1.1])
        with row2c1:
            status_label_options = list(STATUS_LABEL_INV.keys())
            st.multiselect("Status", options=status_label_options, default=[], help="Sem seleção = todos.", key="f_status_admin")
        with row2c2:
            if st.button("🧹 Limpar filtros", use_container_width=True):
                st.session_state["reset_filters_admin"] = True
                if "admin_editor_v2" in st.session_state:
                    del st.session_state["admin_editor_v2"]
                st.rerun()

        # Aplica filtros
        filtered_df = admin_df.copy()
        try:
            if isinstance(st.session_state.get("f_data_admin", None), date):
                d_sel = st.session_state["f_data_admin"]
                filtered_df = filtered_df[filtered_df["TS_DT"].dt.date == d_sel]
        except Exception:
            pass

        sel_labels = st.session_state.get("f_status_admin", [])
        if sel_labels:
            sel_db_statuses = {STATUS_LABEL_INV.get(lbl, "EM ANALISE") for lbl in sel_labels}
            filtered_df = filtered_df[filtered_df["STATUS_NORM"].isin(sel_db_statuses)]

        if st.session_state.get("f_utd_admin", []):
            filtered_df = filtered_df[filtered_df["UTD"].isin(st.session_state["f_utd_admin"])]

        if st.session_state.get("f_base_admin", []):
            filtered_df = filtered_df[filtered_df["BASE"].isin(st.session_state["f_base_admin"])]

        email_contains = (st.session_state.get("f_email_admin", "") or "").strip().lower()
        if email_contains:
            filtered_df = filtered_df[filtered_df["E-MAIL"].astype(str).str.lower().str.contains(email_contains, na=False)]

        # Ordenação consistente
        filtered_df = filtered_df.sort_values("TS_DT", ascending=True)

        # Chave estável da linha (usada na grade e depois no gerador)
        def _make_row_key(row: pd.Series) -> str:
            return f'{row["TIMESTAMP"]}\n{row["NOME"]}\n{row["E-MAIL"]}\n{row["UTD"]}\n{row["BASE"]}\n{row["SERVICO"]}\n{row["PACOTES"]}'

        filtered_df = filtered_df.copy()
        filtered_df["_ROW_KEY"] = filtered_df.apply(_make_row_key, axis=1)
        st.session_state["admin_index_to_key"] = dict(enumerate(filtered_df["_ROW_KEY"].tolist()))

        # -------------------------------- Seleção p/ CSV --------------------------------
        # Prepara defaults de seleção para novas linhas filtradas
        sel_map = st.session_state["csv_row_selection"]
        for rk in filtered_df["_ROW_KEY"]:
            sel_map.setdefault(rk, True)  # padrão: incluir no CSV

        # Contadores de seleção (filtrados)
        total_filtrados = len(filtered_df)
        selecionadas_filtrados = sum(bool(sel_map.get(rk, True)) for rk in filtered_df["_ROW_KEY"])
        sel_bar1, sel_bar2, sel_bar3, sel_bar4 = st.columns([1.3, 1.6, 1.6, 1.5])
        with sel_bar1:
            st.metric("Linhas", total_filtrados)
        with sel_bar2:
            st.metric("Selecionadas p/ CSV", selecionadas_filtrados)
        with sel_bar3:
            if st.button("✅ Selecionar tudo", use_container_width=True, disabled=(total_filtrados == 0)):
                for rk in filtered_df["_ROW_KEY"]:
                    st.session_state["csv_row_selection"][rk] = True
                if "admin_editor_v2" in st.session_state:
                    del st.session_state["admin_editor_v2"]
                st.rerun()
        with sel_bar4:
            if st.button("🚫 Desmarcar tudo", use_container_width=True, disabled=(total_filtrados == 0)):
                for rk in filtered_df["_ROW_KEY"]:
                    st.session_state["csv_row_selection"][rk] = False
                if "admin_editor_v2" in st.session_state:
                    del st.session_state["admin_editor_v2"]
                st.rerun()

        inv_bar = st.columns([1.2, 5])[0]
        with inv_bar:
            if st.button("🔁 Alternar seleção", use_container_width=True, disabled=(total_filtrados == 0)):
                for rk in filtered_df["_ROW_KEY"]:
                    st.session_state["csv_row_selection"][rk] = not bool(st.session_state["csv_row_selection"].get(rk, True))
                if "admin_editor_v2" in st.session_state:
                    del st.session_state["admin_editor_v2"]
                st.rerun()

        # ----------------------------------- Grade -------------------------------------
        ADMIN_EDITOR_KEY = "admin_editor_v2"
        editor_df = filtered_df.copy()

        def _label_from_db_or_pending(idx: int, db_value_norm: str) -> str:
            key = st.session_state["admin_index_to_key"].get(idx)
            if key and key in st.session_state["admin_pending_changes"]:
                return st.session_state["admin_pending_changes"][key]
            return STATUS_LABEL_MAP.get(str(db_value_norm).upper().strip(), "🟡 Pendente")

        editor_df["STATUS"] = [
            _label_from_db_or_pending(i, v) for i, v in enumerate(filtered_df["STATUS_NORM"].tolist())
        ]
        editor_df["DATA_HORA"] = filtered_df["DATA_HORA"]
        editor_df["SELECIONAR"] = [bool(sel_map.get(rk, True)) for rk in filtered_df["_ROW_KEY"]]

        cols_show = [
            "DATA_HORA","NOME","E-MAIL","UTD","BASE","TURMA","SERVICO","PACOTES","CADEIA","JUSTIFICATIVA","COMENTARIOS","STATUS","SELECIONAR"
        ]

        def _capture_admin_edits():
            ed_state = st.session_state.get(ADMIN_EDITOR_KEY, {})
            if not ed_state:
                return
            edited = ed_state.get("edited_rows", {})
            for idx, changes in edited.items():
                # Status (pendente -> sessão)
                if "STATUS" in changes:
                    key = st.session_state["admin_index_to_key"].get(idx)
                    if key:
                        st.session_state["admin_pending_changes"][key] = changes["STATUS"]
                # Seleção p/ CSV (sessão)
                if "SELECIONAR" in changes:
                    key = st.session_state["admin_index_to_key"].get(idx)
                    if key:
                        st.session_state["csv_row_selection"][key] = bool(changes["SELECIONAR"])

        st.data_editor(
            editor_df[cols_show],
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

        # ------------------------------ Ações de Status --------------------------------
        act1, act2, act3, act4 = st.columns([1.3, 1.6, 1.8, 2.5])
        with act1:
            pending_count = len(st.session_state["admin_pending_changes"])
            st.metric("Alterações pendentes", pending_count)
        with act2:
            approve_all_disabled = filtered_df.empty
            if st.button("🟢 Aprovar tudo", use_container_width=True, disabled=approve_all_disabled):
                for rk in filtered_df["_ROW_KEY"].tolist():
                    st.session_state["admin_pending_changes"][rk] = "🟢 Aprovado"
                if ADMIN_EDITOR_KEY in st.session_state:
                    del st.session_state[ADMIN_EDITOR_KEY]
                st.rerun()
        with act3:
            if st.button("💾 Aplicar mudanças no HANA", type="primary", use_container_width=True, disabled=(pending_count == 0)):
                if not _table_has_status_col():
                    st.warning("Coluna STATUS não existe no HANA. Crie a coluna para persistir as alterações.")
                else:
                    try:
                        current_df = fetch_all_pedidos_cached()
                        curr_norm = current_df["STATUS"].fillna("").astype(str).str.upper().str.strip()
                        current_df["STATUS_NORM"] = curr_norm.where(~curr_norm.isin(["", "NAN", "NONE", "NULL"]), "EM ANALISE")
                        current_df["_ROW_KEY"] = current_df.apply(_make_row_key, axis=1)
                        key_to_row = {rk: i for i, rk in enumerate(current_df["_ROW_KEY"].tolist())}
                        changes = []
                        for key, new_label in st.session_state["admin_pending_changes"].items():
                            idx = key_to_row.get(key)
                            if idx is None:
                                continue
                            new_db_val = STATUS_LABEL_INV.get(new_label, "EM ANALISE")
                            old_db_val = str(current_df.loc[idx, "STATUS_NORM"]).upper().strip()
                            if new_db_val != old_db_val:
                                row = current_df.loc[idx, ["TIMESTAMP","NOME","E-MAIL","UTD","BASE","SERVICO","PACOTES"]].to_dict()
                                row["STATUS"] = new_db_val
                                changes.append(row)
                        updated = update_status_in_hana(changes, admin_email=st.session_state.get("admin_email")) if changes else 0
                        st.session_state["admin_pending_changes"].clear()
                        if ADMIN_EDITOR_KEY in st.session_state:
                            del st.session_state[ADMIN_EDITOR_KEY]
                        st.cache_data.clear()
                        st.session_state["admin_last_apply_success"] = updated
                        st.session_state["show_csv_tools"] = True
                        st.rerun()
                    except Exception as e:
                        st.error(f"Falha ao atualizar status: {e}")
        with act4:
            if st.button("🗑️ Descartar mudanças pendentes", use_container_width=True, disabled=(pending_count == 0)):
                st.session_state["admin_pending_changes"].clear()
                if ADMIN_EDITOR_KEY in st.session_state:
                    del st.session_state[ADMIN_EDITOR_KEY]
                st.info("Alterações pendentes descartadas.")
                st.rerun()

        # --------------------------- Geração de CSV (pós-aplicar) -----------------------
        @st.cache_data(ttl=30, show_spinner=False)
        def _run_cluster_config_query(sel_date: date, turmas: Iterable[str], carteiras_db: Iterable[str]) -> pd.DataFrame:
            """
            [CORREÇÃO CHAVE CSV] inclui PACOTES para casar exatamente com a chave do grid.
            """
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
    BP.PACOTES AS CLUSTERS,           -- usado no CSV final
    BP.PACOTES AS PACOTES,            -- [CORREÇÃO] incluído para chave do grid
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
            conn = None
            cur = None
            try:
                conn = get_hana_conn()
                cur = conn.cursor()
                cur.execute(sql, params)
                rows = cur.fetchall()
                cols = [c[0] for c in cur.description]
                df = pd.DataFrame(rows, columns=cols).fillna("")
                return df
            finally:
                try:
                    if cur is not None:
                        cur.close()
                finally:
                    if conn is not None:
                        conn.close()

        def _render_csv_generation_tools():
            st.subheader("Gerar arquivos de configuração (CSV)", divider=True)
            st.caption("Selecione a **data**, **TURMA(s)** e **Carteira(s)**. Um arquivo será gerado para cada combinação.")

            ts_valid = admin_df["TS_DT"].dropna()
            default_date = (ts_valid.max().date() if not ts_valid.empty else datetime.now(TZ).date())
            gen_date = st.date_input("Data do pedido (para geração)", value=default_date, key="csv_gen_date")

            colt1, colt2 = st.columns([1.2, 2])
            with colt1:
                turmas_sel = st.multiselect("TURMA(s)", options=["STC", "EPS"], default=["STC"], key="csv_gen_turmas")
            with colt2:
                carteiras_ui = [
                    ("Corte", "CONVENCIONAL"),
                    ("Recorte", "RECORTE"),
                    ("Baixa", "BAIXA"),
                    ("Disjuntor (Corte Gavião)", "DISJUNTOR"),
                    ("Cobrança domiciliar (COB.DOM)", "COB.DOM"),
                ]
                label_to_db = {lbl: db for lbl, db in carteiras_ui}
                default_labels = ["Corte"]
                carteiras_labels_sel = st.multiselect(
                    "Carteira(s)",
                    options=[lbl for lbl, _ in carteiras_ui],
                    default=default_labels,
                    key="csv_gen_carteiras_labels"
                )
                carteiras_db_sel: List[str] = [label_to_db[lbl] for lbl in carteiras_labels_sel]

            # (A) Opção clara de exportação
            excluir_desmarcados = st.checkbox(
                "Excluir do arquivo as linhas desmarcadas (SELECIONAR=NAO)",
                value=True,  # padrão alinhado ao que você pediu
                key="csv_excluir_desmarcados"
            )

            disabled_gen = (not isinstance(gen_date, date)) or (not turmas_sel) or (not carteiras_db_sel)

            # ---------- Estado persistente dos downloads ----------
            st.session_state.setdefault("csv_gen_state", {
                "date": None, "turmas": [], "carteiras": [], "df_all": pd.DataFrame(), "ready": False
            })

            def _store_csv_state(df_all: pd.DataFrame, dt: date, turmas: List[str], carteiras: List[str]):
                st.session_state["csv_gen_state"] = {
                    "date": dt,
                    "turmas": list(turmas),
                    "carteiras": list(carteiras),
                    "df_all": df_all.copy(),
                    "ready": not df_all.empty,
                }

            def _state_matches_current() -> bool:
                s = st.session_state["csv_gen_state"]
                return (
                    s.get("ready", False)
                    and s.get("date") == gen_date
                    and s.get("turmas") == list(turmas_sel)
                    and s.get("carteiras") == list(carteiras_db_sel)
                    and isinstance(s.get("df_all"), pd.DataFrame)
                    and not s["df_all"].empty
                )

            # (B) Geração
            if st.button("🧾 Gerar arquivos de configuração", type="primary", disabled=disabled_gen):
                try:
                    df_all = _run_cluster_config_query(gen_date, turmas_sel, carteiras_db_sel)
                    if df_all.empty:
                        st.warning("Nenhum dado encontrado para os filtros selecionados.")
                        _store_csv_state(pd.DataFrame(), gen_date, turmas_sel, carteiras_db_sel)
                    else:
                        # ===== [CORREÇÃO CHAVE CSV] usar a MESMA chave do grid =====
                        df_all_for_key = pd.DataFrame({
                            "TIMESTAMP": df_all["TS"],         # TIMESTAMP do gerador
                            "NOME": df_all["NOME"],
                            "E-MAIL": df_all["EMAIL"],         # mesmo nome com hífen
                            "UTD": df_all["UTD"],
                            "BASE": df_all["BASE"],
                            "SERVICO": df_all["SERVICO"],
                            "PACOTES": pd.to_numeric(df_all["PACOTES"], errors="coerce").fillna(0).astype(int),
                        })
                        df_all["_ROW_KEY"] = df_all_for_key.apply(_make_row_key, axis=1)

                        sel_map = st.session_state.get("csv_row_selection", {})
                        df_all["SELECIONAR"] = df_all["_ROW_KEY"].map(lambda k: "SIM" if sel_map.get(k, True) else "NAO")

                        # (C) Se “excluir desmarcados”, mantém apenas os SIM
                        if st.session_state.get("csv_excluir_desmarcados", True):
                            df_all = df_all[df_all["SELECIONAR"].str.upper() == "SIM"].copy()
                            if df_all.empty:
                                st.warning("Nenhuma linha marcada para exportação com os filtros atuais.")

                        _store_csv_state(df_all, gen_date, turmas_sel, carteiras_db_sel)
                        st.success("Arquivos prontos para download abaixo.")
                except Exception as e:
                    st.error(f"Falha ao gerar CSV: {e}")

            # (D) Downloads + limpeza de colunas auxiliares
            if _state_matches_current():
                df_all = st.session_state["csv_gen_state"]["df_all"]

                def carteira_suffix(db_code: str) -> str:
                    if db_code.upper() == "COB.DOM":
                        return "domiciliar"
                    if db_code.upper() == "DISJUNTOR":
                        return "disjuntor"
                    return db_code.lower()

                turmas_presentes = sorted(df_all["TURMA"].dropna().unique().tolist())
                carteiras_presentes = sorted(df_all["CARTEIRA"].dropna().unique().tolist())

                # pré-visualização opcional
                with st.expander("Prévia de contagem por TURMA x CARTEIRA", expanded=False):
                    for turma in turmas_presentes:
                        row = []
                        for cart in carteiras_presentes:
                            n = int(((df_all["TURMA"] == turma) & (df_all["CARTEIRA"] == cart)).sum())
                            row.append(f"{cart}: {n}")
                        st.write(f"**{turma}** → " + " | ".join(row))

                for turma in turmas_presentes:
                    for cart_db in carteiras_presentes:
                        df_part = df_all[(df_all["TURMA"] == turma) & (df_all["CARTEIRA"] == cart_db)].copy()
                        if df_part.empty:
                            continue

                        # Remove colunas auxiliares (inclusive SELECIONAR quando excluímos desmarcados)
                        cols_drop = ["TS", "_ROW_KEY", "NOME", "EMAIL", "BASE", "SERVICO", "PACOTES"]
                        df_part = df_part.drop(columns=[c for c in cols_drop if c in df_part.columns])

                        # Inteiros sem ".0"
                        for c in ["CLUSTERS", "QTD_MAX", "QTD_MIN", "RAIO_IDEAL", "RAIO_MAX", "RAIO_STEP"]:
                            if c in df_part.columns:
                                df_part[c] = pd.to_numeric(df_part[c], errors="coerce").astype("Int64")

                        csv_str = df_part.to_csv(index=False, sep=';', encoding="utf-8-sig", na_rep='')
                        csv_bytes = csv_str.encode("utf-8-sig")
                        fname = f"config_{turma.lower()}_{carteira_suffix(cart_db)}.csv"
                        st.download_button(
                            label=f"⬇️ Baixar {fname}",
                            data=csv_bytes,
                            file_name=fname,
                            mime="text/csv",
                            use_container_width=True,
                        )
            else:
                st.info("Defina os filtros e clique em **Gerar arquivos de configuração**.")

        # Renderiza a seção de CSV apenas após o fluxo de aplicar mudanças (mantém a lógica existente)
        if st.session_state.get("show_csv_tools", False):
            _render_csv_generation_tools()
        else:
            st.info("Aplique as mudanças no HANA para liberar a geração de CSV.")