from __future__ import annotations

import pandas as pd
import streamlit as st
from hdbcli import dbapi

from app.services.hana import HanaConfig
from app.services.pedidos_service import fetch_pedidos_with_labels


@st.cache_data(ttl=15, show_spinner=False)
def fetch_all_pedidos_cached() -> pd.DataFrame:
    """Return all pedidos with human-friendly status labels."""

    cfg = HanaConfig.from_env()
    df = fetch_pedidos_with_labels(connector=dbapi.connect, config=cfg)
    if "PACOTES" in df.columns:
        df["PACOTES"] = pd.to_numeric(df["PACOTES"], errors="coerce").fillna(0).astype(int)
    if "STATUS" not in df.columns:
        df["STATUS"] = "EM ANALISE"
    for column in ["TURMA", "VALIDADO_POR"]:
        if column not in df.columns:
            df[column] = ""
    desired_cols = [
        "TIMESTAMP",
        "NOME",
        "E-MAIL",
        "CADEIA",
        "UTD",
        "BASE",
        "ZONA",
        "SERVICO",
        "PACOTES",
        "JUSTIFICATIVA",
        "COMENTARIOS",
        "STATUS",
        "TURMA",
        "VALIDADO_POR",
        "STATUS_LABEL",
    ]
    existing_cols = [c for c in desired_cols if c in df.columns]
    df = df[existing_cols]
    existing_cols = [c for c in desired_cols if c in df.columns]
    df = df[existing_cols]
    if "TIMESTAMP" not in df.columns:
        return df  # or: raise ValueError("TIMESTAMP column is required")
    return df.sort_values("TIMESTAMP", ascending=True)


def clear_pedidos_cache() -> None:
    """Invalidate the cached pedidos dataset."""

    fetch_all_pedidos_cached.clear()
