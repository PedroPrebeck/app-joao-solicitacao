"""Streamlit cache helpers used across pages."""
from __future__ import annotations

import os
from functools import lru_cache

import pandas as pd
import streamlit as st
from hdbcli import dbapi

from app.services.dag40_service import load_dag40
from app.services.hana import HanaConfig
from app.services.pedidos_service import fetch_pedidos_with_labels


@lru_cache(maxsize=1)
def dag40_cache_path() -> str:
    """Return the path where the DAG40 cache should live."""

    return os.getenv("DAG40_CACHE_PATH", "dag40_cache.csv")


@st.cache_data(show_spinner=False)
def load_dag40_cached() -> pd.DataFrame:
    """Load the DAG40 dataframe from cache or the database."""

    cfg = HanaConfig.from_env()
    path = dag40_cache_path()
    return load_dag40(path, connector=dbapi.connect, config=cfg)


@st.cache_data(ttl=15, show_spinner=False)
def fetch_pedidos_cached() -> pd.DataFrame:
    """Fetch pedidos with status labels and cache them for a short period."""

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
    ]
    existing_cols = [c for c in desired_cols if c in df.columns]
    result = df[existing_cols].copy()
    if "TIMESTAMP" in result.columns:
        result = result.sort_values("TIMESTAMP", ascending=True)
    return result
