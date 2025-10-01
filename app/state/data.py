from __future__ import annotations

import pandas as pd
import streamlit as st
from hdbcli import dbapi

from app.services.dag40_service import load_dag40
from app.services.hana import HanaConfig
from app.settings import CACHE_PATH


@st.cache_data(show_spinner=False)
def _load_dag40_cached(path: str) -> pd.DataFrame:
    cfg = HanaConfig.from_env()
    return load_dag40(path, connector=dbapi.connect, config=cfg)


def get_dag40_dataframe() -> pd.DataFrame:
    """Return the DAG40 dataframe, caching it across reruns."""

    if "dag40_df" not in st.session_state:
        st.session_state["dag40_df"] = _load_dag40_cached(CACHE_PATH)
    return st.session_state["dag40_df"].copy()
