"""Helpers related to the administrator authentication flow."""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Dict

import streamlit as st
import streamlit_authenticator as stauth
import yaml
from yaml.loader import SafeLoader


@st.cache_data(show_spinner=False)
def load_auth_config(path: str = ".streamlit/auth_config.yaml") -> Dict[str, Any]:
    """Load the authentication configuration file."""

    cfg_path = Path(path)
    if not cfg_path.exists():
        raise FileNotFoundError("Arquivo de configuração de autenticação não encontrado.")

    with cfg_path.open("r", encoding="utf-8") as f:
        return yaml.load(f, Loader=SafeLoader)


@lru_cache(maxsize=1)
def authenticator_from_config(config: Dict[str, Any]) -> stauth.Authenticate:
    """Create a Streamlit authenticator instance from a config dict."""

    credentials = config.get("credentials", {})
    cookie_cfg = config.get("cookie", {})
    return stauth.Authenticate(
        credentials=credentials,
        cookie_name=cookie_cfg.get("name", "auth"),
        cookie_key=cookie_cfg.get("key", "auth"),
        cookie_expiry_days=cookie_cfg.get("expiry_days", 1),
    )
