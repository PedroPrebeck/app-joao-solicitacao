from __future__ import annotations

import streamlit as st

from app.state import session_keys as keys


__all__ = ["handle_full_reset", "trigger_full_reset"]


def handle_full_reset() -> None:
    """Clear Streamlit session state when a full reset is requested."""

    if st.session_state.get(keys.FULL_RESET_FLAG, False):
        st.session_state.clear()
        st.session_state[keys.FULL_RESET_FLAG] = False
        st.rerun()


def trigger_full_reset() -> None:
    """Flag the current session to be fully reset on the next run."""

    st.session_state[keys.FULL_RESET_FLAG] = True
