from __future__ import annotations

import streamlit as st


SESSION_RESET_KEY = "_do_full_reset"


def handle_full_reset() -> None:
    """Clear Streamlit session state when a full reset is requested."""

    if st.session_state.get(SESSION_RESET_KEY, False):
        st.session_state.clear()
        st.session_state[SESSION_RESET_KEY] = False


def trigger_full_reset() -> None:
    """Flag the current session to be fully reset on the next run."""

    st.session_state[SESSION_RESET_KEY] = True
