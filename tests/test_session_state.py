from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace

sys.path.append(str(Path(__file__).resolve().parents[1]))

streamlit_stub = types.ModuleType("streamlit")
streamlit_stub.session_state = {}


def _noop() -> None:  # pragma: no cover - executed only if a test fails to patch ``st``
    pass


streamlit_stub.rerun = _noop
sys.modules.setdefault("streamlit", streamlit_stub)

from app.state import session
from app.state import session_keys as keys


class _FakeStreamlit(SimpleNamespace):
    def __init__(self) -> None:
        super().__init__(session_state={}, rerun_called=False)

    def rerun(self) -> None:  # pragma: no cover - invoked indirectly
        self.rerun_called = True


def test_trigger_full_reset_sets_flag(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(session, "st", fake_st)

    session.trigger_full_reset()

    assert fake_st.session_state[keys.FULL_RESET_FLAG] is True


def test_handle_full_reset_clears_state_and_reruns(monkeypatch):
    fake_st = _FakeStreamlit()
    fake_st.session_state[keys.FULL_RESET_FLAG] = True
    monkeypatch.setattr(session, "st", fake_st)

    session.handle_full_reset()

    assert fake_st.session_state == {keys.FULL_RESET_FLAG: False}
    assert fake_st.rerun_called is True


def test_handle_full_reset_no_flag(monkeypatch):
    fake_st = _FakeStreamlit()
    monkeypatch.setattr(session, "st", fake_st)

    session.handle_full_reset()

    assert fake_st.session_state == {}
    assert fake_st.rerun_called is False
