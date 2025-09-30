"""Utilities for reasoning about time windows enforced by the UI."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time as dtime
from zoneinfo import ZoneInfo

from app.utils.constants import BASE_GERACAO_OPCOES


TZ = ZoneInfo("America/Bahia")


@dataclass(frozen=True)
class TimeWindow:
    """Represents the operational flags for the request workflow."""

    after_10: bool
    after_1055: bool
    available_options: list[str]
    default_option: str


def current_time_window(now: datetime | None = None) -> TimeWindow:
    """Return the :class:`TimeWindow` for *now* in the Bahia timezone."""

    now = now or datetime.now(TZ)
    after_10 = now.time() >= dtime(10, 0)
    after_1055 = now.time() >= dtime(10, 55)
    available = [
        option
        for option in BASE_GERACAO_OPCOES
        if not (after_1055 and option == "HOJE")
    ]
    default = available[0] if available else "AMANHÃ"
    return TimeWindow(after_10=after_10, after_1055=after_1055, available_options=available, default_option=default)
