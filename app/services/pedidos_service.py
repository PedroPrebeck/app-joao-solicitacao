"""High level service for pedidos administration."""
from __future__ import annotations

from typing import Dict
from typing import Dict

import pandas as pd

from app.repositories.pedidos_repo import (
    build_status_changes,
    fetch_pedidos,
    insert_pedidos,
    table_has_column,
    update_statuses,
)
from app.services.hana import HanaConfig, SupportsHanaConnect
from app.utils.constants import STATUS_LABEL_MAP


def fetch_pedidos_with_labels(
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> pd.DataFrame:
    df = fetch_pedidos(connector=connector, config=config)
    df = df.copy()
    df["STATUS_LABEL"] = df["STATUS"].map(lambda v: STATUS_LABEL_MAP.get(str(v).upper().strip(), "🟡 Pendente"))
    return df


def apply_status_changes(
    df: pd.DataFrame,
    pending_labels: Dict[str, str],
    *,
    admin_email: str | None,
    has_validado_por: bool,
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> int:
    changes = build_status_changes(
        df,
        pending_labels,
        admin_email=admin_email,
        has_validado_por=has_validado_por,
    )
    if not changes:
        return 0

    return update_statuses(
        changes,
        connector=connector,
        config=config,
        has_validado_por=has_validado_por,
    )


def pedidos_table_has_column(
    column: str,
    *,
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> bool:
    """Expose :func:`table_has_column` with a service level name."""

    return table_has_column(column, connector=connector, config=config)


def insert_pedidos_rows(
    rows: pd.DataFrame,
    *,
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> int:
    """Persist prepared pedidos rows into the database."""

    if rows.empty:
        return 0

    return insert_pedidos(rows, connector=connector, config=config)
