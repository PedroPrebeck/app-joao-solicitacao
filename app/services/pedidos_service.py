from __future__ import annotations

from typing import Dict

import pandas as pd
from hdbcli import dbapi

from app.repositories.pedidos_repo import (
    build_status_changes,
    fetch_pedidos,
    insert_pedidos as repo_insert_pedidos,
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


def has_column(column: str, *, connector: SupportsHanaConnect, config: HanaConfig | None = None) -> bool:
    return table_has_column(column, connector=connector, config=config)


def has_status_column(*, connector: SupportsHanaConnect, config: HanaConfig | None = None) -> bool:
    return has_column("STATUS", connector=connector, config=config)


def insert_pedidos(
    df: pd.DataFrame,
    *,
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> int:
    include_turma = has_column("TURMA", connector=connector, config=config)
    return repo_insert_pedidos(
        df,
        connector=connector,
        config=config,
        include_turma=include_turma,
    )


def insert_pedidos_default(df: pd.DataFrame) -> int:
    cfg = HanaConfig.from_env()
    return insert_pedidos(df, connector=dbapi.connect, config=cfg)


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
