"""Repository abstraction over the ``BASE_PEDIDOS`` table."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Sequence

import pandas as pd

from app.models.pedido import build_row_key_from_series
from app.services.hana import HanaConfig, SupportsHanaConnect, create_connection
from app.utils.constants import PEDIDOS_TABLE, STATUS_LABEL_INV, Status


@dataclass
class StatusChange:
    timestamp: str
    nome: str
    email: str
    utd: str
    base: str
    servico: str
    pacotes: int
    status: str
    validado_por: str | None = None

    def as_tuple(self) -> tuple:
        base = (
            self.status,
            self.timestamp,
            self.nome,
            self.email,
            self.utd,
            self.base,
            self.servico,
            int(self.pacotes),
        )
        if self.validado_por is None:
            return base
        return (
            self.status,
            self.validado_por,
            self.timestamp,
            self.nome,
            self.email,
            self.utd,
            self.base,
            self.servico,
            int(self.pacotes),
        )


def fetch_pedidos(
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> pd.DataFrame:
    cfg = config or HanaConfig.from_env()

    table = PEDIDOS_TABLE.fqn()
    sql = f"SELECT * FROM {table} ORDER BY \"TIMESTAMP\" ASC"

    conn = None
    cur = None
    try:
        conn = create_connection(cfg, connector)
        cur = conn.cursor()
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [col[0] for col in cur.description]
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    df = pd.DataFrame(rows, columns=cols)
    if "PACOTES" in df.columns:
        df["PACOTES"] = pd.to_numeric(df["PACOTES"], errors="coerce").fillna(0).astype(int)
    if "STATUS" not in df.columns:
        df["STATUS"] = Status.EM_ANALISE
    return df

def build_status_changes(
    df: pd.DataFrame,
    pending_labels: dict[str, str],
    *,
    admin_email: str | None,
    has_validado_por: bool,
) -> List[StatusChange]:
    if df.empty or not pending_labels:
        return []

    df = df.copy()
    df["_ROW_KEY"] = df.apply(build_row_key_from_series, axis=1)

    changes: List[StatusChange] = []
    lookup = {key: idx for idx, key in enumerate(df["_ROW_KEY"].tolist())}

    for key, label in pending_labels.items():
        idx = lookup.get(key)
        if idx is None:
            continue
        new_status = STATUS_LABEL_INV.get(label, Status.EM_ANALISE)
        current_status = str(df.iloc[idx].get("STATUS", "")).upper().strip() or Status.EM_ANALISE
        if new_status == current_status:
            continue

        row = df.iloc[idx]
        base_kwargs = dict(
            timestamp=row.get("TIMESTAMP"),
            nome=row.get("NOME"),
            email=row.get("E-MAIL"),
            utd=row.get("UTD"),
            base=row.get("BASE"),
            servico=row.get("SERVICO"),
            pacotes=int(row.get("PACOTES", 0)),
            status=new_status,
        )
        validado_por = None
        if has_validado_por and new_status in (Status.APROVADO, Status.RECUSADO):
            validado_por = admin_email

        changes.append(StatusChange(validado_por=validado_por, **base_kwargs))

    return changes

    try:
        conn = create_connection(cfg, connector)
        cur = conn.cursor()
        cur.executemany(sql, params)
        conn.commit()
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    return len(params)


def table_has_column(
    column: str,
    *,
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> bool:
    """Return ``True`` when *column* exists in the pedidos table."""

    cfg = config or HanaConfig.from_env()
    table = PEDIDOS_TABLE.fqn()
    sql = f'SELECT * FROM {table} WHERE 1=0'

    conn = None
    cur = None
    try:
        conn = create_connection(cfg, connector)
        cur = conn.cursor()
        cur.execute(sql)
        cols = [col[0].upper() for col in cur.description]
    except Exception:
        return False
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    return column.upper() in cols


def insert_pedidos(
    rows: pd.DataFrame,
    *,
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> int:
    """Insert pedidos rows into the database."""

    if rows.empty:
        return 0

    cfg = config or HanaConfig.from_env()
    has_turma = table_has_column("TURMA", connector=connector, config=cfg)
    table = PEDIDOS_TABLE.fqn()

    if has_turma:
        sql = f"""
        INSERT INTO {table}
        ("TIMESTAMP","NOME","E-MAIL","CADEIA","UTD","BASE","TURMA","ZONA","SERVICO","PACOTES","NOTAS","JUSTIFICATIVA","COMENTARIOS")
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """
        param_rows = [
            (
                row["TIMESTAMP"],
                row["NOME"],
                row["E-MAIL"],
                row["CADEIA"],
                row["UTD"],
                row["BASE"],
                row.get("TURMA"),
                row.get("ZONA"),
                row.get("SERVICO_CLEAN"),
                int(row.get("PACOTES", 0)),
                None,
                (str(row.get("JUSTIFICATIVA", "")).strip() or None) if pd.notna(row.get("JUSTIFICATIVA")) else None,
                (str(row.get("COMENTARIO", "")).strip() or None) if pd.notna(row.get("COMENTARIO")) else None,
            )
            for _, row in rows.iterrows()
        ]
    else:
        sql = f"""
        INSERT INTO {table}
        ("TIMESTAMP","NOME","E-MAIL","CADEIA","UTD","BASE","ZONA","SERVICO","PACOTES","NOTAS","JUSTIFICATIVA","COMENTARIOS")
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """
        param_rows = [
            (
                row["TIMESTAMP"],
                row["NOME"],
                row["E-MAIL"],
                row["CADEIA"],
                row["UTD"],
                row["BASE"],
                row.get("ZONA"),
                row.get("SERVICO_CLEAN"),
                int(row.get("PACOTES", 0)),
                None,
                (str(row.get("JUSTIFICATIVA", "")).strip() or None) if pd.notna(row.get("JUSTIFICATIVA")) else None,
                (str(row.get("COMENTARIO", "")).strip() or None) if pd.notna(row.get("COMENTARIO")) else None,
            )
            for _, row in rows.iterrows()
        ]

    conn = None
    cur = None
    try:
        conn = create_connection(cfg, connector)
        cur = conn.cursor()
        cur.executemany(sql, param_rows)
        conn.commit()
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    return len(param_rows)