"""Repository helpers for the DAG40 catalogue."""
from __future__ import annotations

import pandas as pd

from app.services.hana import HanaConfig, SupportsHanaConnect, create_connection

SQL_DAG40 = """
SELECT
  UTD40 AS UTD,
  BASE,
  ZONA,
  TURMA
FROM (
  SELECT DISTINCT
    UTD40,
    BASE_STC AS BASE,
    ID_BASE_STC AS ZONA,
    'STC' AS TURMA
  FROM CLB142840.DAG40
  WHERE BASE_STC NOT LIKE '%.%'
  UNION
  SELECT DISTINCT
    UTD40,
    BASE_EPS AS BASE,
    ID_BASE_EPS AS ZONA,
    'EPS' AS TURMA
  FROM CLB142840.DAG40
  WHERE BASE_EPS NOT LIKE '%.%'
)
WHERE UPPER(BASE) NOT LIKE UPPER('%MANUAL%')
  AND ZONA <> ''
ORDER BY ZONA
"""


def fetch_dag40(connector: SupportsHanaConnect, config: HanaConfig | None = None) -> pd.DataFrame:
    """Fetch the DAG40 table from HANA as a ``pandas`` dataframe."""

    cfg = config or HanaConfig.from_env()
    conn = None
    cur = None
    try:
        conn = create_connection(cfg, connector)
        cur = conn.cursor()
        cur.execute(SQL_DAG40)
        rows = cur.fetchall()
        cols = [col[0] for col in cur.description]
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    frame = pd.DataFrame(rows, columns=cols).fillna("")
    return frame.astype({"UTD": "string", "BASE": "string", "ZONA": "string", "TURMA": "string"})
