from __future__ import annotations

from datetime import date
from typing import Sequence

import pandas as pd
from hdbcli import dbapi

from app.services.hana import HanaConfig, SupportsHanaConnect, create_connection


SQL_CLUSTER_CONFIG_TEMPLATE = """
WITH BASES AS (
  SELECT UTD40, ID_BASE_STC, MIN(BASE_STC) AS BASE_STC, NULL AS ID_BASE_EPS, NULL AS BASE_EPS
  FROM CLB142840.DAG40
  GROUP BY UTD40, ID_BASE_STC
  UNION ALL
  SELECT UTD40, NULL, NULL, ID_BASE_EPS, MIN(BASE_EPS)
  FROM CLB142840.DAG40
  GROUP BY UTD40, ID_BASE_EPS
),
CLUSTER_CONFIG AS (
  SELECT
    CASE WHEN UTD = 'ITAPUAN' THEN 'ITAPOAN' ELSE TRIM(UPPER(UTD)) END AS UTD,
    TRIM(UPPER(CARTEIRA)) AS CARTEIRA,
    TURMA, ZONA,
    QTD_MIN, QTD_MAX,
    RAIO_MIN * 1000 AS RAIO_MIN,
    RAIO_MAX * 1000 AS RAIO_MAX,
    RAIO_INC * 1000 AS RAIO_INC
  FROM CLB142840.CLUSTER_CONFIG5
),
INNER_Q AS (
  SELECT
    BP.UTD,
    'SIM' AS SELECIONAR,
    BP.ZONA,
    '' AS LOCALI,
    '' AS MUNICIPIO,
    '' AS BAIRRO,
    '' AS TIPO_LOCAL,
    BP.PACOTES AS CLUSTERS,
    BP.PACOTES AS PACOTES,
    COALESCE(CNF.QTD_MAX, '15') AS QTD_MAX,
    COALESCE(CNF.QTD_MIN, '10') AS QTD_MIN,
    COALESCE(CNF.RAIO_MIN, '4000') AS RAIO_IDEAL,
    COALESCE(CNF.RAIO_MAX, '5000') AS RAIO_MAX,
    COALESCE(CNF.RAIO_INC, '500') AS RAIO_STEP,
    CASE
      WHEN UPPER(BP.SERVICO) LIKE '%GAVIAO%' THEN 'DISJUNTOR'
      WHEN UPPER(BP.SERVICO) LIKE '%RECORTE%' THEN 'RECORTE'
      WHEN UPPER(BP.SERVICO) LIKE '%BAIXA%' THEN 'BAIXA'
      WHEN UPPER(BP.SERVICO) LIKE '%VISITA%' THEN 'COB.DOM'
      ELSE 'CONVENCIONAL'
    END AS CARTEIRA,
    CASE
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NULL THEN 'STC'
      WHEN E.ID_BASE_EPS IS NOT NULL AND S.ID_BASE_STC IS NULL THEN 'EPS'
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NOT NULL THEN 'AMBIGUO'
      ELSE NULL
    END AS TURMA,
    '1' AS PESO_MTVCOB,
    '2' AS PESO_PECLD,
    '0' AS PESO_QTDFTVE,
    '' AS PREENCHER,
    '' AS QTD_PREENCHER,
    BP."NOME" AS NOME,
    BP."E-MAIL" AS EMAIL,
    BP.BASE AS BASE,
    BP.SERVICO AS SERVICO,
    BP."TIMESTAMP" AS TS
  FROM "U618488"."BASE_PEDIDOS" BP
  LEFT JOIN BASES S
    ON BP.ZONA = S.ID_BASE_STC AND TRIM(UPPER(BP.UTD)) = TRIM(UPPER(S.UTD40))
  LEFT JOIN BASES E
    ON BP.ZONA = E.ID_BASE_EPS AND TRIM(UPPER(BP.UTD)) = TRIM(UPPER(E.UTD40))
  LEFT JOIN CLUSTER_CONFIG CNF
    ON CNF.UTD = TRIM(UPPER(BP.UTD))
   AND CNF.TURMA = CASE
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NULL THEN 'STC'
      WHEN E.ID_BASE_EPS IS NOT NULL AND S.ID_BASE_STC IS NULL THEN 'EPS'
      WHEN S.ID_BASE_STC IS NOT NULL AND E.ID_BASE_EPS IS NOT NULL THEN 'AMBIGUO'
      ELSE NULL
    END
   AND (
      (UPPER(TRIM(COALESCE(BP.SERVICO, ''))) LIKE '%GAVIAO%' AND CNF.CARTEIRA = 'DISJUNTOR')
      OR (UPPER(TRIM(COALESCE(BP.SERVICO, ''))) NOT LIKE '%GAVIAO%' AND CNF.ZONA = BP.ZONA)
   )
  WHERE TO_DATE(BP."TIMESTAMP") = ?
)
SELECT *
FROM INNER_Q
WHERE TURMA IN ({turma_placeholders})
  AND CARTEIRA IN ({carteira_placeholders})
ORDER BY TO_DATE(TS) ASC, TO_VARCHAR(TS, 'HH24:MI:SS') ASC
"""


def fetch_cluster_config(
    sel_date: date,
    turmas: Sequence[str],
    carteiras: Sequence[str],
    *,
    connector: SupportsHanaConnect | None = None,
    config: HanaConfig | None = None,
) -> pd.DataFrame:
    if not turmas or not carteiras:
        return pd.DataFrame()

    connector_fn = connector or dbapi.connect
    cfg = config or HanaConfig.from_env()

    turma_placeholders = ",".join(["?"] * len(turmas))
    carteira_placeholders = ",".join(["?"] * len(carteiras))
    sql = SQL_CLUSTER_CONFIG_TEMPLATE.format(
        turma_placeholders=turma_placeholders,
        carteira_placeholders=carteira_placeholders,
    )

    params: list = [sel_date] + list(turmas) + list(carteiras)

    conn = None
    cur = None
    try:
        conn = create_connection(cfg, connector_fn)
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

    return pd.DataFrame(rows, columns=cols).fillna("")
