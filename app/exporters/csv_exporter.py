"""Utilities that prepare CSV payloads for download."""
from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd

NUMERIC_COLUMNS = ["CLUSTERS", "QTD_MAX", "QTD_MIN", "RAIO_IDEAL", "RAIO_MAX", "RAIO_STEP"]
DROP_AUX_COLUMNS = ["TS", "_ROW_KEY", "NOME", "EMAIL", "BASE", "SERVICO", "PACOTES"]


@dataclass
class CsvPayload:
    turma: str
    carteira: str
    file_name: str
    content: bytes


def _carteira_suffix(db_code: str) -> str:
    code = (db_code or "").strip().upper()
    if code == "COB.DOM":
        return "domiciliar"
    if code == "DISJUNTOR":
        return "disjuntor"
    return code.lower()


def _ensure_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    for column in NUMERIC_COLUMNS:
        if column in df.columns:
            df[column] = pd.to_numeric(df[column], errors="coerce").astype("Int64")
    return df


def _drop_auxiliary_columns(df: pd.DataFrame) -> pd.DataFrame:
    present = [c for c in DROP_AUX_COLUMNS if c in df.columns]
    return df.drop(columns=present, errors="ignore")


def _filter_selection(df: pd.DataFrame, *, exclude_unselected: bool, selection_col: str) -> pd.DataFrame:
    if selection_col not in df.columns:
        return df

    mask = df[selection_col].astype(str).str.upper().eq("SIM")
    if exclude_unselected:
        df = df[mask].copy()
    else:
        df = df.copy()
        df[selection_col] = mask.map({True: "SIM", False: "NAO"})
    return df


def generate_csv_payloads(
    df: pd.DataFrame,
    *,
    exclude_unselected: bool = True,
    selection_col: str = "SELECIONAR",
    sep: str = ";",
) -> List[CsvPayload]:
    """Transform *df* into CSV payloads grouped by ``TURMA`` and ``CARTEIRA``."""

    if df.empty:
        return []

    working = _filter_selection(df, exclude_unselected=exclude_unselected, selection_col=selection_col)
    if working.empty:
        return []

    payloads: List[CsvPayload] = []
    grouped = working.groupby(["TURMA", "CARTEIRA"], dropna=True)

    for (turma, carteira), subset in grouped:
        if subset.empty:
            continue

        subset = _drop_auxiliary_columns(subset)
        subset = _ensure_numeric_columns(subset)
        csv_str = subset.to_csv(index=False, sep=sep, encoding="utf-8-sig", na_rep="")
        payloads.append(
            CsvPayload(
                turma=turma,
                carteira=carteira,
                file_name=f"config_{str(turma).lower()}_{_carteira_suffix(str(carteira))}.csv",
                content=csv_str.encode("utf-8-sig"),
            )
        )

    return payloads
