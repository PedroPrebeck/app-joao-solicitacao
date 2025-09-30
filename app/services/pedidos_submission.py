"""Service helpers dedicated to preparing pedido submissions."""
from __future__ import annotations

from datetime import datetime

import pandas as pd

from app.utils.time_windows import TZ
from app.utils.validators import (
    strip_accents_and_punct_action,
    strip_accents_and_punct_name,
)

REQUIRED_COLUMNS = ["GERACAO_PARA", "SERVIÇO", "PACOTES", "JUSTIFICATIVA"]


def prepare_submission_dataframe(
    lines_df: pd.DataFrame,
    *,
    nome: str,
    email: str,
    turma: str,
    after_1055: bool,
) -> pd.DataFrame:
    """Validate *lines_df* and return the payload ready for insertion."""

    if lines_df.empty:
        raise ValueError("Nenhuma linha para enviar. Adicione ao menos uma BASE e configure os serviços.")

    df = lines_df.copy()
    for column in REQUIRED_COLUMNS:
        if df[column].isna().any() or (df[column].astype(str).str.strip() == "").any():
            raise ValueError(f"Há linhas com **{column}** vazio.")

    if after_1055 and (df["GERACAO_PARA"].astype(str).str.upper().str.strip() == "HOJE").any():
        raise ValueError("Após 10:55, **HOJE** não é permitido. Altere para **AMANHÃ** ou **FIM DE SEMANA**.")

    df["PACOTES"] = pd.to_numeric(df["PACOTES"], errors="coerce").fillna(0).astype(int)
    if (df["PACOTES"] < 1).any():
        raise ValueError("Há linhas com **PACOTES** inválidos (mín. 1).")

    nome_norm = strip_accents_and_punct_name(nome)
    email_norm = email.strip().lower()

    df["SERVICO_CLEAN"] = df["SERVIÇO"].apply(strip_accents_and_punct_action)
    df["GERACAO_PARA"] = df["GERACAO_PARA"].astype(str).str.upper().str.strip()

    timestamp = datetime.now(TZ)
    out = df.copy()
    out.insert(0, "TIMESTAMP", timestamp)
    out.insert(1, "NOME", nome_norm)
    out.insert(2, "E-MAIL", email_norm)
    out.insert(3, "CADEIA", out["GERACAO_PARA"])
    if "TURMA" not in out.columns:
        out["TURMA"] = turma

    return out[
        [
            "TIMESTAMP",
            "NOME",
            "E-MAIL",
            "CADEIA",
            "UTD",
            "BASE",
            "TURMA",
            "ZONA",
            "SERVIÇO",
            "SERVICO_CLEAN",
            "PACOTES",
            "JUSTIFICATIVA",
            "COMENTARIO",
        ]
    ]
