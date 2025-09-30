"""Domain helpers for representing pedidos (requests)."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import pandas as pd

from app.utils.constants import STATUS_LABEL_INV, Status
from app.utils.validators import strip_accents_and_punct_action


@dataclass
class Pedido:
    timestamp: datetime
    nome: str
    email: str
    utd: str
    base: str
    servico: str
    pacotes: int

    def row_key(self) -> str:
        return "\n".join(
            [
                str(self.timestamp),
                self.nome,
                self.email,
                self.utd,
                self.base,
                self.servico,
                str(int(self.pacotes)),
            ]
        )


def build_row_key_from_series(row: pd.Series) -> str:
    """Create the stable row key used for selection and updates."""

    pacotes_raw = row.get("PACOTES", 0)
    try:
        pacotes_int = int(float(pacotes_raw))
    except (TypeError, ValueError):
        pacotes_int = 0

    return "\n".join(
        [
            str(row.get("TIMESTAMP", "")),
            str(row.get("NOME", "")),
            str(row.get("E-MAIL", "")),
            str(row.get("UTD", "")),
            str(row.get("BASE", "")),
            str(row.get("SERVICO", "")),
            str(pacotes_int),
        ]
    )


def label_to_status_db(label: str) -> str:
    return STATUS_LABEL_INV.get(label, Status.EM_ANALISE)


def normalise_servico(value: str | None) -> str:
    return strip_accents_and_punct_action(value or "")
