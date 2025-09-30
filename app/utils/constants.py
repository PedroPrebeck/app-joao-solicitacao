"""Centralised constants used across the application.

The refactor moves the sea of global constants that used to live in
``original.py`` into a dedicated module.  Having a single place for the
values keeps the UI code lean and allows the constants to be imported by
unit tests without triggering Streamlit side effects.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

DEFAULT_SERVICOS = [
    "Corte",
    "Corte Gavião",
    "Recorte",
    "Baixa",
    "Visita de Cobrança",
]

BASE_GERACAO_OPCOES = ["HOJE", "AMANHÃ", "FIM DE SEMANA"]

ALLOWED_ADMINS = {
    "joao.almeida@neoenergia.com",
    "luiz.espozel@neoenergia.com",
    "pedro.azevedo@neoenergia.com",
    "carlla.ventura@neoenergia.com",
    "ccsantos@neoenergia.com",
    "dsaraujo@neoenergia.com",
    "madson.melo@neoenergia.com",
    "jsbrito@neoenergia.com",
}


class Status:
    """Enumeration for status values stored in the HANA database."""

    EM_ANALISE = "EM ANALISE"
    APROVADO = "APROVADO"
    RECUSADO = "RECUSADO"


STATUS_DB_VALUES = [Status.EM_ANALISE, Status.APROVADO, Status.RECUSADO]

STATUS_LABEL_MAP: Dict[str, str] = {
    Status.EM_ANALISE: "🟡 Pendente",
    Status.APROVADO: "🟢 Aprovado",
    Status.RECUSADO: "🔴 Recusado",
}

STATUS_LABEL_INV: Dict[str, str] = {v: k for k, v in STATUS_LABEL_MAP.items()}

STATUS_COLORS_BG: Dict[str, str] = {
    Status.EM_ANALISE: "#FFF6CC",
    Status.APROVADO: "#E8F5E9",
    Status.RECUSADO: "#FFEBEE",
}


@dataclass(frozen=True)
class HanaTableInfo:
    """Small helper describing database tables and columns."""

    schema: str
    table: str

    def fqn(self) -> str:
        return f'"{self.schema}"."{self.table}"'


PEDIDOS_TABLE = HanaTableInfo(schema="U618488", table="BASE_PEDIDOS")
