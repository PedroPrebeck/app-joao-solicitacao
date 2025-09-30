"""Helpers for interacting with the SAP HANA database used by the project."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Protocol

from dotenv import load_dotenv


@dataclass
class HanaConfig:
    host: str
    port: int
    user: str
    password: str

    @classmethod
    def from_env(cls) -> "HanaConfig":
        """Load configuration from ``.env`` files and environment variables."""

        load_dotenv()
        port_raw = os.getenv("HANA_PORT", "30015")
        port = int(port_raw) if str(port_raw).isdigit() else 30015
        return cls(
            host=os.getenv("HANA_HOST", ""),
            port=port,
            user=os.getenv("HANA_USER", ""),
            password=os.getenv("HANA_PASS", ""),
        )


class SupportsHanaConnect(Protocol):
    """Protocol matching ``hdbcli.dbapi.connect`` signature.

    Declaring the protocol allows unit tests to provide a fake connector
    without importing the heavy HANA client.
    """

    def __call__(
        self, *, address: str, port: int, user: str, password: str
    ) -> Any:  # pragma: no cover - signature documentation only
        ...


def create_connection(config: HanaConfig, connector: SupportsHanaConnect) -> Any:
    """Create a raw DB-API connection using *connector*.

    ``original.py`` opened connections inline, which made the code hard to
    test.  This helper centralises the creation so tests can inject a fake
    connector and assert the call parameters.
    """

    if not (config.host and config.user and config.password):
        raise RuntimeError("Defina HANA_HOST, HANA_USER e HANA_PASS no .env.")

    return connector(address=config.host, port=config.port, user=config.user, password=config.password)
