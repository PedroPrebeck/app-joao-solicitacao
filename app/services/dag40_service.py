"""Service layer responsible for caching DAG40 information."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

import pandas as pd

from app.repositories.dag40_repo import fetch_dag40
from app.services.hana import HanaConfig, SupportsHanaConnect


def ensure_cache(path: str | os.PathLike[str], fetcher: Callable[[], pd.DataFrame]) -> None:
    """Ensure the DAG40 cache exists on disk."""

    cache_path = Path(path)
    if cache_path.exists():
        return

    df = fetcher()
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(cache_path, index=False, encoding="utf-8-sig")


def load_dag40(
    path: str | os.PathLike[str],
    connector: SupportsHanaConnect,
    config: HanaConfig | None = None,
) -> pd.DataFrame:
    """Load the DAG40 catalogue from cache, fetching from HANA when needed."""

    cfg = config or HanaConfig.from_env()

    def _fetch() -> pd.DataFrame:
        return fetch_dag40(connector=connector, config=cfg)

    ensure_cache(path, _fetch)
    if not Path(path).exists():
        raise FileNotFoundError("Não foi possível criar o cache DAG40.")

    df = pd.read_csv(path, dtype=str).fillna("")
    return df.astype({"UTD": "string", "BASE": "string", "ZONA": "string", "TURMA": "string"})
