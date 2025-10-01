from __future__ import annotations

import os
from zoneinfo import ZoneInfo

PAGE_TITLE = "Geração de Notas de Cobrança - Painel de Solicitações"
PAGE_ICON = "🧾"
PAGE_LAYOUT = "wide"

TZ = ZoneInfo("America/Bahia")
CACHE_PATH = os.getenv("DAG40_CACHE_PATH", "dag40_cache.csv")
