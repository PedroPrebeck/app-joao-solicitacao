"""Legacy entrypoint kept for backwards compatibility.

The Streamlit application has been migrated to the ``app/`` package.  This
module simply delegates to :mod:`app.Home` so existing deployment scripts that
still point to ``original.py`` keep working.
"""
from __future__ import annotations

from app.Home import main


if __name__ == "__main__":  # pragma: no cover - Streamlit entrypoint
    main()
