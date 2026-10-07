"""Uvicorn entry point: `uvicorn pgs_api.main:app`.

Reads the configuration from the environment at import, so a bad configuration stops
the process before it listens (see core/settings.py for the variables).
"""

from __future__ import annotations

from .app import create_app
from .core.observability import configure_logging
from .core.settings import Settings

settings = Settings.from_env()
configure_logging(settings.log_level)
app = create_app(settings)
