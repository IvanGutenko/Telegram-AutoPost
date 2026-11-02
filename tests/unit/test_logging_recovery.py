"""Guard against the production observability failure.

`emoda run` executes Alembic migrations in the same process; Alembic's env.py
calls logging.config.fileConfig which by default disables every logger that
already exists (all emoda.* module loggers created at import time). The
middleware's logger.exception then became a silent no-op and every admin
mutation error showed "Ошибка записана в журнал" while nothing was written.
"""
from __future__ import annotations

import logging
from logging.config import fileConfig
from pathlib import Path

from alembic.config import Config

from emoda.admin import router as admin_router
from emoda.logging import configure_logging


def test_alembic_env_keeps_import_time_loggers_alive() -> None:
    source = Path("alembic/env.py").read_text(encoding="utf-8")
    assert "disable_existing_loggers=False" in source


def test_fileconfig_with_flag_does_not_disable_import_time_loggers() -> None:
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    try:
        fileConfig(
            Config("alembic.ini").config_file_name, disable_existing_loggers=False
        )
        assert admin_router.logger.disabled is False
    finally:
        admin_router.logger.disabled = False
        root.handlers = handlers
        root.setLevel(level)


def test_configure_logging_rearms_disabled_loggers() -> None:
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    try:
        admin_router.logger.disabled = True
        configure_logging("INFO")
        assert admin_router.logger.disabled is False
    finally:
        admin_router.logger.disabled = False
        root.handlers = handlers
        root.setLevel(level)
