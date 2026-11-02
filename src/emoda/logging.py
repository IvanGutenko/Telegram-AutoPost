import logging

from pythonjsonlogger.json import JsonFormatter


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(
        JsonFormatter(
            "%(asctime)s %(levelname)s %(name)s %(message)s %(source_id)s %(telegram_message_id)s %(grouped_id)s %(product_id)s %(emoda_code)s %(destination)s"
        )
    )
    root = logging.getLogger()
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level.upper())
    # Alembic's env.py runs logging.config.fileConfig from `emoda run` before the
    # runtime starts; with the default disable_existing_loggers=True every logger
    # created at import time (all emoda.* module loggers) stays silenced forever,
    # turning every logger.exception into a no-op. Re-arm them explicitly.
    for existing in root.manager.loggerDict.values():
        if isinstance(existing, logging.Logger):
            existing.disabled = False
