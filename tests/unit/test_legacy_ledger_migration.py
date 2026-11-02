from __future__ import annotations

import importlib.util
from pathlib import Path

from sqlalchemy import create_engine


def load_migration():
    path = (
        Path(__file__).parents[2]
        / "alembic"
        / "versions"
        / "20260827_0011_backfill_legacy_message_ledger.py"
    )
    spec = importlib.util.spec_from_file_location("ledger_backfill_0011", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_legacy_processed_messages_are_backfilled_to_terminal_ledger_states() -> None:
    migration = load_migration()
    assert migration.down_revision == "20260826_0010"
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.exec_driver_sql(
            """
            CREATE TABLE source_messages (
                id INTEGER PRIMARY KEY,
                source_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                album_group_id INTEGER,
                processed_at TEXT,
                updated_at TEXT,
                state TEXT NOT NULL,
                state_reason TEXT
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE products (
                id INTEGER PRIMARY KEY,
                source_message_id INTEGER,
                album_group_id INTEGER,
                status TEXT NOT NULL,
                processing_error TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        connection.exec_driver_sql(
            """
            CREATE TABLE message_event_transitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_message_id INTEGER NOT NULL,
                source_id INTEGER NOT NULL,
                telegram_message_id INTEGER NOT NULL,
                state TEXT NOT NULL,
                reason TEXT,
                origin TEXT NOT NULL,
                occurred_at TEXT NOT NULL
            )
            """
        )
        for message_id in range(1, 7):
            processed = None if message_id == 6 else "2026-08-25 12:00:00+00"
            connection.exec_driver_sql(
                "INSERT INTO source_messages VALUES (?, 10, ?, NULL, ?, "
                "'2026-08-25 12:00:00+00', 'received', NULL)",
                (message_id, 1000 + message_id, processed),
            )
        for product_id, status in enumerate(
            ["published", "needs_review", "reconciliation_required", "skipped", "failed"],
            start=1,
        ):
            connection.exec_driver_sql(
                "INSERT INTO products VALUES (?, ?, NULL, ?, ?, "
                "'2026-08-25 13:00:00+00')",
                (
                    product_id,
                    product_id,
                    status,
                    "legacy failure" if status == "failed" else None,
                ),
            )

        connection.exec_driver_sql(migration.BACKFILL_SOURCE_MESSAGES_SQL)
        connection.exec_driver_sql(migration.BACKFILL_TRANSITIONS_SQL)

        states = connection.exec_driver_sql(
            "SELECT id, state, state_reason FROM source_messages ORDER BY id"
        ).all()
        transitions = connection.exec_driver_sql(
            "SELECT source_message_id, state, origin FROM message_event_transitions "
            "ORDER BY source_message_id"
        ).all()

    assert states == [
        (1, "published", None),
        (2, "review", "legacy product status: needs_review"),
        (3, "review", "legacy product status: reconciliation_required"),
        (4, "ignored", "legacy product status: skipped"),
        (5, "failed", "legacy failure"),
        (6, "received", None),
    ]
    assert transitions == [
        (1, "published", "recovery"),
        (2, "review", "recovery"),
        (3, "review", "recovery"),
        (4, "ignored", "recovery"),
        (5, "failed", "recovery"),
    ]
    engine.dispose()
