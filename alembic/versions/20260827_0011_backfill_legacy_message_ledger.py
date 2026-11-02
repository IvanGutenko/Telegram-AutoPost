"""Backfill terminal ledger state for products processed before the typed ledger.

Revision ID: 20260827_0011
Revises: 20260826_0010
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260827_0011"
down_revision: str | None = "20260826_0010"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


BACKFILL_SOURCE_MESSAGES_SQL = """
WITH linked AS (
    SELECT
        sm.id AS source_message_id,
        (
            SELECT p.id
            FROM products AS p
            WHERE p.source_message_id = sm.id
               OR (
                    sm.album_group_id IS NOT NULL
                    AND p.album_group_id = sm.album_group_id
               )
            ORDER BY p.updated_at DESC, p.id DESC
            LIMIT 1
        ) AS product_id
    FROM source_messages AS sm
    WHERE sm.state = 'received'
      AND sm.processed_at IS NOT NULL
), classified AS (
    SELECT
        linked.source_message_id,
        CASE
            WHEN p.status = 'published' THEN 'published'
            WHEN p.status IN ('needs_review', 'reconciliation_required') THEN 'review'
            WHEN p.status = 'skipped' THEN 'ignored'
            WHEN p.status = 'failed' THEN 'failed'
            WHEN p.status IN ('processing', 'ready_to_publish', 'publishing') THEN 'processing'
            WHEN p.status IN ('collecting_album', 'ready') THEN 'attached_to_product'
            ELSE 'received'
        END AS next_state,
        CASE
            WHEN p.status = 'published' THEN NULL
            WHEN p.status = 'failed' THEN COALESCE(
                p.processing_error,
                'legacy product status: failed'
            )
            ELSE 'legacy product status: ' || p.status
        END AS next_reason
    FROM linked
    JOIN products AS p ON p.id = linked.product_id
)
UPDATE source_messages
SET
    state = (
        SELECT classified.next_state
        FROM classified
        WHERE classified.source_message_id = source_messages.id
    ),
    state_reason = (
        SELECT classified.next_reason
        FROM classified
        WHERE classified.source_message_id = source_messages.id
    )
WHERE id IN (
    SELECT classified.source_message_id
    FROM classified
    WHERE classified.next_state <> 'received'
)
"""


BACKFILL_TRANSITIONS_SQL = """
INSERT INTO message_event_transitions (
    source_message_id,
    source_id,
    telegram_message_id,
    state,
    reason,
    origin,
    occurred_at
)
SELECT
    sm.id,
    sm.source_id,
    sm.telegram_message_id,
    sm.state,
    sm.state_reason,
    'recovery',
    COALESCE(sm.processed_at, sm.updated_at, CURRENT_TIMESTAMP)
FROM source_messages AS sm
WHERE sm.state <> 'received'
  AND sm.processed_at IS NOT NULL
  AND NOT EXISTS (
      SELECT 1
      FROM message_event_transitions AS transition
      WHERE transition.source_message_id = sm.id
        AND transition.state = sm.state
  )
"""


def upgrade() -> None:
    op.execute(sa.text(BACKFILL_SOURCE_MESSAGES_SQL))
    op.execute(sa.text(BACKFILL_TRANSITIONS_SQL))


def downgrade() -> None:
    # The typed states are factual history and must not be erased on code rollback.
    pass
