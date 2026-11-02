"""Add durable per-source catch-up onboarding anchor.

Revision ID: 20260826_0008
Revises: 20260826_0007
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260826_0008"
down_revision: str | None = "20260826_0007"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("sources", sa.Column("catchup_anchor_message_id", sa.BigInteger()))
    op.execute(
        "UPDATE sources SET catchup_anchor_message_id = last_seen_message_id "
        "WHERE catchup_anchor_message_id IS NULL"
    )


def downgrade() -> None:
    op.drop_column("sources", "catchup_anchor_message_id")
