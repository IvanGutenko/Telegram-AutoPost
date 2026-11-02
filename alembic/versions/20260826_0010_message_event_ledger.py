"""Add typed Telegram message event ledger.

Revision ID: 20260826_0010
Revises: 20260826_0009
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260826_0010"
down_revision: str | None = "20260826_0009"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "source_messages",
        sa.Column("state", sa.String(length=32), nullable=False, server_default="received"),
    )
    op.add_column("source_messages", sa.Column("state_reason", sa.Text()))
    op.add_column(
        "source_messages",
        sa.Column("event_origin", sa.String(length=20), nullable=False, server_default="live"),
    )
    op.add_column(
        "source_messages",
        sa.Column("event_type", sa.String(length=20), nullable=False, server_default="new"),
    )
    op.create_table(
        "message_event_transitions",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "source_message_id",
            sa.Integer(),
            sa.ForeignKey("source_messages.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "source_id",
            sa.Integer(),
            sa.ForeignKey("sources.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=False),
        sa.Column("state", sa.String(length=32), nullable=False),
        sa.Column("reason", sa.Text()),
        sa.Column("origin", sa.String(length=20), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index(
        "ix_message_event_correlation",
        "message_event_transitions",
        ["source_id", "telegram_message_id"],
    )
    op.create_index(
        "ix_message_event_transitions_source_message_id",
        "message_event_transitions",
        ["source_message_id"],
    )
    op.create_index(
        "ix_message_event_transitions_source_id",
        "message_event_transitions",
        ["source_id"],
    )


def downgrade() -> None:
    op.drop_table("message_event_transitions")
    op.drop_column("source_messages", "event_type")
    op.drop_column("source_messages", "event_origin")
    op.drop_column("source_messages", "state_reason")
    op.drop_column("source_messages", "state")
