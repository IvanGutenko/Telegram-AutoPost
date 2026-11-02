"""Add durable explicit batches for admin dry-run input.

Revision ID: 20260825_0003
Revises: 20260825_0002
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260825_0003"
down_revision: str | None = "20260825_0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "admin_test_batches",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("source_id", sa.Integer(), nullable=False),
        sa.Column("admin_user_id", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), server_default="collecting", nullable=False),
        sa.Column("test_run_id", sa.Integer(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["admin_user_id"], ["admin_users.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["source_id"], ["sources.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["test_run_id"], ["test_runs.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_admin_test_batches_source_id", "admin_test_batches", ["source_id"])
    op.create_index("ix_admin_test_batches_admin_user_id", "admin_test_batches", ["admin_user_id"])
    op.create_table(
        "admin_test_items",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("batch_id", sa.Integer(), nullable=False),
        sa.Column("telegram_chat_id", sa.BigInteger(), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=False),
        sa.Column("media_group_id", sa.String(length=255), nullable=True),
        sa.Column("text", sa.Text(), server_default="", nullable=False),
        sa.Column("media_paths", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["admin_test_batches.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "batch_id",
            "telegram_chat_id",
            "telegram_message_id",
            name="uq_admin_test_items_batch_id",
        ),
    )
    op.create_index("ix_admin_test_items_batch_id", "admin_test_items", ["batch_id"])


def downgrade() -> None:
    op.drop_index("ix_admin_test_items_batch_id", table_name="admin_test_items")
    op.drop_table("admin_test_items")
    op.drop_index("ix_admin_test_batches_admin_user_id", table_name="admin_test_batches")
    op.drop_index("ix_admin_test_batches_source_id", table_name="admin_test_batches")
    op.drop_table("admin_test_batches")
