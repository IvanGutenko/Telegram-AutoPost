"""Durable admin bulk dispatch."""
import sqlalchemy as sa

from alembic import op

revision = "20260906_0018"
down_revision = "20260901_0017"
branch_labels = None
depends_on = None


def timestamps():
    return [sa.Column(name, sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now())
            for name in ("created_at", "updated_at")]


def upgrade() -> None:
    op.create_table("bulk_send_batches",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("admin_user_id", sa.Integer(), sa.ForeignKey("admin_users.id"), nullable=False),
        sa.Column("text", sa.Text(), nullable=False), *timestamps())
    op.create_table("bulk_send_targets",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("batch_id", sa.Integer(), sa.ForeignKey("bulk_send_batches.id"), nullable=False),
        sa.Column("source_id", sa.Integer(), sa.ForeignKey("sources.id"), nullable=False),
        sa.Column("channel_id", sa.BigInteger(), nullable=False),
        sa.Column("source_name", sa.String(255), nullable=False),
        sa.Column("random_id", sa.BigInteger(), nullable=False, unique=True),
        sa.Column("status", sa.String(20), nullable=False),
        sa.Column("message_ids", sa.JSON(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.UniqueConstraint("batch_id", "source_id"), *timestamps())
    op.create_index("ix_bulk_send_targets_batch_id", "bulk_send_targets", ["batch_id"])


def downgrade() -> None:
    op.drop_table("bulk_send_targets")
    op.drop_table("bulk_send_batches")
