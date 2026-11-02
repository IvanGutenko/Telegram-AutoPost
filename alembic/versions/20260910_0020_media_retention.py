"""Track bounded local-media retention without deleting durable database history."""

import sqlalchemy as sa

from alembic import op

revision = "20260910_0020"
down_revision = "20260907_0019"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "media_items",
        sa.Column("local_deleted_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_media_retention_queue",
        "media_items",
        ["local_deleted_at", "updated_at"],
    )
    op.create_index(
        "ix_admin_test_retention_queue",
        "admin_test_batches",
        ["status", "updated_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_admin_test_retention_queue", table_name="admin_test_batches")
    op.drop_index("ix_media_retention_queue", table_name="media_items")
    op.drop_column("media_items", "local_deleted_at")
