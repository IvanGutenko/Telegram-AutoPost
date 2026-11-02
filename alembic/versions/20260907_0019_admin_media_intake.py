"""Preserve test album captions while media downloads are in flight or fail."""

import sqlalchemy as sa
from alembic import op

revision = "20260907_0019"
down_revision = "20260906_0018"
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        "admin_test_items",
        sa.Column("media_pending", sa.Boolean(), nullable=False, server_default=sa.false()),
    )
    op.add_column("admin_test_items", sa.Column("media_error", sa.Text(), nullable=True))


def downgrade():
    op.drop_column("admin_test_items", "media_error")
    op.drop_column("admin_test_items", "media_pending")
