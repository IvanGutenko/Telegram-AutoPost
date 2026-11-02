"""Add percentage and open-ended markup ranges.

Revision ID: 20260826_0007
Revises: 20260826_0006
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260826_0007"
down_revision: str | None = "20260826_0006"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.alter_column("markup_rules", "max_price", existing_type=sa.Numeric(18, 4), nullable=True)
    op.add_column(
        "markup_rules",
        sa.Column("markup_type", sa.String(length=16), server_default="fixed", nullable=False),
    )
    op.create_check_constraint(
        "ck_markup_rules_markup_type",
        "markup_rules",
        "markup_type IN ('fixed', 'percent')",
    )


def downgrade() -> None:
    connection = op.get_bind()
    open_range_count = connection.scalar(
        sa.text("SELECT count(*) FROM markup_rules WHERE max_price IS NULL")
    )
    if open_range_count:
        raise RuntimeError("cannot downgrade while open-ended markup ranges exist")
    op.drop_constraint("ck_markup_rules_markup_type", "markup_rules", type_="check")
    op.drop_column("markup_rules", "markup_type")
    op.alter_column("markup_rules", "max_price", existing_type=sa.Numeric(18, 4), nullable=False)
