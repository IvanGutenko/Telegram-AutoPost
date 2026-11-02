"""Add destination-scoped publication policy and fixed text.

Revision ID: 20260831_0013
Revises: 20260829_0012
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260831_0013"
down_revision: str | None = "20260829_0012"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DEFAULT_POLICY = "standard_product"


def upgrade() -> None:
    op.add_column(
        "destinations",
        sa.Column(
            "publication_policy",
            sa.Enum(
                "standard_product",
                "media_fixed_text",
                name="destination_publication_policy",
                native_enum=False,
            ),
            nullable=False,
            server_default=DEFAULT_POLICY,
        ),
    )
    op.add_column(
        "destinations",
        sa.Column("fixed_text", sa.Text(), nullable=False, server_default=""),
    )


def downgrade() -> None:
    op.drop_column("destinations", "fixed_text")
    op.drop_column("destinations", "publication_policy")
