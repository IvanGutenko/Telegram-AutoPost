"""Drop obsolete destination presentation settings.

Revision ID: 20260901_0017
Revises: 20260901_0016
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260901_0017"
down_revision: str | None = "20260901_0016"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.drop_column("destinations", "fixed_text")
    op.drop_column("destinations", "publication_policy")
    op.drop_column("destinations", "template")
    op.drop_column("destinations", "signature_en")
    op.drop_column("destinations", "signature_ru")


def downgrade() -> None:
    op.add_column(
        "destinations",
        sa.Column("signature_ru", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column(
        "destinations",
        sa.Column("signature_en", sa.Text(), nullable=False, server_default=""),
    )
    op.add_column("destinations", sa.Column("template", sa.Text(), nullable=True))
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
            server_default="standard_product",
        ),
    )
    op.add_column(
        "destinations",
        sa.Column("fixed_text", sa.Text(), nullable=False, server_default=""),
    )
