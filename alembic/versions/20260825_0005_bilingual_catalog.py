"""Add Russian and English catalog labels.

Revision ID: 20260825_0005
Revises: 20260825_0004
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260825_0005"
down_revision: str | None = "20260825_0004"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("catalog_nodes") as batch:
        batch.alter_column(
            "text",
            new_column_name="text_ru",
            existing_type=sa.String(length=64),
            existing_nullable=False,
        )
        batch.add_column(sa.Column("text_en", sa.String(length=64), nullable=True))
    op.execute("UPDATE catalog_nodes SET text_en = text_ru WHERE text_en IS NULL")
    with op.batch_alter_table("catalog_nodes") as batch:
        batch.alter_column(
            "text_en",
            existing_type=sa.String(length=64),
            nullable=False,
        )


def downgrade() -> None:
    with op.batch_alter_table("catalog_nodes") as batch:
        batch.drop_column("text_en")
        batch.alter_column(
            "text_ru",
            new_column_name="text",
            existing_type=sa.String(length=64),
            existing_nullable=False,
        )
