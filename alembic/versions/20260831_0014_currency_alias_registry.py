"""Add the global data-driven currency alias registry.

Revision ID: 20260831_0014
Revises: 20260831_0013
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260831_0014"
down_revision: str | None = "20260831_0013"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SYSTEM_ALIASES: tuple[tuple[str, str, str], ...] = (
    ("$", "$", "USD"),
    ("💲", "💲", "USD"),
    ("💵", "💵", "USD"),
    ("USD", "usd", "USD"),
    ("US$", "us$", "USD"),
    ("€", "€", "EUR"),
    ("EUR", "eur", "EUR"),
)


def upgrade() -> None:
    table = op.create_table(
        "currency_aliases",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("alias", sa.String(length=64), nullable=False),
        sa.Column("normalized_alias", sa.String(length=64), nullable=False),
        sa.Column("currency", sa.String(length=3), nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default=sa.true(), nullable=False),
        sa.Column("system", sa.Boolean(), server_default=sa.false(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_currency_aliases_normalized_alias",
        "currency_aliases",
        ["normalized_alias"],
        unique=True,
    )
    op.create_index(
        "ix_currency_aliases_currency", "currency_aliases", ["currency"], unique=False
    )
    op.bulk_insert(
        table,
        [
            {
                "alias": alias,
                "normalized_alias": normalized,
                "currency": currency,
                "enabled": True,
                "system": True,
            }
            for alias, normalized, currency in SYSTEM_ALIASES
        ],
    )


def downgrade() -> None:
    op.drop_index("ix_currency_aliases_currency", table_name="currency_aliases")
    op.drop_index("ix_currency_aliases_normalized_alias", table_name="currency_aliases")
    op.drop_table("currency_aliases")
