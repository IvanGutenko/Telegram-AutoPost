"""Switch new E-MODA products to two letters plus five digits.

Revision ID: 20260826_0006
Revises: 20260825_0005
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260826_0006"
down_revision: str | None = "20260825_0005"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO code_counters (name, next_value) "
            "VALUES ('products_alpha2_numeric5', 0) "
            "ON CONFLICT (name) DO NOTHING"
        )
    )
    op.execute(
        sa.text(
            "INSERT INTO global_settings (key, value) "
            "VALUES ('emoda_code', CAST(:value AS JSON)) "
            "ON CONFLICT (key) DO UPDATE "
            "SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP"
        ).bindparams(value='{"format": "alpha2_numeric5"}')
    )


def downgrade() -> None:
    op.execute(
        sa.text(
            "INSERT INTO global_settings (key, value) "
            "VALUES ('emoda_code', CAST(:value AS JSON)) "
            "ON CONFLICT (key) DO UPDATE "
            "SET value = EXCLUDED.value, updated_at = CURRENT_TIMESTAMP"
        ).bindparams(value='{"prefix": "EM", "width": 6}')
    )
    # Keep products_alpha2_numeric5: deleting it would allow reuse after re-upgrade.
