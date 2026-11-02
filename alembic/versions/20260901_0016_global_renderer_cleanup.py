"""Remove dormant destination text now that one global renderer is mandatory.

Revision ID: 20260901_0016
Revises: 20260901_0015
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260901_0016"
down_revision: str | None = "20260901_0015"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute(
        """
        UPDATE destinations
        SET fixed_text = '',
            template = NULL,
            signature_ru = '',
            signature_en = ''
        WHERE fixed_text <> ''
           OR template IS NOT NULL
           OR signature_ru <> ''
           OR signature_en <> ''
        """
    )


def downgrade() -> None:
    # Removed presentation text cannot be reconstructed reliably.
    pass
