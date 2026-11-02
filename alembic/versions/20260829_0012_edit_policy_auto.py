"""Move stored source profiles still persisting edit_policy=review to auto.

Revision ID: 20260829_0012
Revises: 20260827_0011
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260829_0012"
down_revision: str | None = "20260827_0011"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

# Stored profiles serialized the previous default; the persisted value must
# follow the new code default or production keeps demanding an admin review
# for every supplier edit.
EDIT_POLICY_AUTO_SQL = """
UPDATE source_profiles
SET config = jsonb_set(config::jsonb, '{edit_policy}', '"auto"')::json
WHERE config->>'edit_policy' = 'review'
"""


EDIT_POLICY_REVIEW_SQL = """
UPDATE source_profiles
SET config = jsonb_set(config::jsonb, '{edit_policy}', '"review"')::json
WHERE config->>'edit_policy' = 'auto'
"""


def upgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    op.execute(sa.text(EDIT_POLICY_AUTO_SQL))


def downgrade() -> None:
    if op.get_bind().dialect.name != "postgresql":
        return
    op.execute(sa.text(EDIT_POLICY_REVIEW_SQL))
