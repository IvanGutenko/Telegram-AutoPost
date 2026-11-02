"""Independent history progress and visible source failures."""
import sqlalchemy as sa
from alembic import op
revision = '20261002_0021'
down_revision = '20260910_0020'
branch_labels = None
depends_on = None

def upgrade():
    op.add_column('sources', sa.Column('catchup_cursor_message_id', sa.BigInteger(), nullable=True))
    op.add_column('sources', sa.Column('catchup_scan_before_id', sa.BigInteger(), nullable=True))
    op.add_column('sources', sa.Column('catchup_error', sa.Text(), nullable=True))

def downgrade():
    for name in ('catchup_error', 'catchup_scan_before_id', 'catchup_cursor_message_id'):
        op.drop_column('sources', name)
