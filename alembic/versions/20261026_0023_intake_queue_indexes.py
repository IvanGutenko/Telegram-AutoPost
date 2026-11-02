"""Index album membership and active intake work without blocking writes."""
from alembic import op

revision = "20261026_0023"
down_revision = "20261013_0022"
branch_labels = None
depends_on = None


def upgrade():
    with op.get_context().autocommit_block():
        op.execute("SET maintenance_work_mem = '32MB'")
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_source_messages_album_message
            ON source_messages (album_group_id, telegram_message_id)
        """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_source_messages_pending_media
            ON source_messages (album_group_id, id) WHERE media_pending IS TRUE
        """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_source_messages_unprocessed_single
            ON source_messages (source_id, telegram_date, telegram_message_id)
            WHERE grouped_id IS NULL AND album_group_id IS NULL
                AND processed_at IS NULL AND media_pending IS FALSE
        """)
        op.execute("RESET maintenance_work_mem")


def downgrade():
    with op.get_context().autocommit_block():
        for name in (
            "ix_source_messages_unprocessed_single",
            "ix_source_messages_pending_media",
            "ix_source_messages_album_message",
        ):
            op.execute(f"DROP INDEX CONCURRENTLY IF EXISTS {name}")
