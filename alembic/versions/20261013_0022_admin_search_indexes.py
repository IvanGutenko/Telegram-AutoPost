"""Index generic admin substring searches without blocking publication writes."""
from alembic import op

revision = "20261013_0022"
down_revision = "20261002_0021"
branch_labels = None
depends_on = None


def upgrade():
    op.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
    with op.get_context().autocommit_block():
        op.execute("SET maintenance_work_mem = '32MB'")
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_products_admin_search_trgm
            ON products USING gin (
                emoda_code gin_trgm_ops, original_text gin_trgm_ops,
                cleaned_text gin_trgm_ops, final_text gin_trgm_ops,
                (CAST(parsed_result AS VARCHAR)) gin_trgm_ops
            )
        """)
        op.execute("""
            CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_source_messages_telegram_message_id
            ON source_messages (telegram_message_id)
        """)
        op.execute("RESET maintenance_work_mem")


def downgrade():
    with op.get_context().autocommit_block():
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_source_messages_telegram_message_id")
        op.execute("DROP INDEX CONCURRENTLY IF EXISTS ix_products_admin_search_trgm")
