"""Exercise queue access paths on PostgreSQL in a disposable schema."""
import json
import os
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.schema import CreateIndex

from emoda.db.models import SourceMessage


@pytest.mark.integration
@pytest.mark.parametrize('query,expected_count', [
    ('SELECT id FROM source_messages WHERE album_group_id=123', 5),
    ('SELECT id FROM source_messages WHERE media_pending IS TRUE '
     'AND (album_group_id=123 OR id=194)', 1),
    ('SELECT id FROM source_messages WHERE source_id=3 AND grouped_id IS NULL '
     'AND album_group_id IS NULL AND processed_at IS NULL AND media_pending IS FALSE', 49),
])
async def test_queue_queries_do_not_scan_message_history(query, expected_count):
    url = os.environ.get('EMODA_TEST_DATABASE_URL')
    if not url:
        pytest.skip('EMODA_TEST_DATABASE_URL is not set')
    schema = 'qa_queue_indexes_' + uuid4().hex
    engine = create_async_engine(url)
    try:
        async with engine.begin() as conn:
            await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
            await conn.execute(text(f'SET LOCAL search_path TO "{schema}"'))
            await conn.execute(text('''CREATE TABLE source_messages (
                id integer PRIMARY KEY, source_id integer, telegram_message_id bigint,
                telegram_date timestamptz, album_group_id integer, grouped_id bigint,
                media_pending boolean, processed_at timestamptz)'''))
            await conn.execute(text('''INSERT INTO source_messages
                SELECT i, i%20, i, now(), CASE WHEN i<=19000 THEN i/5 END,
                CASE WHEN i<=19000 THEN i/5 END, i%97=0,
                CASE WHEN i<=19000 THEN now() END FROM generate_series(1,20000) i'''))
            names = {'ix_source_messages_album_message', 'ix_source_messages_pending_media',
                     'ix_source_messages_unprocessed_single'}
            for index in SourceMessage.__table__.indexes:
                if index.name in names:
                    await conn.execute(text(str(CreateIndex(index).compile(dialect=postgresql.dialect()))))
            await conn.execute(text('ANALYZE source_messages'))
            rows = (await conn.execute(text(query))).all()
            assert len(rows) == expected_count
            plan = (await conn.execute(text('EXPLAIN (ANALYZE, FORMAT JSON) ' + query))).scalar_one()
            if isinstance(plan, str):
                plan = json.loads(plan)
            assert 'Seq Scan' not in json.dumps(plan), plan
    finally:
        async with engine.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        await engine.dispose()
