"""Real concurrent INSERTs in an isolated PostgreSQL schema, never public data."""
from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import pytest
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin.service import AdminService, SourceAlreadyExistsError
from emoda.db.base import Base
from emoda.db.models import AdminUser, AuditLog, Source, SourceProfile


@pytest.mark.integration
async def test_concurrent_source_creation_preserves_single_source_and_usable_transaction():
    url = os.environ.get("EMODA_TEST_DATABASE_URL")
    if not url:
        pytest.skip("EMODA_TEST_DATABASE_URL is not set")
    schema = "qa_source_onboarding_" + uuid4().hex
    control = create_async_engine(url)
    async with control.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(url, connect_args={"server_settings": {"search_path": schema}})
    try:
        async with engine.begin() as conn:
            await conn.run_sync(
                lambda sync: Base.metadata.create_all(
                    sync, tables=[model.__table__ for model in (AdminUser, Source, SourceProfile, AuditLog)]
                )
            )
        sessions = async_sessionmaker(engine, expire_on_commit=False)
        barrier = asyncio.Barrier(2)

        class ConcurrentAdmin(AdminService):
            async def find_source_by_channel(self, channel_id):
                found = await super().find_source_by_channel(channel_id)
                # Both transactions observe no source before either INSERT.
                if not getattr(self, "checked", False):
                    self.checked = True
                    assert found is None
                    await asyncio.wait_for(barrier.wait(), timeout=30)
                return found

        async def create(name):
            async with sessions() as session, session.begin():
                try:
                    source = await ConcurrentAdmin(session).create_source(
                        name=name, channel_id=-1001234567, telegram_ref="@same_channel",
                        destination_id=None, actor_id=None, last_seen_message_id=501,
                    )
                    return "created", source.id
                except SourceAlreadyExistsError as exc:
                    # The conflict must not poison the outer transaction.
                    assert await session.scalar(select(func.count(Source.id))) == 1
                    return "existing", exc.source_id

        outcomes = await asyncio.wait_for(asyncio.gather(create("Admin A"), create("Admin B")), timeout=45)
        assert sorted(status for status, _ in outcomes) == ["created", "existing"]
        assert outcomes[0][1] == outcomes[1][1]
        async with sessions() as session:
            assert await session.scalar(select(func.count(Source.id))) == 1
            assert await session.scalar(select(func.count(SourceProfile.id))) == 1
            assert await session.scalar(select(func.count(AuditLog.id))) == 1
            source = await session.scalar(select(Source))
            assert not source.enabled and source.last_seen_message_id == 501
        print(f"POSTGRES_CONCURRENT_ONBOARDING_OK {outcomes}")
    finally:
        await engine.dispose()
        async with control.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await control.dispose()
