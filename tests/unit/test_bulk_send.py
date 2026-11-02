from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import AdminUser, Source


async def test_bulk_send_persists_independent_results_and_never_repeats_sent_targets(tmp_path: Path):
    from emoda.services.bulk_send import BulkSendService
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'bulk.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        admin = AdminUser(telegram_user_id=123)
        sources = [Source(name=f"source{i}", telegram_channel_id=i) for i in (1, 2)]
        session.add_all([admin, *sources])
        await session.flush()
        source_ids = [source.id for source in sources]
        admin_id = admin.id
    calls = []

    async def send(channel_id, text, random_id):
        calls.append((channel_id, text, random_id))
        if channel_id == 2:
            raise TimeoutError("acceptance unknown")
        return [101]

    service = BulkSendService(sessions, send)
    batch_id = await service.create(admin_id, source_ids, "same text")
    await service.send(batch_id, admin_id)
    # Restart / repeated callback cannot repeat a completed or uncertain target.
    await BulkSendService(sessions, send).send(batch_id, admin_id)
    assert len(calls) == 2
    results = await service.results(batch_id, admin_id)
    assert [result["status"] for result in results] == ["sent", "uncertain"]
    assert calls[0][1] == calls[1][1] == "same text"
    await engine.dispose()
