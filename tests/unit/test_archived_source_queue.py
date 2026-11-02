from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock
import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from emoda.db.base import Base
from emoda.db.models import Source, SourceProfile, SourceMessage, AlbumGroup, Product
from emoda.domain.schemas import SourceProfileData
from emoda.domain.enums import AlbumStatus, ProductStatus
from emoda.services.processor import ProcessorService

@pytest.mark.parametrize('kind', ['message', 'album', 'product', 'resume'])
async def test_archived_source_backlog_cannot_be_automatically_published(tmp_path, kind):
    engine=create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as c: await c.run_sync(Base.metadata.create_all)
    sessions=async_sessionmaker(engine,expire_on_commit=False)
    now=datetime.now(UTC)
    async with sessions() as s,s.begin():
        source=Source(name='removed',telegram_channel_id=123,enabled=False,archived_at=now)
        source.profile=SourceProfile(config=SourceProfileData().model_dump(mode='json'))
        s.add(source);await s.flush()
        message=SourceMessage(source_id=source.id,telegram_message_id=1,telegram_date=now-timedelta(days=4),text='product')
        if kind=='album':
            album=AlbumGroup(source_id=source.id,grouped_id=123,status=AlbumStatus.COLLECTING,last_seen_at=now-timedelta(days=4),ready_at=now-timedelta(days=1))
            s.add(album);await s.flush();message.album_group_id=album.id;message.grouped_id=123
        s.add(message);await s.flush()
        if kind in {'product','resume'}:
            s.add(Product(source_id=source.id,source_message_id=message.id,emoda_code='AA00001',status=ProductStatus.READY_TO_PUBLISH if kind=='resume' else ProductStatus.READY))
    service=ProcessorService(sessions,tmp_path,None,AsyncMock())
    service.publish_reviewed=AsyncMock()
    try:
        if kind=='resume':
            await service.recover_jobs();service.publish_reviewed.assert_not_awaited()
        else: assert await service._claim_work() is None
    finally: await engine.dispose()
