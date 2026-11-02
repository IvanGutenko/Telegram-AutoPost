from datetime import UTC,datetime,timedelta
from pathlib import Path
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from emoda.db.base import Base
from emoda.db.models import Source,SourceProfile,SourceMessage,Product,AlbumGroup
from emoda.domain.schemas import SourceProfileData
from emoda.domain.enums import ProductStatus,AlbumStatus
from emoda.services.processor import ProcessorService

async def test_ready_product_does_not_starve_older_unprocessed_album(tmp_path):
    engine=create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as c:await c.run_sync(Base.metadata.create_all)
    sessions=async_sessionmaker(engine,expire_on_commit=False)
    now=datetime.now(UTC)
    async with sessions() as s,s.begin():
        sources=[Source(name=str(i),telegram_channel_id=i) for i in (1,2)]
        for source in sources:source.profile=SourceProfile(config=SourceProfileData().model_dump(mode='json'))
        s.add_all(sources);await s.flush()
        g=AlbumGroup(source_id=sources[0].id,grouped_id=123,status=AlbumStatus.READY,last_seen_at=now-timedelta(minutes=15),ready_at=now-timedelta(minutes=10))
        s.add(g);await s.flush()
        m=SourceMessage(source_id=sources[0].id,telegram_message_id=1,grouped_id=123,album_group_id=g.id,telegram_date=now-timedelta(minutes=15),text='product')
        m2=SourceMessage(source_id=sources[1].id,telegram_message_id=2,telegram_date=now,text='product 2')
        s.add_all([m,m2]);await s.flush()
        s.add(Product(source_id=sources[1].id,source_message_id=m2.id,emoda_code='AA99999',status=ProductStatus.READY,updated_at=now))
        expected=sources[0].id
    try:
        result=await ProcessorService(sessions,tmp_path,None,None)._claim_work()
        assert result is not None and result[1]==expected
    finally:await engine.dispose()
