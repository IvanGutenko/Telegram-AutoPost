from datetime import UTC,datetime
from pathlib import Path
import pytest
from sqlalchemy.ext.asyncio import create_async_engine,async_sessionmaker
from emoda.db.base import Base
from emoda.db.models import Source,SourceProfile,SourceMessage,Product,Destination,MediaItem,PublicationOutbox
from emoda.domain.enums import ProductStatus,OutboxStatus
from emoda.domain.schemas import SourceProfileData
from emoda.services.processor import ProcessorService
from emoda.services.publication import PublicationService

@pytest.mark.parametrize('approved,expected',[(True,'edit'),(False,'edit')])
async def test_type_change_preserves_existing_message(tmp_path,approved,expected):
    engine=create_async_engine('sqlite+aiosqlite:///:memory:')
    async with engine.begin() as c:await c.run_sync(Base.metadata.create_all)
    sm=async_sessionmaker(engine,expire_on_commit=False)
    path=tmp_path/'new.mp4';path.write_bytes(b'video fixture')
    class Transport:
        async def validate_destination(self,*a):pass
    class Publication(PublicationService):
        operation=None
        async def replace(self,s,oid,original):self.operation='replace';return await s.get(PublicationOutbox,oid)
        async def update(self,s,oid,original):self.operation='edit';return await s.get(PublicationOutbox,oid)
    pub=Publication(Transport())
    async with sm() as s,s.begin():
        source=Source(name='source',telegram_channel_id=1);source.profile=SourceProfile(config=SourceProfileData().model_dump(mode='json'))
        destination=Destination(name='destination',telegram_channel_id=2);s.add_all([source,destination]);await s.flush()
        m=SourceMessage(source_id=source.id,telegram_message_id=1,telegram_date=datetime.now(UTC));s.add(m);await s.flush()
        p=Product(source_id=source.id,source_message_id=m.id,destination_id=destination.id,emoda_code='AA12345',status=ProductStatus.READY_TO_PUBLISH,published_at=datetime.now(UTC),final_text='Approved alternative',parsed_result={'publication_format':'alternative_text'})
        s.add(p);await s.flush();pid=p.id
        s.add(MediaItem(source_message_id=m.id,telegram_message_id=1,position=0,media_type='video',storage_path='new.mp4',size_bytes=13,sha256='x',active=True))
        s.add(PublicationOutbox(product_id=p.id,destination_id=destination.id,status=OutboxStatus.PUBLISHED,published_at=datetime.now(UTC),payload={'media_paths':[str(tmp_path/'old.jpg')],'text':'Old','publication_format':'alternative_text'},payload_hash='old',keyboard_snapshot=[],telegram_message_ids=[123],random_ids=[]))
    try:
        await ProcessorService(sm,tmp_path,None,pub,bot_username='test_bot')._enqueue_and_publish(pid,[str(path)],review_approved=approved)
        assert pub.operation==expected
    finally:await engine.dispose()
