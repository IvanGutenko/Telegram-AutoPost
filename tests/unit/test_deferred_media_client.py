import asyncio
from datetime import UTC,datetime,timedelta
import pytest
from emoda.runtime.app import DeferredMediaClient

async def test_authorized_session_reused_without_new_login(tmp_path):
    class Client:
        starts=0
        async def connect(self):pass
        async def is_user_authorized(self):return True
        async def start(self,**kwargs):self.starts+=1
        async def send_file(self,*args,**kwargs):return 'sent'
    client=Client();proxy=DeferredMediaClient(client,tmp_path/'bot.session');stop=asyncio.Event()
    task=asyncio.create_task(proxy.run('token',stop))
    try:
        assert await asyncio.wait_for(proxy.send_file(1,'file'),.2)=='sent'
        assert client.starts==0
    finally:stop.set();await task

async def test_persisted_flood_wait_defers_login_and_media_only(tmp_path):
    class Client:
        starts=0
        async def connect(self):pass
        async def is_user_authorized(self):return False
        async def start(self,**kwargs):self.starts+=1
    client=Client();path=tmp_path/'bot.session'
    path.with_suffix('.retry-after').write_text(str((datetime.now(UTC)+timedelta(hours=1)).timestamp()))
    proxy=DeferredMediaClient(client,path);stop=asyncio.Event();task=asyncio.create_task(proxy.run('token',stop))
    await asyncio.sleep(.01)
    assert client.starts==0 and not proxy.ready.is_set()
    stop.set();await asyncio.wait_for(task,.2)
