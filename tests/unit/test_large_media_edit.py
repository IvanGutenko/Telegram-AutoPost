from emoda.integrations.publisher import TelegramBotPublisher
from test_publication import FakeBot

async def test_large_video_edit_uses_same_message_via_mtproto(tmp_path,monkeypatch):
    path=tmp_path/'new.mp4';path.write_bytes(b'1234')
    monkeypatch.setattr('emoda.integrations.publisher.BOT_API_VIDEO_LIMIT_BYTES',3)
    class Media:
        calls=[]
        async def edit_message(self,*args,**kwargs):self.calls.append((args,kwargs))
    bot=FakeBot();media=Media();publisher=TelegramBotPublisher(bot,None,media_client=media)
    ids=await publisher.update(-1001,[40],{'media_paths':['old.mp4'],'text':'old'}, {'media_paths':[str(path)],'text':'new'},[[{'text':'order','url':'https://example.com'}]])
    assert ids==[40] and bot.edited_media_ids==[]
    assert media.calls[0][0]==(-1001,40)
    assert media.calls[0][1]['file']==str(path)
    assert media.calls[0][1]['text']=='new'
    assert media.calls[0][1]['buttons']
