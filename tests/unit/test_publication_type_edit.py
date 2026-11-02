from emoda.integrations.publisher import TelegramBotPublisher
from test_publication import FakeBot

async def test_photo_to_video_edits_existing_message():
    bot=FakeBot()
    result=await TelegramBotPublisher(bot,history_client=None).update(
        -1001,[40],{'text':'old','media_paths':['a.jpg']},
        {'text':'new','media_paths':['a.mp4']},[])
    assert result==[40]
    assert bot.edited_media_ids==[40]
