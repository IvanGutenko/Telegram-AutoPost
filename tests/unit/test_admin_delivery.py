from types import SimpleNamespace

import pytest
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import SendMediaGroup, SendMessage
from aiogram.types import InputMediaPhoto


class Clock:
    def __init__(self):
        self.now = 1000.0

    async def sleep(self, delay):
        self.now += delay


async def test_admin_output_paces_album_and_text_and_isolates_channels(tmp_path):
    from emoda.integrations.admin_delivery import AdminChatLimiter

    clock = Clock()
    guard = AdminChatLimiter(tmp_path / "limits.json", clock=lambda: clock.now, sleep=clock.sleep)
    calls = []

    async def request(bot, method):
        calls.append((method.chat_id, clock.now))
        return SimpleNamespace(message_id=1)

    album = SendMediaGroup(chat_id=123, media=[InputMediaPhoto(media="photo")] * 3)
    await guard(request, None, album)
    await guard(request, None, SendMessage(chat_id=123, text="text"))
    await guard(request, None, SendMessage(chat_id=-100123, text="publication"))
    assert calls[1][1] - calls[0][1] == pytest.approx(3.3)
    assert calls[2][1] == calls[1][1]


async def test_cooldown_survives_restart_and_prevents_api_requests(tmp_path):
    from emoda.integrations.admin_delivery import AdminChatLimiter

    clock = Clock()
    path = tmp_path / "limits.json"
    guard = AdminChatLimiter(path, clock=lambda: clock.now, sleep=clock.sleep)
    calls = 0
    method = SendMessage(chat_id=123, text="text")

    async def request(bot, method):
        nonlocal calls
        calls += 1
        raise TelegramRetryAfter(method=method, message="flood", retry_after=3600)

    with pytest.raises(TelegramRetryAfter):
        await guard(request, None, method)
    guard = AdminChatLimiter(path, clock=lambda: clock.now, sleep=clock.sleep)
    with pytest.raises(TelegramRetryAfter):
        await guard(request, None, method)
    assert calls == 1


async def test_cooldown_blocks_edits_without_api_attempts(tmp_path):
    from aiogram.methods import EditMessageText
    from emoda.integrations.admin_delivery import AdminChatLimiter

    guard = AdminChatLimiter(tmp_path / "limits.json")
    guard.defer(123, 100)

    async def request(bot, method):
        raise AssertionError("must not edit during Telegram cooldown")

    with pytest.raises(TelegramRetryAfter):
        await guard(request, None, EditMessageText(chat_id=123, message_id=77, text="menu"))


async def test_expired_cooldown_resumes_delivery(tmp_path):
    from emoda.integrations.admin_delivery import AdminChatLimiter

    clock = Clock()
    guard = AdminChatLimiter(tmp_path / "limits.json", clock=lambda: clock.now, sleep=clock.sleep)
    guard.defer(123, 100)
    clock.now += 102
    calls = []

    async def request(bot, method):
        calls.append(method.text)
        return SimpleNamespace(message_id=77)

    await guard(request, None, SendMessage(chat_id=123, text="recovered"))
    assert calls == ["recovered"]
