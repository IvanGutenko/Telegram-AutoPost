"""Exercise source onboarding via Dispatcher and inspect persisted state."""
from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
from aiogram import Bot, Dispatcher
from aiogram.types import CallbackQuery, Chat, Message, Update, User
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin.router import create_admin_router
from emoda.admin.service import AdminService
from emoda.db.base import Base, utcnow
from emoda.db.models import AuditLog, Source
from emoda.domain.enums import PublicationMode

ADMIN_ID = 990002


@pytest.fixture
async def journey(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        source = await AdminService(session).create_source(
            name="Existing <supplier>", channel_id=-1001001,
            telegram_ref="@existing", destination_id=None, actor_id=None,
            last_seen_message_id=77,
        )
        source.enabled = True
        source.profile.config = {**source.profile.config, "publication_mode": "auto"}
        source.profile.publication_mode = PublicationMode.AUTO

    class Journey:
        def __init__(self):
            self.sessions = sessions
            self.screens: list[tuple[str, Any]] = []
            self.answers: list[str] = []
            self.index = 0
            self.bot = Bot(token="42:TEST")
            self.dispatcher = Dispatcher()

        async def callback(self, data: str):
            self.index += 1
            user = User(id=ADMIN_ID, is_bot=False, first_name="Admin")
            message = Message(message_id=50, chat=Chat(id=ADMIN_ID, type="private"),
                              date=datetime.now(UTC), from_user=user)
            await self.dispatcher.feed_update(self.bot, Update(
                update_id=self.index, callback_query=CallbackQuery(
                    id=str(self.index), from_user=user, message=message,
                    data=data, chat_instance="test",
                ),
            ))

        async def message(self, text: str):
            self.index += 1
            await self.dispatcher.feed_update(self.bot, Update(
                update_id=self.index, message=Message(
                    message_id=50 + self.index, chat=Chat(id=ADMIN_ID, type="private"),
                    date=datetime.now(UTC), text=text,
                    from_user=User(id=ADMIN_ID, is_bot=False, first_name="Admin"),
                ),
            ))

        async def state(self):
            return self.dispatcher.fsm.get_context(
                bot=self.bot, chat_id=ADMIN_ID, user_id=ADMIN_ID,
            )

        async def snapshot(self):
            async with sessions() as session:
                rows = list(await session.scalars(select(Source).order_by(Source.id)))
                return [
                    (s.id, s.name, s.telegram_channel_id, s.telegram_ref, s.enabled,
                     s.archived_at, s.last_seen_message_id, s.catchup_anchor_message_id,
                     s.profile.config, s.profile.version)
                    for s in rows
                ]

        def buttons(self):
            markup = self.screens[-1][1]
            return [b.callback_data for row in markup.inline_keyboard for b in row]

    j = Journey()

    async def answer_message(self, text="", **kwargs):
        j.screens.append((text, kwargs.get("reply_markup")))
        return self

    async def answer_callback(self, text="", **kwargs):
        j.answers.append(text)
        return True

    monkeypatch.setattr(Message, "answer", answer_message)
    monkeypatch.setattr(Message, "edit_text", answer_message)
    monkeypatch.setattr(CallbackQuery, "answer", answer_callback)

    async def validate(reference):
        channel = -1001001 if "existing" in reference else -1002002
        return channel, "Detected supplier", 500

    async def loader(reference):
        return "", []

    j.dispatcher.include_router(create_admin_router(
        sessions, {ADMIN_ID}, access_validator=validate,
        test_message_loader=loader, intelligence=None, media_root=tmp_path,
    ))
    try:
        yield j
    finally:
        await j.bot.session.close()
        await engine.dispose()


@pytest.mark.parametrize("reference", ["@existing", "https://t.me/existing"])
async def test_existing_channel_opens_existing_source_without_entering_creation(journey, reference):
    before = await journey.snapshot()
    await journey.callback("source:add")
    await journey.message(reference)
    assert "уже подключён" in journey.screens[-1][0]
    assert "Existing &lt;supplier&gt;" in journey.screens[-1][0]
    assert "source:1" in journey.buttons()
    assert await (await journey.state()).get_state() is None
    assert await journey.snapshot() == before


async def test_archived_channel_can_be_restored_without_losing_profile(journey):
    async with journey.sessions() as session, session.begin():
        await AdminService(session).archive_source(1, actor_id=None)
    before = await journey.snapshot()
    await journey.callback("source:add")
    await journey.message("@existing")
    assert "в архиве" in journey.screens[-1][0]
    assert "source:restore:1" in journey.buttons()
    assert await journey.snapshot() == before
    await journey.callback("source:restore:1")
    after = await journey.snapshot()
    assert len(after) == 1 and after[0][0] == 1
    assert after[0][4] is False and after[0][5] is None
    assert after[0][8:] == before[0][8:]


async def test_new_source_is_created_once_and_repeated_save_has_clear_answer(journey):
    await journey.callback("source:add")
    await journey.message("@new")
    await journey.message("New supplier")
    await journey.callback("source:dest:0")
    assert "Источник создан выключенным в REVIEW" in journey.screens[-1][0]
    first = await journey.snapshot()
    assert len(first) == 2 and first[1][4] is False
    assert first[1][8]["publication_mode"] == "review"
    assert first[1][6:8] == (500, 500)
    await journey.callback("source:dest:0")
    assert "уже завершён" in journey.answers[-1]
    assert await journey.snapshot() == first


@pytest.mark.parametrize("archived", [False, True])
async def test_channel_added_while_wizard_is_open_returns_existing_source(journey, archived):
    await journey.callback("source:add")
    await journey.message("@new")
    await journey.message("Must not overwrite")
    async with journey.sessions() as session, session.begin():
        source = await AdminService(session).create_source(
            name="Created by another admin", channel_id=-1002002,
            telegram_ref="https://t.me/new", destination_id=None, actor_id=None,
            last_seen_message_id=123,
        )
        if archived:
            source.archived_at = utcnow()
    before = await journey.snapshot()
    await journey.callback("source:dest:0")
    assert "Код ошибки" not in " ".join(journey.answers)
    assert "в архиве" in journey.screens[-1][0] if archived else "уже подключён" in journey.screens[-1][0]
    assert ("source:restore:2" if archived else "source:2") in journey.buttons()
    assert await (await journey.state()).get_state() is None
    assert await journey.snapshot() == before


async def test_service_duplicate_does_not_poison_transaction_or_create_audit(journey):
    async with journey.sessions() as session, session.begin():
        count = await session.scalar(select(func.count(AuditLog.id)))
        with pytest.raises(ValueError, match="already exists"):
            await AdminService(session).create_source(
                name="Duplicate", channel_id=-1001001, telegram_ref=None,
                destination_id=None, actor_id=None,
            )
        assert await session.scalar(select(func.count(Source.id))) == 1
        assert await session.scalar(select(func.count(AuditLog.id))) == count


async def test_empty_wizard_data_does_not_raise_generic_error(journey):
    from emoda.admin.router import AdminStates

    state = await journey.state()
    await state.set_state(AdminStates.source_destination)
    await journey.callback("source:dest:0")
    assert "Код ошибки" not in " ".join(journey.answers)
    assert "заново" in journey.screens[-1][0]
    assert "source:add" in journey.buttons()
