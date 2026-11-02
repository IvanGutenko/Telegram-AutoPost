from datetime import UTC, datetime

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import MessageEventTransition, Source, SourceMessage, SourceProfile
from emoda.domain.enums import EventOrigin, MessageState, PublicationMode
from emoda.domain.schemas import SourceProfileData
from emoda.services.outcomes import set_duplicate_outcome_once, set_message_outcome


@pytest.mark.asyncio
async def test_message_outcome_keeps_typed_current_state_and_append_only_history() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="ledger", telegram_channel_id=9001, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.AUTO,
            config=SourceProfileData().model_dump(mode="json"),
        )
        session.add(source)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=77,
            telegram_date=datetime.now(UTC),
            event_origin=EventOrigin.CATCHUP,
            event_type="new",
        )
        session.add(message)
        await session.flush()
        set_message_outcome(message, "received")
        set_message_outcome(message, "collecting_album")
        set_message_outcome(message, "processing")
        set_message_outcome(message, "published")
        message_id = message.id
    async with maker() as session:
        stored = await session.get(SourceMessage, message_id)
        transitions = list(
            await session.scalars(
                select(MessageEventTransition)
                .where(MessageEventTransition.source_message_id == message_id)
                .order_by(MessageEventTransition.id)
            )
        )
    assert stored is not None
    assert stored.state == MessageState.PUBLISHED
    assert stored.event_origin == EventOrigin.CATCHUP
    assert [item.state for item in transitions] == [
        MessageState.RECEIVED,
        MessageState.COLLECTING,
        MessageState.PROCESSING,
        MessageState.PUBLISHED,
    ]
    assert all(item.source_id == stored.source_id for item in transitions)
    assert all(item.telegram_message_id == 77 for item in transitions)
    await engine.dispose()


@pytest.mark.asyncio
async def test_duplicate_event_does_not_overwrite_terminal_message_state() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="ledger", telegram_channel_id=9002, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.AUTO,
            config=SourceProfileData().model_dump(mode="json"),
        )
        session.add(source)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=91,
            telegram_date=datetime.now(UTC),
            state=MessageState.PUBLISHED,
        )
        session.add(message)
        await session.flush()
        set_message_outcome(
            message,
            "duplicate",
            "unchanged telegram update",
            update_current=False,
        )
        message_id = message.id
    async with maker() as session:
        stored = await session.get(SourceMessage, message_id)
        duplicate = await session.scalar(
            select(MessageEventTransition).where(
                MessageEventTransition.source_message_id == message_id,
                MessageEventTransition.state == MessageState.DUPLICATE,
            )
        )
    assert stored is not None
    assert stored.state is MessageState.PUBLISHED
    assert duplicate is not None
    assert duplicate.reason == "unchanged telegram update"
    await engine.dispose()


@pytest.mark.asyncio
async def test_repeated_duplicate_event_is_recorded_only_once() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="ledger", telegram_channel_id=9003, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.AUTO,
            config=SourceProfileData().model_dump(mode="json"),
        )
        session.add(source)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=92,
            telegram_date=datetime.now(UTC),
            state=MessageState.REVIEW,
            event_origin=EventOrigin.CATCHUP,
        )
        session.add(message)
        await session.flush()
        assert await set_duplicate_outcome_once(session, message) is True
        message_id = message.id
    async with maker() as session, session.begin():
        message = await session.get(SourceMessage, message_id)
        assert message is not None
        assert await set_duplicate_outcome_once(session, message) is False
    async with maker() as session:
        duplicates = list(
            await session.scalars(
                select(MessageEventTransition).where(
                    MessageEventTransition.source_message_id == message_id,
                    MessageEventTransition.state == MessageState.DUPLICATE,
                )
            )
        )
        stored = await session.get(SourceMessage, message_id)
    assert len(duplicates) == 1
    assert stored is not None and stored.state is MessageState.REVIEW
    await engine.dispose()
