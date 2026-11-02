from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import AlbumGroup, Destination, Product, Source, SourceMessage, SourceProfile
from emoda.domain.enums import AlbumStatus, ProductStatus, PublicationMode


@pytest.fixture
async def session() -> AsyncSession:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


async def make_source(session: AsyncSession, channel_id: int = 100) -> Source:
    destination = Destination(name=f"D{channel_id}", telegram_channel_id=-100000 - channel_id)
    source = Source(name=f"S{channel_id}", telegram_channel_id=channel_id, enabled=True)
    source.profile = SourceProfile(
        config={"schema_version": 1}, publication_mode=PublicationMode.REVIEW
    )
    session.add_all([destination, source])
    await session.flush()
    return source


async def test_duplicate_update_is_blocked_by_database(session: AsyncSession) -> None:
    source = await make_source(session)
    session.add(
        SourceMessage(
            source_id=source.id, telegram_message_id=10, text="one", telegram_date=datetime.now(UTC)
        )
    )
    await session.flush()
    session.add(
        SourceMessage(
            source_id=source.id, telegram_message_id=10, text="two", telegram_date=datetime.now(UTC)
        )
    )
    with pytest.raises(IntegrityError):
        await session.flush()


async def test_same_message_id_is_allowed_for_different_sources(session: AsyncSession) -> None:
    first = await make_source(session, 101)
    second = await make_source(session, 102)
    session.add_all(
        [
            SourceMessage(
                source_id=first.id,
                telegram_message_id=10,
                text="a",
                telegram_date=datetime.now(UTC),
            ),
            SourceMessage(
                source_id=second.id,
                telegram_message_id=10,
                text="b",
                telegram_date=datetime.now(UTC),
            ),
        ]
    )
    await session.flush()


async def test_album_group_is_unique_per_source(session: AsyncSession) -> None:
    source = await make_source(session)
    ready = datetime.now(UTC) + timedelta(seconds=2)
    session.add(
        AlbumGroup(
            source_id=source.id,
            grouped_id=777,
            status=AlbumStatus.COLLECTING,
            ready_at=ready,
            last_seen_at=datetime.now(UTC),
        )
    )
    await session.flush()
    session.add(
        AlbumGroup(
            source_id=source.id,
            grouped_id=777,
            status=AlbumStatus.COLLECTING,
            ready_at=ready,
            last_seen_at=datetime.now(UTC),
        )
    )
    with pytest.raises(IntegrityError):
        await session.flush()


async def test_product_code_is_globally_unique(session: AsyncSession) -> None:
    source = await make_source(session)
    first = SourceMessage(
        source_id=source.id, telegram_message_id=1, text="a", telegram_date=datetime.now(UTC)
    )
    second = SourceMessage(
        source_id=source.id, telegram_message_id=2, text="b", telegram_date=datetime.now(UTC)
    )
    session.add_all([first, second])
    await session.flush()
    session.add(
        Product(
            emoda_code="EM-000001",
            source_id=source.id,
            source_message_id=first.id,
            status=ProductStatus.RECEIVED,
        )
    )
    await session.flush()
    session.add(
        Product(
            emoda_code="EM-000001",
            source_id=source.id,
            source_message_id=second.id,
            status=ProductStatus.RECEIVED,
        )
    )
    with pytest.raises(IntegrityError):
        await session.flush()


async def test_edit_updates_snapshot_without_second_product(session: AsyncSession) -> None:
    source = await make_source(session)
    message = SourceMessage(
        source_id=source.id, telegram_message_id=1, text="old", telegram_date=datetime.now(UTC)
    )
    session.add(message)
    await session.flush()
    product = Product(
        emoda_code="EM-000001",
        source_id=source.id,
        source_message_id=message.id,
        status=ProductStatus.RECEIVED,
    )
    session.add(product)
    await session.flush()
    message.text = "new"
    message.edit_date = datetime.now(UTC)
    await session.flush()
    assert product.source_message_id == message.id


async def test_source_message_processing_deadline_is_persistent(session: AsyncSession) -> None:
    source = await make_source(session, 404)
    deadline = datetime.now(UTC) + timedelta(seconds=90)
    message = SourceMessage(
        source_id=source.id,
        telegram_message_id=44,
        text="same caption",
        telegram_date=datetime.now(UTC),
        processing_after=deadline,
    )
    session.add(message)
    await session.flush()
    await session.refresh(message)
    persisted = message.processing_after
    assert persisted is not None
    if persisted.tzinfo is None:  # SQLite test adapter drops timezone metadata.
        persisted = persisted.replace(tzinfo=UTC)
    assert persisted == deadline
