from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import AlbumGroup, Product, Source, SourceMessage
from emoda.domain.enums import AlbumStatus, ProductStatus
from emoda.services.albums import AlbumCoordinator


@pytest.fixture
async def session() -> AsyncSession:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


async def setup_source(session: AsyncSession) -> Source:
    source = Source(name="supplier", telegram_channel_id=123, enabled=True)
    session.add(source)
    await session.flush()
    return source


async def message(
    session: AsyncSession, source_id: int, message_id: int, grouped_id: int
) -> SourceMessage:
    item = SourceMessage(
        source_id=source_id,
        telegram_message_id=message_id,
        grouped_id=grouped_id,
        text="",
        telegram_date=datetime.now(UTC),
    )
    session.add(item)
    await session.flush()
    return item


async def test_new_album_item_resets_persistent_debounce(session: AsyncSession) -> None:
    source = await setup_source(session)
    coordinator = AlbumCoordinator(Decimal("2"))
    now = datetime.now(UTC)
    first = await coordinator.add_item(session, await message(session, source.id, 1, 77), now=now)
    second = await coordinator.add_item(
        session, await message(session, source.id, 2, 77), now=now + timedelta(seconds=1)
    )
    assert first.id == second.id
    assert second.ready_at == now + timedelta(seconds=3)


async def test_overdue_album_is_claimed_after_restart(session: AsyncSession) -> None:
    source = await setup_source(session)
    now = datetime.now(UTC)
    group = AlbumGroup(
        source_id=source.id,
        grouped_id=77,
        status=AlbumStatus.COLLECTING,
        last_seen_at=now - timedelta(seconds=3),
        ready_at=now - timedelta(seconds=1),
    )
    session.add(group)
    await session.flush()
    claimed = await AlbumCoordinator(Decimal("2")).claim_ready(session, now=now)
    assert [item.id for item in claimed] == [group.id]
    assert group.status is AlbumStatus.READY


async def test_late_item_reopens_existing_group_without_duplicate(session: AsyncSession) -> None:
    source = await setup_source(session)
    now = datetime.now(UTC)
    group = AlbumGroup(
        source_id=source.id,
        grouped_id=77,
        status=AlbumStatus.PROCESSED,
        last_seen_at=now,
        ready_at=now,
        product_id=None,
    )
    session.add(group)
    await session.flush()
    reopened = await AlbumCoordinator(Decimal("2")).add_item(
        session, await message(session, source.id, 5, 77), now=now
    )
    assert reopened.id == group.id
    assert reopened.status is AlbumStatus.COLLECTING
    assert reopened.ready_at == now + timedelta(seconds=2)


async def test_late_item_supersedes_inflight_product_and_forces_complete_reprocess(
    session: AsyncSession,
) -> None:
    source = await setup_source(session)
    now = datetime.now(UTC)
    group = AlbumGroup(
        source_id=source.id,
        grouped_id=78,
        status=AlbumStatus.PROCESSED,
        last_seen_at=now,
        ready_at=now,
    )
    session.add(group)
    await session.flush()
    product = Product(
        emoda_code="EM-LATE-1",
        source_id=source.id,
        album_group_id=group.id,
        status=ProductStatus.PROCESSING,
    )
    session.add(product)
    await session.flush()
    group.product_id = product.id

    await AlbumCoordinator(Decimal("2")).add_item(
        session, await message(session, source.id, 6, 78), now=now
    )
    assert group.status is AlbumStatus.COLLECTING
    assert product.status is ProductStatus.READY
    assert product.processing_error is None
    assert product.review_reasons == []


async def test_album_items_are_sorted_by_telegram_message_id(session: AsyncSession) -> None:
    source = await setup_source(session)
    now = datetime.now(UTC)
    coordinator = AlbumCoordinator(Decimal("2"))
    group = await coordinator.add_item(session, await message(session, source.id, 9, 77), now=now)
    await coordinator.add_item(session, await message(session, source.id, 4, 77), now=now)
    items = await coordinator.items(session, group.id)
    assert [item.telegram_message_id for item in items] == [4, 9]


async def test_ready_album_waits_for_durable_media_download(session: AsyncSession) -> None:
    source = await setup_source(session)
    now = datetime.now(UTC)
    group = AlbumGroup(
        source_id=source.id,
        grouped_id=88,
        status=AlbumStatus.COLLECTING,
        last_seen_at=now - timedelta(seconds=3),
        ready_at=now - timedelta(seconds=1),
    )
    session.add(group)
    await session.flush()
    pending = await message(session, source.id, 1, 88)
    pending.album_group_id = group.id
    pending.media_pending = True
    await session.flush()
    assert await AlbumCoordinator(Decimal("2")).claim_ready(session, now=now) == []


async def test_cross_source_message_cannot_attach_to_album(session: AsyncSession) -> None:
    first = await setup_source(session)
    second = Source(name="other supplier", telegram_channel_id=456, enabled=True)
    session.add(second)
    await session.flush()
    now = datetime.now(UTC)
    group = AlbumGroup(
        source_id=first.id,
        grouped_id=91,
        status=AlbumStatus.COLLECTING,
        last_seen_at=now,
        ready_at=now,
    )
    session.add(group)
    await session.flush()
    foreign_message = await message(session, second.id, 1, 91)

    with pytest.raises(ValueError, match="different source"):
        await AlbumCoordinator(Decimal("2")).attach_item(
            session,
            group,
            foreign_message,
            now=now,
        )

    assert foreign_message.album_group_id is None
