from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import (
    AlbumGroup,
    Destination,
    MediaItem,
    Product,
    PublicationOutbox,
    Source,
    SourceMessage,
)
from emoda.domain.enums import AlbumStatus, OutboxStatus, ProductStatus
from emoda.integrations.collector import CollectorService
from emoda.integrations.storage import LocalMediaStorage


class MissingMediaClient:
    def __init__(self) -> None:
        self.lookups = 0

    async def get_messages(self, channel_id: int, ids: int) -> None:
        assert channel_id == 123 and ids == 7
        self.lookups += 1
        return None

    async def iter_messages(self, *args: Any, **kwargs: Any) -> Any:
        if False:
            yield None


@pytest.mark.asyncio
async def test_old_missing_media_stops_retrying_and_preserves_publication(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=123, enabled=False, last_seen_message_id=7)
        destination = Destination(name="destination", telegram_channel_id=456)
        session.add_all([source, destination])
        await session.flush()
        album = AlbumGroup(
            source_id=source.id,
            grouped_id=99,
            status=AlbumStatus.READY,
            last_seen_at=now - timedelta(days=9),
            ready_at=now - timedelta(days=9),
        )
        session.add(album)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            grouped_id=99,
            album_group_id=album.id,
            text="original text",
            caption="original text",
            telegram_date=now - timedelta(days=9),
            media_pending=True,
            raw_metadata={},
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="AA00001",
            source_id=source.id,
            album_group_id=album.id,
            destination_id=destination.id,
            status=ProductStatus.READY,
            original_text="original text",
            final_text="published text",
            published_at=now - timedelta(days=8),
        )
        session.add(product)
        await session.flush()
        album.product_id = product.id
        session.add_all([
            MediaItem(
                source_message_id=message.id,
                telegram_message_id=7,
                position=0,
                storage_path="source-1/message-7/evicted.jpg",
                active=True,
                local_deleted_at=now - timedelta(days=5),
            ),
            PublicationOutbox(
                product_id=product.id,
                destination_id=destination.id,
                payload={"media_paths": ["source-1/message-7/evicted.jpg"]},
                payload_hash="original-payload",
                status=OutboxStatus.PUBLISHED,
                telegram_message_ids=[100],
                published_at=now - timedelta(days=8),
            ),
        ])
        message_id, product_id, album_id = message.id, product.id, album.id

    client = MissingMediaClient()
    collector = CollectorService(client, sessions, LocalMediaStorage(tmp_path))  # type: ignore[arg-type]
    await collector.catch_up()
    assert client.lookups == 1

    # An immediate catch-up must respect the durable retry deadline.
    await collector.catch_up()
    assert client.lookups == 1

    for expected_count in (2, 3):
        async with sessions() as session, session.begin():
            row = await session.get(SourceMessage, message_id)
            assert row is not None
            row.raw_metadata = {
                **row.raw_metadata,
                "media_recovery_next_at": (datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
            }
        await collector.catch_up()
        assert client.lookups == expected_count

    async with sessions() as session:
        row = await session.get(SourceMessage, message_id)
        current_product = await session.get(Product, product_id)
        current_album = await session.get(AlbumGroup, album_id)
        outbox = await session.get(PublicationOutbox, 1)
        assert row is not None and row.media_pending is False
        assert "media_download_error" in row.raw_metadata
        assert current_product is not None and current_product.status is ProductStatus.NEEDS_REVIEW
        assert current_product.final_text == "published text"
        assert current_product.published_at is not None
        assert current_product.review_reasons[0]["code"] == "media_download_failed"
        assert current_album is not None and current_album.status is AlbumStatus.PROCESSED
        assert outbox is not None and outbox.status is OutboxStatus.PUBLISHED
        assert outbox.telegram_message_ids == [100]

    await collector.catch_up()
    assert client.lookups == 3
    await engine.dispose()


@pytest.mark.asyncio
async def test_recent_missing_media_remains_retryable(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with sessions() as session, session.begin():
        source = Source(name="recent", telegram_channel_id=123, enabled=False, last_seen_message_id=7)
        session.add(source)
        await session.flush()
        row = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            text="caption",
            caption="caption",
            telegram_date=now - timedelta(minutes=5),
            media_pending=True,
            raw_metadata={"media_recovery_missing_count": 2},
        )
        session.add(row)
        await session.flush()
        message_id = row.id

    client = MissingMediaClient()
    collector = CollectorService(client, sessions, LocalMediaStorage(tmp_path))  # type: ignore[arg-type]
    await collector.catch_up()
    async with sessions() as session:
        row = await session.get(SourceMessage, message_id)
        assert row is not None and row.media_pending is True
        assert row.raw_metadata["media_recovery_missing_count"] == 3
        assert row.raw_metadata["media_recovery_next_at"]
        assert "media_download_error" not in row.raw_metadata
    await engine.dispose()
