"""A missing local album member must never produce a partial Telegram post."""

from datetime import UTC, datetime
from pathlib import Path

import pytest
from sqlalchemy import select
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
from emoda.domain.enums import AlbumStatus, OutboxStatus, ProductStatus, ReviewReasonCode
from emoda.services.processor import ProcessorService
from emoda.services.publication import PublicationService


class RecordingPublisher:
    def __init__(self) -> None:
        self.calls = 0

    async def send(self, *args: object) -> list[int]:
        self.calls += 1
        return [701, 702, 703]

    async def update(self, *args: object) -> list[int]:
        self.calls += 1
        return [701, 702, 703]


def rendered(code: str) -> str:
    return (
        f"<b>Код / Code:</b> {code}\n"
        "<b>Цена за шт / Unit price:</b> Уточнить / Check\n"
        "<b>Размеры / Sizes:</b> Уточнить / Check\n"
        "<b>Шт в серии / Pcs per lot:</b> Уточнить / Check"
    )


@pytest.fixture
async def case(tmp_path: Path):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with maker() as session, session.begin():
            source = Source(name="source", telegram_channel_id=-100123)
            destination = Destination(name="destination", telegram_channel_id=-100456)
            session.add_all([source, destination])
            await session.flush()
            album = AlbumGroup(
                source_id=source.id,
                grouped_id=99,
                status=AlbumStatus.PROCESSED,
                last_seen_at=datetime.now(UTC),
                ready_at=datetime.now(UTC),
            )
            session.add(album)
            await session.flush()
            for index in range(2):
                message = SourceMessage(
                    source_id=source.id,
                    telegram_message_id=100 + index,
                    album_group_id=album.id,
                    telegram_date=datetime.now(UTC),
                )
                session.add(message)
                await session.flush()
                session.add(
                    MediaItem(
                        source_message_id=message.id,
                        telegram_message_id=message.telegram_message_id,
                        position=0,
                        media_type="photo",
                        storage_path=f"album/{index}.jpg",
                        active=True,
                    )
                )
            product = Product(
                source_id=source.id,
                album_group_id=album.id,
                destination_id=destination.id,
                emoda_code="AA00555",
                final_text=rendered("AA00555"),
                status=ProductStatus.NEEDS_REVIEW,
            )
            session.add(product)
            await session.flush()
            album.product_id = product.id
            product_id = product.id
            destination_id = destination.id
        (tmp_path / "album").mkdir()
        (tmp_path / "album/0.jpg").write_bytes(b"valid photo")
        transport = RecordingPublisher()
        processor = ProcessorService(
            maker,
            tmp_path,
            None,
            PublicationService(transport),
            bot_username="test_bot",
        )
        yield maker, processor, transport, product_id, destination_id, tmp_path
    finally:
        await engine.dispose()


@pytest.mark.parametrize("admin_review", [False, True])
async def test_missing_album_member_blocks_entire_publication(case, admin_review: bool) -> None:
    maker, processor, transport, product_id, _, media_root = case
    paths = [str(media_root / "album/0.jpg"), str(media_root / "album/1.jpg")]
    if admin_review:
        result = await processor.publish_reviewed(product_id)
        assert result.outcome == "failed"
        assert "media" in (result.detail or "").lower()
    else:
        async with maker() as session, session.begin():
            product = await session.get(Product, product_id)
            product.status = ProductStatus.READY_TO_PUBLISH
        assert await processor._enqueue_and_publish(product_id, paths) is None
    async with maker() as session:
        product = await session.get(Product, product_id)
        outboxes = list(await session.scalars(select(PublicationOutbox)))
    assert product is not None
    assert product.status is ProductStatus.NEEDS_REVIEW
    assert product.processing_error and "media" in product.processing_error.lower()
    assert any(item["code"] == ReviewReasonCode.MEDIA_DOWNLOAD_FAILED.value
               for item in product.review_reasons)
    assert outboxes == []
    assert transport.calls == 0


async def test_missing_edit_media_preserves_original_published_outbox(case) -> None:
    maker, processor, transport, product_id, destination_id, media_root = case
    paths = [str(media_root / "album/0.jpg"), str(media_root / "album/1.jpg")]
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        product.status = ProductStatus.READY_TO_PUBLISH
        product.published_at = datetime.now(UTC)
        session.add(
            PublicationOutbox(
                product_id=product_id,
                destination_id=destination_id,
                payload={"emoda_code": "AA00555", "text": rendered("AA00555"),
                         "media_paths": paths},
                payload_hash="original",
                keyboard_snapshot=[],
                status=OutboxStatus.PUBLISHED,
                telegram_message_ids=[600, 601, 602],
                published_at=datetime.now(UTC),
            )
        )
    assert await processor._enqueue_and_publish(product_id, paths) is None
    async with maker() as session:
        product = await session.get(Product, product_id)
        outboxes = list(await session.scalars(select(PublicationOutbox)))
    assert product is not None and product.status is ProductStatus.NEEDS_REVIEW
    assert len(outboxes) == 1 and outboxes[0].status is OutboxStatus.PUBLISHED
    assert transport.calls == 0


@pytest.mark.parametrize("resume", [False, True])
async def test_terminal_unavailable_album_member_cannot_be_omitted(case, resume: bool) -> None:
    maker, processor, transport, product_id, _, media_root = case
    async with maker() as session, session.begin():
        missing = await session.scalar(
            select(MediaItem).where(MediaItem.telegram_message_id == 101)
        )
        assert missing is not None
        missing.active = False
        message = await session.get(SourceMessage, missing.source_message_id)
        assert message is not None
        message.raw_metadata = {
            **(message.raw_metadata or {}),
            "media_download_error": "Telegram source message no longer accessible",
        }
        if resume:
            product = await session.get(Product, product_id)
            assert product is not None
            product.status = ProductStatus.READY_TO_PUBLISH

    if resume:
        result = await processor.publish_reviewed(product_id, resume=True)
        assert result.outcome == "failed"
    else:
        async with maker() as session, session.begin():
            product = await session.get(Product, product_id)
            assert product is not None
            product.status = ProductStatus.READY_TO_PUBLISH
        paths = [str(media_root / "album/0.jpg")]
        assert await processor._enqueue_and_publish(product_id, paths) is None

    async with maker() as session:
        product = await session.get(Product, product_id)
        outboxes = list(await session.scalars(select(PublicationOutbox)))
    assert product is not None and product.status is ProductStatus.NEEDS_REVIEW
    assert any(item["code"] == ReviewReasonCode.MEDIA_DOWNLOAD_FAILED.value
               for item in product.review_reasons)
    assert outboxes == []
    assert transport.calls == 0
