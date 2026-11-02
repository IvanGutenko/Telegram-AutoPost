from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import (
    AlbumGroup,
    Destination,
    MediaItem,
    Product,
    PublicationOutbox,
    Source,
    SourceMessage,
    SourceProfile,
)
from emoda.domain.enums import AlbumStatus, OutboxStatus, ProductStatus, PublicationMode
from emoda.domain.schemas import SourceProfileData
from emoda.services.processor import ProcessorService
from emoda.services.publication import PublicationService
from emoda.services.review import StaleReviewApproval, review_approval_token

RENDERED = (
    "<b>Код / Code:</b> AA00999\n"
    "<b>Цена за шт / Unit price:</b> $33\n"
    "<b>Размеры / Sizes:</b> S, M, L\n"
    "<b>Шт в серии / Pcs per lot:</b> 6"
)


class RecordingPublisher:
    def __init__(self) -> None:
        self.send_count = 0
        self.update_count = 0
        self.delete_count = 0
        self.deleted_message_ids: list[list[int]] = []
        self.payloads: list[dict[str, object]] = []
        self.keyboards: list[list[list[dict[str, str]]]] = []

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.send_count += 1
        self.payloads.append(payload)
        self.keyboards.append(keyboard)
        return [7001]

    async def update(
        self,
        destination_channel_id: int,
        message_ids: list[int],
        old_payload: dict[str, object],
        new_payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.update_count += 1
        self.payloads.append(new_payload)
        self.keyboards.append(keyboard)
        return message_ids

    async def find_by_code(
        self, destination_channel_id: int, emoda_code: str
    ) -> list[int] | None:
        return None

    async def delete(
        self, destination_channel_id: int, message_ids: list[int]
    ) -> None:
        self.delete_count += 1
        self.deleted_message_ids.append(message_ids)


class BlockingPublisher(RecordingPublisher):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.send_count += 1
        self.payloads.append(payload)
        self.keyboards.append(keyboard)
        self.started.set()
        await self.release.wait()
        return [7001]


class FailIfApprovalReparses:
    async def extract(self, **_kwargs: object) -> object:
        raise AssertionError("REVIEW approval must not invoke extraction")

    async def translate(self, *_args: object, **_kwargs: object) -> str:
        raise AssertionError("REVIEW approval must not invoke translation")


@pytest.fixture
async def review_db(
    tmp_path: Path,
) -> AsyncGenerator[
    tuple[async_sessionmaker[AsyncSession], Path, int, int], None
]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="generic review fixture", telegram_channel_id=911, enabled=True)
        source.profile = SourceProfile(
            config=SourceProfileData(
                translation_enabled=False,
                publication_mode=PublicationMode.REVIEW,
            ).model_dump(mode="json"),
            publication_mode=PublicationMode.REVIEW,
        )
        destination = Destination(name="Review destination", telegram_channel_id=-1911)
        session.add_all([source, destination])
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=991,
            text="incomplete raw source that must never be reparsed on approval",
            telegram_date=datetime.now(UTC),
        )
        session.add(message)
        await session.flush()
        (tmp_path / "review-photo.jpg").write_bytes(b"review fixture photo")
        session.add(
            MediaItem(
                source_message_id=message.id,
                telegram_message_id=message.telegram_message_id,
                position=0,
                storage_path="review-photo.jpg",
                active=True,
            )
        )
        product = Product(
            emoda_code="AA00999",
            source_id=source.id,
            source_message_id=message.id,
            destination_id=destination.id,
            status=ProductStatus.NEEDS_REVIEW,
            original_text=message.text,
            cleaned_text=message.text,
            final_text=RENDERED,
            parsed_result={
                "parsed": {
                    "fields": {
                        "price": {"status": "found", "value": "27.5"},
                        "sizes": {"status": "found", "value": "S, M, L"},
                        "quantity": {"status": "found", "value": "6"},
                    }
                },
                "price": {
                    "raw_price": "27.5",
                    "raw_currency": "USD",
                    "normalized_usd": "27.5",
                    "markup": "5",
                    "final_usd": "33",
                },
            },
            review_reasons=[{"code": "manual_rule", "detail": "fixture", "field": None}],
            raw_price=Decimal("27.5"),
            raw_currency="USD",
            normalized_usd=Decimal("27.5"),
            applied_markup=Decimal("5"),
            final_price=Decimal("33"),
            review_notified_at=datetime.now(UTC),
        )
        session.add(product)
        await session.flush()
        product_id, destination_id = product.id, destination.id
    yield maker, tmp_path, product_id, destination_id
    await engine.dispose()


async def test_review_approval_publishes_the_exact_persisted_product_once(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, product_id, _ = review_db
    transport = RecordingPublisher()
    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    async with maker() as session:
        before = await session.get(Product, product_id)
        assert before is not None
        token = review_approval_token(before)
        parsed_before = before.parsed_result

    first = await processor.publish_reviewed(product_id, expected_token=token)
    duplicate = await processor.publish_reviewed(product_id, expected_token=token)

    async with maker() as session:
        persisted = await session.get(Product, product_id)
        outboxes = list(
            await session.scalars(
                select(PublicationOutbox).where(PublicationOutbox.product_id == product_id)
            )
        )
    assert first.outcome == "published"
    assert duplicate.outcome == "already_published"
    assert persisted is not None and persisted.status is ProductStatus.PUBLISHED
    assert persisted.parsed_result == parsed_before
    assert persisted.raw_price == Decimal("27.5")
    assert persisted.final_price == Decimal("33")
    assert persisted.applied_markup == Decimal("5")
    assert persisted.final_text == RENDERED
    assert len(persisted.final_text.splitlines()) == 4
    assert len(outboxes) == 1 and outboxes[0].status is OutboxStatus.PUBLISHED
    assert outboxes[0].payload["text"] == RENDERED
    assert transport.send_count == 1
    assert transport.update_count == 0
    assert sum(len(row) for row in transport.keyboards[0]) == 3


async def test_review_approval_publishes_persisted_check_without_reparsing(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, product_id, _ = review_db
    expected = RENDERED.replace("$33", "Уточнить / Check")
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        assert product is not None
        product.final_text = expected
        product.parsed_result = {
            "parsed": {
                "fields": {
                    "price": {"status": "missing", "value": None},
                    "sizes": {"status": "found", "value": "S, M, L"},
                    "quantity": {"status": "found", "value": "6"},
                }
            },
            "price": None,
        }
        product.raw_price = None
        product.raw_currency = None
        product.normalized_usd = None
        product.applied_markup = None
        product.final_price = None
        token = review_approval_token(product)

    transport = RecordingPublisher()
    processor = ProcessorService(
        maker,
        media_root,
        FailIfApprovalReparses(),  # type: ignore[arg-type]
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    result = await processor.publish_reviewed(product_id, expected_token=token)

    assert result.outcome == "published"
    assert transport.send_count == 1
    assert transport.payloads[0]["text"] == expected
    assert len(str(transport.payloads[0]["text"]).splitlines()) == 4
    assert sum(len(row) for row in transport.keyboards[0]) == 3


async def test_auto_and_review_use_the_same_persisted_payload_and_keyboard(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, review_product_id, destination_id = review_db
    async with maker() as session, session.begin():
        review_product = await session.get(Product, review_product_id)
        assert review_product is not None
        auto_message = SourceMessage(
            source_id=review_product.source_id,
            telegram_message_id=992,
            telegram_date=datetime.now(UTC),
        )
        session.add(auto_message)
        await session.flush()
        session.add(
            MediaItem(
                source_message_id=auto_message.id,
                telegram_message_id=auto_message.telegram_message_id,
                position=0,
                storage_path="review-photo.jpg",
                active=True,
            )
        )
        auto_product = Product(
            emoda_code="AA00998",
            source_id=review_product.source_id,
            source_message_id=auto_message.id,
            destination_id=destination_id,
            status=ProductStatus.READY_TO_PUBLISH,
            original_text=review_product.original_text,
            cleaned_text=review_product.cleaned_text,
            final_text=RENDERED.replace("AA00999", "AA00998"),
            parsed_result=review_product.parsed_result,
            raw_price=review_product.raw_price,
            raw_currency=review_product.raw_currency,
            normalized_usd=review_product.normalized_usd,
            applied_markup=review_product.applied_markup,
            final_price=review_product.final_price,
        )
        session.add(auto_product)
        await session.flush()
        auto_product_id = auto_product.id
        review_token = review_approval_token(review_product)

    transport = RecordingPublisher()
    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    await processor._enqueue_and_publish(auto_product_id, [str(media_root / "review-photo.jpg")])
    review_result = await processor.publish_reviewed(
        review_product_id, expected_token=review_token
    )

    async with maker() as session:
        outboxes = list(
            await session.scalars(
                select(PublicationOutbox)
                .where(PublicationOutbox.product_id.in_([auto_product_id, review_product_id]))
                .order_by(PublicationOutbox.id)
            )
        )
    assert review_result.outcome == "published"
    assert transport.send_count == 2
    assert len(outboxes) == 2
    auto_payload = dict(outboxes[0].payload)
    review_payload = dict(outboxes[1].payload)
    auto_payload["emoda_code"] = "SAME-CODE"
    review_payload["emoda_code"] = "SAME-CODE"
    auto_payload["text"] = str(auto_payload["text"]).replace("AA00998", "SAME-CODE")
    review_payload["text"] = str(review_payload["text"]).replace(
        "AA00999", "SAME-CODE"
    )
    assert auto_payload == review_payload
    assert outboxes[0].keyboard_snapshot[1] == outboxes[1].keyboard_snapshot[1]
    for index in (0, 2):
        auto_button = outboxes[0].keyboard_snapshot[index][0]
        review_button = outboxes[1].keyboard_snapshot[index][0]
        assert "AA00998" in auto_button["url"] and "AA00999" in review_button["url"]
        assert auto_button["text"] == review_button["text"]
        assert auto_button["url"].replace("AA00998", "SAME-CODE") == review_button["url"].replace("AA00999", "SAME-CODE")
    assert sum(len(row) for row in outboxes[0].keyboard_snapshot) == 3


async def test_stale_review_snapshot_cannot_publish_a_changed_product(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, product_id, _ = review_db
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        assert product is not None
        stale_token = review_approval_token(product)
        product.final_text = product.final_text.replace("$33", "$34")
        product.final_price = Decimal("34")

    transport = RecordingPublisher()
    processor = ProcessorService(
        maker, media_root, None, PublicationService(transport), bot_username="emoda_autopost_bot"
    )
    with pytest.raises(StaleReviewApproval):
        await processor.publish_reviewed(product_id, expected_token=stale_token)

    async with maker() as session:
        product = await session.get(Product, product_id)
        outbox_count = await session.scalar(
            select(func.count()).select_from(PublicationOutbox).where(
                PublicationOutbox.product_id == product_id
            )
        )
    assert product is not None and product.status is ProductStatus.NEEDS_REVIEW
    assert outbox_count == 0
    assert transport.send_count == 0


async def test_two_simultaneous_review_approvals_claim_once_and_send_once(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, product_id, _ = review_db
    async with maker() as session:
        product = await session.get(Product, product_id)
        assert product is not None
        token = review_approval_token(product)
    transport = BlockingPublisher()
    processor = ProcessorService(
        maker, media_root, None, PublicationService(transport), bot_username="emoda_autopost_bot"
    )

    first_task = asyncio.create_task(
        processor.publish_reviewed(product_id, expected_token=token)
    )
    await asyncio.wait_for(transport.started.wait(), timeout=2)
    duplicate = await processor.publish_reviewed(product_id, expected_token=token)
    transport.release.set()
    first = await asyncio.wait_for(first_task, timeout=2)

    assert first.outcome == "published"
    assert duplicate.outcome == "in_progress"
    assert transport.send_count == 1
    async with maker() as session:
        outbox_count = await session.scalar(
            select(func.count()).select_from(PublicationOutbox).where(
                PublicationOutbox.product_id == product_id
            )
        )
        product = await session.get(Product, product_id)
    assert outbox_count == 1
    assert product is not None and product.status is ProductStatus.PUBLISHED


async def test_review_approval_replaces_partial_album_with_complete_product(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    """Regression for SOBE AA00236: REVIEW $33, old AUTO post still said Check."""
    maker, media_root, product_id, destination_id = review_db
    old_storage = [f"sobe/old-{index}.jpg" for index in range(6)]
    current_storage = [*old_storage, "sobe/late-7.jpg", "sobe/late-8.jpg"]
    old_paths = [str(media_root / path) for path in old_storage]
    current_paths = [str(media_root / path) for path in current_storage]
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        assert product is not None
        product.source_message_id = None
        group = AlbumGroup(
            source_id=product.source_id,
            grouped_id=14304671342428916,
            status=AlbumStatus.PROCESSED,
            last_seen_at=datetime.now(UTC),
            ready_at=datetime.now(UTC),
            product_id=product.id,
        )
        session.add(group)
        await session.flush()
        product.album_group_id = group.id
        for index, path in enumerate(current_storage):
            message = SourceMessage(
                source_id=product.source_id,
                telegram_message_id=130576 + index,
                grouped_id=group.grouped_id,
                album_group_id=group.id,
                text="27.5 $" if index == 0 else "",
                telegram_date=datetime.now(UTC),
            )
            session.add(message)
            await session.flush()
            session.add(
                MediaItem(
                    source_message_id=message.id,
                    telegram_message_id=message.telegram_message_id,
                    position=0,
                    storage_path=path,
                    active=True,
                )
            )
        product.published_at = datetime.now(UTC)
        original = PublicationOutbox(
            product_id=product.id,
            destination_id=destination_id,
            payload={
                "emoda_code": product.emoda_code,
                "text": RENDERED.replace("$33", "Уточнить / Check"),
                "media_paths": old_paths,
                "expected_message_count": 7,
            },
            payload_hash="sobe-original-check",
            keyboard_snapshot=[],
            status=OutboxStatus.PUBLISHED,
            telegram_message_ids=list(range(8001, 8008)),
            published_at=product.published_at,
        )
        session.add(original)
        await session.flush()
        token = review_approval_token(product)

    for path in current_paths:
        target = Path(path)
        await asyncio.to_thread(target.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(target.write_bytes, b"album fixture photo")

    transport = RecordingPublisher()
    result = await ProcessorService(
        maker, media_root, None, PublicationService(transport), bot_username="emoda_autopost_bot"
    ).publish_reviewed(product_id, expected_token=token)

    async with maker() as session:
        product = await session.get(Product, product_id)
        outboxes = list(
            await session.scalars(
                select(PublicationOutbox)
                .where(PublicationOutbox.product_id == product_id)
                .order_by(PublicationOutbox.id)
            )
        )
    approved = outboxes[-1]
    assert result.outcome == "published"
    assert product is not None and product.status is ProductStatus.PUBLISHED
    assert approved.payload["text"] == RENDERED
    assert approved.payload["media_paths"] == current_paths
    assert approved.payload["operation"] == "replace"
    assert approved.payload["replaces_outbox_id"] == outboxes[0].id
    assert approved.status is OutboxStatus.PUBLISHED
    assert transport.send_count == 1
    assert transport.update_count == 0
    assert transport.delete_count == 1
    assert transport.deleted_message_ids == [list(range(8001, 8008))]

    # A restart/recovery sees a completed product and never sends or edits it again.
    await ProcessorService(
        maker, media_root, None, PublicationService(transport), bot_username="emoda_autopost_bot"
    ).recover()
    assert transport.send_count == 1
    assert transport.update_count == 0
    assert transport.delete_count == 1


async def test_review_approval_after_skip_is_stale_and_does_not_send(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, product_id, _ = review_db
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        assert product is not None
        old_token = review_approval_token(product)
        product.status = ProductStatus.SKIPPED

    transport = RecordingPublisher()
    processor = ProcessorService(
        maker, media_root, None, PublicationService(transport), bot_username="emoda_autopost_bot"
    )
    with pytest.raises(StaleReviewApproval):
        await processor.publish_reviewed(product_id, expected_token=old_token)

    async with maker() as session:
        product = await session.get(Product, product_id)
        outbox_count = await session.scalar(
            select(func.count()).select_from(PublicationOutbox).where(
                PublicationOutbox.product_id == product_id
            )
        )
    assert product is not None and product.status is ProductStatus.SKIPPED
    assert outbox_count == 0
    assert transport.send_count == 0


async def test_review_approval_blocks_source_with_unavailable_media(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, product_id, _ = review_db
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        assert product is not None and product.source_message_id is not None
        token = review_approval_token(product)
        message = await session.get(SourceMessage, product.source_message_id)
        assert message is not None
        message.raw_metadata = {
            "media_download_error": "source media download failed: Telegram message 991 is no longer accessible",
            "media_recovery_terminal_at": datetime.now(UTC).isoformat(),
        }

    transport = RecordingPublisher()
    result = await ProcessorService(
        maker, media_root, None, PublicationService(transport), bot_username="emoda_autopost_bot"
    ).publish_reviewed(product_id, expected_token=token)

    async with maker() as session:
        product = await session.get(Product, product_id)
        outbox_count = await session.scalar(
            select(func.count()).select_from(PublicationOutbox).where(
                PublicationOutbox.product_id == product_id
            )
        )
    assert result.outcome == "failed"
    assert result.detail is not None and "медиа" in result.detail.lower()
    assert product is not None and product.status is ProductStatus.NEEDS_REVIEW
    assert product.review_reasons[0] == {
        "code": "manual_rule", "detail": "fixture", "field": None
    }
    assert product.review_reasons[-1]["code"] == "media_download_failed"
    assert outbox_count == 0
    assert transport.send_count == 0


async def test_review_approval_blocks_partial_album_with_unavailable_member(
    review_db: tuple[async_sessionmaker[AsyncSession], Path, int, int],
) -> None:
    maker, media_root, product_id, _ = review_db
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        assert product is not None and product.source_message_id is not None
        first_message = await session.get(SourceMessage, product.source_message_id)
        assert first_message is not None
        group = AlbumGroup(
            source_id=product.source_id,
            grouped_id=123456789,
            status=AlbumStatus.PROCESSED,
            last_seen_at=datetime.now(UTC),
            ready_at=datetime.now(UTC),
            product_id=product.id,
        )
        session.add(group)
        await session.flush()
        first_message.album_group_id = group.id
        missing_message = SourceMessage(
            source_id=product.source_id,
            telegram_message_id=992,
            album_group_id=group.id,
            text="",
            telegram_date=datetime.now(UTC),
            raw_metadata={
                "media_download_error": "source media download failed: Telegram message 992 is no longer accessible",
                "media_recovery_terminal_at": datetime.now(UTC).isoformat(),
            },
        )
        session.add(missing_message)
        product.source_message_id = None
        product.album_group_id = group.id
        token = review_approval_token(product)

    transport = RecordingPublisher()
    result = await ProcessorService(
        maker, media_root, None, PublicationService(transport), bot_username="emoda_autopost_bot"
    ).publish_reviewed(product_id, expected_token=token)

    async with maker() as session:
        product = await session.get(Product, product_id)
        outbox_count = await session.scalar(
            select(func.count()).select_from(PublicationOutbox).where(
                PublicationOutbox.product_id == product_id
            )
        )
    assert result.outcome == "failed"
    assert result.detail is not None and "992" in result.detail
    assert product is not None and product.status is ProductStatus.NEEDS_REVIEW
    assert outbox_count == 0
    assert transport.send_count == 0
