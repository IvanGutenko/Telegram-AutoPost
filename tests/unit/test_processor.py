from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.admin.service import AdminService
from emoda.db.base import Base
from emoda.db.models import (
    AlbumGroup,
    Destination,
    GlobalSetting,
    MarkupRule,
    MediaItem,
    Product,
    PublicationOutbox,
    Source,
    SourceMessage,
    SourceProfile,
)
from emoda.domain.enums import (
    AlbumStatus,
    OutboxStatus,
    ProductStatus,
    PublicationMode,
)
from emoda.domain.schemas import (
    ExtractionRule,
    SourceIgnoreRule,
    SourceProfileData,
)
from emoda.services.processor import ProcessorService
from emoda.services.publication import (
    PublicationService,
    SafePublicationFailure,
    UncertainPublicationFailure,
)
from emoda.services.review import ReviewPublishResult


def rendered(code: str) -> str:
    return (
        f"<b>Код / Code:</b> {code}\n"
        "<b>Цена за шт / Unit price:</b> Уточнить / Check\n"
        "<b>Размеры / Sizes:</b> Уточнить / Check\n"
        "<b>Шт в серии / Pcs per lot:</b> Уточнить / Check"
    )


class RecordingPublisher:
    def __init__(self) -> None:
        self.send_count = 0
        self.update_count = 0
        self.keyboards: list[list[list[dict[str, str]]]] = []

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.send_count += 1
        self.keyboards.append(keyboard)
        return [701]

    async def find_by_code(self, destination_channel_id: int, emoda_code: str) -> list[int] | None:
        return None

    async def update(
        self,
        destination_channel_id: int,
        message_ids: list[int],
        old_payload: dict[str, object],
        new_payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.update_count += 1
        return message_ids


class RejectedOncePublisher:
    """Transport whose first edit attempt is rejected before Telegram accepts anything."""

    def __init__(self) -> None:
        self.send_count = 0
        self.update_count = 0

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.send_count += 1
        return [701]

    async def find_by_code(self, destination_channel_id: int, emoda_code: str) -> list[int] | None:
        return None

    async def update(
        self,
        destination_channel_id: int,
        message_ids: list[int],
        old_payload: dict[str, object],
        new_payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.update_count += 1
        if self.update_count == 1:
            raise SafePublicationFailure(
                "media content changed; Telegram media update requires manual review"
            )
        return message_ids


class WedgedRecoveryProcessor(ProcessorService):
    """Processor double that wedges startup recovery for exactly one product."""

    wedged_product_id: int | None = None
    failure: Exception | None = None

    async def publish_reviewed(
        self,
        product_id: int,
        expected_token: str | None = None,
        *,
        resume: bool = False,
    ) -> ReviewPublishResult:
        if product_id == self.wedged_product_id and self.failure is not None:
            raise self.failure
        return await super().publish_reviewed(
            product_id,
            expected_token=expected_token,
            resume=resume,
        )


@pytest.fixture
async def database() -> tuple[async_sessionmaker[AsyncSession], Path]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    yield maker, Path("/tmp/emoda-test-media")
    await engine.dispose()


async def add_source(session: AsyncSession) -> Source:
    source = Source(name="supplier", telegram_channel_id=123, enabled=True)
    source.profile = SourceProfile(
        config=SourceProfileData(
            translation_enabled=False,
            publication_mode=PublicationMode.REVIEW,
        ).model_dump(mode="json"),
        publication_mode=PublicationMode.REVIEW,
    )
    session.add(source)
    await session.flush()
    return source


async def test_worker_claims_album_when_debounce_expires_during_runtime(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
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
        session.add_all(
            [
                SourceMessage(
                    source_id=source.id,
                    telegram_message_id=10 + index,
                    grouped_id=77,
                    album_group_id=group.id,
                    text="Price: 10 USD\nSizes: M\n1 pcs" if index == 0 else "",
                    telegram_date=now,
                )
                for index in range(2)
            ]
        )
    work = await ProcessorService(maker, media_root, None, None)._claim_work()
    assert work is not None
    async with maker() as session:
        products = list(await session.scalars(Product.__table__.select()))
        assert len(products) == 1


async def test_new_products_receive_alpha2_numeric5_codes_in_sequence(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        session.add(GlobalSetting(key="emoda_code", value={"format": "alpha2_numeric5"}))
        session.add_all(
            [
                SourceMessage(
                    source_id=source.id,
                    telegram_message_id=message_id,
                    text=f"Article: {message_id}\nPrice: 10 USD\nSizes: M\n1 pcs",
                    telegram_date=datetime.now(UTC),
                )
                for message_id in (100, 101)
            ]
        )

    processor = ProcessorService(maker, media_root, None, None)
    assert await processor._claim_work() is not None
    assert await processor._claim_work() is not None

    async with maker() as session:
        codes = list(await session.scalars(select(Product.emoda_code).order_by(Product.id)))
    assert codes == ["AA00000", "AA00001"]


async def test_exact_duplicate_updates_existing_product_without_allocating_code(database):
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        for message_id in (110, 111):
            session.add(SourceMessage(source_id=source.id, telegram_message_id=message_id,
                text="Article: A1\nPrice: 20 USD\nSizes: S M\n2 pcs", telegram_date=datetime.now(UTC)))
    processor = ProcessorService(maker, media_root, None, None)
    assert await processor.process_next()
    await processor.process_next()
    async with maker() as session:
        products = list(await session.scalars(select(Product)))
        assert len(products) == 1
        assert products[0].parsed_result["duplicate_count"] == 1
        messages = list(await session.scalars(select(SourceMessage).order_by(SourceMessage.id)))
        assert messages[1].raw_metadata["duplicate_product_id"] == products[0].id


async def test_ready_album_product_waits_until_reopened_debounce_expires(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        now = datetime.now(UTC)
        group = AlbumGroup(
            source_id=source.id,
            grouped_id=78,
            status=AlbumStatus.COLLECTING,
            last_seen_at=now,
            ready_at=now + timedelta(minutes=1),
        )
        session.add(group)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=20,
            grouped_id=78,
            album_group_id=group.id,
            text="Price: 10 USD\nSizes: M\n1 pcs",
            telegram_date=now,
        )
        session.add(message)
        product = Product(
            emoda_code="EM-LATE-2",
            source_id=source.id,
            album_group_id=group.id,
            status=ProductStatus.READY,
        )
        session.add(product)
        await session.flush()
        group.product_id = product.id
        product_id = product.id

    assert await ProcessorService(maker, media_root, None, None)._claim_work() is None
    async with maker() as session:
        persisted = await session.get(Product, product_id)
        assert persisted is not None and persisted.status is ProductStatus.READY


async def test_attached_ungrouped_companion_cannot_be_claimed_before_album_deadline(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        now = datetime.now(UTC)
        group = AlbumGroup(
            source_id=source.id,
            grouped_id=901,
            status=AlbumStatus.COLLECTING,
            last_seen_at=now,
            ready_at=now + timedelta(minutes=1),
        )
        session.add(group)
        await session.flush()
        session.add(
            SourceMessage(
                source_id=source.id,
                telegram_message_id=901,
                grouped_id=None,
                album_group_id=group.id,
                text="Price: 10 USD",
                telegram_date=now,
            )
        )

    assert await ProcessorService(maker, media_root, None, None)._claim_work() is None


async def test_standalone_message_waits_for_persistent_processing_deadline(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=30,
            text="Delayed standalone media",
            telegram_date=datetime.now(UTC),
            processing_after=datetime.now(UTC) + timedelta(minutes=1),
        )
        session.add(message)
        await session.flush()
        message_id = message.id

    processor = ProcessorService(maker, media_root, None, None)
    assert await processor._claim_work() is None
    async with maker() as session, session.begin():
        message = await session.get(SourceMessage, message_id)
        assert message is not None
        message.processing_after = datetime.now(UTC) - timedelta(seconds=1)
    assert await processor._claim_work() is not None


async def test_article_only_media_is_skipped_only_after_deadline_and_not_reprocessed(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        profile = SourceProfileData(
            publication_mode=PublicationMode.AUTO,
            translation_enabled=False,
            extraction_rules={
                "article": [
                    ExtractionRule(
                        kind="regex",
                        pattern=r"(?m)^\s*(?P<value>[A-Z]\d+)\s*$",
                        authoritative=True,
                    )
                ]
            },
            ignore_rules=[
                SourceIgnoreRule(
                    name="article-only-media",
                    found_fields=["article"],
                    missing_fields=["price", "sizes", "quantity"],
                )
            ],
        )
        source.profile.config = profile.model_dump(mode="json")
        source.profile.publication_mode = PublicationMode.AUTO
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=31,
            text="E7193",
            telegram_date=datetime.now(UTC),
            processing_after=datetime.now(UTC) + timedelta(minutes=1),
        )
        session.add(message)
        await session.flush()
        session.add(
            MediaItem(
                source_message_id=message.id,
                telegram_message_id=31,
                position=0,
                media_type="video",
                storage_path="71/31/video.mp4",
            )
        )
        message_id = message.id

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
    )
    assert await processor.process_next() is False
    async with maker() as session:
        assert list(await session.scalars(select(Product))) == []

    async with maker() as session, session.begin():
        message = await session.get(SourceMessage, message_id)
        assert message is not None
        message.processing_after = datetime.now(UTC) - timedelta(seconds=1)

    assert await processor.process_next() is True
    assert await processor.process_next() is False
    async with maker() as session:
        products = list(await session.scalars(select(Product)))
        outboxes = list(await session.scalars(select(PublicationOutbox)))
        message = await session.get(SourceMessage, message_id)
    assert len(products) == 1
    assert products[0].status is ProductStatus.SKIPPED
    assert products[0].parsed_result["ignored"] is True
    assert products[0].parsed_result["ignore_reason"] == "article-only-media"
    assert products[0].final_text == ""
    assert message is not None and message.processed_at is not None
    assert message.raw_metadata["outcome"] == "skipped"
    assert message.raw_metadata["outcome_reason"] == "article-only-media"
    assert outboxes == []
    assert transport.send_count == 0


async def test_auto_source_publishes_when_quantity_is_not_required(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="Romaşka", telegram_channel_id=-1091)
        session.add(destination)
        await session.flush()
        profile = SourceProfileData(
            publication_mode=PublicationMode.AUTO,
            translation_enabled=False,
            default_destination_id=destination.id,
            required_fields=["price", "sizes"],
        )
        source.profile.config = profile.model_dump(mode="json")
        source.profile.publication_mode = PublicationMode.AUTO
        session.add(
            MarkupRule(
                min_price=Decimal("0"),
                max_price=Decimal("100.99"),
                markup=Decimal("3"),
                priority=1,
            )
        )
        session.add(
            SourceMessage(
                source_id=source.id,
                telegram_message_id=91,
                text="Price: 10 USD\nSizes: S M\nFloral dress",
                telegram_date=datetime.now(UTC),
            )
        )

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )

    assert await processor.process_next() is True
    async with maker() as session:
        product = await session.scalar(select(Product))
    assert product is not None and product.status is ProductStatus.PUBLISHED
    assert product.parsed_result["parsed"]["fields"]["quantity"]["status"] == "missing"
    assert (
        "<b>Шт в серии / Pcs per lot:</b> Уточнить / Check"
        in product.final_text
    )
    assert transport.send_count == 1


async def test_auto_source_publishes_when_price_sizes_quantity_all_missing(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    """The global invariant: missing price/sizes/quantity never route to REVIEW."""
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="Blind", telegram_channel_id=-1092)
        session.add(destination)
        await session.flush()
        profile = SourceProfileData(
            publication_mode=PublicationMode.AUTO,
            translation_enabled=False,
            default_destination_id=destination.id,
        )
        source.profile.config = profile.model_dump(mode="json")
        source.profile.publication_mode = PublicationMode.AUTO
        session.add(
            SourceMessage(
                source_id=source.id,
                telegram_message_id=92,
                text="Spor Araboy Kanguru Cepli Tunik Modelimiz Stoklarda",
                telegram_date=datetime.now(UTC),
            )
        )

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )

    assert await processor.process_next() is True
    async with maker() as session:
        product = await session.scalar(select(Product))
    assert product is not None and product.status is ProductStatus.PUBLISHED
    assert product.review_reasons == []
    assert product.final_text.count("Уточнить / Check") == 3
    assert "Уточняйте у менеджера" not in product.final_text
    assert "Ask the manager" not in product.final_text
    assert len(product.final_text.splitlines()) == 4
    assert transport.send_count == 1


async def test_review_product_persists_explicit_source_mode_reason(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        session.add(
            SourceMessage(
                source_id=source.id,
                telegram_message_id=92,
                text="Price: 10 USD\nSizes: S M\n2 pcs",
                telegram_date=datetime.now(UTC),
            )
        )
        session.add(
            MarkupRule(
                min_price=Decimal("0"),
                max_price=Decimal("100.99"),
                markup=Decimal("3"),
                priority=1,
            )
        )
    assert await ProcessorService(maker, media_root, None, None).process_next() is True
    async with maker() as session:
        product = await session.scalar(select(Product))
    assert product is not None
    assert product.status is ProductStatus.NEEDS_REVIEW
    assert product.review_reasons == [
        {
            "code": "source_publication_mode_review",
            "detail": "source is configured for manual approval",
            "field": None,
        }
    ]


async def test_restart_immediately_requeues_processing_product(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=1,
            text="Price: 10 USD\nSizes: M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-000001",
            source_id=source.id,
            source_message_id=message.id,
            status=ProductStatus.PROCESSING,
        )
        session.add(product)
        await session.flush()
        product_id = product.id
    await ProcessorService(maker, media_root, None, None).recover()
    async with maker() as session:
        recovered = await session.get(Product, product_id)
        assert recovered is not None
        assert recovered.status is ProductStatus.READY


@pytest.mark.parametrize(
    "interrupted_status", [ProductStatus.PROCESSING, ProductStatus.READY]
)
async def test_restart_reopens_processed_album_for_interrupted_product(
    database: tuple[async_sessionmaker[AsyncSession], Path],
    interrupted_status: ProductStatus,
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        now = datetime.now(UTC)
        group = AlbumGroup(
            source_id=source.id,
            grouped_id=8765,
            status=AlbumStatus.PROCESSED,
            last_seen_at=now - timedelta(seconds=3),
            ready_at=now - timedelta(seconds=1),
        )
        session.add(group)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=8765,
            grouped_id=group.grouped_id,
            album_group_id=group.id,
            text="Price: 10 USD\nSizes: M\n1 pcs",
            telegram_date=now,
        )
        session.add(message)
        product = Product(
            emoda_code="EM-008765",
            source_id=source.id,
            album_group_id=group.id,
            status=interrupted_status,
        )
        session.add(product)
        await session.flush()
        group.product_id = product.id
        product_id = product.id
        group_id = group.id

    processor = ProcessorService(maker, media_root, None, None)
    await processor.recover_jobs()

    async with maker() as session:
        recovered = await session.get(Product, product_id)
        recovered_group = await session.get(AlbumGroup, group_id)
        assert recovered is not None and recovered.status is ProductStatus.READY
        assert recovered_group is not None and recovered_group.status is AlbumStatus.READY
    assert await processor.process_next() is True
    async with maker() as session:
        completed = await session.get(Product, product_id)
        assert completed is not None and completed.status is ProductStatus.NEEDS_REVIEW


async def test_publication_reconciliation_does_not_finalize_album_before_catchup(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as session, session.begin():
        source = await add_source(session)
        group = AlbumGroup(
            source_id=source.id,
            grouped_id=1234,
            status=AlbumStatus.COLLECTING,
            last_seen_at=datetime.now(UTC) - timedelta(minutes=2),
            ready_at=datetime.now(UTC) - timedelta(minutes=1),
        )
        session.add(group)
        await session.flush()
        group_id = group.id

    processor = ProcessorService(maker, media_root, None, None)
    await processor.reconcile_publications()

    async with maker() as session:
        persisted = await session.get(AlbumGroup, group_id)
    assert persisted is not None and persisted.status is AlbumStatus.COLLECTING


async def test_restart_publishes_durable_ready_to_publish_product(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA", telegram_channel_id=-1001)
        session.add(destination)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=1,
            text="Price: 10 USD\nSizes: M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-000002",
            source_id=source.id,
            source_message_id=message.id,
            destination_id=destination.id,
            final_text=rendered("EM-000002"),
            status=ProductStatus.READY_TO_PUBLISH,
        )
        session.add(product)
        await session.flush()
        product_id = product.id
    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    await processor.recover()
    await processor.publish_reviewed(product_id)
    async with maker() as session:
        recovered = await session.get(Product, product_id)
        message = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 1)
        )
        assert recovered is not None
        assert recovered.status is ProductStatus.PUBLISHED
        assert message is not None and message.raw_metadata["outcome"] == "published"
    assert transport.send_count == 1
    assert transport.update_count == 0


async def test_publication_uses_global_three_button_keyboard(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA buttons", telegram_channel_id=-1002)
        session.add(destination)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=2,
            text="Price: 10 USD\nSizes: M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-BUTTON-1",
            source_id=source.id,
            source_message_id=message.id,
            destination_id=destination.id,
            final_text=rendered("EM-BUTTON-1"),
            status=ProductStatus.NEEDS_REVIEW,
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    await ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    ).publish_reviewed(product_id)
    assert len(transport.keyboards) == 1
    snapshot = transport.keyboards[0]
    assert len(snapshot) == 3
    assert snapshot[0][0]["text"] == "🛒 Заказать / Order"
    assert urlsplit(snapshot[0][0]["url"]).path == "/905552942994"
    assert parse_qs(urlsplit(snapshot[0][0]["url"]).query)["text"] == [
        "Здравствуйте! Хочу заказать товар E-MODA EM-BUTTON-1.\n"
        "Hello! I would like to order E-MODA EM-BUTTON-1."
    ]
    assert snapshot[1] == [{
        "text": "📂 Каталог / Catalog",
        "url": "https://t.me/emoda_autopost_bot?start=catalog",
    }]
    async with maker() as session:
        outbox = await session.scalar(
            select(PublicationOutbox).where(PublicationOutbox.product_id == product_id)
        )
        assert outbox is not None and outbox.status is OutboxStatus.PUBLISHED


async def test_publication_buttons_setting_overrides_urls_and_texts(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA catalog", telegram_channel_id=-1003)
        session.add(destination)
        await session.flush()
        await AdminService(session).set_setting(
            "publication_buttons",
            {
                "order": {"text": "🛒 Заказать / Order", "url": "https://example.com/order"},
                "catalog": {
                    "text": "📂 Каталог / Catalog",
                    "url": "https://example.com/catalog",
                },
                "source_17": [{"text": "Third", "url": "https://example.com/third"}],
            },
            actor_id=None,
        )
        product = Product(
            emoda_code="EM-CATALOG-1",
            source_id=source.id,
            destination_id=destination.id,
            final_text=rendered("EM-CATALOG-1"),
            status=ProductStatus.NEEDS_REVIEW,
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    await ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    ).publish_reviewed(product_id)

    assert [rows[:2] for rows in transport.keyboards] == [
        [
            [{"text": "🛒 Заказать / Order", "url": "https://example.com/order"}],
            [{"text": "📂 Каталог / Catalog", "url": "https://example.com/catalog"}],
        ]
    ]
    assert sum(len(row) for row in transport.keyboards[0]) == 3


async def test_processing_result_is_superseded_by_concurrent_supplier_edit(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    async with maker() as setup_session, setup_session.begin():
        source = await add_source(setup_session)
        product = Product(
            emoda_code="EM-000003",
            source_id=source.id,
            status=ProductStatus.PROCESSING,
        )
        setup_session.add(product)
        await setup_session.flush()
        product_id = product.id
    async with maker() as processing_session:
        stale = await processing_session.get(Product, product_id)
        assert stale is not None
        await processing_session.commit()
        async with maker() as edit_session, edit_session.begin():
            edited = await edit_session.get(Product, product_id)
            assert edited is not None
            edited.status = ProductStatus.READY
            edited.processing_error = "supplier edit requires reprocess"
        processor = ProcessorService(maker, media_root, None, None)
        assert await processor._processing_was_superseded(processing_session, stale) is True
        assert stale.status is ProductStatus.READY


async def test_published_product_is_updated_in_place_after_supplier_edit(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA edit", telegram_channel_id=-1093)
        session.add(destination)
        await session.flush()
        profile = SourceProfileData(
            publication_mode=PublicationMode.AUTO,
            translation_enabled=False,
            default_destination_id=destination.id,
        )
        source.profile.config = profile.model_dump(mode="json")
        source.profile.publication_mode = PublicationMode.AUTO
        session.add(
            MarkupRule(
                min_price=Decimal("0"),
                max_price=Decimal("100.99"),
                markup=Decimal("3"),
                priority=1,
            )
        )
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=71,
            text="Sizes: S M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-EDIT-71",
            source_id=source.id,
            source_message_id=message.id,
            destination_id=destination.id,
            final_text="stale render",
            raw_price=Decimal("10"),
            raw_currency="USD",
            normalized_usd=Decimal("10"),
            applied_markup=Decimal("3"),
            final_price=Decimal("13"),
            status=ProductStatus.READY,
            published_at=datetime.now(UTC),
        )
        session.add(product)
        await session.flush()
        session.add(
            PublicationOutbox(
                product_id=product.id,
                destination_id=destination.id,
                payload={
                    "emoda_code": "EM-EDIT-71",
                    "text": "stale render",
                    "media_paths": [],
                },
                payload_hash="original-payload-hash",
                keyboard_snapshot=[],
                status=OutboxStatus.PUBLISHED,
                telegram_message_ids=[701],
                published_at=datetime.now(UTC),
            )
        )
        product_id = product.id

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    assert await processor.process_next() is True

    async with maker() as session:
        products = list(await session.scalars(select(Product)))
        outboxes = list(
            await session.scalars(select(PublicationOutbox).order_by(PublicationOutbox.id))
        )
        persisted = await session.get(Product, product_id)
    assert len(products) == 1
    assert persisted is not None and persisted.emoda_code == "EM-EDIT-71"
    assert persisted is not None and persisted.status is ProductStatus.PUBLISHED
    assert persisted is not None and persisted.final_price is None
    assert persisted is not None and "Уточнить / Check" in persisted.final_text
    edit_outboxes = [row for row in outboxes if row.payload.get("operation") == "edit"]
    assert len(outboxes) == 2
    assert len(edit_outboxes) == 1
    assert edit_outboxes[0].status is OutboxStatus.PUBLISHED
    assert transport.update_count == 1
    assert transport.send_count == 0


async def test_non_renderer_text_never_reaches_publication_outbox(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA guarded", telegram_channel_id=-1094)
        session.add(destination)
        await session.flush()
        product = Product(
            emoda_code="AA00001",
            source_id=source.id,
            destination_id=destination.id,
            final_text="17 долларов",
            status=ProductStatus.READY_TO_PUBLISH,
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )

    outbox = await processor._enqueue_and_publish(product_id, [])

    async with maker() as session:
        persisted = await session.get(Product, product_id)
        outboxes = list(await session.scalars(select(PublicationOutbox)))
    assert outbox is None
    assert persisted is not None and persisted.status is ProductStatus.NEEDS_REVIEW
    assert persisted is not None and "exactly four lines" in (persisted.processing_error or "")
    assert outboxes == []
    assert transport.send_count == 0


async def test_unchanged_reprocess_leaves_published_post_untouched(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA noop", telegram_channel_id=-1094)
        session.add(destination)
        await session.flush()
        profile = SourceProfileData(
            publication_mode=PublicationMode.AUTO,
            translation_enabled=False,
            default_destination_id=destination.id,
        )
        source.profile.config = profile.model_dump(mode="json")
        source.profile.publication_mode = PublicationMode.AUTO
        session.add(
            MarkupRule(
                min_price=Decimal("0"),
                max_price=Decimal("100.99"),
                markup=Decimal("3"),
                priority=1,
            )
        )
        session.add(
            SourceMessage(
                source_id=source.id,
                telegram_message_id=72,
                text="Price: 10 USD\nSizes: S M\n1 pcs",
                telegram_date=datetime.now(UTC),
            )
        )

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    assert await processor.process_next() is True
    async with maker() as session:
        product = await session.scalar(select(Product))
        product_id = product.id if product else None
        assert product is not None and product.status is ProductStatus.PUBLISHED

    # A no-op supplier edit re-enqueues the identical durable content.
    assert product_id is not None
    async with maker() as session, session.begin():
        persisted = await session.get(Product, product_id)
        assert persisted is not None
        persisted.status = ProductStatus.READY
        persisted.processing_error = None
    assert await processor.process_next() is True

    async with maker() as session:
        product = await session.get(Product, product_id)
        outboxes = list(await session.scalars(select(PublicationOutbox)))
        message = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 72)
        )
    assert product is not None and product.status is ProductStatus.PUBLISHED
    assert len(outboxes) == 1
    assert transport.send_count == 1
    assert transport.update_count == 0
    assert message is not None
    assert message.raw_metadata["outcome"] == "published"
    assert message.raw_metadata["outcome_reason"] == "supplier edit did not change the publication"


async def test_review_mode_source_routes_reprocessed_edit_to_admin(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        session.add(
            MarkupRule(
                min_price=Decimal("0"),
                max_price=Decimal("100.99"),
                markup=Decimal("3"),
                priority=1,
            )
        )
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=73,
            text="Price: 10 USD\nSizes: S M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add(message)
        await session.flush()
        session.add(
            Product(
                emoda_code="EM-EDIT-73",
                source_id=source.id,
                source_message_id=message.id,
                status=ProductStatus.READY,
                published_at=datetime.now(UTC),
            )
        )

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    assert await processor.process_next() is True

    async with maker() as session:
        product = await session.scalar(select(Product))
        outboxes = list(await session.scalars(select(PublicationOutbox)))
    assert product is not None and product.status is ProductStatus.NEEDS_REVIEW
    assert product.review_reasons == [
        {
            "code": "source_publication_mode_review",
            "detail": "source is configured for manual approval",
            "field": None,
        }
    ]
    assert outboxes == []
    assert transport.send_count == 0
    assert transport.update_count == 0


async def test_rejected_edit_wedge_recovers_by_rearming_the_existing_outbox_row(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RejectedOncePublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA wedge", telegram_channel_id=-1095)
        session.add(destination)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=74,
            text="Price: 10 USD\nSizes: S M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-WEDGE-1",
            source_id=source.id,
            source_message_id=message.id,
            destination_id=destination.id,
            final_text=rendered("EM-WEDGE-1"),
            status=ProductStatus.NEEDS_REVIEW,
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    processor = ProcessorService(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    await processor.publish_reviewed(product_id)
    async with maker() as session, session.begin():
        published = await session.get(Product, product_id)
        assert published is not None and published.status is ProductStatus.PUBLISHED
        published.final_text = rendered("EM-WEDGE-1").replace(
            "Уточнить / Check", "$13", 1
        )
        published.status = ProductStatus.NEEDS_REVIEW
    await processor.publish_reviewed(product_id)
    # The identical edit is approved again: the same payload must re-arm the failed
    # row instead of wedging, so the retry stays one outbox row and one publication.
    await processor.publish_reviewed(product_id)

    async with maker() as session:
        persisted = await session.get(Product, product_id)
        outboxes = list(
            await session.scalars(
                select(PublicationOutbox)
                .where(PublicationOutbox.product_id == product_id)
                .order_by(PublicationOutbox.id)
            )
        )
    assert persisted is not None
    assert persisted.status is ProductStatus.PUBLISHED
    assert persisted.emoda_code == "EM-WEDGE-1"
    assert len(outboxes) == 2
    edit_rows = [row for row in outboxes if row.payload.get("operation") == "edit"]
    assert len(edit_rows) == 1
    assert edit_rows[0].status is OutboxStatus.PUBLISHED
    assert edit_rows[0].attempts == 2
    assert transport.send_count == 1
    assert transport.update_count == 2


async def test_startup_recovery_isolates_a_wedged_product_and_publishes_the_rest(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA recovery", telegram_channel_id=-1096)
        session.add(destination)
        await session.flush()
        wedged_message = SourceMessage(
            source_id=source.id,
            telegram_message_id=81,
            text="Price: 10 USD\nSizes: S M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        healthy_message = SourceMessage(
            source_id=source.id,
            telegram_message_id=82,
            text="Price: 12 USD\nSizes: M L\n2 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add_all([wedged_message, healthy_message])
        await session.flush()
        wedged = Product(
            emoda_code="EM-RECOVER-1",
            source_id=source.id,
            source_message_id=wedged_message.id,
            destination_id=destination.id,
            final_text=rendered("EM-RECOVER-1"),
            status=ProductStatus.READY_TO_PUBLISH,
        )
        healthy = Product(
            emoda_code="EM-RECOVER-2",
            source_id=source.id,
            source_message_id=healthy_message.id,
            destination_id=destination.id,
            final_text=rendered("EM-RECOVER-2"),
            status=ProductStatus.READY_TO_PUBLISH,
        )
        session.add_all([wedged, healthy])
        await session.flush()
        wedged_id = wedged.id
        healthy_id = healthy.id

    processor = WedgedRecoveryProcessor(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    processor.wedged_product_id = wedged_id
    processor.failure = UncertainPublicationFailure("publication update state is unsafe")
    await processor.recover()

    async with maker() as session:
        wedged_row = await session.get(Product, wedged_id)
        healthy_row = await session.get(Product, healthy_id)
    assert wedged_row is not None
    assert wedged_row.status is ProductStatus.RECONCILIATION_REQUIRED
    assert wedged_row.review_reasons
    assert "publication recovery failed" in (wedged_row.processing_error or "")
    assert healthy_row is not None
    assert healthy_row.status is ProductStatus.PUBLISHED
    assert transport.send_count == 1


async def test_startup_recovery_records_non_publication_failure_and_completes(
    database: tuple[async_sessionmaker[AsyncSession], Path],
) -> None:
    maker, media_root = database
    transport = RecordingPublisher()
    async with maker() as session, session.begin():
        source = await add_source(session)
        destination = Destination(name="E-MODA boom", telegram_channel_id=-1097)
        session.add(destination)
        await session.flush()
        wedged_message = SourceMessage(
            source_id=source.id,
            telegram_message_id=83,
            text="Price: 10 USD\nSizes: S M\n1 pcs",
            telegram_date=datetime.now(UTC),
        )
        healthy_message = SourceMessage(
            source_id=source.id,
            telegram_message_id=84,
            text="Price: 12 USD\nSizes: M L\n2 pcs",
            telegram_date=datetime.now(UTC),
        )
        session.add_all([wedged_message, healthy_message])
        await session.flush()
        wedged = Product(
            emoda_code="EM-RECOVER-3",
            source_id=source.id,
            source_message_id=wedged_message.id,
            destination_id=destination.id,
            final_text=rendered("EM-RECOVER-3"),
            status=ProductStatus.READY_TO_PUBLISH,
        )
        healthy = Product(
            emoda_code="EM-RECOVER-4",
            source_id=source.id,
            source_message_id=healthy_message.id,
            destination_id=destination.id,
            final_text=rendered("EM-RECOVER-4"),
            status=ProductStatus.READY_TO_PUBLISH,
        )
        session.add_all([wedged, healthy])
        await session.flush()
        wedged_id = wedged.id
        healthy_id = healthy.id

    processor = WedgedRecoveryProcessor(
        maker,
        media_root,
        None,
        PublicationService(transport),
        bot_username="emoda_autopost_bot",
    )
    processor.wedged_product_id = wedged_id
    processor.failure = RuntimeError("boom")
    await processor.recover()

    async with maker() as session:
        wedged_row = await session.get(Product, wedged_id)
        healthy_row = await session.get(Product, healthy_id)
    assert wedged_row is not None
    assert wedged_row.status is ProductStatus.NEEDS_REVIEW
    assert wedged_row.review_reasons
    assert "publication recovery failed" in (wedged_row.processing_error or "")
    assert healthy_row is not None
    assert healthy_row.status is ProductStatus.PUBLISHED
    assert transport.send_count == 1
