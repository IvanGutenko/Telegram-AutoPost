from datetime import UTC, datetime

import pytest
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.methods import EditMessageText, SendMessage
from aiogram.types import InputMediaPhoto, InputMediaVideo
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import (
    Destination,
    MessageEventTransition,
    Product,
    Source,
    SourceMessage,
)
from emoda.domain.enums import ButtonAction, MessageState, OutboxStatus, ProductStatus
from emoda.domain.schemas import NavigationButtonData
from emoda.integrations.publisher import TelegramBotPublisher
from emoda.services.publication import (
    PublicationService,
    SafePublicationFailure,
    UncertainPublicationFailure,
    build_keyboard_snapshot,
    catalog_deep_link,
    normalize_publication_button_url,
)


def test_catalog_button_snapshot_opens_private_telegram_catalog() -> None:
    snapshot = build_keyboard_snapshot(
        [
            NavigationButtonData(
                text="📂 Каталог",
                action=ButtonAction.CATALOG,
                row=0,
                order=1,
            )
        ],
        bot_username="emoda_autopost_bot",
    )

    assert snapshot == [
        [
            {
                "text": "📂 Каталог",
                "url": "https://t.me/emoda_autopost_bot?start=catalog",
            }
        ]
    ]


def test_catalog_deep_link_rejects_untrusted_bot_username() -> None:
    assert catalog_deep_link("@emoda_autopost_bot") == (
        "https://t.me/emoda_autopost_bot?start=catalog"
    )
    with pytest.raises(ValueError):
        catalog_deep_link("bad/name")


@pytest.mark.parametrize(
    ("entered", "expected"),
    [
        ("@emoda_orders", "https://t.me/emoda_orders"),
        ("t.me/emoda_orders", "https://t.me/emoda_orders"),
        ("https://example.com/order", "https://example.com/order"),
    ],
)
def test_publication_button_url_accepts_human_editable_telegram_forms(
    entered: str, expected: str
) -> None:
    assert normalize_publication_button_url(entered) == expected


@pytest.mark.parametrize("entered", ["emoda_orders", "+905552942994", "http://example.com"])
def test_publication_button_url_rejects_ambiguous_or_unsafe_forms(entered: str) -> None:
    with pytest.raises(ValueError):
        normalize_publication_button_url(entered)


class SimulatedCrash(BaseException):
    pass


class FakePublisher:
    def __init__(self) -> None:
        self.sent: dict[tuple[int, str], list[int]] = {}
        self.send_count = 0
        self.update_count = 0
        self.crash_after_accept = False
        self.history_error = False
        self.safe_error = False

    async def validate_destination(self, destination_channel_id: int) -> None:
        from emoda.services.destination_access import DestinationAccessError

        if getattr(self, "access_denied", False):
            raise DestinationAccessError("Канал недоступен; проверьте права бота")

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.send_count += 1
        code = str(payload["emoda_code"])
        self.sent[(destination_channel_id, code)] = [9001]
        if self.safe_error:
            raise SafePublicationFailure("bot cannot post to destination")
        if self.crash_after_accept:
            raise SimulatedCrash()
        return [9001]

    async def find_by_code(self, destination_channel_id: int, emoda_code: str) -> list[int] | None:
        if self.history_error:
            raise TimeoutError("history unavailable")
        return self.sent.get((destination_channel_id, emoda_code))

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


class UpdateRejectingPublisher(FakePublisher):
    def __init__(self) -> None:
        super().__init__()
        self.reject_remaining_updates = 0

    async def update(
        self,
        destination_channel_id: int,
        message_ids: list[int],
        old_payload: dict[str, object],
        new_payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        if self.reject_remaining_updates > 0:
            self.reject_remaining_updates -= 1
            raise SafePublicationFailure(
                "media content changed; Telegram media update requires manual review"
            )
        return await super().update(
            destination_channel_id, message_ids, old_payload, new_payload, keyboard
        )


class FakeBot:
    def __init__(self) -> None:
        self.groups = 0
        self.videos = 0
        self.messages: list[object] = []
        self.sent_texts: list[str] = []
        self.group_media: list[object] = []
        self.edited_text_ids: list[int] = []
        self.edited_caption_ids: list[int] = []
        self.edited_media_ids: list[int] = []
        self.edited_media_sources: list[object] = []
        self.edited_media_captions: list[str | None] = []
        self.edited_media_markups: list[object | None] = []
        self.parse_modes: list[str | None] = []

    async def send_photo(self, channel: int, media: object, **kwargs: object) -> object:
        self.parse_modes.append(str(kwargs.get("parse_mode")) if kwargs.get("parse_mode") else None)
        return type("Message", (), {"message_id": 40})()

    async def send_video(self, channel: int, media: object, **kwargs: object) -> object:
        self.videos += 1
        self.parse_modes.append(str(kwargs.get("parse_mode")) if kwargs.get("parse_mode") else None)
        return type("Message", (), {"message_id": 41})()

    async def send_media_group(self, channel: int, media: list[object]) -> list[object]:
        self.groups += 1
        self.group_media = media
        return [type("Message", (), {"message_id": index + 1})() for index in range(len(media))]

    async def send_message(
        self,
        channel: int,
        text: str,
        reply_markup: object = None,
        parse_mode: str | None = None,
    ) -> object:
        self.messages.append(reply_markup)
        self.sent_texts.append(text)
        self.parse_modes.append(parse_mode)
        return type("Message", (), {"message_id": 50})()

    async def edit_message_text(
        self,
        text: str,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: object = None,
        parse_mode: str | None = None,
    ) -> None:
        self.edited_text_ids.append(message_id)
        self.parse_modes.append(parse_mode)

    async def edit_message_caption(
        self,
        *,
        chat_id: int,
        message_id: int,
        caption: str,
        reply_markup: object = None,
        parse_mode: str | None = None,
    ) -> None:
        self.edited_caption_ids.append(message_id)
        self.parse_modes.append(parse_mode)

    async def edit_message_media(
        self,
        *,
        chat_id: int,
        message_id: int,
        media: object,
        reply_markup: object = None,
    ) -> None:
        self.edited_media_ids.append(message_id)
        self.edited_media_sources.append(getattr(media, "media", None))
        self.edited_media_captions.append(getattr(media, "caption", None))
        self.edited_media_markups.append(reply_markup)



class FloodWaitBot(FakeBot):
    def __init__(self) -> None:
        super().__init__()
        self.calls = 0

    async def send_message(
        self,
        channel: int,
        text: str,
        reply_markup: object = None,
        parse_mode: str | None = None,
    ) -> object:
        self.calls += 1
        if self.calls == 1:
            raise TelegramRetryAfter(
                method=SendMessage(chat_id=channel, text=text),
                message="retry",
                retry_after=0,
            )
        return await super().send_message(
            channel, text, reply_markup=reply_markup, parse_mode=parse_mode
        )


class UnchangedEditBot(FakeBot):
    async def edit_message_caption(self, **kwargs: object) -> None:
        await self.edit_message_text(
            str(kwargs.get("caption", "")),
            chat_id=int(str(kwargs["chat_id"])),
            message_id=int(str(kwargs["message_id"])),
        )

    async def edit_message_text(
        self,
        text: str,
        *,
        chat_id: int,
        message_id: int,
        reply_markup: object = None,
        parse_mode: str | None = None,
    ) -> None:
        raise TelegramBadRequest(
            method=EditMessageText(
                text=text,
                chat_id=chat_id,
                message_id=message_id,
            ),
            message="Bad Request: message is not modified",
        )


class TextRejectingAfterAlbumBot(FakeBot):
    async def send_message(
        self,
        channel: int,
        text: str,
        reply_markup: object = None,
        parse_mode: str | None = None,
    ) -> object:
        raise TelegramBadRequest(
            method=SendMessage(chat_id=channel, text=text),
            message="Bad Request: text rejected",
        )


class HistoryClient:
    def __init__(self, bodies: list[tuple[int, str]]) -> None:
        self.bodies = bodies

    async def iter_messages(self, *args: object, **kwargs: object) -> object:
        for message_id, body in self.bodies:
            yield type("HistoryMessage", (), {"id": message_id, "message": body})()


class LargeMediaClient:
    def __init__(self) -> None:
        self.calls: list[tuple[int, object, dict[str, object]]] = []

    async def send_file(self, channel: int, file: object, **kwargs: object) -> object:
        self.calls.append((channel, file, kwargs))
        if isinstance(file, list):
            return [
                type("Message", (), {"id": 60 + index})()
                for index in range(len(file))
            ]
        return type("Message", (), {"id": 60})()


@pytest.fixture
async def db() -> tuple[AsyncSession, Destination, Product]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    session = maker()
    source = Source(name="supplier", telegram_channel_id=123, enabled=True)
    destination = Destination(name="women", telegram_channel_id=-100123)
    session.add_all([source, destination])
    await session.flush()
    product = Product(
        emoda_code="EM-000001", source_id=source.id, status=ProductStatus.READY_TO_PUBLISH
    )
    session.add(product)
    await session.commit()
    yield session, destination, product
    await session.close()
    await engine.dispose()


async def test_destination_preflight_blocks_before_intent_and_allows_explicit_retry(db):
    session, destination, product = db
    publisher = FakePublisher()
    publisher.access_denied = True
    service = PublicationService(publisher)
    outbox = await service.create(
        session, product=product, destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "test"}, keyboard=[]
    )
    await service.publish(session, outbox.id)
    assert outbox.status is OutboxStatus.FAILED
    assert outbox.attempts == 0
    assert publisher.send_count == 0
    assert product.status is ProductStatus.NEEDS_REVIEW
    assert "права" in product.processing_error
    publisher.access_denied = False
    product.status = ProductStatus.READY_TO_PUBLISH
    product.processing_error = None
    await session.commit()
    await service.publish(session, outbox.id)
    assert outbox.status is OutboxStatus.PUBLISHED
    assert publisher.send_count == 1


@pytest.mark.parametrize("operation", ["update", "replace"])
async def test_lost_access_preserves_existing_publication_without_edit_or_delete(db, operation):
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    original = await service.create(
        session, product=product, destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "old"}, keyboard=[],
    )
    await service.publish(session, original.id)
    publisher.access_denied = True
    product.status = ProductStatus.READY_TO_PUBLISH
    change = await service.create(
        session, product=product, destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "new"}, keyboard=[],
    )
    await getattr(service, operation)(session, change.id, original.id)
    assert change.status is OutboxStatus.FAILED
    assert change.attempts == 0
    assert original.status is OutboxStatus.PUBLISHED
    assert original.telegram_message_ids == [9001]
    assert publisher.send_count == 1
    assert publisher.update_count == 0


async def test_destination_preflight_never_rearms_uncertain_publication(db):
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(
        session, product=product, destination=destination,
        payload={"text": "price only"}, keyboard=[],
    )
    outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
    await session.commit()
    with pytest.raises(UncertainPublicationFailure):
        await service.publish(session, outbox.id)
    assert publisher.send_count == 0
    assert outbox.status is OutboxStatus.RECONCILIATION_REQUIRED


def test_destinations_receive_independent_keyboard_layouts() -> None:
    shared = NavigationButtonData(text="Site", url="https://example.com", row=1, order=1)
    women = build_keyboard_snapshot(
        [NavigationButtonData(text="Women", url="https://t.me/women", row=0, order=1), shared]
    )
    men = build_keyboard_snapshot(
        [NavigationButtonData(text="Men", url="https://t.me/men", row=0, order=1), shared]
    )
    assert women[0][0]["text"] == "Women"
    assert men[0][0]["text"] == "Men"
    assert women[1] == men[1]


async def test_keyboard_is_snapshotted_when_outbox_is_created(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    service = PublicationService(FakePublisher())
    first = build_keyboard_snapshot(
        [NavigationButtonData(text="Old", url="https://example.com/old", row=0, order=0)]
    )
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "post", "media_paths": []},
        keyboard=first,
    )
    first[0][0]["text"] = "New"
    assert outbox.keyboard_snapshot[0][0]["text"] == "Old"


async def test_crash_after_telegram_acceptance_reconciles_without_second_send(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "emoda_code": product.emoda_code,
            "text": "<b>Код / Code:</b> EM-000001\n\n<b>Цена за шт / Unit price:</b> $23",
            "media_paths": [],
        },
        keyboard=[],
    )
    publisher.crash_after_accept = True
    with pytest.raises(SimulatedCrash):
        await service.publish(session, outbox.id)
    await session.refresh(outbox)
    assert outbox.status is OutboxStatus.PUBLISHING
    publisher.crash_after_accept = False
    await service.reconcile(session)
    await session.refresh(outbox)
    assert outbox.status is OutboxStatus.PUBLISHED
    assert outbox.telegram_message_ids == [9001]
    assert publisher.send_count == 1


async def test_price_only_uncertain_send_requires_manual_review_without_retry(db):
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(session, product=product, destination=destination,
        payload={'emoda_code': product.emoda_code, 'publication_format': 'supplier_percent',
                 'text': '<b>Цена:</b> 27.5 USD', 'media_paths': ['photo.jpg']}, keyboard=[])
    publisher.crash_after_accept = True
    with pytest.raises(SimulatedCrash):
        await service.publish(session, outbox.id)
    publisher.crash_after_accept = False
    await service.reconcile(session)
    await session.refresh(outbox)
    assert outbox.status is OutboxStatus.RECONCILIATION_REQUIRED
    assert 'manual verification required' in outbox.last_error
    assert publisher.send_count == 1


async def test_alternative_text_uncertain_send_requires_manual_review_without_retry(db):
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "emoda_code": product.emoda_code,
            "publication_format": "alternative_text",
            "text": "Наценка канала / Destination markup",
            "media_paths": ["photo.jpg"],
        },
        keyboard=[],
    )
    publisher.crash_after_accept = True
    with pytest.raises(SimulatedCrash):
        await service.publish(session, outbox.id)
    publisher.crash_after_accept = False

    await service.reconcile(session)
    await session.refresh(outbox)

    assert outbox.status is OutboxStatus.RECONCILIATION_REQUIRED
    assert "manual verification required" in outbox.last_error
    assert publisher.send_count == 1


async def test_unconfirmed_publishing_is_not_blindly_retried(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "post", "media_paths": []},
        keyboard=[],
    )
    outbox.status = OutboxStatus.PUBLISHING
    outbox.publishing_started_at = datetime.now(UTC)
    await session.commit()
    await service.reconcile(session)
    assert outbox.status is OutboxStatus.RECONCILIATION_REQUIRED
    assert publisher.send_count == 0


async def test_supplier_edit_before_durable_send_intent_cancels_stale_outbox(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "stale", "media_paths": []},
        keyboard=[],
    )
    product.status = ProductStatus.READY
    product.processing_error = "supplier edit requires reprocess"
    await session.commit()
    await service.publish(session, outbox.id)
    assert outbox.status is OutboxStatus.FAILED
    assert product.status is ProductStatus.READY
    assert publisher.send_count == 0


async def test_history_outage_keeps_reconciliation_required_without_crashing_startup(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "post", "media_paths": []},
        keyboard=[],
    )
    outbox.status = OutboxStatus.PUBLISHING
    await session.commit()
    publisher.history_error = True
    await service.reconcile(session)
    assert outbox.status is OutboxStatus.RECONCILIATION_REQUIRED
    assert outbox.last_error == "history reconciliation failed: history unavailable"


async def test_permanent_publish_error_routes_product_to_admin_review(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    publisher.safe_error = True
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "post", "media_paths": []},
        keyboard=[],
    )
    await service.publish(session, outbox.id)
    assert outbox.status is OutboxStatus.FAILED
    assert product.status is ProductStatus.NEEDS_REVIEW
    assert product.processing_error == "publication failed: bot cannot post to destination"


async def test_album_uses_linked_bot_message_for_inline_keyboard() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    keyboard = [[{"text": "Site", "url": "https://example.com"}]]
    ids = await publisher.send(
        -1001,
        {
            "emoda_code": "EM-000001",
            "text": (
                "Код / Code: EM-000001\n"
                "Цена за шт / Unit price: $23\n"
                "Размеры / Sizes: S, M, L\n"
                "Шт в серии / Pcs per lot: 6"
            ),
            "media_paths": ["a.jpg", "b.jpg"],
        },
        keyboard,
    )
    assert bot.groups == 1
    assert len(ids) == 3
    assert bot.messages[-1] is not None
    assert getattr(bot.group_media[0], "caption", None) is None
    assert bot.sent_texts == [
        "Код / Code: EM-000001\n"
        "Цена за шт / Unit price: $23\n"
        "Размеры / Sizes: S, M, L\n"
        "Шт в серии / Pcs per lot: 6"
    ]
    assert "#EMODA_" not in bot.sent_texts[-1]
    assert bot.parse_modes == ["HTML"]


async def test_mixed_album_preserves_photo_and_video_types() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]

    await publisher.send(
        -1001,
        {
            "emoda_code": "EM-000002",
            "text": "Mixed album",
            "media_paths": ["first.jpg", "second.mp4"],
            "media_types": ["photo", "video"],
        },
        [],
    )

    assert isinstance(bot.group_media[0], InputMediaPhoto)
    assert isinstance(bot.group_media[1], InputMediaVideo)


async def test_single_video_is_sent_as_video() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]

    ids = await publisher.send(
        -1001,
        {
            "emoda_code": "EM-000003",
            "text": "Single video",
            "media_paths": ["video.mp4"],
            "media_types": ["video"],
        },
        [],
    )

    assert ids == [41]
    assert bot.videos == 1
    assert bot.parse_modes == ["HTML"]


async def test_published_supplier_edit_updates_existing_message_without_new_send(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    original = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "old", "media_paths": []},
        keyboard=[],
    )
    await service.publish(session, original.id)
    product.status = ProductStatus.READY_TO_PUBLISH
    update = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "operation": "edit",
            "emoda_code": product.emoda_code,
            "text": "new",
            "media_paths": [],
        },
        keyboard=[],
    )
    await service.update(session, update.id, original.id)
    assert publisher.send_count == 1
    assert publisher.update_count == 1
    assert update.status is OutboxStatus.PUBLISHED


async def test_in_place_update_records_the_published_outcome_in_the_ledger(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    source_message = SourceMessage(
        source_id=product.source_id,
        telegram_message_id=77,
        telegram_date=datetime.now(UTC),
    )
    session.add(source_message)
    await session.flush()
    product.source_message_id = source_message.id
    await session.commit()
    publisher = FakePublisher()
    service = PublicationService(publisher)
    original = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "old", "media_paths": []},
        keyboard=[],
    )
    await service.publish(session, original.id)

    async def published_transitions() -> list[MessageEventTransition]:
        return list(
            await session.scalars(
                select(MessageEventTransition).where(
                    MessageEventTransition.source_message_id == source_message.id,
                    MessageEventTransition.state == MessageState.PUBLISHED,
                )
            )
        )

    before = await published_transitions()
    assert before  # publish already ledgered the same outcome for this source message
    product.status = ProductStatus.READY_TO_PUBLISH
    update = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "operation": "edit",
            "emoda_code": product.emoda_code,
            "text": "new",
            "media_paths": [],
        },
        keyboard=[],
    )
    await service.update(session, update.id, original.id)
    after = await published_transitions()
    assert update.status is OutboxStatus.PUBLISHED
    assert len(after) == len(before) + 1  # the update path ledgered its own transition


async def test_newer_supplier_edit_cancels_stale_approved_update_before_io(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    original = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "old", "media_paths": []},
        keyboard=[],
    )
    await service.publish(session, original.id)
    product.status = ProductStatus.READY_TO_PUBLISH
    update = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "operation": "edit",
            "emoda_code": product.emoda_code,
            "text": "approved edit",
            "media_paths": [],
        },
        keyboard=[],
    )
    product.status = ProductStatus.READY
    product.processing_error = "newer supplier edit"
    await session.commit()
    await service.update(session, update.id, original.id)
    assert update.status is OutboxStatus.FAILED
    assert product.status is ProductStatus.READY
    assert publisher.update_count == 0


async def test_reconciled_album_never_blindly_edits_only_its_marker_message(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    original = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "emoda_code": product.emoda_code,
            "text": "album post",
            "media_paths": ["a.jpg", "b.jpg"],
        },
        keyboard=[],
    )
    original.status = OutboxStatus.PUBLISHING
    publisher.sent[(destination.telegram_channel_id, product.emoda_code)] = [9001]
    await session.commit()
    await service.reconcile(session)
    assert original.status is OutboxStatus.PUBLISHED
    assert original.telegram_message_ids == [9001]
    assert original.last_error is not None and "automatic edits disabled" in original.last_error

    update = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "operation": "edit",
            "emoda_code": product.emoda_code,
            "text": "edited album post",
            "media_paths": ["a.jpg", "b.jpg"],
        },
        keyboard=[],
    )
    with pytest.raises(SafePublicationFailure, match="without all multipart message IDs"):
        await service.update(session, update.id, original.id)
    assert publisher.update_count == 0


async def test_same_count_media_replacement_is_edited_in_place() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    message_ids = [1, 2, 3]
    returned = await publisher.update(
        -1001,
        message_ids,
        {"text": "old", "media_paths": ["old-a.jpg", "old-b.jpg"]},
        {"text": "new", "media_paths": ["old-a.jpg", "new-b.jpg"]},
        [[{"text": "Site", "url": "https://example.com"}]],
    )
    assert returned is message_ids
    assert bot.edited_media_ids == [2]
    assert [str(getattr(source, "path", "")) for source in bot.edited_media_sources] == [
        "new-b.jpg"
    ]
    assert bot.edited_media_captions == [None]
    assert bot.edited_media_markups == [None]  # album media cannot own the keyboard
    assert bot.edited_text_ids == [3]


async def test_single_photo_media_and_caption_are_replaced_in_one_call() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    returned = await publisher.update(
        -1001,
        [40],
        {"text": "old", "media_paths": ["old.jpg"]},
        {"text": "new", "media_paths": ["new.jpg"]},
        [[{"text": "Site", "url": "https://example.com"}]],
    )
    assert returned == [40]
    assert bot.edited_media_ids == [40]
    assert [str(getattr(source, "path", "")) for source in bot.edited_media_sources] == ["new.jpg"]
    assert bot.edited_media_captions == ["new"]
    assert all(markup is not None for markup in bot.edited_media_markups)
    assert bot.edited_caption_ids == []


async def test_media_count_change_names_the_structural_difference() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    with pytest.raises(SafePublicationFailure) as exc_info:
        await publisher.update(
            -1001,
            [1, 2],
            {"text": "old", "media_paths": ["a.jpg"]},
            {"text": "new", "media_paths": ["a.jpg", "b.jpg"]},
            [],
        )
    message = str(exc_info.value)
    assert "1 media -> 2 media" in message
    assert "cannot add or remove messages" in message
    assert bot.edited_media_ids == []


async def test_media_type_change_edits_the_same_message() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)
    result = await publisher.update(-1001, [40],
        {"text": "old", "media_paths": ["a.jpg"]},
        {"text": "new", "media_paths": ["a.mp4"]}, [])
    assert result == [40]
    assert bot.edited_media_ids == [40]


async def test_message_count_change_is_rejected_before_any_telegram_call() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    with pytest.raises(SafePublicationFailure) as exc_info:
        await publisher.update(
            -1001,
            [40],
            {"text": "short", "media_paths": ["photo.jpg"]},
            {"text": "x" * 1025, "media_paths": ["photo.jpg"]},
            [],
        )
    assert "1 -> 2 Telegram messages" in str(exc_info.value)
    assert bot.edited_text_ids == []
    assert bot.edited_caption_ids == []
    assert bot.edited_media_ids == []


async def test_unchanged_media_still_edits_only_the_caption() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    await publisher.update(
        -1001,
        [40],
        {"text": "old", "media_paths": ["photo.jpg"]},
        {"text": "new", "media_paths": ["photo.jpg"]},
        [],
    )
    assert bot.edited_caption_ids == [40]
    assert bot.edited_media_ids == []


async def test_history_reconciliation_matches_complete_code_not_prefix() -> None:
    history = HistoryClient(
        [
            (0, "Код / Code: EM-0000012\n\nЦена за шт / Unit price: $25"),
            (1, "supplier text EM-000001"),
            (2, "Код / Code: EM-000001\n\nЦена за шт / Unit price: $23"),
            (3, "#EMODA_EM-000001"),
        ]
    )
    publisher = TelegramBotPublisher(FakeBot(), history)  # type: ignore[arg-type]
    assert await publisher.find_by_code(-1001, "EM-000001") == [2]


async def test_identical_telegram_edit_is_an_idempotent_success() -> None:
    publisher = TelegramBotPublisher(UnchangedEditBot(), history_client=None)  # type: ignore[arg-type]
    payload: dict[str, object] = {"text": "same", "media_paths": ["photo.jpg"]}

    assert await publisher.update(-1001, [50], payload, payload, []) == [50]


async def test_large_single_video_uses_mtproto_bot_with_exact_payload(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "large.mp4"
    path.write_bytes(b"video")
    monkeypatch.setattr("emoda.integrations.publisher.BOT_API_VIDEO_LIMIT_BYTES", 1)
    bot = FakeBot()
    media_client = LargeMediaClient()
    publisher = TelegramBotPublisher(
        bot, history_client=None, media_client=media_client
    )  # type: ignore[arg-type]
    text = (
        "<b>Код / Code:</b> AA01601\n"
        "<b>Цена за шт / Unit price:</b> Уточнить / Check\n"
        "<b>Размеры / Sizes:</b> Уточнить / Check\n"
        "<b>Шт в серии / Pcs per lot:</b> Уточнить / Check"
    )
    keyboard = [
        [{"text": "🛒 Заказать / Order", "url": "https://example.com/order"}],
        [{"text": "📂 Каталог / Catalog", "url": "https://example.com/catalog"}],
    ]

    ids = await publisher.send(
        -1001,
        {
            "emoda_code": "AA01601",
            "text": text,
            "media_paths": [str(path)],
            "media_types": ["video"],
        },
        keyboard,
    )

    assert ids == [60]
    assert len(media_client.calls) == 1
    channel, sent_path, kwargs = media_client.calls[0]
    assert channel == -1001
    assert sent_path == str(path)
    assert kwargs["caption"] == text
    assert kwargs["parse_mode"] == "html"
    assert kwargs["supports_streaming"] is True
    assert len(kwargs["buttons"]) == 2  # type: ignore[arg-type]
    assert bot.videos == 0


async def test_album_with_large_media_uses_mtproto_then_exact_linked_text(
    tmp_path, monkeypatch
) -> None:
    first = tmp_path / "large.jpg"
    second = tmp_path / "normal.jpg"
    first.write_bytes(b"large")
    second.write_bytes(b"small")
    monkeypatch.setattr("emoda.integrations.publisher.BOT_API_PHOTO_LIMIT_BYTES", 4)
    bot = FakeBot()
    media_client = LargeMediaClient()
    publisher = TelegramBotPublisher(
        bot, history_client=None, media_client=media_client
    )  # type: ignore[arg-type]
    text = "exact renderer text"

    ids = await publisher.send(
        -1001,
        {
            "emoda_code": "AA01602",
            "text": text,
            "media_paths": [str(first), str(second)],
            "media_types": ["photo", "photo"],
        },
        [[{"text": "Order", "url": "https://example.com/order"}]],
    )

    assert ids == [60, 61, 50]
    assert media_client.calls[0][1] == [str(first), str(second)]
    assert media_client.calls[0][2]["supports_streaming"] is False
    assert bot.groups == 0
    assert bot.sent_texts == [text]
    assert bot.messages[-1] is not None


async def test_album_over_aggregate_http_limit_uses_mtproto(
    tmp_path, monkeypatch
) -> None:
    first = tmp_path / "first.mp4"
    second = tmp_path / "second.mp4"
    first.write_bytes(b"1234")
    second.write_bytes(b"5678")
    monkeypatch.setattr("emoda.integrations.publisher.BOT_API_VIDEO_LIMIT_BYTES", 10)
    monkeypatch.setattr(
        "emoda.integrations.publisher.BOT_API_MULTIPART_LIMIT_BYTES", 7
    )
    bot = FakeBot()
    media_client = LargeMediaClient()
    publisher = TelegramBotPublisher(
        bot, history_client=None, media_client=media_client
    )  # type: ignore[arg-type]

    ids = await publisher.send(
        -1001,
        {
            "emoda_code": "AA00453",
            "text": "exact renderer text",
            "media_paths": [str(first), str(second)],
            "media_types": ["video", "video"],
        },
        [[{"text": "Order", "url": "https://example.com/order"}]],
    )

    assert ids == [60, 61, 50]
    assert media_client.calls[0][1] == [str(first), str(second)]
    assert bot.groups == 0


async def test_text_failure_after_album_acceptance_is_uncertain_not_retryable() -> None:
    bot = TextRejectingAfterAlbumBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]

    with pytest.raises(UncertainPublicationFailure, match="text rejected"):
        await publisher.send(
            -1001,
            {
                "emoda_code": "AA00001",
                "text": "renderer text",
                "media_paths": ["one.jpg", "two.jpg"],
            },
            [],
        )

    assert bot.groups == 1


async def test_text_only_publication_is_rejected_without_telegram_io() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    text = (
        "<b>Код / Code:</b> EM-TEST\n\n"
        "<b>Цена за шт / Unit price:</b> $23\n\n"
        "<b>Размеры / Sizes:</b> S, M, L\n\n"
        "<b>Шт в серии / Pcs per lot:</b> 6"
    )

    with pytest.raises(SafePublicationFailure, match="media_required"):
        await publisher.send(-1001, {"emoda_code": "EM-TEST", "text": text, "media_paths": []}, [])
    assert bot.sent_texts == []
    assert bot.parse_modes == []


async def test_flood_wait_is_retried_before_any_message_is_accepted() -> None:
    bot = FloodWaitBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    # Exercise the bounded pre-accept retry primitive used by media/text calls.
    message = await publisher._call(bot.send_message, -1001, "E-MODA code: EM-000001")
    assert message.message_id == 50
    assert bot.calls == 2


async def test_single_photo_edit_uses_same_message_shape_as_original_send() -> None:
    bot = FakeBot()
    publisher = TelegramBotPublisher(bot, history_client=None)  # type: ignore[arg-type]
    old_payload: dict[str, object] = {
        "emoda_code": "EM-000001",
        "text": "x" * 1025,
        "media_paths": ["photo.jpg"],
    }
    message_ids = await publisher.send(-1001, old_payload, [])
    assert message_ids == [40, 50]
    await publisher.update(
        -1001,
        message_ids,
        old_payload,
        {
            "emoda_code": "EM-000001",
            "text": "short update",
            "media_paths": ["photo.jpg"],
        },
        [],
    )
    assert bot.edited_text_ids == [50]
    assert bot.edited_caption_ids == []
    assert bot.parse_modes == [None, "HTML", "HTML"]


async def test_failed_publication_without_message_ids_is_rearmed_on_retry(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    publisher.safe_error = True
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "post", "media_paths": []},
        keyboard=[],
    )
    await service.publish(session, outbox.id)
    assert outbox.status is OutboxStatus.FAILED
    publisher.safe_error = False
    product.status = ProductStatus.READY_TO_PUBLISH
    await service.publish(session, outbox.id)
    assert outbox.status is OutboxStatus.PUBLISHED
    assert outbox.last_error is None
    assert outbox.telegram_message_ids == [9001]
    assert publisher.send_count == 2


async def test_failed_outbox_carrying_message_ids_still_refuses_publish(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    outbox = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "post", "media_paths": []},
        keyboard=[],
    )
    outbox.status = OutboxStatus.FAILED
    outbox.telegram_message_ids = [9001]
    outbox.last_error = "stale attempt"
    await session.commit()
    with pytest.raises(UncertainPublicationFailure):
        await service.publish(session, outbox.id)
    assert publisher.send_count == 0


async def test_rejected_edit_without_message_ids_is_rearmed_on_repeated_update(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = UpdateRejectingPublisher()
    publisher.reject_remaining_updates = 1
    service = PublicationService(publisher)
    original = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "old", "media_paths": []},
        keyboard=[],
    )
    await service.publish(session, original.id)
    product.status = ProductStatus.READY_TO_PUBLISH
    update = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "operation": "edit",
            "emoda_code": product.emoda_code,
            "text": "new",
            "media_paths": [],
        },
        keyboard=[],
    )
    await service.update(session, update.id, original.id)
    assert update.status is OutboxStatus.FAILED
    assert update.telegram_message_ids == []
    product.status = ProductStatus.READY_TO_PUBLISH
    await service.update(session, update.id, original.id)
    assert update.status is OutboxStatus.PUBLISHED
    assert update.last_error is None
    assert publisher.update_count == 1


async def test_reconciliation_required_update_row_still_refuses_in_place_edit(
    db: tuple[AsyncSession, Destination, Product],
) -> None:
    session, destination, product = db
    publisher = FakePublisher()
    service = PublicationService(publisher)
    original = await service.create(
        session,
        product=product,
        destination=destination,
        payload={"emoda_code": product.emoda_code, "text": "old", "media_paths": []},
        keyboard=[],
    )
    await service.publish(session, original.id)
    product.status = ProductStatus.READY_TO_PUBLISH
    update = await service.create(
        session,
        product=product,
        destination=destination,
        payload={
            "operation": "edit",
            "emoda_code": product.emoda_code,
            "text": "new",
            "media_paths": [],
        },
        keyboard=[],
    )
    update.status = OutboxStatus.RECONCILIATION_REQUIRED
    await session.commit()
    with pytest.raises(UncertainPublicationFailure):
        await service.update(session, update.id, original.id)
    assert publisher.update_count == 0
