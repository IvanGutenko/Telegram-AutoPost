import asyncio
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from telethon.tl.types import PeerChannel

from emoda.db.base import Base
from emoda.db.models import (
    AlbumGroup,
    MediaItem,
    MessageEventTransition,
    Product,
    Source,
    SourceMessage,
    SourceMessageVersion,
    SourceProfile,
)
from emoda.domain.enums import (
    AlbumStatus,
    EditPolicy,
    EventOrigin,
    MessageState,
    ProductStatus,
    PublicationMode,
)
from emoda.domain.schemas import (
    CollectionBoundaryRule,
    CollectionProfile,
    ExtractionRule,
    SourceIgnoreRule,
    SourceProfileData,
)
from emoda.integrations.collector import CollectorService, edit_reprocess_decision
from emoda.integrations.storage import LocalMediaStorage
from emoda.services.albums import AlbumCoordinator
from emoda.services.processor import ProcessorService


class TelegramMessage:
    def __init__(self, message_id: int, content: bytes, *, video: bool = False) -> None:
        self.id = message_id
        self.content = content
        self.edit_date = datetime(2026, 8, 24, 10, 0, tzinfo=UTC)
        self.grouped_id = None
        self.video = object() if video else None


class CollectorClient:
    def __init__(self, message: TelegramMessage) -> None:
        self.message = message

    async def get_messages(self, channel_id: int, ids: int) -> TelegramMessage:
        if isinstance(ids, list):
            return [self.message if item == self.message.id else None for item in ids]
        assert ids == self.message.id
        return self.message

    async def download_media(self, message: TelegramMessage, file: type[bytes]) -> bytes:
        assert file is bytes
        return message.content

    async def iter_messages(self, *args: Any, **kwargs: Any) -> Any:
        if False:
            yield None


class EditedHistoryClient(CollectorClient):
    async def iter_messages(self, *args: Any, **kwargs: Any) -> Any:
        if "limit" in kwargs:
            yield self.message


class ReferenceAlbumClient:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = {message.id: message for message in messages}

    async def get_messages(self, channel: str | int, ids: int | list[int]) -> Any:
        if isinstance(ids, list):
            return [self.messages[item] for item in ids if item in self.messages]
        return self.messages[ids]

    async def get_entity(self, channel: str | int) -> PeerChannel:
        return PeerChannel(123)

    async def download_media(self, message: Any, file: type[bytes]) -> bytes:
        assert file is bytes
        return message.content


class InitialAnchorClient:
    def __init__(self, latest_id: int) -> None:
        self.latest_id = latest_id
        self.history_iterated = False

    async def get_messages(self, channel_id: int, limit: int) -> list[Any]:
        assert limit == 1
        return [type("LatestMessage", (), {"id": self.latest_id})()]

    async def iter_messages(self, *args: Any, **kwargs: Any) -> Any:
        self.history_iterated = True
        if False:
            yield None


class GapHistoryClient:
    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages

    async def iter_messages(self, *args: Any, **kwargs: Any) -> Any:
        if "min_id" in kwargs:
            for message in sorted(self.messages, key=lambda item: item.id):
                if message.id > kwargs["min_id"]:
                    yield message
            return
        limit = int(kwargs.get("limit", len(self.messages)))
        for message in sorted(self.messages, key=lambda item: item.id, reverse=True)[:limit]:
            yield message

    async def get_messages(self, channel_id: int, ids: int) -> Any:
        return next((item for item in self.messages if item.id == ids), None)

    async def download_media(self, message: Any, file: type[bytes]) -> bytes:
        assert file is bytes
        return message.content


class AlbumHydrationClient:
    """Returns the complete server-side album for one late/high live event."""

    def __init__(self, messages: list[Any]) -> None:
        self.messages = {message.id: message for message in messages}

    async def get_messages(self, channel_id: int, ids: int | list[int]) -> Any:
        assert channel_id == 999
        if isinstance(ids, list):
            return [self.messages.get(message_id) for message_id in ids]
        return self.messages.get(ids)

    async def download_media(self, message: Any, file: type[bytes]) -> bytes:
        assert file is bytes
        return message.content

    async def iter_messages(self, *args: Any, **kwargs: Any) -> Any:
        if False:
            yield None


class PerSourceFailureClient:
    def __init__(self, failing_channel_id: int, messages: dict[int, list[Any]]) -> None:
        self.failing_channel_id = failing_channel_id
        self.messages = messages

    async def iter_messages(self, channel_id: int, **kwargs: Any) -> Any:
        if channel_id == self.failing_channel_id:
            raise RuntimeError("unexpected source history failure")
        values = self.messages.get(channel_id, [])
        if "min_id" in kwargs:
            values = [item for item in values if item.id > kwargs["min_id"]]
            values = sorted(values, key=lambda item: item.id)
        else:
            values = sorted(values, key=lambda item: item.id, reverse=True)[
                : int(kwargs.get("limit", len(values)))
            ]
        for message in values:
            yield message

    async def download_media(self, message: Any, file: type[bytes]) -> bytes:
        assert file is bytes
        return message.content


class IncomingMessage(TelegramMessage):
    def __init__(
        self,
        message_id: int,
        *,
        text: str,
        date: datetime,
        grouped_id: int | None,
        video: bool = False,
        has_media: bool = True,
    ) -> None:
        super().__init__(message_id, f"media-{message_id}".encode(), video=video)
        self.message = text
        self.date = date
        self.edit_date = None
        self.grouped_id = grouped_id
        self.media = object() if has_media else None
        self.photo = object() if has_media and not video else None
        self.post = True
        self.views = 0


class CatchupEditClient:
    """Serves one bounded rescan window; min_id history stays empty."""

    def __init__(self, messages: list[Any]) -> None:
        self.messages = messages

    async def iter_messages(self, channel_id: int, **kwargs: Any) -> Any:
        if "min_id" in kwargs:
            return
        for message in self.messages:
            yield message

    async def get_messages(self, channel_id: int, ids: int) -> Any:
        return next((item for item in self.messages if item.id == ids), None)

    async def download_media(self, message: Any, file: type[bytes]) -> bytes:
        assert file is bytes
        return message.content


async def test_structural_solo_filter_runs_before_persistent_collection(adjacent_text_db):
    maker, source_id, root = adjacent_text_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        profile = SourceProfileData.model_validate(source.profile.config)
        profile.collection = CollectionProfile(strategy="multi_grouped_id", order="media_before_text",
            merge_neighbor_grouped_ids=True, max_time_gap_seconds="86400")
        profile.ignore_rules = [SourceIgnoreRule(name="solo", grouped_id="missing",
            media_types=["photo", "video"], description_missing_required=False)]
        source.profile.config = profile.model_dump(mode="json")
    now = datetime.now(UTC)
    album = IncomingMessage(300, text="", date=now, grouped_id=800)
    collector = CollectorService(CollectorClient(album), maker, LocalMediaStorage(root))
    await persist_incoming(collector, 2001, album)
    solo = IncomingMessage(301, text="advert", date=now + timedelta(seconds=1), grouped_id=None)
    await persist_incoming(collector, 2001, solo)
    async with maker() as session:
        rows = list(await session.scalars(select(SourceMessage).order_by(SourceMessage.id)))
        assert rows[0].album_group_id is not None
        assert rows[1].album_group_id is None
        assert rows[1].raw_metadata["ignored_reason"] == "structural_ignore:solo"
        assert rows[1].processed_at is not None
        media = list(await session.scalars(select(MediaItem)))
        assert len(media) == 1


async def test_structural_ignore_cannot_reach_fallback_gemini_or_critical_check(adjacent_text_db):
    from unittest.mock import AsyncMock

    from emoda.domain.schemas import SourceFallbackRule
    from emoda.services.processor import ProcessorService

    maker, source_id, root = adjacent_text_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        profile = SourceProfileData.model_validate(source.profile.config)
        profile.gemini_enabled = True
        profile.fallback_rules = {"quantity": SourceFallbackRule(value="6", authoritative=True)}
        profile.ignore_rules = [SourceIgnoreRule(name="solo-first", grouped_id="missing",
            media_types=["photo", "video"], description_missing_required=False)]
        source.profile.config = profile.model_dump(mode="json")
    solo = IncomingMessage(401, text="", date=datetime.now(UTC), grouped_id=None)
    collector = CollectorService(CollectorClient(solo), maker, LocalMediaStorage(root))
    await persist_incoming(collector, 2001, solo)
    intelligence = AsyncMock()
    processor = ProcessorService(maker, root, intelligence, None)
    processor.pipeline.run = AsyncMock(side_effect=AssertionError("IGNORE reached pipeline"))
    assert await processor.process_next() is False
    assert not processor.pipeline.run.mock_calls
    assert not intelligence.mock_calls
    async with maker() as session:
        assert await session.scalar(select(Product)) is None
        assert await session.scalar(select(MediaItem)) is None
        message = await session.scalar(select(SourceMessage))
        assert message.raw_metadata["ignored_reason"] == "structural_ignore:solo-first"


@pytest.fixture
async def adjacent_media_db(
    tmp_path: Path,
) -> tuple[async_sessionmaker[AsyncSession], int, int, Path]:
    engine = create_async_engine(f"sqlite+aiosqlite:///{tmp_path / 'adjacent.sqlite3'}")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="merge-source", telegram_channel_id=999, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData(
                translation_enabled=False,
                album_debounce_seconds=Decimal("0.5"),
                adjacent_media_merge_seconds=Decimal("90"),
            ).model_dump(mode="json"),
        )
        other = Source(name="other-source", telegram_channel_id=1000, enabled=True)
        other.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData(
                translation_enabled=False,
                adjacent_media_merge_seconds=Decimal("90"),
            ).model_dump(mode="json"),
        )
        session.add_all([source, other])
        await session.flush()
        source_id, other_id = source.id, other.id
    yield maker, source_id, other_id, tmp_path
    await engine.dispose()


@pytest.fixture
async def adjacent_text_db(
    tmp_path: Path,
) -> tuple[async_sessionmaker[AsyncSession], int, Path]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="text-companion-source", telegram_channel_id=2001, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData(
                translation_enabled=False,
                album_debounce_seconds=Decimal("0.5"),
                adjacent_text_merge_seconds=Decimal("90"),
            ).model_dump(mode="json"),
        )
        session.add(source)
        await session.flush()
        source_id = source.id
    yield maker, source_id, tmp_path
    await engine.dispose()


@pytest.fixture
async def multi_group_db(
    tmp_path: Path,
) -> tuple[async_sessionmaker[AsyncSession], dict[str, int], Path]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    configs = {
        "text_before_media": SourceProfileData(
            translation_enabled=False,
            album_debounce_seconds=Decimal("0.5"),
            product_collection_mode="text_before_media",
            product_collection_window_seconds=Decimal("90"),
        ),
        "media_before_text": SourceProfileData(
            translation_enabled=False,
            album_debounce_seconds=Decimal("0.5"),
            product_collection_mode="media_before_text",
            product_collection_window_seconds=Decimal("90"),
        ),
        "off": SourceProfileData(
            translation_enabled=False,
            product_collection_window_seconds=Decimal("90"),
        ),
    }
    channels: dict[str, int] = {}
    async with maker() as session, session.begin():
        for offset, (mode, profile) in enumerate(configs.items(), start=1):
            channel_id = 3000 + offset
            source = Source(name=mode, telegram_channel_id=channel_id, enabled=True)
            source.profile = SourceProfile(
                publication_mode=PublicationMode.REVIEW,
                config=profile.model_dump(mode="json"),
            )
            session.add(source)
            channels[mode] = channel_id
    yield maker, channels, tmp_path
    await engine.dispose()


async def persist_incoming(
    collector: CollectorService, channel_id: int, message: IncomingMessage
) -> None:
    event = type("IncomingEvent", (), {"chat_id": channel_id, "message": message})()
    await collector.persist_event(event, edited=False)


async def test_album_is_fully_persisted_before_first_media_download(adjacent_media_db):
    maker, source_id, _, root = adjacent_media_db
    messages = [IncomingMessage(710 + i, text='25 USD S M 4 pcs' if i == 2 else '',
        date=datetime.now(UTC), grouped_id=778899) for i in range(3)]
    observed = []

    class Client(AlbumHydrationClient):
        async def download_media(self, message, file):
            async with maker() as session:
                rows = list(await session.scalars(select(SourceMessage)
                    .where(SourceMessage.source_id == source_id)))
                observed.append([r.telegram_message_id for r in rows])
            return message.content

    collector = CollectorService(Client(messages), maker, LocalMediaStorage(root))
    await collector._new_album(type('Event', (), {'chat_id': 999, 'messages': messages})())
    assert observed and all(sorted(ids) == [710, 711, 712] for ids in observed)


@pytest.mark.parametrize('history_failure', [True, False])
async def test_album_event_members_survive_failed_or_partial_history(adjacent_media_db, history_failure):
    maker, source_id, _, root = adjacent_media_db
    messages = [IncomingMessage(810 + i, text='25 USD' if i == 0 else '',
        date=datetime.now(UTC), grouped_id=998877) for i in range(3)]

    class Client(AlbumHydrationClient):
        async def get_messages(self, channel_id, ids):
            if history_failure:
                raise OSError('temporary Telegram history failure')
            return [messages[-1]]

    collector = CollectorService(Client(messages), maker, LocalMediaStorage(root))
    await collector._new_album(type('Event', (), {'chat_id': 999, 'messages': messages})())
    async with maker() as session:
        rows = list(await session.scalars(select(SourceMessage).where(SourceMessage.source_id == source_id)))
    assert sorted(r.telegram_message_id for r in rows) == [810, 811, 812]


@pytest.mark.parametrize('edited', [False, True])
async def test_late_member_of_merged_album_keeps_original_collection(adjacent_media_db, edited):
    maker, source_id, _, root = adjacent_media_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        source.profile.config = SourceProfileData(collection=CollectionProfile(
            strategy='multi_grouped_id', order='media_before_text',
            merge_neighbor_grouped_ids=True, finalize_on_next_start=True,
            max_time_gap_seconds=Decimal('300'))).model_dump(mode='json')
    base = datetime.now(UTC)
    collector = CollectorService(AlbumHydrationClient([]), maker, LocalMediaStorage(root))
    for mid, group, text in [(900, 111, ''), (901, 222, ''), (903, None, '25 USD S M 4 pcs')]:
        await persist_incoming(collector, 999, IncomingMessage(mid, text=text,
            date=base + timedelta(seconds=mid-900), grouped_id=group, has_media=group is not None))
    async with maker() as session, session.begin():
        anchor = await session.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id == 900))
        original_group = anchor.album_group_id
        group = await session.get(AlbumGroup, original_group)
        group.status = AlbumStatus.PROCESSED
        product = Product(source_id=source_id, album_group_id=original_group,
            emoda_code='LATE-MERGE-TEST', status=ProductStatus.PUBLISHED,
            published_at=base, original_text='25 USD S M 4 pcs')
        session.add(product)
        await session.flush()
        group.product_id = product.id
    # A lower member arrives after the closing text, as in a catch-up gap scan.
    member = IncomingMessage(901 if edited else 902, text='',
        date=base + timedelta(seconds=1), grouped_id=222)
    if edited:
        member.edit_date = base + timedelta(hours=1)
    await collector.persist_event(type('Event', (), {'chat_id': 999, 'message': member})(), edited=edited)
    async with maker() as session:
        late = await session.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id == member.id))
    assert late.album_group_id == original_group


async def test_live_album_event_hydrates_missing_lower_ids_before_publication(
    adjacent_media_db: tuple[async_sessionmaker[AsyncSession], int, int, Path],
) -> None:
    """One received album member must durably pull the complete Telegram album.

    Production delivered the two highest IDs live and the four lower IDs only in
    periodic catch-up.  The old code published two photos, then failed the 2 -> 6
    edit and moved an AUTO product to REVIEW.
    """

    maker, source_id, _, root = adjacent_media_db
    base = datetime.now(UTC)
    messages = [
        IncomingMessage(
            50808 + offset,
            text="17 $\nS M L\n6 pcs" if offset == 0 else "",
            date=base,
            grouped_id=14306778519355196,
        )
        for offset in range(6)
    ]
    collector = CollectorService(
        AlbumHydrationClient(messages),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )
    event = type("AlbumEvent", (), {"chat_id": 999, "messages": [messages[-1]]})()

    await collector._new_album(event)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        media = list(
            await session.scalars(
                select(MediaItem)
                .join(SourceMessage, SourceMessage.id == MediaItem.source_message_id)
                .where(SourceMessage.source_id == source_id, MediaItem.active.is_(True))
            )
        )
        source = await session.get(Source, source_id)

    assert [row.telegram_message_id for row in rows] == list(range(50808, 50814))
    assert len({row.album_group_id for row in rows}) == 1
    assert rows[0].album_group_id is not None
    assert all(row.media_pending is False for row in rows)
    assert len(media) == 6
    assert source is not None and source.last_seen_message_id == 50813


async def test_neighboring_real_grouped_ids_create_separate_products(
    adjacent_media_db: tuple[async_sessionmaker[AsyncSession], int, int, Path],
) -> None:
    maker, source_id, _, root = adjacent_media_db
    base = datetime.now(UTC)
    captions = {
        700: "Sunset Stripe Jacket 18 $ (6199)",
        701: "Island Glow Dress 18.5 $ (2052)",
        702: "Terra Breeze Dress 17.5 $ (2057)",
    }
    first = IncomingMessage(7000, text=captions[700], date=base, grouped_id=700)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    for offset, (grouped_id, caption) in enumerate(captions.items()):
        await persist_incoming(
            collector,
            999,
            IncomingMessage(
                7000 + offset * 10,
                text=caption,
                date=base + timedelta(seconds=offset),
                grouped_id=grouped_id,
            ),
        )
        await persist_incoming(
            collector,
            999,
            IncomingMessage(
                7001 + offset * 10,
                text="",
                date=base + timedelta(seconds=offset, milliseconds=100),
                grouped_id=grouped_id,
            ),
        )

    async with maker() as session, session.begin():
        groups = list(
            await session.scalars(
                select(AlbumGroup)
                .where(AlbumGroup.source_id == source_id)
                .order_by(AlbumGroup.grouped_id)
            )
        )
        assert [group.grouped_id for group in groups] == [700, 701, 702]
        assert len({group.id for group in groups}) == 3
        await session.execute(
            update(AlbumGroup)
            .where(AlbumGroup.id.in_([group.id for group in groups]))
            .values(ready_at=base - timedelta(seconds=1))
        )

    processor = ProcessorService(maker, root, None, None)
    work = [await processor._claim_work() for _ in range(3)]
    assert all(item is not None for item in work)
    assert {item[2] for item in work if item is not None} == set(captions.values())
    async with maker() as session:
        products = list(
            await session.scalars(select(Product).where(Product.source_id == source_id))
        )
    assert len(products) == 3
    assert len({product.album_group_id for product in products}) == 3


async def test_text_before_media_collects_multiple_grouped_ids_until_next_text(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    channel_id = channels["text_before_media"]
    base = datetime.now(UTC)
    first = IncomingMessage(100, text="Article M1\nPrice 27 USD", date=base, grouped_id=None, has_media=False)
    collector = CollectorService(
        CollectorClient(first), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    messages = [
        first,
        IncomingMessage(101, text="", date=base + timedelta(seconds=1), grouped_id=10),
        IncomingMessage(102, text="", date=base + timedelta(seconds=1), grouped_id=10),
        IncomingMessage(103, text="", date=base + timedelta(seconds=2), grouped_id=11),
        IncomingMessage(104, text="", date=base + timedelta(seconds=3), grouped_id=None),
        IncomingMessage(105, text="Article M2", date=base + timedelta(seconds=4), grouped_id=None, has_media=False),
    ]
    for message in messages:
        await persist_incoming(collector, channel_id, message)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .join(Source, Source.id == SourceMessage.source_id)
                .where(Source.telegram_channel_id == channel_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        first_group = await session.get(AlbumGroup, rows[0].album_group_id)
    assert [row.grouped_id for row in rows] == [None, 10, 10, 11, None, None]
    assert rows[0].album_group_id is not None
    assert {row.album_group_id for row in rows[:5]} == {rows[0].album_group_id}
    assert rows[5].album_group_id not in {None, rows[0].album_group_id}
    assert first_group is not None
    assert first_group.ready_at.replace(tzinfo=UTC) <= datetime.now(UTC)


async def test_media_before_text_collects_multiple_grouped_ids_and_closes_on_text(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    channel_id = channels["media_before_text"]
    base = datetime.now(UTC)
    first = IncomingMessage(200, text="", date=base, grouped_id=20)
    collector = CollectorService(
        CollectorClient(first), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    messages = [
        first,
        IncomingMessage(201, text="", date=base, grouped_id=20),
        IncomingMessage(202, text="", date=base + timedelta(seconds=1), grouped_id=21),
        IncomingMessage(203, text="", date=base + timedelta(seconds=1), grouped_id=21),
        IncomingMessage(204, text="Price 13 USD\nSizes S M L", date=base + timedelta(seconds=2), grouped_id=None, has_media=False),
    ]
    for message in messages:
        await persist_incoming(collector, channel_id, message)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .join(Source, Source.id == SourceMessage.source_id)
                .where(Source.telegram_channel_id == channel_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        groups = list(
            await session.scalars(
                select(AlbumGroup)
                .join(Source, Source.id == AlbumGroup.source_id)
                .where(Source.telegram_channel_id == channel_id)
            )
        )
    assert [row.grouped_id for row in rows] == [20, 20, 21, 21, None]
    assert len({row.album_group_id for row in rows}) == 1
    assert len(groups) == 1
    assert groups[0].grouped_id == 20
    assert groups[0].ready_at.replace(tzinfo=UTC) <= datetime.now(UTC) + timedelta(seconds=1)
    async with maker() as session, session.begin():
        group = await session.get(AlbumGroup, groups[0].id)
        assert group is not None
        group.ready_at = datetime.now(UTC) - timedelta(seconds=1)
    claimed = await ProcessorService(maker, root, None, None)._claim_work()
    assert claimed is not None
    assert claimed[2] == "Price 13 USD\nSizes S M L"
    async with maker() as session:
        products = list(
            await session.scalars(
                select(Product)
                .join(Source, Source.id == Product.source_id)
                .where(Source.telegram_channel_id == channel_id)
            )
        )
    assert len(products) == 1
    assert products[0].album_group_id == groups[0].id


async def test_profile_v2_collection_rules_drive_live_multi_group_boundaries(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    channel_id = channels["off"]
    async with maker() as session, session.begin():
        source = await session.scalar(
            select(Source).where(Source.telegram_channel_id == channel_id)
        )
        assert source is not None
        source.profile.config = SourceProfileData(
            translation_enabled=False,
            collection=CollectionProfile(
                strategy="multi_grouped_id",
                order="media_before_text",
                merge_neighbor_grouped_ids=True,
                max_time_gap_seconds=Decimal("90"),
                boundary_rules=[
                    CollectionBoundaryRule(
                        name="product-caption",
                        action="hard_boundary",
                        match="regex",
                        pattern=r"(?m)^#[A-Z][A-Z ]+$",
                    )
                ],
            ),
        ).model_dump(mode="json")

    base = datetime.now(UTC)
    first = IncomingMessage(800, text="", date=base, grouped_id=80)
    collector = CollectorService(
        CollectorClient(first), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    for incoming in [
        first,
        IncomingMessage(801, text="", date=base, grouped_id=81),
        IncomingMessage(
            802,
            text="#ALPHA",
            date=base + timedelta(seconds=1),
            grouped_id=None,
            has_media=False,
        ),
        IncomingMessage(803, text="", date=base + timedelta(seconds=2), grouped_id=82),
        IncomingMessage(
            804,
            text="#BETA",
            date=base + timedelta(seconds=3),
            grouped_id=None,
            has_media=False,
        ),
    ]:
        await persist_incoming(collector, channel_id, incoming)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.telegram_message_id.in_([800, 801, 802, 803, 804]))
                .order_by(SourceMessage.telegram_message_id)
            )
        )
    assert {row.album_group_id for row in rows[:3]} == {rows[0].album_group_id}
    assert {row.album_group_id for row in rows[3:]} == {rows[3].album_group_id}
    assert rows[0].album_group_id != rows[3].album_group_id


async def test_album_text_text_next_album_closes_and_starts_durable_products(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    channel_id = channels["off"]
    async with maker() as session, session.begin():
        source = await session.scalar(
            select(Source).where(Source.telegram_channel_id == channel_id)
        )
        assert source is not None
        source.profile.config = SourceProfileData(
            translation_enabled=False,
            collection=CollectionProfile(
                strategy="multi_grouped_id",
                order="any",
                merge_neighbor_grouped_ids=True,
                max_time_gap_seconds=Decimal("90"),
                allow_multiple_text_blocks=True,
                merge_text_blocks=True,
                finalize_on_next_start=True,
                boundary_rules=[
                    CollectionBoundaryRule(
                        name="new-album",
                        action="start",
                        match="grouped_id_change",
                    ),
                    CollectionBoundaryRule(
                        name="following-text",
                        action="continue",
                        match="has_text",
                    ),
                ],
            ),
        ).model_dump(mode="json")

    base = datetime.now(UTC)
    album_a = IncomingMessage(900, text="A100", date=base, grouped_id=9000)
    collector = CollectorService(
        CollectorClient(album_a), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    first_stage = [
        album_a,
        IncomingMessage(901, text="", date=base, grouped_id=9000),
        IncomingMessage(
            902,
            text="27 $",
            date=base + timedelta(seconds=20),
            grouped_id=None,
            has_media=False,
        ),
    ]
    for incoming in first_stage:
        await persist_incoming(collector, channel_id, incoming)

    async with maker() as session:
        open_group = await session.scalar(
            select(AlbumGroup).where(AlbumGroup.grouped_id == 9000)
        )
        assert open_group is not None
        assert open_group.ready_at.replace(tzinfo=UTC) >= datetime.now(UTC) + timedelta(
            seconds=80
        )

    # A fresh CollectorService represents restart; duplicate catch-up is idempotent.
    restarted = CollectorService(
        CollectorClient(album_a), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    await persist_incoming(restarted, channel_id, first_stage[-1])
    for incoming in [
        IncomingMessage(
            903,
            text="Sizes S M L\nQuantity 3",
            date=base + timedelta(seconds=30),
            grouped_id=None,
            has_media=False,
        ),
        IncomingMessage(
            904,
            text="B200",
            date=base + timedelta(seconds=50),
            grouped_id=9001,
        ),
        IncomingMessage(
            905,
            text="28 $",
            date=base + timedelta(seconds=70),
            grouped_id=None,
            has_media=False,
        ),
    ]:
        await persist_incoming(restarted, channel_id, incoming)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.telegram_message_id.between(900, 905))
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        groups = list(
            await session.scalars(
                select(AlbumGroup)
                .where(AlbumGroup.source_id == rows[0].source_id)
                .order_by(AlbumGroup.id)
            )
        )

    assert len(rows) == 6
    assert len(groups) == 2
    assert {row.album_group_id for row in rows[:4]} == {groups[0].id}
    assert {row.album_group_id for row in rows[4:]} == {groups[1].id}
    assert groups[0].ready_at <= groups[1].last_seen_at + timedelta(seconds=3)
    assert any(
        item.raw_metadata.get("generic_collection_closed")
        for item in rows[:4]
    )


async def test_multi_group_collection_does_not_cross_window_or_default_off_boundary(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    base = datetime.now(UTC)
    first = IncomingMessage(300, text="", date=base, grouped_id=30)
    collector = CollectorService(
        CollectorClient(first), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    await persist_incoming(collector, channels["media_before_text"], first)
    await persist_incoming(
        collector,
        channels["media_before_text"],
        IncomingMessage(301, text="", date=base + timedelta(seconds=91), grouped_id=31),
    )
    await persist_incoming(
        collector,
        channels["off"],
        IncomingMessage(400, text="SOBE product A", date=base, grouped_id=40),
    )
    await persist_incoming(
        collector,
        channels["off"],
        IncomingMessage(401, text="SOBE product B", date=base + timedelta(seconds=1), grouped_id=41),
    )

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.telegram_message_id.in_([300, 301, 400, 401]))
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        off_groups = list(
            await session.scalars(
                select(AlbumGroup)
                .join(Source, Source.id == AlbumGroup.source_id)
                .where(Source.telegram_channel_id == channels["off"])
            )
        )
    assert rows[0].album_group_id != rows[1].album_group_id
    assert rows[2].album_group_id != rows[3].album_group_id
    assert all(
        group.ready_at.replace(tzinfo=UTC) <= datetime.now(UTC) + timedelta(seconds=30)
        for group in off_groups
    )


async def test_media_before_text_does_not_absorb_captioned_group(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    channel_id = channels["media_before_text"]
    base = datetime.now(UTC)
    first = IncomingMessage(500, text="", date=base, grouped_id=50)
    collector = CollectorService(
        CollectorClient(first), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    await persist_incoming(collector, channel_id, first)
    await persist_incoming(
        collector,
        channel_id,
        IncomingMessage(501, text="Independent product", date=base + timedelta(seconds=1), grouped_id=51),
    )

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.telegram_message_id.in_([500, 501]))
                .order_by(SourceMessage.telegram_message_id)
            )
        )
    assert rows[0].album_group_id != rows[1].album_group_id


async def test_text_before_media_closes_prior_collection_at_next_text_boundary(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    channel_id = channels["text_before_media"]
    base = datetime.now(UTC)
    first = IncomingMessage(
        600, text="Product A", date=base, grouped_id=None, has_media=False
    )
    collector = CollectorService(
        CollectorClient(first), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    messages = [
        first,
        IncomingMessage(601, text="", date=base + timedelta(seconds=1), grouped_id=60),
        IncomingMessage(
            602,
            text="Product B",
            date=base + timedelta(seconds=2),
            grouped_id=None,
            has_media=False,
        ),
        IncomingMessage(603, text="", date=base + timedelta(seconds=3), grouped_id=61),
    ]
    for message in messages:
        await persist_incoming(collector, channel_id, message)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .join(Source, Source.id == SourceMessage.source_id)
                .where(Source.telegram_channel_id == channel_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
    assert rows[0].album_group_id == rows[1].album_group_id
    assert rows[2].album_group_id == rows[3].album_group_id
    assert rows[0].album_group_id != rows[2].album_group_id


async def test_captioned_group_closes_open_text_before_media_collection(
    multi_group_db: tuple[async_sessionmaker[AsyncSession], dict[str, int], Path],
) -> None:
    maker, channels, root = multi_group_db
    channel_id = channels["text_before_media"]
    base = datetime.now(UTC)
    first = IncomingMessage(
        700, text="Text anchor", date=base, grouped_id=None, has_media=False
    )
    collector = CollectorService(
        CollectorClient(first), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    for message in [
        first,
        IncomingMessage(
            701,
            text="Captioned independent product",
            date=base + timedelta(seconds=1),
            grouped_id=70,
        ),
        IncomingMessage(702, text="", date=base + timedelta(seconds=2), grouped_id=71),
    ]:
        await persist_incoming(collector, channel_id, message)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .join(Source, Source.id == SourceMessage.source_id)
                .where(Source.telegram_channel_id == channel_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
    assert len({row.album_group_id for row in rows}) == 3


async def test_standalone_video_before_album_is_attached_by_exact_caption(
    adjacent_media_db: tuple[async_sessionmaker[AsyncSession], int, int, Path],
) -> None:
    maker, source_id, _, root = adjacent_media_db
    base = datetime.now(UTC)
    placeholder = IncomingMessage(1, text="Same caption", date=base, grouped_id=None, video=True)
    collector = CollectorService(
        CollectorClient(placeholder),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 999, placeholder)
    await persist_incoming(
        collector,
        999,
        IncomingMessage(
            2, text=" Same   Caption ", date=base + timedelta(seconds=70), grouped_id=77
        ),
    )
    await persist_incoming(
        collector,
        999,
        IncomingMessage(3, text="", date=base + timedelta(seconds=70), grouped_id=77),
    )

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        group = await session.scalar(select(AlbumGroup).where(AlbumGroup.grouped_id == 77))
    assert group is not None and group.status is AlbumStatus.COLLECTING
    assert [row.album_group_id for row in rows] == [group.id, group.id, group.id]
    assert rows[0].processing_after is None


async def test_standalone_video_after_album_is_attached_by_exact_caption(
    adjacent_media_db: tuple[async_sessionmaker[AsyncSession], int, int, Path],
) -> None:
    maker, source_id, _, root = adjacent_media_db
    base = datetime.now(UTC)
    first = IncomingMessage(10, text="SKU 1441", date=base, grouped_id=88)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 999, first)
    await persist_incoming(
        collector,
        999,
        IncomingMessage(11, text="", date=base, grouped_id=88),
    )
    await persist_incoming(
        collector,
        999,
        IncomingMessage(
            12, text="sku 1441", date=base + timedelta(seconds=30), grouped_id=None, video=True
        ),
    )

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        group = await session.scalar(select(AlbumGroup).where(AlbumGroup.grouped_id == 88))
    assert group is not None
    assert [row.album_group_id for row in rows] == [group.id, group.id, group.id]


async def test_adjacent_media_different_caption_or_source_is_not_merged(
    adjacent_media_db: tuple[async_sessionmaker[AsyncSession], int, int, Path],
) -> None:
    maker, source_id, other_id, root = adjacent_media_db
    base = datetime.now(UTC)
    first = IncomingMessage(20, text="Product A", date=base, grouped_id=99)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 999, first)
    await persist_incoming(
        collector,
        999,
        IncomingMessage(
            21, text="Product B", date=base + timedelta(seconds=10), grouped_id=None, video=True
        ),
    )
    await persist_incoming(
        collector,
        1000,
        IncomingMessage(
            22, text="Product A", date=base + timedelta(seconds=10), grouped_id=None, video=True
        ),
    )

    async with maker() as session:
        different_caption = await session.scalar(
            select(SourceMessage).where(
                SourceMessage.source_id == source_id,
                SourceMessage.telegram_message_id == 21,
            )
        )
        different_source = await session.scalar(
            select(SourceMessage).where(
                SourceMessage.source_id == other_id,
                SourceMessage.telegram_message_id == 22,
            )
        )
    assert different_caption is not None and different_caption.album_group_id is None
    assert different_source is not None and different_source.album_group_id is None


async def test_simultaneous_sources_with_same_transport_ids_stay_isolated(
    adjacent_media_db: tuple[async_sessionmaker[AsyncSession], int, int, Path],
) -> None:
    maker, first_source_id, second_source_id, root = adjacent_media_db
    now = datetime.now(UTC)
    collector = CollectorService(
        CollectorClient(IncomingMessage(50, text="unused", date=now, grouped_id=700)),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )

    await asyncio.gather(
        persist_incoming(
            collector,
            999,
            IncomingMessage(50, text="FIRST PRICE 10 USD", date=now, grouped_id=700),
        ),
        persist_incoming(
            collector,
            1000,
            IncomingMessage(50, text="SECOND PRICE 20 EUR", date=now, grouped_id=700),
        ),
    )

    async with maker() as session:
        groups = list(await session.scalars(select(AlbumGroup).order_by(AlbumGroup.source_id)))
        messages = list(
            await session.scalars(select(SourceMessage).order_by(SourceMessage.source_id))
        )

    assert [(group.source_id, group.grouped_id) for group in groups] == [
        (first_source_id, 700),
        (second_source_id, 700),
    ]
    assert [(message.source_id, message.text) for message in messages] == [
        (first_source_id, "FIRST PRICE 10 USD"),
        (second_source_id, "SECOND PRICE 20 EUR"),
    ]
    assert messages[0].album_group_id == groups[0].id
    assert messages[1].album_group_id == groups[1].id


async def test_merged_album_and_video_create_one_product_in_media_order(
    adjacent_media_db: tuple[async_sessionmaker[AsyncSession], int, int, Path],
) -> None:
    maker, source_id, _, root = adjacent_media_db
    base = datetime.now(UTC)
    first = IncomingMessage(30, text="Product 30", date=base, grouped_id=101)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 999, first)
    await persist_incoming(
        collector,
        999,
        IncomingMessage(31, text="", date=base, grouped_id=101),
    )
    await persist_incoming(
        collector,
        999,
        IncomingMessage(
            32,
            text="Product 30",
            date=base + timedelta(seconds=20),
            grouped_id=None,
            video=True,
        ),
    )
    async with maker() as session, session.begin():
        group = await session.scalar(select(AlbumGroup).where(AlbumGroup.grouped_id == 101))
        assert group is not None
        group.ready_at = datetime.now(UTC) - timedelta(seconds=1)

    processor = ProcessorService(maker, root, None, None)
    assert await processor.process_next() is True
    assert await processor.process_next() is False
    async with maker() as session:
        products = list(
            await session.scalars(select(Product).where(Product.source_id == source_id))
        )
        media = list(
            await session.scalars(
                select(MediaItem)
                .join(SourceMessage, SourceMessage.id == MediaItem.source_message_id)
                .where(SourceMessage.source_id == source_id)
                .order_by(MediaItem.telegram_message_id)
            )
        )
    assert len(products) == 1
    assert products[0].album_group_id is not None
    assert [item.telegram_message_id for item in media] == [30, 31, 32]
    assert [item.media_type for item in media] == ["photo", "photo", "video"]


async def test_empty_caption_album_followed_by_text_companion_creates_one_product(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    base = datetime.now(UTC)
    first = IncomingMessage(100, text="", date=base, grouped_id=202)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    for message_id in range(100, 110):
        await persist_incoming(
            collector,
            2001,
            IncomingMessage(message_id, text="", date=base, grouped_id=202),
        )
    description = "9866 maxi elbise fiyat 14$"
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            110,
            text=description,
            date=base + timedelta(seconds=10),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session, session.begin():
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        group = await session.scalar(select(AlbumGroup).where(AlbumGroup.grouped_id == 202))
        assert group is not None
        group.ready_at = datetime.now(UTC) - timedelta(seconds=1)
    assert len(rows) == 11
    assert {row.album_group_id for row in rows} == {group.id}

    processor = ProcessorService(maker, root, None, None)
    assert await processor.process_next() is True
    assert await processor.process_next() is False
    async with maker() as session:
        products = list(
            await session.scalars(select(Product).where(Product.source_id == source_id))
        )
    assert len(products) == 1
    assert products[0].album_group_id == group.id
    assert products[0].original_text == description


async def test_advertisement_between_album_and_caption_is_skipped_before_merge(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    base = datetime.now(UTC)
    first = IncomingMessage(140, text="", date=base, grouped_id=303)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            141,
            text="#реклама\nПодпишитесь на внешний сервис",
            date=base + timedelta(seconds=5),
            grouped_id=None,
            has_media=False,
        ),
    )
    product_text = "Price: 10 USD\nSizes: S M\n2 pcs"
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            142,
            text=product_text,
            date=base + timedelta(seconds=10),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session, session.begin():
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        group = await session.scalar(select(AlbumGroup).where(AlbumGroup.grouped_id == 303))
        assert group is not None
        group.ready_at = datetime.now(UTC) - timedelta(seconds=1)
    assert rows[1].processed_at is not None
    assert rows[1].raw_metadata["ignored_reason"] == "advertising:builtin"
    assert rows[1].raw_metadata["outcome"] == "skipped"
    assert rows[1].raw_metadata["outcome_reason"] == "advertising:builtin"
    assert rows[1].album_group_id is None
    assert rows[2].album_group_id == group.id

    processor = ProcessorService(maker, root, None, None)
    assert await processor.process_next() is True
    assert await processor.process_next() is False
    async with maker() as session:
        products = list(
            await session.scalars(select(Product).where(Product.source_id == source_id))
        )
    assert len(products) == 1
    assert products[0].original_text == product_text


async def test_article_media_and_field_continuations_form_one_durable_product(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        profile = SourceProfileData.model_validate(source.profile.config)
        source.profile.config = profile.model_copy(
            update={
                "extraction_rules": {
                    "article": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"(?m)^\s*(?P<value>[A-Z]\d+)\s*$",
                            authoritative=True,
                        )
                    ]
                },
                "ignore_rules": [
                    SourceIgnoreRule(
                        name="article-only-media",
                        media_types=["video"],
                        found_fields=["article"],
                        missing_fields=["price", "sizes", "quantity"],
                    )
                ],
            }
        ).model_dump(mode="json")

    base = datetime.now(UTC)
    first = IncomingMessage(160, text="E7193", date=base, grouped_id=None, video=True)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            161,
            text="Price: 10 USD",
            date=base + timedelta(seconds=5),
            grouped_id=None,
            has_media=False,
        ),
    )
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            162,
            text="Sizes: M-L",
            date=base + timedelta(seconds=10),
            grouped_id=None,
            has_media=False,
        ),
    )
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            163,
            text="1 pcs",
            date=base + timedelta(seconds=15),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session, session.begin():
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        assert len({row.album_group_id for row in rows}) == 1
        assert rows[0].album_group_id is not None
        group = await session.get(AlbumGroup, rows[0].album_group_id)
        assert group is not None and group.status is AlbumStatus.COLLECTING
        group.ready_at = datetime.now(UTC) - timedelta(seconds=1)

    processor = ProcessorService(maker, root, None, None)
    assert await processor.process_next() is True
    assert await processor.process_next() is False
    async with maker() as session:
        products = list(
            await session.scalars(select(Product).where(Product.source_id == source_id))
        )
    assert len(products) == 1
    assert products[0].original_text == "E7193\nPrice: 10 USD\nSizes: M-L\n1 pcs"
    assert products[0].parsed_result["ignored"] is False
    assert products[0].parsed_result["parsed"]["fields"]["price"]["value"] == "10"
    assert products[0].parsed_result["parsed"]["fields"]["sizes"]["value"] == "M-L"
    assert products[0].parsed_result["parsed"]["fields"]["quantity"]["value"] == "1"


async def test_video_no_text_ignore_reaches_durable_processor_with_stored_media_type(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        profile = SourceProfileData.model_validate(source.profile.config)
        source.profile.config = profile.model_copy(
            update={
                "gemini_enabled": True,
                "ignore_rules": [
                    SourceIgnoreRule(
                        name="video-no-text",
                        media_types=["video"],
                        media_required=True,
                        description_missing_required=True,
                        missing_fields=["price", "sizes", "quantity"],
                    )
                ],
            }
        ).model_dump(mode="json")

    video = IncomingMessage(
        164,
        text="",
        date=datetime.now(UTC),
        grouped_id=None,
        video=True,
    )
    collector = CollectorService(
        CollectorClient(video),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, video)
    async with maker() as session, session.begin():
        message = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 164)
        )
        assert message is not None
        message.processing_after = datetime.now(UTC) - timedelta(seconds=1)

    processor = ProcessorService(maker, root, None, None)
    assert await processor.process_next() is True
    async with maker() as session:
        product = await session.scalar(
            select(Product).where(Product.source_id == source_id)
        )
        assert product is not None
        assert product.status is ProductStatus.SKIPPED
        assert product.parsed_result["ignored"] is True
        assert product.parsed_result["ignore_reason"] == "video-no-text"


async def test_continuation_after_persistent_deadline_is_not_attached(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        profile = SourceProfileData.model_validate(source.profile.config)
        source.profile.config = profile.model_copy(
            update={
                "extraction_rules": {
                    "article": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"(?m)^\s*(?P<value>[A-Z]\d+)\s*$",
                        )
                    ]
                },
                "ignore_rules": [
                    SourceIgnoreRule(
                        name="article-only-media",
                        found_fields=["article"],
                        missing_fields=["price", "sizes", "quantity"],
                    )
                ],
            }
        ).model_dump(mode="json")

    base = datetime.now(UTC)
    first = IncomingMessage(170, text="E7193", date=base, grouped_id=None, video=True)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            171,
            text="Price: 10 USD",
            date=base + timedelta(seconds=5),
            grouped_id=None,
            has_media=False,
        ),
    )
    async with maker() as session, session.begin():
        group = await session.scalar(select(AlbumGroup).where(AlbumGroup.source_id == source_id))
        assert group is not None
        group.ready_at = datetime.now(UTC) - timedelta(seconds=1)

    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            172,
            text="Sizes: M-L",
            date=base + timedelta(seconds=10),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session:
        late = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 172)
        )
    assert late is not None and late.album_group_id is None


async def test_continuation_is_not_attached_when_two_media_anchors_are_pending(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        profile = SourceProfileData.model_validate(source.profile.config)
        source.profile.config = profile.model_copy(
            update={
                "extraction_rules": {
                    "article": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"(?m)^\s*(?P<value>[A-Z]\d+)\s*$",
                        )
                    ]
                },
                "ignore_rules": [
                    SourceIgnoreRule(
                        name="article-only-media",
                        found_fields=["article"],
                        missing_fields=["price", "sizes", "quantity"],
                    )
                ],
            }
        ).model_dump(mode="json")

    base = datetime.now(UTC)
    first = IncomingMessage(180, text="E7193", date=base, grouped_id=None, video=True)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            181,
            text="E7194",
            date=base + timedelta(seconds=2),
            grouped_id=None,
            video=True,
        ),
    )
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            182,
            text="Price: 10 USD",
            date=base + timedelta(seconds=4),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
    assert [item.album_group_id for item in rows] == [None, None, None]


async def test_text_companion_merge_is_disabled_by_default(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        profile = SourceProfileData.model_validate(source.profile.config)
        source.profile.config = profile.model_copy(
            update={"adjacent_text_merge_seconds": Decimal("0")}
        ).model_dump(mode="json")
    base = datetime.now(UTC)
    first = IncomingMessage(120, text="", date=base, grouped_id=220)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            121,
            text="separate post",
            date=base + timedelta(seconds=5),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
    assert rows[0].album_group_id is not None
    assert rows[1].album_group_id is None


async def test_captioned_album_does_not_absorb_following_text(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    base = datetime.now(UTC)
    first = IncomingMessage(130, text="album caption", date=base, grouped_id=230)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            131,
            text="next product",
            date=base + timedelta(seconds=5),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session:
        companion = await session.scalar(
            select(SourceMessage).where(
                SourceMessage.source_id == source_id,
                SourceMessage.telegram_message_id == 131,
            )
        )
    assert companion is not None and companion.album_group_id is None


async def test_text_companion_does_not_merge_when_album_is_ambiguous(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    base = datetime.now(UTC)
    first = IncomingMessage(140, text="", date=base, grouped_id=240)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(141, text="", date=base + timedelta(seconds=1), grouped_id=241),
    )
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            142,
            text="ambiguous description",
            date=base + timedelta(seconds=2),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session:
        companion = await session.scalar(
            select(SourceMessage).where(
                SourceMessage.source_id == source_id,
                SourceMessage.telegram_message_id == 142,
            )
        )
    assert companion is not None and companion.album_group_id is None


async def test_restart_catchup_text_reopens_processed_album_instead_of_second_product(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    base = datetime.now(UTC) - timedelta(minutes=3)
    first = IncomingMessage(200, text="", date=base, grouped_id=400)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    for message_id in range(200, 204):
        await persist_incoming(
            collector,
            2001,
            IncomingMessage(message_id, text="", date=base, grouped_id=400),
        )
    async with maker() as session, session.begin():
        group = await session.scalar(select(AlbumGroup).where(AlbumGroup.source_id == source_id))
        assert group is not None
        group.ready_at = datetime.now(UTC) - timedelta(seconds=1)
        group_id = group.id

    first_work = await ProcessorService(maker, root, None, None)._claim_work()
    assert first_work is not None and first_work[2] == ""
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            204,
            text="Price: 13.5 USD",
            date=base + timedelta(seconds=20),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session:
        companion = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 204)
        )
        products = list(
            await session.scalars(select(Product).where(Product.source_id == source_id))
        )
        media = list(
            await session.scalars(
                select(MediaItem)
                .join(SourceMessage, SourceMessage.id == MediaItem.source_message_id)
                .where(SourceMessage.album_group_id == group_id, MediaItem.active.is_(True))
            )
        )
    assert companion is not None and companion.album_group_id == group_id
    assert len(products) == 1
    assert products[0].album_group_id == group_id
    assert products[0].status is ProductStatus.READY
    assert len(media) == 4


async def test_text_companion_outside_window_is_not_merged(
    adjacent_text_db: tuple[async_sessionmaker[AsyncSession], int, Path],
) -> None:
    maker, source_id, root = adjacent_text_db
    base = datetime.now(UTC)
    first = IncomingMessage(150, text="", date=base, grouped_id=250)
    collector = CollectorService(
        CollectorClient(first),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await persist_incoming(collector, 2001, first)
    await persist_incoming(
        collector,
        2001,
        IncomingMessage(
            151,
            text="too late",
            date=base + timedelta(seconds=91),
            grouped_id=None,
            has_media=False,
        ),
    )

    async with maker() as session:
        companion = await session.scalar(
            select(SourceMessage).where(
                SourceMessage.source_id == source_id,
                SourceMessage.telegram_message_id == 151,
            )
        )
    assert companion is not None and companion.album_group_id is None


async def test_message_reference_loads_complete_unpersisted_album(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    base = datetime.now(UTC)
    messages = [
        IncomingMessage(50, text="Album caption", date=base, grouped_id=500),
        IncomingMessage(51, text="", date=base, grouped_id=500),
        IncomingMessage(52, text="", date=base, grouped_id=500, video=True),
    ]
    collector = CollectorService(
        ReferenceAlbumClient(messages),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )

    text, paths = await collector.load_message_reference("https://t.me/supplier/51")

    try:
        assert text == "Album caption"
        assert len(paths) == 3
        assert [Path(path).suffix for path in paths] == [".jpg", ".jpg", ".mp4"]
        contents = [await asyncio.to_thread(Path(path).read_bytes) for path in paths]
        assert contents == [
            b"media-50",
            b"media-51",
            b"media-52",
        ]
    finally:
        for path in paths:
            await asyncio.to_thread(Path(path).unlink, missing_ok=True)
        await engine.dispose()


async def test_uninitialized_source_anchors_at_current_history_without_backfill(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="new-source", telegram_channel_id=333, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData().model_dump(mode="json"),
        )
        session.add(source)
        await session.flush()
        source_id = source.id
    client = InitialAnchorClient(999)
    collector = CollectorService(
        client,
        maker,
        LocalMediaStorage(tmp_path),  # type: ignore[arg-type]
    )

    await collector.catch_up()

    async with maker() as session:
        source = await session.get(Source, source_id)
    assert source is not None and source.last_seen_message_id == 999
    assert client.history_iterated is False
    await engine.dispose()


async def test_catch_up_fills_recent_id_hole_without_pre_onboarding_backfill(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    base = datetime.now(UTC)
    async with maker() as session, session.begin():
        source = Source(
            name="gap-source",
            telegram_channel_id=444,
            enabled=True,
            catchup_anchor_message_id=100,
            last_seen_message_id=105,
        )
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)
        await session.flush()
        source_id = source.id
        session.add(
            SourceMessage(
                source_id=source_id,
                telegram_message_id=105,
                text="already seen",
                caption="already seen",
                telegram_date=base,
            )
        )
    history = [
        IncomingMessage(
            message_id,
            text=f"post {message_id}",
            date=base + timedelta(seconds=message_id),
            grouped_id=None,
            has_media=False,
        )
        for message_id in range(99, 106)
    ]
    collector = CollectorService(
        GapHistoryClient(history),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
        edit_rescan_limit=20,
    )

    await collector.catch_up()

    async with maker() as session:
        caught_up = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
    assert [item.telegram_message_id for item in caught_up] == [101, 102, 103, 104, 105]
    assert all(item.event_origin is EventOrigin.CATCHUP for item in caught_up[:-1])
    await engine.dispose()


async def test_event_failure_is_persisted_as_terminal_outcome(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="invalid-profile", telegram_channel_id=811, enabled=True)
        source.profile = SourceProfile(config={"schema_version": 999})
        session.add(source)
        await session.flush()
        source_id = source.id
    message = IncomingMessage(
        41,
        text="Price: 10 USD",
        date=datetime.now(UTC),
        grouped_id=None,
        has_media=False,
    )
    collector = CollectorService(
        GapHistoryClient([message]),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    event = type("IncomingEvent", (), {"chat_id": 811, "message": message})()

    await collector.handle_event(event, edited=False)

    async with maker() as session:
        row = await session.scalar(
            select(SourceMessage).where(
                SourceMessage.source_id == source_id,
                SourceMessage.telegram_message_id == 41,
            )
        )
        persisted_source = await session.get(Source, source_id)
    assert row is not None
    assert row.raw_metadata["outcome"] == "failed"
    assert "schema_version" in row.raw_metadata["outcome_reason"]
    assert len(row.raw_metadata["outcome_reason"]) <= 500
    assert persisted_source is not None and persisted_source.last_seen_message_id == 41
    await engine.dispose()


async def test_event_failure_does_not_overwrite_existing_product_outcome(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with maker() as session, session.begin():
        source = Source(name="invalid-profile", telegram_channel_id=812, enabled=True)
        source.profile = SourceProfile(config={"schema_version": 999})
        session.add(source)
        await session.flush()
        row = SourceMessage(
            source_id=source.id,
            telegram_message_id=42,
            text="existing",
            caption="existing",
            telegram_date=now,
            raw_metadata={"outcome": "needs_review", "outcome_reason": "manual review"},
        )
        session.add(row)
        await session.flush()
        session.add(
            Product(
                emoda_code="AA80042",
                source_id=source.id,
                source_message_id=row.id,
                status=ProductStatus.NEEDS_REVIEW,
            )
        )
    message = IncomingMessage(
        42,
        text="duplicate delivery",
        date=now,
        grouped_id=None,
        has_media=False,
    )
    collector = CollectorService(
        GapHistoryClient([message]),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    event = type("IncomingEvent", (), {"chat_id": 812, "message": message})()

    await collector.handle_event(event, edited=False)

    async with maker() as session:
        persisted = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 42)
        )
    assert persisted is not None
    assert persisted.raw_metadata == {
        "outcome": "needs_review",
        "outcome_reason": "manual review",
    }
    await engine.dispose()


async def test_event_failure_ignores_unrelated_standalone_product(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with maker() as session, session.begin():
        source = Source(name="invalid-profile", telegram_channel_id=813, enabled=True)
        source.profile = SourceProfile(config={"schema_version": 999})
        session.add(source)
        await session.flush()
        completed = SourceMessage(
            source_id=source.id,
            telegram_message_id=40,
            text="completed",
            caption="completed",
            telegram_date=now,
            raw_metadata={"outcome": "needs_review"},
        )
        failed = SourceMessage(
            source_id=source.id,
            telegram_message_id=43,
            text="received",
            caption="received",
            telegram_date=now,
            raw_metadata={"outcome": "received"},
        )
        session.add_all([completed, failed])
        await session.flush()
        session.add(
            Product(
                emoda_code="AA80040",
                source_id=source.id,
                source_message_id=completed.id,
                status=ProductStatus.NEEDS_REVIEW,
            )
        )
    message = IncomingMessage(
        43,
        text="retry",
        date=now,
        grouped_id=None,
        has_media=False,
    )
    collector = CollectorService(
        GapHistoryClient([message]),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    event = type("IncomingEvent", (), {"chat_id": 813, "message": message})()

    await collector.handle_event(event, edited=False)

    async with maker() as session:
        persisted = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 43)
        )
    assert persisted is not None
    assert persisted.raw_metadata["outcome"] == "failed"
    await engine.dispose()


async def test_catch_up_isolates_unexpected_source_failure(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        for name, channel_id in (("broken", 821), ("working", 822)):
            source = Source(
                name=name,
                telegram_channel_id=channel_id,
                enabled=True,
                catchup_anchor_message_id=10,
                last_seen_message_id=10,
            )
            source.profile = SourceProfile(
                config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
            )
            session.add(source)
    message = IncomingMessage(
        11,
        text="Price: 10 USD",
        date=datetime.now(UTC),
        grouped_id=None,
        has_media=False,
    )
    collector = CollectorService(
        PerSourceFailureClient(821, {822: [message]}),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )

    await collector.catch_up()

    async with maker() as session:
        working = await session.scalar(select(Source).where(Source.telegram_channel_id == 822))
        row = await session.scalar(
            select(SourceMessage).where(
                SourceMessage.source_id == working.id,
                SourceMessage.telegram_message_id == 11,
            )
        )
    assert working is not None and working.last_catchup_at is not None
    assert row is not None
    await engine.dispose()


@pytest.fixture
async def collector_db(
    tmp_path: Path,
) -> tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(
            name="supplier",
            telegram_channel_id=123,
            enabled=True,
            last_seen_message_id=7,
        )
        session.add(source)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            text="caption",
            telegram_date=datetime.now(UTC),
            media_pending=True,
        )
        session.add(message)
        await session.flush()
        source_id = source.id
        message_id = message.id
    async with maker() as session:
        persisted_source = await session.get(Source, source_id)
        persisted_message = await session.get(SourceMessage, message_id)
        assert persisted_source is not None and persisted_message is not None
    yield maker, persisted_source, persisted_message, tmp_path
    await engine.dispose()


async def test_catch_up_recovers_media_download_interrupted_by_restart(
    collector_db: tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path],
) -> None:
    maker, source, message, root = collector_db
    client = CollectorClient(TelegramMessage(message.telegram_message_id, b"photo-one"))
    collector = CollectorService(client, maker, LocalMediaStorage(root))  # type: ignore[arg-type]
    await collector.catch_up()
    async with maker() as session:
        recovered = await session.get(SourceMessage, message.id)
        media = list(
            await session.scalars(
                select(MediaItem).where(MediaItem.source_message_id == message.id)
            )
        )
    assert recovered is not None and recovered.media_pending is False
    assert len(media) == 1
    assert (root / media[0].storage_path).read_bytes() == b"photo-one"


async def test_catch_up_restores_retention_evicted_media_when_it_becomes_needed(
    collector_db: tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path],
) -> None:
    maker, source, message, root = collector_db
    async with maker() as session, session.begin():
        session.add(
            MediaItem(
                source_message_id=message.id,
                telegram_message_id=message.telegram_message_id,
                position=0,
                storage_path="source-1/message-7/evicted.jpg",
                active=True,
                local_deleted_at=datetime.now(UTC) - timedelta(days=1),
            )
        )
    telegram_message = TelegramMessage(message.telegram_message_id, b"restored-photo")
    collector = CollectorService(
        CollectorClient(telegram_message), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )

    await collector.catch_up()

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(MediaItem)
                .where(MediaItem.source_message_id == message.id)
                .order_by(MediaItem.position)
            )
        )
        recovered = await session.get(SourceMessage, message.id)
    assert recovered is not None and recovered.media_pending is False
    assert len(rows) == 2
    assert rows[0].active is False and rows[0].local_deleted_at is not None
    assert rows[1].active is True and rows[1].local_deleted_at is None
    assert (root / rows[1].storage_path).read_bytes() == b"restored-photo"


async def test_missing_active_media_is_redownloaded_before_error_is_cleared(
    collector_db: tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path],
) -> None:
    maker, source, message, root = collector_db
    async with maker() as session, session.begin():
        session.add(
            MediaItem(
                source_message_id=message.id,
                telegram_message_id=message.telegram_message_id,
                position=0,
                storage_path="source-1/message-7/missing.jpg",
                active=True,
            )
        )
        row = await session.get(SourceMessage, message.id)
        assert row is not None
        row.raw_metadata = {"media_download_error": "local file disappeared"}
    telegram_message = TelegramMessage(message.telegram_message_id, b"restored-photo")
    collector = CollectorService(
        CollectorClient(telegram_message), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )

    await collector._download_media(telegram_message, message.id, source.id)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(MediaItem)
                .where(MediaItem.source_message_id == message.id)
                .order_by(MediaItem.position)
            )
        )
        recovered = await session.get(SourceMessage, message.id)
    assert len(rows) == 2
    assert rows[0].active is False
    assert rows[1].active is True and rows[1].local_deleted_at is None
    assert (root / rows[1].storage_path).read_bytes() == b"restored-photo"
    assert recovered is not None and recovered.media_pending is False
    assert "media_download_error" not in recovered.raw_metadata


async def test_catch_up_records_exhausted_telegram_media_retries_for_review(
    collector_db: tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path],
) -> None:
    maker, source, message, root = collector_db

    class UnexpectedDownloadFailureClient(CollectorClient):
        async def download_media(self, message: TelegramMessage, file: type[bytes]) -> bytes:
            raise ValueError("Telegram exhausted its internal retries")

    client = UnexpectedDownloadFailureClient(TelegramMessage(message.telegram_message_id, b"photo"))
    collector = CollectorService(client, maker, LocalMediaStorage(root))  # type: ignore[arg-type]

    await collector.catch_up()

    async with maker() as session:
        pending = await session.get(SourceMessage, message.id)
        refreshed_source = await session.get(Source, source.id)
    assert pending is not None and pending.media_pending is False
    assert pending.raw_metadata["media_download_error"] == (
        "Telegram exhausted its internal retries"
    )
    assert refreshed_source is not None and refreshed_source.last_catchup_at is not None


async def test_two_media_versions_with_same_edit_second_never_overwrite_original(
    collector_db: tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path],
) -> None:
    maker, source, message, root = collector_db
    first_message = TelegramMessage(message.telegram_message_id, b"first-version")
    collector = CollectorService(
        CollectorClient(first_message),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )
    await collector._download_media(first_message, message.id, source.id)
    async with maker() as session, session.begin():
        await session.execute(
            update(MediaItem).where(MediaItem.source_message_id == message.id).values(active=False)
        )
        row = await session.get(SourceMessage, message.id)
        assert row is not None
        row.media_pending = True
    second_message = TelegramMessage(message.telegram_message_id, b"second-version")
    collector.client = CollectorClient(second_message)  # type: ignore[assignment]
    await collector._download_media(second_message, message.id, source.id)
    async with maker() as session:
        media = list(
            await session.scalars(
                select(MediaItem)
                .where(MediaItem.source_message_id == message.id)
                .order_by(MediaItem.position)
            )
        )
    assert len(media) == 2
    assert media[0].storage_path != media[1].storage_path
    assert (root / media[0].storage_path).read_bytes() == b"first-version"
    assert (root / media[1].storage_path).read_bytes() == b"second-version"


async def test_video_is_persisted_with_real_type_and_extension(
    collector_db: tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path],
) -> None:
    maker, source, message, root = collector_db
    video = TelegramMessage(message.telegram_message_id, b"mp4-video", video=True)
    collector = CollectorService(
        CollectorClient(video),
        maker,
        LocalMediaStorage(root),  # type: ignore[arg-type]
    )

    await collector._download_media(video, message.id, source.id)

    async with maker() as session:
        media = await session.scalar(
            select(MediaItem).where(MediaItem.source_message_id == message.id)
        )
    assert media is not None
    assert media.media_type == "video"
    assert media.storage_path.endswith(".mp4")
    assert (root / media.storage_path).read_bytes() == b"mp4-video"


async def test_catch_up_recovers_downtime_edit_once_and_ignores_stale_rescan(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    original_date = datetime(2026, 8, 24, 9, 0, tzinfo=UTC)
    async with maker() as session, session.begin():
        source = Source(
            name="supplier",
            telegram_channel_id=321,
            enabled=True,
            last_seen_message_id=7,
        )
        session.add(source)
        await session.flush()
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            text="old caption",
            caption="old caption",
            telegram_date=original_date,
            edit_date=original_date,
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-EDIT-1",
            source_id=source.id,
            source_message_id=message.id,
            status=ProductStatus.PROCESSING,
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    telegram_message = TelegramMessage(7, b"")
    telegram_message.message = "new caption"
    telegram_message.date = original_date
    telegram_message.media = None
    telegram_message.post = True
    telegram_message.views = 1
    collector = CollectorService(
        EditedHistoryClient(telegram_message),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    await collector.catch_up()
    async with maker() as session, session.begin():
        persisted = await session.scalar(
            select(SourceMessage).where(SourceMessage.telegram_message_id == 7)
        )
        product = await session.get(Product, product_id)
        versions = list(await session.scalars(select(SourceMessageVersion)))
        assert persisted is not None and persisted.text == "new caption"
        assert product is not None and product.status is ProductStatus.READY
        assert len(versions) == 1
        product.status = ProductStatus.PROCESSING
        product.processing_error = None

    await collector.catch_up()
    async with maker() as session:
        product = await session.get(Product, product_id)
        versions = list(await session.scalars(select(SourceMessageVersion)))
        assert product is not None and product.status is ProductStatus.PROCESSING
        assert len(versions) == 1
    await engine.dispose()


async def test_edit_rescan_does_not_ingest_historical_unseen_messages(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(
            name="newly-configured-source",
            telegram_channel_id=654,
            enabled=True,
            last_seen_message_id=100,
        )
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)

    historical = TelegramMessage(7, b"")
    historical.message = "historical edit must not become a new product"
    historical.date = datetime(2026, 8, 20, 9, 0, tzinfo=UTC)
    historical.media = None
    historical.post = True
    historical.views = 1
    collector = CollectorService(
        EditedHistoryClient(historical),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )

    await collector.catch_up()

    async with maker() as session:
        messages = list(await session.scalars(select(SourceMessage)))
    assert messages == []
    await engine.dispose()


@pytest.mark.parametrize(
    ("published", "policy", "expected"),
    [
        (False, EditPolicy.AUTO, (ProductStatus.READY, False)),
        (True, EditPolicy.AUTO, (ProductStatus.READY, False)),
        (False, EditPolicy.REVIEW, (ProductStatus.READY, True)),
        (True, EditPolicy.REVIEW, (ProductStatus.READY, True)),
        (False, EditPolicy.AUTO_BEFORE_PUBLICATION, (ProductStatus.READY, False)),
        (True, EditPolicy.AUTO_BEFORE_PUBLICATION, (ProductStatus.READY, True)),
        (False, EditPolicy.IGNORE, (None, False)),
        (True, EditPolicy.IGNORE, (None, False)),
    ],
)
def test_edit_reprocess_decision_updates_published_products_in_place(
    published: bool,
    policy: EditPolicy,
    expected: tuple[ProductStatus | None, bool],
) -> None:
    assert edit_reprocess_decision(published=published, policy=policy) == expected


def edited_telegram_message(
    message_id: int,
    text: str,
    *,
    edit_date: datetime,
    grouped_id: int | None = None,
) -> TelegramMessage:
    message = TelegramMessage(message_id, b"")
    message.message = text
    message.date = datetime(2026, 8, 24, 9, 0, tzinfo=UTC)
    message.edit_date = edit_date
    message.grouped_id = grouped_id
    message.media = None
    message.post = True
    message.views = 1
    return message


@pytest.fixture
async def edit_db(
    tmp_path: Path,
) -> tuple[async_sessionmaker[AsyncSession], int, int, int, Path]:
    """One enabled source with a known standalone message and its published product."""
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(
            name="supplier",
            telegram_channel_id=5001,
            enabled=True,
            last_seen_message_id=7,
        )
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            text="old caption",
            caption="old caption",
            telegram_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
            edit_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-EDIT-7",
            source_id=source.id,
            source_message_id=message.id,
            status=ProductStatus.PUBLISHED,
            published_at=datetime(2026, 8, 24, 10, 0, tzinfo=UTC),
        )
        session.add(product)
        await session.flush()
        ids = (source.id, message.id, product.id)
    yield maker, *ids, tmp_path
    await engine.dispose()


async def run_edit(
    collector: CollectorService, channel_id: int, message: TelegramMessage
) -> None:
    event = type("IncomingEvent", (), {"chat_id": channel_id, "message": message})()
    await collector.handle_event(event, edited=True)


async def test_disabled_source_still_ingests_edit_of_known_message(
    edit_db: tuple[async_sessionmaker[AsyncSession], int, int, int, Path],
) -> None:
    maker, source_id, message_id, product_id, root = edit_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        source.enabled = False
    telegram_message = edited_telegram_message(
        7, "edited caption", edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    )
    collector = CollectorService(
        CollectorClient(telegram_message),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )

    await run_edit(collector, 5001, telegram_message)

    async with maker() as session:
        row = await session.get(SourceMessage, message_id)
        versions = list(await session.scalars(select(SourceMessageVersion)))
        product = await session.get(Product, product_id)
    assert row is not None
    assert row.text == "edited caption"
    assert row.caption == "edited caption"
    assert row.edit_date is not None
    assert row.edit_date.replace(tzinfo=UTC) == datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    assert row.event_type == "edit"
    assert len(versions) == 1
    assert product is not None
    assert product.status is ProductStatus.READY
    assert product.processing_error is None


async def test_disabled_source_does_not_intake_unknown_message(
    edit_db: tuple[async_sessionmaker[AsyncSession], int, int, int, Path],
) -> None:
    maker, source_id, _, _, root = edit_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        source.enabled = False
    incoming = IncomingMessage(
        9,
        text="brand new post",
        date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC),
        grouped_id=None,
    )
    collector = CollectorService(
        CollectorClient(incoming),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )

    await persist_incoming(collector, 5001, incoming)

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage).where(SourceMessage.source_id == source_id)
            )
        )
        products = list(await session.scalars(select(Product)))
        persisted_source = await session.get(Source, source_id)
    assert [row.telegram_message_id for row in rows] == [7]
    # Only the pre-existing product of the known message remains.
    assert len(products) == 1
    assert persisted_source is not None and persisted_source.last_seen_message_id == 7


async def test_archived_source_creates_and_changes_nothing(
    edit_db: tuple[async_sessionmaker[AsyncSession], int, int, int, Path],
) -> None:
    maker, source_id, message_id, product_id, root = edit_db
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        assert source is not None
        source.enabled = False
        source.archived_at = datetime(2026, 8, 29, 8, 0, tzinfo=UTC)
    telegram_message = edited_telegram_message(
        7, "edited caption", edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    )
    collector = CollectorService(
        CollectorClient(telegram_message),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )

    await run_edit(collector, 5001, telegram_message)

    async with maker() as session:
        row = await session.get(SourceMessage, message_id)
        versions = list(await session.scalars(select(SourceMessageVersion)))
        product = await session.get(Product, product_id)
        persisted_source = await session.get(Source, source_id)
    assert row is not None and row.text == "old caption"
    assert versions == []
    assert product is not None and product.status is ProductStatus.PUBLISHED
    assert persisted_source is not None and persisted_source.last_seen_message_id == 7


async def test_enabled_source_known_message_edit_reprocesses_product_in_place(
    edit_db: tuple[async_sessionmaker[AsyncSession], int, int, int, Path],
) -> None:
    maker, _, message_id, product_id, root = edit_db
    telegram_message = edited_telegram_message(
        7, "edited caption", edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    )
    collector = CollectorService(
        CollectorClient(telegram_message),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )

    await run_edit(collector, 5001, telegram_message)

    async with maker() as session:
        row = await session.get(SourceMessage, message_id)
        versions = list(await session.scalars(select(SourceMessageVersion)))
        product = await session.get(Product, product_id)
    assert row is not None and row.text == "edited caption"
    assert len(versions) == 1
    assert product is not None
    assert product.status is ProductStatus.READY
    assert product.processing_error is None


async def test_grouped_caption_edit_reopens_processed_album_group(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with maker() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=5002, enabled=True)
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)
        await session.flush()
        group = AlbumGroup(
            source_id=source.id,
            grouped_id=555,
            status=AlbumStatus.PROCESSED,
            last_seen_at=now,
            ready_at=now,
        )
        session.add(group)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            grouped_id=555,
            album_group_id=group.id,
            text="old caption",
            caption="old caption",
            telegram_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
            edit_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-EDIT-8",
            source_id=source.id,
            album_group_id=group.id,
            status=ProductStatus.PUBLISHED,
            published_at=now,
        )
        session.add(product)
        await session.flush()
        group.product_id = product.id
        group_id, product_id = group.id, product.id

    telegram_message = edited_telegram_message(
        7,
        "edited caption",
        edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC),
        grouped_id=555,
    )
    collector = CollectorService(
        CollectorClient(telegram_message),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    await run_edit(collector, 5002, telegram_message)

    async with maker() as session:
        group = await session.get(AlbumGroup, group_id)
        products = list(
            await session.scalars(
                select(Product).where(Product.album_group_id == group_id)
            )
        )
    assert group is not None
    assert group.status is AlbumStatus.COLLECTING
    assert group.ready_at is not None
    assert group.ready_at.replace(tzinfo=UTC) > datetime.now(UTC)
    assert len(products) == 1
    assert products[0].id == product_id
    assert products[0].status is ProductStatus.READY
    assert products[0].processing_error is None
    await engine.dispose()


async def test_grouped_caption_edit_before_product_creation_updates_the_claimed_input(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=5005, enabled=True)
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)

    original = IncomingMessage(
        7,
        text="",
        date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
        grouped_id=556,
        has_media=False,
    )
    collector = CollectorService(
        CollectorClient(original),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    await persist_incoming(collector, 5005, original)

    caption = "Sizes : S M L XL 4 PCS\nPrice : 25 $"
    edited = edited_telegram_message(
        7,
        caption,
        edit_date=datetime(2026, 8, 24, 9, 1, tzinfo=UTC),
        grouped_id=556,
    )
    collector.client = CollectorClient(edited)  # type: ignore[assignment]
    await run_edit(collector, 5005, edited)

    async with maker() as session, session.begin():
        rows = list(await session.scalars(select(SourceMessage)))
        groups = list(await session.scalars(select(AlbumGroup)))
        products = list(await session.scalars(select(Product)))
        versions = list(await session.scalars(select(SourceMessageVersion)))
        assert len(rows) == 1
        assert rows[0].text == caption
        assert rows[0].caption == caption
        assert len(groups) == 1
        assert groups[0].status is AlbumStatus.COLLECTING
        assert products == []
        assert len(versions) == 1
        groups[0].ready_at = datetime.now(UTC) - timedelta(seconds=1)

    work = await ProcessorService(maker, tmp_path, None, None)._claim_work()
    assert work is not None and work[2] == caption
    async with maker() as session:
        products = list(await session.scalars(select(Product)))
    assert len(products) == 1
    await engine.dispose()


async def test_edit_arriving_before_original_event_keeps_one_newest_durable_message(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=5006, enabled=True)
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)

    newest_caption = "Price: 25 USD\nSizes: S M L XL\n4 pcs"
    edit_first = edited_telegram_message(
        7,
        newest_caption,
        edit_date=datetime(2026, 8, 24, 9, 1, tzinfo=UTC),
        grouped_id=557,
    )
    collector = CollectorService(
        CollectorClient(edit_first),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    await run_edit(collector, 5006, edit_first)
    stale_original = IncomingMessage(
        7,
        text="",
        date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
        grouped_id=557,
        has_media=False,
    )
    await persist_incoming(collector, 5006, stale_original)

    async with maker() as session:
        rows = list(await session.scalars(select(SourceMessage)))
        groups = list(await session.scalars(select(AlbumGroup)))
        versions = list(await session.scalars(select(SourceMessageVersion)))
        products = list(await session.scalars(select(Product)))
    assert len(rows) == 1
    assert rows[0].text == newest_caption
    assert rows[0].caption == newest_caption
    assert rows[0].edit_date is not None
    assert len(groups) == 1
    assert versions == []
    assert products == []
    await engine.dispose()


async def test_media_edit_replaces_active_media_without_creating_another_product(
    edit_db: tuple[async_sessionmaker[AsyncSession], int, int, int, Path],
) -> None:
    maker, source_id, message_id, product_id, root = edit_db
    async with maker() as session, session.begin():
        session.add(
            MediaItem(
                source_message_id=message_id,
                telegram_message_id=7,
                position=0,
                media_type="photo",
                storage_path=f"source-{source_id}/message-7/old.jpg",
                size_bytes=3,
                sha256="old",
                active=True,
            )
        )
    edited = edited_telegram_message(
        7,
        "edited caption",
        edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC),
    )
    edited.content = b"new-media-version"
    edited.media = object()
    edited.photo = object()
    collector = CollectorService(
        CollectorClient(edited),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )

    await run_edit(collector, 5001, edited)

    async with maker() as session:
        media = list(
            await session.scalars(
                select(MediaItem)
                .where(MediaItem.source_message_id == message_id)
                .order_by(MediaItem.position)
            )
        )
        products = list(await session.scalars(select(Product)))
        product = await session.get(Product, product_id)
    assert len(media) == 2
    assert media[0].active is False
    assert media[1].active is True
    assert media[1].sha256 != media[0].sha256
    assert (root / media[1].storage_path).read_bytes() == b"new-media-version"
    assert len(products) == 1
    assert product is not None and product.status is ProductStatus.READY


async def test_single_message_edit_reopens_processed_collection_group(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    now = datetime.now(UTC)
    async with maker() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=5004, enabled=True)
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)
        await session.flush()
        # A single message owns a synthetic collection group (negative
        # grouped_id); after publication that group is PROCESSED.
        group = AlbumGroup(
            source_id=source.id,
            grouped_id=-4611686018427387899,
            status=AlbumStatus.PROCESSED,
            last_seen_at=now - timedelta(hours=1),
            ready_at=now - timedelta(hours=1),
        )
        session.add(group)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            grouped_id=None,
            album_group_id=group.id,
            text="old caption",
            caption="old caption",
            telegram_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
            edit_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
        )
        session.add(message)
        await session.flush()
        product = Product(
            emoda_code="EM-EDIT-9",
            source_id=source.id,
            source_message_id=message.id,
            album_group_id=group.id,
            status=ProductStatus.PUBLISHED,
            published_at=now,
        )
        session.add(product)
        await session.flush()
        group.product_id = product.id
        source_id, message_id = source.id, message.id
        group_id, product_id = group.id, product.id

    telegram_message = edited_telegram_message(
        7,
        "edited caption",
        edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC),
        grouped_id=None,
    )
    collector = CollectorService(
        CollectorClient(telegram_message),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )
    await run_edit(collector, 5004, telegram_message)

    async with maker() as session:
        row = await session.get(SourceMessage, message_id)
        group = await session.get(AlbumGroup, group_id)
        products = list(
            await session.scalars(select(Product).where(Product.source_id == source_id))
        )
    assert row is not None
    assert row.text == "edited caption"
    assert len(products) == 1
    assert products[0].id == product_id
    assert products[0].status is ProductStatus.READY
    assert products[0].processing_error is None
    assert group is not None
    assert group.status is AlbumStatus.COLLECTING
    assert group.ready_at is not None
    assert group.ready_at.replace(tzinfo=UTC) > datetime.now(UTC)

    # The processor claims a READY product only while its group is READY, so
    # the reopened group must become claimable once the debounce has passed.
    async with maker() as session, session.begin():
        reopened = await session.get(AlbumGroup, group_id)
        assert reopened is not None and reopened.ready_at is not None
        claim_now = reopened.ready_at.replace(tzinfo=UTC) + timedelta(seconds=1)
        claimed = await AlbumCoordinator(Decimal("1.8")).claim_ready(
            session, now=claim_now, limit=10
        )
        assert [item.id for item in claimed] == [group_id]
    async with maker() as session:
        group = await session.get(AlbumGroup, group_id)
    assert group is not None
    assert group.status is AlbumStatus.READY
    await engine.dispose()


async def test_identical_edit_replay_is_applied_once(
    edit_db: tuple[async_sessionmaker[AsyncSession], int, int, int, Path],
) -> None:
    maker, source_id, message_id, product_id, root = edit_db
    telegram_message = edited_telegram_message(
        7, "edited caption", edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    )
    collector = CollectorService(
        CollectorClient(telegram_message),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )
    await run_edit(collector, 5001, telegram_message)
    async with maker() as session, session.begin():
        product = await session.get(Product, product_id)
        assert product is not None
        product.status = ProductStatus.PROCESSING
        product.processing_error = None

    await run_edit(collector, 5001, telegram_message)

    async with maker() as session:
        versions = list(await session.scalars(select(SourceMessageVersion)))
        product = await session.get(Product, product_id)
        duplicates = list(
            await session.scalars(
                select(MessageEventTransition).where(
                    MessageEventTransition.source_message_id == message_id,
                    MessageEventTransition.state == MessageState.DUPLICATE,
                )
            )
        )
    assert len(versions) == 1
    assert product is not None and product.status is ProductStatus.PROCESSING
    assert len(duplicates) == 1


async def test_out_of_order_edit_does_not_overwrite_newer_text(
    edit_db: tuple[async_sessionmaker[AsyncSession], int, int, int, Path],
) -> None:
    maker, _, message_id, product_id, root = edit_db
    async with maker() as session, session.begin():
        row = await session.get(SourceMessage, message_id)
        assert row is not None
        row.text = "newer caption"
        row.caption = "newer caption"
        row.edit_date = datetime(2026, 8, 30, 9, 0, tzinfo=UTC)
    stale = edited_telegram_message(
        7, "older caption", edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    )
    collector = CollectorService(
        CollectorClient(stale),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(root),
    )

    await run_edit(collector, 5001, stale)

    async with maker() as session:
        row = await session.get(SourceMessage, message_id)
        versions = list(await session.scalars(select(SourceMessageVersion)))
        product = await session.get(Product, product_id)
    assert row is not None and row.text == "newer caption"
    assert versions == []
    assert product is not None and product.status is ProductStatus.PUBLISHED


async def test_catch_up_replays_known_edits_for_disabled_source(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(
            name="paused supplier",
            telegram_channel_id=5003,
            enabled=False,
            last_seen_message_id=7,
        )
        source.profile = SourceProfile(
            config=SourceProfileData(translation_enabled=False).model_dump(mode="json")
        )
        session.add(source)
        await session.flush()
        session.add(
            SourceMessage(
                source_id=source.id,
                telegram_message_id=7,
                text="old caption",
                caption="old caption",
                telegram_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
                edit_date=datetime(2026, 8, 24, 9, 0, tzinfo=UTC),
            )
        )
        source_id = source.id

    known = edited_telegram_message(
        7, "edited caption", edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    )
    unknown = edited_telegram_message(
        8, "must not appear", edit_date=datetime(2026, 8, 29, 9, 0, tzinfo=UTC)
    )
    collector = CollectorService(
        CatchupEditClient([known, unknown]),  # type: ignore[arg-type]
        maker,
        LocalMediaStorage(tmp_path),
    )

    await collector.catch_up()

    async with maker() as session:
        rows = list(
            await session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        versions = list(await session.scalars(select(SourceMessageVersion)))
        persisted_source = await session.get(Source, source_id)
    assert [row.telegram_message_id for row in rows] == [7]
    assert rows[0].text == "edited caption"
    assert len(versions) == 1
    assert persisted_source is not None and persisted_source.last_catchup_at is not None
    await engine.dispose()

async def _make_concurrent_media_db(
    tmp_path: Path,
    *,
    edit_date: datetime,
    sqlite_lock_timeout: float = 0.2,
) -> tuple[Any, async_sessionmaker[AsyncSession], int, int]:
    engine = create_async_engine(
        f"sqlite+aiosqlite:///{tmp_path / 'concurrent-media.db'}",
        connect_args={"timeout": sqlite_lock_timeout},
    )
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=123, enabled=True)
        session.add(source)
        await session.flush()
        message = SourceMessage(
            source_id=source.id,
            telegram_message_id=7,
            text="caption",
            telegram_date=edit_date,
            edit_date=edit_date,
            media_pending=True,
        )
        session.add(message)
        await session.flush()
        return engine, maker, source.id, message.id


class PausedMediaClient(CollectorClient):
    def __init__(self, message: TelegramMessage) -> None:
        super().__init__(message)
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def download_media(self, message: TelegramMessage, file: type[bytes]) -> bytes:
        self.started.set()
        await self.release.wait()
        return await super().download_media(message, file)


async def test_slow_media_download_releases_database_transaction(tmp_path: Path) -> None:
    telegram_message = TelegramMessage(7, b"photo-one")
    engine, maker, source_id, message_id = await _make_concurrent_media_db(
        tmp_path, edit_date=telegram_message.edit_date
    )
    client = PausedMediaClient(telegram_message)
    collector = CollectorService(client, maker, LocalMediaStorage(tmp_path))  # type: ignore[arg-type]
    task = asyncio.create_task(collector._download_media(telegram_message, message_id, source_id))
    try:
        await asyncio.wait_for(client.started.wait(), timeout=2)

        async def update_during_download() -> None:
            async with maker() as session, session.begin():
                row = await session.get(SourceMessage, message_id)
                assert row is not None
                row.text = "changed while Telegram waits"

        # An open write transaction in _download_media would block this commit.
        await asyncio.wait_for(update_during_download(), timeout=2)
    finally:
        client.release.set()
        await asyncio.wait_for(task, timeout=5)
        await engine.dispose()


async def test_stale_media_download_does_not_finish_newer_edit(tmp_path: Path) -> None:
    old_message = TelegramMessage(7, b"old-photo")
    engine, maker, source_id, message_id = await _make_concurrent_media_db(
        tmp_path, edit_date=old_message.edit_date
    )
    client = PausedMediaClient(old_message)
    collector = CollectorService(client, maker, LocalMediaStorage(tmp_path))  # type: ignore[arg-type]
    old_task = asyncio.create_task(collector._download_media(old_message, message_id, source_id))
    try:
        await asyncio.wait_for(client.started.wait(), timeout=2)
        newer_edit_date = old_message.edit_date + timedelta(minutes=1)

        async def persist_newer_edit() -> None:
            async with maker() as session, session.begin():
                row = await session.get(SourceMessage, message_id)
                assert row is not None
                row.edit_date = newer_edit_date
                row.media_pending = True

        await asyncio.wait_for(persist_newer_edit(), timeout=2)
        client.release.set()
        await asyncio.wait_for(old_task, timeout=5)
        async with maker() as session:
            row = await session.get(SourceMessage, message_id)
            old_media = list(
                await session.scalars(
                    select(MediaItem).where(MediaItem.source_message_id == message_id)
                )
            )
            assert row is not None and row.media_pending is True
            assert old_media == []

        new_message = TelegramMessage(7, b"new-photo")
        new_message.edit_date = newer_edit_date
        collector.client = CollectorClient(new_message)  # type: ignore[assignment]
        await collector._download_media(new_message, message_id, source_id)
        async with maker() as session:
            row = await session.get(SourceMessage, message_id)
            media = list(
                await session.scalars(
                    select(MediaItem).where(MediaItem.source_message_id == message_id)
                )
            )
            assert row is not None and row.media_pending is False
            assert len(media) == 1
            assert (tmp_path / media[0].storage_path).read_bytes() == b"new-photo"
    finally:
        client.release.set()
        if not old_task.done():
            await asyncio.wait_for(old_task, timeout=5)
        await engine.dispose()


async def test_media_downloads_are_bounded_before_loading_bytes(tmp_path: Path) -> None:
    first = TelegramMessage(7, b"photo")
    engine, maker, source_id, first_id = await _make_concurrent_media_db(
        tmp_path, edit_date=first.edit_date, sqlite_lock_timeout=30
    )
    message_ids = [first_id]
    async with maker() as session, session.begin():
        for telegram_id in range(8, 14):
            row = SourceMessage(
                source_id=source_id,
                telegram_message_id=telegram_id,
                text="caption",
                telegram_date=first.edit_date,
                edit_date=first.edit_date,
                media_pending=True,
            )
            session.add(row)
            await session.flush()
            message_ids.append(row.id)

    class CountingClient(CollectorClient):
        def __init__(self) -> None:
            super().__init__(first)
            self.active = 0
            self.peak = 0
            self.six_started = asyncio.Event()
            self.release = asyncio.Event()

        async def download_media(self, message: TelegramMessage, file: type[bytes]) -> bytes:
            self.active += 1
            self.peak = max(self.peak, self.active)
            if self.active == 6:
                self.six_started.set()
            try:
                await self.release.wait()
                return message.content
            finally:
                self.active -= 1

    client = CountingClient()
    collector = CollectorService(client, maker, LocalMediaStorage(tmp_path))  # type: ignore[arg-type]
    tasks = [
        asyncio.create_task(
            collector._download_media(TelegramMessage(telegram_id, b"photo"), row_id, source_id)
        )
        for telegram_id, row_id in zip(range(7, 14), message_ids, strict=True)
    ]
    try:
        await asyncio.wait_for(client.six_started.wait(), timeout=5)
        await asyncio.sleep(0.1)
        assert client.active == 6
        assert client.peak == 6
        async with maker() as session:
            pending = list(
                await session.scalars(
                    select(SourceMessage.media_pending).where(SourceMessage.id.in_(message_ids))
                )
            )
            assert pending == [True] * 7
    finally:
        client.release.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=10)
        await engine.dispose()


async def test_successful_media_download_clears_recovery_error_markers(
    collector_db: tuple[async_sessionmaker[AsyncSession], Source, SourceMessage, Path],
) -> None:
    maker, source, message, root = collector_db
    async with maker() as session, session.begin():
        row = await session.get(SourceMessage, message.id)
        assert row is not None
        row.raw_metadata = {
            "media_download_error": "old failure",
            "media_recovery_missing_count": 3,
            "media_recovery_next_at": "2026-09-25T10:00:00+00:00",
            "media_recovery_terminal_at": "2026-09-25T11:00:00+00:00",
            "keep": "untouched",
        }
    telegram_message = TelegramMessage(message.telegram_message_id, b"recovered-photo")
    collector = CollectorService(
        CollectorClient(telegram_message), maker, LocalMediaStorage(root)  # type: ignore[arg-type]
    )
    await collector._download_media(telegram_message, message.id, source.id)

    async with maker() as session:
        row = await session.get(SourceMessage, message.id)
        assert row is not None
        assert row.media_pending is False
        assert row.raw_metadata == {"keep": "untouched"}


async def test_first_edit_hydrates_entire_unknown_album(adjacent_media_db):
    maker, source_id, _, root = adjacent_media_db
    messages = [IncomingMessage(6100+i, text='18 $ S M L 3 pcs' if i == 0 else '',
                 date=datetime.now(UTC), grouped_id=9100) for i in range(9)]
    messages[-1].edit_date = datetime.now(UTC)
    collector = CollectorService(AlbumHydrationClient(messages), maker, LocalMediaStorage(root))
    await collector.handle_event(type('Event', (), {'chat_id':999,'message':messages[-1]})(), edited=True)
    async with maker() as session:
        rows = list(await session.scalars(select(SourceMessage).where(SourceMessage.source_id==source_id)))
        media = list(await session.scalars(select(MediaItem)))
    assert len(rows) == 9
    assert len({m.album_group_id for m in rows}) == 1
    assert len(media) == 9


async def test_transient_intake_failure_is_recoverable_not_terminal(adjacent_media_db, monkeypatch):
    from sqlalchemy.exc import TimeoutError as PoolTimeout
    maker, source_id, _, root = adjacent_media_db
    message = IncomingMessage(6200, text='18 $ S M L 3 pcs', date=datetime.now(UTC), grouped_id=None)
    collector = CollectorService(AlbumHydrationClient([message]), maker, LocalMediaStorage(root))
    original = collector.persist_event
    async def failing(*a, **kw): raise PoolTimeout('pool exhausted')
    monkeypatch.setattr(collector, 'persist_event', failing)
    await collector.handle_event(type('Event', (), {'chat_id':999,'message':message})(), edited=False)
    async with maker() as session:
        row = await session.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id==6200))
        assert row.processed_at is None
        assert row.media_pending is True
        assert row.raw_metadata['intake_retry_pending'] is True
    monkeypatch.setattr(collector, 'persist_event', original)
    await collector.handle_event(type('Event', (), {'chat_id':999,'message':message})(), edited=False)
    async with maker() as session:
        row = await session.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id==6200))
        assert not row.raw_metadata.get('intake_retry_pending')
        assert row.media_pending is False
        assert await session.scalar(select(MediaItem.id).where(MediaItem.source_message_id==row.id))


async def test_history_sweep_recovers_hole_older_than_edit_window(adjacent_media_db):
    maker, source_id, _, root = adjacent_media_db
    messages = [IncomingMessage(6300+i, text=f'item {i}', date=datetime.now(UTC),
                    grouped_id=None, has_media=False) for i in range(50)]
    collector = CollectorService(GapHistoryClient(messages), maker, LocalMediaStorage(root), edit_rescan_limit=20)
    async with maker() as session, session.begin():
        source = await session.get(Source, source_id)
        source.catchup_anchor_message_id=6299
        source.last_seen_message_id=6349
    await collector.catch_up()
    async with maker() as session:
        ids=set(await session.scalars(select(SourceMessage.telegram_message_id).where(SourceMessage.source_id==source_id)))
    assert 6300 in ids
    assert len(ids)==50


async def test_hydration_failure_keeps_every_known_member_pending(adjacent_media_db):
    maker, sid, _, root = adjacent_media_db
    messages=[IncomingMessage(8100+i,text='20 USD' if i==0 else '',date=datetime.now(UTC),grouped_id=888) for i in range(3)]
    class Client(AlbumHydrationClient):
        async def get_messages(self, channel_id, ids): raise OSError('history unavailable')
    collector=CollectorService(Client(messages),maker,LocalMediaStorage(root))
    await collector._new_album(type('Event',(),{'chat_id':999,'messages':messages})())
    async with maker() as session:
        rows=list(await session.scalars(select(SourceMessage).where(SourceMessage.source_id==sid)))
        assert len(rows)==3
        assert all(row.processed_at is None and row.media_pending and row.raw_metadata.get('intake_retry_pending') for row in rows)
    assert await ProcessorService(maker,root,None,None)._claim_work() is None

async def test_repeated_transient_failure_advances_durable_backoff(adjacent_media_db):
    from sqlalchemy.exc import TimeoutError as PoolTimeout
    maker, _, _, root=adjacent_media_db
    message=IncomingMessage(6800,text='item',date=datetime.now(UTC),grouped_id=None)
    collector=CollectorService(AlbumHydrationClient([message]),maker,LocalMediaStorage(root))
    event=type('Event',(),{'chat_id':999,'message':message})()
    for _ in range(2):
        await collector._persist_failure(event,edited=False,error=PoolTimeout('pool busy'),origin=EventOrigin.LIVE,retryable=True)
    async with maker() as session:
        row=await session.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id==6800))
        assert row.raw_metadata['intake_retry_attempts']==2
        assert row.processed_at is None and row.media_pending

async def test_download_guard_excludes_link_preview(adjacent_media_db, monkeypatch):
    from telethon.tl.types import MessageMediaWebPage, WebPageEmpty
    maker, sid, _, root=adjacent_media_db
    collector=CollectorService(AlbumHydrationClient([]),maker,LocalMediaStorage(root))
    async def unexpected(*args):raise AssertionError('link preview must not enter media downloader')
    monkeypatch.setattr(collector,'_download_media_with_slot',unexpected)
    message=type('Preview',(),{'media':MessageMediaWebPage(webpage=WebPageEmpty(id=1)),'photo':object(),'video':None})()
    await collector._download_media(message,1,sid)

async def test_deleted_intake_message_gets_bounded_missing_media_recovery(adjacent_media_db):
    from sqlalchemy.exc import TimeoutError as PoolTimeout
    maker, _, _, root=adjacent_media_db
    class Missing:
        async def get_messages(self,*args,**kwargs):return None
    collector=CollectorService(Missing(),maker,LocalMediaStorage(root))
    m=IncomingMessage(6900,text='lost source item',date=datetime.now(UTC)-timedelta(days=2),grouped_id=None)
    await collector._persist_failure(type('Event',(),{'chat_id':999,'message':m})(),edited=False,error=PoolTimeout('busy'),origin=EventOrigin.LIVE,retryable=True)
    for _ in range(3):
        async with maker() as s,s.begin():
            row=await s.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id==6900))
            past=(datetime.now(UTC)-timedelta(minutes=1)).isoformat()
            row.raw_metadata={**row.raw_metadata,'intake_retry_after':past,'media_recovery_next_at':past}
        await collector._retry_intake()
    async with maker() as s:
        row=await s.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id==6900))
        assert not row.media_pending
        assert not row.raw_metadata.get('intake_retry_pending')
        assert 'no longer accessible' in row.raw_metadata['media_download_error']

async def test_empty_cached_media_is_downloaded_again(adjacent_media_db):
    maker,sid,_,root=adjacent_media_db
    m=IncomingMessage(6950,text='item',date=datetime.now(UTC),grouped_id=None)
    collector=CollectorService(AlbumHydrationClient([m]),maker,LocalMediaStorage(root))
    await collector.handle_event(type('Event',(),{'chat_id':999,'message':m})(),edited=False)
    async with maker() as s:
        row=await s.scalar(select(SourceMessage).where(SourceMessage.telegram_message_id==6950))
        media=await s.scalar(select(MediaItem).where(MediaItem.source_message_id==row.id,MediaItem.active.is_(True)))
    path=root/media.storage_path
    path.write_bytes(b'')
    await collector._download_media(m,row.id,sid)
    assert path.stat().st_size>0
