import asyncio
import re
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin import router as admin_router
from emoda.admin.context import current_admin_id
from emoda.db.base import Base
from emoda.db.models import AdminTestItem, AdminUser, Source, SourceProfile
from emoda.domain.enums import PublicationMode
from emoda.domain.schemas import (
    CollectionBoundaryRule,
    CollectionProfile,
    ComputedRule,
    ExtractionRule,
    MarkupRange,
    SourceProfileData,
)
from emoda.services.collection import CollectionEngine, CollectionMessage
from emoda.services.orchestrator import ProcessingMode, process_product
from emoda.services.pipeline import Pipeline, PipelineInput


def _part(
    message_id: int,
    *,
    text: str = "",
    media_paths: list[str] | None = None,
    media_group_id: str | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        telegram_message_id=message_id,
        text=text,
        media_paths=media_paths or [],
        media_group_id=media_group_id,
    )


def _x_top_profile() -> SourceProfileData:
    return SourceProfileData(
        default_currency="USD",
        gemini_enabled=False,
        extraction_rules={
            "price": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?i)\bPrice\s*:\s*(?P<value>\d+(?:[.,]\d+)?)\s*\$",
                    priority=10,
                    authoritative=True,
                )
            ],
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=(
                        r"(?i)\bSizes\s*:\s*(?P<value>(?:XS|XXL|XL|L|M|S)"
                        r"(?:\s+(?:XS|XXL|XL|L|M|S)))\s+\d+\spcs\b"
                    ),
                    priority=10,
                    authoritative=True,
                )
            ],
            "quantity": [
                ExtractionRule(
                    kind="regex",
                    pattern=(
                        r"(?i)\bSizes\s*:\s*(?:XS|XXL|XL|L|M|S)"
                        r"(?:\s+(?:XS|XXL|XL|L|M|S))\s+(?P<value>\d+)\spcs\b"
                    ),
                    priority=10,
                    authoritative=True,
                )
            ],
        },
    )


X_TOP_TEXT = "Sizes  : S M L XL  4 pcs\nPrice   :  25 $"
X_TOP_STANDART_TEXT = "Sizes  : STANDART 4PCS\nPrice   :  27 $"


async def _x_top_result(text: str, *, media_paths: list[str], dry: bool = True):
    return await process_product(
        Pipeline(None),
        PipelineInput(
            source_id=7,
            text=text,
            media_paths=media_paths,
            emoda_code="EM-TEST",
            profile=_x_top_profile(),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("21"),
                    max_price=Decimal("30.99"),
                    markup=Decimal("5"),
                    priority=1,
                )
            ],
        ),
        ProcessingMode.DRY if dry else ProcessingMode.LIVE,
    )


@pytest.mark.asyncio
async def test_plain_and_album_caption_share_deterministic_x_top_fields() -> None:
    profile = _x_top_profile()
    collection = CollectionEngine(profile.effective_collection())
    album_messages = [
        CollectionMessage(
            message_id=100,
            occurred_at=datetime(2026, 8, 28, tzinfo=UTC),
            grouped_id="album",
            media_count=1,
        ),
        CollectionMessage(
            message_id=101,
            occurred_at=datetime(2026, 8, 28, tzinfo=UTC),
            grouped_id="album",
            caption=X_TOP_TEXT,
            media_count=1,
        ),
    ]
    canonical = collection.merged_text(album_messages)

    plain = await _x_top_result(X_TOP_TEXT, media_paths=[])
    album = await _x_top_result(canonical, media_paths=["1.jpg", "2.jpg"])

    assert canonical == X_TOP_TEXT
    assert canonical.count(X_TOP_TEXT) == 1
    for result in (plain, album):
        assert result.parsed.fields["price"].value == "25"
        assert result.parsed.fields["sizes"].value == "S M L XL"
        assert result.parsed.fields["quantity"].value == "4"
        assert result.price is not None and result.price.final_usd == Decimal("30")
    assert plain.rendered_text == album.rendered_text


@pytest.mark.asyncio
async def test_standart_caption_falls_back_from_configured_nonmatch_to_global_strict_rules() -> None:
    result = await _x_top_result(X_TOP_STANDART_TEXT, media_paths=[])

    assert result.parsed.fields["price"].value == "27"
    assert result.parsed.fields["sizes"].value == "STANDART"
    assert result.parsed.fields["quantity"].value == "4"
    assert result.parsed.fields["price"].provenance.value == "source_rule"
    assert result.parsed.fields["sizes"].provenance.value == "strict_text"
    assert result.parsed.fields["quantity"].provenance.value == "strict_text"


@pytest.mark.asyncio
async def test_standart_caption_uses_configured_generic_source_rules() -> None:
    profile = _x_top_profile()
    profile.extraction_rules["sizes"] = [
        ExtractionRule(
            kind="regex",
            pattern=r"(?i)Sizes\s*:\s*(?P<value>STANDART)",
            priority=10,
            authoritative=True,
        )
    ]
    profile.extraction_rules["quantity"] = [
        ExtractionRule(
            kind="regex",
            pattern=r"(?i)Sizes\s*:\s*STANDART\s*(?P<value>\d+)\s*PCS",
            priority=10,
            authoritative=True,
        )
    ]

    result = await process_product(
        Pipeline(None),
        PipelineInput(
            source_id=7,
            text=X_TOP_STANDART_TEXT,
            media_paths=[],
            emoda_code="EM-CONFIGURED-STANDART",
            profile=profile,
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("21"),
                    max_price=Decimal("30.99"),
                    markup=Decimal("5"),
                    priority=1,
                )
            ],
        ),
        ProcessingMode.DRY,
    )

    assert result.parsed.fields["sizes"].value == "STANDART"
    assert result.parsed.fields["quantity"].value == "4"
    assert result.parsed.fields["sizes"].provenance.value == "source_rule"
    assert result.parsed.fields["quantity"].provenance.value == "source_rule"


@pytest.mark.asyncio
async def test_x_top_album_caption_has_exact_dry_live_parity() -> None:
    dry = await _x_top_result(X_TOP_TEXT, media_paths=["album.jpg"], dry=True)
    live = await _x_top_result(X_TOP_TEXT, media_paths=["album.jpg"], dry=False)

    assert dry.model_dump(exclude={"publication_requested"}) == live.model_dump(
        exclude={"publication_requested"}
    )
    assert dry.publication_requested is False
    assert live.publication_requested is True


class _DownloadBot:
    async def download(self, media: object, *, destination: Path) -> None:
        await asyncio.to_thread(destination.write_bytes, b"video")


class _TestState:
    def __init__(self, batch_id: int) -> None:
        self.batch_id = batch_id

    async def get_data(self) -> dict[str, int]:
        return {"test_batch_id": self.batch_id}


class _AlbumMessage:
    def __init__(
        self,
        message_id: int,
        answers: list[tuple[str, object | None]],
        *,
        caption: str = "",
        media_group_id: str = "sobe-album",
    ) -> None:
        self.chat = SimpleNamespace(id=777)
        self.message_id = message_id
        self.text = None
        self.caption = caption
        self.photo = [object()]
        self.video = None
        self.media_group_id = media_group_id
        self.bot = _DownloadBot()
        self.answers = answers

    async def answer(self, text: str, *, reply_markup: object | None = None) -> None:
        self.answers.append((text, reply_markup))


@pytest.mark.asyncio
async def test_album_parts_emit_one_ready_to_process_message_after_quiet_period(
    tmp_path: Path,
) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        admin = AdminUser(telegram_user_id=777, display_name="Admin", enabled=True)
        source = Source(name="Strict grouped source", telegram_channel_id=-1777, enabled=False)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData().model_dump(mode="json"),
        )
        session.add_all([admin, source])
        await session.flush()
        batch = await admin_router.start_or_resume_test_batch(session, source.id, admin.id)
        admin_id = admin.id
        batch_id = batch.id

    async def access_validator(_: str) -> tuple[int, str, int]:
        return -1, "unused", 0

    async def test_message_loader(_: str) -> tuple[str, list[str]]:
        return "", []

    router = admin_router.create_admin_router(
        sessions,
        {777},
        access_validator=access_validator,
        test_message_loader=test_message_loader,
        intelligence=None,
        media_root=tmp_path,
    )
    collect = next(
        handler.callback
        for handler in router.message.handlers
        if handler.callback.__name__ == "test_collect"
    )
    answers: list[tuple[str, object | None]] = []
    state = _TestState(batch_id)
    token = current_admin_id.set(admin_id)
    try:
        for message_id in (100, 101, 102):
            await collect(
                _AlbumMessage(
                    message_id,
                    answers,
                    caption="Island Glow Dress 18.5 $" if message_id == 100 else "",
                ),
                state,
            )
        assert answers == []
        await asyncio.sleep(2.2)
    finally:
        current_admin_id.reset(token)

    assert len(answers) == 1
    text, markup = answers[0]
    assert "3 сообщений, 3 media" in text
    assert markup is not None
    callbacks = [
        button.callback_data
        for row in markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    ]
    assert callbacks == [f"test:process:{batch_id}", f"test:cancel:{batch_id}"]
    async with sessions() as session:
        assert await session.scalar(select(func.count()).select_from(AdminTestItem)) == 3
    await engine.dispose()


@pytest.mark.asyncio
async def test_second_grouped_id_is_accepted_and_keeps_process_controls(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        admin = AdminUser(telegram_user_id=778, display_name="Admin", enabled=True)
        source = Source(name="Strict grouped source", telegram_channel_id=-1778, enabled=False)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData().model_dump(mode="json"),
        )
        session.add_all([admin, source])
        await session.flush()
        batch = await admin_router.start_or_resume_test_batch(session, source.id, admin.id)
        await admin_router.add_test_batch_item(
            session,
            batch_id=batch.id,
            chat_id=778,
            message_id=1,
            media_group_id="first-group",
            text="Product A",
            media_paths=["first.jpg"],
        )
        admin_id = admin.id
        batch_id = batch.id

    async def access_validator(_: str) -> tuple[int, str, int]:
        return -1, "unused", 0

    async def test_message_loader(_: str) -> tuple[str, list[str]]:
        return "", []

    router = admin_router.create_admin_router(
        sessions,
        {778},
        access_validator=access_validator,
        test_message_loader=test_message_loader,
        intelligence=None,
        media_root=tmp_path,
    )
    collect = next(
        handler.callback
        for handler in router.message.handlers
        if handler.callback.__name__ == "test_collect"
    )
    answers: list[tuple[str, object | None]] = []
    token = current_admin_id.set(admin_id)
    try:
        await collect(
            _AlbumMessage(2, answers, media_group_id="second-group"),
            _TestState(batch_id),
        )
        await asyncio.sleep(2.2)
    finally:
        current_admin_id.reset(token)

    assert len(answers) == 1
    text, markup = answers[0]
    assert "Добавлено" in text
    assert "2 сообщений" in text
    assert markup is not None
    callbacks = [
        button.callback_data
        for row in markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    ]
    assert callbacks == [f"test:process:{batch_id}", f"test:cancel:{batch_id}"]
    await engine.dispose()


@pytest.mark.asyncio
async def test_admin_test_video_is_persisted_as_pipeline_media(tmp_path: Path) -> None:
    download = getattr(admin_router, "download_admin_test_media", None)
    assert download is not None
    message = SimpleNamespace(
        chat=SimpleNamespace(id=91),
        message_id=17,
        photo=None,
        video=object(),
        bot=_DownloadBot(),
    )

    paths = await download(message, tmp_path)

    assert len(paths) == 1
    assert paths[0].endswith(".mp4")
    assert await asyncio.to_thread(Path(paths[0]).read_bytes) == b"video"


def test_manual_telegram_album_is_aggregated_into_one_dry_run() -> None:
    partition = getattr(admin_router, "partition_test_inputs", None)
    assert partition is not None

    products = partition(
        [
            _part(
                10,
                text="Nike AB-125\nЦена: $10",
                media_paths=["1.jpg"],
                media_group_id="album-1",
            ),
            _part(11, media_paths=["2.jpg"], media_group_id="album-1"),
            _part(12, media_paths=["3.jpg"], media_group_id="album-1"),
            _part(13, media_paths=["4.jpg"], media_group_id="album-1"),
        ]
    )

    assert products == [("Nike AB-125\nЦена: $10", ["1.jpg", "2.jpg", "3.jpg", "4.jpg"])]


def test_distinct_telegram_albums_partition_into_separate_dry_run_products() -> None:
    products = admin_router.partition_test_inputs(
        [
            _part(
                20,
                text="Sunset Stripe Jacket 18 $ (6199)",
                media_paths=["a1.jpg"],
                media_group_id="album-a",
            ),
            _part(21, media_paths=["a2.jpg"], media_group_id="album-a"),
            _part(
                30,
                text="Island Glow Dress 18.5 $ (2052)",
                media_paths=["b1.jpg"],
                media_group_id="album-b",
            ),
        ]
    )

    assert products == [
        ("Sunset Stripe Jacket 18 $ (6199)", ["a1.jpg", "a2.jpg"]),
        ("Island Glow Dress 18.5 $ (2052)", ["b1.jpg"]),
    ]


def test_opted_in_source_aggregates_multiple_telegram_groups_in_one_dry_run() -> None:
    profile = SourceProfileData(
        product_collection_mode="text_before_media",
        product_collection_window_seconds=Decimal("180"),
    )

    products = admin_router.partition_test_inputs(
        [
            _part(20, text="Article: M1\nPrice: 27 USD"),
            _part(21, media_paths=["a1.jpg"], media_group_id="album-a"),
            _part(22, media_paths=["a2.jpg"], media_group_id="album-a"),
            _part(23, media_paths=["b1.jpg"], media_group_id="album-b"),
        ],
        profile=profile,
    )

    assert products == [("Article: M1\nPrice: 27 USD", ["a1.jpg", "a2.jpg", "b1.jpg"])]


def test_media_before_text_aggregates_one_product_across_multiple_groups() -> None:
    profile = SourceProfileData(
        product_collection_mode="media_before_text",
        product_collection_window_seconds=Decimal("180"),
    )

    products = admin_router.partition_test_inputs(
        [
            _part(30, media_paths=["a1.jpg"], media_group_id="album-a"),
            _part(31, media_paths=["a2.jpg"], media_group_id="album-a"),
            _part(32, media_paths=["b1.jpg"], media_group_id="album-b"),
            _part(33, text="#LUFIAN\nSIZE: S M L\nFIYAT: 13$"),
        ],
        profile=profile,
    )

    assert products == [("#LUFIAN\nSIZE: S M L\nFIYAT: 13$", ["a1.jpg", "a2.jpg", "b1.jpg"])]


def test_media_before_text_partitions_multiple_product_cycles() -> None:
    profile = SourceProfileData(
        product_collection_mode="media_before_text",
        product_collection_window_seconds=Decimal("180"),
    )

    products = admin_router.partition_test_inputs(
        [
            _part(40, media_paths=["l1.jpg"], media_group_id="lufian-a"),
            _part(41, media_paths=["l2.jpg"], media_group_id="lufian-b"),
            _part(42, text="#LUFIAN\nSIZE: S M L\nFIYAT: 13$"),
            _part(43, media_paths=["c1.jpg"], media_group_id="calvin-a"),
            _part(44, media_paths=["c2.jpg"], media_group_id="calvin-b"),
            _part(45, text="#CALVIN KLEIN\nSIZE: S M L\nFIYAT: 12.5$"),
            _part(46, media_paths=["p1.jpg"], media_group_id="prada-a"),
            _part(47, text="#PRADA\nSIZE: S M L\nFIYAT: 13$"),
        ],
        profile=profile,
    )

    assert len(products) == 3
    assert products[0] == ("#LUFIAN\nSIZE: S M L\nFIYAT: 13$", ["l1.jpg", "l2.jpg"])
    assert products[1] == ("#CALVIN KLEIN\nSIZE: S M L\nFIYAT: 12.5$", ["c1.jpg", "c2.jpg"])
    assert products[2] == ("#PRADA\nSIZE: S M L\nFIYAT: 13$", ["p1.jpg"])


def test_album_start_text_continue_next_album_partitions_three_real_products() -> None:
    profile = SourceProfileData(
        collection=CollectionProfile(
            strategy="multi_grouped_id",
            order="any",
            merge_neighbor_grouped_ids=True,
            max_time_gap_seconds=Decimal("90"),
            allow_multiple_text_blocks=True,
            merge_text_blocks=True,
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
        )
    )
    products = admin_router.partition_test_inputs(
        [
            _part(100, media_paths=["a1.jpg"], media_group_id="album-a"),
            _part(101, text="A100", media_paths=["a2.jpg"], media_group_id="album-a"),
            _part(102, media_paths=["a3.jpg"], media_group_id="album-a"),
            _part(103, text="27 $"),
            _part(104, text="Sizes S M L\nQuantity 3"),
            _part(105, media_paths=["b1.jpg"], media_group_id="album-b"),
            _part(106, text="B200", media_paths=["b2.jpg"], media_group_id="album-b"),
            _part(107, text="28 $"),
            _part(108, text="Sizes M L XL\nQuantity 3"),
            _part(109, media_paths=["c1.jpg"], media_group_id="album-c"),
            _part(110, text="C300", media_paths=["c2.jpg"], media_group_id="album-c"),
            _part(111, text="32 $"),
            _part(112, text="Sizes L XL XXL\nQuantity 3"),
        ],
        profile=profile,
    )

    assert products == [
        ("A100\n27 $\nSizes S M L\nQuantity 3", ["a1.jpg", "a2.jpg", "a3.jpg"]),
        ("B200\n28 $\nSizes M L XL\nQuantity 3", ["b1.jpg", "b2.jpg"]),
        ("C300\n32 $\nSizes L XL XXL\nQuantity 3", ["c1.jpg", "c2.jpg"]),
    ]


def test_same_grouped_id_is_atomic_when_caption_is_in_middle_of_album() -> None:
    engine = CollectionEngine(
        CollectionProfile(
            strategy="single_grouped_id",
            order="media_before_text",
            max_media=20,
        )
    )
    base = datetime(2026, 8, 31, tzinfo=UTC)
    partitioned = engine.partition(
        [
            CollectionMessage(1, base, grouped_id="album", media_count=1),
            CollectionMessage(2, base, grouped_id="album", media_count=1),
            CollectionMessage(3, base, grouped_id="album", caption="E7367", media_count=1),
            CollectionMessage(4, base, grouped_id="album", media_count=1),
            CollectionMessage(5, base, grouped_id="album", media_count=1),
        ]
    )

    assert len(partitioned.products) == 1
    assert [item.message_id for item in partitioned.products[0].messages] == [1, 2, 3, 4, 5]


def test_media_before_text_separates_informational_text_from_real_product() -> None:
    profile = SourceProfileData(
        product_collection_mode="media_before_text",
        product_collection_window_seconds=Decimal("180"),
    )

    products = admin_router.partition_test_inputs(
        [
            _part(50, text="Sizi mağazamıza bekliyoruz."),
            _part(51, media_paths=["l1.jpg"], media_group_id="lufian-a"),
            _part(52, media_paths=["l2.jpg"], media_group_id="lufian-b"),
            _part(53, text="#LUFIAN\nSIZE: S M L\nFIYAT: 13$"),
        ],
        profile=profile,
    )

    assert len(products) == 2
    assert products[0] == ("Sizi mağazamıza bekliyoruz.", [])
    assert products[1] == ("#LUFIAN\nSIZE: S M L\nFIYAT: 13$", ["l1.jpg", "l2.jpg"])


def test_text_before_media_partitions_two_opening_texts_into_two_products() -> None:
    profile = SourceProfileData(
        product_collection_mode="text_before_media",
        product_collection_window_seconds=Decimal("180"),
    )

    products = admin_router.partition_test_inputs(
        [
            _part(60, text="Article: M1\nPrice: 27 USD"),
            _part(61, media_paths=["m1.jpg"], media_group_id="msn-a"),
            _part(62, text="Article: M2\nPrice: 29 USD"),
            _part(63, media_paths=["m2.jpg"], media_group_id="msn-b"),
        ],
        profile=profile,
    )

    assert len(products) == 2
    assert products[0] == ("Article: M1\nPrice: 27 USD", ["m1.jpg"])
    assert products[1] == ("Article: M2\nPrice: 29 USD", ["m2.jpg"])


def test_advertisement_between_test_parts_does_not_join_product_batch() -> None:
    products = admin_router.partition_test_inputs(
        [
            _part(1, text="Article: A1", media_paths=["product.jpg"]),
            _part(2, text="#реклама\nВнешний сервис", media_paths=["ad.jpg"]),
            _part(3, text="Price: 10 USD\nSizes: S M\n2 pcs"),
        ],
        profile=SourceProfileData(),
    )

    assert all("реклама" not in text for text, _ in products)
    assert sorted(text for text, _ in products) == [
        "Article: A1",
        "Price: 10 USD\nSizes: S M\n2 pcs",
    ]
    assert [paths for _, paths in products] == [["product.jpg"], []]


def test_test_batch_input_is_durable_and_idempotent_in_database() -> None:
    assert "admin_test_batches" in Base.metadata.tables
    assert "admin_test_items" in Base.metadata.tables

    batch = Base.metadata.tables["admin_test_batches"]
    item = Base.metadata.tables["admin_test_items"]
    assert {"source_id", "admin_user_id", "status", "test_run_id"} <= set(batch.c.keys())
    assert {
        "batch_id",
        "telegram_chat_id",
        "telegram_message_id",
        "media_group_id",
        "text",
        "media_paths",
    } <= set(item.c.keys())
    assert any(
        {column.name for column in constraint.columns}
        == {"batch_id", "telegram_chat_id", "telegram_message_id"}
        for constraint in item.constraints
        if hasattr(constraint, "columns")
    )


@pytest.mark.asyncio
async def test_test_batch_resumes_and_duplicate_update_is_ignored() -> None:
    start_batch = getattr(admin_router, "start_or_resume_test_batch", None)
    add_item = getattr(admin_router, "add_test_batch_item", None)
    assert start_batch is not None
    assert add_item is not None

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        admin = AdminUser(telegram_user_id=123, display_name="Admin", enabled=True)
        source = Source(name="Lastpoint", telegram_channel_id=-1001, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData().model_dump(mode="json"),
        )
        session.add_all([admin, source])
        await session.flush()
        first = await start_batch(session, source.id, admin.id)
        resumed = await start_batch(session, source.id, admin.id)
        assert resumed.id == first.id
        assert await add_item(
            session,
            batch_id=first.id,
            chat_id=123,
            message_id=50,
            media_group_id="album-1",
            text="caption",
            media_paths=["1.jpg"],
        )
        assert not await add_item(
            session,
            batch_id=first.id,
            chat_id=123,
            message_id=50,
            media_group_id="album-1",
            text="caption",
            media_paths=["1.jpg"],
        )
        with pytest.raises(ValueError, match="grouped_id"):
            await add_item(
                session,
                batch_id=first.id,
                chat_id=123,
                message_id=51,
                media_group_id="album-2",
                text="another product",
                media_paths=["2.jpg"],
            )
        assert await session.scalar(select(func.count()).select_from(AdminTestItem)) == 1
    await engine.dispose()


@pytest.mark.asyncio
async def test_opted_in_test_batch_accepts_multiple_grouped_ids() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        admin = AdminUser(telegram_user_id=456, display_name="Admin", enabled=True)
        source = Source(name="Multi group", telegram_channel_id=-1002, enabled=False)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData(
                product_collection_mode="media_before_text",
                product_collection_window_seconds=Decimal("90"),
            ).model_dump(mode="json"),
        )
        session.add_all([admin, source])
        await session.flush()
        batch = await admin_router.start_or_resume_test_batch(session, source.id, admin.id)

        assert await admin_router.add_test_batch_item(
            session,
            batch_id=batch.id,
            chat_id=456,
            message_id=1,
            media_group_id="album-a",
            text="",
            media_paths=["a.jpg"],
            allow_multiple_grouped_ids=True,
        )
        assert await admin_router.add_test_batch_item(
            session,
            batch_id=batch.id,
            chat_id=456,
            message_id=2,
            media_group_id="album-b",
            text="Price: 12 USD",
            media_paths=["b.jpg"],
            allow_multiple_grouped_ids=True,
        )
        assert await session.scalar(select(func.count()).select_from(AdminTestItem)) == 2
    await engine.dispose()


@pytest.mark.asyncio
async def test_four_photo_batch_runs_pipeline_once_as_one_product() -> None:
    products = admin_router.partition_test_inputs(
        [
            _part(
                1,
                text=("Nike AB-125\nЦена: $10\nРазмеры: M-L-XL\nУпаковка: 3 шт\nЦвет: чёрный"),
                media_paths=["1.jpg"],
            ),
            _part(2, media_paths=["2.jpg"]),
            _part(3, media_paths=["3.jpg"]),
            _part(4, media_paths=["4.jpg"]),
        ]
    )
    assert len(products) == 1
    text, media_paths = products[0]
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=1,
            text=text,
            media_paths=media_paths,
            emoda_code="EM-TEST",
            profile=SourceProfileData(translation_enabled=False),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("10"),
                    max_price=Decimal("15"),
                    markup=Decimal("3"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    assert result.media_count == 4
    assert result.price is not None and result.price.final_usd == Decimal("13")
    assert result.parsed.fields["sizes"].value == "M-L-XL"
    assert result.parsed.fields["quantity"].value == "3"
    assert set(re.findall(r"EM-[A-Z0-9-]+", result.rendered_text)) == {"EM-TEST"}
    assert result.publication_requested is False


@pytest.mark.asyncio
async def test_admin_test_video_triggers_media_required_ignore_rule() -> None:
    from emoda.domain.schemas import ExtractionRule, SourceIgnoreRule

    result = await Pipeline(None).run(
        PipelineInput(
            source_id=5,
            text="E7193",
            media_paths=["test-video.mp4"],
            emoda_code="EM-TEST",
            profile=SourceProfileData(
                translation_enabled=False,
                extraction_rules={
                    "article": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"(?m)^(?P<value>E\d+)$",
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
            ),
            markup_rules=[],
        ),
        dry_run=True,
    )

    assert result.media_count == 1
    assert result.parsed.fields["article"].value == "E7193"
    assert result.ignored is True
    preview_text, attach_keyboard = admin_router.dry_run_preview(result)
    assert preview_text == "ignored=true\nignore_reason=article-only-media"
    assert attach_keyboard is False
    assert result.needs_review is False
    assert result.publication_requested is False


@pytest.mark.asyncio
async def test_admin_dry_run_diagnostics_explain_price_resolution_without_changing_payload() -> None:
    pattern = (
        r"(?m)^[ \t]{0,}📌[ \t]{0,}(?P<value>[0-9]+(?:[.,][0-9]+)?)"
        r"[ \t]{0,}[^0-9A-Za-z\r\n ]{1,}[ \t]{0,}$"
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=51,
            text="Kod: 107144\n📌25,5 💵\nSizes: S M\nQuantity: 2",
            media_paths=[],
            emoda_code="EM-TEST",
            profile=SourceProfileData(
                extraction_rules={
                    "price": [
                        ExtractionRule(
                            kind="regex",
                            pattern=pattern,
                            priority=10,
                            authoritative=True,
                        )
                    ]
                }
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=None,
                    markup=Decimal("5"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    diagnostic = admin_router.dry_run_diagnostics(result)
    preview_text, attach_keyboard = admin_router.dry_run_preview(result)

    assert "Price: FOUND" in diagnostic
    assert "Source: source_rule" in diagnostic
    assert "Rule: price:1" in diagnostic
    assert "Raw: 25,5" in diagnostic
    assert "Normalized: 25.5" in diagnostic
    assert "Currency: USD" in diagnostic
    assert "Candidate: FOUND" in diagnostic
    assert "Final: 31" in diagnostic
    assert "Stage: resolved" in diagnostic
    assert "Reason: -" in diagnostic
    assert len(preview_text.splitlines()) == 4
    assert "Pipeline diagnostics" not in preview_text
    assert attach_keyboard is True


@pytest.mark.asyncio
async def test_admin_dry_run_explains_canonical_pattern_match_count() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=45,
            text="S. S. M. M. L",
            media_paths=[],
            emoda_code="EM-TEST",
            profile=SourceProfileData(
                computed_rules=[
                    ComputedRule(
                        name="count-source-sizes",
                        target_field="quantity",
                        operation="count_items",
                        input_source="canonical_text",
                        pattern=r"(?P<size>XS|S|M|L|XL)",
                        capture_group="size",
                    )
                ]
            ),
            markup_rules=[],
        ),
        dry_run=True,
    )

    diagnostic = admin_router.dry_run_diagnostics(result)

    assert "Quantity: FOUND" in diagnostic
    assert "Source: computed_rule" in diagnostic
    assert "Input source: canonical_text" in diagnostic
    assert "Pattern matches: [S, S, M, M, L]" in diagnostic
    assert "Count: 5" in diagnostic
    assert "Candidate: FOUND" in diagnostic


@pytest.mark.asyncio
async def test_admin_dry_run_explains_match_that_cannot_resolve_currency() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=72,
            text="📌25,5 🔸\nSizes: S M\nQuantity: 2",
            media_paths=[],
            emoda_code="EM-TEST",
            profile=SourceProfileData(
                extraction_rules={
                    "price": [
                        ExtractionRule(
                            kind="regex",
                            pattern=(
                                r"(?m)^📌(?P<value>[0-9]+(?:[.,][0-9]+)?)\s*"
                                r"[^0-9A-Za-z\r\n ]+$"
                            ),
                            authoritative=True,
                        )
                    ]
                }
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=None,
                    markup=Decimal("5"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    diagnostic = admin_router.dry_run_diagnostics(result)

    assert result.parsed.fields["price"].status.value == "found"
    assert result.price is None
    assert "Price: FOUND" in diagnostic
    assert "Raw: 25,5" in diagnostic
    assert "Normalized: 25.5" in diagnostic
    assert "Currency: -" in diagnostic
    assert "Candidate: MISSING" in diagnostic
    assert "Stage: currency" in diagnostic
    assert "Reason: currency_missing" in diagnostic
    assert "Цена за шт / Unit price:</b> Уточнить / Check" in result.rendered_text
