import inspect

from emoda.admin import router as admin_router
from emoda.admin import service as admin_service
from emoda.domain.schemas import (
    ComputedRule,
    ExtractionRule,
    MultiPriceRule,
    SourceIgnoreRule,
    SourceProfileData,
)


def test_source_rules_markup_exposes_add_fallback_and_safe_deletion_callbacks() -> None:
    build_source_rules_markup = getattr(admin_router, "build_source_rules_markup", None)
    assert build_source_rules_markup is not None
    profile = SourceProfileData(
        extraction_rules={
            "quantity": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Series:\s*(?P<value>[^\n]+)",
                    transform="series_quantity",
                )
            ]
        },
        fallback_rules={"sizes": {"value": "S, M, L", "authoritative": True}},
        ignore_rules=[
            SourceIgnoreRule(
                name="article-only-media",
                found_fields=["article"],
                missing_fields=["price", "sizes", "quantity"],
            )
        ],
        multi_price_rules=[
            MultiPriceRule(
                name="items",
                pattern=r"(?P<label>A|B)\s*(?P<value>\d+)\$",
                default_currency="USD",
                labels={"A": {"ru": "А", "en": "A"}, "B": {"ru": "Б", "en": "B"}},
            )
        ],
        computed_rules=[
            ComputedRule(
                name="count-sizes",
                target_field="quantity",
                operation="count_items",
                source_field="sizes",
                pattern=r"(?P<item>\d+)",
                capture_group="item",
            )
        ],
    )

    markup = build_source_rules_markup(17, profile)
    callbacks = {button.callback_data for row in markup.inline_keyboard for button in row}
    token = getattr(admin_service, "profile_item_token", None)
    assert token is not None
    rule_token = token(profile.extraction_rules["quantity"][0])
    ignore_token = token(profile.ignore_rules[0])
    multi_price_token = token(profile.multi_price_rules[0])
    computed_token = token(profile.computed_rules[0])

    assert "source:rule:17" in callbacks
    assert "source:fallback:17" in callbacks
    assert "source:ignore:17" in callbacks
    assert "source:multi-price:17" in callbacks
    assert f"source:multi-price-del:17:0:{multi_price_token}" in callbacks
    assert f"source:multi-price-edit:17:0:{multi_price_token}" in callbacks
    assert "source:advertising:17" in callbacks
    assert "source:repeated:17" in callbacks
    assert "source:computed:17" in callbacks
    assert "source:field-modes:17" in callbacks
    assert f"source:computed-del:17:0:{computed_token}" in callbacks
    assert f"source:computed-edit:17:0:{computed_token}" in callbacks
    assert f"source:computed-set:17:0:false:{computed_token}" in callbacks
    assert f"r:d:17:quantity:0:{rule_token}" in callbacks
    assert "source:fallback-del:17:sizes" in callbacks
    assert f"source:ignore-del:17:0:{ignore_token}" in callbacks
    # Rules can be disabled without deleting them.
    assert f"r:s:17:quantity:0:0:{rule_token}" in callbacks
    assert f"source:ignore-set:17:0:false:{ignore_token}" in callbacks
    assert f"source:multi-price-set:17:0:false:{multi_price_token}" in callbacks


def test_extraction_rule_editor_renders_every_independently_editable_setting() -> None:
    editor_text = getattr(admin_router, "extraction_rule_editor_text", None)
    assert editor_text is not None
    rule = ExtractionRule(
        kind="regex",
        pattern=r"Sizes:\s*(?P<value>STANDART)",
        aliases=["Size", "Sizes"],
        priority=7,
        ignore_case=False,
        transform="map",
        token_pattern=r"(?P<size>[A-Z]+)(?P<count>\d+)",
        value_map={"STANDART": "Standard"},
        authoritative=True,
        enabled=False,
    )

    assert editor_text("sizes", rule).splitlines() == [
        "field=sizes",
        "kind=regex",
        r"pattern=Sizes:\s*(?P<value>STANDART)",
        "aliases=Size,Sizes",
        "priority=7",
        "ignore_case=false",
        "transform=map",
        r"token_pattern=(?P<size>[A-Z]+)(?P<count>\d+)",
        "value_map=STANDART=>Standard",
        "authoritative=true",
        "enabled=false",
    ]


def test_admin_computed_rule_screen_uses_understandable_operation_labels() -> None:
    source = inspect.getsource(admin_router.create_admin_router)

    for label in (
        "Количество в размерном ряду",
        "Количество элементов",
        "Сумма значений",
        "Размер + количество",
        "Уникальные значения",
    ):
        assert label in source

    assert "count_series_items" in source
    assert '"count_series_items": "Количество в размерном ряду"' in source
    assert "input_mode=raw" in source
    assert "series_separator_pattern" in source
    assert "range_pattern" in source
    assert "explicit_list_pattern" in source
    assert "numeric_step" in source
    assert "compact_range_pattern" in source


def test_source_rule_deletion_callbacks_resolve_exact_source_field_and_index() -> None:
    parse_rule = getattr(admin_router, "parse_source_rule_delete_callback", None)
    parse_fallback = getattr(admin_router, "parse_source_fallback_delete_callback", None)
    parse_ignore = getattr(admin_router, "parse_source_ignore_delete_callback", None)
    assert parse_rule is not None
    assert parse_fallback is not None
    assert parse_ignore is not None

    assert parse_rule("r:d:17:quantity:2:abc12345") == (
        17,
        "quantity",
        2,
        "abc12345",
    )
    assert parse_fallback("source:fallback-del:17:sizes") == (17, "sizes")
    assert parse_ignore("source:ignore-del:17:2:abc12345") == (17, 2, "abc12345")


def test_collection_screen_exposes_every_collection_control() -> None:
    source = inspect.getsource(admin_router.create_admin_router)

    for fragment in (
        "Сбор товара",
        "collect:strategy:",
        "collect:order:",
        "collect:main:",
        "collect:bool:",
        "collect:num:",
        "collect:bdel:",
        "collect:bset:",
        "collect:bedit:",
        "collect:badd:",
        "debounce_seconds",
        "max_time_gap_seconds",
        "max_message_id_gap",
        "max_messages",
        "max_media",
        "merge_neighbor_grouped_ids",
        "allow_multiple_text_blocks",
        "merge_text_blocks",
        "stop_on_next_start",
    ):
        assert fragment in source, fragment


def test_profile_help_keeps_collection_settings_out_of_keyvalue_editor() -> None:
    source = inspect.getsource(admin_router.create_admin_router)

    assert "collection_debounce_seconds=" not in source
    assert "collection_stop_on_next_start=" not in source


async def test_collection_screen_walkthrough_renders_controls_and_persists_changes(
    tmp_path,
) -> None:
    """A regular admin opens Источники → source → Сбор товара and changes settings."""

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from emoda.db.base import Base
    from emoda.db.models import AdminUser, Source, SourceProfile
    from emoda.domain.enums import PublicationMode
    from emoda.domain.schemas import CollectionBoundaryRule, CollectionProfile

    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        admin = AdminUser(telegram_user_id=900, display_name="Admin", enabled=True)
        source = Source(name="Blind source", telegram_channel_id=-1900, enabled=True)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData(
                collection=CollectionProfile(
                    boundary_rules=[
                        CollectionBoundaryRule(
                            name="next-album",
                            action="start",
                            match="grouped_id_change",
                        )
                    ]
                )
            ).model_dump(mode="json"),
        )
        session.add_all([admin, source])
        await session.flush()
        source_id = source.id

    async def access_validator(_: str) -> tuple[int, str, int]:
        return -1, "unused", 0

    async def test_message_loader(_: str) -> tuple[str, list[str]]:
        return "", []

    router = admin_router.create_admin_router(
        sessions,
        {900},
        access_validator=access_validator,
        test_message_loader=test_message_loader,
        intelligence=None,
        media_root=tmp_path,
    )

    edits: list[tuple[str, object]] = []

    class FakeMessage:
        async def edit_text(self, text: str, reply_markup: object = None) -> None:
            edits.append((text, reply_markup))

    class FakeCallback:
        def __init__(self, data: str) -> None:
            self.data = data
            self.message = FakeMessage()
            self.answers: list[str] = []

        async def answer(self, text: str | None = None, show_alert: bool = False) -> None:
            self.answers.append(text or "")

    def handler_by_name(name: str):
        for handler in router.callback_query.handlers:
            if handler.callback.__name__ == name:
                return handler.callback
        raise AssertionError(f"handler not found for {name}")

    screen = handler_by_name("source_collection_screen")
    callback = FakeCallback(f"source:collect:{source_id}")
    await screen(callback)

    assert len(edits) == 1
    text, markup = edits[0]
    assert "Сбор товара" in text
    assert "Стратегия" in text and "Debounce" in text and "Окно по message ID" in text
    callbacks = {
        button.callback_data for row in markup.inline_keyboard for button in row  # type: ignore[union-attr]
    }
    assert f"collect:strategy:{source_id}:multi_grouped_id" in callbacks
    assert f"collect:bool:{source_id}:merge:true" in callbacks
    assert f"collect:num:{source_id}:debounce" in callbacks
    assert any(value.startswith(f"collect:bset:{source_id}:0:false:") for value in callbacks)
    assert any(value.startswith(f"collect:bedit:{source_id}:0:") for value in callbacks)
    assert any(value.startswith(f"collect:bdel:{source_id}:0:") for value in callbacks)
    assert f"collect:badd:{source_id}:text_start" in callbacks
    assert f"source:boundary:{source_id}" in callbacks

    strategy_set = handler_by_name("collection_strategy_set")
    edits.clear()
    await strategy_set(FakeCallback(f"collect:strategy:{source_id}:multi_grouped_id"))

    async with sessions() as session:
        source = await session.get(Source, source_id)
        profile = SourceProfileData.model_validate(source.profile.config)
    assert profile.collection is not None
    assert profile.collection.strategy == "multi_grouped_id"
    assert profile.collection.merge_neighbor_grouped_ids is True

    toggle = handler_by_name("collection_bool_set")
    edits.clear()
    await toggle(FakeCallback(f"collect:bool:{source_id}:stop:false"))
    async with sessions() as session:
        source = await session.get(Source, source_id)
        profile = SourceProfileData.model_validate(source.profile.config)
    assert profile.collection is not None and profile.collection.stop_on_next_start is False

    boundary_toggle = handler_by_name("collection_boundary_set")
    edits.clear()
    async with sessions() as session:
        source = await session.get(Source, source_id)
        profile = SourceProfileData.model_validate(source.profile.config)
    profile_item_token = getattr(admin_service, "profile_item_token", None)
    assert profile_item_token is not None
    boundary_token = profile_item_token(profile.effective_collection().boundary_rules[0])
    await boundary_toggle(
        FakeCallback(f"collect:bset:{source_id}:0:false:{boundary_token}")
    )
    async with sessions() as session:
        source = await session.get(Source, source_id)
        profile = SourceProfileData.model_validate(source.profile.config)
    assert profile.collection is not None
    assert profile.collection.boundary_rules[0].enabled is False
    await engine.dispose()
