from datetime import timedelta
from decimal import Decimal

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.admin import service as admin_service
from emoda.admin.service import (
    AdminService,
    parse_catalog_node_input,
    parse_cleaning_rule_input,
    parse_collection_boundary_input,
    parse_computed_rule_input,
    parse_key_value_lines,
    parse_markup_rules_input,
    parse_multi_price_rule_input,
    parse_navigation_button_input,
    parse_repeated_pattern_rule_input,
    parse_routing_pair_input,
    parse_source_ignore_rule_input,
    update_profile_from_lines,
)
from emoda.db.base import Base, utcnow
from emoda.db.models import (
    AlbumGroup,
    CatalogNode,
    Destination,
    Product,
    Source,
    SourceExample,
    SourceMessage,
)
from emoda.domain.enums import (
    AlbumStatus,
    ButtonAction,
    CatalogNodeType,
    OCRMode,
    ProductStatus,
    PublicationMode,
)
from emoda.domain.schemas import (
    CatalogNodeData,
    CategoryRule,
    ComputedRule,
    ExtractionRule,
    NavigationButtonData,
    RoutingRule,
    SourceProfileData,
)


def test_admin_parses_all_computed_operations_without_json() -> None:
    series_rule = parse_computed_rule_input(
        "name=count-series\ntarget_field=quantity\noperation=count_series_items\n"
        "source_field=sizes\ninput_mode=raw\nseries_separator_pattern=\\s+\n"
        "range_pattern=(?P<start>\\d+)-(?P<end>\\d+)\n"
        "explicit_list_pattern=(?P<item>\\d+)\nexplicit_separator_pattern=-\n"
        "compact_range_pattern=(?P<start>\\d{2})(?P<end>\\d{2})\n"
        "numeric_step=2\npriority=6\nauthoritative=true\n"
        "conflict_policy=ambiguous"
    )
    count_rule = parse_computed_rule_input(
        "name=count-sizes\ntarget_field=quantity\noperation=count_items\n"
        "source_field=sizes\npattern=(?P<item>\\d+)\ncapture_group=item\n"
        "priority=7\nauthoritative=true\nconflict_policy=explicit"
    )
    pair_rule = parse_computed_rule_input(
        "name=size-counts\ntarget_field=quantity\nsecondary_target_field=sizes\n"
        "operation=sum_size_counts\npattern=(?P<size>[A-Z]+)(?P<count>\\d+)\n"
        "capture_group=size\nvalue_group=count\nseparator=space\nconflict_policy=ambiguous"
    )
    sum_rule = parse_computed_rule_input(
        "name=sum\ntarget_field=quantity\noperation=sum_matches\n"
        "pattern=(?P<units>\\d+)\nvalue_group=units"
    )
    unique_rule = parse_computed_rule_input(
        "name=unique\ntarget_field=sizes\noperation=unique_matches\n"
        "pattern=(?P<label>[A-Z]+)\ncapture_group=label"
    )
    canonical_rule = parse_computed_rule_input(
        "name=count-source-sizes\ntarget_field=quantity\noperation=count_items\n"
        "input_source=canonical_text\npattern=(?P<size>XS|S|M|L|XL)\n"
        "capture_group=size"
    )

    assert series_rule.operation == "count_series_items"
    assert series_rule.input_mode == "raw"
    assert series_rule.numeric_step == Decimal("2")
    assert series_rule.series_separator_pattern == r"\s+"
    assert series_rule.compact_range_pattern == r"(?P<start>\d{2})(?P<end>\d{2})"
    assert series_rule.authoritative is True
    assert count_rule.operation == "count_items"
    assert count_rule.priority == 7
    assert count_rule.authoritative is True
    assert count_rule.conflict_policy == "explicit"
    assert pair_rule.secondary_target_field == "sizes"
    assert pair_rule.separator == " "
    assert sum_rule.value_group == "units"
    assert unique_rule.capture_group == "label"
    assert canonical_rule.input_source == "canonical_text"
    assert canonical_rule.source_field is None


async def test_admin_computed_rule_crud_persists_in_source_profile(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="Computed CRUD",
        channel_id=818,
        telegram_ref=None,
        destination_id=None,
        actor_id=None,
    )
    rule = ComputedRule(
        name="count-sizes",
        target_field="quantity",
        operation="count_items",
        source_field="sizes",
        pattern=r"(?P<item>\d+)",
        capture_group="item",
    )

    await service.add_computed_rule(source.id, rule, actor_id=None)
    configured = SourceProfileData.model_validate(source.profile.config)
    assert configured.computed_rules == [rule]

    token = admin_service.profile_item_token(configured.computed_rules[0])
    await service.toggle_computed_rule(
        source.id, 0, enabled=False, expected_token=token, actor_id=None
    )
    configured = SourceProfileData.model_validate(source.profile.config)
    assert configured.computed_rules[0].enabled is False

    token = admin_service.profile_item_token(configured.computed_rules[0])
    replacement = configured.computed_rules[0].model_copy(
        update={"name": "count-sizes-updated", "priority": 3}
    )
    await service.replace_computed_rule(
        source.id, 0, replacement, expected_token=token, actor_id=None
    )
    configured = SourceProfileData.model_validate(source.profile.config)
    assert configured.computed_rules[0].name == "count-sizes-updated"
    assert configured.computed_rules[0].priority == 3

    token = admin_service.profile_item_token(configured.computed_rules[0])
    await service.delete_computed_rule(
        source.id, 0, expected_token=token, actor_id=None
    )
    assert SourceProfileData.model_validate(source.profile.config).computed_rules == []


async def test_admin_field_processing_mode_persists_and_can_return_to_normal(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="Raw policy CRUD",
        channel_id=819,
        telegram_ref=None,
        destination_id=None,
        actor_id=None,
    )
    source_id = source.id

    await service.set_field_processing_mode(
        source_id, "sizes", "passthrough", actor_id=None
    )
    await session.commit()
    session.expire_all()
    reloaded = await session.get(Source, source_id)
    assert reloaded is not None
    configured = SourceProfileData.model_validate(reloaded.profile.config)
    assert configured.field_policies["sizes"].processing_mode == "passthrough"

    await service.set_field_processing_mode(
        source_id, "sizes", "normal", actor_id=None
    )
    await session.commit()
    session.expire_all()
    reloaded = await session.get(Source, source_id)
    assert reloaded is not None
    configured = SourceProfileData.model_validate(reloaded.profile.config)
    assert "sizes" not in configured.field_policies


@pytest.fixture
async def session() -> AsyncSession:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


async def test_new_source_is_disabled_and_review_by_default(session: AsyncSession) -> None:
    destination = Destination(name="Women", telegram_channel_id=-1001)
    session.add(destination)
    await session.flush()
    source = await AdminService(session).create_source(
        name="Supplier",
        channel_id=123,
        telegram_ref="@supplier",
        destination_id=destination.id,
        actor_id=None,
    )
    assert source.enabled is False
    assert source.profile.publication_mode is PublicationMode.REVIEW
    assert source.profile.config["default_destination_id"] == destination.id
    assert source.profile.schema_version == 2
    assert source.profile.config["collection"]["strategy"] == "single_grouped_id"


async def test_review_queue_keeps_the_newest_incidents_reachable(session: AsyncSession) -> None:
    source = Source(name="review queue", telegram_channel_id=100500, enabled=True)
    session.add(source)
    await session.flush()
    now = utcnow()
    session.add_all(
        [
            Product(
                emoda_code=f"QUEUE-{index:02d}",
                source_id=source.id,
                status=ProductStatus.NEEDS_REVIEW,
                created_at=now + timedelta(seconds=index),
                updated_at=now + timedelta(seconds=index),
            )
            for index in range(35)
        ]
    )
    await session.flush()

    items = await AdminService(session).review_queue(limit=30)

    assert len(items) == 30
    assert items[0].emoda_code == "QUEUE-34"
    assert items[-1].emoda_code == "QUEUE-05"
    assert "QUEUE-00" not in {item.emoda_code for item in items}


async def test_new_source_starts_catchup_from_validated_current_message(
    session: AsyncSession,
) -> None:
    source = await AdminService(session).create_source(
        name="current-only",
        channel_id=222,
        telegram_ref="@current",
        destination_id=None,
        last_seen_message_id=71443,
        actor_id=None,
    )
    assert source.last_seen_message_id == 71443
    assert source.catchup_anchor_message_id == 71443


async def test_source_recent_outcomes_are_product_authoritative_and_reasoned(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="observable",
        channel_id=333,
        telegram_ref="@observable",
        destination_id=None,
        actor_id=None,
    )
    received = SourceMessage(
        source_id=source.id,
        telegram_message_id=10,
        text="received",
        telegram_date=utcnow(),
        raw_metadata={"outcome": "received"},
    )
    reviewed = SourceMessage(
        source_id=source.id,
        telegram_message_id=11,
        text="review",
        telegram_date=utcnow(),
        raw_metadata={"outcome": "processing"},
    )
    skipped = SourceMessage(
        source_id=source.id,
        telegram_message_id=12,
        text="ad",
        telegram_date=utcnow(),
        raw_metadata={"outcome": "skipped", "outcome_reason": "advertising:builtin"},
    )
    session.add_all([received, reviewed, skipped])
    await session.flush()
    session.add(
        Product(
            emoda_code="AA90001",
            source_id=source.id,
            source_message_id=reviewed.id,
            status=ProductStatus.NEEDS_REVIEW,
            processing_error="missing critical fields: price",
        )
    )
    await session.flush()

    outcomes = await service.recent_source_outcomes(source.id, limit=5)

    assert outcomes == [
        {
            "telegram_message_id": 12,
            "outcome": "skipped",
            "reason": "advertising:builtin",
            "emoda_code": None,
            "state": "received",
            "origin": "live",
            "event_type": "new",
        },
        {
            "telegram_message_id": 11,
            "outcome": "needs_review",
            "reason": "missing critical fields: price",
            "emoda_code": "AA90001",
            "state": "received",
            "origin": "live",
            "event_type": "new",
        },
        {
            "telegram_message_id": 10,
            "outcome": "received",
            "reason": None,
            "emoda_code": None,
            "state": "received",
            "origin": "live",
            "event_type": "new",
        },
    ]


async def test_product_review_reason_falls_back_to_durable_policy_outcome(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="Mybee", channel_id=444, telegram_ref="@mybee", destination_id=None, actor_id=None
    )
    message = SourceMessage(
        source_id=source.id,
        telegram_message_id=17673,
        text="complete critical fields",
        telegram_date=utcnow(),
        raw_metadata={
            "outcome": "needs_review",
            "outcome_reason": "source publication mode is REVIEW",
        },
    )
    session.add(message)
    await session.flush()
    product = Product(
        emoda_code="AA00029",
        source_id=source.id,
        source_message_id=message.id,
        status=ProductStatus.NEEDS_REVIEW,
        parsed_result={"needs_review": False, "review_reasons": []},
    )
    session.add(product)
    await session.flush()

    assert await service.product_review_reason(product) == "source publication mode is REVIEW"


async def test_product_review_reason_renders_structured_codes_and_details(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="structured", channel_id=445, telegram_ref=None, destination_id=None, actor_id=None
    )
    product = Product(
        emoda_code="AA00031",
        source_id=source.id,
        status=ProductStatus.NEEDS_REVIEW,
        review_reasons=[
            {
                "code": "parser_ambiguous",
                "field": "price",
                "detail": "two explicit values",
            }
        ],
        processing_error="legacy human-readable detail",
    )
    session.add(product)
    await session.flush()
    assert await service.product_review_reason(product) == (
        "stage=extraction; field=price; rule=parser_ambiguous; parser_ambiguous: two explicit values"
    )


async def test_invalid_profile_is_rejected_before_database_write(session: AsyncSession) -> None:
    source = await AdminService(session).create_source(
        name="Supplier", channel_id=123, telegram_ref=None, destination_id=None, actor_id=None
    )
    with pytest.raises(ValidationError):
        await AdminService(session).update_profile(source.id, {"schema_version": 99}, actor_id=None)


async def test_profile_rejects_unknown_destination_binding(session: AsyncSession) -> None:
    source = await AdminService(session).create_source(
        name="Supplier", channel_id=123, telegram_ref=None, destination_id=None, actor_id=None
    )
    config = SourceProfileData(default_destination_id=999).model_dump(mode="json")
    with pytest.raises(ValueError, match="unknown destination IDs"):
        await AdminService(session).update_profile(source.id, config, actor_id=None)


async def test_examples_are_returned_only_for_selected_source(session: AsyncSession) -> None:
    service = AdminService(session)
    first = await service.create_source(
        name="A", channel_id=1, telegram_ref=None, destination_id=None, actor_id=None
    )
    second = await service.create_source(
        name="B", channel_id=2, telegram_ref=None, destination_id=None, actor_id=None
    )
    await service.add_example(first.id, "A input", {"article": "A1"}, "", actor_id=None)
    await service.add_example(second.id, "B input", {"article": "B1"}, "", actor_id=None)
    examples = await service.examples(first.id, 10)
    assert [item.original_input for item in examples] == ["A input"]


async def test_overlapping_markup_is_rejected(session: AsyncSession) -> None:
    service = AdminService(session)
    await service.replace_markup_rules([(Decimal("0"), Decimal("5"), Decimal("1"))], actor_id=None)
    with pytest.raises(ValueError, match="overlap"):
        await service.replace_markup_rules(
            [
                (Decimal("0"), Decimal("5"), Decimal("1")),
                (Decimal("5"), Decimal("9"), Decimal("2")),
            ],
            actor_id=None,
        )


def test_admin_parses_fixed_and_open_percentage_markup_without_json() -> None:
    ranges = parse_markup_rules_input("0-100.99:10\n101-inf:25%")

    assert ranges == [
        (Decimal("0"), Decimal("100.99"), Decimal("10"), "fixed"),
        (Decimal("101"), None, Decimal("25"), "percent"),
    ]


def test_admin_parses_source_multi_price_rule_without_json() -> None:
    rule = parse_multi_price_rule_input(
        "\n".join(
            [
                "name=products-with-prices",
                r"pattern=(?P<label>Ayakkabı|Küçük Çanta)\s*(?P<value>\d+)\s*(?P<currency>\$)",
                "default_currency=USD",
                "priority=7",
                "ignore_case=false",
                "enabled=false",
                "labels=Ayakkabı=>Обувь=>Shoes;Küçük Çanta=>Маленькая сумка=>Small bag",
            ]
        )
    )

    assert rule.default_currency == "USD"
    assert rule.priority == 7
    assert rule.ignore_case is False
    assert rule.enabled is False
    assert rule.labels["Ayakkabı"].ru == "Обувь"
    assert rule.labels["Küçük Çanta"].en == "Small bag"


def test_admin_parses_collection_repeated_pattern_and_cleaning_without_json() -> None:
    boundary = parse_collection_boundary_input(
        "\n".join(
            [
                "name=next-product",
                "action=hard_boundary",
                "match=regex",
                r"pattern=(?m)^#[A-Z][A-Z ]+$",
                "target=incoming",
            ]
        )
    )
    repeated = parse_repeated_pattern_rule_input(
        "\n".join(
            [
                "name=range-price",
                r"pattern=(?m)^(?P<size>\d+-\d+)-(?P<price>\d+)$",
                "fields=sizes=>size;price=>price",
                "aggregation=sizes=>unique;price=>join_unique",
                "separators=price=>/",
                "authoritative=true",
            ]
        )
    )
    cleaning = parse_cleaning_rule_input(
        "kind=prefix\nvalue=Service:\nscope=line\nlanguage=both\nenabled=false"
    )
    assert boundary.action == "hard_boundary"
    assert repeated.fields == {"sizes": "size", "price": "price"}
    assert repeated.aggregation["price"] == "join_unique"
    assert cleaning.kind == "prefix"
    assert cleaning.enabled is False


def test_admin_parses_complete_routing_pair_without_json() -> None:
    category, route = parse_routing_pair_input(
        "category=evening\n"
        "keywords=dress,gown\n"
        r"pattern=(?i)\b(dress|gown)=?\b" "\n"
        "destination_id=17\n"
        "priority=9"
    )

    assert category == CategoryRule(
        category="evening",
        keywords=["dress", "gown"],
        pattern=r"(?i)\b(dress|gown)=?\b",
        priority=9,
    )
    assert route == RoutingRule(
        category="evening",
        keywords=["dress", "gown"],
        pattern=r"(?i)\b(dress|gown)=?\b",
        destination_id=17,
        priority=9,
    )


def test_admin_routing_parser_rejects_unknown_settings() -> None:
    with pytest.raises(ValueError, match="unknown routing settings"):
        parse_routing_pair_input(
            "category=evening\ndestination_id=17\npython_handler=custom"
        )


@pytest.mark.parametrize(
    ("kind", "value"),
    [
        ("exact", "OLD EXACT"),
        ("contains", "old fragment"),
        ("regex", r"OLD\\s+REGEX"),
    ],
)
async def test_admin_edits_existing_cleaning_rule_in_place_with_token_isolation(
    session: AsyncSession, kind: str, value: str
) -> None:
    service = AdminService(session)
    source_a = await service.create_source(
        name=f"Cleaning A {kind}",
        channel_id={"exact": 701, "contains": 702, "regex": 703}[kind],
        telegram_ref=None,
        destination_id=None,
        actor_id=None,
    )
    source_b = await service.create_source(
        name=f"Cleaning B {kind}",
        channel_id={"exact": 711, "contains": 712, "regex": 713}[kind],
        telegram_ref=None,
        destination_id=None,
        actor_id=None,
    )
    for source in (source_a, source_b):
        await service.add_cleaning_rule(
            source.id,
            parse_cleaning_rule_input(f"kind={kind}\nvalue={value}"),
            actor_id=None,
        )
        await service.add_cleaning_rule(
            source.id,
            parse_cleaning_rule_input("kind=suffix\nvalue=KEEP"),
            actor_id=None,
        )

    before_a = SourceProfileData.model_validate(source_a.profile.config)
    token = admin_service.profile_item_token(before_a.cleaning_rules[0])
    replacement = parse_cleaning_rule_input(
        "kind=regex\nvalue=NEW\\s+VALUE\nreplacement=-\nscope=text\n"
        "language=en\nignore_case=false\nenabled=false"
    )

    updated = await service.replace_cleaning_rule(
        source_a.id,
        0,
        replacement,
        expected_token=token,
        actor_id=None,
    )

    assert updated.cleaning_rules == [replacement, before_a.cleaning_rules[1]]
    assert SourceProfileData.model_validate(source_b.profile.config).cleaning_rules[0].value == value
    with pytest.raises(LookupError, match="cleaning rule not found"):
        await service.replace_cleaning_rule(
            source_a.id,
            0,
            parse_cleaning_rule_input("kind=exact\nvalue=STALE"),
            expected_token=token,
            actor_id=None,
        )


async def test_admin_edits_existing_multi_price_rule_without_duplicate(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source_a = await service.create_source(
        name="Multi edit A",
        channel_id=720,
        telegram_ref=None,
        destination_id=None,
        actor_id=None,
    )
    source_b = await service.create_source(
        name="Multi edit B",
        channel_id=721,
        telegram_ref=None,
        destination_id=None,
        actor_id=None,
    )
    original = parse_multi_price_rule_input(
        "name=items\n"
        r"pattern=(?P<label>A|B)\s*(?P<value>\d+)\$"
        "\ndefault_currency=USD\npriority=40\n"
        "labels=A=>А=>A;B=>Б=>B"
    )
    for source in (source_a, source_b):
        await service.add_multi_price_rule(source.id, original, actor_id=None)

    before = SourceProfileData.model_validate(source_a.profile.config)
    token = admin_service.profile_item_token(before.multi_price_rules[0])
    replacement = parse_multi_price_rule_input(
        "name=items\n"
        r"pattern=(?P<label>C|D)\s*(?P<value>\d+)\s*EUR"
        "\ndefault_currency=EUR\npriority=3\nignore_case=false\nenabled=false\n"
        "labels=C=>Топ=>Top;D=>Юбка=>Skirt"
    )

    updated = await service.replace_multi_price_rule(
        source_a.id,
        0,
        replacement,
        expected_token=token,
        actor_id=None,
    )

    assert len(updated.multi_price_rules) == 1
    assert updated.multi_price_rules[0] == replacement
    assert updated.multi_price_rules[0].name == original.name
    untouched = SourceProfileData.model_validate(source_b.profile.config)
    assert untouched.multi_price_rules == [original]
    with pytest.raises(LookupError, match="multi-price rule not found"):
        await service.replace_multi_price_rule(
            source_a.id,
            0,
            original,
            expected_token=token,
            actor_id=None,
        )


async def test_archived_source_is_restored_in_place_disabled_with_profile_and_examples(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    destination = Destination(name="Restore destination", telegram_channel_id=-1000722)
    session.add(destination)
    await session.flush()
    source = await service.create_source(
        name="Restorable source",
        channel_id=722,
        telegram_ref="@restorable",
        destination_id=destination.id,
        actor_id=None,
    )
    configured = SourceProfileData.model_validate(source.profile.config).model_copy(
        update={
            "publication_mode": PublicationMode.AUTO,
            "cleaning_rules": [
                parse_cleaning_rule_input("kind=contains\nvalue=NOISE")
            ],
            "multi_price_rules": [
                parse_multi_price_rule_input(
                    "name=restore-items\n"
                    r"pattern=(?P<label>A|B)\s*(?P<value>\d+)"
                    "\ndefault_currency=USD\nlabels=A=>А=>A;B=>Б=>B"
                )
            ],
            "gemini_enabled": True,
            "gemini_instruction": "Extract only evidence",
        }
    )
    await service.update_profile(source.id, configured.model_dump(mode="json"), actor_id=None)
    example = await service.add_example(
        source.id,
        "A 10 B 20",
        {"price": "10"},
        "restore proof",
        actor_id=None,
    )
    await service.set_source_enabled(source.id, True, actor_id=None)
    source_id = source.id
    profile_before = dict(source.profile.config)

    await service.archive_source(source_id, actor_id=None)
    assert source.archived_at is not None
    assert source.enabled is False

    restored = await service.restore_source(source_id, actor_id=None)

    assert restored.id == source_id
    assert restored.archived_at is None
    assert restored.enabled is False
    assert restored.profile.config == profile_before
    assert restored.profile.publication_mode is PublicationMode.AUTO
    stored_example = await session.get(SourceExample, example.id)
    assert stored_example is not None and stored_example.source_id == source_id
    with pytest.raises(LookupError, match="source is not archived"):
        await service.restore_source(source_id, actor_id=None)

    active = list(
        await session.scalars(
            __import__("sqlalchemy", fromlist=["select"]).select(Source).where(
                Source.archived_at.is_(None)
            )
        )
    )
    assert source_id in {item.id for item in active}


async def test_admin_can_remove_every_new_generic_rule_type(session: AsyncSession) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="self-service",
        channel_id=335,
        telegram_ref=None,
        destination_id=None,
        actor_id=None,
    )
    await service.add_repeated_pattern_rule(
        source.id,
        parse_repeated_pattern_rule_input(
            "name=variants\npattern=(?P<size>42-48)-(?P<price>\\d+)\n"
            "fields=sizes=>size;price=>price\n"
            "aggregation=sizes=>unique;price=>join_unique"
        ),
        actor_id=None,
    )
    await service.add_collection_boundary(
        source.id,
        parse_collection_boundary_input(
            "name=new-product\naction=hard_boundary\nmatch=regex\npattern=^NEW"
        ),
        actor_id=None,
    )
    await service.add_cleaning_rule(
        source.id,
        parse_cleaning_rule_input("kind=contains\nvalue=Order Line"),
        actor_id=None,
    )

    await service.delete_repeated_pattern_rule(source.id, 0, actor_id=None)
    await service.delete_collection_boundary(source.id, 0, actor_id=None)
    profile = await service.delete_cleaning_rule(source.id, 0, actor_id=None)

    assert profile.repeated_pattern_rules == []
    assert profile.collection is not None
    assert profile.collection.boundary_rules == []
    assert profile.cleaning_rules == []


async def test_admin_can_remove_routing_and_examples_without_database_access(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    destination = Destination(name="route", telegram_channel_id=-1000336)
    session.add(destination)
    await session.flush()
    source = await service.create_source(
        name="routing-self-service",
        channel_id=336,
        telegram_ref=None,
        destination_id=destination.id,
        actor_id=None,
    )
    profile = SourceProfileData.model_validate(source.profile.config).model_copy(
        update={
            "category_rules": [CategoryRule(category="dress", keywords=["dress"])],
            "routing_rules": [
                RoutingRule(
                    category="dress",
                    keywords=["dress"],
                    destination_id=destination.id,
                )
            ],
        }
    )
    await service.update_profile(source.id, profile.model_dump(mode="json"), actor_id=None)
    example = await service.add_example(
        source.id, "dress 10 USD", {"price": "10"}, "", actor_id=None
    )

    profile = await service.delete_routing_pair(source.id, 0, actor_id=None)
    await service.disable_example(source.id, example.id, actor_id=None)

    assert profile.category_rules == []
    assert profile.routing_rules == []
    assert await service.examples(source.id, 10) == []


async def test_admin_can_add_and_edit_complete_routing_pair_with_source_isolation(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    destination = Destination(name="routing", telegram_channel_id=-1000337)
    session.add(destination)
    await session.flush()
    source_a = await service.create_source(
        name="routing-a",
        channel_id=337,
        telegram_ref=None,
        destination_id=destination.id,
        actor_id=None,
    )
    source_b = await service.create_source(
        name="routing-b",
        channel_id=338,
        telegram_ref=None,
        destination_id=destination.id,
        actor_id=None,
    )
    category, route = parse_routing_pair_input(
        "category=dress\nkeywords=dress\npattern=(?i)gown\n"
        f"destination_id={destination.id}\npriority=11"
    )

    created = await service.add_routing_pair(
        source_a.id, category, route, actor_id=None
    )
    token = admin_service.profile_item_token(created.routing_rules[0])
    replacement_category, replacement_route = parse_routing_pair_input(
        "category=bag\nkeywords=bag,purse\npattern=(?i)clutch\n"
        f"destination_id={destination.id}\npriority=3"
    )
    updated = await service.replace_routing_pair(
        source_a.id,
        0,
        replacement_category,
        replacement_route,
        expected_token=token,
        actor_id=None,
    )

    assert updated.category_rules == [replacement_category]
    assert updated.routing_rules == [replacement_route]
    untouched = SourceProfileData.model_validate(source_b.profile.config)
    assert untouched.category_rules == []
    assert untouched.routing_rules == []
    with pytest.raises(LookupError, match="routing rule not found"):
        await service.replace_routing_pair(
            source_a.id,
            0,
            category,
            route,
            expected_token=token,
            actor_id=None,
        )


def test_profile_editor_configures_collection_limits_without_json() -> None:
    updated = update_profile_from_lines(
        SourceProfileData(),
        "\n".join(
            [
                "collection_strategy=multi_grouped_id",
                "collection_order=text_between_media",
                "collection_merge_grouped_ids=true",
                "collection_max_time_gap_seconds=120",
                "collection_max_message_id_gap=20",
                "collection_max_messages=18",
                "collection_max_media=30",
                "collection_allow_multiple_text_blocks=true",
                "collection_merge_text_blocks=true",
                "collection_main_text=longest",
            ]
        ),
    )
    assert updated.collection is not None
    assert updated.collection.strategy == "multi_grouped_id"
    assert updated.collection.max_message_id_gap == 20
    assert updated.collection.main_text == "longest"


def test_profile_editor_configures_advertising_filter_without_json() -> None:
    updated = update_profile_from_lines(
        SourceProfileData(),
        "advertising_filter_enabled=false\n"
        "advertising_pattern=(?im)^Partner post:\\s\n"
        "advertising_pattern=(?im)^External ad:\\s",
    )

    assert updated.advertising_filter_enabled is False
    assert updated.advertising_patterns == [
        r"(?im)^Partner post:\s",
        r"(?im)^External ad:\s",
    ]


async def test_admin_can_add_and_delete_multi_price_rule(session: AsyncSession) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="Multi", channel_id=900, telegram_ref=None, destination_id=None, actor_id=None
    )
    rule = parse_multi_price_rule_input(
        "name=items\n"
        r"pattern=(?P<label>A|B)\s*(?P<value>\d+)\$"
        "\n"
        "default_currency=USD\nlabels=A=>А=>A;B=>Б=>B"
    )

    await service.add_multi_price_rule(source.id, rule, actor_id=None)
    configured = SourceProfileData.model_validate(source.profile.config)
    assert [item.name for item in configured.multi_price_rules] == ["items"]

    await service.delete_multi_price_rule(source.id, "items", actor_id=None)
    configured = SourceProfileData.model_validate(source.profile.config)
    assert configured.multi_price_rules == []


def test_profile_settings_are_editable_without_json() -> None:
    updated = update_profile_from_lines(
        SourceProfileData(),
        "\n".join(
            [
                "publication_mode=auto",
                "default_currency=EUR",
                "default_destination_id=17",
                "default_category=women",
                "gemini_enabled=true",
                "ocr_mode=always",
                "text_priority=false",
                "collection_strategy=multi_grouped_id",
                "collection_order=media_before_text",
                "collection_merge_grouped_ids=true",
                "collection_debounce_seconds=2.5",
                "collection_max_time_gap_seconds=120",
                "remove_line=Supplier signature",
                "cleaning_pattern=Advertisement:.*",
                "preserve_pattern=AB-[0-9]+",
            ]
        ),
    )
    assert updated.publication_mode is PublicationMode.AUTO
    assert updated.default_currency == "EUR"
    assert updated.default_destination_id == 17
    assert updated.ocr_mode is OCRMode.ALWAYS
    collection = updated.effective_collection()
    assert collection.strategy == "multi_grouped_id"
    assert collection.order == "media_before_text"
    assert collection.merge_neighbor_grouped_ids is True
    assert collection.debounce_seconds == Decimal("2.5")
    assert collection.max_time_gap_seconds == Decimal("120")
    assert updated.remove_lines == ["Supplier signature"]
    assert updated.cleaning_patterns == ["Advertisement:.*"]
    assert updated.preserve_patterns == ["AB-[0-9]+"]


def test_profile_editor_rejects_settings_that_have_no_runtime_effect() -> None:
    for setting in (
        "translation_enabled=false",
        "album_debounce_seconds=2.5",
        "adjacent_media_merge_seconds=90",
        "adjacent_text_merge_seconds=75",
        "product_collection_mode=media_before_text",
        "product_collection_window_seconds=120",
    ):
        with pytest.raises(ValueError, match="unknown profile setting"):
            update_profile_from_lines(SourceProfileData(), setting)


@pytest.mark.asyncio
async def test_destination_editor_accepts_only_effective_operational_settings(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    destination = await service.create_destination("Old", -1007001, actor_id=None)

    updated = await service.update_destination(
        destination.id,
        {"name": "New", "telegram_channel_id": -1007002, "enabled": False},
        actor_id=None,
    )

    assert updated.name == "New"
    assert updated.telegram_channel_id == -1007002
    assert updated.enabled is False
    for stale_setting in ("signature_ru", "signature_en", "template", "fixed_text"):
        with pytest.raises(ValueError, match="unknown destination settings"):
            await service.update_destination(
                destination.id,
                {stale_setting: "must not be accepted"},
                actor_id=None,
            )


def test_adjacent_media_merge_window_defaults_off_and_is_bounded() -> None:
    assert SourceProfileData().adjacent_media_merge_seconds == Decimal("0")
    with pytest.raises(ValueError):
        SourceProfileData(adjacent_media_merge_seconds=Decimal("301"))


def test_adjacent_text_merge_window_defaults_off_and_is_bounded() -> None:
    assert SourceProfileData().adjacent_text_merge_seconds == Decimal("0")
    with pytest.raises(ValueError):
        SourceProfileData(adjacent_text_merge_seconds=Decimal("301"))


def test_key_value_editor_rejects_unknown_or_duplicate_keys() -> None:
    with pytest.raises(ValueError, match="unknown profile setting"):
        update_profile_from_lines(SourceProfileData(), "python_code=exec('x')")
    with pytest.raises(ValueError, match="duplicate key"):
        parse_key_value_lines("name=one\nname=two")


def test_admin_updates_required_fields_without_json() -> None:
    updated = update_profile_from_lines(
        SourceProfileData(),
        "required_fields= price, sizes ",
    )

    assert updated.required_fields == ["price", "sizes"]


def test_admin_rejects_empty_required_fields() -> None:
    with pytest.raises(ValidationError):
        update_profile_from_lines(SourceProfileData(), "required_fields=")


def test_admin_parses_series_rule_without_json_or_supplier_code() -> None:
    parse_extraction_rule_input = getattr(admin_service, "parse_extraction_rule_input", None)
    assert parse_extraction_rule_input is not None
    field, rule = parse_extraction_rule_input(
        "\n".join(
            [
                "field=quantity",
                "kind=regex",
                r"pattern=(?i)\bSeries:\s*(?P<value>(?:[A-Z]+\d+\s*)+)",
                "transform=series_quantity",
                r"token_pattern=(?P<size>[A-Z]+)(?P<count>\d+)",
                "priority=10",
                "authoritative=true",
            ]
        )
    )

    assert field == "quantity"
    assert rule.transform == "series_quantity"
    assert rule.priority == 10
    assert rule.authoritative is True


def test_admin_parses_value_mapping_and_legacy_rule_format() -> None:
    parse_extraction_rule_input = getattr(admin_service, "parse_extraction_rule_input", None)
    assert parse_extraction_rule_input is not None
    field, mapped = parse_extraction_rule_input(
        "\n".join(
            [
                "field=sizes",
                "kind=regex",
                r"pattern=Seri\s+\d+\s+li\s+(?P<value>Standart)",
                "transform=map",
                "value_map=Standart=>Standard;STD=>Standard",
            ]
        )
    )
    legacy_field, legacy = parse_extraction_rule_input("article|label|Артикул")

    assert field == "sizes"
    assert mapped.value_map == {"Standart": "Standard", "STD": "Standard"}
    assert legacy_field == "article"
    assert legacy.kind == "label"


def test_admin_parses_source_ignore_rule_without_json() -> None:
    rule = parse_source_ignore_rule_input(
        "\n".join(
            [
                "name=article-only-media",
                "media_required=true",
                "found_fields=article",
                "missing_fields=price,sizes,quantity",
            ]
        )
    )

    assert rule.name == "article-only-media"
    assert rule.media_required is True
    assert rule.found_fields == ["article"]
    assert rule.missing_fields == ["price", "sizes", "quantity"]


def test_legacy_regex_with_equals_is_not_misread_as_key_value_input() -> None:
    parse_extraction_rule_input = getattr(admin_service, "parse_extraction_rule_input", None)
    assert parse_extraction_rule_input is not None

    field, rule = parse_extraction_rule_input(
        r"article|regex|Code:\s*(?=[A-Z])(?P<value>[A-Z0-9]+)"
    )

    assert field == "article"
    assert rule.kind == "regex"


@pytest.mark.parametrize(
    ("text", "expected_field", "changed_field", "expected_value"),
    [
        ("field=quantity", "quantity", None, None),
        ("kind=label", "sizes", "kind", "label"),
        (
            r"pattern=(?i)Sizes\s*:\s*(?P<value>STANDART)",
            "sizes",
            "pattern",
            r"(?i)Sizes\s*:\s*(?P<value>STANDART)",
        ),
        ("aliases=Size,Sizes", "sizes", "aliases", ["Size", "Sizes"]),
        ("priority=7", "sizes", "priority", 7),
        ("ignore_case=false", "sizes", "ignore_case", False),
        ("transform=series_sizes", "sizes", "transform", "series_sizes"),
        (
            r"token_pattern=(?P<size>[A-Z]+)(?P<count>\d+)",
            "sizes",
            "token_pattern",
            r"(?P<size>[A-Z]+)(?P<count>\d+)",
        ),
        (
            "value_map=STANDART=>Standard;STD=>Standard",
            "sizes",
            "value_map",
            {"STANDART": "Standard", "STD": "Standard"},
        ),
        ("authoritative=false", "sizes", "authoritative", False),
        ("enabled=false", "sizes", "enabled", False),
    ],
)
def test_admin_edit_patch_changes_only_the_requested_extraction_rule_setting(
    text: str,
    expected_field: str,
    changed_field: str | None,
    expected_value: object,
) -> None:
    parse_edit = getattr(admin_service, "parse_extraction_rule_edit_input", None)
    assert parse_edit is not None
    current = ExtractionRule(
        kind="regex",
        pattern=r"OLD:\s*(?P<value>[^\n]+)",
        aliases=["Old"],
        priority=10,
        ignore_case=True,
        transform="identity",
        token_pattern=r"(?P<size>[A-Za-z]+)\s*(?P<count>\d+)",
        value_map={"Old": "Existing"},
        authoritative=True,
        enabled=True,
    )

    field, updated = parse_edit(text, current_field="sizes", current_rule=current)

    assert field == expected_field
    expected = (
        current
        if changed_field is None
        else current.model_copy(update={changed_field: expected_value})
    )
    assert updated == expected


def test_admin_edit_accepts_the_exact_single_line_production_rule_form() -> None:
    parse_edit = getattr(admin_service, "parse_extraction_rule_edit_input", None)
    assert parse_edit is not None
    current = ExtractionRule(
        kind="regex",
        pattern=r"OLD:\s*(?P<value>[^\n]+)",
        priority=100,
        authoritative=False,
    )

    field, updated = parse_edit(
        "field=sizes kind=regex "
        r"pattern=(?i)\bSizes\s*:\s*(?P<value>STANDART|STANDARD)\s+\d+\spcs\b "
        "transform=identity priority=10 authoritative=true ignore_case=true "
        r"token_pattern=(?P<size>[A-Za-zА-Яа-я]+)\s*(?P<count>\d+)",
        current_field="sizes",
        current_rule=current,
    )

    assert field == "sizes"
    assert updated.pattern == (
        r"(?i)\bSizes\s*:\s*(?P<value>STANDART|STANDARD)\s+\d+\spcs\b"
    )
    assert updated.priority == 10
    assert updated.authoritative is True
    assert updated.ignore_case is True


def test_admin_parses_fallback_add_and_delete_without_json() -> None:
    parse_source_fallback_input = getattr(admin_service, "parse_source_fallback_input", None)
    assert parse_source_fallback_input is not None
    field, fallback = parse_source_fallback_input("field=sizes\nvalue=S, M, L\nauthoritative=true")
    deleted_field, deleted = parse_source_fallback_input("field=sizes\ndelete=true")

    assert field == "sizes"
    assert fallback is not None and fallback.value == "S, M, L"
    assert fallback.authoritative is True
    assert deleted_field == "sizes"
    assert deleted is None


def test_admin_parses_group_fallback_condition() -> None:
    parse_source_fallback_input = getattr(admin_service, "parse_source_fallback_input", None)
    assert parse_source_fallback_input is not None

    _, fallback = parse_source_fallback_input(
        "field=quantity\nvalue=6\nwhen_all_missing=sizes,quantity\nauthoritative=true"
    )

    assert fallback is not None
    assert fallback.when_all_missing == ["sizes", "quantity"]


def test_admin_parses_missing_only_ignore_rule_without_found_fields() -> None:
    rule = parse_source_ignore_rule_input(
        "name=missing-price-and-sizes\n"
        "media_required=false\n"
        "description_missing_required=false\n"
        "missing_fields=price,sizes"
    )

    assert rule.found_fields == []
    assert rule.missing_fields == ["price", "sizes"]
    assert rule.enabled is True


async def test_admin_can_add_and_remove_source_rule_and_fallback(session: AsyncSession) -> None:
    parse_extraction_rule_input = getattr(admin_service, "parse_extraction_rule_input", None)
    parse_source_fallback_input = getattr(admin_service, "parse_source_fallback_input", None)
    assert parse_extraction_rule_input is not None
    assert parse_source_fallback_input is not None
    service = AdminService(session)
    source = await service.create_source(
        name="Configurable", channel_id=808, telegram_ref=None, destination_id=None, actor_id=None
    )
    field, rule = parse_extraction_rule_input(
        "field=quantity\nkind=regex\npattern=Pack:\\s*(?P<value>\\d+)"
    )
    fallback_field, fallback = parse_source_fallback_input(
        "field=sizes\nvalue=Standard\nauthoritative=true"
    )
    ignore_rule = parse_source_ignore_rule_input(
        "name=article-only-media\nfound_fields=article\nmissing_fields=price,sizes,quantity"
    )
    assert fallback is not None

    await service.add_source_rule(source.id, field, rule, actor_id=None)
    await service.set_source_fallback(source.id, fallback_field, fallback, actor_id=None)
    await service.add_source_ignore_rule(source.id, ignore_rule, actor_id=None)
    configured = SourceProfileData.model_validate(source.profile.config)
    assert configured.extraction_rules["quantity"][0].pattern.startswith("Pack")
    assert configured.fallback_rules["sizes"].value == "Standard"
    assert configured.ignore_rules[0].name == "article-only-media"

    await service.delete_source_rule(source.id, "quantity", 0, actor_id=None)
    await service.set_source_fallback(source.id, "sizes", None, actor_id=None)
    await service.delete_source_ignore_rule(source.id, "article-only-media", actor_id=None)
    configured = SourceProfileData.model_validate(source.profile.config)
    assert "quantity" not in configured.extraction_rules
    assert "sizes" not in configured.fallback_rules
    assert configured.ignore_rules == []


def test_admin_parses_url_and_catalog_buttons_without_json() -> None:
    url_button, url_destinations = parse_navigation_button_input(
        "🛒 Заказать|https://wa.me/90555|0|0|1,2", ButtonAction.URL
    )
    catalog_button, catalog_destinations = parse_navigation_button_input(
        "📂 Каталог|0|1|2,3", ButtonAction.CATALOG
    )

    assert url_button.url == "https://wa.me/90555"
    assert url_destinations == [1, 2]
    assert catalog_button.url is None
    assert catalog_destinations == [2, 3]


def test_admin_parses_catalog_category_and_link_without_json() -> None:
    category = parse_catalog_node_input(
        "Женская одежда|Women's clothing|1", CatalogNodeType.CATEGORY
    )
    link = parse_catalog_node_input(
        "Платья|Dresses|https://t.me/e_moda_female|2", CatalogNodeType.LINK
    )

    assert category.position == 1
    assert category.text_ru == "Женская одежда"
    assert category.text_en == "Women's clothing"
    assert category.url is None
    assert link.text_en == "Dresses"
    assert link.url == "https://t.me/e_moda_female"


def test_admin_button_input_requires_destinations_and_rejects_duplicates() -> None:
    with pytest.raises(ValueError, match="destination"):
        parse_navigation_button_input("📂 Каталог|0|1|", ButtonAction.CATALOG)
    with pytest.raises(ValueError, match="duplicate"):
        parse_navigation_button_input("📂 Каталог|0|1|2,2", ButtonAction.CATALOG)


async def test_source_can_be_renamed_and_repointed_by_validated_service(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="Old", channel_id=1, telegram_ref="@old", destination_id=None, actor_id=None
    )
    await service.update_source(
        source.id,
        name="New",
        channel_id=2,
        telegram_ref="@new",
        actor_id=None,
    )
    assert source.name == "New"
    assert source.telegram_channel_id == 2
    assert source.telegram_ref == "@new"


async def test_button_can_be_disabled_and_reenabled_without_recreating_it(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    destination = await service.create_destination("D", -1001, actor_id=None)
    button = await service.create_button(
        NavigationButtonData(text="🛍 Shop", url="https://example.com", row=1, order=2),
        [destination.id],
        actor_id=None,
    )
    await service.set_button_enabled(button.id, False, actor_id=None)
    assert button.enabled is False
    await service.set_button_enabled(button.id, True, actor_id=None)
    assert button.enabled is True


async def test_catalog_button_is_created_without_external_url(session: AsyncSession) -> None:
    service = AdminService(session)
    destination = await service.create_destination("Catalog destination", -1002, actor_id=None)

    button = await service.create_button(
        NavigationButtonData(
            text="📂 Каталог",
            action=ButtonAction.CATALOG,
            row=0,
            order=1,
        ),
        [destination.id],
        actor_id=None,
    )

    assert button.action is ButtonAction.CATALOG
    assert button.url is None


async def test_admin_creates_nested_catalog_without_json(session: AsyncSession) -> None:
    service = AdminService(session)
    root = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Женская одежда",
            text_en="Women's clothing",
            node_type=CatalogNodeType.CATEGORY,
        ),
        parent_id=None,
        actor_id=None,
    )
    subsection = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Платья",
            text_en="Dresses",
            node_type=CatalogNodeType.CATEGORY,
            position=1,
        ),
        parent_id=root.id,
        actor_id=None,
    )
    link = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Открыть канал",
            text_en="Open channel",
            node_type=CatalogNodeType.LINK,
            url="https://t.me/e_moda_female",
        ),
        parent_id=subsection.id,
        actor_id=None,
    )

    assert link.parent_id == subsection.id
    assert link.url == "https://t.me/e_moda_female"


async def test_catalog_link_cannot_contain_children(session: AsyncSession) -> None:
    service = AdminService(session)
    link = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Ссылка",
            text_en="Leaf",
            node_type=CatalogNodeType.LINK,
            url="https://t.me/catalog",
        ),
        parent_id=None,
        actor_id=None,
    )

    with pytest.raises(ValueError, match="category"):
        await service.create_catalog_node(
            CatalogNodeData(
                text_ru="Ошибочный пункт",
                text_en="Invalid child",
                node_type=CatalogNodeType.CATEGORY,
            ),
            parent_id=link.id,
            actor_id=None,
        )


async def test_catalog_node_can_be_edited_disabled_and_archived(session: AsyncSession) -> None:
    service = AdminService(session)
    node = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Старое",
            text_en="Old",
            node_type=CatalogNodeType.CATEGORY,
        ),
        parent_id=None,
        actor_id=None,
    )
    await service.update_catalog_node(
        node.id,
        CatalogNodeData(
            text_ru="Новое",
            text_en="New",
            node_type=CatalogNodeType.CATEGORY,
            position=4,
        ),
        actor_id=None,
    )
    await service.set_catalog_node_enabled(node.id, False, actor_id=None)
    await service.archive_catalog_node(node.id, actor_id=None)

    stored = await session.get(CatalogNode, node.id)
    assert stored is not None
    assert stored.text_ru == "Новое"
    assert stored.text_en == "New"
    assert stored.position == 4
    assert stored.enabled is False
    assert stored.archived_at is not None


async def test_archived_catalog_root_is_restored_in_place_with_its_tree(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    root = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Женская одежда",
            text_en="Women's clothing",
            node_type=CatalogNodeType.CATEGORY,
            position=2,
        ),
        parent_id=None,
        actor_id=None,
    )
    child = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Платья",
            text_en="Dresses",
            node_type=CatalogNodeType.LINK,
            url="https://t.me/e_moda_female",
            position=3,
        ),
        parent_id=root.id,
        actor_id=None,
    )
    root_id = root.id
    child_id = child.id

    await service.archive_catalog_node(root_id, actor_id=None)
    restored = await service.restore_catalog_node(root_id, actor_id=None)

    stored_child = await session.get(CatalogNode, child_id)
    assert restored.id == root_id
    assert restored.text_ru == "Женская одежда"
    assert restored.text_en == "Women's clothing"
    assert restored.position == 2
    assert restored.archived_at is None
    assert restored.enabled is True
    assert stored_child is not None
    assert stored_child.id == child_id
    assert stored_child.parent_id == root_id
    assert stored_child.url == "https://t.me/e_moda_female"


async def test_catalog_child_cannot_be_restored_beneath_archived_parent(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    root = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Корень",
            text_en="Root",
            node_type=CatalogNodeType.CATEGORY,
        ),
        parent_id=None,
        actor_id=None,
    )
    child = await service.create_catalog_node(
        CatalogNodeData(
            text_ru="Дочерний",
            text_en="Child",
            node_type=CatalogNodeType.CATEGORY,
        ),
        parent_id=root.id,
        actor_id=None,
    )
    await service.archive_catalog_node(root.id, actor_id=None)
    await service.archive_catalog_node(child.id, actor_id=None)

    with pytest.raises(ValueError, match="restore its parent first"):
        await service.restore_catalog_node(child.id, actor_id=None)


async def test_product_search_uses_article_model_and_telegram_message_id(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="Supplier", channel_id=123, telegram_ref=None, destination_id=None, actor_id=None
    )
    message = SourceMessage(
        source_id=source.id,
        telegram_message_id=987654,
        text="Model AB-123",
        telegram_date=utcnow(),
    )
    session.add(message)
    await session.flush()
    product = Product(
        emoda_code="EM-000123",
        source_id=source.id,
        source_message_id=message.id,
        status=ProductStatus.NEEDS_REVIEW,
        original_text="Nike model AB-123",
        parsed_result={"fields": {"article": {"value": "AB-123"}}},
    )
    session.add(product)
    await session.flush()
    new_format_product = Product(
        emoda_code="XY54321",
        source_id=source.id,
        status=ProductStatus.NEEDS_REVIEW,
        original_text="Second product",
    )
    session.add(new_format_product)
    await session.flush()
    assert [item.id for item in await service.find_products("AB-123")] == [product.id]
    assert [item.id for item in await service.find_products("987654")] == [product.id]
    assert [item.id for item in await service.find_products("em-000123")] == [product.id]
    assert [item.id for item in await service.find_products("xy54321")] == [new_format_product.id]


async def test_numeric_search_keeps_album_message_and_json_matches(session: AsyncSession) -> None:
    service = AdminService(session)
    source = await service.create_source(
        name="Search supplier", channel_id=7755, telegram_ref=None,
        destination_id=None, actor_id=None,
    )
    album = AlbumGroup(source_id=source.id, grouped_id=12, status=AlbumStatus.READY,
                       last_seen_at=utcnow(), ready_at=utcnow())
    session.add(album)
    await session.flush()
    message = SourceMessage(source_id=source.id, telegram_message_id=7728,
                            album_group_id=album.id, telegram_date=utcnow())
    session.add(message)
    await session.flush()
    products = [
        Product(emoda_code="ZZ00001", source_id=source.id, album_group_id=album.id),
        Product(emoda_code="ZZ00002", source_id=source.id, source_message_id=message.id),
        Product(emoda_code="ZZ00003", source_id=source.id,
                parsed_result={"fields": {"article": {"value": "7728"}}}),
        Product(emoda_code="ZZ00004", source_id=source.id, original_text="model: 7728"),
    ]
    session.add_all(products)
    await session.flush()
    found = await service.find_products("7728")
    assert {p.id for p in found} == {p.id for p in products}
    assert len(found) == 4
    assert [p.id for p in await service.find_products("missing-article")] == []


async def test_admin_cannot_change_fixed_eur_rate(session: AsyncSession) -> None:
    with pytest.raises(ValueError, match="fixed at 1.2"):
        await AdminService(session).set_setting("eur_usd_rate", {"value": "1.3"}, actor_id=None)
