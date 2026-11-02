from datetime import UTC, datetime, timedelta
from decimal import Decimal

from emoda.domain.schemas import (
    CollectionBoundaryRule,
    CollectionProfile,
    RepeatedPatternRule,
    SourceProfileData,
)
from emoda.rules.engine import RuleEngine
from emoda.services.collection import CollectionEngine, CollectionMessage

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


def _message(
    message_id: int,
    *,
    grouped_id: str | None = None,
    text: str = "",
    media: int = 0,
) -> CollectionMessage:
    return CollectionMessage(
        message_id=message_id,
        occurred_at=NOW + timedelta(seconds=message_id),
        grouped_id=grouped_id,
        text=text,
        media_count=media,
    )


def test_lastpoint_fixture_joins_several_grouped_ids_from_profile_only() -> None:
    profile = CollectionProfile(
        strategy="multi_grouped_id",
        order="text_between_media",
        merge_neighbor_grouped_ids=True,
        max_time_gap_seconds=Decimal("60"),
        max_message_id_gap=10,
        allow_multiple_text_blocks=True,
        merge_text_blocks=True,
    )
    result = CollectionEngine(profile).partition(
        [
            _message(1, grouped_id="first", media=4),
            _message(2, text="Price 13 USD\nSizes S M L"),
            _message(3, grouped_id="second", media=3),
        ]
    )
    assert [[row.message_id for row in item.messages] for item in result.products] == [
        [1, 2, 3]
    ]


def test_mortopuk_fixture_stops_media_before_text_at_next_product() -> None:
    profile = CollectionProfile(
        strategy="multi_grouped_id",
        order="media_before_text",
        merge_neighbor_grouped_ids=True,
        max_time_gap_seconds=Decimal("60"),
        max_message_id_gap=10,
        max_messages=8,
        max_media=12,
        boundary_rules=[
            CollectionBoundaryRule(
                name="product-caption",
                action="hard_boundary",
                match="regex",
                pattern=r"^PRODUCT [A-Z]$",
            )
        ],
    )
    result = CollectionEngine(profile).partition(
        [
            _message(10, grouped_id="a", media=4),
            _message(11, grouped_id="b", media=4),
            _message(12, text="PRODUCT A"),
            _message(13, grouped_id="c", media=4),
            _message(14, text="PRODUCT B"),
        ]
    )
    assert [[row.message_id for row in item.messages] for item in result.products] == [
        [10, 11, 12],
        [13, 14],
    ]
    assert max(len(item.messages) for item in result.products) < 8


def test_strong_eagle_fixture_keeps_neighbor_products_and_info_separate() -> None:
    profile = CollectionProfile(
        strategy="multi_grouped_id",
        order="media_before_text",
        merge_neighbor_grouped_ids=True,
        max_time_gap_seconds=Decimal("120"),
        max_message_id_gap=20,
        boundary_rules=[
            CollectionBoundaryRule(
                name="info",
                action="ignore",
                match="regex",
                pattern=r"^INFO:",
                reason="information post",
            ),
            CollectionBoundaryRule(
                name="next-caption",
                action="hard_boundary",
                match="regex",
                pattern=r"^#[A-Z][A-Z ]+$",
            ),
        ],
    )
    result = CollectionEngine(profile).partition(
        [
            _message(20, text="INFO: schedule"),
            _message(21, grouped_id="a", media=3),
            _message(22, text="#LUFIAN"),
            _message(23, grouped_id="b", media=3),
            _message(24, text="#CALVIN KLEIN"),
            _message(25, grouped_id="c", media=3),
            _message(26, text="#PRADA"),
        ]
    )
    assert [row.message_id for row in result.ignored] == [20]
    assert [[row.message_id for row in item.messages] for item in result.products] == [
        [21, 22],
        [23, 24],
        [25, 26],
    ]


def test_romaska_fixture_extracts_range_and_price_variants_from_profile_only() -> None:
    profile = SourceProfileData(
        repeated_pattern_rules=[
            RepeatedPatternRule(
                name="range-price",
                pattern=r"(?m)^(?P<size>\d+-\d+)-(?P<price>\d+)$",
                fields={"sizes": "size", "price": "price"},
                aggregation={"sizes": "unique", "price": "join_unique"},
                separators={"price": "/"},
                authoritative=True,
            )
        ]
    )
    parsed = RuleEngine(profile).parse("42-48-27\n42-48-30")
    assert parsed.fields["sizes"].value == "42-48"
    assert parsed.fields["price"].value == "27/30"
