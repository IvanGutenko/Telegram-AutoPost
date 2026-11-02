from datetime import UTC, datetime, timedelta
from decimal import Decimal

from emoda.domain.schemas import CollectionBoundaryRule, CollectionProfile
from emoda.services.collection import CollectionEngine, CollectionMessage

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=UTC)


def message(
    message_id: int,
    *,
    seconds: int = 0,
    grouped_id: str | None = None,
    text: str = "",
    media: int = 0,
) -> CollectionMessage:
    return CollectionMessage(
        message_id=message_id,
        occurred_at=NOW + timedelta(seconds=seconds),
        grouped_id=grouped_id,
        text=text,
        media_count=media,
    )


def test_multi_group_profile_collects_media_text_media_as_one_product() -> None:
    engine = CollectionEngine(
        CollectionProfile(
            strategy="multi_grouped_id",
            order="text_between_media",
            merge_neighbor_grouped_ids=True,
            max_time_gap_seconds=Decimal("60"),
            max_message_id_gap=10,
            allow_multiple_text_blocks=True,
            merge_text_blocks=True,
        )
    )
    messages = [
        message(10, grouped_id="a", media=3),
        message(11, seconds=2, text="details"),
        message(12, seconds=4, grouped_id="b", media=2),
    ]
    result = engine.partition(messages)
    assert [[item.message_id for item in batch.messages] for batch in result.products] == [
        [10, 11, 12]
    ]


def test_hard_next_product_boundary_separates_neighboring_brands_and_ignores_info() -> None:
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
                pattern=r"(?i)^INFO:",
                reason="information post",
            ),
            CollectionBoundaryRule(
                name="product-caption",
                action="hard_boundary",
                match="regex",
                pattern=r"(?m)^#[A-Z][A-Z ]+$",
            ),
        ],
    )
    result = CollectionEngine(profile).partition(
        [
            message(20, text="INFO: schedule"),
            message(21, grouped_id="l1", media=4),
            message(22, text="#LUFIAN"),
            message(23, grouped_id="c1", media=3),
            message(24, text="#CALVIN KLEIN"),
            message(25, grouped_id="p1", media=2),
            message(26, text="#PRADA"),
        ]
    )
    assert [item.message_id for item in result.ignored] == [20]
    assert [[item.message_id for item in batch.messages] for batch in result.products] == [
        [21, 22],
        [23, 24],
        [25, 26],
    ]


def test_continuation_overrides_soft_boundary_but_never_hard_boundary() -> None:
    engine = CollectionEngine(
        CollectionProfile(
            strategy="multi_grouped_id",
            merge_neighbor_grouped_ids=True,
            max_time_gap_seconds=Decimal("30"),
            allow_multiple_text_blocks=True,
            merge_text_blocks=True,
            boundary_rules=[
                CollectionBoundaryRule(
                    name="all-text",
                    action="soft_boundary",
                    match="has_text",
                ),
                CollectionBoundaryRule(
                    name="details",
                    action="continue",
                    match="keyword",
                    keywords=["details"],
                ),
                CollectionBoundaryRule(
                    name="stop",
                    action="hard_boundary",
                    match="keyword",
                    keywords=["STOP"],
                ),
            ],
        )
    )
    result = engine.partition(
        [
            message(1, grouped_id="a", media=1),
            message(2, seconds=1, text="details: cotton"),
            message(3, seconds=2, text="STOP details"),
        ]
    )
    assert [[item.message_id for item in batch.messages] for batch in result.products] == [
        [1, 2],
        [3],
    ]


def test_time_id_message_and_media_limits_stop_merge() -> None:
    base = CollectionProfile(
        strategy="multi_grouped_id",
        merge_neighbor_grouped_ids=True,
        max_time_gap_seconds=Decimal("5"),
        max_message_id_gap=2,
        max_messages=2,
        max_media=2,
    )
    assert len(
        CollectionEngine(base).partition(
            [message(1, media=1), message(2, seconds=6, media=1)]
        ).products
    ) == 2
    assert len(
        CollectionEngine(base).partition([message(1, media=1), message(4, media=1)]).products
    ) == 2
    assert len(
        CollectionEngine(base).partition(
            [message(1, media=1), message(2, media=1), message(3)]
        ).products
    ) == 2
    assert len(
        CollectionEngine(base).partition([message(1, media=2), message(2, media=1)]).products
    ) == 2


def test_single_grouped_id_never_merges_neighboring_albums() -> None:
    result = CollectionEngine(CollectionProfile()).partition(
        [message(1, grouped_id="a", media=2), message(2, grouped_id="b", media=2)]
    )
    assert [[item.message_id for item in batch.messages] for batch in result.products] == [
        [1],
        [2],
    ]


def test_main_text_policy_and_text_merge_are_profile_driven() -> None:
    engine = CollectionEngine(
        CollectionProfile(
            strategy="multi_grouped_id",
            merge_neighbor_grouped_ids=True,
            allow_multiple_text_blocks=True,
            merge_text_blocks=True,
            main_text="longest",
        )
    )
    batch = engine.partition(
        [message(1, text="short"), message(2, text="a much longer caption")]
    ).products[0]
    assert engine.main_text(batch.messages) == "a much longer caption"
    assert engine.merged_text(batch.messages) == "short\na much longer caption"
