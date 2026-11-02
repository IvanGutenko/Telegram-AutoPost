from decimal import Decimal

import pytest

from emoda.admin.service import parse_extraction_rule_edit_input, parse_multi_price_rule_input
from emoda.domain.enums import FieldStatus
from emoda.domain.schemas import (
    CollectionProfile,
    ComputedRule,
    ExtractionRule,
    RepeatedPatternRule,
    SourceFallbackRule,
    SourceProfileData,
)
from emoda.rules.engine import RuleEngine, extract_multi_price_candidates


def test_collection_accepts_200_items_and_seven_days():
    profile = CollectionProfile(max_messages=200, max_media=200, max_time_gap_seconds="604800")
    assert profile.max_time_gap_seconds == Decimal("604800")


@pytest.mark.parametrize("input_source", ["field", "raw_field_candidate"])
@pytest.mark.parametrize("second,expected", [("50 52 54 56", "4"), ("50 52 54 56 58", None)])
def test_alternative_repeated_rows_feed_computed_quantity(input_source, second, expected):
    profile = SourceProfileData(
        repeated_pattern_rules=[RepeatedPatternRule(
            name="size-rows", pattern=r"(?m)^(?P<sizes>\d+(?: +\d+)+) +\d+\$$",
            fields={"sizes": "sizes"}, aggregation={"sizes": "join_unique"},
            separators={"sizes": " / "}, authoritative=True,
        )],
        computed_rules=[ComputedRule(
            name="series-count", target_field="quantity", operation="count_series_items",
            source_field="sizes", input_source=input_source, series_separator_pattern="/",
            range_pattern=r"(?P<start>\d+)-(?P<end>\d+)", numeric_step="2",
            explicit_list_pattern=r"(?P<item>\d+)", explicit_separator_pattern=r"\s+",
        )],
    )
    result = RuleEngine(profile).parse(f"42 44 46 48 29$\n{second} 30$")
    quantity = result.fields["quantity"]
    assert quantity.value == expected
    assert quantity.status == (FieldStatus.FOUND if expected else FieldStatus.AMBIGUOUS)


def test_dynamic_multi_price_is_explicit_and_keeps_size_price_pairs():
    rule = parse_multi_price_rule_input(
        "name=size-prices\nlabel_mode=dynamic\n"
        r"pattern=(?m)^(?P<label>\d+(?: +\d+)+) +(?P<value>\d+)\$$"
        "\ndefault_currency=USD"
    )
    candidates, errors = extract_multi_price_candidates("42 44 46 48 29$\n50 52 54 56 30$", [rule])
    assert errors == []
    assert [(c.label_original, c.value) for c in candidates] == [
        ("42 44 46 48", "29"), ("50 52 54 56", "30"),
    ]


def test_fallbacks_independent_but_cannot_create_product_evidence():
    profile = SourceProfileData(fallback_rules={
        "sizes": SourceFallbackRule(value="SS MM LL"),
        "quantity": SourceFallbackRule(value="6"),
    })
    engine = RuleEngine(profile)
    assert engine.parse("Price: 20 USD").fields["sizes"].value == "SS MM LL"
    assert engine.parse("Price: 20 USD").fields["quantity"].value == "6"
    assert engine.parse("Visit our shop").fields["quantity"].status == FieldStatus.MISSING


def test_edit_pattern_alone_is_not_interpreted_as_field():
    pattern = r"(?i)Sizes\s*:\s*(?P<value>STANDART)\s*4PCS"
    field, rule = parse_extraction_rule_edit_input(
        pattern, current_field="sizes", current_rule=ExtractionRule(kind="regex", pattern="old")
    )
    assert field == "sizes"
    assert rule.pattern == pattern


def test_product_name_is_bounded_and_independent_of_legacy_name():
    fields = RuleEngine(SourceProfileData()).parse(
        "Name: advertising heading\nProduct name: Cotton dress\nPrice: 20 USD"
    ).fields
    assert fields["product_name"].value == "Cotton dress"


def test_structural_ignore_accepts_solo_media_but_not_album_or_text():
    from emoda.admin.service import parse_source_ignore_rule_input
    from emoda.rules.engine import match_structural_ignore_rule
    rule = parse_source_ignore_rule_input(
        "name=solo-media\ngrouped_id=missing\nmedia_types=photo,video\n"
        "description_missing_required=false"
    )
    for kind in ("photo", "video"):
        assert match_structural_ignore_rule(
            [rule], grouped_id=None, media_type=kind, media_count=1, message_count=1,
        ) == rule
    assert match_structural_ignore_rule(
        [rule], grouped_id=123, media_type="photo", media_count=1, message_count=1,
    ) is None
    assert match_structural_ignore_rule(
        [rule], grouped_id=None, media_type=None, media_count=0, message_count=1,
    ) is None


def test_media_type_with_text_conditions_is_a_post_collection_ignore_rule():
    from emoda.admin.service import parse_source_ignore_rule_input
    from emoda.rules.engine import match_structural_ignore_rule

    rule = parse_source_ignore_rule_input(
        "name=video-no-text\n"
        "media_types=video\n"
        "media_required=true\n"
        "description_missing_required=true\n"
        "missing_fields=price,sizes,quantity"
    )

    assert rule.has_structure() is False
    assert match_structural_ignore_rule(
        [rule],
        grouped_id=None,
        media_type="video",
        media_count=1,
        message_count=1,
    ) is None


def test_telegram_link_preview_is_not_product_media():
    from types import SimpleNamespace

    from telethon.tl.types import MessageMediaWebPage, WebPageEmpty

    from emoda.integrations.collector import telegram_product_media_type
    message = SimpleNamespace(media=MessageMediaWebPage(webpage=WebPageEmpty(id=1)), video=None)
    assert telegram_product_media_type(message) is None


def test_review_diagnostics_identifies_stage_field_rule_and_reason():
    from emoda.services.review import review_reason_details
    result = review_reason_details({"code": "collection_ambiguous", "detail": "two possible anchors"})
    assert all(word in result for word in ("collection", "field=", "rule=", "two possible anchors"))


def test_dry_run_structural_counts_use_complete_telegram_album():
    from types import SimpleNamespace

    from emoda.admin.router import partition_test_inputs
    from emoda.domain.schemas import SourceIgnoreRule
    profile = SourceProfileData(ignore_rules=[SourceIgnoreRule(name="small-albums",
        grouped_id="present", max_media=2, description_missing_required=False)])
    items = [SimpleNamespace(telegram_message_id=i, media_group_id="99", text="", media_paths=[f"{i}.jpg"])
             for i in range(3)]
    assert len(partition_test_inputs(items, profile=profile)) == 1
