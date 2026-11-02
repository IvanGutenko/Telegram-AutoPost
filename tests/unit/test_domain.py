from decimal import Decimal

import pytest
from pydantic import ValidationError

from emoda.domain.enums import FieldStatus, OCRMode, PublicationMode, ReviewReasonCode
from emoda.domain.schemas import (
    CleaningRule,
    CollectionBoundaryRule,
    CollectionProfile,
    ExtractedField,
    ExtractionRule,
    MultiPriceRule,
    NavigationButtonData,
    RepeatedPatternRule,
    ReviewReason,
    SourceIgnoreRule,
    SourceProfileData,
)


def test_profile_v2_exposes_validated_generic_engines_and_loads_legacy_config() -> None:
    configured = SourceProfileData(
        schema_version=2,
        collection=CollectionProfile(
            strategy="multi_grouped_id",
            order="text_between_media",
            merge_neighbor_grouped_ids=True,
            max_time_gap_seconds=Decimal("90"),
            max_message_id_gap=12,
            max_messages=16,
            max_media=24,
            boundary_rules=[
                CollectionBoundaryRule(
                    name="next-product",
                    action="hard_boundary",
                    match="regex",
                    pattern=r"(?im)^NEW\s+PRODUCT\b",
                )
            ],
        ),
        cleaning_rules=[CleaningRule(kind="prefix", value="Service:")],
        repeated_pattern_rules=[
            RepeatedPatternRule(
                name="size-price-series",
                pattern=r"(?m)^(?P<size>\d+-\d+)-(?P<price>\d+)$",
                fields={"sizes": "size", "price": "price"},
                aggregation={"sizes": "unique", "price": "join_unique"},
                separators={"price": "/"},
            )
        ],
    )
    assert configured.collection.max_message_id_gap == 12
    assert configured.cleaning_rules[0].kind == "prefix"
    assert configured.repeated_pattern_rules[0].fields == {
        "sizes": "size",
        "price": "price",
    }

    legacy = SourceProfileData.model_validate(
        {
            "schema_version": 1,
            "product_collection_mode": "media_before_text",
            "product_collection_window_seconds": "45",
        }
    )
    effective = legacy.effective_collection()
    assert effective.strategy == "multi_grouped_id"
    assert effective.order == "media_before_text"
    assert effective.max_time_gap_seconds == Decimal("45")


@pytest.mark.parametrize(
    "payload",
    [
        {"name": "bad", "action": "hard_boundary", "match": "regex", "pattern": "["},
        {"name": "bad", "action": "continue", "match": "keyword", "keywords": []},
    ],
)
def test_collection_boundary_rejects_invalid_match_configuration(
    payload: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        CollectionBoundaryRule.model_validate(payload)


def test_review_reason_is_a_stable_structured_value() -> None:
    reason = ReviewReason(
        code=ReviewReasonCode.PARSER_AMBIGUOUS,
        field="price",
        detail="two explicit values",
    )
    assert reason.model_dump(mode="json") == {
        "code": "parser_ambiguous",
        "detail": "two explicit values",
        "field": "price",
    }


def test_profile_rejects_unknown_schema_version() -> None:
    with pytest.raises(ValidationError):
        SourceProfileData(schema_version=99)


def test_profile_has_safe_review_defaults() -> None:
    profile = SourceProfileData()
    assert profile.publication_mode is PublicationMode.REVIEW
    assert profile.ocr_mode is OCRMode.MISSING_ONLY
    assert profile.advertising_filter_enabled is True
    assert profile.advertising_patterns == []
    assert profile.required_fields == ["price", "sizes", "quantity"]


@pytest.mark.parametrize(
    "required_fields",
    [[], ["price", "price"], ["price", "unsupported"]],
)
def test_profile_rejects_unsafe_required_fields(required_fields: list[str]) -> None:
    with pytest.raises(ValidationError):
        SourceProfileData(required_fields=required_fields)


def test_extracted_field_does_not_allow_found_without_value_or_evidence() -> None:
    with pytest.raises(ValidationError):
        ExtractedField(status=FieldStatus.FOUND, value=None, evidence=None)


def test_regex_rule_rejects_invalid_expression() -> None:
    with pytest.raises(ValidationError):
        ExtractionRule(kind="regex", pattern="[")


@pytest.mark.parametrize(
    "url", ["https://t.me/emoda", "https://example.com", "https://wa.me/90555"]
)
def test_navigation_button_accepts_safe_https_urls(url: str) -> None:
    assert NavigationButtonData(text="Open", url=url, row=0, order=0).url == url


@pytest.mark.parametrize(
    "url",
    [
        "http://example.com",
        "javascript:alert(1)",
        "ftp://example.com",
        "https://user:password@example.com",
        "https://example.com/line\nbreak",
        "https://../escape",
    ],
)
def test_navigation_button_rejects_non_https_urls(url: str) -> None:
    with pytest.raises(ValidationError):
        NavigationButtonData(text="Open", url=url, row=0, order=0)


def test_decimal_values_are_not_coerced_from_float() -> None:
    with pytest.raises(ValidationError):
        SourceProfileData(eur_usd_rate=1.2)
    with pytest.raises(ValidationError):
        SourceProfileData(adjacent_text_merge_seconds=1.5)
    with pytest.raises(ValidationError):
        SourceProfileData(product_collection_window_seconds=90.0)
    assert SourceProfileData(eur_usd_rate=Decimal("1.2")).eur_usd_rate == Decimal("1.2")


def test_product_collection_is_source_opt_in_with_positive_bounded_window() -> None:
    disabled = SourceProfileData()
    assert disabled.product_collection_mode == "off"
    assert disabled.product_collection_window_seconds == Decimal("0")

    configured = SourceProfileData(
        product_collection_mode="text_before_media",
        product_collection_window_seconds=Decimal("300"),
    )
    assert configured.product_collection_mode == "text_before_media"
    assert configured.product_collection_window_seconds == Decimal("300")

    with pytest.raises(ValidationError):
        SourceProfileData(product_collection_mode="media_before_text")
    with pytest.raises(ValidationError):
        SourceProfileData(product_collection_window_seconds=Decimal("301"))


def test_eur_rate_is_fixed_by_business_contract() -> None:
    with pytest.raises(ValidationError):
        SourceProfileData(eur_usd_rate=Decimal("1.3"))


def test_source_publication_mode_overrides_global_unless_inheritance_enabled() -> None:
    from emoda.services.processor import resolve_publication_mode

    direct = SourceProfileData(publication_mode=PublicationMode.AUTO)
    inherited = SourceProfileData(
        publication_mode=PublicationMode.AUTO,
        use_global_publication_mode=True,
    )
    assert resolve_publication_mode(direct, PublicationMode.REVIEW) is PublicationMode.AUTO
    assert resolve_publication_mode(inherited, PublicationMode.REVIEW) is PublicationMode.REVIEW


def test_source_ignore_rules_are_disabled_by_default_and_validate_conditions() -> None:
    assert SourceProfileData().ignore_rules == []
    rule = SourceIgnoreRule(
        name="article-only-media",
        media_required=True,
        found_fields=["article"],
        missing_fields=["price", "sizes", "quantity"],
    )
    assert rule.found_fields == ["article"]
    assert rule.enabled is True

    missing_only = SourceIgnoreRule(
        name="missing-product-fields",
        media_required=False,
        description_missing_required=False,
        missing_fields=["price", "sizes"],
    )
    assert missing_only.found_fields == []

    with pytest.raises(ValidationError):
        SourceIgnoreRule(name="unsafe")
    with pytest.raises(ValidationError):
        SourceIgnoreRule(
            name="overlap",
            found_fields=["article"],
            missing_fields=["article"],
        )


def test_multi_price_rule_requires_named_label_and_value_groups() -> None:
    rule = MultiPriceRule(
        name="bags-and-shoes",
        pattern=r"(?P<label>Ayakkabı|Küçük Çanta)\s*(?P<value>\d+)\s*(?P<currency>\$)",
        labels={
            "Ayakkabı": {"ru": "Обувь", "en": "Shoes"},
            "Küçük Çanta": {"ru": "Маленькая сумка", "en": "Small bag"},
        },
    )
    assert rule.name == "bags-and-shoes"

    with pytest.raises(ValidationError):
        MultiPriceRule(name="broken", pattern=r"(?P<value>\d+)", labels={})
