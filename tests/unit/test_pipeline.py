import re
from decimal import Decimal
from typing import Any, cast

import pytest

from emoda.domain.enums import FieldStatus, Provenance
from emoda.domain.schemas import (
    CleaningRule,
    ComputedRule,
    ExtractedField,
    ExtractionRule,
    FieldProcessingPolicy,
    GeminiExtraction,
    MarkupRange,
    MultiPriceRule,
    RepeatedPatternRule,
    RoutingRule,
    SourceIgnoreRule,
    SourceProfileData,
)
from emoda.rules.engine import RuleEngine
from emoda.services.orchestrator import ProcessingMode, process_product
from emoda.services.pipeline import Pipeline, PipelineInput
from emoda.services.publication import publication_keyboard
from emoda.services.render import CHECK_VALUE, render_publication
from emoda.services.translation import ProtectedTranslator, TranslationIntegrityError

RULES = [
    MarkupRange(min_price=Decimal("0"), max_price=Decimal("5"), markup=Decimal("1"), priority=1),
    MarkupRange(min_price=Decimal("6"), max_price=Decimal("9"), markup=Decimal("2"), priority=2),
    MarkupRange(min_price=Decimal("10"), max_price=Decimal("15"), markup=Decimal("3"), priority=3),
]

NEW_CHECK_VALUE = "Уточнить / Check"
LEGACY_CHECK_VALUES = ("Уточняйте у менеджера", "Ask the manager")

NICCY_PRICE_PATTERN = (
    r"(?m)^[ \t]{0,}📌[ \t]{0,}(?P<value>[0-9]+(?:[.,][0-9]+)?)"
    r"[ \t]{0,}[^0-9A-Za-z\r\n ]{1,}[ \t]{0,}$"
)


async def test_adjacent_currency_alias_reaches_markup_and_has_dry_live_parity() -> None:
    profile = SourceProfileData(
        extraction_rules={
            "price": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?i)F[İI]YAT\s*:?\s*(?P<value>\d+(?:[.,]\d+)?)",
                    priority=1,
                    authoritative=True,
                )
            ],
            "quantity": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?i)SIZE\s*:\s*(?P<value>S\s+M\s+L\s+XL\s+XXL)",
                    transform="map",
                    value_map={"S M L XL XXL": "5"},
                    authoritative=True,
                )
            ],
        },
        default_currency=None,
        gemini_enabled=False,
    )
    pipeline_input = PipelineInput(
        source_id=13,
        text=(
            "#PRADA 👑 #NEWSEZON DABILPİKE KUMAŞ ⛓️‍💥 AVRUPA KALIP 📌 "
            "SIZE : S M L XL XXL❗️ FIYAT : 13💲"
        ),
        media_paths=["fixture.jpg"],
        emoda_code="EM-TEST",
        profile=profile,
        markup_rules=RULES,
    )
    pipeline = Pipeline(None)

    dry = await process_product(pipeline, pipeline_input, ProcessingMode.DRY)
    live = await process_product(pipeline, pipeline_input, ProcessingMode.LIVE)

    assert dry.parsed.fields["price"].value == "13"
    assert dry.parsed.fields["price"].metadata["currency"] == "USD"
    assert dry.parsed.fields["sizes"].value == "S M L XL XXL"
    assert dry.parsed.fields["quantity"].value == "5"
    assert dry.price is not None
    assert dry.price.raw_price == Decimal("13")
    assert dry.price.markup == Decimal("3")
    assert dry.price.final_usd == Decimal("16")
    assert dry.rendered_text == live.rendered_text
    assert "<b>Цена за шт / Unit price:</b> $16" in dry.rendered_text
    assert dry.publication_requested is False
    assert live.publication_requested is True


async def test_niccy_authoritative_decimal_comma_dollar_banknote_reaches_markup() -> None:
    text = (
        "Kod: 107144\n\n🔥RPT🔥\n🪬Katmanlı Çift Fermuarlı Takım🪬\n\n"
        "📌1-1/2-2 \n📌(paketler 4 lü)\n📌Krinkıl&Modal Kumaş \n\n"
        "📌25,5 💵\n\n\n\nWhatsaap:+905536138944\n"
        "Arabic whatsaap :+905320530488"
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=51,
            text=text,
            media_paths=["niccy.jpg"],
            emoda_code="EM-TEST",
            profile=SourceProfileData(
                default_currency=None,
                field_policies={
                    "price": FieldProcessingPolicy(processing_mode="passthrough")
                },
                extraction_rules={
                    "price": [
                        ExtractionRule(
                            kind="regex",
                            pattern=NICCY_PRICE_PATTERN,
                            priority=10,
                            ignore_case=True,
                            transform="identity",
                            authoritative=True,
                        )
                    ]
                },
                gemini_enabled=False,
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("21"),
                    max_price=Decimal("30.99"),
                    markup=Decimal("5"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    assert result.parsed.original_text == text
    assert result.parsed.fields["price"].status is FieldStatus.FOUND
    assert result.parsed.fields["price"].value == "25.5"
    assert result.parsed.fields["price"].metadata["currency"] == "USD"
    assert result.parsed.fields["price"].metadata["rule_id"] == "price:1"
    assert result.price is not None
    assert result.price.raw_price == Decimal("25.5")
    assert result.price.raw_currency == "USD"
    assert result.price.markup == Decimal("5")
    assert result.price.final_usd == Decimal("31")
    assert "<b>Цена за шт / Unit price:</b> $31" in result.rendered_text
    trace = result.field_diagnostics["price"]
    assert trace.status is FieldStatus.FOUND
    assert trace.candidate_status is FieldStatus.FOUND
    assert trace.source == "source_rule"
    assert trace.rule_id == "price:1"
    assert trace.raw == "25,5"
    assert trace.normalized == "25.5"
    assert trace.currency == "USD"
    assert trace.final == "31"
    assert trace.stage == "resolved"
    assert trace.reason is None


async def test_computed_fields_have_exact_dry_live_parity() -> None:
    profile = SourceProfileData(
        default_currency="USD",
        computed_rules=[
            ComputedRule(
                name="count-sizes",
                target_field="quantity",
                operation="count_items",
                source_field="sizes",
                pattern=r"(?P<item>\d+)",
                capture_group="item",
                authoritative=True,
            )
        ],
    )
    pipeline_input = PipelineInput(
        source_id=1001,
        text="Price: 10 USD\nSizes: 36 37 38 39 40",
        media_paths=["fixture.jpg"],
        emoda_code="COMPUTED-PARITY",
        profile=profile,
        markup_rules=RULES,
    )
    pipeline = Pipeline(None)

    dry = await process_product(pipeline, pipeline_input, ProcessingMode.DRY)
    live = await process_product(pipeline, pipeline_input, ProcessingMode.LIVE)

    assert dry.parsed.fields["quantity"].value == "5"
    assert dry.parsed.fields["quantity"].provenance is Provenance.COMPUTED_RULE
    assert dry.rendered_text == live.rendered_text
    assert dry.model_dump(exclude={"publication_requested"}) == live.model_dump(
        exclude={"publication_requested"}
    )
    assert dry.publication_requested is False
    assert live.publication_requested is True


async def test_disabled_computed_rule_uses_new_fallback_and_stays_auto() -> None:
    profile = SourceProfileData(
        default_currency="USD",
        computed_rules=[
            ComputedRule(
                name="count-sizes",
                target_field="quantity",
                operation="count_items",
                source_field="sizes",
                pattern=r"(?P<item>\d+)",
                capture_group="item",
                authoritative=True,
                enabled=False,
            )
        ],
    )
    pipeline_input = PipelineInput(
        source_id=1001,
        text="Price: 10 USD\nSizes: 36 37 38 39 40",
        media_paths=[],
        emoda_code="COMPUTED-DISABLED",
        profile=profile,
        markup_rules=RULES,
    )
    pipeline = Pipeline(None)

    dry = await process_product(pipeline, pipeline_input, ProcessingMode.DRY)
    live = await process_product(pipeline, pipeline_input, ProcessingMode.LIVE)

    assert dry.parsed.fields["quantity"].status is FieldStatus.MISSING
    assert f"<b>Шт в серии / Pcs per lot:</b> {NEW_CHECK_VALUE}" in dry.rendered_text
    assert all(legacy not in dry.rendered_text for legacy in LEGACY_CHECK_VALUES)
    assert dry.rendered_text == live.rendered_text
    assert dry.needs_review is False
    assert live.needs_review is False


async def test_disabled_canonical_match_count_is_check_without_review() -> None:
    profile = SourceProfileData(
        computed_rules=[
            ComputedRule(
                name="unique-sizes",
                target_field="sizes",
                operation="unique_matches",
                input_source="canonical_text",
                pattern=r"(?P<size>XS|S|M|L|XL)",
                separator=" ",
                authoritative=True,
                conflict_policy="computed",
            ),
            ComputedRule(
                name="count-source-size-occurrences",
                target_field="quantity",
                operation="count_items",
                input_source="canonical_text",
                pattern=r"(?P<item>XS|S|M|L|XL)",
                capture_group="item",
                authoritative=True,
                enabled=False,
            ),
        ]
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=45,
            text="S. S. M. M. L",
            media_paths=[],
            emoda_code="EM-TEST",
            profile=profile,
            markup_rules=[],
        ),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == "S M L"
    assert result.parsed.fields["quantity"].status is FieldStatus.MISSING
    assert f"<b>Шт в серии / Pcs per lot:</b> {NEW_CHECK_VALUE}" in result.rendered_text
    assert result.needs_review is False
    assert result.review_reasons == []


async def test_computed_conflict_uses_check_value_and_stays_auto() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=1002,
            text="Price: 10 USD\nS1 M2 L2 XL1\nQuantity: 8",
            media_paths=[],
            emoda_code="COMPUTED-CONFLICT",
            profile=SourceProfileData(
                default_currency="USD",
                computed_rules=[
                    ComputedRule(
                        name="size-count-pairs",
                        target_field="quantity",
                        secondary_target_field="sizes",
                        operation="sum_size_counts",
                        pattern=r"(?P<size>[A-Z]+)(?P<count>\d+)",
                        conflict_policy="ambiguous",
                    )
                ],
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == "S, M, L, XL"
    assert result.parsed.fields["quantity"].status is FieldStatus.AMBIGUOUS
    assert f"<b>Шт в серии / Pcs per lot:</b> {CHECK_VALUE}" in result.rendered_text
    assert len(result.rendered_text.splitlines()) == 4
    assert result.needs_review is False
    assert result.review_reasons == []


async def test_authoritative_computed_value_wins_over_conflicting_gemini_ocr() -> None:
    gemini_quantity = ExtractedField(
        status=FieldStatus.FOUND,
        value="99",
        evidence="99 pcs",
        provenance=Provenance.GEMINI_OCR,
    )
    result = await Pipeline(
        FakeGemini(GeminiExtraction(fields={"quantity": gemini_quantity}))
    ).run(
        PipelineInput(
            source_id=1003,
            text="Price: 10 USD\nSizes: 36 37 38 39 40",
            media_paths=["label.jpg"],
            emoda_code="COMPUTED-AUTH",
            profile=SourceProfileData(
                default_currency="USD",
                gemini_enabled=True,
                ocr_mode="always",
                computed_rules=[
                    ComputedRule(
                        name="count-sizes",
                        target_field="quantity",
                        operation="count_items",
                        source_field="sizes",
                        pattern=r"(?P<item>\d+)",
                        capture_group="item",
                        authoritative=True,
                    )
                ],
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["quantity"].value == "5"
    assert result.parsed.fields["quantity"].provenance is Provenance.COMPUTED_RULE


class FakeGemini:
    def __init__(self, result: GeminiExtraction | None = None) -> None:
        self.result = result or GeminiExtraction()
        self.calls: list[dict[str, object]] = []

    async def extract(self, **kwargs: object) -> GeminiExtraction:
        self.calls.append(kwargs)
        return self.result

    async def translate(
        self, text: str, target_language: str, instruction: str = ""
    ) -> str:
        protected = " ".join(re.findall(r"__P\d+__", text))
        if target_language == "RU":
            return f"{protected} Описание товара"
        return f"{protected} Product description"


class BrokenTranslator(FakeGemini):
    async def translate(
        self, text: str, target_language: str, instruction: str = ""
    ) -> str:
        return text.replace("__P0__", "lost")


class UnavailableGemini(FakeGemini):
    async def extract(self, **kwargs: object) -> GeminiExtraction:
        raise TimeoutError("temporary outage")


class QuotaExhaustedGemini(FakeGemini):
    async def extract(self, **kwargs: object) -> GeminiExtraction:
        raise RuntimeError("Gemini quota/rate limit exhausted (429 RESOURCE_EXHAUSTED)")


class AdversarialTranslator(FakeGemini):
    async def translate(
        self, text: str, target_language: str, instruction: str = ""
    ) -> str:
        return (
            text.replace("Nike", "Найк")
            .replace("AB-123", "AB-999")
            .replace("XL", "ХЛ")
            .replace("$18", "$19")
            .replace("100%", "сто процентов")
            .replace("SKU123", "SKU999")
            .replace("NIKE", "НАЙК")
            .replace("2 pockets", "two pockets")
            .replace("cotton", "хлопок")
            .replace("pockets", "кармана")
        )


class InvalidRussianTranslator(FakeGemini):
    async def translate(
        self, text: str, target_language: str, instruction: str = ""
    ) -> str:
        if target_language == "RU":
            return text
        return "Dress with a decorative buckle."


async def test_strict_complete_input_does_not_call_gemini() -> None:
    gemini = FakeGemini()
    profile = SourceProfileData(gemini_enabled=True, default_currency="USD")
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=1,
            text="Price: 10 USD\nSizes: S M\n4 pcs",
            media_paths=[],
            emoda_code="EM-000001",
            profile=profile,
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=Decimal("100.99"),
                    markup=Decimal("3"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )
    assert result.price is not None and result.price.final_usd == Decimal("13")
    assert result.parsed.fields["quantity"].value == "4"
    assert gemini.calls == []
    assert result.publication_requested is False


async def test_shared_orchestrator_has_sink_only_dry_live_difference() -> None:
    pipeline_input = PipelineInput(
        source_id=100,
        text="Price: 10 USD\nSizes: S M\n2 pcs",
        media_paths=["fixture.jpg"],
        emoda_code="AA00100",
        profile=SourceProfileData(translation_enabled=False),
        markup_rules=RULES,
    )
    pipeline = Pipeline(None)
    dry = await process_product(pipeline, pipeline_input, ProcessingMode.DRY)
    live = await process_product(pipeline, pipeline_input, ProcessingMode.LIVE)
    assert dry.model_dump(exclude={"publication_requested"}) == live.model_dump(
        exclude={"publication_requested"}
    )
    assert dry.rendered_text == live.rendered_text
    buttons_setting = {
        "order": {"text": "🛒 Заказать / Order", "url": "https://example.com/order"},
        "catalog": {"text": "📂 Каталог / Catalog", "url": "https://example.com/catalog"},
        "destination_override": [
            {"text": "Forbidden third button", "url": "https://example.com/third"}
        ],
    }
    dry_keyboard = publication_keyboard(buttons_setting, bot_username="emoda_bot")
    live_keyboard = publication_keyboard(buttons_setting, bot_username="emoda_bot")
    assert dry_keyboard == live_keyboard
    assert sum(len(row) for row in dry_keyboard) == 3
    assert dry.publication_requested is False
    assert live.publication_requested is True


async def test_source_ignore_rule_short_circuits_article_only_media_before_gemini() -> None:
    gemini = FakeGemini()
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=71,
            text="E7193",
            media_paths=["madmext-video.mp4"],
            emoda_code="AA00071",
            profile=SourceProfileData(
                gemini_enabled=True,
                translation_enabled=True,
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
            ),
            markup_rules=RULES,
        ),
        dry_run=False,
    )

    assert result.ignored is True
    assert result.ignore_reason == "article-only-media"
    assert result.parsed.fields["article"].value == "E7193"
    assert result.needs_review is False
    assert result.review_reasons == []
    assert result.publication_requested is False
    assert result.rendered_text == ""
    assert gemini.calls == []


async def test_video_no_text_ignore_filters_media_type_after_collection_before_gemini() -> None:
    gemini = FakeGemini()
    profile = SourceProfileData(
        gemini_enabled=True,
        ignore_rules=[
            SourceIgnoreRule(
                name="video-no-text",
                media_types=["video"],
                media_required=True,
                description_missing_required=True,
                missing_fields=["price", "sizes", "quantity"],
            )
        ],
    )

    video = await Pipeline(gemini).run(
        PipelineInput(
            source_id=714,
            text="",
            media_paths=["teaser.mp4"],
            media_types=["video"],
            emoda_code="IGNORE-VIDEO",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=False,
    )
    photo = await Pipeline(None).run(
        PipelineInput(
            source_id=714,
            text="",
            media_paths=["product.jpg"],
            media_types=["photo"],
            emoda_code="KEEP-PHOTO",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=True,
    )
    mixed_media = await Pipeline(None).run(
        PipelineInput(
            source_id=714,
            text="",
            media_paths=["teaser.mp4", "product.jpg"],
            media_types=["video", "photo"],
            emoda_code="KEEP-MIXED-MEDIA",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=True,
    )
    product_video = await Pipeline(None).run(
        PipelineInput(
            source_id=714,
            text="Price: 10 USD\nSizes: S M\n2 pcs",
            media_paths=["product.mp4"],
            media_types=["video"],
            emoda_code="KEEP-VIDEO-TEXT",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert video.ignored is True
    assert video.ignore_reason == "video-no-text"
    assert video.publication_requested is False
    assert gemini.calls == []
    assert photo.ignored is False
    assert mixed_media.ignored is False
    assert product_video.ignored is False


async def test_media_type_ignore_does_not_count_ambiguous_as_missing() -> None:
    profile = SourceProfileData(
        ignore_rules=[
            SourceIgnoreRule(
                name="video-missing-fields",
                media_types=["video"],
                media_required=True,
                description_missing_required=False,
                missing_fields=["price", "sizes", "quantity"],
            )
        ]
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=715,
            text="Price: 10 USD\nPrice: 12 USD",
            media_paths=["ambiguous.mp4"],
            media_types=["video"],
            emoda_code="KEEP-AMBIGUOUS",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["price"].status is FieldStatus.AMBIGUOUS
    assert result.ignored is False


async def test_post_collection_none_media_type_matches_a_text_rule_without_media() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=716,
            text="informational post",
            media_paths=[],
            media_types=[],
            emoda_code="IGNORE-NO-MEDIA",
            profile=SourceProfileData(
                ignore_rules=[
                    SourceIgnoreRule(
                        name="text-without-product-fields",
                        media_types=["none"],
                        media_required=False,
                        description_missing_required=False,
                        missing_fields=["price", "sizes", "quantity"],
                    )
                ]
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is True
    assert result.ignore_reason == "text-without-product-fields"


async def test_missing_only_ignore_rule_short_circuits_variable_text_before_gemini() -> None:
    gemini = FakeGemini()
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=711,
            text="Поздравляем с праздником — сегодня новый текст без карточки товара!",
            media_paths=[],
            emoda_code="IGNORE-MISSING",
            profile=SourceProfileData(
                gemini_enabled=True,
                ignore_rules=[
                    SourceIgnoreRule(
                        name="missing-price-and-sizes",
                        media_required=False,
                        description_missing_required=False,
                        missing_fields=["price", "sizes"],
                    )
                ],
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is True
    assert result.ignore_reason == "missing-price-and-sizes"
    assert result.parsed.fields["price"].status is FieldStatus.MISSING
    assert result.parsed.fields["sizes"].status is FieldStatus.MISSING
    assert result.rendered_text == ""
    assert gemini.calls == []


async def test_disabled_missing_only_ignore_rule_does_not_match() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=712,
            text="Variable informational text",
            media_paths=[],
            emoda_code="IGNORE-OFF",
            profile=SourceProfileData(
                ignore_rules=[
                    SourceIgnoreRule(
                        name="missing-price-and-sizes",
                        media_required=False,
                        description_missing_required=False,
                        missing_fields=["price", "sizes"],
                        enabled=False,
                    )
                ],
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is False
    assert result.ignore_reason is None
    assert result.rendered_text


async def test_missing_only_ignore_sees_strict_multi_price_before_matching() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=713,
            text="Ayakkabı 25$ Küçük Çanta 22$",
            media_paths=[],
            emoda_code="IGNORE-MULTI-PRICE",
            profile=SourceProfileData(
                fallback_rules={
                    "sizes": {"value": "36, 37", "authoritative": True},
                    "quantity": {"value": "2", "authoritative": True},
                },
                ignore_rules=[
                    SourceIgnoreRule(
                        name="missing-price-and-sizes",
                        media_required=False,
                        description_missing_required=False,
                        missing_fields=["price", "article"],
                    )
                ],
                multi_price_rules=[
                    MultiPriceRule(
                        name="product-pairs",
                        pattern=(
                            r"(?P<label>Ayakkabı|Küçük Çanta)\s*"
                            r"(?P<value>\d+(?:[.,]\d+)?)\s*(?P<currency>\$)"
                        ),
                        labels={
                            "Ayakkabı": {"ru": "Обувь", "en": "Shoes"},
                            "Küçük Çanta": {
                                "ru": "Маленькая сумка",
                                "en": "Small bag",
                            },
                        },
                    )
                ],
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

    assert result.ignored is False
    assert result.parsed.fields["price"].status is FieldStatus.FOUND
    assert [item.label_ru for item in result.line_items] == ["Обувь", "Маленькая сумка"]


async def test_confident_advertisement_is_ignored_before_review_and_gemini() -> None:
    gemini = FakeGemini()
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=80,
            text="#реклама\nПодпишитесь на внешний сервис",
            media_paths=["ad.mp4"],
            emoda_code="AD00001",
            profile=SourceProfileData(gemini_enabled=True),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is True
    assert result.ignore_reason == "advertising:builtin"
    assert result.needs_review is False
    assert result.publication_requested is False
    assert gemini.calls == []


async def test_source_multi_price_rule_prices_each_item_without_common_price() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=81,
            text=(
                "Ayakkabı 25$ Küçük Çanta 22$ Büyük Çanta 24$\n"
                "Size 36 37 38 39 40\nQuantity: 5 pairs"
            ),
            media_paths=["mortopuk.jpg"],
            emoda_code="MP00001",
            profile=SourceProfileData(
                translation_enabled=False,
                extraction_rules={
                    "quantity": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"Quantity:\s*(?P<value>\d+)\s*pairs",
                            authoritative=True,
                        )
                    ]
                },
                multi_price_rules=[
                    MultiPriceRule(
                        name="products-with-prices",
                        pattern=(
                            r"(?P<label>Ayakkabı|Küçük Çanta|Büyük Çanta)\s*"
                            r"(?P<value>\d+(?:[.,]\d+)?)\s*(?P<currency>\$)"
                        ),
                        labels={
                            "Ayakkabı": {"ru": "Обувь", "en": "Shoes"},
                            "Küçük Çanta": {
                                "ru": "Маленькая сумка",
                                "en": "Small bag",
                            },
                            "Büyük Çanta": {
                                "ru": "Большая сумка",
                                "en": "Large bag",
                            },
                        },
                    )
                ],
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=Decimal("100.99"),
                    markup=Decimal("5"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    assert result.price is None
    assert [item.label_ru for item in result.line_items] == [
        "Обувь",
        "Маленькая сумка",
        "Большая сумка",
    ]
    assert [item.price.final_usd for item in result.line_items] == [
        Decimal("30"),
        Decimal("27"),
        Decimal("29"),
    ]
    assert result.parsed.fields["price"].status is FieldStatus.FOUND
    assert result.parsed.fields["price"].metadata == {
        "multi_price": True,
        "item_count": 3,
    }
    assert (
        "<b>Цена за шт / Unit price:</b> Обувь / Shoes: $30; "
        "Маленькая сумка / Small bag: $27; "
        "Большая сумка / Large bag: $29"
    ) in result.rendered_text
    assert len(result.rendered_text.splitlines()) == 4
    assert CHECK_VALUE not in result.rendered_text
    assert result.needs_review is False


async def test_generic_multi_price_keeps_arbitrary_labels_order_prices_and_metadata_private() -> (
    None
):
    raw_labels = ["Ceket", "Kemer", "Şapka"]
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=991,
            text="Ceket 30$ Kemer 27$ Şapka 29$",
            media_paths=[],
            emoda_code="GENERIC-MP",
            profile=SourceProfileData(
                fallback_rules={
                    "sizes": {"value": "S, M, L", "authoritative": True},
                    "quantity": {"value": "3", "authoritative": True},
                },
                multi_price_rules=[
                    MultiPriceRule(
                        name="generic-clothing-pairs",
                        pattern=(
                            r"(?P<label>Ceket|Kemer|Şapka)\s*"
                            r"(?P<value>\d+(?:[.,]\d+)?)\s*(?P<currency>\$)"
                        ),
                        labels={
                            "Ceket": {"ru": "Куртка", "en": "Jacket"},
                            "Kemer": {"ru": "Ремень", "en": "Belt"},
                            "Şapka": {"ru": "Шляпа", "en": "Hat"},
                        },
                    )
                ],
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=None,
                    markup=Decimal("0"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    assert [item.label_original for item in result.line_items] == raw_labels
    assert [item.label_ru for item in result.line_items] == ["Куртка", "Ремень", "Шляпа"]
    assert [item.label_en for item in result.line_items] == ["Jacket", "Belt", "Hat"]
    assert [item.price.final_usd for item in result.line_items] == [
        Decimal("30"),
        Decimal("27"),
        Decimal("29"),
    ]
    assert result.rendered_text == (
        "<b>Код / Code:</b> GENERIC-MP\n"
        "<b>Цена за шт / Unit price:</b> Куртка / Jacket: $30; "
        "Ремень / Belt: $27; Шляпа / Hat: $29\n"
        "<b>Размеры / Sizes:</b> S, M, L\n"
        "<b>Шт в серии / Pcs per lot:</b> 3"
    )
    for raw_label in raw_labels:
        assert raw_label not in result.rendered_text
    assert result.needs_review is False


async def test_multi_price_dry_and_live_use_identical_rendering() -> None:
    pipeline_input = PipelineInput(
        source_id=992,
        text="Юбка 18$ Жакет 24$",
        media_paths=["fixture.jpg"],
        emoda_code="PARITY-MP",
        profile=SourceProfileData(
            fallback_rules={
                "sizes": {"value": "M, L", "authoritative": True},
                "quantity": {"value": "2", "authoritative": True},
            },
            multi_price_rules=[
                MultiPriceRule(
                    name="generic-russian-labels",
                    pattern=r"(?P<label>Юбка|Жакет)\s*(?P<value>\d+)\s*(?P<currency>\$)",
                    labels={
                        "Юбка": {"ru": "Юбка", "en": "Skirt"},
                        "Жакет": {"ru": "Жакет", "en": "Jacket"},
                    },
                )
            ],
        ),
        markup_rules=[
            MarkupRange(
                min_price=Decimal("0"),
                max_price=None,
                markup=Decimal("0"),
                priority=1,
            )
        ],
    )

    pipeline = Pipeline(None)
    dry = await process_product(pipeline, pipeline_input, ProcessingMode.DRY)
    live = await process_product(pipeline, pipeline_input, ProcessingMode.LIVE)

    assert dry.rendered_text == live.rendered_text
    assert dry.model_dump(exclude={"publication_requested"}) == live.model_dump(
        exclude={"publication_requested"}
    )
    assert dry.publication_requested is False
    assert live.publication_requested is True


async def test_typed_cleaning_runs_before_multi_price_extraction() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=995,
            text="P1 99$ P2 98$ REMOVE-MULTI\nP1 4$ P2 5$",
            media_paths=[],
            emoda_code="CLEAN-MULTI",
            profile=SourceProfileData(
                cleaning_rules=[
                    CleaningRule(kind="contains", value="REMOVE-MULTI")
                ],
                fallback_rules={
                    "sizes": {"value": "S M", "authoritative": True},
                    "quantity": {"value": "2", "authoritative": True},
                },
                multi_price_rules=[
                    MultiPriceRule(
                        name="cleaned-pairs",
                        pattern=(
                            r"(?P<label>P1|P2)\s*(?P<value>\d+)\s*(?P<currency>\$)"
                        ),
                        labels={
                            "P1": {"ru": "Обувь", "en": "Shoes"},
                            "P2": {"ru": "Сумка", "en": "Bag"},
                        },
                    )
                ],
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert [item.price.raw_price for item in result.line_items] == [
        Decimal("4"),
        Decimal("5"),
    ]
    assert "Обувь / Shoes: $5; Сумка / Bag: $6" in result.rendered_text
    assert result.needs_review is False


async def test_true_missing_price_uses_bilingual_fallback_without_review() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=993,
            text="Sizes: M L\nQuantity: 2",
            media_paths=[],
            emoda_code="MISSING-PRICE",
            profile=SourceProfileData(),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["price"].status is FieldStatus.MISSING
    assert result.line_items == []
    assert f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}" in result.rendered_text
    assert result.needs_review is False
    assert result.review_reasons == []


async def test_multi_price_structured_only_post_contains_no_descriptions() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=83,
            text="Ayakkabı 25$ Küçük Çanta 22$\nSize 36 37 38 39 40",
            media_paths=[],
            emoda_code="MP00003",
            profile=SourceProfileData(
                translation_enabled=True,
                extraction_rules={
                    "quantity": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"(?i)Size\s+(?P<value>36\s+37\s+38\s+39\s+40)",
                            transform="map",
                            value_map={"36 37 38 39 40": "5"},
                            authoritative=True,
                        )
                    ]
                },
                multi_price_rules=[
                    MultiPriceRule(
                        name="two-products",
                        pattern=r"(?P<label>Ayakkabı|Küçük Çanta)\s*(?P<value>\d+)\s*(?P<currency>\$)",
                        labels={
                            "Ayakkabı": {"ru": "Обувь", "en": "Shoes"},
                            "Küçük Çanta": {"ru": "Сумка", "en": "Bag"},
                        },
                    )
                ],
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=Decimal("100.99"),
                    markup=Decimal("3"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    assert result.description_ru == ""
    assert result.description_en == ""
    assert result.needs_review is False


async def test_multi_price_does_not_infer_quantity_from_sizes() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=82,
            text="Ayakkabı 25$ Küçük Çanta 22$\nSize 36 37 38 39 40",
            media_paths=[],
            emoda_code="MP00002",
            profile=SourceProfileData(
                translation_enabled=False,
                multi_price_rules=[
                    MultiPriceRule(
                        name="two-products",
                        pattern=r"(?P<label>Ayakkabı|Küçük Çanta)\s*(?P<value>\d+)\s*(?P<currency>\$)",
                        labels={
                            "Ayakkabı": {"ru": "Обувь", "en": "Shoes"},
                            "Küçük Çanta": {"ru": "Сумка", "en": "Bag"},
                        },
                    )
                ],
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=Decimal("100.99"),
                    markup=Decimal("3"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )

    assert result.line_items
    assert result.parsed.fields["quantity"].status is FieldStatus.MISSING
    assert result.needs_review is False
    assert result.review_reasons == []


async def test_source_ignore_rule_does_not_match_when_continuation_resolves_a_field() -> None:
    profile = SourceProfileData(
        gemini_enabled=False,
        translation_enabled=False,
        extraction_rules={
            "article": [ExtractionRule(kind="regex", pattern=r"Code:\s*(?P<value>[A-Z]\d+)")]
        },
        ignore_rules=[
            SourceIgnoreRule(
                name="article-only-media",
                found_fields=["article"],
                missing_fields=["price", "sizes", "quantity"],
            )
        ],
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=71,
            text="Code: E7193\nPrice: 10 USD",
            media_paths=["product.mp4"],
            emoda_code="AA00072",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is False
    assert result.ignore_reason is None
    assert result.price is not None
    assert result.needs_review is False


async def test_source_without_ignore_rule_keeps_article_only_media_in_auto() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=72,
            text="Article: E7193",
            media_paths=["other-source.mp4"],
            emoda_code="AA00073",
            profile=SourceProfileData(translation_enabled=False),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is False
    assert result.needs_review is False
    assert f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}" in result.rendered_text


async def test_ignore_positive_condition_cannot_be_satisfied_by_fallback() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=73,
            text="supplier teaser",
            media_paths=["teaser.mp4"],
            emoda_code="AA00074",
            profile=SourceProfileData(
                translation_enabled=False,
                fallback_rules={"article": {"value": "E7193"}},
                ignore_rules=[
                    SourceIgnoreRule(
                        name="article-only-media",
                        found_fields=["article"],
                        missing_fields=["price", "sizes", "quantity"],
                    )
                ],
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is False
    assert result.needs_review is False


async def test_article_media_with_product_description_is_not_ignored() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=74,
            text="E7193\nNew cotton jacket",
            media_paths=["product.mp4"],
            emoda_code="AA00075",
            profile=SourceProfileData(
                translation_enabled=False,
                extraction_rules={
                    "article": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"(?m)^\s*(?P<value>[A-Z]\d+)\s*$",
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
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.ignored is False
    assert result.needs_review is False


async def test_lastpoint_dry_run_parses_labeled_symbol_price_without_extraction() -> None:
    gemini = FakeGemini()
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=1,
            text=(
                "Nike AB-123\n"
                "Цена: $10\n"
                "Размеры: S-M-L-XL\n"
                "Упаковка: 4 шт\n"
                "Цвет: чёрный\n"
                "Состав: 100% cotton"
            ),
            media_paths=[],
            emoda_code="EM-TEST",
            profile=SourceProfileData(
                gemini_enabled=True,
                translation_enabled=False,
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["price"].status is FieldStatus.FOUND
    assert result.price is not None
    assert result.price.raw_price == Decimal("10")
    assert result.price.final_usd == Decimal("13")
    assert gemini.calls == []
    assert result.needs_review is False


async def test_source_series_rules_run_before_gemini_and_critical_validation() -> None:
    gemini = FakeGemini()
    series_pattern = r"(?i)\bSeries:\s*(?P<value>(?:[A-Z]+\s*\d+\s*)+)"
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=41,
            text="Price: $10\nFf Series: S2 M2 L2",
            media_paths=[],
            emoda_code="EM-SERIES",
            profile=SourceProfileData(
                gemini_enabled=True,
                translation_enabled=False,
                extraction_rules={
                    "sizes": [
                        ExtractionRule(
                            kind="regex",
                            pattern=series_pattern,
                            transform="series_sizes",
                        )
                    ],
                    "quantity": [
                        ExtractionRule(
                            kind="regex",
                            pattern=series_pattern,
                            transform="series_quantity",
                        )
                    ],
                },
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == "S, M, L"
    assert result.parsed.fields["quantity"].value == "6"
    assert result.needs_review is False
    assert gemini.calls == []


async def test_optional_gemini_quota_failure_does_not_force_review_when_critical_fields_are_complete() -> (  # noqa: E501
    None
):
    series_pattern = r"(?i)Series\s*:\s*(?P<value>(?:[A-Z]+\s*\d+\s*)+)"
    result = await Pipeline(QuotaExhaustedGemini()).run(
        PipelineInput(
            source_id=44,
            text="Price: $10\nFfSeries : S1 M2 L2 XL1\nCode : 2604",
            media_paths=["mybee-product.jpg"],
            emoda_code="EM-MYBEE",
            profile=SourceProfileData(
                gemini_enabled=True,
                translation_enabled=False,
                extraction_rules={
                    "sizes": [
                        ExtractionRule(
                            kind="regex",
                            pattern=series_pattern,
                            transform="series_sizes",
                            authoritative=True,
                        )
                    ],
                    "quantity": [
                        ExtractionRule(
                            kind="regex",
                            pattern=series_pattern,
                            transform="series_quantity",
                            authoritative=True,
                        )
                    ],
                    "article": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"(?i)Code\s*:\s*(?P<value>\d+)",
                            authoritative=True,
                        )
                    ],
                },
            ),
            markup_rules=RULES,
        ),
        dry_run=False,
    )

    assert result.parsed.fields["sizes"].value == "S, M, L, XL"
    assert result.parsed.fields["quantity"].value == "6"
    assert result.parsed.fields["article"].value == "2604"
    assert result.price is not None and result.price.final_usd == Decimal("13")
    assert result.needs_review is False
    assert not any("Gemini" in reason for reason in result.review_reasons)


async def test_gemini_outage_degrades_to_check_lines_without_review() -> None:
    result = await Pipeline(UnavailableGemini()).run(
        PipelineInput(
            source_id=1,
            text="Price: 10 USD",
            media_paths=[],
            emoda_code="EM-000008",
            profile=SourceProfileData(gemini_enabled=True),
            markup_rules=RULES,
        ),
        dry_run=True,
    )
    assert result.price is not None and result.price.final_usd == Decimal("13")
    assert result.needs_review is False
    assert result.review_reasons == []
    assert f"<b>Размеры / Sizes:</b> {CHECK_VALUE}" in result.rendered_text


async def test_authoritative_source_rule_wins_over_conflicting_gemini_ocr() -> None:
    ocr_sizes = ExtractedField(
        status=FieldStatus.FOUND,
        value="XL",
        evidence="image label",
        provenance=Provenance.GEMINI_OCR,
    )
    result = await Pipeline(FakeGemini(GeminiExtraction(fields={"sizes": ocr_sizes}))).run(
        PipelineInput(
            source_id=42,
            text="Price: $10\nSizes: S-M-L\n4 pcs",
            media_paths=["photo.jpg"],
            emoda_code="EM-PRIORITY",
            profile=SourceProfileData(
                gemini_enabled=True,
                translation_enabled=False,
                ocr_mode="always",
                extraction_rules={
                    "sizes": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"Sizes:\s*(?P<value>[^\n]+)",
                            authoritative=True,
                        )
                    ]
                },
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == "S-M-L"
    assert result.parsed.fields["sizes"].status is FieldStatus.FOUND
    assert result.needs_review is False


async def test_configured_fallbacks_exist_before_critical_validation() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=43,
            text="Price: $10\nLastpoint dress",
            media_paths=[],
            emoda_code="EM-FALLBACK",
            profile=SourceProfileData(
                translation_enabled=False,
                fallback_rules={
                    "sizes": {"value": "S, M, L", "authoritative": True},
                    "quantity": {"value": "6", "authoritative": True},
                },
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == "S, M, L"
    assert result.parsed.fields["quantity"].value == "6"
    assert result.needs_review is False


async def test_missing_fields_use_only_current_source_examples() -> None:
    gemini = FakeGemini()
    profile = SourceProfileData(gemini_enabled=True, gemini_example_limit=2)
    examples = [{"source_id": 7, "input": "a", "expected": {}}]
    await Pipeline(gemini).run(
        PipelineInput(
            source_id=7,
            text="Dress",
            media_paths=[],
            emoda_code="EM-000002",
            profile=profile,
            markup_rules=RULES,
            examples=examples,
        ),
        dry_run=True,
    )
    assert gemini.calls[0]["examples"] == examples
    assert gemini.calls[0]["missing_fields"]


async def test_gemini_text_value_without_matching_evidence_is_not_accepted() -> None:
    claimed = ExtractedField(
        status=FieldStatus.FOUND,
        value="Nike",
        evidence="Brand: Nike",
        provenance=Provenance.GEMINI_TEXT,
    )
    result = await Pipeline(FakeGemini(GeminiExtraction(fields={"brand": claimed}))).run(
        PipelineInput(
            source_id=1,
            text="A dress",
            media_paths=[],
            emoda_code="EM-000003",
            profile=SourceProfileData(gemini_enabled=True),
            markup_rules=RULES,
        ),
        dry_run=True,
    )
    assert result.parsed.fields["brand"].status is FieldStatus.MISSING


async def test_ocr_result_keeps_image_provenance_without_review() -> None:
    found = ExtractedField(
        status=FieldStatus.FOUND,
        value="AB-12",
        evidence="label region",
        provenance=Provenance.GEMINI_OCR,
    )
    result = await Pipeline(FakeGemini(GeminiExtraction(fields={"article": found}))).run(
        PipelineInput(
            source_id=1,
            text="Dress",
            media_paths=["photo.jpg"],
            emoda_code="EM-000004",
            profile=SourceProfileData(gemini_enabled=True),
            markup_rules=RULES,
        ),
        dry_run=True,
    )
    assert result.parsed.fields["article"].value == "AB-12"
    assert result.parsed.fields["article"].provenance is Provenance.GEMINI_OCR
    assert result.needs_review is False


async def test_ocr_only_critical_fields_publish_in_auto_with_four_lines() -> None:
    fields = {
        "price": ExtractedField(
            status=FieldStatus.FOUND,
            value="20",
            evidence="price tag",
            provenance=Provenance.GEMINI_OCR,
            metadata={"currency": "USD"},
        ),
        "sizes": ExtractedField(
            status=FieldStatus.FOUND,
            value="M",
            evidence="size label",
            provenance=Provenance.GEMINI_OCR,
        ),
        "quantity": ExtractedField(
            status=FieldStatus.FOUND,
            value="1",
            evidence="quantity label",
            provenance=Provenance.GEMINI_OCR,
        ),
    }
    result = await Pipeline(FakeGemini(GeminiExtraction(fields=fields))).run(
        PipelineInput(
            source_id=1,
            text="Dress",
            media_paths=["tag.jpg"],
            emoda_code="EM-000014",
            profile=SourceProfileData(
                gemini_enabled=True,
                translation_enabled=False,
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=Decimal("100"),
                    markup=Decimal("1"),
                    priority=1,
                )
            ],
        ),
        dry_run=False,
    )
    assert result.price is not None
    assert result.needs_review is False
    assert result.publication_requested is True
    assert [line for line in result.rendered_text.splitlines() if line] == [
        "<b>Код / Code:</b> EM-000014",
        "<b>Цена за шт / Unit price:</b> $21",
        "<b>Размеры / Sizes:</b> M",
        "<b>Шт в серии / Pcs per lot:</b> 1",
    ]


async def test_ambiguous_ocr_price_renders_check_line_without_review() -> None:
    fields = {
        "price": ExtractedField(
            status=FieldStatus.AMBIGUOUS,
            evidence="27 $\n50 $",
            provenance=Provenance.GEMINI_OCR,
        ),
        "sizes": ExtractedField(
            status=FieldStatus.FOUND,
            value="36-42",
            evidence="36-42",
            provenance=Provenance.GEMINI_OCR,
        ),
        "quantity": ExtractedField(
            status=FieldStatus.FOUND,
            value="4",
            evidence="4 pcs",
            provenance=Provenance.GEMINI_OCR,
        ),
    }

    result = await Pipeline(FakeGemini(GeminiExtraction(fields=fields))).run(
        PipelineInput(
            source_id=1,
            text="",
            media_paths=["supplier-card.jpg"],
            emoda_code="EM-TEST",
            profile=SourceProfileData(
                gemini_enabled=True,
                translation_enabled=False,
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.parsed.fields["price"].status is FieldStatus.AMBIGUOUS
    assert result.parsed.fields["price"].evidence == "27 $\n50 $"
    assert result.price is None
    assert result.needs_review is False
    assert f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}" in result.rendered_text


async def test_all_ambiguous_customer_fields_fall_back_without_review() -> None:
    fields = {
        name: ExtractedField(
            status=FieldStatus.AMBIGUOUS,
            evidence=f"conflicting {name} evidence",
            provenance=Provenance.GEMINI_OCR,
        )
        for name in ("price", "sizes", "quantity")
    }
    result = await Pipeline(FakeGemini(GeminiExtraction(fields=fields))).run(
        PipelineInput(
            source_id=1,
            text="",
            media_paths=["supplier-card.jpg"],
            emoda_code="EM-TEST",
            profile=SourceProfileData(gemini_enabled=True, translation_enabled=False),
            markup_rules=RULES,
        ),
        dry_run=False,
    )

    assert result.needs_review is False
    assert result.review_reasons == []
    assert result.publication_requested is True
    assert result.rendered_text == (
        "<b>Код / Code:</b> EM-TEST\n"
        f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}\n"
        f"<b>Размеры / Sizes:</b> {CHECK_VALUE}\n"
        f"<b>Шт в серии / Pcs per lot:</b> {CHECK_VALUE}"
    )


async def test_missing_price_renders_check_value() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=1,
            text="Sizes: XL",
            media_paths=[],
            emoda_code="EM-000005",
            profile=SourceProfileData(),
            markup_rules=RULES,
        ),
        dry_run=True,
    )
    assert f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}" in result.rendered_text
    assert "English" not in result.rendered_text


async def test_translation_protects_product_values() -> None:
    translator = ProtectedTranslator(FakeGemini())
    result = await translator.translate(
        "Nike AB-123 XL costs $15", "RU", ["Nike", "AB-123", "XL", "$15"]
    )
    for value in ["Nike", "AB-123", "XL", "$15"]:
        assert value in result


async def test_translation_fails_when_placeholder_is_lost() -> None:
    with pytest.raises(TranslationIntegrityError):
        await ProtectedTranslator(BrokenTranslator()).translate(
            "Nike AB-123", "RU", ["Nike", "AB-123"]
        )


async def test_translation_protects_explicit_names_and_automatic_identifiers_values() -> None:
    original = "Nike AB-123, SKU123, NIKE, XL, $18, 100% cotton, 2 pockets"
    translated = await ProtectedTranslator(AdversarialTranslator()).translate(
        original, "RU", ["Nike", "NIKE"]
    )
    for value in ["Nike", "AB-123", "SKU123", "NIKE", "XL", "$18", "100%", "2"]:
        assert value in translated


async def test_translation_never_runs_for_publication() -> None:
    class TrackingGemini(FakeGemini):
        def __init__(self) -> None:
            super().__init__()
            self.translate_calls = 0

        async def translate(
            self, text: str, target_language: str, instruction: str = ""
        ) -> str:
            self.translate_calls += 1
            return "unreachable"

    gemini = TrackingGemini()
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=4,
            text="Price: 14 USD\nSizes: S M L XL\nQuantity: 6\nTOKA DETAY ELBISE",
            media_paths=[],
            emoda_code="AA00029",
            profile=SourceProfileData(translation_enabled=True),
            markup_rules=RULES,
        ),
        dry_run=False,
    )

    assert gemini.translate_calls == 0
    assert result.description_ru == ""
    assert result.description_en == ""
    assert result.needs_review is False
    assert "TOKA DETAY ELBISE" not in result.rendered_text
    assert "Please provide" not in result.rendered_text


async def test_missing_price_sizes_and_quantity_use_check_fallback_without_review() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=1,
            text="Beautiful dress",
            media_paths=["fixture.jpg"],
            emoda_code="EM-000011",
            profile=SourceProfileData(translation_enabled=False),
            markup_rules=RULES,
        ),
        dry_run=False,
    )
    assert result.needs_review is False
    assert result.publication_requested is True
    assert result.rendered_text == (
        "<b>Код / Code:</b> EM-000011\n"
        f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}\n"
        f"<b>Размеры / Sizes:</b> {CHECK_VALUE}\n"
        f"<b>Шт в серии / Pcs per lot:</b> {CHECK_VALUE}"
    )
    assert result.review_reasons == []


async def test_configured_repeated_variant_prices_do_not_force_review() -> None:
    profile = SourceProfileData(
        default_currency="USD",
        translation_enabled=False,
        repeated_pattern_rules=[
            RepeatedPatternRule(
                name="range-price",
                pattern=r"(?m)^(?P<size>\d+-\d+)-(?P<price>\d+)$",
                fields={"sizes": "size", "price": "price"},
                aggregation={"sizes": "unique", "price": "join_unique"},
                separators={"price": "/"},
                authoritative=True,
            )
        ],
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=92,
            text="42-48-27\n42-48-30",
            media_paths=[],
            emoda_code="AA90003",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=False,
    )

    assert result.parsed.fields["sizes"].value == "42-48"
    assert result.parsed.fields["price"].value == "27/30"
    assert result.price is None
    assert result.needs_review is False
    assert f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}" in result.rendered_text


async def test_missing_optional_quantity_allows_auto_and_renders_check_fallback() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=91,
            text="Price: 10 USD\nSizes: S M\nFloral dress",
            media_paths=["fixture.jpg"],
            emoda_code="AA90002",
            profile=SourceProfileData(
                required_fields=["price", "sizes"],
                translation_enabled=False,
            ),
            markup_rules=RULES,
        ),
        dry_run=False,
    )

    assert result.parsed.fields["quantity"].status is FieldStatus.MISSING
    assert result.needs_review is False
    assert result.publication_requested is True
    assert f"<b>Шт в серии / Pcs per lot:</b> {CHECK_VALUE}" in result.rendered_text


async def test_caption_and_ocr_price_conflict_is_ambiguous_without_review() -> None:
    ocr_price = ExtractedField(
        status=FieldStatus.FOUND,
        value="25",
        evidence="price tag: 25",
        provenance=Provenance.GEMINI_OCR,
        metadata={"currency": "USD"},
    )
    gemini = FakeGemini(GeminiExtraction(fields={"price": ocr_price}))
    result = await Pipeline(gemini).run(
        PipelineInput(
            source_id=1,
            text="Price: 20 USD\nSizes: M\n1 pcs",
            media_paths=["tag.jpg"],
            emoda_code="EM-000012",
            profile=SourceProfileData(
                gemini_enabled=True,
                text_priority=False,
                translation_enabled=False,
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=Decimal("100"),
                    markup=Decimal("1"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )
    assert result.parsed.fields["price"].status is FieldStatus.AMBIGUOUS
    assert result.price is None
    assert result.needs_review is False
    assert result.review_reasons == []
    assert f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}" in result.rendered_text
    assert "price" in cast(list, gemini.calls[0]["missing_fields"])


async def test_explicit_source_text_priority_resolves_ocr_conflict() -> None:
    ocr_price = ExtractedField(
        status=FieldStatus.FOUND,
        value="25",
        evidence="price tag: 25",
        provenance=Provenance.GEMINI_OCR,
        metadata={"currency": "USD"},
    )
    result = await Pipeline(FakeGemini(GeminiExtraction(fields={"price": ocr_price}))).run(
        PipelineInput(
            source_id=1,
            text="Price: 20 USD\nSizes: M\n1 pcs",
            media_paths=["tag.jpg"],
            emoda_code="EM-000013",
            profile=SourceProfileData(
                gemini_enabled=True,
                text_priority=True,
                translation_enabled=False,
            ),
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("0"),
                    max_price=Decimal("100"),
                    markup=Decimal("1"),
                    priority=1,
                )
            ],
        ),
        dry_run=True,
    )
    assert result.parsed.fields["price"].value == "20"
    assert result.price is not None and result.price.raw_price == Decimal("20")
    assert result.needs_review is False


def test_global_renderer_is_the_only_publication_template() -> None:
    parsed = RuleEngine(SourceProfileData()).parse("Sizes: XL")
    rendered = render_publication(
        code="EM-000007",
        parsed=parsed,
        price=None,
    )
    assert rendered == (
        "<b>Код / Code:</b> EM-000007\n"
        f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}\n"
        "<b>Размеры / Sizes:</b> XL\n"
        f"<b>Шт в серии / Pcs per lot:</b> {CHECK_VALUE}"
    )


def test_global_renderer_normal_payload_is_exactly_four_lines() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            fallback_rules={
                "sizes": {"value": "S, M, L", "authoritative": True},
                "quantity": {"value": "6", "authoritative": True},
            }
        )
    ).parse("Price: 20 USD")

    class _Price:
        final_usd = Decimal("23")

    rendered = render_publication(
        code="EM-TEST",
        parsed=parsed,
        price=cast(Any, _Price()),
    )

    assert rendered == (
        "<b>Код / Code:</b> EM-TEST\n"
        "<b>Цена за шт / Unit price:</b> $23\n"
        "<b>Размеры / Sizes:</b> S, M, L\n"
        "<b>Шт в серии / Pcs per lot:</b> 6"
    )
    assert len(rendered.splitlines()) == 4
    for forbidden in (
        "#EMODA_",
        "RU description",
        "EN description",
        "Gemini output",
        "supplier text",
        "title",
        "signature",
    ):
        assert forbidden not in rendered


def test_bilingual_renderer_complete_values_are_exact() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            fallback_rules={
                "sizes": {"value": "S(2)M(2)L(2)", "authoritative": True},
                "quantity": {"value": "6", "authoritative": True},
            }
        )
    ).parse("Price: 20 USD")

    class _Price:
        final_usd = Decimal("17")

    rendered = render_publication(
        code="G045 LP4002",
        parsed=parsed,
        price=cast(Any, _Price()),
    )

    assert rendered == (
        "<b>Код / Code:</b> G045 LP4002\n"
        "<b>Цена за шт / Unit price:</b> $17\n"
        "<b>Размеры / Sizes:</b> S(2)M(2)L(2)\n"
        "<b>Шт в серии / Pcs per lot:</b> 6"
    )
    assert len(rendered.splitlines()) == 4


@pytest.mark.parametrize("field", ["price", "sizes", "quantity"])
@pytest.mark.parametrize("status", [FieldStatus.MISSING, FieldStatus.AMBIGUOUS])
async def test_missing_or_ambiguous_customer_field_is_bilingual_fallback_and_auto(
    field: str, status: FieldStatus
) -> None:
    extracted = {
        field: ExtractedField(
            status=status,
            evidence=f"{field} evidence" if status is FieldStatus.AMBIGUOUS else None,
            provenance=Provenance.GEMINI_OCR,
        )
    }
    result = await Pipeline(FakeGemini(GeminiExtraction(fields=extracted))).run(
        PipelineInput(
            source_id=1,
            text="",
            media_paths=["label.jpg"],
            emoda_code="EM-TEST",
            profile=SourceProfileData(gemini_enabled=True),
            markup_rules=RULES,
        ),
        dry_run=False,
    )

    labels = {
        "price": "Цена за шт / Unit price:",
        "sizes": "Размеры / Sizes:",
        "quantity": "Шт в серии / Pcs per lot:",
    }
    assert CHECK_VALUE == NEW_CHECK_VALUE
    assert f"<b>{labels[field]}</b> {NEW_CHECK_VALUE}" in result.rendered_text
    assert all(legacy not in result.rendered_text for legacy in LEGACY_CHECK_VALUES)
    assert result.needs_review is False
    assert result.review_reasons == []
    assert result.publication_requested is True


async def test_internal_descriptions_and_extraction_metadata_never_leak() -> None:
    internal_only = {
        "title": "NEW SEASON title",
        "supplier description": "supplier prose",
        "RU description": "русское описание",
        "EN description": "English description",
        "Gemini text": "model/service output",
        "brand": "SecretBrand",
        "material": "SecretMaterial",
        "#debug": "reconciliation marker",
    }
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=1,
            text="\n".join(f"{key}: {value}" for key, value in internal_only.items()),
            media_paths=[],
            emoda_code="EM-NOLEAK",
            profile=SourceProfileData(),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert len([line for line in result.rendered_text.splitlines() if line]) == 4
    for value in internal_only.values():
        assert value not in result.rendered_text


def test_global_renderer_keeps_supplier_price_precision() -> None:
    from emoda.services.render import price_line

    class _Price:
        final_usd = Decimal("6.5")

    assert price_line(cast(Any, _Price()), None) == "<b>Цена за шт / Unit price:</b> $6.5"
    assert price_line(None, None) == f"<b>Цена за шт / Unit price:</b> {CHECK_VALUE}"


async def test_evidence_backed_gemini_category_reapplies_source_routing() -> None:
    category = ExtractedField(
        status=FieldStatus.FOUND,
        value="shoes",
        evidence="shoes",
        provenance=Provenance.GEMINI_TEXT,
    )
    profile = SourceProfileData(
        gemini_enabled=True,
        default_destination_id=1,
        routing_rules=[RoutingRule(category="shoes", destination_id=2, priority=1)],
    )
    result = await Pipeline(FakeGemini(GeminiExtraction(category=category))).run(
        PipelineInput(
            source_id=1,
            text="New shoes",
            media_paths=[],
            emoda_code="EM-000009",
            profile=profile,
            markup_rules=RULES,
        ),
        dry_run=True,
    )
    assert result.parsed.destination_id == 2


async def test_supplier_text_never_leaks_into_publication() -> None:
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=1,
            text="9834 elegant dress fiyat 13.5$\nElegant dress\nOrder Line : wa.me/905525311244",
            media_paths=[],
            emoda_code="AA00013",
            profile=SourceProfileData(
                translation_enabled=True,
                fallback_rules={
                    "sizes": {"value": "S, M, L", "authoritative": True},
                    "quantity": {"value": "6", "authoritative": True},
                },
            ),
            markup_rules=RULES,
        ),
        dry_run=True,
    )

    assert result.rendered_text == (
        "<b>Код / Code:</b> AA00013\n"
        "<b>Цена за шт / Unit price:</b> $17\n"
        "<b>Размеры / Sizes:</b> S, M, L\n"
        "<b>Шт в серии / Pcs per lot:</b> 6"
    )
    assert "13.5" not in result.rendered_text
    assert "Elegant" not in result.rendered_text
    assert "wa.me" not in result.rendered_text
    assert "English" not in result.rendered_text


async def test_translation_rejects_non_russian_visible_text_for_ru_target() -> None:
    with pytest.raises(ValueError, match="RU target language validation"):
        await ProtectedTranslator(InvalidRussianTranslator()).translate(
            "TOKA DETAY ELBISE", "RU", []
        )
