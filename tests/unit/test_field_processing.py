from decimal import Decimal
from html import unescape

import pytest

from emoda.domain.enums import FieldStatus, Provenance
from emoda.domain.schemas import (
    ComputedRule,
    ExtractedField,
    ExtractionRule,
    GeminiExtraction,
    MarkupRange,
    SourceProfileData,
)
from emoda.services.orchestrator import ProcessingMode, process_product
from emoda.services.pipeline import Pipeline, PipelineInput
from emoda.services.render import CHECK_VALUE

RAW_BODY_PATTERN = (
    r"(?ms)\A(?:(?:[^\r\n]*//[^\r\n]*)\r?\n)+\s*(?P<value>.+?)\s*\Z"
)


class FakeGemini:
    def __init__(self, extraction: GeminiExtraction) -> None:
        self.extraction = extraction

    async def extract(self, **_: object) -> GeminiExtraction:
        return self.extraction

    async def translate(self, text: str, target_language: str, instruction: str = "") -> str:
        return text


def passthrough_profile() -> SourceProfileData:
    return SourceProfileData(
        field_policies={"sizes": {"processing_mode": "passthrough"}},
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=RAW_BODY_PATTERN,
                    authoritative=True,
                )
            ]
        },
    )


def pipeline_input(text: str, profile: SourceProfileData) -> PipelineInput:
    return PipelineInput(
        source_id=101,
        text=text,
        media_paths=[],
        emoda_code="RAW0001",
        profile=profile,
        markup_rules=[
            MarkupRange(
                min_price=Decimal("0"),
                max_price=None,
                markup=Decimal("0"),
                priority=1,
            )
        ],
    )


@pytest.mark.parametrize(
    "raw_sizes",
    [
        "XS-1 S-2 M-2 L-1",
        "34-1 36-2 38-2 40-1",
        "XS(1) S(1) M(2) L(1) XL(1)",
    ],
)
async def test_passthrough_preserves_nonstandard_sizes_exactly(raw_sizes: str) -> None:
    result = await Pipeline(None).run(
        pipeline_input(f"1575-2 // 1602\n\n{raw_sizes}", passthrough_profile()),
        dry_run=True,
    )

    sizes = result.parsed.fields["sizes"]
    assert sizes.status is FieldStatus.FOUND
    assert sizes.value == raw_sizes
    assert sizes.evidence is not None and raw_sizes in sizes.evidence
    assert sizes.provenance is Provenance.SOURCE_RULE
    assert sizes.metadata["processing_mode"] == "passthrough"
    assert sizes.metadata["rule_id"] == "sizes:1"
    assert f"Размеры / Sizes:</b> {raw_sizes}" in result.rendered_text
    assert result.parsed.fields["price"].status is FieldStatus.MISSING
    assert result.parsed.fields["quantity"].status is FieldStatus.MISSING
    assert result.rendered_text.count(CHECK_VALUE) == 2
    assert result.needs_review is False


async def test_passthrough_preserves_configured_multiline_fragment_in_order() -> None:
    raw_sizes = (
        "(S.SERIA)\n"
        "34-1 36-2 38-2 40-2\n\n"
        "(W.SERIA)\n"
        "34-1 36-1 38-2 40-2 42-1 44-1"
    )
    result = await Pipeline(None).run(
        pipeline_input(f"1477-4 // 1576\n{raw_sizes}", passthrough_profile()),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == raw_sizes
    assert raw_sizes in unescape(result.rendered_text)
    assert "&#10;" in result.rendered_text
    assert len(result.rendered_text.splitlines()) == 4
    assert result.parsed.fields["quantity"].status is FieldStatus.MISSING


async def test_configured_capture_excludes_article_model_color_junk_from_raw_sizes() -> None:
    raw_sizes = "XS-1 S-2 M-2 L-1"
    text = "1575-2 // 1602\nTR-56 SİYAH //1426\n\n" + raw_sizes

    result = await Pipeline(None).run(
        pipeline_input(text, passthrough_profile()), dry_run=True
    )

    assert result.parsed.fields["sizes"].value == raw_sizes
    assert "1575-2" not in result.parsed.fields["sizes"].value
    assert "TR-56" not in result.parsed.fields["sizes"].value
    assert "SİYAH" not in result.parsed.fields["sizes"].value


async def test_passthrough_does_not_infer_quantity_without_computed_rule() -> None:
    result = await Pipeline(None).run(
        pipeline_input("MODEL // 100\nS-1 M-2 L-2", passthrough_profile()),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == "S-1 M-2 L-2"
    assert result.parsed.fields["quantity"].status is FieldStatus.MISSING
    assert "Pcs per lot:</b> Уточнить / Check" in result.rendered_text


async def test_gemini_passthrough_candidate_keeps_evidence_and_provenance() -> None:
    value = "XS-1 S-2 M-2 L-1"
    gemini = GeminiExtraction(
        fields={
            "sizes": ExtractedField(
                status=FieldStatus.FOUND,
                value=value,
                evidence=value,
                provenance=Provenance.GEMINI_TEXT,
            )
        }
    )
    profile = SourceProfileData(
        gemini_enabled=True,
        field_policies={"sizes": {"processing_mode": "passthrough"}},
    )

    result = await Pipeline(FakeGemini(gemini)).run(
        pipeline_input(value, profile), dry_run=True
    )

    sizes = result.parsed.fields["sizes"]
    assert sizes.value == value
    assert sizes.evidence == value
    assert sizes.provenance is Provenance.GEMINI_TEXT
    assert sizes.metadata["processing_mode"] == "passthrough"


async def test_normal_mode_and_source_profiles_remain_isolated() -> None:
    raw_result = await Pipeline(None).run(
        pipeline_input("MODEL // 1\nS1 M2 L2", passthrough_profile()), dry_run=True
    )
    normal_profile = SourceProfileData(
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?P<value>(?:[A-Z]+\d+\s*)+)",
                    transform="series_sizes",
                    authoritative=True,
                )
            ]
        }
    )
    normal_result = await Pipeline(None).run(
        pipeline_input("S1 M2 L2", normal_profile), dry_run=True
    )

    assert raw_result.parsed.fields["sizes"].value == "S1 M2 L2"
    assert normal_result.parsed.fields["sizes"].value == "S, M, L"
    assert "processing_mode" not in normal_result.parsed.fields["sizes"].metadata


async def test_passthrough_bypasses_a_configured_normalizing_transform() -> None:
    profile = SourceProfileData(
        field_policies={"sizes": {"processing_mode": "passthrough"}},
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Series:\s*(?P<value>(?:[A-Z]+\d+\s*)+)",
                    transform="series_sizes",
                    authoritative=True,
                )
            ]
        },
    )

    result = await Pipeline(None).run(
        pipeline_input("Series: S2 M2 L1", profile), dry_run=True
    )

    sizes = result.parsed.fields["sizes"]
    assert sizes.value == "S2 M2 L1"
    assert sizes.metadata["processing_mode"] == "passthrough"
    assert sizes.metadata["configured_transform_bypassed"] == "series_sizes"


async def test_passthrough_keeps_normal_price_validation_isolated() -> None:
    profile = SourceProfileData(
        field_policies={"sizes": {"processing_mode": "passthrough"}},
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?m)^Sizes:\s*(?P<value>[^\n]+)$",
                    authoritative=True,
                )
            ],
            "price": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Price:\s*(?P<value>not-a-number)",
                    authoritative=True,
                )
            ],
        },
    )

    result = await Pipeline(None).run(
        pipeline_input("Price: not-a-number\nSizes: XS-1 S-2 M-2 L-1", profile),
        dry_run=True,
    )

    assert result.parsed.fields["sizes"].value == "XS-1 S-2 M-2 L-1"
    assert result.parsed.fields["price"].status is FieldStatus.FOUND
    assert result.price is None
    assert "Unit price:</b> Уточнить / Check" in result.rendered_text


async def test_passthrough_runs_only_explicitly_configured_quantity_computation() -> None:
    profile = passthrough_profile().model_copy(
        update={
            "computed_rules": [
                ComputedRule(
                    name="explicit-size-count-sum",
                    target_field="quantity",
                    operation="sum_matches",
                    source_field="sizes",
                    pattern=r"[A-Z]+-(?P<count>\d+)",
                    value_group="count",
                    authoritative=True,
                )
            ]
        }
    )

    result = await Pipeline(None).run(
        pipeline_input("MODEL // 1\nS-1 M-2 L-2", profile), dry_run=True
    )

    assert result.parsed.fields["sizes"].value == "S-1 M-2 L-2"
    assert result.parsed.fields["quantity"].value == "5"
    assert result.parsed.fields["quantity"].provenance is Provenance.COMPUTED_RULE


async def test_passthrough_does_not_bypass_source_candidate_conflicts() -> None:
    profile = SourceProfileData(
        field_policies={"sizes": {"processing_mode": "passthrough"}},
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"A:\s*(?P<value>[^\n]+)",
                    priority=10,
                ),
                ExtractionRule(
                    kind="regex",
                    pattern=r"B:\s*(?P<value>[^\n]+)",
                    priority=10,
                ),
            ]
        },
    )

    result = await Pipeline(None).run(
        pipeline_input("A: XS-1 S-2\nB: M-2 L-1", profile), dry_run=True
    )

    sizes = result.parsed.fields["sizes"]
    assert sizes.status is FieldStatus.AMBIGUOUS
    assert sizes.value is None
    assert sizes.provenance is Provenance.SOURCE_RULE
    assert sizes.metadata["processing_mode"] == "passthrough"
    assert "Sizes:</b> Уточнить / Check" in result.rendered_text


async def test_passthrough_dry_and_live_payloads_are_identical() -> None:
    value = "34-1 36-2 38-2 40-1"
    input_data = pipeline_input(f"MODEL // 1\n{value}", passthrough_profile())

    dry = await process_product(Pipeline(None), input_data, ProcessingMode.DRY)
    live = await process_product(Pipeline(None), input_data, ProcessingMode.LIVE)

    assert dry.rendered_text == live.rendered_text
    assert dry.parsed.fields["sizes"] == live.parsed.fields["sizes"]
    assert dry.publication_requested is False
    assert live.publication_requested is True


async def test_ambiguous_raw_series_quantity_publishes_check_without_review() -> None:
    profile = SourceProfileData(
        field_policies={"sizes": {"processing_mode": "passthrough"}},
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Sizes:\s*(?P<value>[^\n]+)",
                    authoritative=True,
                )
            ]
        },
        computed_rules=[
            ComputedRule(
                name="raw-series",
                target_field="quantity",
                operation="count_series_items",
                source_field="sizes",
                input_mode="raw",
                series_separator_pattern=r"\s+",
                range_pattern=r"(?P<start>\d+)-(?P<end>\d+)",
                explicit_list_pattern=r"(?P<item>\d+)",
                explicit_separator_pattern=r"-",
                numeric_step=Decimal("2"),
            )
        ],
    )

    result = await Pipeline(None).run(
        pipeline_input("Sizes: 42-48 36-44", profile), dry_run=True
    )

    assert result.parsed.fields["sizes"].value == "42-48 36-44"
    assert result.parsed.fields["quantity"].status is FieldStatus.AMBIGUOUS
    assert result.parsed.fields["quantity"].metadata["series_counts"] == [4, 5]
    assert "Pcs per lot:</b> Уточнить / Check" in result.rendered_text
    assert len(result.rendered_text.splitlines()) == 4
    assert result.needs_review is False


async def test_gemini_cannot_override_ambiguous_raw_series_quantity() -> None:
    profile = SourceProfileData(
        gemini_enabled=True,
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Sizes:\s*(?P<value>[^\n]+)",
                    authoritative=True,
                )
            ]
        },
        computed_rules=[
            ComputedRule(
                name="raw-series-safe-ambiguity",
                target_field="quantity",
                operation="count_series_items",
                source_field="sizes",
                input_mode="raw",
                series_separator_pattern=r"\s+",
                range_pattern=r"(?P<start>\d+)-(?P<end>\d+)",
                numeric_step=Decimal("2"),
            )
        ],
    )
    gemini = FakeGemini(
        GeminiExtraction(
            fields={
                "quantity": ExtractedField(
                    status=FieldStatus.FOUND,
                    value="4",
                    evidence="42-48",
                    provenance=Provenance.GEMINI_TEXT,
                )
            }
        )
    )

    result = await Pipeline(gemini).run(
        pipeline_input("Sizes: 42-48 36-44", profile), dry_run=True
    )

    quantity = result.parsed.fields["quantity"]
    assert quantity.status is FieldStatus.AMBIGUOUS
    assert quantity.value is None
    assert quantity.provenance is Provenance.COMPUTED_RULE
    assert quantity.metadata["series_counts"] == [4, 5]
    assert "Pcs per lot:</b> Уточнить / Check" in result.rendered_text
    assert result.needs_review is False


async def test_count_series_items_has_exact_dry_live_parity() -> None:
    profile = SourceProfileData(
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Sizes:\s*(?P<value>[^\n]+)",
                    authoritative=True,
                )
            ]
        },
        computed_rules=[
            ComputedRule(
                name="raw-series-parity",
                target_field="quantity",
                operation="count_series_items",
                source_field="sizes",
                input_mode="raw",
                series_separator_pattern=r"\s+",
                range_pattern=r"(?P<start>\d+)-(?P<end>\d+)",
                explicit_list_pattern=r"(?P<item>\d+)",
                explicit_separator_pattern=r"-",
                numeric_step=Decimal("2"),
            )
        ],
    )
    input_data = pipeline_input("Sizes: 42-48 50-56", profile)

    dry = await process_product(Pipeline(None), input_data, ProcessingMode.DRY)
    live = await process_product(Pipeline(None), input_data, ProcessingMode.LIVE)

    assert dry.rendered_text == live.rendered_text
    assert dry.parsed == live.parsed
    assert dry.parsed.fields["quantity"].value == "4"
    assert dry.parsed.fields["quantity"].metadata["series_counts"] == [4, 4]
    assert dry.publication_requested is False
    assert live.publication_requested is True
