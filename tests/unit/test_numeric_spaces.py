from decimal import Decimal

import pytest

from emoda.admin.service import RULE_TRANSFORMS, update_profile_from_lines
from emoda.domain.schemas import ExtractionRule, MarkupRange, SourceProfileData
from emoda.rules.numbers import normalize_number
from emoda.services.pipeline import Pipeline, PipelineInput


@pytest.mark.parametrize("transform", ["identity", "normalize_number"])
@pytest.mark.parametrize("raw,expected", [
    ("14", "14"), ("1 4", "14"), ("8 .9", "8.9"), ("6 .75", "6.75"),
    ("9 .75", "9.75"), ("12.50", "12.50"), ("12,50", "12.50"),
    ("12 . 50", "12.50"), ("17 , 5", "17.5"), ("1\u00a04\u202f. 5", "14.5"),
])
async def test_price_candidate_spaces(raw, expected, transform):
    profile = SourceProfileData(gemini_enabled=False, default_currency="USD", extraction_rules={
        "price": [ExtractionRule(kind="regex", pattern=r"Price:\s*(?P<value>[^$\r\n]+)\$", transform=transform)],
        "sizes": [ExtractionRule(kind="regex", pattern=r"Sizes: (?P<value>[^\n]+)")],
    })
    text = f"Price: {raw} $ / 1 Pieces\nSizes: 1 4\nQuantity: 6"
    result = await Pipeline(None).run(PipelineInput(
        source_id=987654, text=text, media_paths=[], emoda_code="EM-TEST", profile=profile,
        markup_rules=[MarkupRange(min_price=Decimal(0), max_price=None, markup=Decimal(0), priority=1)],
    ), dry_run=True)
    assert result.price is not None
    assert result.price.raw_price == Decimal(expected)
    assert result.parsed.fields["price"].value == expected
    assert result.parsed.fields["sizes"].value == "1 4"
    assert result.parsed.original_text == text


def test_cleaning_only_profile_input():
    result = update_profile_from_lines(SourceProfileData(), r"cleaning_pattern=Advertisement:.*")
    assert result.cleaning_patterns == [r"Advertisement:.*"]


def test_number_transform_available_to_admin():
    assert "normalize_number" in RULE_TRANSFORMS


@pytest.mark.parametrize("raw", ["8 / 9", "8-9", "8 . 9 . 5", "12,50.75", "8\n9", "8\u20289", "Price: 1 4 $"])
def test_number_normalization_does_not_join_other_values(raw):
    assert normalize_number(raw) == raw


def test_empty_profile_input_still_rejected():
    with pytest.raises(ValueError, match="at least one"):
        update_profile_from_lines(SourceProfileData(), " \n")


def test_profile_cleaning_preserves_unicode_regex():
    pattern = "[\u2028\u2029]"
    result = update_profile_from_lines(SourceProfileData(), "cleaning_pattern=" + pattern)
    assert result.cleaning_patterns == [pattern]


def test_profile_cleaning_can_be_cleared_through_admin():
    result = update_profile_from_lines(SourceProfileData(cleaning_patterns=["old"]), "cleaning_pattern=")
    assert result.cleaning_patterns == []
