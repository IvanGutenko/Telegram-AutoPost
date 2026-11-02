from __future__ import annotations

from decimal import Decimal

import pytest

from emoda.domain.enums import FieldStatus, Provenance
from emoda.domain.schemas import ExtractedField, MultiPriceLineItem, ParsedProduct, PriceResult
from emoda.services.render import render_publication, validate_publication_text

NEW_CHECK_VALUE = "Уточнить / Check"
LEGACY_CHECK_VALUES = ("Уточняйте у менеджера", "Ask the manager")


def _field(value: str) -> ExtractedField:
    return ExtractedField(
        status=FieldStatus.FOUND,
        value=value,
        evidence=value,
        provenance=Provenance.STRICT_TEXT,
    )


def _parsed(*, sizes: ExtractedField, quantity: ExtractedField) -> ParsedProduct:
    return ParsedProduct(
        original_text="",
        cleaned_text="",
        fields={
            "price": ExtractedField(),
            "sizes": sizes,
            "quantity": quantity,
        },
    )


def _price(value: str = "17") -> PriceResult:
    amount = Decimal(value)
    return PriceResult(
        raw_price=amount,
        raw_currency="USD",
        normalized_usd=amount,
        markup=Decimal("0"),
        final_usd=amount,
    )


def test_dynamic_size_price_labels_are_not_duplicated():
    rendered = render_publication(
        code="QA", parsed=_parsed(sizes=_field("42 44 46 48"), quantity=_field("4")),
        price=None, line_items=[MultiPriceLineItem(
            label_original="42 44 46 48", label_ru="42 44 46 48", label_en="42 44 46 48",
            evidence="42 44 46 48 29$", price=_price("29"),
        )],
    )
    assert "42 44 46 48: $29" in rendered
    assert "42 44 46 48 / 42 44 46 48" not in rendered


def test_global_renderer_bolds_exactly_four_lines() -> None:
    rendered = render_publication(
        code="G045 LP4002",
        parsed=_parsed(sizes=_field("S(2)M(2)L(2)"), quantity=_field("6")),
        price=_price(),
    )

    assert rendered == (
        "<b>Код / Code:</b> G045 LP4002\n"
        "<b>Цена за шт / Unit price:</b> $17\n"
        "<b>Размеры / Sizes:</b> S(2)M(2)L(2)\n"
        "<b>Шт в серии / Pcs per lot:</b> 6"
    )
    assert rendered.count("<b>") == 4
    assert rendered.count("</b>") == 4
    assert rendered.count("\n") == 3
    assert len(rendered.splitlines()) == 4
    validate_publication_text(rendered, code="G045 LP4002")


@pytest.mark.parametrize(
    "text,error",
    [
        (
            "Цена указана на фото поставщика.\nКомиссия E-MODA: +10%\nк указанной цене.",
            "exactly four lines",
        ),
        (
            "<b>Код / Code:</b> AA00001\n"
            "<b>Цена за шт / Unit price:</b> 17 долларов\n"
            "<b>Размеры / Sizes:</b> S, M\n"
            "<b>Шт в серии / Pcs per lot:</b> 4",
            r"must contain \$",
        ),
        (
            "<b>Код / Code:</b> WRONG\n"
            "<b>Цена за шт / Unit price:</b> $17\n"
            "<b>Размеры / Sizes:</b> S, M\n"
            "<b>Шт в серии / Pcs per lot:</b> 4",
            "code line",
        ),
    ],
)
def test_publication_validation_rejects_non_renderer_payloads(text: str, error: str) -> None:
    with pytest.raises(ValueError, match=error):
        validate_publication_text(text, code="AA00001")


def test_global_renderer_uses_check_fallback_for_missing_and_ambiguous_values() -> None:
    rendered = render_publication(
        code="EM-123",
        parsed=_parsed(
            sizes=ExtractedField(status=FieldStatus.MISSING),
            quantity=ExtractedField(
                status=FieldStatus.AMBIGUOUS,
                evidence="6 or 8",
                provenance=Provenance.STRICT_TEXT,
            ),
        ),
        price=None,
    )

    assert rendered == (
        "<b>Код / Code:</b> EM-123\n"
        f"<b>Цена за шт / Unit price:</b> {NEW_CHECK_VALUE}\n"
        f"<b>Размеры / Sizes:</b> {NEW_CHECK_VALUE}\n"
        f"<b>Шт в серии / Pcs per lot:</b> {NEW_CHECK_VALUE}"
    )
    assert rendered.count(NEW_CHECK_VALUE) == 3
    assert all(legacy not in rendered for legacy in LEGACY_CHECK_VALUES)
    assert len(rendered.splitlines()) == 4


def test_global_renderer_html_escapes_all_extracted_values() -> None:
    rendered = render_publication(
        code='EM<&>"123',
        parsed=_parsed(
            sizes=_field('S<&>"XL'),
            quantity=_field('6<&>"8'),
        ),
        price=_price(),
    )

    assert 'EM&lt;&amp;&gt;&quot;123' in rendered
    assert 'S&lt;&amp;&gt;&quot;XL' in rendered
    assert '6&lt;&amp;&gt;&quot;8' in rendered
    assert 'EM<&>"123' not in rendered
    assert 'S<&>"XL' not in rendered
    assert '6<&>"8' not in rendered
