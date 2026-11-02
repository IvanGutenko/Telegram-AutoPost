from decimal import Decimal

import pytest

from emoda.domain.schemas import MarkupRange
from emoda.services.price import PriceEngine, PriceUnresolved

RULES = [
    MarkupRange(min_price=Decimal("0"), max_price=Decimal("5"), markup=Decimal("1"), priority=1),
    MarkupRange(min_price=Decimal("6"), max_price=Decimal("9"), markup=Decimal("2"), priority=2),
    MarkupRange(min_price=Decimal("10"), max_price=Decimal("15"), markup=Decimal("3"), priority=3),
]


@pytest.mark.parametrize(
    ("raw", "expected_markup", "expected_final"),
    [("5", "1", "6"), ("6", "2", "8"), ("9", "2", "11"), ("10", "3", "13"), ("15", "3", "18")],
)
def test_usd_markup_boundaries(raw: str, expected_markup: str, expected_final: str) -> None:
    result = PriceEngine(Decimal("1.2")).calculate(Decimal(raw), "USD", RULES)
    assert result.normalized_usd == Decimal(raw)
    assert result.markup == Decimal(expected_markup)
    assert result.final_usd == Decimal(expected_final)


def test_eur_converts_before_selecting_markup() -> None:
    result = PriceEngine(Decimal("1.2")).calculate(Decimal("10"), "EUR", RULES)
    assert result.normalized_usd == Decimal("12.0")
    assert result.markup == Decimal("3")
    assert result.final_usd == Decimal("15")


@pytest.mark.parametrize(
    ("raw", "normalized", "final"),
    [("5", "6.0", "8"), ("10", "12.0", "15"), ("17.20", "20.640", "24")],
)
def test_required_eur_examples_use_decimal_before_markup(
    raw: str, normalized: str, final: str
) -> None:
    rules = [
        MarkupRange(
            min_price=Decimal("0"),
            max_price=Decimal("100"),
            markup=Decimal("2") if raw == "5" else Decimal("3"),
            priority=1,
        )
    ]
    result = PriceEngine(Decimal("1.2")).calculate(Decimal(raw), "EUR", rules)
    assert result.normalized_usd == Decimal(normalized)
    assert result.final_usd == Decimal(final)


def test_final_price_uses_decimal_ceiling() -> None:
    custom = [
        MarkupRange(
            min_price=Decimal("0"), max_price=Decimal("5"), markup=Decimal("1.01"), priority=1
        )
    ]
    result = PriceEngine(Decimal("1.2")).calculate(Decimal("4.20"), "USD", custom)
    assert result.final_usd == Decimal("6")


def test_price_without_markup_range_is_unresolved() -> None:
    with pytest.raises(PriceUnresolved):
        PriceEngine(Decimal("1.2")).calculate(Decimal("16"), "USD", RULES)


def test_conflicting_ranges_are_rejected() -> None:
    rules = RULES + [
        MarkupRange(min_price=Decimal("4"), max_price=Decimal("7"), markup=Decimal("8"), priority=4)
    ]
    with pytest.raises(ValueError, match="overlap"):
        PriceEngine.validate_ranges(rules)


@pytest.mark.parametrize(
    ("raw", "expected_markup", "expected_final"),
    [("101", "25.25", "127"), ("120", "30", "150"), ("200", "50", "250")],
)
def test_open_ended_percentage_markup_uses_decimal_and_ceiling(
    raw: str, expected_markup: str, expected_final: str
) -> None:
    rules = [
        MarkupRange(
            min_price=Decimal("101"),
            max_price=None,
            markup=Decimal("25"),
            markup_type="percent",
            priority=1,
        )
    ]

    result = PriceEngine().calculate(Decimal(raw), "USD", rules)

    assert result.markup == Decimal(expected_markup)
    assert result.final_usd == Decimal(expected_final)


def test_fixed_range_ends_before_open_percentage_range() -> None:
    rules = [
        MarkupRange(
            min_price=Decimal("0"),
            max_price=Decimal("100.99"),
            markup=Decimal("10"),
            priority=1,
        ),
        MarkupRange(
            min_price=Decimal("101"),
            max_price=None,
            markup=Decimal("25"),
            markup_type="percent",
            priority=2,
        ),
    ]
    PriceEngine.validate_ranges(rules)
    assert PriceEngine().calculate(Decimal("100.99"), "USD", rules).final_usd == Decimal("111")
    assert PriceEngine().calculate(Decimal("101"), "USD", rules).final_usd == Decimal("127")
