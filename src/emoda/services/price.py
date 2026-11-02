import re
from collections.abc import Mapping
from decimal import ROUND_CEILING, Decimal

from emoda.domain.schemas import MarkupRange, PriceResult


class PriceUnresolved(ValueError):
    """Raised when price cannot safely be calculated."""


class PriceEngine:
    def __init__(
        self,
        eur_usd_rate: Decimal = Decimal("1.2"),
        *,
        currency_rates: dict[str, Decimal] | None = None,
    ) -> None:
        self.eur_usd_rate = eur_usd_rate
        self.currency_rates = self.validate_currency_rates(currency_rates or {})

    @staticmethod
    def validate_currency_rates(rates: Mapping[str, object]) -> dict[str, Decimal]:
        validated = {}
        for currency, value in rates.items():
            rate = Decimal(str(value))
            if not re.fullmatch(r"[A-Z]{3}", currency) or currency in {"USD", "EUR"}:
                raise ValueError(
                    "Use an ISO currency code other than USD/EUR; their rates are fixed"
                )
            if not rate.is_finite() or rate <= 0:
                raise ValueError("Currency rate must be finite and positive")
            validated[currency] = rate
        return validated

    @staticmethod
    def validate_ranges(rules: list[MarkupRange]) -> None:
        enabled = sorted((r for r in rules if r.enabled), key=lambda r: r.min_price)
        for previous, current in zip(enabled, enabled[1:], strict=False):
            if previous.max_price is None or current.min_price <= previous.max_price:
                raise ValueError("markup ranges overlap")

    def calculate(self, raw_price: Decimal, currency: str, rules: list[MarkupRange]) -> PriceResult:
        if raw_price < 0:
            raise PriceUnresolved("negative price")
        normalized_currency = currency.upper()
        if normalized_currency == "TRY" and normalized_currency not in self.currency_rates:
            raise PriceUnresolved("currency_rate_missing:TRY")
        if normalized_currency not in {"USD", "EUR", *self.currency_rates}:
            raise PriceUnresolved("unsupported currency")
        self.validate_ranges(rules)
        normalized = (
            raw_price
            if normalized_currency == "USD"
            else raw_price * self.eur_usd_rate
            if normalized_currency == "EUR"
            else raw_price / self.currency_rates[normalized_currency]
        )
        matches = [
            r
            for r in rules
            if r.enabled
            and r.min_price <= normalized
            and (r.max_price is None or normalized <= r.max_price)
        ]
        if len(matches) != 1:
            raise PriceUnresolved("no unambiguous markup range")
        selected = matches[0]
        markup = (
            selected.markup
            if selected.markup_type == "fixed"
            else normalized * selected.markup / Decimal("100")
        )
        final = (normalized + markup).to_integral_value(rounding=ROUND_CEILING)
        return PriceResult(
            raw_price=raw_price,
            raw_currency=normalized_currency,
            normalized_usd=normalized,
            markup=markup,
            final_usd=final,
        )
