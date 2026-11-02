from decimal import Decimal, InvalidOperation
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from emoda.domain.enums import FieldStatus
from emoda.domain.schemas import ParsedProduct

DESTINATION_PRICE_SETTING = "destination_price_policies"


class DestinationPricePolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["standard", "supplier_percent"] = "standard"
    percent: Decimal = Field(default=Decimal("10"), ge=0, le=1000, allow_inf_nan=False)
    currency_mode: Literal["supplier", "usd"] = "supplier"
    alternative_text: str | None = Field(default=None, max_length=4096)

    @field_validator("alternative_text")
    @classmethod
    def normalize_alternative_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None


class SupplierQuote(BaseModel):
    raw_amount: Decimal
    raw_currency: str
    amount: Decimal
    currency: str


def supplier_quote(
    parsed: ParsedProduct, policy: DestinationPricePolicy, currency_rates: dict[str, Decimal]
) -> SupplierQuote | None:
    price = parsed.fields.get("price")
    if price is None or price.status is not FieldStatus.FOUND or not price.value:
        return None
    currency = price.metadata.get("currency")
    if (
        not isinstance(currency, str)
        or len(currency) != 3
        or not currency.isascii()
        or not currency.isupper()
    ):
        return None
    try:
        raw = Decimal(price.value.replace(",", "."))
        if not raw.is_finite() or raw < 0:
            return None
        amount = raw
        output_currency = currency
        if policy.currency_mode == "usd":
            if currency == "EUR":
                amount *= Decimal("1.2")
            elif currency != "USD":
                rate = currency_rates.get(currency)
                if rate is None or not rate.is_finite() or rate <= 0:
                    return None
                amount /= rate
            output_currency = "USD"
        amount *= Decimal("1") + policy.percent / Decimal("100")
        return SupplierQuote(
            raw_amount=raw, raw_currency=currency, amount=amount, currency=output_currency
        )
    except InvalidOperation:
        return None
