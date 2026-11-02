from __future__ import annotations

import re
from decimal import Decimal
from html import escape

from emoda.domain.enums import FieldStatus
from emoda.domain.schemas import MultiPriceLineItem, ParsedProduct, PriceResult
from emoda.services.destination_price import SupplierQuote

CHECK_VALUE = "Уточнить / Check"
PUBLICATION_PARSE_MODE = "HTML"
CODE_PREFIX = "Код / Code:"
PRICE_PREFIX = "Цена за шт / Unit price:"
SIZES_PREFIX = "Размеры / Sizes:"
QUANTITY_PREFIX = "Шт в серии / Pcs per lot:"


def _value(parsed: ParsedProduct, *names: str) -> str | None:
    values = [
        parsed.fields[name].value
        for name in names
        if parsed.fields.get(name) and parsed.fields[name].status is FieldStatus.FOUND
    ]
    return " / ".join(value for value in values if value) or None


def _money(value: Decimal) -> str:
    # The published price keeps the supplier's precision: $6.5 stays $6.5.
    normalized = value.normalize()
    if normalized == normalized.to_integral_value():
        normalized = normalized.quantize(Decimal("1"))
    return format(normalized, "f")


def _label(prefix: str) -> str:
    return f"<b>{prefix}</b>"


def _field_html(value: str) -> str:
    """Keep the Bot API payload four-lined while preserving displayed line breaks.

    Telegram's HTML parse mode supports numeric entities. Encoding field-local
    newlines prevents a multi-line passthrough value from creating additional
    payload lines; Telegram decodes them back into line breaks for customers.
    """

    return escape(value).replace("\r\n", "&#10;").replace("\r", "&#10;").replace("\n", "&#10;")


def price_line(price: PriceResult | None, line_items: list[MultiPriceLineItem] | None) -> str:
    if line_items:
        values = "; ".join(
            f"{_field_html(item.label_ru if item.label_ru == item.label_en else item.label_ru + ' / ' + item.label_en)}: "
            f"${_money(item.price.final_usd)}"
            for item in line_items
        )
        return f"{_label(PRICE_PREFIX)} {values}"
    if price is not None:
        return f"{_label(PRICE_PREFIX)} ${_money(price.final_usd)}"
    return f"{_label(PRICE_PREFIX)} {CHECK_VALUE}"


def render_publication(
    *,
    code: str,
    parsed: ParsedProduct,
    price: PriceResult | None,
    line_items: list[MultiPriceLineItem] | None = None,
    publication_format: str = "standard",
    supplier_price: SupplierQuote | None = None,
    alternative_text: str | None = None,
) -> str:
    """The single global publication renderer.

    The standard format publishes exactly code, price, sizes and quantity. The
    two explicitly configured exceptions are supplier-percent and escaped
    source/destination alternative text. Missing or ambiguous standard values
    fall back to the bilingual check value and the product still flows to AUTO.
    """
    if publication_format == "alternative_text":
        if not alternative_text or not alternative_text.strip():
            raise ValueError("alternative publication text is empty")
        return escape(alternative_text.strip())
    if publication_format == "supplier_percent":
        value = (
            f"{_money(supplier_price.amount)} {supplier_price.currency}"
            if supplier_price is not None
            else "Уточнить у менеджера"
        )
        return f"<b>Цена:</b> {escape(value)}"
    line_items = line_items or []
    sizes = _value(parsed, "sizes")
    quantity = _value(parsed, "quantity")
    code_line = f"{_label(CODE_PREFIX)} {escape(code)}"
    sizes_line = (
        f"{_label(SIZES_PREFIX)} {_field_html(sizes)}"
        if sizes
        else f"{_label(SIZES_PREFIX)} {CHECK_VALUE}"
    )
    quantity_line = (
        f"{_label(QUANTITY_PREFIX)} {_field_html(quantity)}"
        if quantity
        else f"{_label(QUANTITY_PREFIX)} {CHECK_VALUE}"
    )
    rendered_price = price_line(price, line_items)
    return "\n".join([code_line, rendered_price, sizes_line, quantity_line])


def validate_publication_text(
    text: str, *, code: str, publication_format: str = "standard"
) -> None:
    """Reject any live payload that did not preserve the global renderer contract."""

    if publication_format == "alternative_text":
        if not text.strip():
            raise ValueError("alternative publication text is empty")
        return
    if publication_format == "supplier_percent":
        if not re.fullmatch(
            r"<b>Цена:</b> (?:Уточнить у менеджера|[0-9]+(?:\.[0-9]+)? [A-Z]{3})", text
        ):
            raise ValueError("supplier-percent publication must contain only a validated price")
        return
    if publication_format != "standard":
        raise ValueError("unsupported publication format")
    lines = text.splitlines()
    if len(lines) != 4:
        raise ValueError("publication must contain exactly four lines")
    prefixes = (CODE_PREFIX, PRICE_PREFIX, SIZES_PREFIX, QUANTITY_PREFIX)
    values: list[str] = []
    for line, prefix in zip(lines, prefixes, strict=True):
        marker = f"{_label(prefix)} "
        if not line.startswith(marker):
            raise ValueError(f"publication line must start with {marker}")
        value = line.removeprefix(marker)
        if not value:
            raise ValueError(f"publication value is empty after {prefix}")
        values.append(value)
    if values[0] != escape(code):
        raise ValueError("publication code line does not match the product")
    if values[1] != CHECK_VALUE and "$" not in values[1]:
        raise ValueError("resolved publication price must contain $")


def telegram_chunks(text: str, limit: int = 4096) -> list[str]:
    if limit < 1:
        raise ValueError("limit must be positive")
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        split = remaining.rfind("\n", 0, limit + 1)
        if split <= 0:
            split = limit
        chunks.append(remaining[:split].rstrip())
        remaining = remaining[split:].lstrip("\n")
    if remaining:
        chunks.append(remaining)
    return chunks
