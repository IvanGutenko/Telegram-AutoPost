from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Literal

from emoda.domain.enums import ProductStatus, ReviewReasonCode
from emoda.domain.schemas import ReviewReason

if TYPE_CHECKING:
    from emoda.db.models import Product


class StaleReviewApproval(ValueError):
    """The button belongs to a different persisted product snapshot."""


ReviewPublishOutcome = Literal["published", "already_published", "in_progress", "failed"]


@dataclass(frozen=True, slots=True)
class ReviewPublishResult:
    product_id: int
    outcome: ReviewPublishOutcome
    status: ProductStatus
    outbox_id: int | None = None
    detail: str | None = None


def _stable_scalar(value: object) -> object:
    """Normalize DB round-trip differences that are not content changes."""
    if isinstance(value, Decimal):
        return format(value.normalize(), "f")
    if isinstance(value, datetime):
        aware = value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
        return aware.isoformat(timespec="microseconds")
    return value


def review_approval_token(product: Product) -> str:
    """Short content-version token embedded into every REVIEW Publish button.

    Notification timestamps are intentionally excluded: notifying an admin is
    not a product change. Everything that can alter the rendered publication or
    its provenance is included, so an old card cannot approve a newer mixture.
    """
    snapshot = {
        "id": product.id,
        "emoda_code": product.emoda_code,
        "source_id": product.source_id,
        "source_message_id": product.source_message_id,
        "album_group_id": product.album_group_id,
        "destination_id": product.destination_id,
        "status": product.status.value,
        "original_text": product.original_text,
        "cleaned_text": product.cleaned_text,
        "final_text": product.final_text,
        "parsed_result": product.parsed_result,
        "review_reasons": product.review_reasons,
        "raw_price": _stable_scalar(product.raw_price),
        "raw_currency": product.raw_currency,
        "normalized_usd": _stable_scalar(product.normalized_usd),
        "applied_markup": _stable_scalar(product.applied_markup),
        "final_price": _stable_scalar(product.final_price),
        "processing_error": product.processing_error,
        "published_at": _stable_scalar(product.published_at),
    }
    encoded = json.dumps(
        snapshot,
        ensure_ascii=False,
        sort_keys=True,
        default=str,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.blake2s(encoded, digest_size=8).hexdigest()


def reason(
    code: ReviewReasonCode,
    *,
    detail: str | None = None,
    field: str | None = None,
) -> ReviewReason:
    return ReviewReason(code=code, detail=detail, field=field)


def review_reason_details(item: dict[str, Any]) -> str:
    code = str(item.get("code") or "system_error")
    stage = str(item.get("stage") or (
        "collection" if "collection" in code else
        "media_download" if "media_download" in code else
        "routing" if "destination" in code else
        "publication_policy" if code in {"source_publication_mode_review", "source_edit_review", "manual_rule"} else
        "publication" if "publication" in code else
        "extraction" if any(word in code for word in ("parser", "gemini", "ocr", "price")) else
        "processing"
    ))
    field = item.get("field") or "не относится к полю"
    rule = item.get("rule_id") or code
    detail = item.get("detail") or "Причина не сохранена в старой записи; нужна диагностика исходных сообщений"
    return f"stage={stage}; field={field}; rule={rule}; {code}: {detail}"


def classify_review_messages(messages: list[str]) -> list[ReviewReason]:
    result: list[ReviewReason] = []
    seen: set[tuple[ReviewReasonCode, str | None, str | None]] = set()
    for message in messages:
        code, field = _classify(message)
        identity = (code, message, field)
        if identity in seen:
            continue
        seen.add(identity)
        result.append(reason(code, detail=message, field=field))
    return result


def set_product_review_reason(
    product: Product,
    code: ReviewReasonCode,
    *,
    detail: str,
    field: str | None = None,
    append: bool = False,
) -> None:
    item = reason(code, detail=detail, field=field).model_dump(mode="json")
    existing = list(product.review_reasons or []) if append else []
    if item not in existing:
        existing.append(item)
    product.review_reasons = existing


def _classify(message: str) -> tuple[ReviewReasonCode, str | None]:
    lowered = message.casefold()
    field_match = re.search(r"(?:fields?|conflict):\s*(.+)$", message, re.IGNORECASE)
    field = field_match.group(1).strip() if field_match else None
    if "ambiguous" in lowered:
        return ReviewReasonCode.PARSER_AMBIGUOUS, field
    if "multi-price" in lowered or "price currency" in lowered or "price unresolved" in lowered:
        return ReviewReasonCode.CONFLICTING_PRICE, field or "price"
    if "ocr" in lowered or "text/image conflict" in lowered:
        return ReviewReasonCode.UNSAFE_OCR_CONFLICT, field
    if "translation" in lowered or "sanitation" in lowered:
        return ReviewReasonCode.TRANSLATION_FAILED, field
    if "gemini" in lowered and ("unavailable" in lowered or "failed" in lowered):
        return ReviewReasonCode.GEMINI_EXTRACTION_FAILED, field
    if "rejected" in lowered and "gemini" in lowered:
        return ReviewReasonCode.GEMINI_EVIDENCE_REJECTED, field
    if "destination" in lowered:
        return ReviewReasonCode.DESTINATION_MISSING, field
    if "publication" in lowered:
        return ReviewReasonCode.PUBLICATION_FAILED, field
    if "media" in lowered and ("download" in lowered or "persistence" in lowered):
        return ReviewReasonCode.MEDIA_DOWNLOAD_FAILED, field
    if "collection" in lowered:
        return ReviewReasonCode.COLLECTION_AMBIGUOUS, field
    return ReviewReasonCode.SYSTEM_ERROR, field
