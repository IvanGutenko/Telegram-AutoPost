from __future__ import annotations

import copy
import hashlib
import json
import re
from datetime import UTC, datetime
from typing import Any, Protocol
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from emoda.db.models import Destination, Product, PublicationAttempt, PublicationOutbox
from emoda.domain.enums import ButtonAction, OutboxStatus, ProductStatus, ReviewReasonCode
from emoda.domain.schemas import NavigationButtonData, validate_safe_https_url
from emoda.services.destination_access import DestinationAccessError
from emoda.services.outcomes import set_product_message_outcome
from emoda.services.render import telegram_chunks
from emoda.services.review import set_product_review_reason

PUBLICATION_BUTTONS_SETTING_KEY = "publication_buttons"
DEFAULT_PUBLICATION_BUTTONS: dict[str, dict[str, str | None]] = {
    "order": {"text": "🛒 Заказать / Order", "url": "https://wa.me/905552942994"},
    "catalog": {"text": "📂 Каталог / Catalog", "url": None},
    "check_price": {"text": "💲 Уточнить цену / Check Price", "url": None},
}


class Publisher(Protocol):
    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]: ...
    async def update(
        self,
        destination_channel_id: int,
        message_ids: list[int],
        old_payload: dict[str, object],
        new_payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]: ...
    async def find_by_code(
        self, destination_channel_id: int, emoda_code: str
    ) -> list[int] | None: ...
    async def delete(self, destination_channel_id: int, message_ids: list[int]) -> None: ...


class SafePublicationFailure(RuntimeError):
    """Failure known to occur before Telegram accepted the publication."""


class UncertainPublicationFailure(RuntimeError):
    """Telegram acceptance cannot be determined; retry is unsafe."""


def catalog_deep_link(bot_username: str) -> str:
    normalized = bot_username.removeprefix("@")
    if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", normalized):
        raise ValueError("invalid Telegram bot username")
    return f"https://t.me/{normalized}?start=catalog"


def normalize_publication_button_url(value: str) -> str:
    """Accept copy-pasted HTTPS links or a public Telegram @username."""

    normalized = value.strip()
    if normalized.startswith("@"):
        username = normalized[1:]
        if not re.fullmatch(r"[A-Za-z0-9_]{5,32}", username):
            raise ValueError("Telegram @username must contain 5-32 letters, digits or underscores")
        normalized = f"https://t.me/{username}"
    elif normalized.casefold().startswith(("t.me/", "www.t.me/", "telegram.me/")):
        normalized = f"https://{normalized}"
    return validate_safe_https_url(normalized)


def build_keyboard_snapshot(
    buttons: list[NavigationButtonData], *, bot_username: str | None = None
) -> list[list[dict[str, str]]]:
    rows: dict[int, list[NavigationButtonData]] = {}
    for button in sorted(
        (button for button in buttons if button.enabled), key=lambda value: (value.row, value.order)
    ):
        rows.setdefault(button.row, []).append(button)
    snapshot: list[list[dict[str, str]]] = []
    for row in sorted(rows):
        rendered: list[dict[str, str]] = []
        for button in rows[row]:
            if button.action is ButtonAction.CATALOG:
                if bot_username is None:
                    raise ValueError("bot username is required for catalog buttons")
                url = catalog_deep_link(bot_username)
            else:
                assert button.url is not None
                url = button.url
            rendered.append({"text": button.text, "url": url})
        snapshot.append(rendered)
    return snapshot


def _safe_https_url(value: object) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        return validate_safe_https_url(value.strip())
    except ValueError:
        return None


def _contact_draft_url(contact_url: str, draft: str) -> str:
    """Open a manager chat with a product-specific draft; never send it automatically."""
    parts = urlsplit(validate_safe_https_url(contact_url))
    host = (parts.hostname or "").casefold()
    path = parts.path.strip("/")
    query = dict(parse_qsl(parts.query, keep_blank_values=True))
    telegram = host in {"t.me", "www.t.me", "telegram.me"}
    whatsapp = (
        host == "wa.me" and re.fullmatch(r"[0-9]{5,20}", path)
    ) or (
        host == "api.whatsapp.com" and path == "send"
        and re.fullmatch(r"[0-9]{5,20}", query.get("phone", ""))
    )
    if telegram:
        reserved = {"share", "joinchat", "addstickers", "addlist", "proxy", "socks", "c", "s", "m"}
        valid_peer = re.fullmatch(r"[A-Za-z0-9_]{5,32}|\+[0-9]{5,20}", path)
        if (
            not valid_peer or path.casefold() in reserved
            or any(key != "text" for key in query)
        ):
            raise ValueError("Нужна ссылка на личный чат Telegram без start/profile или WhatsApp")
    elif not whatsapp:
        raise ValueError("Укажите WhatsApp wa.me/номер или Telegram @username менеджера")
    query["text"] = draft
    return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))


def price_inquiry_url(contact_url: str, emoda_code: str) -> str:
    return _contact_draft_url(contact_url, (
        f"Здравствуйте! Уточните цену товара E-MODA {emoda_code}.\n"
        f"Hello! Please check the price of E-MODA {emoda_code}."
    ))


def order_request_url(contact_url: str, emoda_code: str) -> str:
    return _contact_draft_url(contact_url, (
        f"Здравствуйте! Хочу заказать товар E-MODA {emoda_code}.\n"
        f"Hello! I would like to order E-MODA {emoda_code}."
    ))


def publication_keyboard(
    setting_value: dict[str, Any] | None, *, bot_username: str | None,
    emoda_code: str = "EM-TEST",
) -> list[list[dict[str, str]]]:
    """The one global keyboard attached under every publication.

    Three separate rows: order and price drafts carrying the code, and catalog.
    URLs and texts come from the
    ``publication_buttons`` global setting managed in ``/admin → Кнопки``.
    """
    stored: dict[str, Any] = setting_value if isinstance(setting_value, dict) else {}
    order_raw = stored.get("order")
    catalog_raw = stored.get("catalog")
    price_raw = stored.get("check_price")
    order_stored: dict[str, Any] = order_raw if isinstance(order_raw, dict) else {}
    catalog_stored: dict[str, Any] = catalog_raw if isinstance(catalog_raw, dict) else {}
    price_stored: dict[str, Any] = price_raw if isinstance(price_raw, dict) else {}
    order_text = str(order_stored.get("text") or DEFAULT_PUBLICATION_BUTTONS["order"]["text"])
    catalog_text = str(catalog_stored.get("text") or DEFAULT_PUBLICATION_BUTTONS["catalog"]["text"])
    order_url = _safe_https_url(order_stored.get("url")) or _safe_https_url(
        DEFAULT_PUBLICATION_BUTTONS["order"]["url"]
    )
    if order_url is None:
        raise ValueError("order button URL is not configured")
    catalog_url = _safe_https_url(catalog_stored.get("url"))
    if catalog_url is None:
        if bot_username is None:
            raise ValueError("bot username is required for catalog buttons")
        catalog_url = catalog_deep_link(bot_username)
    price_text = str(price_stored.get("text") or DEFAULT_PUBLICATION_BUTTONS["check_price"]["text"])
    price_url = None
    for contact in (price_stored.get("url"), order_url):
        if isinstance(contact, str):
            try:
                price_url = price_inquiry_url(contact, emoda_code)
                break
            except ValueError:
                continue
    if price_url is None:
        # A custom order URL must never route inquiries to an unrelated default
        # phone or block publication. Admin can configure a compatible contact.
        price_url = order_url
    try:
        order_draft_url = order_request_url(order_url, emoda_code)
    except ValueError:
        # Preserve legacy non-contact URLs; never guess a different recipient.
        order_draft_url = order_url
    return [
        [{"text": order_text, "url": order_draft_url}],
        [{"text": catalog_text, "url": catalog_url}],
        [{"text": price_text, "url": price_url}],
    ]


def expected_message_count(payload: dict[str, object]) -> int:
    stored = payload.get("expected_message_count")
    if isinstance(stored, int) and stored > 0:
        return stored
    text = str(payload.get("text", ""))
    media = payload.get("media_paths", [])
    media_count = len(media) if isinstance(media, list) else 0
    chunks = telegram_chunks(text)
    if media_count == 0:
        return len(chunks)
    if media_count == 1:
        return 1 if len(text) <= 1024 else 1 + len(chunks)
    return media_count + len(chunks)


class PublicationService:
    def __init__(self, publisher: Publisher) -> None:
        self.publisher = publisher

    async def create(
        self,
        session: AsyncSession,
        *,
        product: Product,
        destination: Destination,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> PublicationOutbox:
        snapshot_payload = copy.deepcopy(payload)
        snapshot_payload["expected_message_count"] = expected_message_count(snapshot_payload)
        snapshot_keyboard = copy.deepcopy(keyboard)
        serialized = json.dumps(
            {"payload": snapshot_payload, "keyboard": snapshot_keyboard},
            sort_keys=True,
            ensure_ascii=False,
            default=str,
            separators=(",", ":"),
        )
        payload_hash = hashlib.sha256(serialized.encode()).hexdigest()
        existing = await session.scalar(
            select(PublicationOutbox).where(
                PublicationOutbox.product_id == product.id,
                PublicationOutbox.destination_id == destination.id,
                PublicationOutbox.payload_hash == payload_hash,
            )
        )
        if existing is not None:
            return existing
        if session.bind and session.bind.dialect.name == "postgresql":
            inserted_id = await session.scalar(
                insert(PublicationOutbox)
                .values(
                    product_id=product.id,
                    destination_id=destination.id,
                    payload=snapshot_payload,
                    payload_hash=payload_hash,
                    keyboard_snapshot=snapshot_keyboard,
                    status=OutboxStatus.PENDING,
                )
                .on_conflict_do_nothing(
                    index_elements=["product_id", "destination_id", "payload_hash"]
                )
                .returning(PublicationOutbox.id)
            )
            if inserted_id is not None:
                inserted = await session.get(PublicationOutbox, inserted_id)
                assert inserted is not None
                await session.commit()
                return inserted
            concurrent = await session.scalar(
                select(PublicationOutbox).where(
                    PublicationOutbox.product_id == product.id,
                    PublicationOutbox.destination_id == destination.id,
                    PublicationOutbox.payload_hash == payload_hash,
                )
            )
            assert concurrent is not None
            return concurrent
        outbox = PublicationOutbox(
            product_id=product.id,
            destination_id=destination.id,
            payload=snapshot_payload,
            payload_hash=payload_hash,
            keyboard_snapshot=snapshot_keyboard,
            status=OutboxStatus.PENDING,
        )
        session.add(outbox)
        await session.commit()
        return outbox

    async def _destination_accessible(
        self, session: AsyncSession, destination: Destination,
        product: Product, outbox: PublicationOutbox,
    ) -> bool:
        checker = getattr(self.publisher, "validate_destination", None)
        if checker is None:
            return True
        try:
            await checker(destination.telegram_channel_id)
        except DestinationAccessError as exc:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = str(exc)[:2000]
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = outbox.last_error
            product.review_notified_at = None
            set_product_review_reason(
                product, ReviewReasonCode.DESTINATION_MISSING, detail=outbox.last_error
            )
            await set_product_message_outcome(session, product, "needs_review", outbox.last_error)
            await session.commit()
            return False
        return True

    async def publish(self, session: AsyncSession, outbox_id: int) -> PublicationOutbox:
        outbox = await session.get(PublicationOutbox, outbox_id, with_for_update=True)
        if outbox is None:
            raise LookupError("outbox not found")
        if outbox.status is OutboxStatus.PUBLISHED:
            return outbox
        if outbox.status is OutboxStatus.FAILED and not outbox.telegram_message_ids:
            # FAILED is only written before Telegram accepted anything, so an explicit retry
            # of the same payload re-arms the row instead of wedging on the stale attempt.
            outbox.status = OutboxStatus.PENDING
            outbox.last_error = None
        if outbox.status is not OutboxStatus.PENDING:
            raise UncertainPublicationFailure(f"outbox is {outbox.status}")
        destination = await session.get(Destination, outbox.destination_id)
        product = await session.get(Product, outbox.product_id, with_for_update=True)
        if destination is None or product is None:
            raise SafePublicationFailure("publication references missing records")
        if product.status is not ProductStatus.READY_TO_PUBLISH:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = f"publication superseded by product state {product.status.value}"
            await set_product_message_outcome(
                session, product, product.status.value, outbox.last_error
            )
            await session.commit()
            return outbox
        if not destination.enabled:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = "destination is disabled"
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = "publication failed: destination is disabled"
            set_product_review_reason(
                product, ReviewReasonCode.DESTINATION_MISSING, detail=product.processing_error
            )
            await set_product_message_outcome(
                session, product, "needs_review", product.processing_error
            )
            await session.commit()
            return outbox
        if not await self._destination_accessible(session, destination, product, outbox):
            return outbox
        outbox.status = OutboxStatus.PUBLISHING
        outbox.publishing_started_at = datetime.now(UTC)
        outbox.attempts += 1
        product.status = ProductStatus.PUBLISHING
        attempt = PublicationAttempt(
            outbox_id=outbox.id, attempt_no=outbox.attempts, started_at=datetime.now(UTC)
        )
        session.add(attempt)
        await session.commit()  # durable intent before Telegram I/O
        try:
            message_ids = await self.publisher.send(
                destination.telegram_channel_id, outbox.payload, outbox.keyboard_snapshot
            )
        except SafePublicationFailure as exc:
            outbox.status = OutboxStatus.FAILED
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = f"publication failed: {exc}"[:2000]
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_FAILED, detail=product.processing_error
            )
            product.review_notified_at = None
            outbox.last_error = str(exc)
            attempt.outcome = "failed_before_acceptance"
            attempt.error = str(exc)
            attempt.finished_at = datetime.now(UTC)
            await set_product_message_outcome(
                session, product, "needs_review", product.processing_error
            )
            await session.commit()
            return outbox
        except Exception as exc:
            outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
            product.status = ProductStatus.RECONCILIATION_REQUIRED
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_RECONCILIATION, detail=str(exc)
            )
            outbox.last_error = str(exc)
            attempt.outcome = "acceptance_uncertain"
            attempt.error = str(exc)
            attempt.finished_at = datetime.now(UTC)
            await set_product_message_outcome(session, product, "reconciliation_required", str(exc))
            await session.commit()
            return outbox
        await session.refresh(product, attribute_names=["status", "processing_error"])
        outbox.telegram_message_ids = message_ids
        outbox.status = OutboxStatus.PUBLISHED
        outbox.published_at = datetime.now(UTC)
        if product.status is ProductStatus.PUBLISHING:
            product.status = (
                ProductStatus.READY if product.processing_error else ProductStatus.PUBLISHED
            )
        product.published_at = outbox.published_at
        await set_product_message_outcome(session, product, "published")
        attempt.outcome = "published"
        attempt.finished_at = datetime.now(UTC)
        await session.commit()
        return outbox

    async def replace(
        self,
        session: AsyncSession,
        outbox_id: int,
        original_outbox_id: int,
    ) -> PublicationOutbox:
        """Replace a known partial publication without blindly duplicating it.

        Removing the exact stored Telegram IDs is idempotent and happens before
        the new send.  The send itself retains the normal durable intent and
        reconciliation rules, so an uncertain Telegram acceptance is never retried
        automatically.
        """

        outbox = await session.get(PublicationOutbox, outbox_id, with_for_update=True)
        original = await session.get(PublicationOutbox, original_outbox_id, with_for_update=True)
        if outbox is None or original is None:
            raise SafePublicationFailure("replacement references missing outbox records")
        if outbox.status is OutboxStatus.PUBLISHED:
            return outbox
        if outbox.status is OutboxStatus.FAILED and not outbox.telegram_message_ids:
            outbox.status = OutboxStatus.PENDING
            outbox.last_error = None
        if outbox.status is not OutboxStatus.PENDING:
            raise UncertainPublicationFailure(f"replacement outbox is {outbox.status}")
        destination = await session.get(Destination, outbox.destination_id)
        product = await session.get(Product, outbox.product_id, with_for_update=True)
        if destination is None or product is None:
            raise SafePublicationFailure("replacement references missing records")
        if product.status is not ProductStatus.READY_TO_PUBLISH:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = f"replacement superseded by product state {product.status.value}"
            await set_product_message_outcome(
                session, product, product.status.value, outbox.last_error
            )
            await session.commit()
            return outbox
        if not destination.enabled:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = "destination is disabled"
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = "publication replacement failed: destination is disabled"
            set_product_review_reason(
                product, ReviewReasonCode.DESTINATION_MISSING, detail=product.processing_error
            )
            await set_product_message_outcome(
                session, product, "needs_review", product.processing_error
            )
            await session.commit()
            return outbox
        if not await self._destination_accessible(session, destination, product, outbox):
            return outbox
        if not original.telegram_message_ids:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = "partial publication has no exact Telegram message IDs"
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = f"publication replacement failed: {outbox.last_error}"
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_FAILED, detail=product.processing_error
            )
            product.review_notified_at = None
            await set_product_message_outcome(
                session, product, "needs_review", product.processing_error
            )
            await session.commit()
            return outbox

        outbox.status = OutboxStatus.PUBLISHING
        outbox.publishing_started_at = datetime.now(UTC)
        outbox.attempts += 1
        product.status = ProductStatus.PUBLISHING
        attempt = PublicationAttempt(
            outbox_id=outbox.id,
            attempt_no=outbox.attempts,
            started_at=datetime.now(UTC),
        )
        session.add(attempt)
        await session.commit()

        try:
            await self.publisher.delete(
                destination.telegram_channel_id,
                [int(message_id) for message_id in original.telegram_message_ids],
            )
        except Exception as exc:
            # Exact-message deletion is idempotent: no new publication was sent,
            # so an explicit approval may safely retry this cleanup.
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = f"partial publication cleanup failed: {exc}"[:2000]
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = f"publication replacement failed: {exc}"[:2000]
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_FAILED, detail=product.processing_error
            )
            product.review_notified_at = None
            attempt.outcome = "replacement_cleanup_failed"
            attempt.error = str(exc)[:2000]
            attempt.finished_at = datetime.now(UTC)
            await set_product_message_outcome(
                session, product, "needs_review", product.processing_error
            )
            await session.commit()
            return outbox

        try:
            message_ids = await self.publisher.send(
                destination.telegram_channel_id, outbox.payload, outbox.keyboard_snapshot
            )
        except SafePublicationFailure as exc:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = str(exc)
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = f"publication replacement failed: {exc}"[:2000]
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_FAILED, detail=product.processing_error
            )
            product.review_notified_at = None
            attempt.outcome = "failed_before_acceptance"
            attempt.error = str(exc)
            attempt.finished_at = datetime.now(UTC)
            await set_product_message_outcome(
                session, product, "needs_review", product.processing_error
            )
            await session.commit()
            return outbox
        except Exception as exc:
            outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
            outbox.last_error = str(exc)
            product.status = ProductStatus.RECONCILIATION_REQUIRED
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_RECONCILIATION, detail=str(exc)
            )
            attempt.outcome = "acceptance_uncertain"
            attempt.error = str(exc)
            attempt.finished_at = datetime.now(UTC)
            await set_product_message_outcome(session, product, "reconciliation_required", str(exc))
            await session.commit()
            return outbox

        await session.refresh(product, attribute_names=["status", "processing_error"])
        outbox.telegram_message_ids = message_ids
        outbox.status = OutboxStatus.PUBLISHED
        outbox.published_at = datetime.now(UTC)
        if product.status is ProductStatus.PUBLISHING:
            product.status = (
                ProductStatus.READY if product.processing_error else ProductStatus.PUBLISHED
            )
        product.published_at = outbox.published_at
        await set_product_message_outcome(session, product, "published")
        attempt.outcome = "published_replacement"
        attempt.finished_at = datetime.now(UTC)
        await session.commit()
        return outbox

    async def reconcile(self, session: AsyncSession) -> None:
        rows = await session.scalars(
            select(PublicationOutbox)
            .where(
                PublicationOutbox.status.in_(
                    [OutboxStatus.PUBLISHING, OutboxStatus.RECONCILIATION_REQUIRED]
                )
            )
            .order_by(PublicationOutbox.created_at)
            .with_for_update(skip_locked=True)
        )
        for outbox in rows:
            destination = await session.get(Destination, outbox.destination_id)
            product = await session.get(Product, outbox.product_id)
            if destination is None or product is None:
                outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
                continue
            if (
                outbox.payload.get("operation") == "edit"
                or outbox.payload.get("publication_format", "standard") != "standard"
            ):
                outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
                product.status = ProductStatus.RECONCILIATION_REQUIRED
                outbox.last_error = (
                    "publication cannot be reconciled by code alone; manual verification required"
                )
                set_product_review_reason(
                    product,
                    ReviewReasonCode.PUBLICATION_RECONCILIATION,
                    detail=outbox.last_error,
                )
                await set_product_message_outcome(
                    session, product, "reconciliation_required", outbox.last_error
                )
                continue
            code = str(outbox.payload.get("emoda_code", product.emoda_code))
            try:
                found = await self.publisher.find_by_code(destination.telegram_channel_id, code)
            except Exception as exc:
                outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
                product.status = ProductStatus.RECONCILIATION_REQUIRED
                outbox.last_error = f"history reconciliation failed: {exc}"[:2000]
                set_product_review_reason(
                    product,
                    ReviewReasonCode.PUBLICATION_RECONCILIATION,
                    detail=outbox.last_error,
                )
                await set_product_message_outcome(
                    session, product, "reconciliation_required", outbox.last_error
                )
                continue
            if found:
                if len(found) != 1:
                    outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
                    product.status = ProductStatus.RECONCILIATION_REQUIRED
                    outbox.last_error = "multiple posts with this E-MODA code found; automatic association is unsafe"
                    set_product_review_reason(
                        product,
                        ReviewReasonCode.PUBLICATION_RECONCILIATION,
                        detail=outbox.last_error,
                    )
                    await set_product_message_outcome(
                        session, product, "reconciliation_required", outbox.last_error
                    )
                    continue
                outbox.telegram_message_ids = found
                outbox.status = OutboxStatus.PUBLISHED
                outbox.published_at = datetime.now(UTC)
                outbox.last_error = (
                    "multipart publication reconciled by E-MODA code; "
                    "full Telegram message IDs unavailable, automatic edits disabled"
                    if expected_message_count(outbox.payload) > 1
                    else None
                )
                if product.status in {
                    ProductStatus.PUBLISHING,
                    ProductStatus.RECONCILIATION_REQUIRED,
                }:
                    product.status = (
                        ProductStatus.READY if product.processing_error else ProductStatus.PUBLISHED
                    )
                product.published_at = outbox.published_at
                await set_product_message_outcome(session, product, "published")
            else:
                outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
                product.status = ProductStatus.RECONCILIATION_REQUIRED
                outbox.last_error = (
                    "destination history did not confirm publication; blind retry disabled"
                )
                set_product_review_reason(
                    product,
                    ReviewReasonCode.PUBLICATION_RECONCILIATION,
                    detail=outbox.last_error,
                )
                await set_product_message_outcome(
                    session, product, "reconciliation_required", outbox.last_error
                )
        await session.commit()

    async def update(
        self, session: AsyncSession, outbox_id: int, original_outbox_id: int
    ) -> PublicationOutbox:
        outbox = await session.get(PublicationOutbox, outbox_id, with_for_update=True)
        original = await session.get(PublicationOutbox, original_outbox_id)
        if outbox is None or original is None:
            raise LookupError("outbox not found")
        if outbox.status is OutboxStatus.PUBLISHED:
            return outbox
        if (
            outbox.status is OutboxStatus.FAILED
            and not outbox.telegram_message_ids
            and original.status is OutboxStatus.PUBLISHED
        ):
            # A rejected edit never reached Telegram, so a repeated update request re-arms the
            # existing row: the retry stays one outbox row and one destination publication.
            outbox.status = OutboxStatus.PENDING
            outbox.last_error = None
        if (
            outbox.status is not OutboxStatus.PENDING
            or original.status is not OutboxStatus.PUBLISHED
        ):
            raise UncertainPublicationFailure("publication update state is unsafe")
        destination = await session.get(Destination, outbox.destination_id)
        product = await session.get(Product, outbox.product_id, with_for_update=True)
        if destination is None or product is None or not original.telegram_message_ids:
            raise SafePublicationFailure("publication update references missing records")
        if product.status is not ProductStatus.READY_TO_PUBLISH:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = f"publication update superseded by state {product.status.value}"
            await session.commit()
            return outbox
        if not destination.enabled:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = "destination is disabled"
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = "publication update failed: destination is disabled"
            set_product_review_reason(
                product, ReviewReasonCode.DESTINATION_MISSING, detail=product.processing_error
            )
            product.review_notified_at = None
            await session.commit()
            return outbox
        if not await self._destination_accessible(session, destination, product, outbox):
            return outbox
        if len(original.telegram_message_ids) != expected_message_count(original.payload):
            raise SafePublicationFailure(
                "publication was reconciled without all multipart message IDs; "
                "automatic update requires manual review"
            )
        outbox.status = OutboxStatus.PUBLISHING
        outbox.publishing_started_at = datetime.now(UTC)
        outbox.attempts += 1
        product.status = ProductStatus.PUBLISHING
        attempt = PublicationAttempt(
            outbox_id=outbox.id, attempt_no=outbox.attempts, started_at=datetime.now(UTC)
        )
        session.add(attempt)
        await session.commit()
        try:
            message_ids = await self.publisher.update(
                destination.telegram_channel_id,
                original.telegram_message_ids,
                original.payload,
                outbox.payload,
                outbox.keyboard_snapshot,
            )
        except SafePublicationFailure as exc:
            outbox.status = OutboxStatus.FAILED
            outbox.last_error = str(exc)
            product.status = ProductStatus.NEEDS_REVIEW
            product.processing_error = f"publication update failed: {exc}"[:2000]
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_FAILED, detail=product.processing_error
            )
            product.review_notified_at = None
            attempt.outcome = "update_rejected"
            attempt.error = str(exc)
            attempt.finished_at = datetime.now(UTC)
            await set_product_message_outcome(
                session, product, "needs_review", product.processing_error
            )
            await session.commit()
            return outbox
        except Exception as exc:
            outbox.status = OutboxStatus.RECONCILIATION_REQUIRED
            outbox.last_error = str(exc)
            product.status = ProductStatus.RECONCILIATION_REQUIRED
            set_product_review_reason(
                product, ReviewReasonCode.PUBLICATION_RECONCILIATION, detail=str(exc)
            )
            attempt.outcome = "update_uncertain"
            attempt.error = str(exc)
            attempt.finished_at = datetime.now(UTC)
            await set_product_message_outcome(session, product, "reconciliation_required", str(exc))
            await session.commit()
            return outbox
        await session.refresh(product, attribute_names=["status", "processing_error"])
        outbox.telegram_message_ids = message_ids
        outbox.status = OutboxStatus.PUBLISHED
        outbox.published_at = datetime.now(UTC)
        if product.status is ProductStatus.PUBLISHING:
            product.status = (
                ProductStatus.READY if product.processing_error else ProductStatus.PUBLISHED
            )
        product.published_at = outbox.published_at
        attempt.outcome = "updated"
        attempt.finished_at = datetime.now(UTC)
        await set_product_message_outcome(session, product, "published")
        await session.commit()
        return outbox
