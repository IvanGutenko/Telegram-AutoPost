from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import re
import tempfile
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import exists, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, PhoneNotOccupiedError, RPCError
from telethon.tl.functions.contacts import ResolvePhoneRequest
from telethon.tl.functions.messages import SendMessageRequest
from telethon.tl.types import MessageMediaWebPage

from emoda.db.models import (
    AlbumGroup,
    CurrencyAlias,
    MediaItem,
    Product,
    Source,
    SourceMessage,
)
from emoda.db.intake import lock_source, transient_database_error
from emoda.db.repositories import MessageRepository, SourceRepository
from emoda.domain.enums import (
    AlbumStatus,
    EditPolicy,
    EventOrigin,
    FieldStatus,
    ProductStatus,
    ReviewReasonCode,
)
from emoda.domain.schemas import SourceIgnoreRule, SourceProfileData
from emoda.integrations.storage import MediaStorage
from emoda.rules.currency import CurrencyAliasResolver
from emoda.rules.engine import (
    RuleEngine,
    advertisement_reason,
    match_source_ignore_rule,
    match_structural_ignore_rule,
)
from emoda.services.albums import AlbumCoordinator
from emoda.services.collection import CollectionAction, CollectionEngine, CollectionMessage
from emoda.services.outcomes import set_duplicate_outcome_once, set_message_outcome
from emoda.services.review import set_product_review_reason

logger = logging.getLogger(__name__)

SYNTHETIC_COLLECTION_GROUP_BASE = -(2**62)


class ReferenceMediaPaths(list[str]):
    """Keep Telegram album identity alongside the backward-compatible path list."""
    def __init__(self, paths: list[str], grouped_id: int | None):
        super().__init__(paths)
        self.grouped_id = grouped_id


def telegram_product_media_type(message: Any) -> str | None:
    media = getattr(message, "media", None)
    if media is None or isinstance(media, MessageMediaWebPage):
        return None
    if getattr(message, "video", None):
        return "video"
    if getattr(message, "document", None):
        return "document"
    return "photo"


def normalize_merge_caption(value: str) -> str:
    return " ".join(value.split()).casefold()


def edit_reprocess_decision(
    *, published: bool, policy: EditPolicy
) -> tuple[ProductStatus | None, bool]:
    """Decide how a supplier edit re-enters the durable pipeline.

    A late caption is content, not a systemic failure, so the default policy
    reprocesses and lets the publication update itself in place. Whether the
    live post can actually absorb the change is decided by the publisher, which
    escalates to review only when Telegram cannot perform the update.
    """
    if policy is EditPolicy.IGNORE:
        return None, False
    if policy is EditPolicy.AUTO:
        return ProductStatus.READY, False
    if policy is EditPolicy.AUTO_BEFORE_PUBLICATION:
        return ProductStatus.READY, published
    return ProductStatus.READY, True


class CollectorService:
    def __init__(
        self,
        client: TelegramClient,
        sessions: async_sessionmaker[AsyncSession],
        storage: MediaStorage,
        *,
        edit_rescan_limit: int = 500,
    ) -> None:
        self.client = client
        self.sessions = sessions
        self.storage = storage
        self.edit_rescan_limit = edit_rescan_limit
        self._media_download_slots = asyncio.Semaphore(6)
        self._intake_slots = asyncio.Semaphore(4)

    def register(self) -> None:
        self.client.add_event_handler(self._new_message, events.NewMessage())
        self.client.add_event_handler(self._new_album, events.Album())
        self.client.add_event_handler(self._edited_message, events.MessageEdited())

    async def send_admin_text(self, channel_id: int, text: str, random_id: int) -> list[int]:
        peer = await self.client.get_input_entity(channel_id)
        result = await self.client(SendMessageRequest(peer=peer, message=text,
            random_id=random_id, no_webpage=True))
        direct_id = getattr(result, "id", None)
        if direct_id is not None:
            return [int(direct_id)]
        ids = [int(update.message.id) for update in getattr(result, "updates", [])
               if getattr(update, "message", None) is not None and hasattr(update.message, "id")]
        if not ids:
            raise RuntimeError("Telegram не вернул ID сообщения; отправка требует проверки")
        return ids

    async def validate_order_button_url(self, url: str) -> None:
        """Verify direct Telegram contacts with the authorized user session."""

        parsed = urlparse(url)
        if (parsed.hostname or "").casefold() not in {"t.me", "www.t.me", "telegram.me"}:
            return
        target = parsed.path.strip("/").split("/", 1)[0]
        if target.startswith("+") and target[1:].isdigit():
            try:
                result = await self.client(ResolvePhoneRequest(target[1:]))
            except PhoneNotOccupiedError as exc:
                raise ValueError(
                    "Telegram не находит аккаунт по этому номеру; укажите публичный @username"
                ) from exc
            if not getattr(result, "users", None):
                raise ValueError(
                    "Telegram не находит аккаунт по этому номеру; укажите публичный @username"
                )
            return
        if re.fullmatch(r"[A-Za-z0-9_]{5,32}", target):
            try:
                await self.client.get_entity(f"@{target}")
            except (RPCError, ValueError) as exc:
                raise ValueError(
                    "Telegram не находит этот @username; проверьте имя пользователя"
                ) from exc

    async def _new_message(self, event: Any) -> None:
        # Grouped transport updates are handled once by Telethon's Album builder.
        # Processing every NewMessage member would hydrate the same album N times.
        if getattr(event.message, "grouped_id", None) is not None:
            return
        await self.handle_event(event, edited=False)

    async def _new_album(self, event: Any) -> None:
        messages = list(getattr(event, "messages", []))
        if not messages:
            return
        anchor = max(messages, key=lambda item: int(item.id))
        album_event = type(
            "TelegramAlbumEvent",
            (),
            {"chat_id": event.chat_id, "message": anchor, "messages": messages},
        )()
        await self.handle_event(album_event, edited=False)

    async def _edited_message(self, event: Any) -> None:
        await self.handle_event(event, edited=True)

    async def handle_event(
        self,
        event: Any,
        *,
        edited: bool,
        origin: EventOrigin = EventOrigin.LIVE,
    ) -> None:
        message = getattr(event, "message", None)
        messages = [message] if message is not None else []
        hydrate = getattr(message, "grouped_id", None) is not None
        if edited and hydrate:
            async with self.sessions() as session:
                known = await session.scalar(select(SourceMessage).join(Source,
                    Source.id == SourceMessage.source_id).where(
                    Source.telegram_channel_id == event.chat_id,
                    SourceMessage.telegram_message_id == message.id))
                hydrate = known is None or bool(known.raw_metadata.get("intake_retry_pending"))
        if hydrate:
            try:
                messages = await self._hydrate_album_messages(event)
            except (RPCError, OSError, TimeoutError):
                members = list(getattr(event, "messages", [])) or [message]
                for member in members:
                    failed_event = type("PendingAlbumEvent", (), {"chat_id":event.chat_id,"message":member})()
                    await self._persist_failure(failed_event, edited=edited,
                        error=OSError("album hydration unavailable; retry required"), origin=origin,
                        retryable=True)
                return False
        pending_downloads: list[tuple[Any, int, int]] = []
        for item in messages:
            item_event = type(
                "HydratedTelegramEvent",
                (),
                {"chat_id": event.chat_id, "message": item,
                 "transport_message_count": len(messages),
                 "transport_media_count": sum(telegram_product_media_type(part) is not None for part in messages)},
            )()
            pending = await self._persist_event_safely(
                item_event, edited=(edited if item is message else bool(getattr(item,"edit_date",None))),
                origin=origin, defer_media_download=True
            )
            if pending is False:
                return False
            if pending is not None:
                pending_downloads.append((item, *pending))
        # Persist all album members with media_pending before slow network I/O.
        # The durable pending flags prevent the processor claiming a partial album.
        for item, row_id, source_id in pending_downloads:
            await self._download_media(item, row_id, source_id)
        return True

    async def _hydrate_album_messages(self, event: Any) -> list[Any]:
        """Read a Telegram album as one transport unit before its debounce starts.

        Telegram can deliver only the highest message IDs through the live update
        stream while lower members become visible to a later history scan.  Fetching
        the bounded Bot-API album range immediately prevents publication of that
        partial live subset.  The database upsert keeps concurrent live events and
        periodic catch-up idempotent.
        """

        message = event.message
        message_id = int(message.id)
        grouped_id = int(message.grouped_id)
        known = {
            int(item.id): item for item in getattr(event, "messages", [])
            if getattr(item, "grouped_id", None) == grouped_id
        }
        known[message_id] = message
        nearby_ids = list(range(max(1, message_id - 9), message_id + 10))
        try:
            nearby = await self.client.get_messages(event.chat_id, ids=nearby_ids)
        except Exception:
            logger.warning(
                "live album hydration failed; durable catch-up remains enabled",
                exc_info=True,
                extra={
                    "source_id": None,
                    "telegram_message_id": message_id,
                    "grouped_id": grouped_id,
                    "telegram_channel_id": event.chat_id,
                },
            )
            raise
        values = nearby if isinstance(nearby, (list, tuple)) else [nearby]
        hydrated = {
            int(item.id): item
            for item in values
            if item is not None and getattr(item, "grouped_id", None) == grouped_id
        }
        hydrated.update(known)
        return [hydrated[item_id] for item_id in sorted(hydrated)]

    async def _persist_event_safely(
        self,
        event: Any,
        *,
        edited: bool,
        origin: EventOrigin,
        defer_media_download: bool = False,
    ) -> tuple[int, int] | None:
        async with self._intake_slots:
            for attempt in range(4):
                try:
                    return await self.persist_event(event, edited=edited, origin=origin,
                        defer_media_download=defer_media_download)
                except Exception as exc:
                    retryable = transient_database_error(exc)
                    if retryable and attempt < 3:
                        await asyncio.sleep(0.2 * 2**attempt)
                        continue
                    logger.exception("supplier update persistence failed", extra={
                        "telegram_message_id": getattr(event.message, "id", None),
                        "telegram_channel_id": getattr(event, "chat_id", None)})
                    await self._persist_failure(event, edited=edited, error=exc, origin=origin,
                        retryable=retryable)
                    return False if retryable else None
        return None

    async def _source_for_update(
        self, session: AsyncSession, channel_id: int, telegram_message_id: int
    ) -> Source | None:
        """Resolve the source allowed to accept this Telegram update.

        Disabling a source stops the intake of NEW products; it must not silently
        strand a publication the system already tracks, so updates of messages
        already persisted for that source are still accepted. An archived source
        is closed for both.
        """
        source = await SourceRepository(session).by_channel(channel_id, enabled_only=False)
        if source is None or source.archived_at is not None:
            return None
        await lock_source(session, source.id)
        await session.refresh(source)
        if source.enabled:
            return source
        known = await session.scalar(
            select(
                exists().where(
                    SourceMessage.source_id == source.id,
                    SourceMessage.telegram_message_id == telegram_message_id,
                )
            )
        )
        if not known:
            logger.info(
                "supplier update ignored: source intake is disabled",
                extra={
                    "source_id": source.id,
                    "telegram_message_id": telegram_message_id,
                    "grouped_id": None,
                },
            )
            return None
        return source

    async def _persist_failure(
        self,
        event: Any,
        *,
        edited: bool,
        error: Exception,
        origin: EventOrigin,
        retryable: bool = False,
    ) -> None:
        channel_id = int(event.chat_id)
        message = event.message
        message_id = int(message.id)
        reason = f"{type(error).__name__}: {error}"[:500]
        failed_at = datetime.now(UTC)
        async with self.sessions() as session, session.begin():
            source = await self._source_for_update(session, channel_id, int(message.id))
            if source is None:
                return
            existing = await session.scalar(
                select(SourceMessage)
                .where(
                    SourceMessage.source_id == source.id,
                    SourceMessage.telegram_message_id == message_id,
                )
                .with_for_update()
            )
            if existing is not None:
                product_links = [Product.source_message_id == existing.id]
                if existing.album_group_id is not None:
                    product_links.append(Product.album_group_id == existing.album_group_id)
                has_product = await session.scalar(
                    select(
                        exists().where(
                            Product.source_id == source.id,
                            or_(*product_links),
                        )
                    )
                )
                if has_product or (not existing.raw_metadata.get("intake_retry_pending") and existing.raw_metadata.get("outcome") in {
                    "needs_review",
                    "ready_to_publish",
                    "published",
                    "skipped",
                    "failed",
                    "duplicate",
                }):
                    source.last_seen_message_id = max(
                        source.last_seen_message_id or 0, message_id
                    )
                    return
            row, _, _ = await MessageRepository(session).upsert(
                source_id=source.id,
                telegram_message_id=message_id,
                text=str(getattr(message, "message", "") or ""),
                caption=str(getattr(message, "message", "") or ""),
                telegram_date=self._utc(getattr(message, "date", None)),
                edit_date=self._utc(getattr(message, "edit_date", None)) if edited else None,
                grouped_id=int(message.grouped_id)
                if getattr(message, "grouped_id", None)
                else None,
                raw_metadata={
                    "post": bool(getattr(message, "post", False)),
                    "views": getattr(message, "views", None),
                },
            )
            row.event_origin = origin
            row.event_type = "edit" if edited else "new"
            set_message_outcome(row, "failed", reason)
            row.processing_started_at = None if retryable else failed_at
            row.processed_at = None if retryable else failed_at
            row.processing_after = None
            row.media_pending = retryable
            if retryable:
                attempts = int((row.raw_metadata or {}).get("intake_retry_attempts", 0)) + 1
                row.raw_metadata = {**row.raw_metadata, "intake_retry_pending": True,
                    "intake_retry_attempts": attempts,
                    "intake_retry_after": (failed_at + timedelta(seconds=min(3600, 30 * 2**min(attempts,7)))).isoformat()}
            source.last_seen_message_id = max(source.last_seen_message_id or 0, message_id)

    async def persist_event(
        self,
        event: Any,
        *,
        edited: bool,
        origin: EventOrigin = EventOrigin.LIVE,
        defer_media_download: bool = False,
    ) -> tuple[int, int] | None:
        channel_id = int(event.chat_id)
        message = event.message
        received_at = datetime.now(UTC)
        async with self.sessions() as session, session.begin():
            source = await self._source_for_update(session, channel_id, int(message.id))
            if source is None:
                return None
            profile = SourceProfileData.model_validate(source.profile.config)
            row, created, changed = await MessageRepository(session).upsert(
                source_id=source.id,
                telegram_message_id=int(message.id),
                text=str(getattr(message, "message", "") or ""),
                caption=str(getattr(message, "message", "") or ""),
                telegram_date=self._utc(getattr(message, "date", None)),
                edit_date=self._utc(getattr(message, "edit_date", None)) if edited else None,
                grouped_id=int(message.grouped_id)
                if getattr(message, "grouped_id", None)
                else None,
                raw_metadata={
                    "post": bool(getattr(message, "post", False)),
                    "views": getattr(message, "views", None),
                },
            )
            if row.raw_metadata.get("intake_retry_pending"):
                created = True  # existing durable receipt, not yet collected or processed
                row.processed_at = None
                row.processing_started_at = None
                row.media_pending = False
                row.raw_metadata = {k:v for k,v in row.raw_metadata.items() if not k.startswith("intake_retry_")}
            row.event_origin = origin
            row.event_type = "edit" if edited else "new"
            if created:
                set_message_outcome(row, "received")
            elif not changed:
                await set_duplicate_outcome_once(session, row)
            source.last_seen_message_id = max(source.last_seen_message_id or 0, int(message.id))
            ad_reason = advertisement_reason(row.text or row.caption, profile)
            if row.grouped_id is not None and ad_reason is None:
                sibling = await session.scalar(
                    select(SourceMessage).where(
                        SourceMessage.source_id == source.id,
                        SourceMessage.grouped_id == row.grouped_id,
                        SourceMessage.id != row.id,
                    )
                )
                if sibling is not None and str(
                    sibling.raw_metadata.get("ignored_reason", "")
                ).startswith("advertising:"):
                    ad_reason = str(sibling.raw_metadata["ignored_reason"]).split(":", 1)[1]
            if ad_reason is not None:
                ignored_at = datetime.now(UTC)
                related = [row]
                if row.grouped_id is not None:
                    related = list(
                        await session.scalars(
                            select(SourceMessage)
                            .where(
                                SourceMessage.source_id == source.id,
                                SourceMessage.grouped_id == row.grouped_id,
                            )
                            .with_for_update()
                        )
                    )
                group_ids: set[int] = set()
                for item in related:
                    item.raw_metadata = {
                        **item.raw_metadata,
                        "ignored_reason": f"advertising:{ad_reason}",
                    }
                    set_message_outcome(item, "skipped", f"advertising:{ad_reason}")
                    item.processing_started_at = ignored_at
                    item.processed_at = ignored_at
                    item.processing_after = None
                    item.media_pending = False
                    if item.album_group_id is not None:
                        group_ids.add(item.album_group_id)
                for group_id in group_ids:
                    group = await session.get(AlbumGroup, group_id, with_for_update=True)
                    if group is not None and group.product_id is None:
                        group.status = AlbumStatus.PROCESSED
                return None
            media_type = telegram_product_media_type(message)
            has_media = media_type is not None
            structural_ignore = match_structural_ignore_rule(
                profile.ignore_rules, grouped_id=row.grouped_id, media_type=media_type,
                media_count=getattr(event, "transport_media_count", int(has_media)),
                message_count=getattr(event, "transport_message_count", 1),
            )
            if structural_ignore is not None:
                reason = f"structural_ignore:{structural_ignore.name}"
                row.raw_metadata = {**row.raw_metadata, "ignored_reason": reason}
                set_message_outcome(row, "skipped", reason)
                row.processed_at = datetime.now(UTC)
                row.processing_after = None
                row.media_pending = False
                return None
            effective_edit = edited and changed and not created
            if effective_edit:
                await session.execute(
                    update(MediaItem)
                    .where(MediaItem.source_message_id == row.id, MediaItem.active.is_(True))
                    .values(active=False)
                )
            if has_media and (created or effective_edit):
                row.media_pending = True
            merge_window = profile.adjacent_media_merge_seconds
            text_merge_window = profile.adjacent_text_merge_seconds
            product_collection_window = (
                profile.product_collection_window_seconds
                if profile.product_collection_mode != "off"
                else Decimal("0")
            )
            configured_collection_window = (
                profile.collection.max_time_gap_seconds
                if profile.collection is not None
                else Decimal("0")
            )
            standalone_window = max(merge_window, text_merge_window)
            if created and has_media and row.grouped_id is None and standalone_window > 0:
                row.processing_after = received_at + timedelta(seconds=float(standalone_window))
            row_id = row.id
            source_id = source.id
            next_status: ProductStatus | None = None
            if effective_edit:
                product_links = [Product.source_message_id == row.id]
                if row.album_group_id is not None:
                    product_links.append(Product.album_group_id == row.album_group_id)
                product = await session.scalar(
                    select(Product).where(or_(*product_links)).with_for_update()
                )
                if product is not None:
                    next_status, force_review = edit_reprocess_decision(
                        published=bool(product.published_at), policy=profile.edit_policy
                    )
                    if next_status is None:
                        set_message_outcome(
                            row,
                            "deferred",
                            "supplier edit stored; source edit policy ignores reprocessing",
                        )
                    if next_status is not None:
                        product.status = next_status
                        product.processing_error = (
                            "supplier edit requires review after reprocess"
                            if force_review
                            else None
                        )
                        if force_review and product.processing_error:
                            set_product_review_reason(
                                product,
                                ReviewReasonCode.SOURCE_EDIT_REVIEW,
                                detail=product.processing_error,
                            )
            coordinator = AlbumCoordinator(
                max(
                    profile.album_debounce_seconds,
                    merge_window,
                    text_merge_window,
                    product_collection_window,
                    configured_collection_window,
                )
            )
            collected_multi_group = False
            if created and (
                profile.collection is not None or profile.product_collection_mode != "off"
            ):
                if profile.collection is not None:
                    configured_coordinator = AlbumCoordinator(
                        profile.effective_collection().debounce_seconds
                    )
                    collected_multi_group = await self._collect_configured_product(
                        session,
                        row,
                        has_media=has_media,
                        profile=profile,
                        coordinator=configured_coordinator,
                        now=received_at,
                    )
                else:
                    collected_multi_group = await self._collect_multi_group_product(
                        session,
                        row,
                        has_media=has_media,
                        profile=profile,
                        coordinator=coordinator,
                        now=received_at,
                    )
                if collected_multi_group:
                    if row.raw_metadata.get("outcome") != "skipped":
                        set_message_outcome(row, "collecting_album")
            if not collected_multi_group and row.grouped_id is not None and (
                created or (effective_edit and next_status is not None)
            ):
                # Editing a member of a merged product must not move that one
                # photo back into a new group keyed by its transport grouped_id.
                existing_group = await session.get(AlbumGroup, row.album_group_id,
                    with_for_update=True) if row.album_group_id is not None else None
                if existing_group is not None:
                    group = existing_group
                    await coordinator.attach_item(session, group, row, now=received_at)
                else:
                    group = await coordinator.add_item(session, row, now=received_at)
                set_message_outcome(row, "collecting_album")
                if created and merge_window > 0:
                    await self._attach_prior_standalone(
                        session,
                        group,
                        row,
                        coordinator,
                        window=timedelta(seconds=float(merge_window)),
                        now=received_at,
                    )
            elif not collected_multi_group and created and has_media and merge_window > 0:
                await self._attach_to_recent_album(
                    session,
                    row,
                    coordinator,
                    window=timedelta(seconds=float(merge_window)),
                    now=received_at,
                )
            elif (
                not collected_multi_group
                and created
                and not has_media
                and row.grouped_id is None
                and text_merge_window > 0
                and normalize_merge_caption(row.text or row.caption)
            ):
                attached = await self._attach_text_to_recent_album(
                    session,
                    row,
                    coordinator,
                    window=timedelta(seconds=float(text_merge_window)),
                    now=received_at,
                )
                if not attached and profile.ignore_rules:
                    await self._attach_text_to_pending_collection(
                        session,
                        row,
                        profile,
                        coordinator,
                        window=timedelta(seconds=float(text_merge_window)),
                        now=received_at,
                    )
            elif (
                not collected_multi_group
                and effective_edit
                and next_status is not None
                and row.album_group_id is not None
            ):
                # A single message also owns a collection group that is
                # PROCESSED once published; reopening it is what lets the
                # processor claim the product again, so an edit of a
                # non-album message is reprocessed exactly like an album
                # caption edit.
                group = await session.get(AlbumGroup, row.album_group_id, with_for_update=True)
                if group is not None:
                    await coordinator.reopen(session, group, now=received_at)
                    set_message_outcome(row, "collecting_album")
            needs_media = has_media and (created or effective_edit or row.media_pending)
        if needs_media:
            if defer_media_download:
                return row_id, source_id
            await self._download_media(message, row_id, source_id)
        logger.info(
            "supplier update persisted",
            extra={
                "source_id": source_id,
                "telegram_message_id": message.id,
                "grouped_id": getattr(message, "grouped_id", None),
            },
        )
        return None

    async def _collect_configured_product(
        self,
        session: AsyncSession,
        message: SourceMessage,
        *,
        has_media: bool,
        profile: SourceProfileData,
        coordinator: AlbumCoordinator,
        now: datetime,
    ) -> bool:
        collection = profile.effective_collection()
        engine = CollectionEngine(collection)
        incoming = CollectionMessage(
            message_id=message.telegram_message_id,
            occurred_at=self._utc(message.telegram_date),
            grouped_id=message.grouped_id,
            text=message.text,
            caption=message.caption,
            media_count=1 if has_media else 0,
        )
        empty_decision = engine.decide([], incoming)
        if empty_decision.action is CollectionAction.IGNORE:
            reason = empty_decision.rule_name or empty_decision.reason
            message.processing_started_at = now
            message.processed_at = now
            message.processing_after = None
            message.media_pending = False
            message.raw_metadata = {**message.raw_metadata, "ignored_reason": reason}
            set_message_outcome(message, "skipped", reason)
            return True

        # A late transport member still belongs to the original product, even
        # when that product merged several grouped_ids and has already closed.
        if message.grouped_id is not None:
            existing_group_ids = set(await session.scalars(
                select(SourceMessage.album_group_id).where(
                    SourceMessage.source_id == message.source_id,
                    SourceMessage.grouped_id == message.grouped_id,
                    SourceMessage.id != message.id,
                    SourceMessage.album_group_id.is_not(None),
                )
            ))
            if len(existing_group_ids) == 1:
                group = await session.get(AlbumGroup, existing_group_ids.pop(), with_for_update=True)
                if group is not None:
                    previous_deadline = group.ready_at
                    await coordinator.attach_item(session, group, message, now=now)
                    # Do not shorten a media-before-text collection's waiting window.
                    group.ready_at = max(self._utc(previous_deadline), self._utc(group.ready_at))
                    return True
            if len(existing_group_ids) > 1:
                raise ValueError('unsafe collection split: grouped_id belongs to multiple products')

        groups = list(
            await session.scalars(
                select(AlbumGroup)
                .where(
                    AlbumGroup.source_id == message.source_id,
                    AlbumGroup.status.in_([AlbumStatus.COLLECTING, AlbumStatus.READY]),
                    AlbumGroup.product_id.is_(None),
                )
                .with_for_update()
            )
        )
        accepted: list[tuple[AlbumGroup, CollectionAction]] = []
        start_boundaries: list[tuple[AlbumGroup, list[SourceMessage]]] = []
        for group in groups:
            items = list(
                await session.scalars(
                    select(SourceMessage)
                    .where(SourceMessage.album_group_id == group.id)
                    .order_by(SourceMessage.telegram_message_id)
                )
            )
            if not items or not any(
                item.raw_metadata.get("generic_collection_anchor") for item in items
            ):
                continue
            if any(item.raw_metadata.get("generic_collection_closed") for item in items):
                continue
            item_ids = [item.id for item in items]
            media_count_rows = await session.execute(
                select(MediaItem.source_message_id, func.count(MediaItem.id))
                .where(
                    MediaItem.source_message_id.in_(item_ids),
                    MediaItem.active.is_(True),
                    MediaItem.local_deleted_at.is_(None),
                )
                .group_by(MediaItem.source_message_id)
            )
            media_counts: dict[int, int] = {
                int(source_message_id): int(count)
                for source_message_id, count in media_count_rows.tuples()
            }
            current = [
                CollectionMessage(
                    message_id=item.telegram_message_id,
                    occurred_at=self._utc(item.telegram_date),
                    grouped_id=item.grouped_id,
                    text=item.text,
                    caption=item.caption,
                    media_count=int(media_counts.get(item.id, 1 if item.media_pending else 0)),
                )
                for item in items
            ]
            decision = engine.decide(current, incoming)
            if decision.action in {CollectionAction.APPEND, CollectionAction.APPEND_AND_CLOSE}:
                accepted.append((group, decision.action))
            elif decision.action is CollectionAction.START_NEW:
                start_boundaries.append((group, items))

        # START_NEW is a durable close+start transition. Leaving the previous
        # group open made later standalone texts match both products after a
        # restart or catch-up pass.
        for group, items in start_boundaries:
            await self._close_generic_collection_group(
                group,
                items,
                coordinator=coordinator,
                now=now,
            )

        if len(accepted) == 1:
            group, action = accepted[0]
            await coordinator.attach_item(session, group, message, now=now)
            if action is CollectionAction.APPEND_AND_CLOSE:
                message.raw_metadata = {
                    **message.raw_metadata,
                    "generic_collection_closed": True,
                }
                group.ready_at = now + coordinator.debounce
            else:
                self._hold_generic_collection_open(
                    group,
                    collection=collection,
                    coordinator=coordinator,
                    now=now,
                )
            return True
        if len(accepted) > 1:
            set_message_outcome(message, "deferred", "collection_ambiguous")
            message.processing_after = now + coordinator.debounce
            return False

        if collection.strategy == "single_message":
            return False
        if message.grouped_id is not None:
            group = await coordinator.add_item(session, message, now=now)
        else:
            group = await self._new_synthetic_collection(
                session,
                message,
                coordinator=coordinator,
                now=now,
                mode="generic",
            )
            await coordinator.attach_item(session, group, message, now=now)
        message.raw_metadata = {**message.raw_metadata, "generic_collection_anchor": True}
        if empty_decision.action is CollectionAction.APPEND_AND_CLOSE:
            message.raw_metadata = {
                **message.raw_metadata,
                "generic_collection_closed": True,
            }
        else:
            self._hold_generic_collection_open(
                group,
                collection=collection,
                coordinator=coordinator,
                now=now,
            )
        return True

    @staticmethod
    async def _close_generic_collection_group(
        group: AlbumGroup,
        items: list[SourceMessage],
        *,
        coordinator: AlbumCoordinator,
        now: datetime,
    ) -> None:
        for item in items:
            if item.raw_metadata.get("generic_collection_anchor"):
                item.raw_metadata = {
                    **item.raw_metadata,
                    "generic_collection_closed": True,
                }
        group.status = AlbumStatus.COLLECTING
        group.ready_at = now + coordinator.debounce

    @staticmethod
    def _hold_generic_collection_open(
        group: AlbumGroup,
        *,
        collection: Any,
        coordinator: AlbumCoordinator,
        now: datetime,
    ) -> None:
        if not collection.finalize_on_next_start:
            return
        timeout = max(
            coordinator.debounce,
            timedelta(seconds=float(collection.max_time_gap_seconds)),
        )
        group.ready_at = now + timeout

    async def _collect_multi_group_product(
        self,
        session: AsyncSession,
        message: SourceMessage,
        *,
        has_media: bool,
        profile: SourceProfileData,
        coordinator: AlbumCoordinator,
        now: datetime,
    ) -> bool:
        mode = profile.product_collection_mode
        window = timedelta(seconds=float(profile.product_collection_window_seconds))
        text = normalize_merge_caption(message.text or message.caption)

        if message.grouped_id is not None:
            existing_group_ids = set(
                await session.scalars(
                    select(SourceMessage.album_group_id).where(
                        SourceMessage.source_id == message.source_id,
                        SourceMessage.grouped_id == message.grouped_id,
                        SourceMessage.id != message.id,
                        SourceMessage.album_group_id.is_not(None),
                    )
                )
            )
            if len(existing_group_ids) == 1:
                group = await session.get(
                    AlbumGroup, existing_group_ids.pop(), with_for_update=True
                )
                if group is not None:
                    await coordinator.attach_item(session, group, message, now=now)
                    return True

        if mode == "text_before_media":
            if text and message.grouped_id is None and not has_media:
                await self._close_collection_groups(
                    session, message.source_id, mode=mode, now=now
                )
                group = await self._new_synthetic_collection(
                    session, message, coordinator=coordinator, now=now, mode=mode
                )
                await coordinator.attach_item(session, group, message, now=now)
                return True
            if text and message.grouped_id is not None:
                await self._close_collection_groups(
                    session, message.source_id, mode=mode, now=now
                )
                return False
            if not text and has_media:
                candidates = await self._open_collection_candidates(
                    session,
                    message,
                    mode=mode,
                    window=window,
                    require_no_text=False,
                )
                if len(candidates) == 1:
                    await coordinator.attach_item(session, candidates[0], message, now=now)
                    return True
            return False

        if mode == "media_before_text":
            if not text and has_media:
                candidates = await self._open_collection_candidates(
                    session,
                    message,
                    mode=mode,
                    window=window,
                    require_no_text=True,
                )
                if len(candidates) == 1:
                    await coordinator.attach_item(session, candidates[0], message, now=now)
                    return True
                if message.grouped_id is not None:
                    group = await coordinator.add_item(session, message, now=now)
                    self._mark_collection_anchor(message, mode)
                    return group is not None
                group = await self._new_synthetic_collection(
                    session, message, coordinator=coordinator, now=now, mode=mode
                )
                await coordinator.attach_item(session, group, message, now=now)
                return True
            if text and message.grouped_id is None and not has_media:
                candidates = await self._open_collection_candidates(
                    session,
                    message,
                    mode=mode,
                    window=window,
                    require_no_text=True,
                )
                if len(candidates) == 1:
                    group = candidates[0]
                    await coordinator.attach_item(session, group, message, now=now)
                    await self._mark_group_closed(session, group, mode=mode)
                    group.ready_at = now + timedelta(
                        seconds=float(profile.album_debounce_seconds)
                    )
                    return True
            return False
        return False

    async def _new_synthetic_collection(
        self,
        session: AsyncSession,
        message: SourceMessage,
        *,
        coordinator: AlbumCoordinator,
        now: datetime,
        mode: str,
    ) -> AlbumGroup:
        group = AlbumGroup(
            source_id=message.source_id,
            grouped_id=SYNTHETIC_COLLECTION_GROUP_BASE + message.telegram_message_id,
            status=AlbumStatus.COLLECTING,
            last_seen_at=now,
            ready_at=now + coordinator.debounce,
        )
        session.add(group)
        await session.flush()
        self._mark_collection_anchor(message, mode)
        return group

    @staticmethod
    def _mark_collection_anchor(message: SourceMessage, mode: str) -> None:
        message.raw_metadata = {
            **message.raw_metadata,
            "product_collection_anchor": mode,
        }

    async def _close_collection_groups(
        self, session: AsyncSession, source_id: int, *, mode: str, now: datetime
    ) -> None:
        groups = list(
            await session.scalars(
                select(AlbumGroup)
                .where(
                    AlbumGroup.source_id == source_id,
                    AlbumGroup.status.in_([AlbumStatus.COLLECTING, AlbumStatus.READY]),
                    AlbumGroup.product_id.is_(None),
                )
                .with_for_update()
            )
        )
        for group in groups:
            items = list(
                await session.scalars(
                    select(SourceMessage).where(SourceMessage.album_group_id == group.id)
                )
            )
            if any(
                item.raw_metadata.get("product_collection_anchor") == mode
                for item in items
            ):
                await self._mark_group_closed(session, group, mode=mode, items=items)
                group.status = AlbumStatus.COLLECTING
                group.ready_at = now

    async def _mark_group_closed(
        self,
        session: AsyncSession,
        group: AlbumGroup,
        *,
        mode: str,
        items: list[SourceMessage] | None = None,
    ) -> None:
        if items is None:
            items = list(
                await session.scalars(
                    select(SourceMessage).where(SourceMessage.album_group_id == group.id)
                )
            )
        for item in items:
            if item.raw_metadata.get("product_collection_anchor") == mode:
                item.raw_metadata = {
                    **item.raw_metadata,
                    "product_collection_closed": True,
                }

    async def _open_collection_candidates(
        self,
        session: AsyncSession,
        message: SourceMessage,
        *,
        mode: str,
        window: timedelta,
        require_no_text: bool,
    ) -> list[AlbumGroup]:
        groups = list(
            await session.scalars(
                select(AlbumGroup)
                .where(
                    AlbumGroup.source_id == message.source_id,
                    AlbumGroup.status.in_([AlbumStatus.COLLECTING, AlbumStatus.READY]),
                    AlbumGroup.product_id.is_(None),
                )
                .with_for_update()
            )
        )
        message_date = self._utc(message.telegram_date)
        candidates: list[AlbumGroup] = []
        for group in groups:
            items = list(
                await session.scalars(
                    select(SourceMessage).where(SourceMessage.album_group_id == group.id)
                )
            )
            if not items or not any(
                item.raw_metadata.get("product_collection_anchor") == mode
                for item in items
            ):
                continue
            if any(item.raw_metadata.get("product_collection_closed") for item in items):
                continue
            if require_no_text and any(
                normalize_merge_caption(item.text or item.caption) for item in items
            ):
                continue
            latest = max(self._utc(item.telegram_date) for item in items)
            if latest <= message_date <= latest + window:
                candidates.append(group)
        return candidates

    async def _attach_prior_standalone(
        self,
        session: AsyncSession,
        group: AlbumGroup,
        album_message: SourceMessage,
        coordinator: AlbumCoordinator,
        *,
        window: timedelta,
        now: datetime,
    ) -> None:
        caption = normalize_merge_caption(album_message.text or album_message.caption)
        if not caption:
            return
        candidates = list(
            await session.scalars(
                select(SourceMessage)
                .where(
                    SourceMessage.source_id == album_message.source_id,
                    SourceMessage.grouped_id.is_(None),
                    SourceMessage.album_group_id.is_(None),
                    SourceMessage.telegram_date >= album_message.telegram_date - window,
                    SourceMessage.telegram_date <= album_message.telegram_date + window,
                    or_(
                        SourceMessage.media_pending.is_(True),
                        exists(
                            select(MediaItem.id).where(
                                MediaItem.source_message_id == SourceMessage.id,
                                MediaItem.active.is_(True),
                                MediaItem.local_deleted_at.is_(None),
                            )
                        ),
                    ),
                    ~exists(
                        select(Product.id).where(Product.source_message_id == SourceMessage.id)
                    ),
                )
                .with_for_update()
            )
        )
        matches = [
            candidate
            for candidate in candidates
            if normalize_merge_caption(candidate.text or candidate.caption) == caption
        ]
        if len(matches) == 1:
            await coordinator.attach_item(session, group, matches[0], now=now)

    async def _attach_to_recent_album(
        self,
        session: AsyncSession,
        standalone: SourceMessage,
        coordinator: AlbumCoordinator,
        *,
        window: timedelta,
        now: datetime,
    ) -> None:
        caption = normalize_merge_caption(standalone.text or standalone.caption)
        if not caption:
            return
        candidates = list(
            await session.scalars(
                select(SourceMessage)
                .where(
                    SourceMessage.source_id == standalone.source_id,
                    SourceMessage.album_group_id.is_not(None),
                    SourceMessage.telegram_date >= standalone.telegram_date - window,
                    SourceMessage.telegram_date <= standalone.telegram_date + window,
                )
                .with_for_update()
            )
        )
        group_ids = {
            candidate.album_group_id
            for candidate in candidates
            if candidate.album_group_id is not None
            and normalize_merge_caption(candidate.text or candidate.caption) == caption
        }
        if len(group_ids) != 1:
            return
        group = await session.get(AlbumGroup, group_ids.pop(), with_for_update=True)
        if group is not None:
            await coordinator.attach_item(session, group, standalone, now=now)

    async def _attach_text_to_recent_album(
        self,
        session: AsyncSession,
        companion: SourceMessage,
        coordinator: AlbumCoordinator,
        *,
        window: timedelta,
        now: datetime,
    ) -> bool:
        latest_item = (
            select(
                SourceMessage.album_group_id.label("group_id"),
                func.max(SourceMessage.telegram_date).label("latest_date"),
            )
            .where(SourceMessage.album_group_id.is_not(None))
            .group_by(SourceMessage.album_group_id)
            .subquery()
        )
        candidates = list(
            await session.scalars(
                select(AlbumGroup)
                .join(latest_item, latest_item.c.group_id == AlbumGroup.id)
                .where(
                    AlbumGroup.source_id == companion.source_id,
                    latest_item.c.latest_date <= companion.telegram_date,
                    latest_item.c.latest_date >= companion.telegram_date - window,
                    ~exists(
                        select(SourceMessage.id).where(
                            SourceMessage.album_group_id == AlbumGroup.id,
                            or_(
                                func.length(func.trim(SourceMessage.text)) > 0,
                                func.length(func.trim(SourceMessage.caption)) > 0,
                            ),
                        )
                    ),
                )
                .order_by(latest_item.c.latest_date.desc())
                .limit(2)
                .with_for_update(of=AlbumGroup)
            )
        )
        if len(candidates) == 1:
            await coordinator.attach_item(session, candidates[0], companion, now=now)
            return True
        return False

    async def _attach_text_to_pending_collection(
        self,
        session: AsyncSession,
        companion: SourceMessage,
        profile: SourceProfileData,
        coordinator: AlbumCoordinator,
        *,
        window: timedelta,
        now: datetime,
    ) -> bool:
        standalone = list(
            await session.scalars(
                select(SourceMessage)
                .where(
                    SourceMessage.source_id == companion.source_id,
                    SourceMessage.grouped_id.is_(None),
                    SourceMessage.album_group_id.is_(None),
                    SourceMessage.telegram_date <= companion.telegram_date,
                    SourceMessage.telegram_date >= companion.telegram_date - window,
                    SourceMessage.processing_after.is_not(None),
                    SourceMessage.processing_after > now,
                    or_(
                        SourceMessage.media_pending.is_(True),
                        exists(
                            select(MediaItem.id).where(
                                MediaItem.source_message_id == SourceMessage.id,
                                MediaItem.active.is_(True),
                                MediaItem.local_deleted_at.is_(None),
                            )
                        ),
                    ),
                    ~exists(
                        select(Product.id).where(Product.source_message_id == SourceMessage.id)
                    ),
                )
                .with_for_update()
            )
        )
        synthetic_groups = list(
            await session.scalars(
                select(AlbumGroup)
                .where(
                    AlbumGroup.source_id == companion.source_id,
                    AlbumGroup.grouped_id < 0,
                    AlbumGroup.status == AlbumStatus.COLLECTING,
                    AlbumGroup.ready_at > now,
                    AlbumGroup.product_id.is_(None),
                )
                .with_for_update()
            )
        )

        candidates: list[tuple[AlbumGroup | None, list[SourceMessage]]] = []
        for anchor in standalone:
            if await self._is_safe_collection_continuation(
                session, [anchor], companion, profile.ignore_rules, profile
            ):
                candidates.append((None, [anchor]))
        for group in synthetic_groups:
            messages = list(
                await session.scalars(
                    select(SourceMessage)
                    .where(SourceMessage.album_group_id == group.id)
                    .order_by(SourceMessage.telegram_message_id)
                )
            )
            if not messages:
                continue
            latest = self._utc(max(item.telegram_date for item in messages))
            companion_date = self._utc(companion.telegram_date)
            if not (latest <= companion_date and latest >= companion_date - window):
                continue
            if await self._is_safe_collection_continuation(
                session, messages, companion, profile.ignore_rules, profile
            ):
                candidates.append((group, messages))

        if len(candidates) != 1:
            return False
        selected_group, messages = candidates[0]
        if selected_group is None:
            anchor = messages[0]
            selected_group = AlbumGroup(
                source_id=anchor.source_id,
                grouped_id=SYNTHETIC_COLLECTION_GROUP_BASE + anchor.telegram_message_id,
                status=AlbumStatus.COLLECTING,
                last_seen_at=now,
                ready_at=now + coordinator.debounce,
            )
            session.add(selected_group)
            await session.flush()
            anchor.album_group_id = selected_group.id
            anchor.processing_after = None
        await coordinator.attach_item(session, selected_group, companion, now=now)
        return True

    @staticmethod
    async def _is_safe_collection_continuation(
        session: AsyncSession,
        messages: list[SourceMessage],
        companion: SourceMessage,
        rules: list[SourceIgnoreRule],
        profile: SourceProfileData,
    ) -> bool:
        message_ids = [item.id for item in messages]
        media_records = list(
            (
                await session.execute(
                    select(MediaItem.source_message_id, MediaItem.media_type).where(
                        MediaItem.source_message_id.in_(message_ids),
                        MediaItem.active.is_(True),
                        MediaItem.local_deleted_at.is_(None),
                    )
                )
            ).all()
        )
        media_message_ids = {
            source_message_id for source_message_id, _media_type in media_records
        }
        anchors = [item for item in messages if item.id in media_message_ids or item.media_pending]
        if not anchors:
            return False
        alias_rows = list(
            await session.scalars(
                select(CurrencyAlias).where(CurrencyAlias.enabled.is_(True))
            )
        )
        engine = RuleEngine(
            profile,
            CurrencyAliasResolver({item.alias: item.currency for item in alias_rows}),
        )
        anchor = anchors[0]
        anchor_parsed = engine.parse(anchor.text or anchor.caption)
        rule = match_source_ignore_rule(
            anchor_parsed,
            has_media=True,
            media_types=[
                media_type
                for source_message_id, media_type in media_records
                if source_message_id == anchor.id
            ],
            rules=rules,
        )
        if rule is None:
            return False
        combined_text = "\n".join(
            dict.fromkeys(
                part for item in messages for part in (item.text, item.caption) if part.strip()
            )
        )
        combined = engine.parse(combined_text)
        supplemental = engine.parse(companion.text or companion.caption)
        for name in rule.found_fields:
            anchor_value = anchor_parsed.fields.get(name)
            supplemental_value = supplemental.fields.get(name)
            if (
                supplemental_value is not None
                and supplemental_value.status is FieldStatus.FOUND
                and anchor_value is not None
                and supplemental_value.value != anchor_value.value
            ):
                return False
        unresolved = [
            name
            for name in rule.missing_fields
            if combined.fields.get(name) is None
            or combined.fields[name].status is FieldStatus.MISSING
        ]
        return any(
            supplemental.fields.get(name) is not None
            and supplemental.fields[name].status is FieldStatus.FOUND
            for name in unresolved
        )

    @staticmethod
    def _media_version_is_current(row: SourceMessage, telegram_message: Any) -> bool:
        # A supplier edit can arrive while an older download is in flight.
        # Never let the old transfer clear the newer version's pending flag.
        incoming_edit = getattr(telegram_message, "edit_date", None)
        if row.edit_date is None:
            return True
        return (
            incoming_edit is not None
            and CollectorService._utc(incoming_edit) >= CollectorService._utc(row.edit_date)
        )

    @staticmethod
    def _mark_media_recovered(row: SourceMessage) -> None:
        row.media_pending = False
        metadata = dict(row.raw_metadata or {})
        for key in (
            "media_download_error",
            "media_recovery_missing_count",
            "media_recovery_next_at",
            "media_recovery_terminal_at",
        ):
            metadata.pop(key, None)
        row.raw_metadata = metadata

    async def _download_media(
        self, telegram_message: Any, source_message_id: int, source_id: int
    ) -> None:
        if isinstance(getattr(telegram_message, "media", None), MessageMediaWebPage):
            return
        # The semaphore is acquired before opening any session. A burst of live
        # Telegram updates must not retain many downloaded byte arrays or starve
        # the database while waiting for a download slot.
        async with self._media_download_slots:
            await self._download_media_with_slot(
                telegram_message, source_message_id, source_id
            )

    async def _download_media_with_slot(
        self, telegram_message: Any, source_message_id: int, source_id: int
    ) -> None:
        async with self.sessions() as session, session.begin():
            await lock_source(session, source_id)
            row = await session.scalar(
                select(SourceMessage).where(SourceMessage.id == source_message_id).with_for_update()
            )
            if row is None or not self._media_version_is_current(row, telegram_message):
                return
            existing = list(await session.scalars(
                select(MediaItem).where(
                    MediaItem.source_message_id == source_message_id,
                    MediaItem.active.is_(True),
                    MediaItem.local_deleted_at.is_(None),
                )
            ))
            if existing and all(
                [await self.storage.exists(item.storage_path) for item in existing]
            ):
                self._mark_media_recovered(row)
                return

        last_error: Exception | None = None
        for attempt in range(3):
            try:
                # No database transaction is open during Telegram I/O or retry waits.
                content = await self.client.download_media(telegram_message, file=bytes)
                if not isinstance(content, bytes) or not content:
                    raise OSError("Telegram returned no media bytes")
                digest = hashlib.sha256(content).hexdigest()
                media_type = "video" if getattr(telegram_message, "video", None) else "photo"
                extension = ".mp4" if media_type == "video" else ".jpg"
                filename = (
                    f"source-{source_id}/message-{telegram_message.id}/"
                    f"{telegram_message.id}-{digest}{extension}"
                )
                async with self.sessions() as session, session.begin():
                    await lock_source(session, source_id)
                    row = await session.scalar(
                        select(SourceMessage)
                        .where(SourceMessage.id == source_message_id)
                        .with_for_update()
                    )
                    if row is None or not self._media_version_is_current(
                        row, telegram_message
                    ):
                        return
                    existing = list(await session.scalars(
                        select(MediaItem).where(
                            MediaItem.source_message_id == source_message_id,
                            MediaItem.active.is_(True),
                            MediaItem.local_deleted_at.is_(None),
                        )
                    ))
                    if existing and all(
                        [await self.storage.exists(item.storage_path) for item in existing]
                    ):
                        self._mark_media_recovered(row)
                        return
                    await session.execute(
                        update(MediaItem)
                        .where(
                            MediaItem.source_message_id == source_message_id,
                            MediaItem.active.is_(True),
                        )
                        .values(active=False)
                    )
                    # Keep the version lock through local storage so a stale edit
                    # cannot leave an unreferenced file in the media volume.
                    stored = await self.storage.store(filename, content)
                    maximum = await session.scalar(
                        select(func.max(MediaItem.position)).where(
                            MediaItem.source_message_id == source_message_id
                        )
                    )
                    session.add(
                        MediaItem(
                            source_message_id=source_message_id,
                            telegram_message_id=int(telegram_message.id),
                            position=(maximum if maximum is not None else -1) + 1,
                            media_type=media_type,
                            storage_path=stored.path,
                            size_bytes=stored.size_bytes,
                            sha256=stored.sha256,
                            active=True,
                        )
                    )
                    self._mark_media_recovered(row)
                return
            except FloodWaitError as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(min(float(exc.seconds), 60))
            except (RPCError, OSError) as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(2**attempt)
            except ValueError as exc:
                # Telethon raises ValueError after its own request retry budget.
                last_error = exc
                break

        async with self.sessions() as session, session.begin():
            row = await session.scalar(
                select(SourceMessage).where(SourceMessage.id == source_message_id).with_for_update()
            )
            if row is None or not self._media_version_is_current(row, telegram_message):
                return
            metadata = dict(row.raw_metadata or {})
            metadata["media_download_error"] = str(last_error)[:500]
            row.raw_metadata = metadata
            row.media_pending = False
        logger.error(
            "media persistence failed after retries",
            extra={
                "source_id": source_id,
                "telegram_message_id": telegram_message.id,
                "grouped_id": getattr(telegram_message, "grouped_id", None),
            },
        )

    async def validate_access(self, reference: str | int) -> tuple[int, str, int]:
        entity = await self.client.get_entity(reference)
        title = str(
            getattr(entity, "title", None) or getattr(entity, "username", None) or reference
        )
        from telethon import utils

        channel_id = int(utils.get_peer_id(entity))
        return channel_id, title, await self._latest_message_id(entity)

    async def _latest_message_id(self, channel: object) -> int:
        result = await self.client.get_messages(channel, limit=1)
        if result is None:
            return 0
        messages = result if isinstance(result, (list, tuple)) else [result]
        return max((int(item.id) for item in messages if item is not None), default=0)

    async def load_message_reference(self, reference: str) -> tuple[str, list[str]]:
        value = reference.strip()
        channel: str | int
        message_id: int
        public = re.fullmatch(r"https?://t\.me/([A-Za-z0-9_]+)/([0-9]+)", value)
        private = re.fullmatch(r"https?://t\.me/c/([0-9]+)/([0-9]+)", value)
        direct = re.fullmatch(r"(-?[0-9]+):([0-9]+)", value)
        if public:
            channel, message_id = public.group(1), int(public.group(2))
        elif private:
            channel, message_id = int(f"-100{private.group(1)}"), int(private.group(2))
        elif direct:
            channel, message_id = int(direct.group(1)), int(direct.group(2))
        else:
            raise ValueError("use a t.me message link or channel_id:message_id")
        message = await self.client.get_messages(channel, ids=message_id)
        if message is None:
            raise LookupError("Telegram message not found or inaccessible")
        reference_messages = [message]
        grouped_id = getattr(message, "grouped_id", None)
        if grouped_id is not None:
            nearby = await self.client.get_messages(
                channel,
                ids=list(range(max(1, message_id - 9), message_id + 10)),
            )
            reference_messages = sorted(
                [
                    item
                    for item in nearby
                    if item is not None and getattr(item, "grouped_id", None) == grouped_id
                ],
                key=lambda item: int(item.id),
            )
        text = "\n".join(
            dict.fromkeys(
                value
                for item in reference_messages
                for value in [str(getattr(item, "message", "") or "")]
                if value.strip()
            )
        )
        paths: list[str] = []
        reference_grouped_id = grouped_id
        async with self.sessions() as session:
            entity = await self.client.get_entity(channel)
            from telethon import utils

            channel_id = int(utils.get_peer_id(entity))
            source = await SourceRepository(session).by_channel(channel_id, enabled_only=False)
            if source:
                row = await session.scalar(
                    select(SourceMessage).where(
                        SourceMessage.source_id == source.id,
                        SourceMessage.telegram_message_id == message_id,
                    )
                )
                if row:
                    if row.album_group_id is not None:
                        album_messages = list(
                            await session.scalars(
                                select(SourceMessage)
                                .where(SourceMessage.album_group_id == row.album_group_id)
                                .order_by(SourceMessage.telegram_message_id)
                            )
                        )
                        text = "\n".join(
                            dict.fromkeys(
                                part
                                for item in album_messages
                                for part in (item.text, item.caption)
                                if part.strip()
                            )
                        )
                        source_message_ids = [item.id for item in album_messages]
                        reference_grouped_id = next((item.grouped_id for item in album_messages if item.grouped_id is not None), None)
                    else:
                        source_message_ids = [row.id]
                    media = list(
                        await session.scalars(
                            select(MediaItem)
                            .where(
                                MediaItem.source_message_id.in_(source_message_ids),
                                MediaItem.active.is_(True),
                                MediaItem.local_deleted_at.is_(None),
                            )
                            .order_by(MediaItem.telegram_message_id, MediaItem.position)
                        )
                    )
                    root = getattr(self.storage, "root", None)
                    if root:
                        previews = {int(item.id) for item in reference_messages
                                    if isinstance(getattr(item, "media", None), MessageMediaWebPage)}
                        paths = [str(root / item.storage_path) for item in media
                                 if item.telegram_message_id not in previews]
        if not paths:
            temporary_paths: list[str] = []
            try:
                for item in reference_messages:
                    if telegram_product_media_type(item) is None:
                        continue
                    content = await self.client.download_media(item, file=bytes)
                    if not isinstance(content, bytes):
                        continue
                    suffix = ".mp4" if getattr(item, "video", None) else ".jpg"
                    descriptor, temporary_path = tempfile.mkstemp(
                        prefix="emoda-admin-reference-", suffix=suffix
                    )
                    os.close(descriptor)
                    await asyncio.to_thread(Path(temporary_path).write_bytes, content)
                    temporary_paths.append(temporary_path)
                paths = temporary_paths
            except Exception:
                for temporary_path in temporary_paths:
                    await asyncio.to_thread(Path(temporary_path).unlink, missing_ok=True)
                raise
        return text, ReferenceMediaPaths(paths, reference_grouped_id)

    async def _record_unavailable_pending_media(
        self, source_message_id: int, *, checked_at: datetime
    ) -> tuple[bool, int]:
        """Bound missing-message retries and preserve a published product for review."""
        async with self.sessions() as session, session.begin():
            source_id = await session.scalar(select(SourceMessage.source_id).where(
                SourceMessage.id == source_message_id))
            if source_id is None:
                return False, 0
            await lock_source(session, source_id)
            row = await session.scalar(
                select(SourceMessage)
                .where(SourceMessage.id == source_message_id)
                .with_for_update()
            )
            if row is None or not row.media_pending:
                return False, 0
            metadata = dict(row.raw_metadata or {})
            previous = metadata.get("media_recovery_missing_count")
            missing_count = (previous if isinstance(previous, int) and previous > 0 else 0) + 1
            metadata["media_recovery_missing_count"] = missing_count
            old_enough = self._utc(row.telegram_date) <= checked_at - timedelta(days=1)
            terminal = old_enough and missing_count >= 3
            if terminal:
                detail = (
                    "source media download failed: Telegram message "
                    f"{row.telegram_message_id} is no longer accessible after repeated lookups"
                )
                metadata.pop("media_recovery_next_at", None)
                metadata.pop("intake_retry_pending", None)
                metadata.pop("intake_retry_after", None)
                metadata["media_recovery_terminal_at"] = checked_at.isoformat()
                metadata["media_download_error"] = detail
                row.media_pending = False

                product_links = [Product.source_message_id == row.id]
                if row.album_group_id is not None:
                    product_links.append(Product.album_group_id == row.album_group_id)
                product = await session.scalar(
                    select(Product)
                    .where(Product.source_id == row.source_id, or_(*product_links))
                    .with_for_update()
                )
                if product is not None:
                    if product.status in {ProductStatus.READY, ProductStatus.PROCESSING}:
                        product.status = ProductStatus.NEEDS_REVIEW
                        product.processing_error = detail
                        product.review_notified_at = None
                        set_product_review_reason(
                            product,
                            ReviewReasonCode.MEDIA_DOWNLOAD_FAILED,
                            detail=detail,
                            field="media",
                            append=True,
                        )
                    if row.album_group_id is not None and product.status in {
                        ProductStatus.NEEDS_REVIEW,
                        ProductStatus.PUBLISHED,
                        ProductStatus.SKIPPED,
                        ProductStatus.FAILED,
                        ProductStatus.RECONCILIATION_REQUIRED,
                    }:
                        album = await session.get(
                            AlbumGroup, row.album_group_id, with_for_update=True
                        )
                        if (
                            album is not None
                            and album.product_id == product.id
                            and album.status is AlbumStatus.READY
                        ):
                            album.status = AlbumStatus.PROCESSED
            else:
                delay_minutes = min(15 * (2 ** min(missing_count - 1, 5)), 360)
                metadata["media_recovery_next_at"] = (
                    checked_at + timedelta(minutes=delay_minutes)
                ).isoformat()
            row.raw_metadata = metadata
            return terminal, missing_count

    async def _retry_intake(self) -> None:
        async with self.sessions() as session:
            rows = list(await session.execute(select(SourceMessage, Source.telegram_channel_id)
                .join(Source, Source.id == SourceMessage.source_id)
                .where(Source.enabled.is_(True), Source.archived_at.is_(None),
                    SourceMessage.media_pending.is_(True))))
        for row, channel in rows:
            metadata = row.raw_metadata or {}
            if not metadata.get("intake_retry_pending"):
                continue
            after = metadata.get("media_recovery_next_at") or metadata.get("intake_retry_after")
            if after and self._utc(datetime.fromisoformat(after)) > datetime.now(UTC):
                continue
            try:
                message = await self.client.get_messages(channel, ids=row.telegram_message_id)
                if message is None:
                    await self._record_unavailable_pending_media(row.id, checked_at=datetime.now(UTC))
                    continue  # Missing input becomes actionable review, never false success.
                event = type("RetryEvent", (), {"chat_id":channel,"message":message})()
                await self.handle_event(event, edited=bool(getattr(message,"edit_date",None)),
                    origin=EventOrigin.CATCHUP)
            except Exception:
                logger.exception("intake recovery failed", extra={"source_id":row.source_id,
                    "telegram_message_id":row.telegram_message_id})

    async def _catchup_error(self, source_id: int, error: Exception) -> None:
        async with self.sessions() as session, session.begin():
            await lock_source(session, source_id)
            source = await session.get(Source, source_id)
            if source:
                source.catchup_error = f"{datetime.now(UTC).isoformat()} {type(error).__name__}: {error}"[:1000]

    async def catch_up(self) -> None:
        await self._retry_intake()
        async with self.sessions() as session:
            sources = list(
                await session.scalars(
                    select(Source).where(Source.enabled.is_(True), Source.archived_at.is_(None))
                )
            )
            # A disabled source stops NEW intake, yet a restart must not lose a
            # supplier edit belonging to a publication the system already tracks.
            update_only_sources = list(
                await session.scalars(
                    select(Source).where(
                        Source.enabled.is_(False),
                        Source.archived_at.is_(None),
                        exists(
                            select(SourceMessage.id).where(
                                SourceMessage.source_id == Source.id
                            )
                        ),
                    )
                )
            )
            pending_media = list(
                await session.execute(
                    select(SourceMessage, Source.telegram_channel_id)
                    .join(Source, Source.id == SourceMessage.source_id)
                    .where(
                        Source.archived_at.is_(None),
                        SourceMessage.media_pending.is_(True),
                    )
                    .order_by(SourceMessage.telegram_date, SourceMessage.telegram_message_id)
                )
            )
        for row, channel_id in pending_media:
            if (row.raw_metadata or {}).get("intake_retry_pending"):
                continue
            retry_at = (row.raw_metadata or {}).get("media_recovery_next_at")
            if isinstance(retry_at, str):
                try:
                    if self._utc(datetime.fromisoformat(retry_at)) > datetime.now(UTC):
                        continue
                except ValueError:
                    pass
            try:
                telegram_message = await self.client.get_messages(
                    channel_id, ids=row.telegram_message_id
                )
                if telegram_message is None:
                    terminal, missing_count = await self._record_unavailable_pending_media(
                        row.id, checked_at=datetime.now(UTC)
                    )
                    if terminal:
                        logger.error(
                            "pending source media is unavailable; product requires review",
                            extra={
                                "source_id": row.source_id,
                                "telegram_message_id": row.telegram_message_id,
                                "grouped_id": row.grouped_id,
                            },
                        )
                    elif missing_count == 1:
                        logger.warning(
                            "pending source media is temporarily unavailable; retry scheduled",
                            extra={
                                "source_id": row.source_id,
                                "telegram_message_id": row.telegram_message_id,
                                "grouped_id": row.grouped_id,
                            },
                        )
                    continue
                await self._download_media(telegram_message, row.id, row.source_id)
            except Exception:
                logger.exception(
                    "pending media recovery failed",
                    extra={
                        "source_id": row.source_id,
                        "telegram_message_id": row.telegram_message_id,
                        "grouped_id": row.grouped_id,
                    },
                )
        for source in sources:
            try:
                if source.last_seen_message_id is None:
                    latest_id = await self._latest_message_id(source.telegram_channel_id)
                    async with self.sessions() as session, session.begin():
                        current = await session.get(Source, source.id, with_for_update=True)
                        if current is not None and current.last_seen_message_id is None:
                            current.catchup_anchor_message_id = latest_id
                            current.last_seen_message_id = latest_id
                            current.catchup_cursor_message_id = latest_id
                            current.last_catchup_at = datetime.now(UTC)
                    continue
                cursor = source.catchup_cursor_message_id
                if cursor is None:
                    cursor = source.last_seen_message_id or source.catchup_anchor_message_id or 0
                completed_cursor = cursor
                async for message in self.client.iter_messages(
                    source.telegram_channel_id, min_id=cursor, reverse=True, limit=100,
                ):
                    if int(message.id) <= cursor:
                        continue
                    event = type("CatchupEvent", (), {"chat_id":source.telegram_channel_id,
                        "message":message})()
                    if await self.handle_event(event, edited=False, origin=EventOrigin.CATCHUP) is False:
                        raise OSError("intake incomplete; history cursor not advanced")
                    completed_cursor = max(completed_cursor, int(message.id))
                # Independent, durable backwards sweep catches lower-ID holes even
                # when live updates have already moved last_seen beyond them.
                recent_messages = [message async for message in self.client.iter_messages(
                    source.telegram_channel_id, limit=max(100, self.edit_rescan_limit),
                    offset_id=source.catchup_scan_before_id or 0)]
                cutoff = datetime.now(UTC) - timedelta(days=7)
                existing_message_ids: set[int] = set()
                if recent_messages:
                    async with self.sessions() as session:
                        existing_message_ids = set(
                            await session.scalars(
                                select(SourceMessage.telegram_message_id).where(
                                    SourceMessage.source_id == source.id,
                                    SourceMessage.telegram_message_id.in_(
                                        [int(message.id) for message in recent_messages]
                                    ),
                                )
                            )
                        )
                anchor = source.catchup_anchor_message_id
                if anchor is None:
                    anchor = source.last_seen_message_id or 0
                for message in sorted(recent_messages, key=lambda item: int(item.id)):
                    message_id = int(message.id)
                    if (message_id <= anchor or message_id in existing_message_ids
                        or self._utc(getattr(message,"date",None)) < cutoff):
                        continue
                    event = type(
                        "CatchupGapEvent",
                        (),
                        {"chat_id": source.telegram_channel_id, "message": message},
                    )()
                    if await self.handle_event(event, edited=False, origin=EventOrigin.CATCHUP) is False:
                        raise OSError("gap intake incomplete; sweep cursor not advanced")
                    existing_message_ids.add(message_id)
                edited_messages = [
                    message
                    for message in recent_messages
                    if getattr(message, "edit_date", None) is not None
                ]
                for message in edited_messages:
                    # The edit window must never backfill pre-onboarding history.
                    # Messages received during downtime were persisted by the
                    # min_id pass immediately above and are therefore included.
                    if int(message.id) not in existing_message_ids:
                        continue
                    event = type(
                        "CatchupEditEvent",
                        (),
                        {"chat_id": source.telegram_channel_id, "message": message},
                    )()
                    if await self.handle_event(event, edited=True, origin=EventOrigin.CATCHUP) is False:
                        raise OSError("edit intake incomplete; sweep cursor not advanced")
                async with self.sessions() as session, session.begin():
                    await lock_source(session, source.id)
                    current = await session.get(Source, source.id)
                    if current:
                        current.last_catchup_at = datetime.now(UTC)
                        current.catchup_error = None
                        current.catchup_cursor_message_id = completed_cursor
                        oldest = min((int(m.id) for m in recent_messages), default=0)
                        current.catchup_scan_before_id = (
                            oldest if len(recent_messages) >= max(100,self.edit_rescan_limit)
                            and oldest > (source.catchup_anchor_message_id or 0)
                            and all(self._utc(getattr(m,"date",None)) >= cutoff for m in recent_messages) else None)
            except FloodWaitError as exc:
                await self._catchup_error(source.id, exc)
                logger.warning(
                    "catch-up flood wait",
                    extra={
                        "source_id": source.id,
                        "telegram_message_id": None,
                        "grouped_id": None,
                        "wait_seconds": exc.seconds,
                    },
                )
            except (RPCError, OSError, TimeoutError) as exc:
                await self._catchup_error(source.id, exc)
                logger.exception(
                    "catch-up failed",
                    extra={"source_id": source.id, "telegram_message_id": None, "grouped_id": None},
                )
            except Exception:
                logger.exception(
                    "unexpected catch-up failure isolated to source",
                    extra={"source_id": source.id, "telegram_message_id": None, "grouped_id": None},
                )
        for source in update_only_sources:
            try:
                await self._replay_known_edits(source)
                async with self.sessions() as session, session.begin():
                    current = await session.get(Source, source.id)
                    if current:
                        current.last_catchup_at = datetime.now(UTC)
            except FloodWaitError as exc:
                logger.warning(
                    "catch-up flood wait",
                    extra={
                        "source_id": source.id,
                        "telegram_message_id": None,
                        "grouped_id": None,
                        "wait_seconds": exc.seconds,
                    },
                )
            except (RPCError, OSError, TimeoutError):
                logger.exception(
                    "catch-up failed",
                    extra={"source_id": source.id, "telegram_message_id": None, "grouped_id": None},
                )
            except Exception:
                logger.exception(
                    "unexpected catch-up failure isolated to source",
                    extra={"source_id": source.id, "telegram_message_id": None, "grouped_id": None},
                )

    async def _replay_known_edits(self, source: Source) -> None:
        """Re-read a bounded window and replay edits of already-known messages.

        `min_id` cannot reveal edits, and a disabled source must never backfill
        history, so only messages this source already persisted are replayed.
        `edit_date` versioning suppresses unchanged repeats.
        """
        edited = [
            message
            async for message in self.client.iter_messages(
                source.telegram_channel_id, limit=self.edit_rescan_limit
            )
            if getattr(message, "edit_date", None) is not None
        ]
        if not edited:
            return
        async with self.sessions() as session:
            known = set(
                await session.scalars(
                    select(SourceMessage.telegram_message_id).where(
                        SourceMessage.source_id == source.id,
                        SourceMessage.telegram_message_id.in_(
                            [int(message.id) for message in edited]
                        ),
                    )
                )
            )
        for message in sorted(edited, key=lambda item: int(item.id)):
            if int(message.id) not in known:
                continue
            event = type(
                "CatchupEditEvent",
                (),
                {"chat_id": source.telegram_channel_id, "message": message},
            )()
            await self.handle_event(event, edited=True, origin=EventOrigin.CATCHUP)

    @staticmethod
    def _utc(value: datetime | None) -> datetime:
        if value is None:
            return datetime.now(UTC)
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
