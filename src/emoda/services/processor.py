from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

from sqlalchemy import exists, false, func, or_, select, union_all
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from emoda.db.intake import lock_source, transient_database_error
from emoda.db.models import (
    AlbumGroup,
    CurrencyAlias,
    Destination,
    GlobalSetting,
    MarkupRule,
    MediaItem,
    Product,
    PublicationOutbox,
    Source,
    SourceExample,
    SourceMessage,
)
from emoda.domain.enums import (
    AlbumStatus,
    OutboxStatus,
    ProductStatus,
    PublicationMode,
    ReviewReasonCode,
)
from emoda.domain.schemas import MarkupRange, SourceProfileData
from emoda.rules.currency import CurrencyAliasResolver
from emoda.services.albums import AlbumCoordinator
from emoda.services.codes import CodeAllocator
from emoda.services.collection import CollectionEngine, CollectionMessage
from emoda.services.orchestrator import ProcessingMode, process_product
from emoda.services.outcomes import set_message_outcome, set_product_message_outcome
from emoda.services.pipeline import IntelligenceAdapter, Pipeline, PipelineInput
from emoda.services.publication import (
    PUBLICATION_BUTTONS_SETTING_KEY,
    PublicationService,
    UncertainPublicationFailure,
    publication_keyboard,
)
from emoda.services.render import validate_publication_text
from emoda.services.review import (
    ReviewPublishResult,
    StaleReviewApproval,
    classify_review_messages,
    reason,
    review_approval_token,
    set_product_review_reason,
)

logger = logging.getLogger(__name__)


def resolve_publication_mode(
    profile: SourceProfileData,
    global_mode: PublicationMode,
) -> PublicationMode:
    return global_mode if profile.use_global_publication_mode else profile.publication_mode


def _media_path_tokens(value: object) -> list[str]:
    """Render a stored or freshly built media path list comparably."""
    if not isinstance(value, list):
        return []
    return [str(item) for item in value]


def _media_files_present(paths: list[str]) -> list[bool]:
    return [Path(path).is_file() for path in paths]


def _logical_media_path_tokens(payload: dict[str, object]) -> list[str]:
    """Media belonging to the approved source state, even after a text-only repair."""
    return _media_path_tokens(
        payload.get("approved_source_media_paths", payload.get("media_paths", []))
    )


def _publication_is_unchanged(
    original: PublicationOutbox,
    payload: dict[str, object],
    keyboard: list[list[dict[str, str]]],
) -> bool:
    """True when a reprocess renders exactly the published payload.

    `operation` and `expected_message_count` describe how to deliver the payload,
    not what the reader sees, so they are excluded from the comparison.
    """
    stored = original.payload
    return (
        str(stored.get("text", "")) == str(payload.get("text", ""))
        and _logical_media_path_tokens(stored) == _logical_media_path_tokens(payload)
        and str(stored.get("emoda_code", "")) == str(payload.get("emoda_code", ""))
        and original.keyboard_snapshot == keyboard
    )


class ProcessorService:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        media_root: Path,
        intelligence: IntelligenceAdapter | None,
        publication: PublicationService | None,
        bot_username: str | None = None,
    ) -> None:
        self.sessions = sessions
        self.media_root = media_root
        self.pipeline = Pipeline(intelligence)
        self.publication = publication
        self.bot_username = bot_username

    async def recover(self) -> None:
        await self.reconcile_publications()
        await self.recover_jobs()

    async def reconcile_publications(self) -> None:
        if self.publication:
            async with self.sessions() as session:
                await self.publication.reconcile(session)

    async def recover_jobs(self) -> None:
        async with self.sessions() as session, session.begin():
            recovery_candidates = list(
                await session.scalars(
                    select(Product)
                    .where(
                        Product.status.in_(
                            [
                                ProductStatus.RECEIVED,
                                ProductStatus.PROCESSING,
                                ProductStatus.READY,
                            ]
                        )
                    )
                    .with_for_update(skip_locked=True)
                )
            )
            for product in recovery_candidates:
                if product.album_group_id is not None:
                    album = await session.get(
                        AlbumGroup, product.album_group_id, with_for_update=True
                    )
                    if (
                        album is not None
                        and album.source_id == product.source_id
                        and album.product_id == product.id
                        and album.status is AlbumStatus.PROCESSED
                    ):
                        # Claiming an album durably marks it PROCESSED before the slow
                        # extraction step. A process restart must reopen that same group
                        # together with its interrupted product or READY becomes unreachable.
                        album.status = AlbumStatus.READY
                if product.status is not ProductStatus.READY:
                    product.status = ProductStatus.READY
        if self.publication:
            async with self.sessions() as session:
                ready_to_publish_ids = list(
                    await session.scalars(
                        select(Product.id)
                        .join(Source, Source.id == Product.source_id)
                        .where(Product.status == ProductStatus.READY_TO_PUBLISH, Source.archived_at.is_(None))
                        .order_by(Product.created_at)
                    )
                )
            for product_id in ready_to_publish_ids:
                try:
                    await self.publish_reviewed(product_id, resume=True)
                except Exception as exc:
                    await self._record_recovery_failure(product_id, exc)

    async def _record_recovery_failure(self, product_id: int, exc: Exception) -> None:
        """Startup recovery audits a wedged publication instead of aborting the app."""
        uncertain = isinstance(exc, UncertainPublicationFailure)
        async with self.sessions() as session, session.begin():
            product = await session.get(Product, product_id)
            if product:
                product.processing_error = f"publication recovery failed: {exc}"[:2000]
                if uncertain:
                    product.status = ProductStatus.RECONCILIATION_REQUIRED
                    code = ReviewReasonCode.PUBLICATION_RECONCILIATION
                    outcome = "reconciliation_required"
                else:
                    product.status = ProductStatus.NEEDS_REVIEW
                    code = ReviewReasonCode.PUBLICATION_FAILED
                    outcome = "needs_review"
                set_product_review_reason(product, code, detail=product.processing_error)
                product.review_notified_at = None
                await set_product_message_outcome(
                    session, product, outcome, product.processing_error
                )
        logger.exception(
            "publication recovery failed",
            extra={
                "source_id": None,
                "telegram_message_id": None,
                "grouped_id": None,
                "product_id": product_id,
                "emoda_code": None,
            },
        )

    async def process_next(self) -> bool:
        try:
            work = await self._claim_work()
        except Exception as exc:
            if not transient_database_error(exc):
                raise
            logger.warning("transient claim conflict; durable work will be retried", exc_info=True)
            return False
        if work is None:
            return False
        product_id, source_id, text, media_paths, media_types = work
        try:
            async with self.sessions() as session:
                product = await session.get(Product, product_id)
                source = await session.get(Source, source_id)
                if product is None or source is None:
                    return True
                profile = SourceProfileData.model_validate(source.profile.config)
                global_mode_setting = await session.get(GlobalSetting, "default_publication_mode")
                global_mode = (
                    PublicationMode(str(global_mode_setting.value["value"]))
                    if global_mode_setting
                    else PublicationMode.REVIEW
                )
                currency_alias_rows = list(
                    await session.scalars(
                        select(CurrencyAlias).where(CurrencyAlias.enabled.is_(True))
                    )
                )
                currency_aliases = CurrencyAliasResolver(
                    {item.alias: item.currency for item in currency_alias_rows}
                )
                rates_setting = await session.get(GlobalSetting, "currency_rates")
                destination_prices = await session.get(GlobalSetting, "destination_price_policies")
                destination_price_policies = destination_prices.value if destination_prices else {}
                currency_rates = {
                    k: Decimal(str(v))
                    for k, v in (rates_setting.value if rates_setting else {}).items()
                }
                markup_rows = list(
                    await session.scalars(
                        select(MarkupRule)
                        .where(MarkupRule.enabled.is_(True))
                        .order_by(MarkupRule.priority)
                    )
                )
                markup = [
                    MarkupRange.model_validate(
                        {
                            "min_price": row.min_price,
                            "max_price": row.max_price,
                            "markup": row.markup,
                            "markup_type": row.markup_type,
                            "priority": row.priority,
                            "enabled": row.enabled,
                        }
                    )
                    for row in markup_rows
                ]
                examples_rows = list(
                    await session.scalars(
                        select(SourceExample)
                        .where(
                            SourceExample.source_id == source_id,
                            SourceExample.enabled.is_(True),
                        )
                        .order_by(SourceExample.updated_at.desc())
                        .limit(profile.gemini_example_limit)
                    )
                )
                examples = [
                    {
                        "source_id": source_id,
                        "input": item.original_input,
                        "expected": item.expected_values,
                        "comment": item.admin_comment or "",
                    }
                    for item in examples_rows
                ]
                result = await process_product(
                    self.pipeline,
                    PipelineInput(
                        source_id=source_id,
                        text=text,
                        media_paths=media_paths,
                        media_types=media_types,
                        emoda_code=product.emoda_code,
                        profile=profile,
                        markup_rules=markup,
                        examples=examples,
                        currency_aliases=currency_aliases,
                        currency_rates=currency_rates,
                        destination_price_policies=destination_price_policies,
                    ),
                    ProcessingMode.LIVE,
                )
                if await self._processing_was_superseded(session, product):
                    # A supplier edit or another durable recovery action changed the
                    # row while the slow pipeline was running.  Its result belongs to
                    # the old message version and must never be published.
                    await session.rollback()
                    return True
                if result.ignored or result.publication_blocked_reason:
                    durable_review_reason = product.processing_error
                    if result.publication_blocked_reason and product.published_at:
                        durable_review_reason = (
                            "media_required: supplier edit removed all publication media"
                        )
                        product.processing_error = durable_review_reason
                        set_product_review_reason(
                            product,
                            ReviewReasonCode.PUBLICATION_FAILED,
                            detail=durable_review_reason,
                            field="media",
                        )
                    product.original_text = text
                    product.cleaned_text = result.parsed.cleaned_text
                    if not product.published_at:
                        product.final_text = ""
                    product.parsed_result = result.model_dump(mode="json")
                    product.destination_id = result.parsed.destination_id
                    product.review_notified_at = None
                    if durable_review_reason:
                        product.status = ProductStatus.NEEDS_REVIEW
                        await set_product_message_outcome(
                            session,
                            product,
                            "needs_review",
                            durable_review_reason,
                        )
                    else:
                        product.processing_error = None
                        product.review_reasons = []
                        product.status = ProductStatus.SKIPPED
                        await set_product_message_outcome(
                            session,
                            product,
                            "skipped",
                            result.publication_blocked_reason or result.ignore_reason,
                        )
                    await session.commit()
                    return True
                durable_review_reason = product.processing_error
                # A reprocessed publication updates itself in place; only a durable
                # reason recorded upstream still demands an admin decision.
                force_review = bool(durable_review_reason)
                product.original_text = text
                product.cleaned_text = result.parsed.cleaned_text
                product.final_text = result.rendered_text
                product.parsed_result = result.model_dump(mode="json")
                product.destination_id = (
                    result.parsed.destination_id or profile.default_destination_id
                )
                publication_mode = resolve_publication_mode(
                    profile,
                    global_mode,
                )
                if result.price:
                    product.raw_price = result.price.raw_price
                    product.raw_currency = result.price.raw_currency
                    product.normalized_usd = result.price.normalized_usd
                    product.applied_markup = result.price.markup
                    product.final_price = result.price.final_usd
                else:
                    # Supplier edits can remove or invalidate a formerly resolved
                    # price. Never retain numeric columns that contradict the
                    # current four-line renderer's Check value or multi-price text.
                    product.raw_price = None
                    product.raw_currency = None
                    product.normalized_usd = None
                    product.applied_markup = None
                    product.final_price = None
                if result.supplier_price is not None:
                    product.raw_price = result.supplier_price.raw_amount
                    product.raw_currency = result.supplier_price.raw_currency
                    # USD columns cannot store lira/euro totals. The complete
                    # exact quote and policy remain in parsed_result.
                    # final_price is an integer USD column; keep fractional
                    # supplier quotes exclusively in their typed JSON snapshot.
                product.processing_error = durable_review_reason if force_review else None
                structured_reasons = classify_review_messages(result.review_reasons)
                if durable_review_reason:
                    structured_reasons.extend(classify_review_messages([durable_review_reason]))
                if publication_mode is PublicationMode.REVIEW:
                    structured_reasons.append(
                        reason(
                            ReviewReasonCode.SOURCE_PUBLICATION_MODE_REVIEW,
                            detail="source is configured for manual approval",
                        )
                    )
                unique_reason_keys = dict.fromkeys(
                    (item.code, item.detail, item.field) for item in structured_reasons
                )
                product.review_reasons = [
                    reason(code, detail=detail, field=field).model_dump(mode="json")
                    for code, detail, field in unique_reason_keys
                ]
                product.status = (
                    ProductStatus.NEEDS_REVIEW
                    if force_review
                    or result.needs_review
                    or publication_mode is PublicationMode.REVIEW
                    else ProductStatus.READY_TO_PUBLISH
                )
                outcome_reason = durable_review_reason or "; ".join(result.review_reasons) or None
                if (
                    product.status is ProductStatus.NEEDS_REVIEW
                    and outcome_reason is None
                    and publication_mode is PublicationMode.REVIEW
                ):
                    outcome_reason = "source publication mode is REVIEW"
                await set_product_message_outcome(
                    session,
                    product,
                    product.status.value,
                    outcome_reason,
                )
                await session.commit()
                if product.status is ProductStatus.READY_TO_PUBLISH:
                    await self._enqueue_and_publish(product.id, media_paths)
        except Exception as exc:
            async with self.sessions() as session, session.begin():
                product = await session.get(Product, product_id)
                if product:
                    product.status = ProductStatus.NEEDS_REVIEW
                    product.processing_error = str(exc)[:2000]
                    product.review_reasons = [
                        reason(
                            ReviewReasonCode.SYSTEM_ERROR,
                            detail=product.processing_error,
                        ).model_dump(mode="json")
                    ]
                    product.review_notified_at = None
                    await set_product_message_outcome(
                        session, product, "needs_review", product.processing_error
                    )
            logger.exception(
                "product processing failed",
                extra={
                    "source_id": source_id,
                    "telegram_message_id": None,
                    "grouped_id": None,
                    "product_id": product_id,
                    "emoda_code": None,
                },
            )
        return True

    @staticmethod
    async def _processing_was_superseded(session: AsyncSession, product: Product) -> bool:
        await lock_source(session, product.source_id)
        await session.refresh(
            product,
            attribute_names=["status", "processing_error"],
            with_for_update=True,
        )
        return product.status is not ProductStatus.PROCESSING

    async def _claim_work(self) -> tuple[int, int, str, list[str], list[str]] | None:
        async with self.sessions() as session, session.begin():
            now = datetime.now(UTC)
            # Pick a source without row locks, then take the common source guard
            # before album/product/message locks. Downloads never hold this guard.
            ready_products = select(Product.source_id.label("source_id"), Product.updated_at.label("ready_at")).where(
                Product.status == ProductStatus.READY,
                or_(Product.album_group_id.is_(None), exists(select(AlbumGroup.id).where(
                    AlbumGroup.id == Product.album_group_id, AlbumGroup.ready_at <= now,
                    AlbumGroup.status.in_([AlbumStatus.COLLECTING, AlbumStatus.READY])))),
                ~exists(select(SourceMessage.id).where(SourceMessage.media_pending.is_(True),
                    or_(SourceMessage.id == Product.source_message_id,
                        SourceMessage.album_group_id == Product.album_group_id))))
            ready_albums = select(AlbumGroup.source_id.label("source_id"), AlbumGroup.ready_at.label("ready_at")).where(
                AlbumGroup.status.in_([AlbumStatus.COLLECTING, AlbumStatus.READY]),
                AlbumGroup.ready_at <= now,
                or_(AlbumGroup.product_id.is_(None), exists(select(Product.id).where(
                    Product.id == AlbumGroup.product_id, Product.status == ProductStatus.READY))),
                ~exists(select(SourceMessage.id).where(SourceMessage.album_group_id == AlbumGroup.id,
                    SourceMessage.media_pending.is_(True))))
            ready_messages = select(SourceMessage.source_id.label("source_id"),
                func.coalesce(SourceMessage.processing_after, SourceMessage.telegram_date).label("ready_at")).where(
                SourceMessage.grouped_id.is_(None), SourceMessage.album_group_id.is_(None),
                SourceMessage.processed_at.is_(None), SourceMessage.media_pending.is_(False),
                or_(SourceMessage.processing_after.is_(None),SourceMessage.processing_after<=now),
                ~exists(select(Product.id).where(Product.source_message_id == SourceMessage.id)))
            eligible = union_all(ready_products, ready_albums, ready_messages).subquery()
            candidate = await session.scalar(select(eligible.c.source_id)
                .join(Source, Source.id == eligible.c.source_id)
                .where(Source.archived_at.is_(None))
                .order_by(eligible.c.ready_at, eligible.c.source_id).limit(1))
            if candidate is None or not await lock_source(session, candidate, wait=False):
                return None
            await AlbumCoordinator(Decimal("1.8")).claim_ready(session, now=now, limit=100, source_id=candidate)
            ready_product = await session.scalar(
                select(Product)
                .where(
                    Product.status == ProductStatus.READY,
                    Product.source_id == candidate,
                    or_(
                        Product.album_group_id.is_(None),
                        exists(
                            select(AlbumGroup.id).where(
                                AlbumGroup.id == Product.album_group_id,
                                AlbumGroup.status == AlbumStatus.READY,
                            )
                        ),
                    ),
                    ~exists(
                        select(SourceMessage.id).where(
                            SourceMessage.media_pending.is_(True),
                            or_(
                                SourceMessage.id == Product.source_message_id,
                                SourceMessage.album_group_id == Product.album_group_id,
                            ),
                        )
                    ),
                )
                .order_by(Product.updated_at)
                .limit(1)
                .with_for_update(skip_locked=True)
            )
            messages: list[SourceMessage]
            album: AlbumGroup | None = None
            existing: Product | None
            if ready_product:
                product = ready_product
                product.status = ProductStatus.PROCESSING
                if product.album_group_id:
                    album = await session.get(AlbumGroup, product.album_group_id)
                    if album is None or album.source_id != product.source_id:
                        product.status = ProductStatus.NEEDS_REVIEW
                        product.processing_error = "product album source identity mismatch"
                        product.review_notified_at = None
                        set_product_review_reason(
                            product,
                            ReviewReasonCode.SYSTEM_ERROR,
                            detail=product.processing_error,
                        )
                        return None
                    messages = list(
                        await session.scalars(
                            select(SourceMessage)
                            .where(
                                SourceMessage.album_group_id == product.album_group_id,
                                SourceMessage.source_id == product.source_id,
                            )
                            .order_by(SourceMessage.telegram_message_id)
                        )
                    )
                elif product.source_message_id:
                    single = await session.get(SourceMessage, product.source_message_id)
                    messages = (
                        [single]
                        if single is not None and single.source_id == product.source_id
                        else []
                    )
                else:
                    messages = []
                existing = product
            else:
                group = await session.scalar(
                    select(AlbumGroup)
                    .where(
                        AlbumGroup.status == AlbumStatus.READY,
                        AlbumGroup.source_id == candidate,
                        or_(
                            AlbumGroup.product_id.is_(None),
                            exists(
                                select(Product.id).where(
                                    Product.id == AlbumGroup.product_id,
                                    Product.status == ProductStatus.READY,
                                )
                            ),
                        ),
                        ~exists(
                            select(SourceMessage.id).where(
                                SourceMessage.album_group_id == AlbumGroup.id,
                                SourceMessage.media_pending.is_(True),
                            )
                        ),
                    )
                    .order_by(AlbumGroup.ready_at)
                    .limit(1)
                    .with_for_update(skip_locked=True)
                )
                if group:
                    album = group
                    messages = list(
                        await session.scalars(
                            select(SourceMessage)
                            .where(
                                SourceMessage.album_group_id == group.id,
                                SourceMessage.source_id == group.source_id,
                            )
                            .order_by(SourceMessage.telegram_message_id)
                        )
                    )
                    existing = await session.scalar(
                        select(Product).where(
                            Product.album_group_id == group.id,
                            Product.source_id == group.source_id,
                        )
                    )
                else:
                    message = await session.scalar(
                        select(SourceMessage)
                        .where(
                            SourceMessage.source_id == candidate,
                            SourceMessage.grouped_id.is_(None),
                            SourceMessage.album_group_id.is_(None),
                            SourceMessage.processed_at.is_(None),
                            SourceMessage.media_pending.is_(False),
                            or_(
                                SourceMessage.processing_after.is_(None),
                                SourceMessage.processing_after <= now,
                            ),
                            ~exists(
                                select(Product.id).where(
                                    Product.source_message_id == SourceMessage.id
                                )
                            ),
                        )
                        .order_by(SourceMessage.telegram_date, SourceMessage.telegram_message_id)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                    if message is None:
                        return None
                    messages = [message]
                    existing = None
            if not messages:
                if ready_product:
                    ready_product.status = ProductStatus.NEEDS_REVIEW
                    ready_product.processing_error = "product has no source messages"
                    ready_product.review_notified_at = None
                    set_product_review_reason(
                        ready_product,
                        ReviewReasonCode.SYSTEM_ERROR,
                        detail=ready_product.processing_error,
                    )
                    return None
                return None
            first = messages[0]
            if album is not None and first.source_id != album.source_id:
                raise ValueError("claimed album contains a message from a different source")
            if any(message.source_id != first.source_id for message in messages):
                raise ValueError("claimed product contains messages from different sources")
            source = await session.get(Source, first.source_id)
            profile = (
                SourceProfileData.model_validate(source.profile.config)
                if source
                else SourceProfileData()
            )
            if existing is not None:
                available_message_ids = set(
                    await session.scalars(
                        select(MediaItem.source_message_id).where(
                            MediaItem.source_message_id.in_([item.id for item in messages]),
                            MediaItem.active.is_(True),
                            MediaItem.local_deleted_at.is_(None),
                        )
                    )
                )
                evicted_message_ids = set(
                    await session.scalars(
                        select(MediaItem.source_message_id).where(
                            MediaItem.source_message_id.in_([item.id for item in messages]),
                            MediaItem.active.is_(True),
                            MediaItem.local_deleted_at.is_not(None),
                        )
                    )
                )
                needs_restore = evicted_message_ids - available_message_ids
                if needs_restore:
                    for item in messages:
                        if item.id in needs_restore:
                            item.media_pending = True
                    existing.status = ProductStatus.READY
                    return None
            media_rows = list(
                await session.scalars(
                    select(MediaItem)
                    .where(
                        MediaItem.source_message_id.in_([item.id for item in messages]),
                        MediaItem.active.is_(True),
                        MediaItem.local_deleted_at.is_(None),
                    )
                    .order_by(MediaItem.telegram_message_id, MediaItem.position)
                )
            )
            collection_engine = CollectionEngine(profile.effective_collection())
            text = collection_engine.merged_text(
                [
                    CollectionMessage(
                        message_id=item.telegram_message_id,
                        occurred_at=item.telegram_date,
                        grouped_id=item.grouped_id,
                        text=item.text,
                        caption=item.caption,
                        media_count=sum(media.source_message_id == item.id for media in media_rows),
                    )
                    for item in messages
                ]
            )
            if existing is None and profile.deduplicate_exact_products and text.strip():
                duplicate = await self._exact_duplicate(session, first.source_id, text, media_rows)
                if duplicate is not None:
                    duplicate.parsed_result = {
                        **duplicate.parsed_result,
                        "duplicate_count": int(duplicate.parsed_result.get("duplicate_count", 0))
                        + 1,
                        "last_duplicate_message_id": first.telegram_message_id,
                    }
                    for item in messages:
                        item.processed_at = now
                        item.raw_metadata = {
                            **item.raw_metadata,
                            "duplicate_product_id": duplicate.id,
                        }
                        set_message_outcome(
                            item,
                            "duplicate",
                            f"existing product_id={duplicate.id}; code={duplicate.emoda_code}",
                        )
                    if album:
                        album.status = AlbumStatus.PROCESSED
                    return None
            if existing is None:
                setting = await session.get(GlobalSetting, "emoda_code")
                config = setting.value if setting else {"format": "alpha2_numeric5"}
                if config.get("format") == "alpha2_numeric5":
                    code = await CodeAllocator().next_alpha2_numeric5(session)
                else:
                    width_value = config.get("width", 6)
                    if not isinstance(width_value, (str, int)):
                        raise ValueError("invalid E-MODA code width setting")
                    code = await CodeAllocator().next(
                        session,
                        prefix=str(config.get("prefix", "EM")),
                        width=int(width_value),
                    )
                product = Product(
                    emoda_code=code,
                    source_id=first.source_id,
                    source_message_id=None if album else first.id,
                    album_group_id=album.id if album else None,
                    status=ProductStatus.PROCESSING,
                    original_text=text,
                )
                session.add(product)
                await session.flush()
                if album:
                    album.product_id = product.id
            else:
                product = existing
                product.status = ProductStatus.PROCESSING
            for message in messages:
                message.processing_started_at = datetime.now(UTC)
                message.processed_at = datetime.now(UTC)
                set_message_outcome(
                    message,
                    "attached_to_product",
                    f"product_id={product.id}",
                    update_current=False,
                )
                set_message_outcome(message, "processing")
            if any(message.raw_metadata.get("media_download_error") for message in messages):
                product.processing_error = "source media download failed; review required"
                set_product_review_reason(
                    product,
                    ReviewReasonCode.MEDIA_DOWNLOAD_FAILED,
                    detail=product.processing_error,
                )
            if album:
                album.status = AlbumStatus.PROCESSED
            paths = [str(self.media_root / row.storage_path) for row in media_rows]
            media_types = [row.media_type for row in media_rows]
            return product.id, product.source_id, text, paths, media_types

    @staticmethod
    async def _exact_duplicate(
        session: AsyncSession,
        source_id: int,
        text: str,
        media_rows: list[MediaItem],
    ) -> Product | None:
        # Unknown hashes cannot prove identity. Article alone is never sufficient.
        if any(not row.sha256 for row in media_rows):
            return None
        signature = [(row.media_type, row.sha256) for row in media_rows]
        candidates = list(
            await session.scalars(
                select(Product)
                .where(
                    Product.source_id == source_id,
                    Product.original_text == text,
                )
                .order_by(Product.id)
            )
        )
        for candidate in candidates:
            old_media = list(
                await session.scalars(
                    select(MediaItem)
                    .join(
                        SourceMessage,
                        SourceMessage.id == MediaItem.source_message_id,
                    )
                    .where(
                        SourceMessage.source_id == source_id,
                        MediaItem.active.is_(True),
                        or_(
                            SourceMessage.id == candidate.source_message_id,
                            SourceMessage.album_group_id == candidate.album_group_id
                            if candidate.album_group_id is not None
                            else false(),
                        ),
                    )
                    .order_by(MediaItem.telegram_message_id, MediaItem.position)
                )
            )
            if signature == [(row.media_type, row.sha256) for row in old_media]:
                return candidate
        return None

    async def _media_preflight(
        self,
        session: AsyncSession,
        product: Product,
        media_paths: list[str],
    ) -> tuple[ReviewReasonCode, str] | None:
        """Check the full durable source media set before any Telegram send."""
        if product.source_message_id is not None:
            linked = SourceMessage.id == product.source_message_id
        elif product.album_group_id is not None:
            linked = SourceMessage.album_group_id == product.album_group_id
        else:
            linked = false()
        linked_messages = list(
            await session.scalars(
                select(SourceMessage).where(
                    SourceMessage.source_id == product.source_id,
                    linked,
                )
            )
        )
        unavailable_ids = [
            message.telegram_message_id
            for message in linked_messages
            if (message.raw_metadata or {}).get("media_download_error")
        ]
        if unavailable_ids:
            labels = ", ".join(str(item) for item in unavailable_ids[:3])
            if len(unavailable_ids) > 3:
                labels += f" и ещё {len(unavailable_ids) - 3}"
            return (
                ReviewReasonCode.MEDIA_DOWNLOAD_FAILED,
                f"media_download_failed: медиа исходного Telegram-сообщения {labels} "
                "недоступно; публикация заблокирована",
            )
        rows = list(
            await session.scalars(
                select(MediaItem)
                .join(SourceMessage, SourceMessage.id == MediaItem.source_message_id)
                .where(
                    SourceMessage.source_id == product.source_id,
                    linked,
                    MediaItem.active.is_(True),
                )
                .order_by(MediaItem.telegram_message_id, MediaItem.position)
            )
        )
        expected_paths = [str(self.media_root / row.storage_path) for row in rows]
        present = await asyncio.to_thread(_media_files_present, expected_paths)
        unavailable = [
            path
            for row, path, exists_on_disk in zip(rows, expected_paths, present, strict=True)
            if row.local_deleted_at is not None or not exists_on_disk
        ]
        if unavailable:
            return (
                ReviewReasonCode.MEDIA_DOWNLOAD_FAILED,
                f"media_download_failed: {len(unavailable)} of {len(rows)} source media "
                "files are unavailable on disk; publication blocked",
            )
        if expected_paths != media_paths:
            return (
                ReviewReasonCode.SYSTEM_ERROR,
                "source media set changed before publication; publication blocked for review",
            )
        return None

    @staticmethod
    async def _block_media_publication(
        session: AsyncSession,
        product: Product,
        failure: tuple[ReviewReasonCode, str],
    ) -> None:
        code, detail = failure
        product.status = ProductStatus.NEEDS_REVIEW
        product.processing_error = detail
        product.review_notified_at = None
        set_product_review_reason(product, code, detail=detail, field="media", append=True)
        await set_product_message_outcome(session, product, "needs_review", detail)

    async def _enqueue_and_publish(
        self,
        product_id: int,
        media_paths: list[str],
        *,
        review_approved: bool = False,
    ) -> PublicationOutbox | None:
        # Archive also blocks already collected work and stale REVIEW buttons.
        async with self.sessions() as session:
            source = await session.scalar(select(Source).join(Product, Product.source_id == Source.id)
                .where(Product.id == product_id))
            if source is not None and source.archived_at is not None:
                return None
        # Final policy guard also covers old READY rows and explicit admin approval.
        if not media_paths:
            async with self.sessions() as session, session.begin():
                product = await session.get(Product, product_id, with_for_update=True)
                if product:
                    product.parsed_result = {
                        **product.parsed_result,
                        "publication_blocked_reason": "media_required",
                    }
                    if product.published_at:
                        product.status = ProductStatus.NEEDS_REVIEW
                        product.processing_error = (
                            "media_required: supplier edit removed all publication media"
                        )
                        set_product_review_reason(
                            product,
                            ReviewReasonCode.PUBLICATION_FAILED,
                            detail=product.processing_error,
                            field="media",
                        )
                        product.review_notified_at = None
                    else:
                        product.status = ProductStatus.SKIPPED
                        product.processing_error = None
                        product.review_reasons = []
                    await set_product_message_outcome(
                        session, product, product.status.value, "media_required: no photo or video"
                    )
            return None
        if self.publication is None:
            async with self.sessions() as session, session.begin():
                product = await session.get(Product, product_id)
                if product:
                    product.status = ProductStatus.NEEDS_REVIEW
                    product.processing_error = "publisher is not configured"
                    set_product_review_reason(
                        product,
                        ReviewReasonCode.PUBLICATION_FAILED,
                        detail=product.processing_error,
                    )
            return None
        async with self.sessions() as session:
            product = await session.get(Product, product_id)
            if product is None or product.destination_id is None:
                if product:
                    product.status = ProductStatus.NEEDS_REVIEW
                    product.processing_error = "destination unresolved"
                    set_product_review_reason(
                        product,
                        ReviewReasonCode.DESTINATION_MISSING,
                        detail=product.processing_error,
                    )
                    await session.commit()
                return None
            media_failure = await self._media_preflight(session, product, media_paths)
            if media_failure is not None:
                await self._block_media_publication(session, product, media_failure)
                await session.commit()
                return None
            destination = await session.get(Destination, product.destination_id)
            assert destination is not None
            try:
                validate_publication_text(
                    product.final_text,
                    code=product.emoda_code,
                    publication_format=product.parsed_result.get("publication_format", "standard"),
                )
            except ValueError as exc:
                product.status = ProductStatus.NEEDS_REVIEW
                product.processing_error = f"publication payload invariant failed: {exc}"[:2000]
                set_product_review_reason(
                    product,
                    ReviewReasonCode.SYSTEM_ERROR,
                    detail=product.processing_error,
                )
                product.review_notified_at = None
                await set_product_message_outcome(
                    session, product, "needs_review", product.processing_error
                )
                await session.commit()
                return None
            buttons_setting = await session.get(GlobalSetting, PUBLICATION_BUTTONS_SETTING_KEY)
            keyboard = publication_keyboard(
                buttons_setting.value if buttons_setting else None,
                bot_username=self.bot_username,
                emoda_code=product.emoda_code,
            )
            original_outbox = None
            if product.published_at:
                original_outbox = await session.scalar(
                    select(PublicationOutbox)
                    .where(
                        PublicationOutbox.product_id == product.id,
                        PublicationOutbox.status == OutboxStatus.PUBLISHED,
                    )
                    .order_by(PublicationOutbox.published_at.desc())
                    .limit(1)
                )
            payload: dict[str, object] = {
                "emoda_code": product.emoda_code,
                "text": product.final_text,
                "media_paths": media_paths,
            }
            publication_format = product.parsed_result.get("publication_format", "standard")
            if publication_format != "standard":
                payload["publication_format"] = publication_format
            if original_outbox:
                payload["operation"] = "edit"
            if original_outbox is not None and _publication_is_unchanged(
                original_outbox, payload, keyboard
            ):
                # The same edit can arrive again live and through catch-up. When the
                # rendered publication is identical, the live post stays untouched:
                # no outbox row, no Telegram call, no duplicate.
                product.status = ProductStatus.PUBLISHED
                product.processing_error = None
                product.review_reasons = []
                await set_product_message_outcome(
                    session,
                    product,
                    "published",
                    "supplier edit did not change the publication",
                )
                await session.commit()
                return original_outbox
            if original_outbox is not None and review_approved:
                delivered_media = _media_path_tokens(original_outbox.payload.get("media_paths", []))
                if len(delivered_media) != len(media_paths):
                    # A late album member means the existing destination post is a
                    # partial product.  Explicit approval replaces its exact stored
                    # Telegram IDs with the complete canonical media set.
                    payload["operation"] = "replace"
                    payload["replaces_outbox_id"] = original_outbox.id
            outbox = await self.publication.create(
                session,
                product=product,
                destination=destination,
                payload=payload,
                keyboard=keyboard,
            )
            if original_outbox:
                if payload.get("operation") == "replace":
                    return await self.publication.replace(session, outbox.id, original_outbox.id)
                return await self.publication.update(session, outbox.id, original_outbox.id)
            return await self.publication.publish(session, outbox.id)

    async def _review_result(
        self,
        session: AsyncSession,
        product: Product,
        outcome: str,
        *,
        outbox_id: int | None = None,
    ) -> ReviewPublishResult:
        if outbox_id is None:
            outbox_id = await session.scalar(
                select(PublicationOutbox.id)
                .where(PublicationOutbox.product_id == product.id)
                .order_by(PublicationOutbox.created_at.desc(), PublicationOutbox.id.desc())
                .limit(1)
            )
        normalized_outcome = outcome
        if normalized_outcome not in {
            "published",
            "already_published",
            "in_progress",
            "failed",
        }:
            raise ValueError(f"invalid review publication outcome: {outcome}")
        return ReviewPublishResult(
            product_id=product.id,
            outcome=normalized_outcome,  # type: ignore[arg-type]
            status=product.status,
            outbox_id=outbox_id,
            detail=product.processing_error,
        )

    async def publish_reviewed(
        self,
        product_id: int,
        expected_token: str | None = None,
        *,
        resume: bool = False,
    ) -> ReviewPublishResult:
        review_approved = False
        async with self.sessions() as session:
            product = await session.scalar(
                select(Product).where(Product.id == product_id).with_for_update()
            )
            if product is None:
                raise LookupError("product not found")
            if product.status is ProductStatus.PUBLISHED:
                return await self._review_result(session, product, "already_published")
            if product.status in {ProductStatus.READY_TO_PUBLISH, ProductStatus.PUBLISHING}:
                if not resume or product.status is ProductStatus.PUBLISHING:
                    return await self._review_result(session, product, "in_progress")
            elif product.status is ProductStatus.NEEDS_REVIEW:
                if expected_token is not None and expected_token != review_approval_token(product):
                    raise StaleReviewApproval("product changed after this REVIEW card was rendered")
                review_approved = True
            else:
                if expected_token is not None:
                    raise StaleReviewApproval(
                        f"product status changed after this REVIEW card was rendered: {product.status}"
                    )
                raise ValueError(f"product cannot be published from {product.status}")
            message_ids: list[int]
            if product.source_message_id:
                message_ids = [product.source_message_id]
            elif product.album_group_id:
                message_ids = list(
                    await session.scalars(
                        select(SourceMessage.id).where(
                            SourceMessage.album_group_id == product.album_group_id,
                            SourceMessage.source_id == product.source_id,
                        )
                    )
                )
            else:
                message_ids = []
            rows = (
                list(
                    await session.scalars(
                        select(MediaItem)
                        .where(
                            MediaItem.source_message_id.in_(message_ids),
                            MediaItem.active.is_(True),
                            MediaItem.local_deleted_at.is_(None),
                        )
                        .order_by(MediaItem.telegram_message_id, MediaItem.position)
                    )
                )
                if message_ids
                else []
            )
            paths = [str(self.media_root / row.storage_path) for row in rows]
            media_failure = await self._media_preflight(session, product, paths)
            if media_failure is not None:
                await self._block_media_publication(session, product, media_failure)
                await session.commit()
                return ReviewPublishResult(
                    product_id=product.id,
                    outcome="failed",
                    status=product.status,
                    detail=product.processing_error,
                )
            # Admin approval acknowledges the current review reason. Any newer
            # supplier edit arriving after this commit writes a fresh reason,
            # which publication completion preserves instead of overwriting.
            product.processing_error = None
            product.review_reasons = []
            product.status = ProductStatus.READY_TO_PUBLISH
            await session.commit()
        outbox = await self._enqueue_and_publish(
            product_id,
            paths,
            review_approved=review_approved,
        )
        async with self.sessions() as session:
            product = await session.get(Product, product_id)
            if product is None:
                raise LookupError("product not found")
            if product.status is ProductStatus.PUBLISHED:
                outcome = "published"
            elif product.status in {ProductStatus.READY_TO_PUBLISH, ProductStatus.PUBLISHING}:
                outcome = "in_progress"
            else:
                outcome = "failed"
            return await self._review_result(
                session,
                product,
                outcome,
                outbox_id=outbox.id if outbox is not None else None,
            )
