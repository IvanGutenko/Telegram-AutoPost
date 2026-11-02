from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
from datetime import UTC, datetime
from html import escape
from pathlib import Path
from typing import Any

from aiogram import Bot, Dispatcher
from aiogram.exceptions import TelegramBadRequest, TelegramRetryAfter
from aiogram.types import FSInputFile, InlineKeyboardButton, InlineKeyboardMarkup
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from telethon import TelegramClient
from telethon.errors import FloodWaitError

from emoda.admin.router import create_admin_router
from emoda.catalog.router import create_catalog_router
from emoda.config import Settings
from emoda.db.models import AdminUser, GlobalSetting, MediaItem, Product, SourceMessage
from emoda.db.session import create_engine, create_session_factory
from emoda.domain.enums import ProductStatus
from emoda.integrations.admin_delivery import AdminChatLimiter
from emoda.integrations.collector import CollectorService
from emoda.integrations.gemini import GoogleGeminiAdapter
from emoda.integrations.publisher import TelegramBotPublisher, input_media
from emoda.integrations.session import secure_session_path
from emoda.integrations.storage import LocalMediaStorage
from emoda.logging import configure_logging
from emoda.services.admin_delivery import (
    ReviewDeliveryProgress,
    delivery_stopped,
    review_delivery_key,
)
from emoda.services.media_retention import MediaRetentionService
from emoda.services.processor import ProcessorService
from emoda.services.publication import PublicationService
from emoda.services.review import review_approval_token, review_reason_details

logger = logging.getLogger(__name__)


def _all_review_media_files_exist(media: list[tuple[str, str]]) -> bool:
    try:
        return all(Path(path).is_file() and Path(path).stat().st_size > 0 for path, _media_type in media)
    except OSError:
        return False


async def deliver_review_notification(
    bot: Any,
    admin_id: int,
    product: Product,
    media: list[tuple[str, str]],
    markup: InlineKeyboardMarkup,
    *,
    media_client: Any = None,
) -> None:
    """Show persisted product media together with every automatic REVIEW card."""

    body = (
        f"<b>Товар требует проверки: {escape(product.emoda_code)}</b>\n\n"
        + escape(
            (
                "; ".join(
                    review_reason_details(item)
                    for item in (getattr(product, "review_reasons", None) or [])
                )
                or getattr(product, "processing_error", None)
                or "Причина не сохранена в старой записи"
            )[:650]
        )
        + "\n\n"
        f"{product.final_text[:3000]}"
    )
    if media and not await asyncio.to_thread(_all_review_media_files_exist, media):
        # An old retention record can still reference a missing local file. Show
        # one complete text-only card rather than sending a partial album first.
        body += (
            "\n\n⚠️ Часть медиа недоступна. Публикация заблокирована до восстановления."
        )
        await bot.send_message(admin_id, body, parse_mode="HTML", reply_markup=markup)
        return
    if not media:
        await bot.send_message(admin_id, body, parse_mode="HTML", reply_markup=markup)
        return
    if media_client is not None and TelegramBotPublisher.requires_mtproto_batch(media):
        for offset in range(0, len(media), 10):
            batch = media[offset : offset + 10]
            paths = [path for path, _media_type in batch]
            await media_client.send_file(
                admin_id,
                paths[0] if len(paths) == 1 else paths,
                supports_streaming=any(media_type == "video" for _path, media_type in batch),
            )
        # Callback buttons remain on a Bot API message; MTProto is used only for the
        # oversized bytes, so the review workflow and versioned callbacks stay unchanged.
        await bot.send_message(admin_id, body, parse_mode="HTML", reply_markup=markup)
        return
    if len(media) == 1 and len(body) <= 1024:
        path, media_type = media[0]
        send_media = bot.send_video if media_type == "video" else bot.send_photo
        await send_media(
            admin_id,
            FSInputFile(path),
            caption=body,
            parse_mode="HTML",
            reply_markup=markup,
        )
        return

    caption_attached = False
    for offset in range(0, len(media), 10):
        batch = media[offset : offset + 10]
        caption = body if not caption_attached and len(body) <= 1024 else None
        if len(batch) == 1:
            path, media_type = batch[0]
            send_media = bot.send_video if media_type == "video" else bot.send_photo
            await send_media(
                admin_id,
                FSInputFile(path),
                caption=caption,
                parse_mode="HTML" if caption else None,
            )
        else:
            album: list[Any] = []
            for index, (path, media_type) in enumerate(batch):
                item_caption = caption if index == 0 else None
                album.append(
                    input_media(
                        path,
                        media_type,
                        caption=item_caption,
                        parse_mode="HTML" if item_caption else None,
                    )
                )
            await bot.send_media_group(admin_id, album)
        caption_attached = caption_attached or caption is not None
    action_text = (
        f"Действия для <code>{escape(product.emoda_code)}</code>" if caption_attached else body
    )
    await bot.send_message(admin_id, action_text, parse_mode="HTML", reply_markup=markup)


def request_stop_on_task_exit(task: asyncio.Task[Any], stop: asyncio.Event) -> None:
    if stop.is_set():
        return
    task_name = task.get_name()
    if task.cancelled():
        logger.error("runtime task %s was cancelled unexpectedly", task_name)
    else:
        error = task.exception()
        if error is None:
            logger.error("runtime task %s exited unexpectedly", task_name)
        else:
            logger.error(
                "runtime task %s failed: %s",
                task_name,
                error,
                exc_info=(type(error), error, error.__traceback__),
            )
    stop.set()


async def periodic_catchup(
    collector: Any,
    stop: asyncio.Event,
    *,
    interval_seconds: float,
    recovery_complete: asyncio.Event | None = None,
    immediate: bool = False,
) -> None:
    if recovery_complete is not None:
        await recovery_complete.wait()
    while not stop.is_set():
        if not immediate:
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
                continue
            except TimeoutError:
                pass
        immediate = False
        try:
            await collector.catch_up()
        except Exception:
            logger.exception("periodic source catch-up failed")


async def processing_worker(
    processor: Any,
    collector: Any,
    stop: asyncio.Event,
    recovery_complete: asyncio.Event,
    health_file: Path,
    *,
    poll_seconds: float,
) -> None:
    """Recover durable jobs; source history runs independently of publication.

    Persisted media_pending flags and album readiness gate incomplete inputs.
    A slow or inaccessible source must not hold every destination queue.
    """
    await processor.recover_jobs()
    recovery_complete.set()
    async def process_queue() -> None:
        while not stop.is_set():
            handled = await processor.process_next()
            await asyncio.to_thread(health_file.touch)
            if not handled:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=poll_seconds)
                except TimeoutError:
                    pass

    # Claims are atomic in PostgreSQL; publication retains its destination lock
    # across the entire product. Slow extraction/upload must not stop other queues.
    async with asyncio.TaskGroup() as workers:
        for index in range(4):
            workers.create_task(process_queue(), name=f"processor-{index}")


async def periodic_health_heartbeat(
    health_file: Path,
    stop: asyncio.Event,
    *,
    interval_seconds: float = 30,
) -> None:
    while not stop.is_set():
        await asyncio.to_thread(health_file.touch)
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass


async def periodic_media_retention(
    retention: Any, stop: asyncio.Event, *, interval_seconds: float
) -> None:
    """Drain one small retention batch per interval without affecting other workers."""
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
            continue
        except TimeoutError:
            pass
        try:
            report = await retention.cleanup_batch()
            deleted = report.source_deleted_files + report.admin_deleted_files
            if deleted or report.missing_files or report.unsafe_files or report.failed_files:
                logger.info(
                    "media retention batch completed",
                    extra={
                        "deleted_files": deleted,
                        "logical_deleted_bytes": report.logical_deleted_bytes,
                        "physical_reclaimed_bytes": report.physical_reclaimed_bytes,
                        "missing_files": report.missing_files,
                        "unsafe_files": report.unsafe_files,
                        "failed_files": report.failed_files,
                        "pressure_cleanup_active": report.pressure_cleanup_active,
                        "disk_used_percent": report.disk_used_percent,
                    },
                )
        except Exception:
            logger.exception("periodic media retention failed")


async def notify_pending_reviews_once(
    sessions: Any, settings: Settings, bot: Any, media_client: Any,
    *, limiter: AdminChatLimiter | None = None,
) -> None:
    # Read everything needed for Telegram before closing the session. A send can
    # take seconds, and holding the connection here starves the other workers.
    async with sessions() as session:
        admins = set(settings.bootstrap_admin_ids)
        admins.update(
            await session.scalars(
                select(AdminUser.telegram_user_id).where(AdminUser.enabled.is_(True))
            )
        )
        pending: list[tuple[Product, list[tuple[str, str]]]] = []
        cursor: int | None = None
        products: list[Product] = []
        while len(products) < 20:
            query = select(Product).where(
                Product.status.in_([ProductStatus.NEEDS_REVIEW, ProductStatus.RECONCILIATION_REQUIRED]),
                Product.review_notified_at.is_(None),
            )
            if cursor is not None:
                query = query.where(Product.id > cursor)
            page = list(await session.scalars(query.order_by(Product.id).limit(20)))
            if not page:
                break
            cursor = page[-1].id
            keys = [review_delivery_key(admin_id, product) for product in page for admin_id in admins]
            states = {row.key: row.value for row in await session.scalars(
                select(GlobalSetting).where(GlobalSetting.key.in_(keys))
            )} if keys else {}
            for product in page:
                recipient_states = [states.get(review_delivery_key(admin_id, product), {}) for admin_id in admins]
                # Uncertain/terminal recipients remain visible in /admin, but
                # must not occupy the first 20 slots and starve newer sources.
                if not admins or all(state.get("complete") for state in recipient_states) or any(
                    not state.get("complete") and not delivery_stopped(state) for state in recipient_states
                ):
                    products.append(product)
                    if len(products) == 20:
                        break
        for product in products:
            message_scope = (
                SourceMessage.id == product.source_message_id
                if product.source_message_id is not None
                else SourceMessage.album_group_id == product.album_group_id
            )
            media_rows = list(
                await session.execute(
                    select(MediaItem.storage_path, MediaItem.media_type)
                    .join(SourceMessage, SourceMessage.id == MediaItem.source_message_id)
                    .where(
                        SourceMessage.source_id == product.source_id,
                        message_scope,
                        MediaItem.active.is_(True),
                        MediaItem.local_deleted_at.is_(None),
                    )
                    .order_by(MediaItem.telegram_message_id, MediaItem.position)
                )
            )
            media = [
                (str(settings.media_root / storage_path), media_type)
                for storage_path, media_type in media_rows
            ]
            session.expunge(product)
            pending.append((product, media))

    blocked_admins: set[int] = set()
    for product, review_media in pending:
        if product.status is ProductStatus.RECONCILIATION_REQUIRED:
            rows = [
                [
                    InlineKeyboardButton(
                        text="Подтвердить IDs", callback_data=f"reconcile:confirm:{product.id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="Разрешить retry", callback_data=f"reconcile:retry:{product.id}"
                    )
                ],
            ]
        else:
            rows = [
                [
                    InlineKeyboardButton(
                        text="Опубликовать",
                        callback_data=(
                            f"review:publish:{product.id}:" f"{review_approval_token(product)}"
                        ),
                    ),
                    InlineKeyboardButton(text="Изменить", callback_data=f"review:edit:{product.id}"),
                ],
                [InlineKeyboardButton(text="Пропустить", callback_data=f"review:skip:{product.id}")],
            ]
        markup = InlineKeyboardMarkup(inline_keyboard=rows)
        delivered = bool(admins)
        for admin_id in admins:
            if admin_id in blocked_admins or (limiter and limiter.remaining(admin_id)):
                delivered = False
                continue
            progress = await ReviewDeliveryProgress.load(
                sessions, review_delivery_key(admin_id, product)
            )
            if progress.state.get("complete"):
                continue
            if delivery_stopped(progress.state):
                delivered = False
                continue
            delivery_bot = progress.wrap(bot)
            try:
                if progress.state.get("invalid_media"):
                    progress.position = progress.state.get("steps", 0)
                    await delivery_bot.send_message(
                        admin_id,
                        f"<b>Товар требует проверки: {escape(product.emoda_code)}</b>\n\n"
                        f"{product.final_text[:3000]}\n\n"
                        "⚠️ Telegram отклонил медиа. Откройте карточку для проверки; автоматическая пересылка остановлена.",
                        parse_mode="HTML", reply_markup=markup,
                    )
                else:
                    try:
                        await deliver_review_notification(
                            delivery_bot, admin_id, product, review_media, markup,
                            media_client=progress.wrap(media_client) if media_client else None,
                        )
                    except TelegramBadRequest as exc:
                        if not any(value in str(exc).casefold() for value in (
                            "file must be non-empty", "image_process_failed", "video_content_type_invalid",
                            "wrong type of the web page content", "photo_invalid_dimensions",
                        )):
                            raise
                        await progress.save(invalid_media=True)
                        await delivery_bot.send_message(
                            admin_id,
                            f"<b>Товар требует проверки: {escape(product.emoda_code)}</b>\n\n"
                            f"{product.final_text[:3000]}\n\n"
                            "⚠️ Telegram отклонил медиа. Откройте карточку для проверки; автоматическая пересылка остановлена.",
                            parse_mode="HTML", reply_markup=markup,
                        )
                await progress.save(complete=True)
            except TelegramRetryAfter as exc:
                delivered = False
                blocked_admins.add(admin_id)
                logger.warning("review delivery paused by Telegram", extra={"admin_id": admin_id, "product_id": product.id, "retry_after": exc.retry_after})
            except Exception:
                # A blocked/invalid admin must not prevent delivery to others.
                delivered = False
                logger.exception("review delivery failed", extra={"admin_id": admin_id, "product_id": product.id})
        if delivered:
            # A product edited or handled during Telegram I/O must keep its new
            # notification state. The row lock exists only for this short
            # transaction, after the send has finished.
            async with sessions() as session, session.begin():
                current = await session.scalar(
                    select(Product)
                    .where(
                        Product.id == product.id,
                        Product.status == product.status,
                        Product.review_notified_at.is_(None),
                    )
                    .with_for_update()
                )
                if current is not None and review_approval_token(current) == review_approval_token(
                    product
                ):
                    current.review_notified_at = datetime.now(UTC)


async def review_notification_loop(
    sessions: Any,
    settings: Settings,
    bot: Any,
    media_client: Any,
    stop: asyncio.Event,
    *,
    interval_seconds: float = 5,
    limiter: AdminChatLimiter | None = None,
) -> None:
    while not stop.is_set():
        try:
            if limiter is None:
                await notify_pending_reviews_once(sessions, settings, bot, media_client)
            else:
                await notify_pending_reviews_once(sessions, settings, bot, media_client, limiter=limiter)
        except SQLAlchemyError:
            logger.exception("review notification database operation failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval_seconds)
        except TimeoutError:
            pass


class DeferredMediaClient:
    """Reuse bot authorization and honor Telegram login waits outside startup."""
    def __init__(self, client: Any, session_path: Path, *, limiter: AdminChatLimiter | None = None) -> None:
        self.client = client
        self.limiter = limiter
        self.session_path = session_path
        self.retry_path = session_path.with_suffix(".retry-after")
        self.ready = asyncio.Event()

    async def run(self, token: str, stop: asyncio.Event) -> None:
        await self.client.connect()
        if await self.client.is_user_authorized():
            self.ready.set()
        while not self.ready.is_set() and not stop.is_set():
            try:
                retry_at = float(self.retry_path.read_text())
            except (OSError, ValueError):
                retry_at = 0
            delay = max(0, retry_at - datetime.now(UTC).timestamp())
            if delay:
                logger.warning("MTProto bot login deferred by Telegram for %.0f seconds", delay)
                try:
                    await asyncio.wait_for(stop.wait(), timeout=delay)
                    return
                except TimeoutError:
                    pass
            try:
                await self.client.start(bot_token=token)
            except FloodWaitError as exc:
                self.retry_path.write_text(str(datetime.now(UTC).timestamp() + exc.seconds + 1))
                continue
            secure_session_path(self.session_path)
            self.retry_path.unlink(missing_ok=True)
            self.ready.set()
            logger.info("persistent MTProto bot session authorized")
        await stop.wait()

    async def send_file(self, *args: Any, **kwargs: Any) -> Any:
        await self.ready.wait()
        if self.limiter is not None:
            return await self.limiter.send_file(self.client, *args, **kwargs)
        return await self.client.send_file(*args, **kwargs)

    async def edit_message(self, *args: Any, **kwargs: Any) -> Any:
        await self.ready.wait()
        return await self.client.edit_message(*args, **kwargs)

    async def get_messages(self, *args: Any, **kwargs: Any) -> Any:
        await self.ready.wait()
        return await self.client.get_messages(*args, **kwargs)

    async def download_media(self, *args: Any, **kwargs: Any) -> Any:
        await self.ready.wait()
        return await self.client.download_media(*args, **kwargs)

    async def disconnect(self) -> None:
        await self.client.disconnect()


async def run(settings: Settings) -> None:
    configure_logging(settings.log_level)
    if settings.telegram_api_id is None or settings.telegram_api_hash is None:
        raise RuntimeError("TELEGRAM_API_ID and TELEGRAM_API_HASH are required")
    if settings.bot_token is None:
        raise RuntimeError("BOT_TOKEN is required")
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    settings.media_root.mkdir(parents=True, exist_ok=True)
    secure_session_path(settings.telegram_session_path)
    client = TelegramClient(
        str(settings.telegram_session_path),
        settings.telegram_api_id,
        settings.telegram_api_hash.get_secret_value(),
    )
    await client.connect()
    secure_session_path(settings.telegram_session_path)
    if not await client.is_user_authorized():
        await client.disconnect()
        raise RuntimeError("collector session is not authorized; run `emoda login` first")
    bot_token = settings.bot_token.get_secret_value()
    bot = Bot(bot_token)
    admin_limiter = AdminChatLimiter(settings.telegram_session_path.parent / "admin-delivery.json")
    bot.session.middleware(admin_limiter)
    bot_user = await bot.get_me()
    if not bot_user.username:
        raise RuntimeError("Telegram bot username is required for catalog deep links")
    intelligence = (
        GoogleGeminiAdapter(
            settings.gemini_api_key.get_secret_value(),
            settings.gemini_model,
            ocr_model=settings.gemini_ocr_model,
        )
        if settings.gemini_api_key
        else None
    )
    collector = CollectorService(
        client,
        sessions,
        LocalMediaStorage(settings.media_root),
        edit_rescan_limit=settings.catchup_edit_rescan_limit,
    )
    media_session_path = settings.telegram_session_path.with_name("publisher.session")
    secure_session_path(media_session_path)
    media_client = DeferredMediaClient(TelegramClient(
        str(media_session_path), settings.telegram_api_id,
        settings.telegram_api_hash.get_secret_value(), receive_updates=False,
    ), media_session_path, limiter=admin_limiter)
    publisher = PublicationService(
        TelegramBotPublisher(bot, client, media_client=media_client, lock_engine=engine)
    )
    processor = ProcessorService(
        sessions,
        settings.media_root,
        intelligence,
        publisher,
        bot_username=bot_user.username,
    )
    retention = MediaRetentionService(
        sessions,
        settings.media_root,
        retention_days=settings.media_retention_days,
        batch_size=settings.media_retention_batch_size,
        high_watermark_percent=settings.media_retention_high_watermark_percent,
        low_watermark_percent=settings.media_retention_low_watermark_percent,
        pressure_min_age_days=settings.media_retention_pressure_min_age_days,
    )
    dispatcher = Dispatcher()
    dispatcher.include_router(create_catalog_router(sessions))
    dispatcher.include_router(
        create_admin_router(
            sessions,
            set(settings.bootstrap_admin_ids),
            access_validator=collector.validate_access,
            test_message_loader=collector.load_message_reference,
            intelligence=intelligence,
            media_root=settings.media_root,
            publish_product=processor.publish_reviewed,
            media_client=media_client,
            source_sender=collector.send_admin_text,
            order_url_validator=collector.validate_order_button_url,
            admin_chat_limiter=admin_limiter,
        )
    )

    # Reconcile uncertain Telegram I/O before exposing any worker that may retry it.
    # Durable job recovery precedes processing; source history has its own task.
    await processor.reconcile_publications()
    collector.register()
    settings.health_file.write_text("starting\n", encoding="utf-8")

    stop = asyncio.Event()
    recovery_complete = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(signum, stop.set)

    tasks = [
        asyncio.create_task(media_client.run(bot_token, stop), name="media-authorization"),
        asyncio.create_task(
            processing_worker(
                processor,
                collector,
                stop,
                recovery_complete,
                settings.health_file,
                poll_seconds=float(settings.worker_poll_seconds),
            ),
            name="worker",
        ),
        asyncio.create_task(
            review_notification_loop(sessions, settings, bot, media_client, stop, limiter=admin_limiter),
            name="review-notifier",
        ),
        asyncio.create_task(
            periodic_catchup(
                collector,
                stop,
                interval_seconds=float(settings.catchup_interval_seconds),
                recovery_complete=recovery_complete,
                immediate=True,
            ),
            name="source-catchup",
        ),
        asyncio.create_task(
            periodic_media_retention(
                retention,
                stop,
                interval_seconds=float(settings.media_retention_interval_seconds),
            ),
            name="media-retention",
        ),
        asyncio.create_task(dispatcher.start_polling(bot, handle_signals=False), name="admin-bot"),
        asyncio.create_task(client.run_until_disconnected(), name="collector"),
        asyncio.create_task(
            periodic_health_heartbeat(settings.health_file, stop),
            name="health-heartbeat",
        ),
    ]
    for task in tasks:
        task.add_done_callback(lambda completed: request_stop_on_task_exit(completed, stop))
    await stop.wait()
    await dispatcher.stop_polling()
    # Let accepted work finish while Telegram clients are still connected.
    # Cancelling an upload before draining would create an uncertain publication.
    processing_task = next(task for task in tasks if task.get_name() == "worker")
    try:
        await asyncio.wait_for(asyncio.shield(processing_task), timeout=240)
    except Exception:
        logger.exception("processing drain did not complete before shutdown")
    await client.disconnect()
    await media_client.disconnect()
    secure_session_path(settings.telegram_session_path)
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await bot.session.close()
    await engine.dispose()
    settings.health_file.unlink(missing_ok=True)
