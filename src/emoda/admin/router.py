# mypy: disable-error-code="union-attr"
from __future__ import annotations

import asyncio
import html
import logging
import re
import shutil
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, cast
from uuid import uuid4

from aiogram import BaseMiddleware, F, Router
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    TelegramObject,
)
from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.engine import CursorResult
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from emoda.admin.context import current_admin_id
from emoda.admin.service import (
    EXTRACTION_RULE_SETTING_KEYS,
    RULE_KINDS,
    RULE_TRANSFORMS,
    AdminService,
    SourceAlreadyExistsError,
    apply_extraction_rule_settings,
    build_extraction_rule,
    normalize_admin_input,
    parse_catalog_node_input,
    parse_cleaning_rule_input,
    parse_collection_boundary_input,
    parse_computed_rule_input,
    parse_extraction_rule_edit_input,
    parse_extraction_rule_input,
    parse_extraction_rule_values,
    parse_key_value_lines,
    parse_markup_rules_input,
    parse_multi_price_rule_input,
    parse_repeated_pattern_rule_input,
    parse_routing_pair_input,
    parse_source_fallback_input,
    parse_source_field_name,
    parse_source_ignore_rule_input,
    profile_item_token,
    update_profile_from_lines,
)
from emoda.db.models import (
    AdminTestBatch,
    AdminTestItem,
    AdminUser,
    CatalogNode,
    CurrencyAlias,
    Destination,
    GlobalSetting,
    MarkupRule,
    MediaItem,
    Product,
    PublicationOutbox,
    Source,
    SourceMessage,
    TestRun,
)
from emoda.domain.enums import (
    CatalogNodeType,
    OCRMode,
    OutboxStatus,
    ProductStatus,
    PublicationMode,
)
from emoda.domain.schemas import (
    DEFAULT_SERIES_TOKEN_PATTERN,
    PRODUCT_FIELD_NAMES,
    ExtractionRule,
    MarkupRange,
    SourceProfileData,
)
from emoda.integrations.admin_delivery import AdminChatLimiter
from emoda.integrations.publisher import TelegramBotPublisher
from emoda.rules.currency import CurrencyAliasResolver
from emoda.rules.engine import RuleEngine, preview_extraction_rule
from emoda.services.admin_delivery import delivery_stopped, review_delivery_key
from emoda.services.collection import CollectionEngine, CollectionMessage
from emoda.services.collection_diagnostics import collection_report
from emoda.services.destination_access import DestinationAccessError, validate_destination_access
from emoda.services.orchestrator import ProcessingMode, process_product
from emoda.services.pipeline import IntelligenceAdapter, Pipeline, PipelineInput, PipelineResult
from emoda.services.publication import (
    PUBLICATION_BUTTONS_SETTING_KEY,
    normalize_publication_button_url,
    price_inquiry_url,
    publication_keyboard,
)
from emoda.services.render import PUBLICATION_PARSE_MODE, validate_publication_text
from emoda.services.review import (
    ReviewPublishResult,
    StaleReviewApproval,
    review_approval_token,
)

logger = logging.getLogger(__name__)
TEST_BATCH_FEEDBACK_DELAY_SECONDS = 2.0


def usable_admin_media(path: str) -> bool:
    try:
        return Path(path).is_file() and Path(path).stat().st_size > 0
    except OSError:
        return False


async def deliver_admin_product_media(
    message: Message,
    path: str,
    media_type: str,
    *,
    media_client: Any = None,
) -> None:
    """Show the real product media in admin search without Bot API size failures."""

    if media_client is not None and TelegramBotPublisher.requires_mtproto(path, media_type):
        await media_client.send_file(
            message.chat.id,
            path,
            supports_streaming=media_type == "video",
        )
        return
    send_media = message.answer_video if media_type == "video" else message.answer_photo
    await send_media(FSInputFile(path))


class TestProductBoundaryError(ValueError):
    """Raised when one dry-run batch contains multiple Telegram albums."""


class TestBatchFeedbackDebouncer:
    def __init__(self, delay_seconds: float = TEST_BATCH_FEEDBACK_DELAY_SECONDS) -> None:
        self.delay_seconds = delay_seconds
        self._tasks: dict[int, asyncio.Task[None]] = {}

    def schedule(self, batch_id: int, deliver: Callable[[], Awaitable[None]]) -> None:
        previous = self._tasks.get(batch_id)
        if previous is not None:
            previous.cancel()
        task = asyncio.create_task(self._deliver(batch_id, deliver))
        self._tasks[batch_id] = task

    async def _deliver(self, batch_id: int, deliver: Callable[[], Awaitable[None]]) -> None:
        task = asyncio.current_task()
        try:
            await asyncio.sleep(self.delay_seconds)
            await deliver()
        except asyncio.CancelledError:
            return
        except Exception:
            logger.exception("admin test batch feedback failed", extra={"batch_id": batch_id})
        finally:
            if self._tasks.get(batch_id) is task:
                self._tasks.pop(batch_id, None)


def prepare_test_collection(
    items: list[Any], *, profile: SourceProfileData | None = None
) -> tuple[list[Any], list[CollectionMessage]]:
    """Split an explicitly collected admin test batch exactly like live collection.

    The real effective collection profile of the source drives the boundaries, so
    a dry-run containing several products renders one preview per product — the
    same partition the listener would produce.
    """
    from emoda.rules.engine import advertisement_reason, match_structural_ignore_rule

    ordered = [
        item
        for item in sorted(items, key=lambda item: item.telegram_message_id)
        if profile is None
        or (
            advertisement_reason(item.text, profile) is None
            and match_structural_ignore_rule(
                profile.ignore_rules,
                grouped_id=getattr(item, "media_group_id", None),
                media_type=(
                    "video"
                    if any(str(path).endswith(".mp4") for path in item.media_paths)
                    else "photo"
                    if item.media_paths
                    else None
                ),
                media_count=sum(
                    len(part.media_paths)
                    for part in items
                    if getattr(item, "media_group_id", None) is not None
                    and getattr(part, "media_group_id", None) == item.media_group_id
                )
                if getattr(item, "media_group_id", None) is not None
                else len(item.media_paths),
                message_count=sum(
                    getattr(part, "media_group_id", None) == item.media_group_id for part in items
                )
                if getattr(item, "media_group_id", None) is not None
                else 1,
            )
            is None
        )
    ]
    group_ids = {
        str(item.media_group_id)
        for item in ordered
        if getattr(item, "media_group_id", None) is not None
    }
    standalone_group: str | None = "__dry_batch__" if not group_ids else None
    base_time = datetime(2000, 1, 1, tzinfo=UTC)
    collection_messages = [
        CollectionMessage(
            message_id=int(item.telegram_message_id),
            occurred_at=base_time + timedelta(milliseconds=index),
            grouped_id=getattr(item, "media_group_id", None) or standalone_group,
            text=str(getattr(item, "text", "") or ""),
            media_count=len(getattr(item, "media_paths", [])),
        )
        for index, item in enumerate(ordered)
    ]
    return ordered, collection_messages


def partition_test_inputs(
    items: list[Any], *, profile: SourceProfileData | None = None
) -> list[tuple[str, list[str]]]:
    ordered, collection_messages = prepare_test_collection(items, profile=profile)
    effective = (profile or SourceProfileData()).effective_collection()
    engine = CollectionEngine(effective)
    partitioned = engine.partition(collection_messages)
    products: list[tuple[str, list[str]]] = []
    message_ids_by_product = [
        [message.message_id for message in batch.messages] for batch in partitioned.products
    ]
    for batch_ids in message_ids_by_product:
        batch_items = [item for item in ordered if int(item.telegram_message_id) in batch_ids]
        batch_messages = [
            message for message in collection_messages if message.message_id in set(batch_ids)
        ]
        text = engine.merged_text(batch_messages)
        media_paths: list[str] = []
        for item in batch_items:
            media_paths.extend(item.media_paths)
        products.append((text, media_paths))
    return products


async def download_admin_test_media(
    message: Message, batch_dir: Path, *, media_client: Any = None
) -> list[str]:
    media: object | None = None
    extension = ""
    if message.photo:
        media = message.photo[-1]
        extension = ".jpg"
    elif message.video:
        media = message.video
        extension = ".mp4"
    if media is None:
        return []
    destination = batch_dir / f"{message.chat.id}-{message.message_id}-{uuid4().hex}{extension}"
    try:
        await message.bot.download(cast(Any, media), destination=destination)
    except TelegramBadRequest as exc:
        if "file is too big" not in str(exc).lower() or media_client is None:
            raise
        # Modern Bot API file IDs cannot reliably be decoded by Telethon.
        # Fetch this bot's original message, including the fresh file reference.
        original = await media_client.get_messages(
            None if message.chat.id > 0 else message.chat.id, ids=message.message_id
        )
        if (
            original is None
            or original.id != message.message_id
            or original.chat_id != message.chat.id
            or not original.media
        ):
            raise ValueError("Telegram original message is unavailable or mismatched") from exc
        downloaded = await media_client.download_media(original, file=str(destination))
        if not downloaded or not destination.is_file():
            raise OSError("MTProto returned no media file") from exc
    return [str(destination)]


async def start_or_resume_test_batch(
    session: AsyncSession, source_id: int, admin_user_id: int
) -> AdminTestBatch:
    # Locking the admin serializes concurrent button presses that could otherwise create two batches.
    await session.scalar(select(AdminUser).where(AdminUser.id == admin_user_id).with_for_update())
    batch = await session.scalar(
        select(AdminTestBatch)
        .where(
            AdminTestBatch.source_id == source_id,
            AdminTestBatch.admin_user_id == admin_user_id,
            AdminTestBatch.status.in_(["collecting", "processing"]),
        )
        .order_by(AdminTestBatch.created_at.desc(), AdminTestBatch.id.desc())
        .limit(1)
    )
    if batch is None:
        batch = AdminTestBatch(source_id=source_id, admin_user_id=admin_user_id)
        session.add(batch)
        await session.flush()
    elif batch.status == "processing":
        # A dry-run has no publication side effect, so an interrupted calculation is safe to resume.
        batch.status = "collecting"
    return batch


async def add_test_batch_item(
    session: AsyncSession,
    *,
    batch_id: int,
    chat_id: int,
    message_id: int,
    media_group_id: str | None,
    text: str,
    media_paths: list[str],
    allow_multiple_grouped_ids: bool = False,
    media_pending: bool = False,
) -> bool:
    batch = await session.scalar(
        select(AdminTestBatch).where(AdminTestBatch.id == batch_id).with_for_update()
    )
    if batch is None:
        raise LookupError("admin test batch not found")
    if media_group_id is not None and not allow_multiple_grouped_ids:
        existing_group_ids = set(
            await session.scalars(
                select(AdminTestItem.media_group_id)
                .where(
                    AdminTestItem.batch_id == batch_id,
                    AdminTestItem.media_group_id.is_not(None),
                )
                .distinct()
            )
        )
        if existing_group_ids and media_group_id not in existing_group_ids:
            raise TestProductBoundaryError(
                "a different grouped_id is already collected for this dry-run product"
            )
    values = {
        "batch_id": batch_id,
        "telegram_chat_id": chat_id,
        "telegram_message_id": message_id,
        "media_group_id": media_group_id,
        "text": text,
        "media_paths": media_paths,
        "media_pending": media_pending,
    }
    bind = session.get_bind()
    if bind.dialect.name == "postgresql":
        result = await session.execute(
            pg_insert(AdminTestItem)
            .values(**values)
            .on_conflict_do_nothing(
                index_elements=["batch_id", "telegram_chat_id", "telegram_message_id"]
            )
        )
        return bool(cast(CursorResult[Any], result).rowcount)
    result = await session.execute(
        sqlite_insert(AdminTestItem)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=["batch_id", "telegram_chat_id", "telegram_message_id"]
        )
    )
    return bool(cast(CursorResult[Any], result).rowcount)


def keyboard(*rows: list[tuple[str, str]]) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text=text, callback_data=data) for text, data in row]
            for row in rows
        ]
    )


SOURCE_THREE_COLUMN_MAX_NAME_LENGTH = 10
SOURCE_PAGE_SIZE = 30


def build_source_grid_rows(items: list[Source]) -> list[list[tuple[str, str]]]:
    """Group source buttons into predictable two- or three-column rows."""
    rows: list[list[tuple[str, str]]] = []
    index = 0
    while index < len(items):
        remaining = len(items) - index
        candidates = items[index : index + 3]
        three_short_names = len(candidates) == 3 and all(
            len(item.name.strip()) <= SOURCE_THREE_COLUMN_MAX_NAME_LENGTH for item in candidates
        )
        # Four items fit more readably as 2+2 than 3+1.
        width = 3 if three_short_names and remaining != 4 else min(2, remaining)
        row = items[index : index + width]
        rows.append(
            [(f"{'🟢' if item.enabled else '⚪'} {item.name}", f"source:{item.id}") for item in row]
        )
        index += width
    return rows


def build_sources_markup(
    items: list[Source],
    *,
    page: int = 0,
    navigation_rows: list[list[tuple[str, str]]] | None = None,
) -> InlineKeyboardMarkup:
    """Render source rows while keeping controls and navigation out of the grid."""
    page_count = max(1, (len(items) + SOURCE_PAGE_SIZE - 1) // SOURCE_PAGE_SIZE)
    page = max(0, min(page, page_count - 1))
    rows = build_source_grid_rows(items[page * SOURCE_PAGE_SIZE : (page + 1) * SOURCE_PAGE_SIZE])
    if page_count > 1:
        navigation = []
        if page:
            navigation.append(("⬅️", f"menu:sources:page:{page - 1}"))
        navigation.append((f"{page + 1}/{page_count}", f"menu:sources:page:{page}"))
        if page + 1 < page_count:
            navigation.append(("➡️", f"menu:sources:page:{page + 1}"))
        rows.append(navigation)
    rows.extend(
        [
            [("➕ Добавить", "source:add")],
            [("🗄 Архивированные", "menu:sources:archived")],
        ]
    )
    rows.extend(navigation_rows or [])
    rows.append([("⬅️ Назад", "menu:main")])
    return keyboard(*rows)


async def edit_screen(
    callback: CallbackQuery, text: str, reply_markup: InlineKeyboardMarkup
) -> None:
    """Re-render a text screen or the caption of a one-media REVIEW card.

    Telegram rejects editMessageText when neither text nor markup changed. For
    a stale callback that re-opens an unchanged list this is the expected
    outcome and must not surface as a generic admin error.
    """
    try:
        if getattr(callback.message, "photo", None) or getattr(callback.message, "video", None):
            await callback.message.edit_caption(caption=text, reply_markup=reply_markup)
        else:
            await callback.message.edit_text(text, reply_markup=reply_markup)
    except TelegramBadRequest as exc:
        if "message is not modified" not in str(exc).lower():
            raise


def _stale_notice(exc: LookupError) -> str | None:
    """Toast shown when the target of a stale callback is already gone.

    A missing source keeps its own dedicated alert rendered by the screen, so
    only object-level misses get the friendly toast.
    """
    if "source not found" in str(exc):
        return None
    return "Кнопка устарела: правило уже изменено или отсутствует. Показан актуальный список."


def cleaning_rule_editor_text(rule: Any) -> str:
    """Return the complete non-JSON representation accepted by the editor."""
    return "\n".join(
        (
            f"kind={rule.kind}",
            f"value={rule.value}",
            f"replacement={rule.replacement}",
            f"scope={rule.scope}",
            f"language={rule.language}",
            f"ignore_case={str(rule.ignore_case).lower()}",
            f"enabled={str(rule.enabled).lower()}",
        )
    )


def multi_price_rule_editor_text(rule: Any) -> str:
    """Return every editable MultiPriceRule field in the Telegram key=value format."""
    labels = ";".join(
        f"{source}=>{localized.ru}=>{localized.en}" for source, localized in rule.labels.items()
    )
    return "\n".join(
        (
            f"name={rule.name}",
            f"pattern={rule.pattern}",
            f"label_mode={rule.label_mode}",
            f"default_currency={rule.default_currency or ''}",
            f"priority={rule.priority}",
            f"ignore_case={str(rule.ignore_case).lower()}",
            f"enabled={str(rule.enabled).lower()}",
            f"labels={labels}",
        )
    )


def routing_rule_editor_text(route: Any) -> str:
    """Return the complete editable routing/category pair."""
    return "\n".join(
        (
            f"category={route.category or ''}",
            f"keywords={','.join(route.keywords)}",
            f"pattern={route.pattern or ''}",
            f"destination_id={route.destination_id}",
            f"priority={route.priority}",
        )
    )


def extraction_rule_editor_text(field: str, rule: Any) -> str:
    """Return every independently editable extraction-rule setting."""
    return "\n".join(
        (
            f"field={field}",
            f"kind={rule.kind}",
            f"pattern={rule.pattern}",
            f"aliases={','.join(rule.aliases)}",
            f"priority={rule.priority}",
            f"ignore_case={str(rule.ignore_case).lower()}",
            f"transform={rule.transform}",
            f"token_pattern={rule.token_pattern}",
            "value_map="
            + ";".join(f"{source}=>{target}" for source, target in rule.value_map.items()),
            f"authoritative={str(rule.authoritative).lower()}",
            f"enabled={str(rule.enabled).lower()}",
        )
    )


RULE_DRAFT_CORE_SETTINGS: tuple[str, ...] = (
    "field",
    "kind",
    "pattern",
    "priority",
    "ignore_case",
    "transform",
    "authoritative",
    "enabled",
)
RULE_DRAFT_OPTIONAL_SETTINGS: tuple[str, ...] = ("aliases", "token_pattern", "value_map")
RULE_DRAFT_EXPIRED = (
    "Черновик правила потерян: экран старый или бот перезапускался. "
    "Откройте «➕ Правило» или «✏️» ещё раз - настройки сохранённых правил не изменились."
)
RULE_BUILDER_HINT = (
    "Меняйте по одной настройке кнопками ниже. Можно и прислать строки key=value одним "
    "сообщением, например: field=price kind=regex "
    "pattern=(?i)Price\\s*:\\s*(?P<value>\\d+) priority=11"
)
RULE_SETTING_PROMPTS: dict[str, str] = {
    "field": "Пришлите название поля: строчные латинские буквы, цифры, подчёркивание.",
    "pattern": (
        "Пришлите regex одним сообщением - ровно тот, который должен работать.\n"
        "Значение берётся из группы (?P<value>...).\n"
        "Пример: (?m)^[ ]*(?P<value>[0-9]+(?:[.,][0-9]+)?)[ ]*[^0-9A-Za-z\\r\\n]*$\n"
        "Текст не экранируется и не изменяется: \\s \\d | = ^ $ сохраняются как есть.\n"
        "Значимые пробелы по краям пишите как [ ] или \\x20."
    ),
    "priority": "Пришлите целое число: правило с меньшим priority выполняется первым.",
    "aliases": "Пришлите синонимы через запятую (или - чтобы очистить список).",
    "token_pattern": (
        "Пришлите regex размерного ряда с группами (?P<size>...) и (?P<count>...) "
        "(или - чтобы вернуть стандартный)."
    ),
    "value_map": (
        "Пришлите замены значений: Standart=>Standard;STD=>Standard (или - чтобы очистить)."
    ),
}


def empty_rule_draft() -> dict[str, str]:
    """Defaults of a brand-new rule in the same key=value form as the editor."""
    return {
        "kind": "regex",
        "transform": "identity",
        "priority": "100",
        "ignore_case": "true",
        "authoritative": "false",
        "enabled": "true",
    }


def rule_draft_from_rule(field: str, rule: Any) -> dict[str, str]:
    """Turn a stored rule into an editable draft without rewriting the pattern."""
    return {
        "field": field,
        "kind": rule.kind,
        "pattern": rule.pattern,
        "aliases": ",".join(rule.aliases),
        "priority": str(rule.priority),
        "ignore_case": str(rule.ignore_case).lower(),
        "transform": rule.transform,
        "token_pattern": rule.token_pattern,
        "value_map": ";".join(f"{source}=>{target}" for source, target in rule.value_map.items()),
        "authoritative": str(rule.authoritative).lower(),
        "enabled": str(rule.enabled).lower(),
    }


def rule_draft_lines(draft: dict[str, str]) -> str:
    """Render the draft exactly as the admin may send it back as one message."""
    shown = set(RULE_DRAFT_CORE_SETTINGS)
    for key in RULE_DRAFT_OPTIONAL_SETTINGS:
        value = draft.get(key, "")
        if not value:
            continue
        if key == "token_pattern" and value == DEFAULT_SERIES_TOKEN_PATTERN:
            if not draft.get("transform", "").startswith("series"):
                continue
        shown.add(key)
    return "\n".join(
        f"{key}={draft.get(key, '')}" for key in EXTRACTION_RULE_SETTING_KEYS if key in shown
    )


def missing_rule_settings(draft: dict[str, str]) -> list[str]:
    return [key for key in ("field", "pattern") if not draft.get(key, "")]


def rule_builder_text(draft: dict[str, str], *, mode: str, notice: str | None = None) -> str:
    """One screen for CREATE and EDIT: the whole draft plus what is still missing."""
    parts = ["Текущее правило:" if mode == "edit" else "Новое правило:", rule_draft_lines(draft)]
    missing = missing_rule_settings(draft)
    if missing:
        parts.append("Ещё не заполнено: " + ", ".join(missing))
    parts.append(RULE_BUILDER_HINT)
    if mode == "edit":
        parts.append("field тоже можно изменить - правило переедет в другое поле.")
    if notice:
        parts.insert(0, notice)
    return "\n\n".join(parts)


def build_rule_builder_markup(
    draft: dict[str, str], *, mode: str, source_id: int
) -> InlineKeyboardMarkup:
    """Every rule setting reachable by one button, plus test and save."""

    def flag(name: str) -> str:
        return "да" if draft.get(name) == "true" else "нет"

    return keyboard(
        [
            (f"Поле: {draft.get('field') or 'не выбрано'}", "rb:fields"),
            (f"Как искать: {draft.get('kind', 'regex')}", "rb:kinds"),
        ],
        [(f"✏️ pattern: {(draft.get('pattern') or 'не задан')[:24]}", "rb:ask:pattern")],
        [(f"Преобразование: {draft.get('transform', 'identity')}", "rb:transforms")],
        [
            (f"Приоритет: {draft.get('priority', '100')}", "rb:ask:priority"),
            (
                "Регистр: "
                + ("игнорировать" if draft.get("ignore_case") == "true" else "учитывать"),
                "rb:toggle:ignore_case",
            ),
        ],
        [
            (f"Важнее Gemini: {flag('authoritative')}", "rb:toggle:authoritative"),
            (f"Включено: {flag('enabled')}", "rb:toggle:enabled"),
        ],
        [
            ("✏️ Синонимы", "rb:ask:aliases"),
            ("✏️ Размерный ряд", "rb:ask:token_pattern"),
            ("✏️ Замены", "rb:ask:value_map"),
        ],
        [("🧪 Проверить regex", "rb:test")],
        [("💾 Сохранить", "rb:save")],
        [
            (
                "⬅️ Отмена" if mode == "create" else "⬅️ Не сохранять",
                f"source:rules:{source_id}",
            )
        ],
    )


def test_batch_controls(batch_id: int) -> InlineKeyboardMarkup:
    return keyboard(
        [("🧪 Проверить сборку", f"test:collection:{batch_id}")],
        [("Обработать", f"test:process:{batch_id}")],
        [("Отменить", f"test:cancel:{batch_id}")],
    )


def dry_run_preview(result: PipelineResult) -> tuple[str, bool]:
    """Return the visible dry-run result and whether live buttons apply."""
    if result.publication_blocked_reason:
        return (
            "Публикация запрещена: нет фото или видео (media_required).\n"
            "Разбор текста выполнен; добавьте media для проверки публикации.\n\n"
            + result.rendered_text,
            False,
        )
    if result.ignored:
        return (
            f"ignored=true\nignore_reason={result.ignore_reason or 'configured ignore rule'}",
            False,
        )
    return result.rendered_text, True


def dry_run_diagnostics(result: PipelineResult) -> str:
    """Render a separate admin-only trace without changing the live payload."""

    if result.ignored:
        return (
            "🔎 Pipeline diagnostics\n"
            "Result: IGNORED\n"
            f"Reason: {result.ignore_reason or 'configured ignore rule'}"
        )

    def shown(value: object | None) -> str:
        if value is None or value == "":
            return "-"
        return str(value).replace("\r", "\\r").replace("\n", "\\n")[:240]

    labels = {
        "price": "Price",
        "sizes": "Sizes",
        "quantity": "Quantity",
        "article": "Article",
    }
    names: list[str] = [name for name in ("price", "sizes", "quantity") if name in result.field_diagnostics]
    names.extend(
        name
        for name, trace in result.field_diagnostics.items()
        if name not in names and trace.status.value != "missing"
    )
    lines = ["🔎 Pipeline diagnostics"]
    for name in names:
        trace = result.field_diagnostics[name]
        lines.extend(
            (
                f"{labels.get(name, name)}: {trace.status.value.upper()}",
                f"Source: {shown(trace.source)}",
                f"Rule: {shown(trace.rule_id)}",
                f"Raw: {shown(trace.raw)}",
                f"Normalized: {shown(trace.normalized)}",
            )
        )
        if name == "price":
            lines.append(f"Currency: {shown(trace.currency)}")
            lines.append(f"Currency source: {shown(trace.currency_source)}")
            lines.append(f"Matched alias: {shown(trace.matched_currency_alias)}")
        parsed_field = result.parsed.fields.get(name)
        metadata = parsed_field.metadata if parsed_field is not None else {}
        input_source = metadata.get("input_source")
        if input_source is not None:
            lines.append(f"Input source: {shown(input_source)}")
        pattern_matches = metadata.get("pattern_matches")
        if isinstance(pattern_matches, list):
            lines.append(
                "Pattern matches: [" + ", ".join(shown(value) for value in pattern_matches) + "]"
            )
        match_count = metadata.get("match_count")
        if isinstance(match_count, int):
            lines.append(f"Count: {match_count}")
        lines.extend(
            (
                f"Candidate: {trace.candidate_status.value.upper()}",
                f"Final: {shown(trace.final)}",
                f"Stage: {trace.stage}",
                f"Reason: {shown(trace.reason)}",
                "",
            )
        )
    return "\n".join(lines).rstrip()


def build_catalog_archive_markup(nodes: list[CatalogNode]) -> InlineKeyboardMarkup:
    rows = [
        [
            (
                f"↩️ {'📁' if node.node_type is CatalogNodeType.CATEGORY else '🔗'} "
                f"{node.text_ru} / {node.text_en}",
                f"admincat:restore:{node.id}",
            )
        ]
        for node in nodes
    ]
    rows.append([("⬅️ К каталогу", "menu:catalog")])
    return keyboard(*rows)


def build_source_rules_markup(source_id: int, profile: SourceProfileData) -> InlineKeyboardMarkup:
    rows: list[list[tuple[str, str]]] = []
    for field, rules in sorted(profile.extraction_rules.items()):
        for index, rule in enumerate(rules):
            state = "⏸" if not rule.enabled else "🗑"
            token = profile_item_token(rule)
            rows.append(
                [
                    (
                        f"{state} {field} #{index + 1} ({rule.transform})",
                        f"r:d:{source_id}:{field}:{index}:{token}",
                    ),
                    (
                        "✏️",
                        f"r:e:{source_id}:{field}:{index}:{token}",
                    ),
                    (
                        "⏯" if rule.enabled else "▶️",
                        f"r:s:{source_id}:{field}:{index}:{int(not rule.enabled)}:{token}",
                    ),
                ]
            )
    for index, computed_rule in enumerate(profile.computed_rules):
        state = "⏸" if not computed_rule.enabled else "🗑"
        token = profile_item_token(computed_rule)
        rows.append(
            [
                (
                    f"{state} computed {computed_rule.name}",
                    f"source:computed-del:{source_id}:{index}:{token}",
                ),
                (
                    "✏️",
                    f"source:computed-edit:{source_id}:{index}:{token}",
                ),
                (
                    "🧪",
                    f"source:computed-test:{source_id}:{index}:{token}",
                ),
                (
                    "⏯" if computed_rule.enabled else "▶️",
                    f"source:computed-set:{source_id}:{index}:"
                    f"{str(not computed_rule.enabled).lower()}:{token}",
                ),
            ]
        )
    for field in sorted(profile.fallback_rules):
        rows.append(
            [
                (
                    f"🗑 fallback {field}",
                    f"source:fallback-del:{source_id}:{field}",
                ),
                ("✏️", f"source:fallback-edit:{source_id}:{field}"),
            ]
        )
    for index, ignore_rule in enumerate(profile.ignore_rules):
        state = "⏸" if not ignore_rule.enabled else "🗑"
        token = profile_item_token(ignore_rule)
        rows.append(
            [
                (
                    f"{state} ignore {ignore_rule.name}",
                    f"source:ignore-del:{source_id}:{index}:{token}",
                ),
                ("✏️", f"source:ignore-edit:{source_id}:{index}:{token}"),
                (
                    "⏯" if ignore_rule.enabled else "▶️",
                    f"source:ignore-set:{source_id}:{index}:"
                    f"{str(not ignore_rule.enabled).lower()}:{token}",
                ),
            ]
        )
    for index, multi_price_rule in enumerate(profile.multi_price_rules):
        state = "⏸" if not multi_price_rule.enabled else "🗑"
        token = profile_item_token(multi_price_rule)
        rows.append(
            [
                (
                    f"{state} multi-price {multi_price_rule.name}",
                    f"source:multi-price-del:{source_id}:{index}:{token}",
                ),
                (
                    "✏️ Изменить",
                    f"source:multi-price-edit:{source_id}:{index}:{token}",
                ),
                (
                    "⏯" if multi_price_rule.enabled else "▶️",
                    f"source:multi-price-set:{source_id}:{index}:"
                    f"{str(not multi_price_rule.enabled).lower()}:{token}",
                ),
            ]
        )
    for index, repeated_rule in enumerate(profile.repeated_pattern_rules):
        state = "⏸" if not repeated_rule.enabled else "🗑"
        token = profile_item_token(repeated_rule)
        rows.append(
            [
                (
                    f"{state} repeated {repeated_rule.name}",
                    f"source:repeated-del:{source_id}:{index}:{token}",
                ),
                (
                    "⏯" if repeated_rule.enabled else "▶️",
                    f"source:repeated-set:{source_id}:{index}:"
                    f"{str(not repeated_rule.enabled).lower()}:{token}",
                ),
            ]
        )
    for index, cleaning_rule in enumerate(profile.cleaning_rules):
        state = "⏸" if not cleaning_rule.enabled else "🗑"
        token = profile_item_token(cleaning_rule)
        rows.append(
            [
                (
                    f"{state} cleaning {cleaning_rule.kind}: {cleaning_rule.value[:18]}",
                    f"source:clean-del:{source_id}:{index}:{token}",
                ),
                (
                    "✏️ Изменить",
                    f"source:clean-edit:{source_id}:{index}:{token}",
                ),
                (
                    "⏯" if cleaning_rule.enabled else "▶️",
                    f"source:clean-set:{source_id}:{index}:"
                    f"{str(not cleaning_rule.enabled).lower()}:{token}",
                ),
            ]
        )
    rows.extend(
        [
            [("⚙️ Режимы полей", f"source:field-modes:{source_id}")],
            [("➕ Правило (regex/label)", f"source:rule:{source_id}")],
            [("➕ Вычисляемое правило", f"source:computed:{source_id}")],
            [("➕ Repeated fields", f"source:repeated:{source_id}")],
            [("➕/заменить Fallback", f"source:fallback:{source_id}")],
            [("➕ IGNORE фильтр", f"source:ignore:{source_id}")],
            [("➕ Multi-price", f"source:multi-price:{source_id}")],
            [("Реклама", f"source:advertising:{source_id}")],
            [("⬅️ Источник", f"source:{source_id}")],
        ]
    )
    return keyboard(*rows)


def build_field_modes_markup(source_id: int, profile: SourceProfileData) -> InlineKeyboardMarkup:
    rows: list[list[tuple[str, str]]] = []
    for field in PRODUCT_FIELD_NAMES:
        policy = profile.field_policies.get(field)
        mode = policy.processing_mode if policy is not None else "normal"
        if mode == "passthrough":
            label = f"● {field} — RAW / Passthrough"
            target = "normal"
        else:
            label = f"○ {field} — Normal"
            target = "passthrough"
        rows.append([(label, f"source:field-mode:{source_id}:{field}:{target}")])
    rows.append([("⬅️ Парсинг", f"source:rules:{source_id}")])
    return keyboard(*rows)


def parse_source_rule_delete_callback(data: str) -> tuple[int, str, int, str]:
    prefix, action, source_text, field, index_text, token = data.split(":", 5)
    if prefix != "r" or action != "d":
        raise ValueError("invalid source rule callback")
    return int(source_text), field, int(index_text), token


def parse_source_fallback_delete_callback(data: str) -> tuple[int, str]:
    prefix, action, source_text, field = data.split(":", 3)
    if prefix != "source" or action != "fallback-del":
        raise ValueError("invalid source fallback callback")
    return int(source_text), field


def parse_source_ignore_delete_callback(data: str) -> tuple[int, int, str]:
    prefix, action, source_text, index_text, token = data.split(":", 4)
    if prefix != "source" or action != "ignore-del":
        raise ValueError("invalid source ignore callback")
    return int(source_text), int(index_text), token


MAIN = keyboard(
    [("Источники", "menu:sources"), ("Каналы E-MODA", "menu:destinations")],
    [("Товары", "menu:products"), ("На проверке", "menu:review")],
    [("Тестирование", "menu:test")],
    [("Массовая отправка", "menu:bulk")],
    [("Наценки", "menu:markup"), ("Валюты", "menu:currencies")],
    [("Кнопки", "menu:buttons")],
    [("📂 Каталог", "menu:catalog")],
    [("Общие настройки", "menu:settings")],
)


class AdminStates(StatesGroup):
    destination_percent = State()
    destination_publication_text = State()
    bulk_text = State()
    destination_name = State()
    destination_channel = State()
    destination_edit = State()
    source_reference = State()
    source_name = State()
    source_edit = State()
    source_publication_text = State()
    source_destination = State()
    source_rule = State()
    source_rule_edit = State()
    source_computed = State()
    source_computed_edit = State()
    source_computed_test = State()
    source_fallback = State()
    source_fixed = State()
    source_ignore = State()
    source_ignore_edit = State()
    source_multi_price = State()
    source_multi_price_edit = State()
    source_repeated = State()
    source_boundary = State()
    source_clean_rule = State()
    source_clean_rule_edit = State()
    source_advertising = State()
    source_profile = State()
    source_cleaning = State()
    source_routing = State()
    source_routing_edit = State()
    source_gemini = State()
    example = State()
    test_input = State()
    test_correction = State()
    product_query = State()
    markup = State()
    button = State()
    catalog_node = State()
    currency_alias = State()
    currency_rates = State()
    product_edit = State()
    reconciliation_ids = State()
    collection_number = State()
    boundary_edit = State()
    buttons_url = State()
    buttons_texts = State()
    gemini_instruction = State()


class AdminAccessMiddleware(BaseMiddleware):
    def __init__(self, sessions: async_sessionmaker[AsyncSession], bootstrap_ids: set[int], *, chat_limiter: AdminChatLimiter | None = None) -> None:
        self.sessions = sessions
        self.bootstrap_ids = bootstrap_ids
        self.chat_limiter = chat_limiter

    async def __call__(
        self,
        handler: Callable[[TelegramObject, dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        allowed = bool(user and user.id in self.bootstrap_ids)
        admin_id: int | None = None
        if user:
            async with self.sessions() as session, session.begin():
                admin = await session.scalar(
                    select(AdminUser).where(AdminUser.telegram_user_id == user.id)
                )
                if admin is None and allowed:
                    admin = AdminUser(
                        telegram_user_id=user.id, display_name=user.full_name, enabled=True
                    )
                    session.add(admin)
                    await session.flush()
                if admin and admin.enabled:
                    allowed = True
                    admin_id = admin.id
        if not allowed:
            if isinstance(event, Message):
                await event.answer("Доступ запрещён.")
            elif isinstance(event, CallbackQuery):
                await event.answer("Доступ запрещён.", show_alert=True)
            return None
        chat = (
            event.chat if isinstance(event, Message)
            else event.message.chat if isinstance(event, CallbackQuery) and event.message is not None
            else None
        )
        if self.chat_limiter is not None and chat and self.chat_limiter.remaining(chat.id):
            minutes = (self.chat_limiter.remaining(chat.id) + 59) // 60
            logger.warning("admin action deferred by Telegram", extra={"chat_id": chat.id, "remaining_minutes": minutes})
            if isinstance(event, CallbackQuery):
                await event.answer(f"Telegram временно ограничил работу в этом чате. Осталось около {minutes} мин.", show_alert=True)
            return None
        token = current_admin_id.set(admin_id)
        try:
            return await handler(event, data)
        except LookupError:
            if not isinstance(event, CallbackQuery):
                raise
            await event.answer("Объект уже отсутствует или недоступен.", show_alert=True)
            return None
        except Exception as exc:
            if not isinstance(event, CallbackQuery):
                raise
            telegram_error = str(exc).casefold()
            if isinstance(exc, TelegramBadRequest) and (
                "query is too old" in telegram_error or "query id is invalid" in telegram_error
            ):
                logger.warning(
                    "expired admin callback acknowledgment ignored",
                    extra={
                        "callback_data": event.data,
                        "telegram_user_id": user.id if user else None,
                    },
                )
                return None
            error_id = uuid4().hex[:8]
            logger.exception(
                "admin callback handler failed",
                extra={
                    "callback_data": event.data,
                    "telegram_user_id": user.id if user else None,
                    "error_id": error_id,
                },
            )
            try:
                await event.answer(
                    f"Не удалось выполнить действие. Код ошибки: {error_id}",
                    show_alert=True,
                )
            except Exception:
                logger.exception(
                    "admin callback error notification failed",
                    extra={"callback_data": event.data, "error_id": error_id},
                )
            return None
        finally:
            current_admin_id.reset(token)


def create_admin_router(
    sessions: async_sessionmaker[AsyncSession],
    bootstrap_ids: set[int],
    *,
    access_validator: Callable[[str], Awaitable[tuple[int, str, int]]],
    test_message_loader: Callable[[str], Awaitable[tuple[str, list[str]]]],
    intelligence: IntelligenceAdapter | None,
    media_root: Path,
    publish_product: Callable[..., Awaitable[ReviewPublishResult]] | None = None,
    media_client: Any = None,
    source_sender: Callable[[int, str, int], Awaitable[list[int]]] | None = None,
    order_url_validator: Callable[[str], Awaitable[None]] | None = None,
    admin_chat_limiter: AdminChatLimiter | None = None,
) -> Router:
    router = Router(name="admin")
    test_feedback = TestBatchFeedbackDebouncer()
    router.message.middleware(AdminAccessMiddleware(sessions, bootstrap_ids, chat_limiter=admin_chat_limiter))
    router.callback_query.middleware(AdminAccessMiddleware(sessions, bootstrap_ids, chat_limiter=admin_chat_limiter))

    async def show_bulk_sources(callback: CallbackQuery, state: FSMContext, page: int = 0) -> None:
        async with sessions() as session:
            items = list(
                await session.scalars(
                    select(Source).where(Source.archived_at.is_(None)).order_by(Source.name)
                )
            )
        data = await state.get_data()
        selected = set(data.get("bulk_sources", []))
        page = max(0, min(page, max(0, (len(items) - 1) // 20)))
        rows = [
            [
                (
                    f"{'✅' if item.id in selected else '☐'} {item.name}"[:60],
                    f"bulk:select:{item.id}:{page}",
                )
            ]
            for item in items[page * 20 : (page + 1) * 20]
        ]
        rows.append([("◀", f"bulk:page:{max(0, page - 1)}"), ("▶", f"bulk:page:{page + 1}")])
        rows.append([("Ввести текст", "bulk:text"), ("История", "bulk:history")])
        rows.append([("Главная", "menu:main")])
        await edit_screen(
            callback,
            f"Массовая отправка в Telegram-каналы источников\nВыбрано: {len(selected)}. Страница {page + 1}.\nДля отправки collector должен иметь право писать в канал.",
            keyboard(*rows),
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:bulk")
    async def bulk_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await show_bulk_sources(callback, state)

    @router.callback_query(F.data.startswith("bulk:page:"))
    async def bulk_page(callback: CallbackQuery, state: FSMContext) -> None:
        await show_bulk_sources(callback, state, int(callback.data.rsplit(":", 1)[1]))

    @router.callback_query(F.data.startswith("bulk:select:"))
    async def bulk_select(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_id, page = callback.data.split(":")
        selected = set((await state.get_data()).get("bulk_sources", []))
        selected.symmetric_difference_update({int(source_id)})
        await state.update_data(bulk_sources=sorted(selected))
        await show_bulk_sources(callback, state, int(page))

    @router.callback_query(F.data == "bulk:text")
    async def bulk_text_start(callback: CallbackQuery, state: FSMContext) -> None:
        if not (await state.get_data()).get("bulk_sources"):
            await callback.answer("Сначала выберите источники", show_alert=True)
            return
        await state.set_state(AdminStates.bulk_text)
        await edit_screen(
            callback,
            "Введите одинаковый текст для выбранных источников (до 4096 символов).",
            keyboard([("Отмена", "menu:bulk")]),
        )
        await callback.answer()

    @router.message(AdminStates.bulk_text)
    async def bulk_text_save(message: Message, state: FSMContext) -> None:
        from emoda.services.bulk_send import BulkSendService

        try:
            if source_sender is None or current_admin_id.get() is None:
                raise ValueError("Отправка недоступна")
            selected = (await state.get_data()).get("bulk_sources", [])
            service = BulkSendService(sessions, source_sender)
            batch_id = await service.create(
                cast(int, current_admin_id.get()), selected, message.text or ""
            )
            targets = await service.results(batch_id, cast(int, current_admin_id.get()))
            await state.clear()
            recipients = ", ".join(item["source"] for item in targets)
            await message.answer(
                f"Рассылка #{batch_id}. Получатели ({len(targets)}): {recipients[:600]}\n\nТекст ({len(message.text or '')} символов):\n{(message.text or '')[:3000]}"
                + (
                    "\n[Превью сокращено; отправится весь сохранённый текст.]"
                    if len(message.text or "") > 3000
                    else ""
                ),
                parse_mode=None,
                reply_markup=keyboard(
                    [("Отправить во все выбранные", f"bulk:send:{batch_id}")],
                    [("История", "bulk:history")],
                ),
            )
        except Exception as exc:
            await message.answer(f"Не сохранено: {exc}", parse_mode=None)

    @router.callback_query(F.data.startswith("bulk:send:"))
    async def bulk_send(callback: CallbackQuery) -> None:
        from emoda.services.bulk_send import BulkSendService

        if source_sender is None or current_admin_id.get() is None:
            await callback.answer("Отправка недоступна", show_alert=True)
            return
        batch_id = int(callback.data.rsplit(":", 1)[1])
        await callback.answer("Отправка начата")
        await BulkSendService(sessions, source_sender).send(
            batch_id, cast(int, current_admin_id.get())
        )
        await show_bulk_result(callback, batch_id)

    async def show_bulk_result(callback: CallbackQuery, batch_id: int) -> None:
        from emoda.services.bulk_send import BulkSendService

        if source_sender is None or current_admin_id.get() is None:
            return
        results = await BulkSendService(sessions, source_sender).results(
            batch_id, cast(int, current_admin_id.get())
        )
        lines = [f"Рассылка #{batch_id}"]
        for item in results:
            status = {
                "sent": "Отправлено",
                "pending": "Ожидает отправки",
                "sending": "Отправляется / после прерывания нужна проверка",
                "failed": "Ошибка",
                "uncertain": "Нужна проверка принятия Telegram",
            }[item["status"]]
            lines.append(
                f"{item['source']}: {status}; IDs={item['message_ids']} {item['error'] or ''}"
            )
        rows = [[("Обновить", f"bulk:result:{batch_id}"), ("История", "bulk:history")]]
        if any(item["status"] == "pending" for item in results):
            rows.append([("Отправить оставшиеся", f"bulk:send:{batch_id}")])
        await edit_screen(callback, html.escape("\n".join(lines))[:3900], keyboard(*rows))

    @router.callback_query(F.data.startswith("bulk:result:"))
    async def bulk_result(callback: CallbackQuery) -> None:
        await show_bulk_result(callback, int(callback.data.rsplit(":", 1)[1]))
        await callback.answer()

    @router.callback_query(F.data == "bulk:history")
    async def bulk_history(callback: CallbackQuery) -> None:
        from emoda.db.models import BulkSendBatch

        async with sessions() as session:
            batches = list(
                await session.scalars(
                    select(BulkSendBatch)
                    .where(BulkSendBatch.admin_user_id == current_admin_id.get())
                    .order_by(BulkSendBatch.id.desc())
                    .limit(20)
                )
            )
        await edit_screen(
            callback,
            "Последние рассылки",
            keyboard(
                *[
                    [(f"#{batch.id}: {batch.text[:35]}", f"bulk:result:{batch.id}")]
                    for batch in batches
                ],
                [("Новая рассылка", "menu:bulk")],
            ),
        )
        await callback.answer()

    @router.message(F.text == "/admin")
    async def admin(message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer("E-MODA AutoPost — администрирование", reply_markup=MAIN)

    @router.callback_query(F.data == "menu:main")
    async def main(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await callback.message.edit_text("E-MODA AutoPost — администрирование", reply_markup=MAIN)
        await callback.answer()

    @router.message(Command("cancel"))
    async def cancel_state(message: Message, state: FSMContext) -> None:
        current = await state.get_state()
        data = await state.get_data()
        await state.clear()
        if current is None:
            await message.answer("Нечего отменять - вы в главном меню.", reply_markup=MAIN)
            return
        source_id = data.get("source_id")
        await message.answer(
            "Отменено.",
            reply_markup=(
                keyboard([("Сбор товара", f"source:collect:{source_id}")]) if source_id else MAIN
            ),
        )

    @router.callback_query(F.data.regexp(r"^menu:sources(?::page:\d+)?$"))
    async def sources(callback: CallbackQuery) -> None:
        async with sessions() as session:
            items = list(
                await session.scalars(
                    select(Source)
                    .where(Source.archived_at.is_(None))
                    .order_by(Source.name, Source.id)
                )
            )
        enabled_count = sum(source.enabled for source in items)
        page = (
            int(cast(str, callback.data).rsplit(":", 1)[1])
            if ":page:" in cast(str, callback.data)
            else 0
        )
        await edit_screen(
            callback,
            f"Источники\nВсего: {len(items)} · Включено: {enabled_count} · "
            f"Выключено: {len(items) - enabled_count}",
            reply_markup=build_sources_markup(items, page=page),
        )
        await callback.answer()

    async def render_archived_sources(
        callback: CallbackQuery, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            items = list(
                await session.scalars(
                    select(Source)
                    .where(Source.archived_at.is_not(None))
                    .order_by(Source.name, Source.id)
                )
            )
            destinations = {
                item.id: item.name
                for item in await session.scalars(select(Destination).order_by(Destination.id))
            }
        lines = ["Архивированные источники"]
        rows: list[list[tuple[str, str]]] = []
        for source in items:
            profile = SourceProfileData.model_validate(source.profile.config)
            destination_id = profile.default_destination_id
            destination = (
                f"{destinations.get(destination_id, 'недоступен')} (ID {destination_id})"
                if destination_id is not None
                else "не назначен"
            )
            identity = source.telegram_ref or str(source.telegram_channel_id)
            lines.append(
                "\n"
                f"{html.escape(source.name)}\n"
                f"Telegram: {html.escape(identity)} ({source.telegram_channel_id})\n"
                f"Destination: {html.escape(destination)}\n"
                f"Mode: {profile.publication_mode.value.upper()} · "
                f"Enabled: {source.enabled} · Status: archived"
            )
            rows.append(
                [
                    (
                        f"♻️ Восстановить {source.name}"[:60],
                        f"source:restore:{source.id}",
                    )
                ]
            )
        if not items:
            lines.append("\nАрхив пуст.")
        rows.append([("⬅️ Источники", "menu:sources")])
        await edit_screen(callback, "\n".join(lines)[:3900], keyboard(*rows))
        await callback.answer(notice or "", show_alert=notice is not None)

    @router.callback_query(F.data == "menu:sources:archived")
    async def archived_sources(callback: CallbackQuery) -> None:
        await render_archived_sources(callback)

    @router.callback_query(F.data.regexp(r"^source:restore:\d+$"))
    async def source_restore(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).restore_source(source_id, actor_id=None)
        except LookupError as exc:
            notice = (
                "Источник уже восстановлен; показан актуальный архив."
                if "not archived" in str(exc)
                else "Источник больше недоступен; показан актуальный архив."
            )
            await render_archived_sources(callback, notice=notice)
            return
        await render_archived_sources(
            callback,
            notice="Источник восстановлен выключенным (Enabled=False).",
        )

    @router.callback_query(F.data == "source:add")
    async def source_add(callback: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(AdminStates.source_reference)
        await callback.message.edit_text("Отправьте @username, ссылку или channel ID источника.")
        await callback.answer()

    def existing_source_screen(exc: SourceAlreadyExistsError) -> tuple[str, InlineKeyboardMarkup]:
        name = html.escape(exc.source_name)
        if exc.archived:
            text = (
                f"Этот канал уже есть в архиве: {name} (ID {exc.source_id}).\n"
                "Восстановите источник, чтобы сохранить его настройки и историю. "
                "После восстановления он останется выключенным."
            )
            action = ("♻️ Восстановить источник", f"source:restore:{exc.source_id}")
        else:
            text = (
                f"Этот канал уже подключён: {name} (ID {exc.source_id}).\n"
                "Откройте существующий источник, чтобы изменить название, "
                "канал назначения или настройки."
            )
            action = ("Открыть источник", f"source:{exc.source_id}")
        return text, keyboard([action], [("Источники", "menu:sources")])

    @router.message(AdminStates.source_reference)
    async def source_reference(message: Message, state: FSMContext) -> None:
        try:
            channel_id, detected_name, latest_message_id = await access_validator(
                message.text or ""
            )
        except Exception as exc:
            await message.answer(f"Collector не имеет доступа: {html.escape(str(exc))}. Повторите.")
            return
        async with sessions() as session:
            existing = await AdminService(session).find_source_by_channel(channel_id)
            duplicate = SourceAlreadyExistsError(existing) if existing is not None else None
        if duplicate is not None:
            await state.clear()
            text, markup = existing_source_screen(duplicate)
            await message.answer(text, reply_markup=markup)
            return
        await state.update_data(
            channel_id=channel_id,
            telegram_ref=message.text,
            detected_name=detected_name,
            latest_message_id=latest_message_id,
        )
        await state.set_state(AdminStates.source_name)
        await message.answer(
            f"Доступ подтверждён: {html.escape(detected_name)}. Введите название источника."
        )

    @router.message(AdminStates.source_name)
    async def source_name(message: Message, state: FSMContext) -> None:
        await state.update_data(name=(message.text or "").strip())
        async with sessions() as session:
            items = list(
                await session.scalars(
                    select(Destination)
                    .where(Destination.enabled.is_(True))
                    .order_by(Destination.name)
                )
            )
        await state.set_state(AdminStates.source_destination)
        await message.answer(
            "Выберите destination (или без назначения):",
            reply_markup=keyboard(
                *[[(item.name, f"source:dest:{item.id}")] for item in items],
                [("Без назначения", "source:dest:0")],
            ),
        )

    @router.callback_query(AdminStates.source_destination, F.data.startswith("source:dest:"))
    async def source_destination(callback: CallbackQuery, state: FSMContext) -> None:
        destination_id = int(callback.data.rsplit(":", 1)[1]) or None
        data = await state.get_data()
        if not {"name", "channel_id", "telegram_ref", "latest_message_id"} <= data.keys():
            await state.clear()
            await edit_screen(
                callback, "Данные добавления устарели. Начните добавление источника заново.",
                keyboard([("➕ Добавить источник", "source:add")], [("Источники", "menu:sources")]),
            )
            await callback.answer()
            return
        try:
            async with sessions() as session, session.begin():
                source = await AdminService(session).create_source(
                    name=data["name"],
                    channel_id=data["channel_id"],
                    telegram_ref=data["telegram_ref"],
                    destination_id=destination_id,
                    actor_id=None,
                    last_seen_message_id=data["latest_message_id"],
                )
                source_id = source.id
        except SourceAlreadyExistsError as exc:
            await state.clear()
            text, markup = existing_source_screen(exc)
            await edit_screen(callback, text, markup)
            await callback.answer()
            return
        await state.clear()
        await edit_screen(
            callback,
            f"Источник создан выключенным в REVIEW: ID {source_id}",
            keyboard(
                [("Открыть", f"source:{source_id}")], [("Источники", "menu:sources")]
            ),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:dest:\d+$"))
    async def source_destination_finished(callback: CallbackQuery) -> None:
        await callback.answer(
            "Этот шаг добавления уже завершён или устарел. Откройте /admin заново.",
            show_alert=True,
        )

    @router.callback_query(F.data.regexp(r"^source:\d+$"))
    async def source_card(callback: CallbackQuery) -> None:
        await render_source_card(callback, int(cast(str, callback.data).split(":")[1]))

    async def render_source_card(callback: CallbackQuery, source_id: int) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
            outcomes = await AdminService(session).recent_source_outcomes(source_id)
            product_count = await session.scalar(
                select(func.count(Product.id)).where(Product.source_id == source_id)
            )
            review_count = await session.scalar(
                select(func.count(Product.id)).where(
                    Product.source_id == source_id, Product.status == ProductStatus.NEEDS_REVIEW
                )
            )
        recent = (
            "\n".join(
                f"{item['telegram_message_id']}: {item['outcome']}"
                + (f" ({html.escape(str(item['reason']))[:160]})" if item["reason"] else "")
                + (f" [{item['emoda_code']}]" if item["emoda_code"] else "")
                + f" · {item['state']}/{item['origin']}/{item['event_type']}"
                for item in outcomes
            )
            or "нет полученных сообщений после подключения"
        )
        text = (
            f"{html.escape(source.name)}\nChannel: {source.telegram_channel_id}\n"
            f"Enabled: {source.enabled}\nMode: {profile.publication_mode.value.upper()}\n"
            f"Собрано товаров: {product_count}; REVIEW: {review_count}\n"
            f"OCR: {profile.ocr_mode}\nProfile v{source.profile.version}\n"
            f"Required: {', '.join(profile.required_fields)}\n"
            f"Альтернативный текст: "
            f"{html.escape((profile.alternative_publication_text or 'не задан')[:500])}\n"
            f"Last seen: {source.last_seen_message_id or '-'}\n"
            f"Last catch-up: {source.last_catchup_at or '-'}\n"
            f"Проверено до ID: {source.catchup_cursor_message_id or '-'}\n"
            f"Ошибка получения: {html.escape(source.catchup_error or 'нет')}\n\nПоследние результаты:\n{recent}"
        )
        await edit_screen(
            callback,
            text,
            keyboard(
                [
                    (
                        "Выключить" if source.enabled else "Включить",
                        f"source:enabled:{source.id}:{str(not source.enabled).lower()}",
                    ),
                    ("Изменить source", f"source:edit:{source.id}"),
                ],
                [
                    ("Сбор товара", f"source:collect:{source.id}"),
                    ("Парсинг (правила)", f"source:rules:{source.id}"),
                ],
                [
                    ("Очистка", f"source:clean:{source.id}"),
                    ("Категория/routing", f"source:route:{source.id}"),
                ],
                [
                    ("Gemini/OCR", f"source:gemini:{source.id}"),
                    ("Основные", f"source:profile:{source.id}"),
                ],
                [("Текст публикации", f"source:text:{source.id}")],
                [("Фиксированные размеры / количество", f"source:fixed:{source.id}")],
                [("Examples", f"source:examples:{source.id}")],
                [("Тест", f"source:test:{source.id}"), ("Архив", f"source:archive:{source.id}")],
                [("⬅️ Источники", "menu:sources")],
            ),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:text:\d+$"))
    async def source_publication_text(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        current = profile.alternative_publication_text or "не задан"
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_publication_text)
        await callback.message.edit_text(
            "Отправьте альтернативный текст публикации для этого источника. "
            "Он применяется, только если у канала назначения нет своего текста. "
            "Отправьте - для сброса. /cancel — отмена.\n\n"
            f"Сейчас: {html.escape(current[:3000])}"
        )
        await callback.answer()

    @router.message(AdminStates.source_publication_text)
    async def source_publication_text_save(message: Message, state: FSMContext) -> None:
        try:
            source_id = int((await state.get_data())["source_id"])
            raw = (message.text or "").strip()
            value = None if raw.casefold() in {"-", "none", "нет"} else raw
            async with sessions() as session, session.begin():
                source = await session.get(Source, source_id)
                if source is None:
                    raise LookupError("Источник не найден")
                profile = SourceProfileData.model_validate(source.profile.config)
                updated = SourceProfileData.model_validate(
                    {
                        **profile.model_dump(mode="json"),
                        "alternative_publication_text": value,
                    }
                )
                await AdminService(session).update_profile(
                    source_id, updated.model_dump(mode="json"), actor_id=current_admin_id.get()
                )
        except (ValueError, LookupError) as exc:
            await message.answer(f"Текст не сохранён: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Альтернативный текст источника сохранён."
            if value
            else "Альтернативный текст источника сброшен.",
            reply_markup=keyboard([("Открыть источник", f"source:{source_id}")]),
        )

    STRATEGY_LABELS = {
        "single_message": "1 сообщение",
        "single_grouped_id": "1 альбом (grouped_id)",
        "multi_grouped_id": "несколько grouped_id",
    }
    ORDER_LABELS = {
        "any": "любой",
        "media_before_text": "media → текст",
        "text_before_media": "текст → media",
        "text_between_media": "текст между media",
    }
    MAIN_TEXT_LABELS = {
        "first": "первый",
        "last": "последний",
        "longest": "самый длинный",
        "caption": "caption",
        "all": "все блоки",
    }
    COLLECTION_BOOL_FIELDS = {
        "merge": "merge_neighbor_grouped_ids",
        "stop": "stop_on_next_start",
        "multitext": "allow_multiple_text_blocks",
        "mergetext": "merge_text_blocks",
        "finalize": "finalize_on_next_start",
    }
    COLLECTION_NUM_FIELDS = {
        "debounce": ("debounce_seconds", "debounce, сек (0.5–300)"),
        "gap": (
            "max_time_gap_seconds",
            "окно по времени, сек (0–604800; 86400 = сутки, максимум 7 суток)",
        ),
        "gapids": ("max_message_id_gap", "окно по message ID (0–10000, 0=выкл)"),
        "maxmsg": ("max_messages", "макс. сообщений в товаре (1–200)"),
        "maxmedia": ("max_media", "макс. media в товаре (0–200)"),
    }

    def collection_screen_rows(
        source_id: int, collection: Any, *, explicit: bool
    ) -> tuple[str, InlineKeyboardMarkup]:
        text = (
            "Сбор товара\n"
            f"Профиль: {'явные настройки' if explicit else 'наследуемые умолчания'}\n\n"
            f"Стратегия: {STRATEGY_LABELS[collection.strategy]}\n"
            f"Порядок: {ORDER_LABELS[collection.order]}\n"
            f"Объединять соседние grouped_id: {'да' if collection.merge_neighbor_grouped_ids else 'нет'}\n"
            f"Окно по времени: {collection.max_time_gap_seconds} c\n"
            f"Окно по message ID: {collection.max_message_id_gap}\n"
            f"Debounce: {collection.debounce_seconds} c\n"
            f"Макс. сообщений: {collection.max_messages}\n"
            f"Макс. media: {collection.max_media}\n"
            f"Несколько текстовых блоков: {'да' if collection.allow_multiple_text_blocks else 'нет'}\n"
            f"Склеивать текстовые блоки: {'да' if collection.merge_text_blocks else 'нет'}\n"
            f"Главный текст: {MAIN_TEXT_LABELS[collection.main_text]}\n"
            f"Новый товар обрывает сбор: {'да' if collection.stop_on_next_start else 'нет'}\n"
            f"Ждать следующий старт: {'да' if collection.finalize_on_next_start else 'нет'}\n"
        )
        strategy_row = [
            (
                ("✅ " if value == collection.strategy else "") + label,
                f"collect:strategy:{source_id}:{value}",
            )
            for value, label in STRATEGY_LABELS.items()
        ]
        order_row = [
            (
                ("✅ " if value == collection.order else "") + label,
                f"collect:order:{source_id}:{value}",
            )
            for value, label in ORDER_LABELS.items()
        ]
        main_row = [
            (
                ("✅ " if value == collection.main_text else "") + label,
                f"collect:main:{source_id}:{value}",
            )
            for value, label in MAIN_TEXT_LABELS.items()
        ]
        bool_rows = [
            (
                f"{'✅' if collection.merge_neighbor_grouped_ids else '○'} Объед. grouped_id",
                f"collect:bool:{source_id}:merge:"
                f"{str(not collection.merge_neighbor_grouped_ids).lower()}",
            ),
            (
                f"{'✅' if collection.stop_on_next_start else '○'} Стоп на новом",
                f"collect:bool:{source_id}:stop:{str(not collection.stop_on_next_start).lower()}",
            ),
        ]
        bool_rows_2 = [
            (
                f"{'✅' if collection.allow_multiple_text_blocks else '○'} Неск. текстов",
                f"collect:bool:{source_id}:multitext:"
                f"{str(not collection.allow_multiple_text_blocks).lower()}",
            ),
            (
                f"{'✅' if collection.merge_text_blocks else '○'} Склеивать тексты",
                f"collect:bool:{source_id}:mergetext:"
                f"{str(not collection.merge_text_blocks).lower()}",
            ),
        ]
        finalize_row = [
            (
                f"{'✅' if collection.finalize_on_next_start else '○'} Ждать новый album",
                f"collect:bool:{source_id}:finalize:"
                f"{str(not collection.finalize_on_next_start).lower()}",
            )
        ]
        num_rows_1 = [
            ("⏱ Debounce", f"collect:num:{source_id}:debounce"),
            ("🕒 Окно времени", f"collect:num:{source_id}:gap"),
        ]
        num_rows_2 = [
            ("#️⃣ Окно ID", f"collect:num:{source_id}:gapids"),
            ("🔢 Макс. сообщений", f"collect:num:{source_id}:maxmsg"),
            ("🖼 Макс. media", f"collect:num:{source_id}:maxmedia"),
        ]
        boundary_rows: list[list[tuple[str, str]]] = []
        for index, rule in enumerate(collection.boundary_rules):
            state = "✅" if rule.enabled else "⏸"
            token = profile_item_token(rule)
            boundary_rows.append(
                [
                    (
                        f"{state} {rule.action}: {rule.name}",
                        f"collect:bset:{source_id}:{index}:{str(not rule.enabled).lower()}:{token}",
                    ),
                    ("✏️", f"collect:bedit:{source_id}:{index}:{token}"),
                    ("🗑", f"collect:bdel:{source_id}:{index}:{token}"),
                ]
            )
        if not collection.boundary_rules:
            boundary_rows.append([("Границы не заданы", "collect:none")])
        boundary_rows.extend(
            [
                [("➕ Граница (custom)", f"source:boundary:{source_id}")],
                [
                    (
                        "Шаблон: album → texts → next album",
                        f"collect:preset:{source_id}:album_texts",
                    )
                ],
                [
                    ("Пресет: текст = новый товар", f"collect:badd:{source_id}:text_start"),
                    ("Пресет: новый grouped_id", f"collect:badd:{source_id}:grouped_change"),
                ],
            ]
        )
        rows = [
            [("🧪 Тестер сборки (история)", f"source:collect-test:{source_id}")],
            [("🧪 Тест на своих сообщениях", f"source:test:{source_id}")],
            strategy_row,
            order_row,
            main_row,
            bool_rows,
            bool_rows_2,
            finalize_row,
            num_rows_1,
            num_rows_2,
            *boundary_rows,
            [("⬅️ Источник", f"source:{source_id}")],
        ]
        return text, keyboard(*rows)

    async def show_collection_screen(
        callback: CallbackQuery, source_id: int, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        explicit = profile.collection is not None
        collection = profile.effective_collection()
        text, markup = collection_screen_rows(source_id, collection, explicit=explicit)
        await edit_screen(callback, text, markup)
        await callback.answer(notice or "")

    @router.callback_query(F.data.regexp(r"^source:collect:\d+$"))
    async def source_collection_screen(callback: CallbackQuery) -> None:
        await show_collection_screen(callback, int(callback.data.rsplit(":", 1)[1]))

    @router.callback_query(
        F.data.regexp(r"^collect:strategy:\d+:(single_message|single_grouped_id|multi_grouped_id)$")
    )
    async def collection_strategy_set(callback: CallbackQuery) -> None:
        _, _, source_text, value = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        updates: dict[str, Any] = {"strategy": value}
        if value == "multi_grouped_id":
            updates["merge_neighbor_grouped_ids"] = True
        async with sessions() as session, session.begin():
            await AdminService(session).set_collection_settings(source_id, updates, actor_id=None)
        await show_collection_screen(callback, source_id)

    @router.callback_query(
        F.data.regexp(
            r"^collect:order:\d+:(any|media_before_text|text_before_media|text_between_media)$"
        )
    )
    async def collection_order_set(callback: CallbackQuery) -> None:
        _, _, source_text, value = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        async with sessions() as session, session.begin():
            await AdminService(session).set_collection_settings(
                source_id, {"order": value}, actor_id=None
            )
        await show_collection_screen(callback, source_id)

    @router.callback_query(F.data.regexp(r"^collect:main:\d+:(first|last|longest|caption|all)$"))
    async def collection_main_set(callback: CallbackQuery) -> None:
        _, _, source_text, value = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        async with sessions() as session, session.begin():
            await AdminService(session).set_collection_settings(
                source_id, {"main_text": value}, actor_id=None
            )
        await show_collection_screen(callback, source_id)

    @router.callback_query(
        F.data.regexp(r"^collect:bool:\d+:(merge|stop|multitext|mergetext|finalize):(true|false)$")
    )
    async def collection_bool_set(callback: CallbackQuery) -> None:
        _, _, source_text, key, enabled_text = cast(str, callback.data).split(":", 4)
        source_id = int(source_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        collection = profile.effective_collection()
        field = COLLECTION_BOOL_FIELDS[key]
        updates = {field: enabled_text == "true"}
        if field == "merge_neighbor_grouped_ids" and not updates[field]:
            if collection.strategy == "multi_grouped_id":
                await callback.answer(
                    "Стратегия «несколько grouped_id» требует объединения",
                    show_alert=True,
                )
                return
        async with sessions() as session, session.begin():
            await AdminService(session).set_collection_settings(source_id, updates, actor_id=None)
        await show_collection_screen(callback, source_id)

    @router.callback_query(
        F.data.regexp(r"^collect:num:\d+:(debounce|gap|gapids|maxmsg|maxmedia)$")
    )
    async def collection_number_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, key = cast(str, callback.data).split(":", 3)
        await state.update_data(source_id=int(source_text), collection_field=key)
        await state.set_state(AdminStates.collection_number)
        label = COLLECTION_NUM_FIELDS[key][1]
        await callback.message.edit_text(f"Введите новое значение:\n{label}\n\n/cancel — отмена.")
        await callback.answer()

    @router.message(AdminStates.collection_number)
    async def collection_number_save(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        source_id = int(data["source_id"])
        key = str(data["collection_field"])
        field, _label = COLLECTION_NUM_FIELDS[key]
        raw = (message.text or "").strip().replace(",", ".")
        try:
            updates: dict[str, Any] = (
                {field: Decimal(raw)}
                if field in {"debounce_seconds", "max_time_gap_seconds"}
                else {field: int(raw)}
            )
        except (InvalidOperation, ValueError):
            await message.answer("Введите число.")
            return
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).set_collection_settings(
                    source_id, updates, actor_id=None
                )
        except Exception as exc:
            await message.answer(f"Не сохранено: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Сохранено.", reply_markup=keyboard([("Сбор товара", f"source:collect:{source_id}")])
        )

    @router.callback_query(F.data.regexp(r"^collect:bdel:\d+:\d+:[0-9a-f]{10}$"))
    async def collection_boundary_delete(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_collection_boundary(
                    source_id, index, actor_id=None, expected_token=token
                )
        except LookupError as exc:
            await show_collection_screen(callback, source_id, notice=_stale_notice(exc))
            return
        await show_collection_screen(callback, source_id)

    @router.callback_query(F.data.regexp(r"^collect:bset:\d+:\d+:(true|false):[0-9a-f]{10}$"))
    async def collection_boundary_set(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, enabled_text, token = cast(str, callback.data).split(":", 5)
        source_id, index = int(source_text), int(index_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).toggle_collection_boundary(
                    source_id,
                    index,
                    actor_id=None,
                    expected_token=token,
                    enabled=enabled_text == "true",
                )
        except LookupError as exc:
            await show_collection_screen(callback, source_id, notice=_stale_notice(exc))
            return
        await show_collection_screen(callback, source_id)

    @router.callback_query(F.data.regexp(r"^collect:(bdel|btoggle|bedit):\d+:\d+$"))
    async def legacy_collection_boundary_callback(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).split(":")[2])
        await show_collection_screen(
            callback,
            source_id,
            notice="Кнопка устарела; показаны актуальные настройки.",
        )

    @router.callback_query(F.data.regexp(r"^collect:bedit:\d+:\d+:[0-9a-f]{10}$"))
    async def collection_boundary_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        rules = profile.effective_collection().boundary_rules
        if index < 0 or index >= len(rules):
            await show_collection_screen(callback, source_id, notice="Граница уже отсутствует.")
            return
        if profile_item_token(rules[index]) != token:
            await show_collection_screen(callback, source_id, notice="Граница уже изменилась.")
            return
        rule = rules[index]
        await state.update_data(source_id=source_id, boundary_index=index, boundary_token=token)
        await state.set_state(AdminStates.boundary_edit)
        lines = [
            f"name={rule.name}",
            f"action={rule.action}",
            f"match={rule.match}",
        ]
        if rule.pattern is not None:
            lines.append(f"pattern={rule.pattern}")
        if rule.keywords:
            lines.append(f"keywords={','.join(rule.keywords)}")
        lines.append(f"target={rule.target}")
        lines.append(f"ignore_case={'true' if rule.ignore_case else 'false'}")
        if rule.reason:
            lines.append(f"reason={rule.reason}")
        await callback.message.edit_text(
            "Текущая граница (отправьте исправленные строки key=value):\n"
            + "\n".join(html.escape(line) for line in lines)
            + "\n\n/cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.boundary_edit)
    async def collection_boundary_edit_save(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        source_id = int(data["source_id"])
        index = int(data["boundary_index"])
        expected_token = str(data["boundary_token"])
        try:
            rule = parse_collection_boundary_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).replace_collection_boundary(
                    source_id,
                    index,
                    rule,
                    actor_id=None,
                    expected_token=expected_token,
                )
            await state.clear()
            await message.answer(
                "Граница обновлена.",
                reply_markup=keyboard([("Сбор товара", f"source:collect:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Граница не сохранена: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^collect:badd:\d+:(text_start|grouped_change)$"))
    async def collection_boundary_preset(callback: CallbackQuery) -> None:
        _, _, source_text, preset = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        from emoda.domain.schemas import CollectionBoundaryRule

        rule = (
            CollectionBoundaryRule(
                name=f"text-starts-product-{source_id}",
                action="start",
                match="has_text",
                target="incoming",
                reason="любой текст открывает новый товар",
            )
            if preset == "text_start"
            else CollectionBoundaryRule(
                name=f"grouped-id-change-{source_id}",
                action="start",
                match="grouped_id_change",
                target="incoming",
                reason="новый grouped_id открывает новый товар",
            )
        )
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).add_collection_boundary(source_id, rule, actor_id=None)
        except Exception as exc:
            await callback.answer(f"Не добавлено: {str(exc)[:160]}", show_alert=True)
            return
        await show_collection_screen(callback, source_id)

    @router.callback_query(F.data.regexp(r"^collect:preset:\d+:album_texts$"))
    async def collection_album_texts_preset(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).split(":")[2])
        from emoda.domain.schemas import CollectionBoundaryRule

        rules = [
            CollectionBoundaryRule(
                name="new-grouped-album",
                action="start",
                match="grouped_id_change",
                target="incoming",
                reason="новый album закрывает предыдущий товар и начинает следующий",
            ),
            CollectionBoundaryRule(
                name="text-after-album",
                action="continue",
                match="has_text",
                target="incoming",
                reason="standalone text продолжает текущий товар",
            ),
        ]
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).set_collection_settings(
                    source_id,
                    {
                        "strategy": "multi_grouped_id",
                        "order": "any",
                        "merge_neighbor_grouped_ids": True,
                        "allow_multiple_text_blocks": True,
                        "merge_text_blocks": True,
                        "main_text": "all",
                        "stop_on_next_start": True,
                        "finalize_on_next_start": True,
                        "boundary_rules": rules,
                    },
                    actor_id=None,
                )
        except Exception as exc:
            await callback.answer(f"Не применено: {str(exc)[:160]}", show_alert=True)
            return
        await show_collection_screen(
            callback,
            source_id,
            notice="Шаблон применён; окна и лимиты сохранены.",
        )

    @router.callback_query(F.data == "collect:none")
    async def collection_noop(callback: CallbackQuery) -> None:
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:destination:\d+:\d+$"))
    async def choose_source_destination(callback: CallbackQuery) -> None:
        source_id, page = map(int, cast(str, callback.data).split(":")[-2:])
        async with sessions() as session:
            items = list(
                await session.scalars(
                    select(Destination)
                    .where(Destination.enabled.is_(True))
                    .order_by(Destination.name, Destination.id)
                )
            )
        pages = max(1, (len(items) + 19) // 20)
        page = min(page, pages - 1)
        rows = [
            [(d.name[:55], f"source:destination-save:{source_id}:{d.id}")]
            for d in items[page * 20 : (page + 1) * 20]
        ]
        nav = []
        if page:
            nav.append(("⬅️", f"source:destination:{source_id}:{page - 1}"))
        if page + 1 < pages:
            nav.append(("➡️", f"source:destination:{source_id}:{page + 1}"))
        if nav:
            rows.append(nav)
        rows.append([("⬅️ Настройки", f"source:edit:{source_id}")])
        await edit_screen(
            callback,
            "Выберите канал назначения для новых обработок. Уже опубликованные посты не переносятся. Правила routing, если заданы, могут выбирать другой канал.",
            keyboard(*rows),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:destination-save:\d+:\d+$"))
    async def save_source_destination(callback: CallbackQuery) -> None:
        source_id, destination_id = map(int, cast(str, callback.data).split(":")[-2:])
        try:
            async with sessions() as session, session.begin():
                destination = await session.get(Destination, destination_id)
                if destination is None or not destination.enabled:
                    raise ValueError("Канал не найден или выключен")
                await validate_destination_access(callback.bot, destination.telegram_channel_id)
                await AdminService(session).set_source_destination(
                    source_id, destination_id, actor_id=current_admin_id.get()
                )
        except DestinationAccessError as exc:
            await edit_screen(
                callback, f"Не сохранено: {html.escape(str(exc))}",
                keyboard(
                    [("Проверить доступ", f"destination:check:{destination_id}")],
                    [("Выбрать другой канал", f"source:destination:{source_id}:0")],
                ),
            )
            await callback.answer()
            return
        except (ValueError, LookupError) as exc:
            await callback.answer(str(exc)[:180], show_alert=True)
            return
        await show_source_edit_screen(callback, source_id)
        await callback.answer("Канал назначения сохранён")

    async def show_source_edit_screen(callback: CallbackQuery, source_id: int) -> bool:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден или удалён.", show_alert=True)
                return False
            profile = SourceProfileData.model_validate(source.profile.config)
            destination = (
                await session.get(Destination, profile.default_destination_id)
                if profile.default_destination_id is not None
                else None
            )
        destination_label = (
            f"{html.escape(destination.name)} ({destination.telegram_channel_id})"
            if destination is not None
            else "не назначен"
        )
        target_mode = (
            PublicationMode.AUTO
            if profile.publication_mode is PublicationMode.REVIEW
            else PublicationMode.REVIEW
        )
        target_enabled = not source.enabled
        await edit_screen(
            callback,
            "Настройки source\n\n"
            f"Название: {html.escape(source.name)}\n"
            f"Reference: {html.escape(source.telegram_ref or str(source.telegram_channel_id))}\n"
            f"Channel ID: {source.telegram_channel_id}\n"
            f"Enabled: {source.enabled}\n"
            f"Publication mode: {profile.publication_mode.value.upper()}\n"
            f"Destination: {destination_label}",
            keyboard(
                [
                    (
                        "Выключить source" if source.enabled else "Включить source",
                        f"source:enabled:{source_id}:{str(target_enabled).lower()}",
                    )
                ],
                [
                    (
                        f"Переключить в {target_mode.value.upper()}",
                        f"source:mode:{source_id}:{target_mode.value}",
                    )
                ],
                [("Название / reference", f"source:identity:{source_id}")],
                [("Канал назначения / Destination", f"source:destination:{source_id}:0")],
                [("Фиксированные размеры / количество", f"source:fixed:{source_id}")],
                [
                    ("Сбор товара", f"source:collect:{source_id}"),
                    ("Парсинг", f"source:rules:{source_id}"),
                ],
                [
                    ("Очистка", f"source:clean:{source_id}"),
                    ("Gemini/OCR", f"source:gemini:{source_id}"),
                ],
                [
                    ("Routing/destination", f"source:route:{source_id}"),
                    ("Examples", f"source:examples:{source_id}"),
                ],
                [
                    ("Основные", f"source:profile:{source_id}"),
                    ("Dry-run", f"source:test:{source_id}"),
                ],
                [("⬅️ Карточка source", f"source:{source_id}")],
            ),
        )
        await callback.answer()
        return True

    @router.callback_query(F.data.regexp(r"^source:edit:\d+$"))
    async def source_edit_screen(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        await state.clear()
        await state.update_data(source_id=source_id)
        if not await show_source_edit_screen(callback, source_id):
            await state.clear()

    @router.callback_query(F.data.regexp(r"^source:identity:\d+$"))
    async def source_identity_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session:
            source = await session.get(Source, source_id)
        if source is None:
            await callback.answer("Источник не найден или удалён.", show_alert=True)
            return
        await state.clear()
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_edit)
        await callback.message.edit_text(
            "Измените source без JSON:\n"
            f"name={source.name}\nreference={source.telegram_ref or source.telegram_channel_id}\n\n"
            "Для переименования достаточно name=Новое название.\n"
            "Доступ проверяется только при смене reference.",
            reply_markup=keyboard([("⬅️ Настройки source", f"source:edit:{source_id}")]),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:mode:\d+:(auto|review)$"))
    async def source_publication_mode_set(callback: CallbackQuery) -> None:
        _, _, source_text, mode_text = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        mode = PublicationMode(mode_text)
        async with sessions() as session, session.begin():
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден или удалён.", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config).model_copy(
                update={
                    "publication_mode": mode,
                    "use_global_publication_mode": False,
                }
            )
            await AdminService(session).update_profile(
                source_id,
                profile.model_dump(mode="json"),
                actor_id=None,
            )
        await show_source_edit_screen(callback, source_id)

    @router.callback_query(F.data.regexp(r"^source:enabled:\d+:(true|false)$"))
    async def source_enabled_set(callback: CallbackQuery) -> None:
        _, _, source_text, enabled_text = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        enabled = enabled_text == "true"
        async with sessions() as session, session.begin():
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден или удалён.", show_alert=True)
                return
            await AdminService(session).set_source_enabled(source_id, enabled, actor_id=None)
        await show_source_edit_screen(callback, source_id)

    @router.message(AdminStates.source_edit)
    async def source_edit_save(message: Message, state: FSMContext) -> None:
        try:
            values = parse_key_value_lines(message.text or "")
            unknown = set(values) - {"name", "reference"}
            if unknown:
                raise ValueError(f"unknown source setting: {sorted(unknown)}")
            source_id = int((await state.get_data())["source_id"])
            async with sessions() as session:
                source = await session.get(Source, source_id)
                if source is None:
                    raise LookupError("source not found")
                reference = values.get(
                    "reference", source.telegram_ref or str(source.telegram_channel_id)
                )
                name = values.get("name", source.name)
                old_reference = source.telegram_ref or str(source.telegram_channel_id)
                channel_id = source.telegram_channel_id
            latest_message_id = None
            if reference != old_reference:
                checked_channel_id, _, checked_latest = await access_validator(reference)
                if checked_channel_id != channel_id:
                    latest_message_id = checked_latest
                channel_id = checked_channel_id
            async with sessions() as session, session.begin():
                await AdminService(session).update_source(
                    source_id,
                    name=name,
                    channel_id=channel_id,
                    telegram_ref=reference,
                    actor_id=None,
                    last_seen_message_id=latest_message_id,
                )
            await state.clear()
            await message.answer(
                "Source обновлён.", reply_markup=keyboard([("Источник", f"source:{source_id}")])
            )
        except Exception as exc:
            await message.answer(f"Source не изменён: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:toggle:"))
    async def source_toggle(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        await render_source_card(callback, source_id)

    @router.callback_query(F.data.regexp(r"^source:archive:\d+$"))
    async def source_archive(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        async with sessions() as session:
            source = await session.get(Source, source_id)
        if source is None:
            await callback.answer("Источник не найден или удалён.", show_alert=True)
            return
        if source.archived_at is not None:
            await callback.answer("Источник уже архивирован.", show_alert=True)
            return
        await edit_screen(
            callback,
            f"Архивировать {html.escape(source.name)}?\n\n"
            "Источник можно будет восстановить через «Архивированные». "
            "При архивировании он будет выключен.",
            keyboard(
                [("Да, архивировать", f"source:archive-do:{source_id}")],
                [("Отмена", f"source:{source_id}")],
            ),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:archive-do:\d+$"))
    async def source_archive_do(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).archive_source(source_id, actor_id=None)
        except LookupError as exc:
            if "already archived" in str(exc):
                await render_archived_sources(
                    callback,
                    notice="Источник уже архивирован; показан актуальный архив.",
                )
            else:
                await callback.answer(
                    "Источник больше недоступен. Откройте список заново.",
                    show_alert=True,
                )
            return
        await callback.message.edit_text(
            "Источник архивирован; история и настройки сохранены.",
            reply_markup=keyboard(
                [("Архивированные", "menu:sources:archived")],
                [("Источники", "menu:sources")],
            ),
        )
        await callback.answer()

    async def rule_draft_state(state: FSMContext) -> tuple[dict[str, str], int, str] | None:
        """Return ``(draft, source_id, mode)``, or ``None`` for a stale screen."""
        data = await state.get_data()
        draft = data.get("rule_draft")
        source_id = data.get("source_id")
        if not isinstance(draft, dict) or source_id is None:
            return None
        return (
            {str(key): str(value) for key, value in draft.items()},
            int(source_id),
            str(data.get("rule_mode", "create")),
        )

    def rule_from_draft(draft: dict[str, str]) -> tuple[str, ExtractionRule]:
        """One model for CREATE and EDIT: draft settings -> validated rule."""
        field = parse_source_field_name(draft.get("field", ""))
        return field, build_extraction_rule(apply_extraction_rule_settings({}, draft))

    async def answer_rule_builder(
        message: Message,
        draft: dict[str, str],
        *,
        mode: str,
        source_id: int,
        notice: str | None = None,
    ) -> None:
        await message.answer(
            rule_builder_text(draft, mode=mode, notice=notice),
            reply_markup=build_rule_builder_markup(draft, mode=mode, source_id=source_id),
        )

    async def show_rule_builder(
        callback: CallbackQuery, state: FSMContext, *, notice: str | None = None
    ) -> None:
        loaded = await rule_draft_state(state)
        if loaded is None:
            await callback.answer(RULE_DRAFT_EXPIRED, show_alert=True)
            return
        draft, source_id, mode = loaded
        await state.update_data(rule_awaiting=None)
        await edit_screen(
            callback,
            rule_builder_text(draft, mode=mode, notice=notice),
            build_rule_builder_markup(draft, mode=mode, source_id=source_id),
        )
        await callback.answer()

    async def save_rule_draft(
        source_id: int, draft: dict[str, str], mode: str, data: dict[str, Any]
    ) -> str:
        """Persist the draft through the same service call as the text editor."""
        field, rule = rule_from_draft(draft)
        async with sessions() as session, session.begin():
            service = AdminService(session)
            if mode == "edit":
                await service.replace_source_rule(
                    source_id,
                    str(data["field"]),
                    int(data["index"]),
                    field,
                    rule,
                    actor_id=None,
                    expected_token=str(data["rule_token"]),
                )
                return "Правило обновлено."
            await service.add_source_rule(source_id, field, rule, actor_id=None)
        return "Правило сохранено и будет применено до Gemini/critical validation."

    @router.callback_query(F.data.startswith("source:rule:"))
    async def source_rule_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        draft = empty_rule_draft()
        await state.set_state(AdminStates.source_rule)
        await state.update_data(
            source_id=source_id, rule_draft=draft, rule_mode="create", rule_awaiting=None
        )
        await edit_screen(
            callback,
            rule_builder_text(draft, mode="create"),
            build_rule_builder_markup(draft, mode="create", source_id=source_id),
        )
        await callback.answer()

    @router.callback_query(F.data == "rb:back")
    async def rule_builder_back(callback: CallbackQuery, state: FSMContext) -> None:
        await show_rule_builder(callback, state)

    @router.callback_query(F.data.in_({"rb:fields", "rb:kinds", "rb:transforms"}))
    async def rule_builder_picker(callback: CallbackQuery, state: FSMContext) -> None:
        loaded = await rule_draft_state(state)
        if loaded is None:
            await callback.answer(RULE_DRAFT_EXPIRED, show_alert=True)
            return
        which = cast(str, callback.data).split(":", 1)[1]
        options: list[tuple[str, str]] = []
        if which == "fields":
            title = "Какое поле товара заполняет правило?"
            options = [(str(name), f"rb:field:{name}") for name in PRODUCT_FIELD_NAMES]
            options.append(("✏️ Другое поле", "rb:ask:field"))
        elif which == "kinds":
            title = (
                "Как искать значение? regex - обычный выбор, "
                "label/keyword ищут по подписи, exact сравнивает строку целиком."
            )
            options = [(kind, f"rb:kind:{kind}") for kind in RULE_KINDS]
        else:
            title = (
                "Что делать с найденным значением? identity - оставить как есть, "
                "series_sizes/series_quantity разбирают размерный ряд, map применяет замены. "
                "normalize_number убирает пробелы внутри числа и заменяет запятую точкой "
                "(9 .75 → 9.75; 1 4 → 14). Для price это также выполняется автоматически."
            )
            options = [(name, f"rb:transform:{name}") for name in RULE_TRANSFORMS]
        rows = [options[start : start + 2] for start in range(0, len(options), 2)]
        rows.append([("⬅️ К правилу", "rb:back")])
        await edit_screen(callback, title, keyboard(*rows))
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^rb:(field|kind|transform):[a-z][a-z0-9_]{0,31}$"))
    async def rule_builder_set(callback: CallbackQuery, state: FSMContext) -> None:
        loaded = await rule_draft_state(state)
        if loaded is None:
            await callback.answer(RULE_DRAFT_EXPIRED, show_alert=True)
            return
        draft, source_id, mode = loaded
        _, setting, value = cast(str, callback.data).split(":", 2)
        draft[setting] = value
        await state.update_data(rule_draft=draft, rule_awaiting=None)
        await edit_screen(
            callback,
            rule_builder_text(draft, mode=mode),
            build_rule_builder_markup(draft, mode=mode, source_id=source_id),
        )
        await callback.answer(f"{setting} = {value}")

    @router.callback_query(F.data.regexp(r"^rb:toggle:(ignore_case|authoritative|enabled)$"))
    async def rule_builder_toggle(callback: CallbackQuery, state: FSMContext) -> None:
        loaded = await rule_draft_state(state)
        if loaded is None:
            await callback.answer(RULE_DRAFT_EXPIRED, show_alert=True)
            return
        draft, source_id, mode = loaded
        flag = cast(str, callback.data).split(":", 2)[2]
        draft[flag] = "false" if draft.get(flag) == "true" else "true"
        await state.update_data(rule_draft=draft, rule_awaiting=None)
        await edit_screen(
            callback,
            rule_builder_text(draft, mode=mode),
            build_rule_builder_markup(draft, mode=mode, source_id=source_id),
        )
        await callback.answer(f"{flag} = {draft[flag]}")

    @router.callback_query(F.data.startswith("rb:ask:"))
    async def rule_builder_ask(callback: CallbackQuery, state: FSMContext) -> None:
        loaded = await rule_draft_state(state)
        if loaded is None:
            await callback.answer(RULE_DRAFT_EXPIRED, show_alert=True)
            return
        draft, _, _ = loaded
        setting = cast(str, callback.data).split(":", 2)[2]
        prompt = RULE_SETTING_PROMPTS.get(setting)
        if prompt is None:
            await callback.answer("Эта настройка меняется отдельной кнопкой.", show_alert=True)
            return
        await state.update_data(rule_awaiting=setting)
        current = draft.get(setting, "")
        await edit_screen(
            callback,
            f"{prompt}\n\nСейчас: {current or 'не задано'}",
            keyboard([("⬅️ К правилу", "rb:back")]),
        )
        await callback.answer()

    @router.callback_query(F.data == "rb:test")
    async def rule_builder_test(callback: CallbackQuery, state: FSMContext) -> None:
        loaded = await rule_draft_state(state)
        if loaded is None:
            await callback.answer(RULE_DRAFT_EXPIRED, show_alert=True)
            return
        draft, _, _ = loaded
        try:
            rule_from_draft(draft)
        except ValueError as exc:
            await callback.answer(f"Сначала правило: {exc}"[:190], show_alert=True)
            return
        await state.update_data(rule_awaiting="test")
        await edit_screen(
            callback,
            "Пришлите текст поста (можно целиком) - проверю правило на нём тем же движком, "
            "что работает на реальных постах.\nОтвет: MATCH или NO MATCH и найденное value.",
            keyboard([("⬅️ К правилу", "rb:back")]),
        )
        await callback.answer()

    @router.callback_query(F.data == "rb:save")
    async def rule_builder_save(callback: CallbackQuery, state: FSMContext) -> None:
        loaded = await rule_draft_state(state)
        if loaded is None:
            await callback.answer(RULE_DRAFT_EXPIRED, show_alert=True)
            return
        draft, source_id, mode = loaded
        data = await state.get_data()
        try:
            saved = await save_rule_draft(source_id, draft, mode, data)
        except ValueError as exc:
            await callback.answer(f"Не сохранено: {exc}"[:190], show_alert=True)
            return
        except LookupError as exc:
            await state.clear()
            await render_source_rules(
                callback, source_id, notice=_stale_notice(exc) or "Источник не найден."
            )
            return
        await state.clear()
        await edit_screen(callback, saved, keyboard([("Все правила", f"source:rules:{source_id}")]))
        await callback.answer("Готово")

    async def run_rule_preview(
        message: Message,
        state: FSMContext,
        draft: dict[str, str],
        *,
        source_id: int,
        mode: str,
        sample: str,
    ) -> None:
        """🧪 Проверить regex: the production engine, one rule, admin-supplied text."""
        try:
            field, rule = rule_from_draft(draft)
        except ValueError as exc:
            await message.answer(
                f"Правило ещё не готово: {exc}",
                reply_markup=keyboard([("⬅️ К правилу", "rb:back")]),
            )
            return
        async with sessions() as session:
            source = await session.get(Source, source_id)
            profile = (
                SourceProfileData.model_validate(source.profile.config)
                if source is not None
                else SourceProfileData()
            )
            alias_rows = list(
                await session.scalars(select(CurrencyAlias).where(CurrencyAlias.enabled.is_(True)))
            )
        preview = preview_extraction_rule(
            profile,
            field,
            rule,
            sample,
            CurrencyAliasResolver({item.alias: item.currency for item in alias_rows}),
        )
        lines = ["🧪 MATCH" if preview.matched else "🧪 NO MATCH"]
        if preview.matched:
            for index, (value, matched) in enumerate(
                zip(preview.values, preview.matches, strict=False)
            ):
                lines.append(f"value = {value}")
                lines.append(f"Captured: {preview.captures[index]}")
                lines.append(f"Normalized: {preview.normalized_values[index]}")
                lines.append(f"Currency: {preview.currencies[index] or 'MISSING'}")
                lines.append(f"Candidate: {preview.candidate_statuses[index].value.upper()}")
                lines.append(f"совпало: {matched.strip()[:120]}")
            lines.append(f"Flags: {', '.join(preview.flags) or 'none'}")
            lines.append(f"всего совпадений: {len(preview.values)}")
        else:
            lines.append(f"Правило не нашло значение для поля {field} в этом тексте.")
        if preview.cleaned_text != sample:
            lines.append("Текст после правил очистки источника:")
            lines.append(preview.cleaned_text[:400])
        await state.update_data(rule_awaiting=None)
        await answer_rule_builder(
            message, draft, mode=mode, source_id=source_id, notice="\n".join(lines)
        )

    async def rule_builder_reply(
        message: Message,
        state: FSMContext,
        draft: dict[str, str],
        *,
        source_id: int,
        mode: str,
        awaiting: str,
    ) -> None:
        """Store one setting typed after a ✏️ button, or run the regex test."""
        raw = (
            message.text or ""
            if awaiting in {"pattern", "token_pattern"}
            else normalize_admin_input(message.text or "")
        )
        if awaiting == "test":
            await run_rule_preview(
                message, state, draft, source_id=source_id, mode=mode, sample=raw
            )
            return
        value = (
            ""
            if awaiting == "token_pattern" and raw.strip() == "-"
            else raw
            if awaiting in {"pattern", "token_pattern"}
            else ""
            if raw.strip() == "-"
            else raw.strip()
        )
        try:
            if awaiting == "field":
                parse_source_field_name(value)
            else:
                apply_extraction_rule_settings(
                    {"kind": draft.get("kind", "regex")}, {awaiting: value}
                )
        except ValueError as exc:
            await message.answer(
                f"Не принято: {exc}\n\nПришлите значение ещё раз или вернитесь кнопкой ниже.",
                reply_markup=keyboard([("⬅️ К правилу", "rb:back")]),
            )
            return
        draft[awaiting] = value
        await state.update_data(rule_draft=draft, rule_awaiting=None)
        await answer_rule_builder(
            message,
            draft,
            mode=mode,
            source_id=source_id,
            notice=f"{awaiting} сохранён в черновике.",
        )

    @router.message(AdminStates.source_rule)
    async def source_rule_save(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        source_id = int(data.get("source_id") or 0)
        raw_draft = data.get("rule_draft")
        draft = (
            {str(key): str(value) for key, value in raw_draft.items()}
            if isinstance(raw_draft, dict)
            else empty_rule_draft()
        )
        awaiting = data.get("rule_awaiting")
        if awaiting:
            await rule_builder_reply(
                message,
                state,
                draft,
                source_id=source_id,
                mode="create",
                awaiting=str(awaiting),
            )
            return
        try:
            values = parse_extraction_rule_values(message.text or "")
            if values is None:
                field_name, rule = parse_extraction_rule_input(message.text or "")
                draft = rule_draft_from_rule(field_name, rule)
            else:
                draft.update(values)
        except ValueError as exc:
            await answer_rule_builder(
                message,
                draft,
                mode="create",
                source_id=source_id,
                notice=f"Ошибка правила: {exc}",
            )
            return
        await state.update_data(rule_draft=draft)
        try:
            saved = await save_rule_draft(source_id, draft, "create", data)
        except ValueError as exc:
            missing = missing_rule_settings(draft)
            await answer_rule_builder(
                message,
                draft,
                mode="create",
                source_id=source_id,
                notice=(
                    "Ещё не хватает: " + ", ".join(missing) if missing else f"Ошибка правила: {exc}"
                ),
            )
            return
        except LookupError as exc:
            await state.clear()
            await message.answer(
                f"Источник не найден: {exc}",
                reply_markup=keyboard([("Источники", "menu:sources")]),
            )
            return
        await state.clear()
        await message.answer(
            saved, reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")])
        )

    @router.callback_query(F.data.regexp(r"^r:e:\d+:[a-z][a-z0-9_]{0,31}:\d+:[0-9a-f]{10}$"))
    async def source_rule_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, field, index_text, token = cast(str, callback.data).split(":", 5)
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        rules = profile.extraction_rules.get(field, [])
        if index < 0 or index >= len(rules):
            await render_source_rules(callback, source_id, notice="Правило уже отсутствует.")
            return
        if profile_item_token(rules[index]) != token:
            await render_source_rules(callback, source_id, notice="Правило уже изменилось.")
            return
        rule = rules[index]
        draft = rule_draft_from_rule(field, rule)
        await state.set_state(AdminStates.source_rule_edit)
        await state.update_data(
            source_id=source_id,
            field=field,
            index=index,
            rule_token=token,
            rule_draft=draft,
            rule_mode="edit",
            rule_awaiting=None,
        )
        await edit_screen(
            callback,
            rule_builder_text(draft, mode="edit"),
            build_rule_builder_markup(draft, mode="edit", source_id=source_id),
        )
        await callback.answer()

    @router.message(AdminStates.source_rule_edit)
    async def source_rule_edit_save(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        source_id = int(data.get("source_id") or 0)
        raw_draft = data.get("rule_draft")
        draft = (
            {str(key): str(value) for key, value in raw_draft.items()}
            if isinstance(raw_draft, dict)
            else {}
        )
        awaiting = data.get("rule_awaiting")
        if awaiting:
            await rule_builder_reply(
                message,
                state,
                draft,
                source_id=source_id,
                mode="edit",
                awaiting=str(awaiting),
            )
            return
        try:
            old_field = str(data["field"])
            index = int(data["index"])
            expected_token = str(data["rule_token"])
            async with sessions() as session:
                source = await session.get(Source, source_id)
                if source is None:
                    raise LookupError("source not found")
                profile = SourceProfileData.model_validate(source.profile.config)
                current_rules = profile.extraction_rules.get(old_field, [])
                if index < 0 or index >= len(current_rules):
                    raise LookupError("source extraction rule not found")
                current_rule = current_rules[index]
                if profile_item_token(current_rule) != expected_token:
                    raise LookupError("source extraction rule not found")
            base_field, base_rule = old_field, current_rule
            if draft:
                # Button changes already made on this screen must not be lost.
                try:
                    base_field, base_rule = rule_from_draft(draft)
                except ValueError:
                    base_field, base_rule = old_field, current_rule
            new_field, rule = parse_extraction_rule_edit_input(
                message.text or "",
                current_field=base_field,
                current_rule=base_rule,
            )
            async with sessions() as session, session.begin():
                await AdminService(session).replace_source_rule(
                    source_id,
                    old_field,
                    index,
                    new_field,
                    rule,
                    actor_id=None,
                    expected_token=expected_token,
                )
            await state.clear()
            await message.answer(
                "Правило обновлено.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except LookupError as exc:
            await state.clear()
            await message.answer(
                f"Правило не найдено: {exc}",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except (ValueError, KeyError) as exc:
            await answer_rule_builder(
                message,
                draft,
                mode="edit",
                source_id=source_id,
                notice=f"Ошибка правила: {exc}",
            )

    @router.callback_query(F.data.startswith("source:computed:"))
    async def source_computed_start(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        await edit_screen(
            callback,
            "Выберите понятный тип вычисления. Правило действует только для этого источника:",
            keyboard(
                [
                    (
                        "Количество в размерном ряду",
                        f"source:computed-op:{source_id}:count_series_items",
                    )
                ],
                [
                    (
                        "Количество элементов",
                        f"source:computed-op:{source_id}:count_items",
                    )
                ],
                [
                    (
                        "Сумма значений",
                        f"source:computed-op:{source_id}:sum_matches",
                    )
                ],
                [
                    (
                        "Размер + количество",
                        f"source:computed-op:{source_id}:sum_size_counts",
                    )
                ],
                [
                    (
                        "Уникальные значения",
                        f"source:computed-op:{source_id}:unique_matches",
                    )
                ],
                [("Отмена", f"source:rules:{source_id}")],
            ),
        )
        await callback.answer()

    @router.callback_query(
        F.data.regexp(
            r"^source:computed-op:\d+:(count_items|count_series_items|sum_matches|sum_size_counts|unique_matches)$"
        )
    )
    async def source_computed_operation(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, operation = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        await state.update_data(source_id=source_id, operation=operation, input_source=None)
        await state.set_state(AdminStates.source_computed)
        input_rows = [
            [
                (
                    "Исходный текст",
                    f"source:computed-input:{source_id}:canonical_text",
                )
            ],
            [
                ("Значение поля (final)", f"source:computed-input:{source_id}:field"),
                (
                    "Raw candidate поля",
                    f"source:computed-input:{source_id}:raw_field_candidate",
                ),
            ],
            [("Отмена", f"source:rules:{source_id}")],
        ]
        await edit_screen(
            callback,
            "Выберите, откуда вычисляемое правило получает input.\n\n"
            "Исходный текст — canonical product text до field merge.\n"
            "Значение поля — уже выбранное final field value.\n"
            "Raw candidate поля — bounded значение extraction rule до transform/normalization.\n\n"
            "Старые правила можно по-прежнему отправить сразу в прежнем key=value формате; "
            "input_mode=raw сохраняет прежнее значение Raw candidate поля.",
            keyboard(*input_rows),
        )
        await callback.answer()

    @router.callback_query(
        F.data.regexp(r"^source:computed-input:\d+:(field|raw_field_candidate|canonical_text)$")
    )
    async def source_computed_input(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, input_source = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        data = await state.get_data()
        operation = str(data.get("operation") or "")
        if int(data.get("source_id") or 0) != source_id or operation not in {
            "count_items",
            "count_series_items",
            "sum_matches",
            "sum_size_counts",
            "unique_matches",
        }:
            await callback.answer("Начните создание правила заново.", show_alert=True)
            return
        await state.update_data(input_source=input_source)
        examples = {
            "count_series_items": (
                "name=count-size-series\ntarget_field=quantity\nsource_field=sizes\n"
                "series_separator_pattern=\\s+\n"
                "range_pattern=(?P&lt;start&gt;\\d+)-(?P&lt;end&gt;\\d+)\n"
                "explicit_list_pattern=(?P&lt;item&gt;\\d+)\n"
                "explicit_separator_pattern=-\nnumeric_step=2\n"
                "compact_range_pattern=(?P&lt;start&gt;\\d{2})(?P&lt;end&gt;\\d{2})"
            ),
            "count_items": (
                "name=count-sizes\ntarget_field=quantity\nsource_field=sizes\n"
                "pattern=(?P&lt;item&gt;\\d+)\ncapture_group=item"
            ),
            "sum_matches": (
                "name=sum-units\ntarget_field=quantity\n"
                "pattern=(?P&lt;units&gt;\\d+)\nvalue_group=units"
            ),
            "sum_size_counts": (
                "name=size-count-pairs\ntarget_field=quantity\n"
                "secondary_target_field=sizes\n"
                "pattern=(?P&lt;size&gt;[A-Z]+)(?P&lt;count&gt;\\d+)\n"
                "capture_group=size\nvalue_group=count\nseparator=space"
            ),
            "unique_matches": (
                "name=unique-sizes\ntarget_field=sizes\n"
                "pattern=(?P&lt;size&gt;[A-Z]+)\ncapture_group=size\nseparator=space"
            ),
        }
        source_labels = {
            "canonical_text": "Исходный текст",
            "field": "Значение поля (final)",
            "raw_field_candidate": "Raw candidate поля",
        }
        source_setting = f"input_source={input_source}"
        example = examples[operation]
        if input_source == "canonical_text":
            example = re.sub(r"(?m)^source_field=.*\n?", "", example)
        elif "source_field=" not in example:
            example = "source_field=sizes\n" + example
        example = source_setting + "\n" + example
        await edit_screen(
            callback,
            f"Input source: {source_labels[input_source]}\n"
            "Введите параметры строками key=value (без JSON). Выбранная операция: "
            f"<code>{operation}</code>\n\n<code>{example}</code>\n\n"
            "Дополнительно: priority=10, ignore_case=true, authoritative=true|false.\n"
            "Для count_series_items выберите значение поля или Raw candidate (включая Repeated fields); "
            "series_separator_pattern делит альтернативные ряды, numeric_step задаёт шаг.\n"
            "Конфликт: conflict_policy=explicit (оставить явное), computed "
            "(взять вычисленное), ambiguous (Уточнить / Check).\n"
            "Разделитель: space, comma_space, slash_space или свой текст.\n"
            "После сохранения кнопка 🧪 проверяет pattern на любом canonical text.",
            keyboard([("Отмена", f"source:rules:{source_id}")]),
        )
        await callback.answer()

    @router.message(AdminStates.source_computed)
    async def source_computed_save(message: Message, state: FSMContext) -> None:
        try:
            data = await state.get_data()
            source_id = int(data["source_id"])
            operation = str(data["operation"])
            supplied = message.text or ""
            input_source = data.get("input_source")
            supplied_has_input_source = (
                re.search(r"(?m)(?:^|\s)input_source[ \t]*=", supplied) is not None
            )
            selected = (
                f"input_source={input_source}\n"
                if input_source and not supplied_has_input_source
                else ""
            )
            payload = f"operation={operation}\n{selected}{supplied}"
            rule = parse_computed_rule_input(payload)
            if input_source and rule.input_source != input_source:
                raise ValueError("input_source does not match the selected button")
            async with sessions() as session, session.begin():
                await AdminService(session).add_computed_rule(source_id, rule, actor_id=None)
            await state.clear()
            await message.answer(
                "Вычисляемое правило сохранено. Оно выполняется до Gemini.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Ошибка вычисляемого правила: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^source:computed-edit:\d+:\d+:[0-9a-f]{10}$"))
    async def source_computed_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.computed_rules):
            await render_source_rules(callback, source_id, notice="Правило уже отсутствует.")
            return
        rule = profile.computed_rules[index]
        if profile_item_token(rule) != token:
            await render_source_rules(callback, source_id, notice="Правило уже изменилось.")
            return
        separator_alias = {
            " ": "space",
            ",": "comma",
            ", ": "comma_space",
            "/": "slash",
            " / ": "slash_space",
        }.get(rule.separator, rule.separator)
        lines = [
            f"name={rule.name}",
            f"target_field={rule.target_field}",
            f"operation={rule.operation}",
        ]
        if rule.source_field is not None:
            lines.append(f"source_field={rule.source_field}")
        if rule.input_source is not None:
            lines.append(f"input_source={rule.input_source}")
        else:
            lines.append(f"input_mode={rule.input_mode}")
        if rule.secondary_target_field is not None:
            lines.append(f"secondary_target_field={rule.secondary_target_field}")
        if rule.pattern is not None:
            lines.append(f"pattern={rule.pattern}")
        for key in (
            "series_separator_pattern",
            "range_pattern",
            "explicit_list_pattern",
            "explicit_separator_pattern",
            "compact_range_pattern",
        ):
            value = getattr(rule, key)
            if value is not None:
                lines.append(f"{key}={value}")
        lines.extend(
            [
                f"range_start_group={rule.range_start_group}",
                f"range_end_group={rule.range_end_group}",
                f"explicit_item_group={rule.explicit_item_group}",
            ]
        )
        if rule.numeric_step is not None:
            lines.append(f"numeric_step={rule.numeric_step}")
        lines.extend(
            [
                f"capture_group={rule.capture_group}",
                f"value_group={rule.value_group}",
                f"separator={separator_alias}",
                f"priority={rule.priority}",
                f"ignore_case={'true' if rule.ignore_case else 'false'}",
                f"authoritative={'true' if rule.authoritative else 'false'}",
                f"conflict_policy={rule.conflict_policy}",
            ]
        )
        await state.update_data(source_id=source_id, index=index, rule_token=token)
        await state.set_state(AdminStates.source_computed_edit)
        await edit_screen(
            callback,
            "Текущее вычисляемое правило (enabled меняется кнопкой ⏯):\n\n"
            f"<code>{html.escape(chr(10).join(lines))}</code>\n\n"
            "Отправьте полную новую версию строками key=value:",
            keyboard([("Отмена", f"source:rules:{source_id}")]),
        )
        await callback.answer()

    @router.message(AdminStates.source_computed_edit)
    async def source_computed_edit_save(message: Message, state: FSMContext) -> None:
        try:
            data = await state.get_data()
            source_id = int(data["source_id"])
            index = int(data["index"])
            token = str(data["rule_token"])
            rule = parse_computed_rule_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).replace_computed_rule(
                    source_id,
                    index,
                    rule,
                    actor_id=None,
                    expected_token=token,
                )
            await state.clear()
            await message.answer(
                "Вычисляемое правило обновлено.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except LookupError as exc:
            await message.answer(
                f"Правило не найдено: {html.escape(str(exc))}",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Ошибка вычисляемого правила: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^source:computed-test:\d+:\d+:[0-9a-f]{10}$"))
    async def source_computed_test_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.computed_rules):
            await callback.answer("Правило уже отсутствует", show_alert=True)
            return
        rule = profile.computed_rules[index]
        if profile_item_token(rule) != token:
            await callback.answer("Правило уже изменилось", show_alert=True)
            return
        await state.clear()
        await state.update_data(source_id=source_id, index=index, rule_token=token)
        await state.set_state(AdminStates.source_computed_test)
        await edit_screen(
            callback,
            "🧪 Tester вычисляемого правила\n"
            f"Rule: {html.escape(rule.name)}\n"
            f"Input source: {rule.resolved_input_source()}\n"
            f"Pattern: {html.escape(rule.pattern or '-')}\n\n"
            "Отправьте canonical product text. Профиль не изменяется.",
            keyboard([("Отмена", f"source:rules:{source_id}")]),
        )
        await callback.answer()

    @router.message(AdminStates.source_computed_test)
    async def source_computed_test_run(message: Message, state: FSMContext) -> None:
        try:
            data = await state.get_data()
            source_id = int(data["source_id"])
            index = int(data["index"])
            token = str(data["rule_token"])
            async with sessions() as session:
                source = await session.get(Source, source_id)
                if source is None:
                    raise LookupError("source not found")
                profile = SourceProfileData.model_validate(source.profile.config)
                alias_rows = list(
                    await session.scalars(
                        select(CurrencyAlias).where(CurrencyAlias.enabled.is_(True))
                    )
                )
            if index < 0 or index >= len(profile.computed_rules):
                raise LookupError("computed rule not found")
            rule = profile.computed_rules[index]
            if profile_item_token(rule) != token:
                raise LookupError("computed rule changed")
            test_profile = profile.model_copy(
                update={"computed_rules": [rule.model_copy(update={"enabled": True})]}
            )
            parsed = RuleEngine(
                test_profile,
                CurrencyAliasResolver({item.alias: item.currency for item in alias_rows}),
            ).parse(message.text or "")
            candidate = parsed.fields.get(rule.target_field)
            metadata = candidate.metadata if candidate is not None else {}
            matches = metadata.get("pattern_matches")
            shown_matches = (
                "[" + ", ".join(str(value) for value in matches) + "]"
                if isinstance(matches, list)
                else "[]"
            )
            count = metadata.get("match_count")
            await state.clear()
            await message.answer(
                "🧪 Computed rule result\n"
                f"Input source: {rule.resolved_input_source()}\n"
                f"Pattern matches: {html.escape(shown_matches)}\n"
                f"Count: {count if isinstance(count, int) else 0}\n"
                f"Candidate: {candidate.status.value.upper() if candidate is not None else 'MISSING'}\n"
                f"Value: {html.escape(candidate.value) if candidate and candidate.value else '-'}\n"
                "Provenance: "
                f"{candidate.provenance.value if candidate and candidate.provenance else '-'}",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await state.clear()
            await message.answer(f"Tester error: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:repeated:"))
    async def source_repeated_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(source_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.source_repeated)
        await callback.message.edit_text(
            "Repeated pattern без JSON:\nname=range-price\n"
            "pattern=(?m)^(?P&lt;size&gt;\\d+-\\d+)-(?P&lt;price&gt;\\d+)$\n"
            "fields=sizes=&gt;size;price=&gt;price\n"
            "aggregation=sizes=&gt;unique;price=&gt;join_unique\n"
            "separators=price=&gt;/\npriority=10\nauthoritative=true"
        )
        await callback.answer()

    @router.message(AdminStates.source_repeated)
    async def source_repeated_save(message: Message, state: FSMContext) -> None:
        try:
            source_id = int((await state.get_data())["source_id"])
            rule = parse_repeated_pattern_rule_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).add_repeated_pattern_rule(
                    source_id, rule, actor_id=None
                )
            await state.clear()
            await message.answer(
                "Repeated pattern сохранён.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Repeated pattern не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^source:repeated-del:\d+:\d+:[0-9a-f]{10}$"))
    async def source_repeated_delete(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_repeated_pattern_rule(
                    source_id, index, actor_id=None, expected_token=token
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.startswith("source:boundary:"))
    async def source_boundary_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(source_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.source_boundary)
        await callback.message.edit_text(
            "Граница товара (строками key=value, без JSON):\n"
            "name=next-product\n"
            "action=start|continue|end|hard_boundary|soft_boundary|ignore\n"
            "match=regex|keyword|has_text|has_media|captioned_media|grouped_id_change|always\n"
            "pattern=regex (для match=regex)\n"
            "keywords=word1,word2 (для match=keyword)\n"
            "target=incoming|current|combined\n"
            "ignore_case=true\nreason=описание\n\n"
            "Пример «новый grouped_id = новый товар»:\n"
            "name=new-album\naction=start\nmatch=grouped_id_change\n\n"
            "/cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.source_boundary)
    async def source_boundary_save(message: Message, state: FSMContext) -> None:
        try:
            source_id = int((await state.get_data())["source_id"])
            rule = parse_collection_boundary_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).add_collection_boundary(source_id, rule, actor_id=None)
            await state.clear()
            await message.answer(
                "Граница сохранена.",
                reply_markup=keyboard([("Сбор товара", f"source:collect:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Boundary не сохранена: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:boundary-del:"))
    async def source_boundary_delete(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).split(":")[2])
        await show_collection_screen(
            callback,
            source_id,
            notice="Кнопка устарела; показаны актуальные границы.",
        )

    @router.callback_query(F.data.startswith("source:clean-rule:"))
    async def source_clean_rule_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(source_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.source_clean_rule)
        await callback.message.edit_text(
            "Создание нового Cleaning rule (без JSON):\n"
            "kind=exact|contains|prefix|suffix|regex\n"
            "value=Service:\nreplacement=\nscope=line|text\n"
            "language=both|ru|en\nignore_case=true|false\nenabled=true|false"
        )
        await callback.answer()

    @router.message(AdminStates.source_clean_rule)
    async def source_clean_rule_save(message: Message, state: FSMContext) -> None:
        try:
            source_id = int((await state.get_data())["source_id"])
            rule = parse_cleaning_rule_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).add_cleaning_rule(source_id, rule, actor_id=None)
            await state.clear()
            await message.answer(
                "Cleaning rule сохранено.",
                reply_markup=keyboard([("Очистка", f"source:clean:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Cleaning rule не сохранено: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^source:clean-edit:\d+:\d+:[0-9a-f]{10}$"))
    async def source_clean_rule_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден или удалён.", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.cleaning_rules):
            await render_cleaning_screen(
                callback,
                source_id,
                notice="Кнопка устарела: правило уже отсутствует.",
            )
            return
        rule = profile.cleaning_rules[index]
        if profile_item_token(rule) != token:
            await render_cleaning_screen(
                callback,
                source_id,
                notice="Кнопка устарела: правило уже изменено.",
            )
            return
        await state.update_data(
            source_id=source_id,
            cleaning_rule_index=index,
            cleaning_rule_token=token,
        )
        await state.set_state(AdminStates.source_clean_rule_edit)
        current = html.escape(cleaning_rule_editor_text(rule))
        await callback.message.edit_text(
            "Редактирование существующего Cleaning rule.\n"
            "Текущие значения:\n"
            f"<code>{current}</code>\n\n"
            "Отправьте полный обновлённый набор key=value. /cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.source_clean_rule_edit)
    async def source_clean_rule_edit_save(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        source_id = int(data["source_id"])
        try:
            rule = parse_cleaning_rule_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).replace_cleaning_rule(
                    source_id,
                    int(data["cleaning_rule_index"]),
                    rule,
                    actor_id=None,
                    expected_token=str(data["cleaning_rule_token"]),
                )
        except LookupError:
            await state.clear()
            await message.answer(
                "Cleaning rule не изменено: правило уже изменилось или удалено. "
                "Откройте актуальный список.",
                reply_markup=keyboard([("Очистка", f"source:clean:{source_id}")]),
            )
            return
        except Exception as exc:
            await message.answer(f"Cleaning rule не изменено: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Cleaning rule изменено.",
            reply_markup=keyboard([("Очистка", f"source:clean:{source_id}")]),
        )

    @router.callback_query(F.data.regexp(r"^source:clean-del:\d+:\d+:[0-9a-f]{10}$"))
    async def source_clean_rule_delete(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_cleaning_rule(
                    source_id, index, actor_id=None, expected_token=token
                )
        except LookupError as exc:
            await render_cleaning_screen(callback, source_id, notice=_stale_notice(exc))
            return
        await render_cleaning_screen(callback, source_id)

    @router.callback_query(F.data.startswith("source:rules:"))
    async def source_rules(callback: CallbackQuery) -> None:
        await render_source_rules(callback, int(cast(str, callback.data).rsplit(":", 1)[1]))

    async def render_fixed_fields(callback: CallbackQuery, source_id: int) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None or source.archived_at is not None:
                await callback.answer("Источник не найден или архивирован", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        rows = []
        lines = [f"Фиксированные значения — {html.escape(source.name)} (ID {source_id})", ""]
        fixed_values: dict[str, str] = {key: value for key, value in profile.fixed_fields.items()}
        for field, label in (("sizes", "Размеры / Sizes"), ("quantity", "Количество / Quantity")):
            value = fixed_values.get(field)
            lines.append(f"{label}: {html.escape(value) if value else 'выключено'}")
            row = [(f"✏️ {label}", f"source:fixed-set:{source_id}:{field}")]
            if value is not None:
                row.append(("Отключить", f"source:fixed-off:{source_id}:{field}"))
            rows.append(row)
        lines.append(
            "\nДействуют только для этого источника, поверх текста, правил и Gemini/OCR, "
            "включая фото и альбомы без подписи. Отключение возвращает обычное извлечение. "
            "Количество задаётся отдельно и не вычисляется из размеров.\n\n"
            "Применяются при следующей обработке товара; старые публикации автоматически не меняются. "
            "Настройки сбора, ignore и альтернативного текста продолжают действовать."
        )
        rows.append([("⬅️ Источник", f"source:{source_id}")])
        await edit_screen(callback, "\n".join(lines), keyboard(*rows))
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:fixed:\d+$"))
    async def fixed_fields_screen(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await render_fixed_fields(callback, int(cast(str, callback.data).rsplit(":", 1)[1]))

    @router.callback_query(F.data.regexp(r"^source:fixed-set:\d+:(sizes|quantity)$"))
    async def fixed_field_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, field = cast(str, callback.data).split(":")
        async with sessions() as session:
            source = await session.get(Source, int(source_text))
            if source is None or source.archived_at is not None:
                await callback.answer("Источник не найден или архивирован", show_alert=True)
                return
        await state.set_state(AdminStates.source_fixed)
        await state.update_data(fixed_source_id=int(source_text), fixed_field=field)
        example = "S M L XL XXL" if field == "sizes" else "5"
        await edit_screen(
            callback, f"Источник: {html.escape(source.name)} (ID {source_text})\n"
            f"Введите фиксированное значение {field}, например: {example}\n"
            "Для отключения отправьте «-». /cancel — отмена.",
            keyboard([("⬅️ Фиксированные значения", f"source:fixed:{source_text}")]),
        )
        await callback.answer()

    @router.message(AdminStates.source_fixed)
    async def fixed_field_save(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        source_id = int(data["fixed_source_id"])
        value = (message.text or "").strip()
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).set_source_fixed_field(
                    source_id, str(data["fixed_field"]), None if value == "-" else value,
                    actor_id=current_admin_id.get(),
                )
        except (ValueError, LookupError) as exc:
            await message.answer(f"Не сохранено: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Фиксированное значение отключено." if value == "-" else "Фиксированное значение сохранено.",
            reply_markup=keyboard([("Фиксированные значения", f"source:fixed:{source_id}")]),
        )

    @router.callback_query(F.data.regexp(r"^source:fixed-off:\d+:(sizes|quantity)$"))
    async def fixed_field_disable(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, field = cast(str, callback.data).split(":")
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).set_source_fixed_field(
                    int(source_text), field, None, actor_id=current_admin_id.get(),
                )
        except (ValueError, LookupError) as exc:
            await callback.answer(str(exc)[:180], show_alert=True)
            return
        await state.clear()
        await render_fixed_fields(callback, int(source_text))

    async def render_field_modes(
        callback: CallbackQuery, source_id: int, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден или удалён.", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        raw_fields = [
            field
            for field, policy in profile.field_policies.items()
            if policy.processing_mode == "passthrough"
        ]
        raw_summary = ", ".join(raw_fields) if raw_fields else "нет"
        await edit_screen(
            callback,
            "Режим обработки полей\n\n"
            "Normal — обычное извлечение, transforms и validation.\n"
            "RAW / Passthrough — сохраняет только bounded candidate выбранного поля "
            "без normal normalization/semantic validation. Evidence, provenance и "
            "conflict policy продолжают действовать.\n\n"
            "Границы значения задаются обычным extraction rule (regex capture/label); "
            "весь supplier post автоматически не копируется.\n\n"
            f"RAW поля: {html.escape(raw_summary)}",
            build_field_modes_markup(source_id, profile),
        )
        await callback.answer(notice or "", show_alert=notice is not None)

    @router.callback_query(F.data.regexp(r"^source:field-modes:\d+$"))
    async def source_field_modes(callback: CallbackQuery) -> None:
        await render_field_modes(callback, int(cast(str, callback.data).rsplit(":", 1)[1]))

    @router.callback_query(
        F.data.regexp(r"^source:field-mode:\d+:[a-z][a-z0-9_]{0,31}:(normal|passthrough)$")
    )
    async def source_field_mode_set(callback: CallbackQuery) -> None:
        _, _, source_text, field, mode = cast(str, callback.data).split(":", 4)
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).set_field_processing_mode(
                    source_id,
                    field,
                    cast(Any, mode),
                    actor_id=None,
                )
        except LookupError as exc:
            await render_field_modes(callback, source_id, notice=_stale_notice(exc))
            return
        except ValueError as exc:
            await render_field_modes(callback, source_id, notice=str(exc))
            return
        await render_field_modes(callback, source_id)

    async def render_source_rules(
        callback: CallbackQuery, source_id: int, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        lines = [
            f"Источник: {source.name} (ID {source.id})\n"
            "Правила выбранного источника (меньший priority выполняется первым):\n"
            "product_name — название товара; name у правила — его идентификатор."
        ]
        for policy_field, policy in sorted(profile.field_policies.items()):
            if policy.processing_mode == "passthrough":
                lines.append(f"{policy_field}: RAW / Passthrough")
        for extraction_field, rules in sorted(profile.extraction_rules.items()):
            for index, rule in enumerate(rules):
                lines.append(
                    f"{extraction_field} #{index + 1}: {rule.kind}/{rule.transform}, "
                    f"priority={rule.priority}, authoritative={rule.authoritative}, "
                    f"enabled={'вкл' if rule.enabled else 'выкл'}\n"
                    f"pattern={rule.pattern[:240]}"
                )
        operation_labels = {
            "count_items": "Количество элементов",
            "count_series_items": "Количество в размерном ряду",
            "sum_matches": "Сумма значений",
            "sum_size_counts": "Размер + количество",
            "unique_matches": "Уникальные значения",
        }
        for computed_rule in profile.computed_rules:
            targets = computed_rule.target_field
            if computed_rule.secondary_target_field:
                targets += f" + {computed_rule.secondary_target_field}"
            lines.append(
                f"computed {computed_rule.name}: "
                f"{operation_labels[computed_rule.operation]} → {targets}, "
                f"input={computed_rule.resolved_input_source()}, "
                f"priority={computed_rule.priority}, conflict={computed_rule.conflict_policy}, "
                f"authoritative={computed_rule.authoritative}, "
                f"enabled={'вкл' if computed_rule.enabled else 'выкл'}"
                + (f"\npattern={computed_rule.pattern[:240]}" if computed_rule.pattern else "")
            )
        for fallback_field, fallback in sorted(profile.fallback_rules.items()):
            lines.append(
                f"fallback {fallback_field}={fallback.value}, "
                f"authoritative={fallback.authoritative}"
            )
        for ignore_rule in profile.ignore_rules:
            lines.append(
                f"ignore {ignore_rule.name}: "
                f"enabled={'вкл' if ignore_rule.enabled else 'выкл'}, "
                f"stage={'до collection' if ignore_rule.has_structure() else 'после collection'}, "
                f"media={ignore_rule.media_required}, "
                f"description_missing={ignore_rule.description_missing_required}, "
                f"found={','.join(ignore_rule.found_fields)}, "
                f"missing={','.join(ignore_rule.missing_fields)}"
                + (f", types={','.join(ignore_rule.media_types)}" if ignore_rule.media_types else "")
                + (
                    f", grouped_id={ignore_rule.grouped_id}, "
                    f"media={ignore_rule.min_media}..{ignore_rule.max_media}, "
                    f"messages={ignore_rule.min_messages}..{ignore_rule.max_messages}"
                    if ignore_rule.has_structure()
                    else ""
                )
            )
        for multi_price_rule in profile.multi_price_rules:
            labels = "; ".join(
                f"{source}=>{localized.ru}=>{localized.en}"
                for source, localized in multi_price_rule.labels.items()
            )
            lines.append(
                f"multi-price {multi_price_rule.name}: "
                f"priority={multi_price_rule.priority}, "
                f"currency={multi_price_rule.default_currency or 'from regex'}, "
                f"ignore_case={multi_price_rule.ignore_case}, "
                f"enabled={'вкл' if multi_price_rule.enabled else 'выкл'}\n"
                f"pattern={multi_price_rule.pattern[:240]}\n"
                f"labels: {labels[:360]}"
            )
        lines.append(
            f"advertising filter: enabled={profile.advertising_filter_enabled}, "
            f"source patterns={len(profile.advertising_patterns)}"
        )
        if len(lines) == 1:
            lines.append("Пока нет source-specific правил.")
        await edit_screen(
            callback,
            "\n\n".join(lines)[:3900],
            build_source_rules_markup(source_id, profile),
        )
        await callback.answer(notice or "", show_alert=notice is not None)

    @router.callback_query(F.data.regexp(r"^r:d:\d+:[a-z][a-z0-9_]{0,31}:\d+:[0-9a-f]{10}$"))
    async def source_rule_delete(callback: CallbackQuery) -> None:
        source_id, field, index, token = parse_source_rule_delete_callback(cast(str, callback.data))
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_source_rule(
                    source_id, field, index, actor_id=None, expected_token=token
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.regexp(r"^source:computed-del:\d+:\d+:[0-9a-f]{10}$"))
    async def source_computed_delete(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_computed_rule(
                    source_id,
                    int(index_text),
                    actor_id=None,
                    expected_token=token,
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(
        F.data.regexp(r"^source:computed-set:\d+:\d+:(true|false):[0-9a-f]{10}$")
    )
    async def source_computed_set(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, enabled_text, token = cast(str, callback.data).split(":", 5)
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).toggle_computed_rule(
                    source_id,
                    int(index_text),
                    actor_id=None,
                    expected_token=token,
                    enabled=enabled_text == "true",
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.regexp(r"^r:s:\d+:[a-z][a-z0-9_]{0,31}:\d+:[01]:[0-9a-f]{10}$"))
    async def source_rule_set(callback: CallbackQuery) -> None:
        _, _, source_text, field, index_text, enabled_text, token = cast(str, callback.data).split(
            ":", 6
        )
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).toggle_source_rule(
                    source_id,
                    field,
                    int(index_text),
                    actor_id=None,
                    expected_token=token,
                    enabled=enabled_text == "1",
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(
        F.data.regexp(r"^source:repeated-set:\d+:\d+:(true|false):[0-9a-f]{10}$")
    )
    async def source_repeated_set(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, enabled_text, token = cast(str, callback.data).split(":", 5)
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).toggle_repeated_pattern_rule(
                    source_id,
                    int(index_text),
                    actor_id=None,
                    expected_token=token,
                    enabled=enabled_text == "true",
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(
        F.data.regexp(r"^source:multi-price-set:\d+:\d+:(true|false):[0-9a-f]{10}$")
    )
    async def source_multi_price_set(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, enabled_text, token = cast(str, callback.data).split(":", 5)
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).toggle_multi_price_rule(
                    source_id,
                    int(index_text),
                    actor_id=None,
                    expected_token=token,
                    enabled=enabled_text == "true",
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.regexp(r"^source:ignore-set:\d+:\d+:(true|false):[0-9a-f]{10}$"))
    async def source_ignore_set(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, enabled_text, token = cast(str, callback.data).split(":", 5)
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).toggle_source_ignore_rule(
                    source_id,
                    int(index_text),
                    actor_id=None,
                    expected_token=token,
                    enabled=enabled_text == "true",
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.regexp(r"^source:clean-set:\d+:\d+:(true|false):[0-9a-f]{10}$"))
    async def source_clean_set(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, enabled_text, token = cast(str, callback.data).split(":", 5)
        source_id = int(source_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).toggle_cleaning_rule(
                    source_id,
                    int(index_text),
                    actor_id=None,
                    expected_token=token,
                    enabled=enabled_text == "true",
                )
        except LookupError as exc:
            await render_cleaning_screen(callback, source_id, notice=_stale_notice(exc))
            return
        await render_cleaning_screen(callback, source_id)

    @router.callback_query(
        F.data.regexp(
            r"^(?:source:(?:rule-del|rule-toggle|rule-edit):\d+:[a-z][a-z0-9_]{0,31}:\d+|"
            r"source:(?:repeated-del|repeated-toggle|multi-price-del|multi-price-toggle|"
            r"ignore-del|ignore-toggle):\d+:\d+)$"
        )
    )
    async def legacy_source_rule_callback(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).split(":")[2])
        await render_source_rules(
            callback,
            source_id,
            notice="Кнопка устарела; показан актуальный список.",
        )

    @router.callback_query(F.data.regexp(r"^source:(clean-rule-del|clean-toggle):\d+:\d+$"))
    async def legacy_cleaning_callback(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).split(":")[2])
        await render_cleaning_screen(
            callback,
            source_id,
            notice="Кнопка устарела; показан актуальный список.",
        )

    @router.callback_query(F.data.startswith("source:fallback:"))
    async def source_fallback_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_fallback)
        await callback.message.edit_text(
            "Явный fallback применяется только если поле missing, до Gemini и critical validation.\n"
            "Введите без JSON:\nfield=sizes\nvalue=S, M, L\nauthoritative=true\n\n"
            "Для fallback только когда отсутствует вся группа добавьте:\n"
            "when_all_missing=sizes,quantity\n\n"
            "Затем отдельно: field=quantity / value=6.\n"
            "Удаление: field=sizes / delete=true."
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("source:fallback-edit:"))
    async def source_fallback_edit(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, field = callback.data.split(":", 3)
        source_id = int(source_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            profile = SourceProfileData.model_validate(source.profile.config)
        fallback = profile.fallback_rules.get(field)
        if fallback is None:
            await callback.answer("Fallback уже удалён", show_alert=True)
            return
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_fallback)
        current = f"field={field}\nvalue={fallback.value}\nauthoritative={str(fallback.authoritative).lower()}\nwhen_all_missing={','.join(fallback.when_all_missing)}"
        await edit_screen(
            callback,
            "Редактирование одного fallback. Остальные сохраняются.\n<code>"
            + html.escape(current)
            + "</code>",
            keyboard([("Назад", f"source:rules:{source_id}")]),
        )
        await callback.answer()

    @router.message(AdminStates.source_fallback)
    async def source_fallback_save(message: Message, state: FSMContext) -> None:
        try:
            field, fallback = parse_source_fallback_input(message.text or "")
            source_id = int((await state.get_data())["source_id"])
            async with sessions() as session, session.begin():
                await AdminService(session).set_source_fallback(
                    source_id, field, fallback, actor_id=None
                )
            await state.clear()
            await message.answer(
                "Fallback удалён." if fallback is None else "Fallback сохранён.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Fallback не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:fallback-del:"))
    async def source_fallback_delete(callback: CallbackQuery) -> None:
        source_id, field = parse_source_fallback_delete_callback(cast(str, callback.data))
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).set_source_fallback(
                    source_id, field, None, actor_id=None
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.startswith("source:ignore:"))
    async def source_ignore_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_ignore)
        await callback.message.edit_text(
            "Структурный IGNORE применяется ДО collection, без Gemini/OCR.\n"
            "Одиночные фото/видео вне альбома:\n"
            "name=solo-media\ngrouped_id=missing\nmedia_types=photo,video\n"
            "description_missing_required=false\n\n"
            "grouped_id: any/present/missing; media_types: photo,video,document,none.\n"
            "Лимиты: min_media/max_media, min_messages/max_messages (до 200).\n"
            "Текстовые IGNORE проверяются после collection:\n"
            "Введите без JSON:\n"
            "name=video-no-text\n"
            "media_types=video\n"
            "media_required=true\n"
            "description_missing_required=true\n"
            "missing_fields=price,sizes,quantity\n\n"
            "media_types можно сочетать с текстовыми условиями: тогда тип media "
            "проверяется после collection и строгих/source rules, до Gemini/OCR.\n\n"
            "name=article-only-media\n"
            "media_required=true\n"
            "description_missing_required=true\n"
            "found_fields=article\n"
            "missing_fields=price,sizes,quantity\n\n"
            "Для меняющегося инфопоста без price и sizes:\n"
            "name=missing-price-sizes\n"
            "media_required=false\n"
            "description_missing_required=false\n"
            "missing_fields=price,sizes\n\n"
            "Все found_fields должны быть найдены строгими/source rules. "
            "Все missing_fields должны остаться missing; ambiguous не игнорируется."
        )
        await callback.answer()

    @router.message(AdminStates.source_ignore)
    async def source_ignore_save(message: Message, state: FSMContext) -> None:
        try:
            rule = parse_source_ignore_rule_input(message.text or "")
            source_id = int((await state.get_data())["source_id"])
            async with sessions() as session, session.begin():
                await AdminService(session).add_source_ignore_rule(source_id, rule, actor_id=None)
            await state.clear()
            await message.answer(
                "IGNORE фильтр сохранён.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"IGNORE фильтр не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:ignore-edit:"))
    async def source_ignore_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, index_text, token = callback.data.split(":")
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            profile = SourceProfileData.model_validate(source.profile.config)
        if (
            index >= len(profile.ignore_rules)
            or profile_item_token(profile.ignore_rules[index]) != token
        ):
            await callback.answer("Правило изменилось; откройте заново", show_alert=True)
            return
        rule = profile.ignore_rules[index]
        values = rule.model_dump(exclude_none=True)
        current = "\n".join(
            f"{key}={','.join(value) if isinstance(value, list) else str(value).lower() if isinstance(value, bool) else value}"
            for key, value in values.items()
        )
        await state.update_data(source_id=source_id, ignore_index=index, ignore_token=token)
        await state.set_state(AdminStates.source_ignore_edit)
        await edit_screen(
            callback,
            "Измените правило IGNORE:\n<code>" + html.escape(current) + "</code>",
            keyboard([("Назад", f"source:rules:{source_id}")]),
        )
        await callback.answer()

    @router.message(AdminStates.source_ignore_edit)
    async def source_ignore_edit_save(message: Message, state: FSMContext) -> None:
        try:
            rule = parse_source_ignore_rule_input(message.text or "")
            data = await state.get_data()
            async with sessions() as session, session.begin():
                source = await session.get(Source, data["source_id"], with_for_update=True)
                profile = SourceProfileData.model_validate(source.profile.config)
                index = data["ignore_index"]
                if (
                    index >= len(profile.ignore_rules)
                    or profile_item_token(profile.ignore_rules[index]) != data["ignore_token"]
                ):
                    raise ValueError("Правило изменилось; откройте заново")
                profile.ignore_rules[index] = rule
                await AdminService(session).update_profile(
                    source.id, profile.model_dump(mode="json"), actor_id=None
                )
            await state.clear()
            await message.answer(
                "IGNORE фильтр сохранён.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source.id}")]),
            )
        except Exception as exc:
            await message.answer(f"IGNORE не сохранён: {exc}", parse_mode=None)

    @router.callback_query(F.data.regexp(r"^source:ignore-del:\d+:\d+:[0-9a-f]{10}$"))
    async def source_ignore_delete(callback: CallbackQuery) -> None:
        source_id, index, token = parse_source_ignore_delete_callback(cast(str, callback.data))
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_source_ignore_rule_at(
                    source_id, index, actor_id=None, expected_token=token
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.startswith("source:multi-price:"))
    async def source_multi_price_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_multi_price)
        await callback.message.edit_text(
            "Создание нового Multi-price rule (без JSON). "
            "Multi-price сохраняет один товар и считает каждую пару отдельно:\n"
            "name=products-with-prices\n"
            "pattern=(?P&lt;label&gt;Ayakkabı|Küçük Çanta)\\s*"
            "(?P&lt;value&gt;\\d+)\\s*(?P&lt;currency&gt;\\$)\n"
            "default_currency=USD\npriority=10\nignore_case=true\nenabled=true\n"
            "labels=Ayakkabı=&gt;Обувь=&gt;Shoes;"
            "Küçük Çanta=&gt;Маленькая сумка=&gt;Small bag\n\n"
            "Для динамических размерных рядов добавьте label_mode=dynamic; labels не нужны.\n"
            "Группа label сохраняет исходный ряд размеров, value — его цену."
        )
        await callback.answer()

    @router.message(AdminStates.source_multi_price)
    async def source_multi_price_save(message: Message, state: FSMContext) -> None:
        try:
            rule = parse_multi_price_rule_input(message.text or "")
            source_id = int((await state.get_data())["source_id"])
            async with sessions() as session, session.begin():
                await AdminService(session).add_multi_price_rule(source_id, rule, actor_id=None)
            await state.clear()
            await message.answer(
                "Multi-price правило сохранено.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Multi-price не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^source:multi-price-edit:\d+:\d+:[0-9a-f]{10}$"))
    async def source_multi_price_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден или удалён.", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.multi_price_rules):
            await render_source_rules(
                callback,
                source_id,
                notice="Кнопка устарела: Multi-price rule уже отсутствует.",
            )
            return
        rule = profile.multi_price_rules[index]
        if profile_item_token(rule) != token:
            await render_source_rules(
                callback,
                source_id,
                notice="Кнопка устарела: Multi-price rule уже изменён.",
            )
            return
        await state.update_data(
            source_id=source_id,
            multi_price_rule_index=index,
            multi_price_rule_token=token,
        )
        await state.set_state(AdminStates.source_multi_price_edit)
        current = html.escape(multi_price_rule_editor_text(rule))
        await callback.message.edit_text(
            "Редактирование существующего Multi-price rule.\n"
            "Текущие значения:\n"
            f"<code>{current}</code>\n\n"
            "Отправьте полный обновлённый набор key=value. "
            "name — стабильная identity правила и должен остаться прежним. /cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.source_multi_price_edit)
    async def source_multi_price_edit_save(message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        source_id = int(data["source_id"])
        try:
            rule = parse_multi_price_rule_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).replace_multi_price_rule(
                    source_id,
                    int(data["multi_price_rule_index"]),
                    rule,
                    actor_id=None,
                    expected_token=str(data["multi_price_rule_token"]),
                )
        except LookupError:
            await state.clear()
            await message.answer(
                "Multi-price rule не изменён: правило уже изменилось или удалено. "
                "Откройте актуальный список.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
            return
        except Exception as exc:
            await message.answer(f"Multi-price не изменён: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Multi-price rule изменён; новый объект не создавался.",
            reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
        )

    @router.callback_query(F.data.regexp(r"^source:multi-price-del:\d+:\d+:[0-9a-f]{10}$"))
    async def source_multi_price_delete(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_multi_price_rule_at(
                    source_id, index, actor_id=None, expected_token=token
                )
        except LookupError as exc:
            await render_source_rules(callback, source_id, notice=_stale_notice(exc))
            return
        await render_source_rules(callback, source_id)

    @router.callback_query(F.data.startswith("source:advertising:"))
    async def source_advertising_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_advertising)
        await callback.message.edit_text(
            "Общий безопасный рекламный фильтр включён по умолчанию. Настройка source:\n"
            "enabled=true\npattern=(?im)^Partner post:\\s\npattern=(?im)^External ad:\\s\n\n"
            "Отправьте только enabled=true, чтобы удалить source patterns."
        )
        await callback.answer()

    @router.message(AdminStates.source_advertising)
    async def source_advertising_save(message: Message, state: FSMContext) -> None:
        try:
            enabled: bool | None = None
            patterns: list[str] = []
            for line in (message.text or "").splitlines():
                key, value = (part.strip() for part in line.split("=", 1))
                if key == "enabled":
                    if value.casefold() not in {"true", "false"}:
                        raise ValueError("enabled must be true or false")
                    enabled = value.casefold() == "true"
                elif key == "pattern":
                    re.compile(value)
                    patterns.append(value)
                else:
                    raise ValueError(f"unknown advertising setting: {key}")
            if enabled is None:
                raise ValueError("enabled is required")
            source_id = int((await state.get_data())["source_id"])
            async with sessions() as session, session.begin():
                source = await session.get(Source, source_id)
                if source is None:
                    raise LookupError("source not found")
                profile = SourceProfileData.model_validate(source.profile.config)
                updated = profile.model_copy(
                    update={
                        "advertising_filter_enabled": enabled,
                        "advertising_patterns": patterns,
                    }
                )
                await AdminService(session).update_profile(
                    source_id, updated.model_dump(mode="json"), actor_id=None
                )
            await state.clear()
            await message.answer(
                "Рекламный фильтр сохранён.",
                reply_markup=keyboard([("Все правила", f"source:rules:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Фильтр не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:profile:"))
    async def profile_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_profile)
        await callback.message.edit_text(
            "Измените нужные настройки, по одной key=value строке (без JSON). "
            "Сбор товара настраивается в разделе «Сбор товара»:\n"
            "publication_mode=auto|review\n"
            "use_global_publication_mode=true|false\n"
            "deduplicate_exact_products=true|false (одинаковые текст и media обновляют существующий товар)\n"
            "default_currency=USD|EUR|TRY|none\n"
            "required_fields=price,sizes,quantity\n"
            "default_destination_id=ID|none\n"
            "default_category=name|none\n"
            "alternative_publication_text=text|none\n"
            "gemini_enabled=true|false\nocr_mode=off|missing_only|always\n"
            "text_priority=true|false\n"
            "edit_policy=auto|review|auto_before_publication|ignore\n"
            "gemini_instruction=text\ngemini_example_limit=5\n"
            "remove_line=text\nremove_string=text\n"
            "cleaning_pattern=regex\npreserve_pattern=regex\n"
            "Пустое значение (например cleaning_pattern=) очищает соответствующий список."
        )
        await callback.answer()

    @router.message(AdminStates.source_profile)
    async def profile_save(message: Message, state: FSMContext) -> None:
        try:
            source_id = int((await state.get_data())["source_id"])
            async with sessions() as session, session.begin():
                source = await session.get(Source, source_id)
                if source is None:
                    raise LookupError("source not found")
                current = SourceProfileData.model_validate(source.profile.config)
                updated = update_profile_from_lines(current, message.text or "")
                await AdminService(session).update_profile(
                    source_id, updated.model_dump(mode="json"), actor_id=None
                )
            await state.clear()
            await message.answer(
                "Настройки проверены и сохранены.",
                reply_markup=keyboard([("Источник", f"source:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Profile не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:clean:"))
    async def cleaning_screen(callback: CallbackQuery) -> None:
        await render_cleaning_screen(callback, int(cast(str, callback.data).rsplit(":", 1)[1]))

    async def render_cleaning_screen(
        callback: CallbackQuery, source_id: int, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        lines = ["Очистка текста источника", ""]
        rows: list[list[tuple[str, str]]] = []
        if profile.cleaning_rules:
            lines.append("Правила (kind / значение):")
            for index, rule in enumerate(profile.cleaning_rules):
                state_icon = "✅" if rule.enabled else "⏸"
                token = profile_item_token(rule)
                lines.append(
                    f"{state_icon} {rule.kind}: <code>{html.escape(rule.value[:60])}</code>\n"
                    f"scope={rule.scope}, language={rule.language}, "
                    f"ignore_case={rule.ignore_case}, "
                    f"replacement=<code>{html.escape(rule.replacement[:60])}</code>"
                )
                rows.append(
                    [
                        (
                            "⏯" if rule.enabled else "▶️",
                            f"source:clean-set:{source_id}:{index}:"
                            f"{str(not rule.enabled).lower()}:{token}",
                        ),
                        (
                            "✏️ Изменить",
                            f"source:clean-edit:{source_id}:{index}:{token}",
                        ),
                        ("🗑", f"source:clean-del:{source_id}:{index}:{token}"),
                    ]
                )
        else:
            lines.append("Правил очистки пока нет.")
        if profile.remove_strings:
            lines.append("")
            lines.append("Строки-исключения (список заменяется целиком):")
            lines.extend(f"• {html.escape(value[:60])}" for value in profile.remove_strings[:15])
        else:
            lines.append("Строки-исключения не заданы.")
        rows.extend(
            [
                [("➕ Правило очистки", f"source:clean-rule:{source_id}")],
                [("✏️ Строки-исключения", f"source:clean-strings:{source_id}")],
                [("⬅️ Источник", f"source:{source_id}")],
            ]
        )
        await edit_screen(callback, "\n".join(lines), keyboard(*rows))
        await callback.answer(notice or "", show_alert=notice is not None)

    @router.callback_query(F.data.startswith("source:clean-strings:"))
    async def cleaning_strings_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(source_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.source_cleaning)
        await callback.message.edit_text(
            "Введите удаляемые supplier-specific строки, по одной на строке. "
            "Существующий список будет заменён. /cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.source_cleaning)
    async def cleaning_save(message: Message, state: FSMContext) -> None:
        source_id = int((await state.get_data())["source_id"])
        values = [line.strip() for line in (message.text or "").splitlines() if line.strip()]
        async with sessions() as session, session.begin():
            source = await session.get(Source, source_id)
            assert source is not None
            profile = SourceProfileData.model_validate(source.profile.config)
            await AdminService(session).update_profile(
                source_id,
                profile.model_copy(update={"remove_strings": values}).model_dump(mode="json"),
                actor_id=None,
            )
        await state.clear()
        await message.answer(
            "Cleaning strings сохранены.",
            reply_markup=keyboard([("Очистка", f"source:clean:{source_id}")]),
        )

    @router.callback_query(F.data.startswith("source:route:"))
    async def routing_list(callback: CallbackQuery) -> None:
        await render_routing_list(callback, int(cast(str, callback.data).rsplit(":", 1)[1]))

    async def render_routing_list(
        callback: CallbackQuery, source_id: int, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        lines = ["Category/routing rules"]
        rows: list[list[tuple[str, str]]] = []
        for index, route in enumerate(profile.routing_rules):
            token = profile_item_token(route)
            lines.append(
                f"#{index + 1} {html.escape(route.category or '*')} → "
                f"destination {route.destination_id}; priority={route.priority}; "
                f"keywords={html.escape(', '.join(route.keywords) or '—')}; "
                f"pattern=<code>{html.escape(route.pattern or '—')}</code>"
            )
            rows.append(
                [
                    (
                        f"✏️ #{index + 1}",
                        f"source:route-edit:{source_id}:{index}:{token}",
                    ),
                    (
                        f"🗑 #{index + 1}",
                        f"source:route-del:{source_id}:{index}:{token}",
                    ),
                ]
            )
        if not profile.routing_rules:
            lines.append("Правил пока нет.")
        rows.extend(
            [
                [("➕ Добавить", f"source:route-add:{source_id}")],
                [("⬅️ Источник", f"source:{source_id}")],
            ]
        )
        await edit_screen(callback, "\n\n".join(lines), keyboard(*rows))
        await callback.answer(notice or "")

    @router.callback_query(F.data.startswith("source:route-add:"))
    async def routing_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        await state.clear()
        await state.update_data(source_id=source_id)
        await state.set_state(AdminStates.source_routing)
        await callback.message.edit_text(
            "Новое routing rule (без JSON):\n"
            "category=category-name\n"
            "keywords=keyword1,keyword2\n"
            "pattern=regex (можно оставить пустым)\n"
            "destination_id=ID\n"
            "priority=100\n\n"
            "Keywords и pattern определяют категорию, затем эта же пара направляет товар."
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:route-edit:\d+:\d+:[0-9a-f]{10}$"))
    async def routing_edit_start(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден или удалён.", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.routing_rules):
            await render_routing_list(callback, source_id, notice="Правило уже отсутствует.")
            return
        route = profile.routing_rules[index]
        if profile_item_token(route) != token:
            await render_routing_list(callback, source_id, notice="Правило уже изменилось.")
            return
        await state.clear()
        await state.update_data(
            source_id=source_id,
            routing_index=index,
            routing_token=token,
        )
        await state.set_state(AdminStates.source_routing_edit)
        await callback.message.edit_text(
            "Текущее routing rule. Отправьте полный исправленный набор key=value:\n"
            f"<code>{html.escape(routing_rule_editor_text(route))}</code>\n\n"
            "/cancel — отмена."
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^source:route-del:\d+:\d+:[0-9a-f]{10}$"))
    async def routing_delete(callback: CallbackQuery) -> None:
        _, _, source_text, index_text, token = cast(str, callback.data).split(":", 4)
        source_id, index = int(source_text), int(index_text)
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_routing_pair(
                    source_id, index, actor_id=None, expected_token=token
                )
        except LookupError as exc:
            await render_routing_list(callback, source_id, notice=_stale_notice(exc))
            return
        await render_routing_list(callback, source_id)

    @router.callback_query(F.data.regexp(r"^source:route-del:\d+:\d+$"))
    async def legacy_routing_delete(callback: CallbackQuery) -> None:
        source_id = int(cast(str, callback.data).split(":")[2])
        await render_routing_list(
            callback,
            source_id,
            notice="Кнопка устарела; показан актуальный список.",
        )

    @router.message(StateFilter(AdminStates.source_routing, AdminStates.source_routing_edit))
    async def routing_save(message: Message, state: FSMContext) -> None:
        try:
            data = await state.get_data()
            source_id = int(data["source_id"])
            category, route = parse_routing_pair_input(message.text or "")
            async with sessions() as session, session.begin():
                service = AdminService(session)
                if "routing_index" in data:
                    await service.replace_routing_pair(
                        source_id,
                        int(data["routing_index"]),
                        category,
                        route,
                        actor_id=None,
                        expected_token=str(data["routing_token"]),
                    )
                else:
                    await service.add_routing_pair(source_id, category, route, actor_id=None)
            await state.clear()
            await message.answer(
                "Category/routing rule сохранено.",
                reply_markup=keyboard([("Routing", f"source:route:{source_id}")]),
            )
        except LookupError:
            await state.clear()
            await message.answer(
                "Routing rule не изменено: правило уже изменилось или удалено.",
                reply_markup=keyboard([("Routing", f"source:route:{source_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Не сохранено: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("source:gemini:"))
    async def gemini_screen(callback: CallbackQuery) -> None:
        await render_gemini_screen(callback, int(cast(str, callback.data).rsplit(":", 1)[1]))

    async def render_gemini_screen(
        callback: CallbackQuery, source_id: int, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        ocr_labels = {
            "off": "выкл",
            "missing_only": "только для пропусков",
            "always": "всегда",
        }
        text = (
            "Gemini / OCR\n\n"
            f"Gemini extraction: {'вкл' if profile.gemini_enabled else 'выкл'}\n"
            f"OCR: {ocr_labels[profile.ocr_mode.value]}\n"
            f"Examples в запросе: до {profile.gemini_example_limit}\n"
            f"Инструкция: {html.escape(profile.gemini_instruction[:200]) or '—'}\n\n"
            "Gemini используется для extraction/OCR данных; текст Gemini не попадает "
            "в публикацию."
        )
        ocr_row = [
            (
                ("✅ " if profile.ocr_mode.value == value else "") + label,
                f"gemini:ocr:{source_id}:{value}",
            )
            for value, label in ocr_labels.items()
        ]
        await edit_screen(
            callback,
            text,
            keyboard(
                [
                    (
                        f"{'✅' if profile.gemini_enabled else '○'} Gemini extraction",
                        f"gemini:enabled:{source_id}:{str(not profile.gemini_enabled).lower()}",
                    )
                ],
                ocr_row,
                [("✏️ Инструкция", f"gemini:instr:{source_id}")],
                [("Examples", f"source:examples:{source_id}")],
                [("⬅️ Источник", f"source:{source_id}")],
            ),
        )
        await callback.answer(notice or "", show_alert=notice is not None)

    @router.callback_query(F.data.regexp(r"^gemini:enabled:\d+:(true|false)$"))
    async def gemini_enabled_set(callback: CallbackQuery) -> None:
        _, _, source_text, enabled_text = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        async with sessions() as session, session.begin():
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
            await AdminService(session).update_profile(
                source_id,
                profile.model_copy(update={"gemini_enabled": enabled_text == "true"}).model_dump(
                    mode="json"
                ),
                actor_id=None,
            )
        await render_gemini_screen(callback, source_id)

    @router.callback_query(F.data.regexp(r"^gemini:ocr:\d+:(off|missing_only|always)$"))
    async def gemini_ocr_set(callback: CallbackQuery) -> None:
        _, _, source_text, mode = cast(str, callback.data).split(":", 3)
        source_id = int(source_text)
        async with sessions() as session, session.begin():
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Не найден", show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
            await AdminService(session).update_profile(
                source_id,
                profile.model_copy(update={"ocr_mode": OCRMode(mode)}).model_dump(mode="json"),
                actor_id=None,
            )
        await render_gemini_screen(callback, source_id)

    @router.callback_query(F.data.regexp(r"^gemini:instr:\d+$"))
    async def gemini_instruction_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(source_id=int(cast(str, callback.data).rsplit(":", 1)[1]))
        await state.set_state(AdminStates.gemini_instruction)
        await callback.message.edit_text(
            "Отправьте инструкцию Gemini для этого источника (или «-» чтобы очистить). "
            "/cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.gemini_instruction)
    async def gemini_instruction_save(message: Message, state: FSMContext) -> None:
        source_id = int((await state.get_data())["source_id"])
        instruction = (message.text or "").strip()
        if instruction == "-":
            instruction = ""
        async with sessions() as session, session.begin():
            source = await session.get(Source, source_id)
            assert source is not None
            profile = SourceProfileData.model_validate(source.profile.config)
            await AdminService(session).update_profile(
                source_id,
                profile.model_copy(update={"gemini_instruction": instruction}).model_dump(
                    mode="json"
                ),
                actor_id=None,
            )
        await state.clear()
        await message.answer(
            "Инструкция сохранена.",
            reply_markup=keyboard([("Gemini/OCR", f"source:gemini:{source_id}")]),
        )

    @router.callback_query(F.data.startswith("source:examples:"))
    async def examples_list(callback: CallbackQuery) -> None:
        await render_examples_list(callback, int(cast(str, callback.data).rsplit(":", 1)[1]))

    async def render_examples_list(
        callback: CallbackQuery, source_id: int, *, notice: str | None = None
    ) -> None:
        async with sessions() as session:
            examples = await AdminService(session).examples(source_id, 20)
        rows = [
            [
                (
                    f"🗑 #{example.id} {example.original_input[:24]}",
                    f"source:example-del:{source_id}:{example.id}",
                )
            ]
            for example in examples
        ]
        rows.extend(
            [
                [("➕ Добавить", f"source:example:{source_id}")],
                [("⬅️ Источник", f"source:{source_id}")],
            ]
        )
        await edit_screen(callback, "Source examples", keyboard(*rows))
        await callback.answer(notice or "")

    @router.callback_query(F.data.startswith("source:example:"))
    async def example_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(source_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.example)
        await callback.message.edit_text(
            "Введите example без JSON:\n"
            "input=исходный текст\narticle=AB-123\nmodel=M-7\nprice=10\ncomment=проверено\n"
            "Можно указывать любые подтверждённые поля; input обязателен."
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("source:example-del:"))
    async def example_delete(callback: CallbackQuery) -> None:
        _, _, source_text, example_text = cast(str, callback.data).split(":", 3)
        source_id, example_id = int(source_text), int(example_text)
        async with sessions() as session, session.begin():
            await AdminService(session).disable_example(source_id, example_id, actor_id=None)
        await render_examples_list(callback, source_id)

    @router.message(AdminStates.example)
    async def example_save(message: Message, state: FSMContext) -> None:
        try:
            payload = parse_key_value_lines(message.text or "")
            original = payload.pop("input")
            comment = payload.pop("comment", "")
            if not payload:
                raise ValueError("at least one expected field is required")
            source_id = int((await state.get_data())["source_id"])
            async with sessions() as session, session.begin():
                await AdminService(session).add_example(
                    source_id, original, payload, comment, actor_id=None
                )
            await state.clear()
            await message.answer("Example сохранён только для выбранного source.")
        except Exception as exc:
            await message.answer(f"Ошибка: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^source:test:\d+$"))
    async def test_start(callback: CallbackQuery, state: FSMContext) -> None:
        source_id = int(callback.data.rsplit(":", 1)[1])
        actor_id = current_admin_id.get()
        if actor_id is None:
            await callback.answer("Admin user не найден", show_alert=True)
            return
        async with sessions() as session, session.begin():
            batch = await start_or_resume_test_batch(session, source_id, actor_id)
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer("Источник не найден", show_alert=True)
                return
            items = list(
                await session.scalars(
                    select(AdminTestItem)
                    .where(AdminTestItem.batch_id == batch.id)
                    .order_by(AdminTestItem.telegram_message_id, AdminTestItem.id)
                )
            )
        await state.update_data(source_id=source_id, test_batch_id=batch.id)
        await state.set_state(AdminStates.test_input)
        await callback.message.edit_text(
            "Режим накопления dry-run. Отправьте части товара: текст, одиночное "
            "media или Telegram-альбом. Можно добавить несколько товаров — "
            "границы определятся настройками «Сбор товара» этого источника, и "
            "превью будет показано для каждого товара отдельно. "
            "Когда всё добавлено, нажмите «Обработать». До нажатия pipeline "
            "не запускается."
            f"\n\nУже в сессии: {len(items)} сообщений, "
            f"{sum(len(item.media_paths) for item in items)} media.",
            reply_markup=test_batch_controls(batch.id),
        )
        await callback.answer()

    @router.message(AdminStates.test_input)
    async def test_collect(message: Message, state: FSMContext) -> None:
        batch_id = int((await state.get_data())["test_batch_id"])
        actor_id = current_admin_id.get()
        if actor_id is None:
            await message.answer("Admin user не найден.")
            return
        input_text = message.text or message.caption or ""
        input_grouped_id = message.media_group_id
        media_paths: list[str] = []
        created_paths: list[str] = []
        batch_dir = media_root / "admin-tests" / str(batch_id)
        batch_dir.mkdir(parents=True, exist_ok=True)
        reserved = False
        if message.photo or message.video:
            # Commit the caption BEFORE network I/O: an oversized/failed file must
            # never silently remove the caption-bearing member from its album.
            async with sessions() as session, session.begin():
                batch = await session.get(AdminTestBatch, batch_id, with_for_update=True)
                if batch is None or batch.status != "collecting" or batch.admin_user_id != actor_id:
                    await message.answer("Сессия уже завершена. Откройте Тест заново.")
                    return
                reserved = await add_test_batch_item(
                    session,
                    batch_id=batch_id,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    media_group_id=input_grouped_id,
                    text=input_text,
                    media_paths=[],
                    allow_multiple_grouped_ids=True,
                    media_pending=True,
                )
                if not reserved:
                    return
        try:
            media_paths = await download_admin_test_media(
                message, batch_dir, media_client=media_client
            )
        except Exception as exc:
            if reserved:
                async with sessions() as session, session.begin():
                    item = await session.scalar(
                        select(AdminTestItem).where(
                            AdminTestItem.batch_id == batch_id,
                            AdminTestItem.telegram_chat_id == message.chat.id,
                            AdminTestItem.telegram_message_id == message.message_id,
                        )
                    )
                    if item:
                        item.media_pending = False
                        item.media_error = str(exc)[:1000]
            await message.answer(
                f"Media сообщения {message.message_id} не загружено; caption сохранён. "
                "Обработка неполного альбома заблокирована. Откройте новый тест и добавьте альбом повторно.\n"
                + html.escape(str(exc)[:300]),
                reply_markup=test_batch_controls(batch_id),
            )
            return
        created_paths.extend(media_paths)
        if not reserved and (
            input_text.startswith("http") or re.fullmatch(r"-?\d+:\d+", input_text)
        ):
            try:
                input_text, loaded_paths = await test_message_loader(input_text)
                loaded_grouped_id = getattr(loaded_paths, "grouped_id", None)
                input_grouped_id = str(loaded_grouped_id) if loaded_grouped_id is not None else None
                media_paths = []
                for loaded_path in loaded_paths:
                    source_path = Path(loaded_path)
                    destination = (
                        batch_dir / f"reference-{uuid4().hex}{source_path.suffix or '.bin'}"
                    )
                    await asyncio.to_thread(shutil.copy2, source_path, destination)
                    media_paths.append(str(destination))
                    created_paths.append(str(destination))
                    if source_path.name.startswith("emoda-admin-reference-"):
                        await asyncio.to_thread(source_path.unlink, missing_ok=True)
            except Exception as exc:
                for created_path in created_paths:
                    await asyncio.to_thread(Path(created_path).unlink, missing_ok=True)
                await message.answer(f"Сообщение недоступно: {html.escape(str(exc))}")
                return
        if not input_text.strip() and not media_paths:
            await message.answer(
                "Добавьте текст или фотографию.",
                reply_markup=test_batch_controls(batch_id),
            )
            return
        async with sessions() as session, session.begin():
            batch = await session.scalar(
                select(AdminTestBatch)
                .where(
                    AdminTestBatch.id == batch_id,
                    AdminTestBatch.admin_user_id == actor_id,
                    AdminTestBatch.status == "collecting",
                )
                .with_for_update()
            )
            if batch is None:
                for created_path in created_paths:
                    await asyncio.to_thread(Path(created_path).unlink, missing_ok=True)
                await message.answer("Сессия уже завершена. Откройте Тест заново.")
                return
            try:
                source = await session.get(Source, batch.source_id)
                if source is None:
                    raise LookupError("source not found")
                created = reserved or await add_test_batch_item(
                    session,
                    batch_id=batch_id,
                    chat_id=message.chat.id,
                    message_id=message.message_id,
                    media_group_id=input_grouped_id,
                    text=input_text,
                    media_paths=media_paths,
                    allow_multiple_grouped_ids=True,
                )
                if reserved:
                    item = await session.scalar(
                        select(AdminTestItem).where(
                            AdminTestItem.batch_id == batch_id,
                            AdminTestItem.telegram_chat_id == message.chat.id,
                            AdminTestItem.telegram_message_id == message.message_id,
                        )
                    )
                    assert item is not None
                    item.media_paths = media_paths
                    item.media_pending = False
                    item.media_error = None
            except TestProductBoundaryError:
                for created_path in created_paths:
                    await asyncio.to_thread(Path(created_path).unlink, missing_ok=True)
                await message.answer(
                    "Этот альбом имеет другой grouped_id и относится к другому товару. "
                    "Сначала обработайте или отмените текущую тестовую сессию, затем "
                    "откройте Тест для следующего товара.",
                    reply_markup=test_batch_controls(batch_id),
                )
                return
        if not created:
            for created_path in created_paths:
                await asyncio.to_thread(Path(created_path).unlink, missing_ok=True)

        async def deliver_feedback() -> None:
            async with sessions() as session:
                active_batch = await session.scalar(
                    select(AdminTestBatch).where(
                        AdminTestBatch.id == batch_id,
                        AdminTestBatch.admin_user_id == actor_id,
                        AdminTestBatch.status == "collecting",
                    )
                )
                if active_batch is None:
                    return
                current_items = list(
                    await session.scalars(
                        select(AdminTestItem)
                        .where(AdminTestItem.batch_id == batch_id)
                        .order_by(AdminTestItem.telegram_message_id, AdminTestItem.id)
                    )
                )
            await message.answer(
                ("Добавлено." if created else "Это сообщение уже было добавлено.")
                + f" В сессии: {len(current_items)} сообщений, "
                + f"{sum(len(item.media_paths) for item in current_items)} media.\n"
                + "Добавьте остальные части или нажмите «Обработать».",
                reply_markup=test_batch_controls(batch_id),
            )

        test_feedback.schedule(batch_id, deliver_feedback)

    async def send_collection_report(callback: CallbackQuery, text: str, controls: InlineKeyboardMarkup) -> None:
        # Keep escaped HTML safely below Telegram's message length limit.
        await callback.answer()
        for offset in range(0, len(text), 1800):
            await callback.message.answer(html.escape(text[offset:offset + 1800]), reply_markup=controls)

    @router.callback_query(F.data.regexp(r"^source:collect-test:\d+$"))
    async def source_collection_test(callback: CallbackQuery) -> None:
        source_id = int(callback.data.rsplit(':', 1)[1])
        async with sessions() as session:
            source = await session.get(Source, source_id)
            if source is None:
                await callback.answer('Источник не найден', show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
            rows = list(await session.scalars(select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id.desc()).limit(60)))
            counts = {int(row[0]): int(row[1]) for row in (await session.execute(select(MediaItem.source_message_id, func.count(MediaItem.id))
                .where(MediaItem.source_message_id.in_([r.id for r in rows]), MediaItem.active.is_(True),
                    MediaItem.local_deleted_at.is_(None))
                .group_by(MediaItem.source_message_id))).all()}
        messages = [CollectionMessage(r.telegram_message_id, r.telegram_date,
            grouped_id=r.grouped_id, text=r.text, caption=r.caption,
            media_count=int(counts.get(r.id, 0))) for r in rows]
        report = collection_report(messages, profile.effective_collection(),
            stored_groups={r.telegram_message_id: r.album_group_id for r in rows})
        pending = sum(r.media_pending for r in rows)
        report = f'{source.name}\nПоследние {len(rows)} сохранённых сообщений. Media ожидают загрузки: {pending}.\n' + report
        report += '\nИстория: проверяются границы сборки; фильтры рекламы/парсинга здесь не запускаются.'
        await send_collection_report(callback, report, keyboard(
            [('Настройки сборки', f'source:collect:{source_id}')],
            [('Повторить тест', f'source:collect-test:{source_id}')],
            [('Тест на своих сообщениях', f'source:test:{source_id}')]))

    @router.callback_query(F.data.regexp(r"^test:collection:\d+$"))
    async def batch_collection_test(callback: CallbackQuery) -> None:
        batch_id = int(callback.data.rsplit(':', 1)[1])
        async with sessions() as session:
            batch = await session.scalar(select(AdminTestBatch).where(
                AdminTestBatch.id == batch_id, AdminTestBatch.admin_user_id == current_admin_id.get()))
            if batch is None or batch.status != 'collecting':
                await callback.answer('Нет активной тестовой сессии', show_alert=True)
                return
            items = list(await session.scalars(select(AdminTestItem).where(AdminTestItem.batch_id == batch_id)))
            if not items or any(i.media_pending or i.media_error for i in items):
                await callback.answer('Добавьте сообщения и дождитесь загрузки всех media', show_alert=True)
                return
            source = await session.get(Source, batch.source_id)
            if source is None:
                await callback.answer('Источник не найден', show_alert=True)
                return
            profile = SourceProfileData.model_validate(source.profile.config)
        ordered, messages = prepare_test_collection(items, profile=profile)
        report = collection_report(messages, profile.effective_collection())
        report += '\nПакет: временные интервалы оригинала не сохранены; для проверки таймингов используйте историю источника.'
        excluded = sorted({i.telegram_message_id for i in items} - {i.telegram_message_id for i in ordered})
        if excluded:
            report += '\nИсключены рекламой/структурными фильтрами: ' + ', '.join(map(str, excluded))
        await send_collection_report(callback, report, test_batch_controls(batch_id))

    @router.callback_query(F.data.regexp(r"^test:process:\d+$"))
    async def test_process(callback: CallbackQuery, state: FSMContext) -> None:
        batch_id = int(callback.data.rsplit(":", 1)[1])
        actor_id = current_admin_id.get()
        if actor_id is None:
            await callback.answer("Admin user не найден", show_alert=True)
            return
        async with sessions() as session, session.begin():
            batch = await session.scalar(
                select(AdminTestBatch)
                .where(
                    AdminTestBatch.id == batch_id,
                    AdminTestBatch.admin_user_id == actor_id,
                )
                .with_for_update()
            )
            if batch is None:
                await callback.answer("Тестовая сессия не найдена", show_alert=True)
                return
            if batch.status == "processing":
                await callback.answer("Dry-run уже обрабатывается", show_alert=True)
                return
            if batch.status == "completed":
                await callback.answer("Dry-run уже завершён", show_alert=True)
                return
            if batch.status != "collecting":
                await callback.answer("Тестовая сессия отменена", show_alert=True)
                return
            items = list(
                await session.scalars(
                    select(AdminTestItem)
                    .where(AdminTestItem.batch_id == batch_id)
                    .order_by(AdminTestItem.telegram_message_id, AdminTestItem.id)
                )
            )
            if not items:
                await callback.answer("Сначала добавьте товар", show_alert=True)
                return
            if any(item.media_pending or item.media_error for item in items):
                await callback.answer(
                    "Не все media загружены. Caption сохранены; дождитесь загрузки или начните новый тест после ошибки.",
                    show_alert=True,
                )
                return
            source = await session.get(Source, batch.source_id)
            if source is None:
                await callback.answer("Источник не найден", show_alert=True)
                return
            batch_profile = SourceProfileData.model_validate(source.profile.config)
            products = partition_test_inputs(items, profile=batch_profile)
            if not products:
                await callback.answer(
                    "Все сообщения отфильтрованы (реклама/ignore). Добавьте товар.",
                    show_alert=True,
                )
                return
            batch.status = "processing"
            source_id = batch.source_id
        all_media_paths = [path for _, paths in products for path in paths]
        media_exists = await asyncio.gather(
            *(asyncio.to_thread(Path(path).is_file) for path in all_media_paths)
        )
        if not all(media_exists):
            async with sessions() as session, session.begin():
                batch = await session.get(AdminTestBatch, batch_id)
                if batch is not None:
                    batch.status = "collecting"
            await callback.answer("Часть media отсутствует; добавьте товар заново", show_alert=True)
            return
        try:
            bot_username = (await callback.bot.get_me()).username
            async with sessions() as session:
                source = await session.get(Source, source_id)
                assert source is not None
                source_name = source.name
                profile = SourceProfileData.model_validate(source.profile.config)
                products = partition_test_inputs(items, profile=profile)
                ranges = list(
                    await session.scalars(
                        select(MarkupRule)
                        .where(MarkupRule.enabled.is_(True))
                        .order_by(MarkupRule.priority)
                    )
                )
                examples = await AdminService(session).examples(
                    source_id, profile.gemini_example_limit
                )
                buttons_setting = await session.get(GlobalSetting, PUBLICATION_BUTTONS_SETTING_KEY)
                keyboard_snapshot = publication_keyboard(
                    buttons_setting.value if buttons_setting else None,
                    bot_username=bot_username,
                    emoda_code="EM-TEST",
                )
                alias_rows = list(
                    await session.scalars(
                        select(CurrencyAlias).where(CurrencyAlias.enabled.is_(True))
                    )
                )
                currency_aliases = CurrencyAliasResolver(
                    {item.alias: item.currency for item in alias_rows}
                )
                rates_setting = await session.get(GlobalSetting, "currency_rates")
                destination_prices = await session.get(GlobalSetting, "destination_price_policies")
                destination_price_policies = destination_prices.value if destination_prices else {}
                currency_rates = {
                    k: Decimal(str(v))
                    for k, v in (rates_setting.value if rates_setting else {}).items()
                }
            results = []
            for product_text, product_media in products:
                result = await process_product(
                    Pipeline(intelligence),
                    PipelineInput(
                        source_id=source_id,
                        text=product_text,
                        media_paths=product_media,
                        emoda_code="EM-TEST",
                        profile=profile,
                        markup_rules=[
                            MarkupRange.model_validate(
                                {
                                    "min_price": item.min_price,
                                    "max_price": item.max_price,
                                    "markup": item.markup,
                                    "markup_type": item.markup_type,
                                    "priority": item.priority,
                                }
                            )
                            for item in ranges
                        ],
                        examples=[
                            {
                                "source_id": source_id,
                                "input": item.original_input,
                                "expected": item.expected_values,
                            }
                            for item in examples
                        ],
                        currency_aliases=currency_aliases,
                        currency_rates=currency_rates,
                        destination_price_policies=destination_price_policies,
                    ),
                    ProcessingMode.DRY,
                )
                results.append(result)
        except Exception as exc:
            async with sessions() as session, session.begin():
                batch = await session.get(AdminTestBatch, batch_id)
                if batch is not None:
                    batch.status = "collecting"
            await callback.message.answer(
                f"Dry-run не завершён, накопленные данные сохранены: {html.escape(str(exc))}"
            )
            await callback.answer()
            return
        report_data = {
            "products": [result.model_dump(mode="json") for result in results],
            "source_name": source_name,
            "dry_run": True,
            "applied_buttons": [
                {"text": item["text"], "url": item["url"]}
                for row in keyboard_snapshot
                for item in row
            ],
        }
        async with sessions() as session, session.begin():
            test_run = TestRun(
                source_id=source_id,
                admin_user_id=actor_id,
                original_input="\n\n".join(text for text, _ in products),
                result=report_data,
            )
            session.add(test_run)
            await session.flush()
            test_run_id = test_run.id
            batch = await session.get(AdminTestBatch, batch_id, with_for_update=True)
            if batch is not None:
                batch.status = "completed"
                batch.test_run_id = test_run_id
            await AdminService(session).record_action("source.test", "source", source_id)
        await state.clear()
        # Each preview message is byte-identical to what live publication sends
        # for that product: the global renderer text with the real publication
        # keyboard attached.
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=item["text"], url=item["url"]) for item in row]
                for row in keyboard_snapshot
            ]
        )
        for index, result in enumerate(results, start=1):
            if len(results) > 1:
                await callback.message.answer(f"Товар {index} из {len(results)}:")
            await callback.message.answer(dry_run_diagnostics(result), parse_mode=None)
            preview_text, attach_keyboard = dry_run_preview(result)
            await callback.message.answer(
                preview_text,
                parse_mode=PUBLICATION_PARSE_MODE,
                reply_markup=markup if attach_keyboard else None,
            )
        await callback.message.answer(
            f"Dry-run: {len(results)} товар(ов). Публикация не выполнялась. "
            "Код E-MODA будет присвоен автоматически при live-публикации.",
            reply_markup=keyboard(
                [
                    ("Исправить", f"test:correct:{test_run_id}"),
                    ("Сохранить как example", f"test:example:{test_run_id}"),
                ],
                [("Всё верно", f"source:{source_id}")],
                [("Выйти", "menu:main")],
            ),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^test:cancel:\d+$"))
    async def test_cancel(callback: CallbackQuery, state: FSMContext) -> None:
        batch_id = int(callback.data.rsplit(":", 1)[1])
        actor_id = current_admin_id.get()
        async with sessions() as session, session.begin():
            batch = await session.scalar(
                select(AdminTestBatch)
                .where(
                    AdminTestBatch.id == batch_id,
                    AdminTestBatch.admin_user_id == actor_id,
                )
                .with_for_update()
            )
            if batch is not None and batch.status in {"collecting", "processing"}:
                batch.status = "cancelled"
        await state.clear()
        await callback.message.edit_text("Тестовая сессия отменена.", reply_markup=MAIN)
        await callback.answer()

    @router.callback_query(F.data.startswith("test:correct:"))
    async def test_correct_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(test_run_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.test_correction)
        await callback.message.edit_text(
            "Введите подтверждённые исправления, по одной строке field=value. JSON не нужен."
        )
        await callback.answer()

    @router.message(AdminStates.test_correction)
    async def test_correct_save(message: Message, state: FSMContext) -> None:
        try:
            corrected = parse_key_value_lines(message.text or "")
            test_run_id = int((await state.get_data())["test_run_id"])
            async with sessions() as session, session.begin():
                test_run = await session.get(TestRun, test_run_id)
                if test_run is None:
                    raise LookupError("test run not found")
                test_run.corrected_result = corrected
                await AdminService(session).record_action(
                    "source.test.correct", "test_run", test_run_id, after=corrected
                )
            await state.clear()
            await message.answer(
                "Исправления сохранены.",
                reply_markup=keyboard(
                    [("Сохранить как example", f"test:example:{test_run_id}")],
                    [("Главное меню", "menu:main")],
                ),
            )
        except Exception as exc:
            await message.answer(f"Исправления не сохранены: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("test:example:"))
    async def test_save_example(callback: CallbackQuery) -> None:
        test_run_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session, session.begin():
            test_run = await session.get(TestRun, test_run_id)
            if test_run is None:
                await callback.answer("Test run не найден", show_alert=True)
                return
            products = test_run.result.get("products") or []
            expected = test_run.corrected_result or (
                products[0].get("parsed") if len(products) == 1 else test_run.result
            )
            await AdminService(session).add_example(
                test_run.source_id,
                test_run.original_input,
                expected,
                f"Saved from dry-run {test_run.id}",
                actor_id=None,
            )
        await callback.message.edit_text(
            "Dry-run сохранён как source-local example.", reply_markup=MAIN
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:destinations")
    async def destinations(callback: CallbackQuery) -> None:
        async with sessions() as session:
            items = list(await session.scalars(select(Destination).order_by(Destination.name)))
        rows = [
            [(f"{'🟢' if item.enabled else '⚪'} {item.name}", f"destination:{item.id}")]
            for item in items
        ]
        rows.extend([[("➕ Добавить", "destination:add")], [("⬅️ Назад", "menu:main")]])
        await callback.message.edit_text("Каналы E-MODA", reply_markup=keyboard(*rows))
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^destination:\d+$"))
    async def destination_card(callback: CallbackQuery) -> None:
        destination_id = int(callback.data.split(":")[1])
        async with sessions() as session:
            destination = await session.get(Destination, destination_id)
            prices_setting = await session.get(GlobalSetting, "destination_price_policies")
        from emoda.services.destination_price import DestinationPricePolicy

        policy = DestinationPricePolicy.model_validate(
            (prices_setting.value if prices_setting else {}).get(str(destination_id), {})
        )
        if destination is None:
            await callback.answer("Не найден", show_alert=True)
            return
        body = (
            f"{html.escape(destination.name)}\n"
            f"Channel: {destination.telegram_channel_id}\n"
            f"Enabled: {destination.enabled}\n"
            f"Режим публикации: {'только цена поставщика' if policy.mode == 'supplier_percent' else 'глобальный E-MODA (4 поля)'}\n"
            f"Наценка специального режима: +{policy.percent}%\n"
            f"Валюта специального режима: {'поставщика' if policy.currency_mode == 'supplier' else 'USD'}\n"
            "Альтернативный текст канала: "
            f"{html.escape((policy.alternative_text or 'не задан')[:1000])}\n"
            "Dry-run использует эти же настройки. Нет цены — Уточнить у менеджера."
        )
        await callback.message.edit_text(
            body,
            reply_markup=keyboard(
                [("Изменить", f"destination:edit:{destination_id}")],
                [("Проверить доступ", f"destination:check:{destination_id}")],
                [
                    ("4 поля E-MODA", f"destination:format:{destination_id}:standard"),
                    ("Только цена +%", f"destination:format:{destination_id}:supplier_percent"),
                ],
                [("Изменить процент", f"destination:percent:{destination_id}")],
                [("Текст публикации", f"destination:text:{destination_id}")],
                [
                    ("Валюта поставщика", f"destination:currency:{destination_id}:supplier"),
                    ("В USD", f"destination:currency:{destination_id}:usd"),
                ],
                [("Назад", "menu:destinations")],
            ),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^destination:check:\d+$"))
    async def destination_check(callback: CallbackQuery) -> None:
        destination_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        async with sessions() as session:
            destination = await session.get(Destination, destination_id)
        if destination is None:
            await callback.answer("Канал не найден", show_alert=True)
            return
        try:
            title = await validate_destination_access(callback.bot, destination.telegram_channel_id)
            body = (
                f"Доступ подтверждён: {title}\nID: {destination.telegram_channel_id}\n"
                f"Канал {'включён' if destination.enabled else 'выключен в настройках'}. "
                "Бот может публиковать. Тестовые сообщения не отправлялись.\n"
                "Товары из очереди проверки нужно опубликовать повторно вручную."
            )
        except DestinationAccessError as exc:
            body = str(exc)
        await edit_screen(
            callback, html.escape(body), keyboard(
                [("Проверить ещё раз", f"destination:check:{destination_id}")],
                [("Настройки канала", f"destination:{destination_id}")],
                [("Очередь проверки", "menu:review")],
            ),
        )
        await callback.answer()

    async def save_destination_price(destination_id: int, patch: dict[str, Any]) -> None:
        async with sessions() as session, session.begin():
            destination = await session.get(Destination, destination_id)
            if destination is None:
                raise LookupError("Канал не найден")
            # Serialize read/merge even when the global row is first created.
            await session.scalar(
                select(Destination.id).order_by(Destination.id).limit(1).with_for_update()
            )
            setting = await session.get(
                GlobalSetting, "destination_price_policies", with_for_update=True
            )
            values = dict(setting.value) if setting else {}
            values[str(destination_id)] = {**values.get(str(destination_id), {}), **patch}
            await AdminService(session).set_setting(
                "destination_price_policies", values, actor_id=current_admin_id.get()
            )

    @router.callback_query(F.data.regexp(r"^destination:(?:format|currency):\d+:[a-z_]+$"))
    async def destination_price_mode(callback: CallbackQuery) -> None:
        _, kind, identifier, value = cast(str, callback.data).split(":")
        try:
            await save_destination_price(
                int(identifier), {"mode" if kind == "format" else "currency_mode": value}
            )
        except (ValueError, LookupError) as exc:
            await callback.answer(str(exc)[:180], show_alert=True)
            return
        await callback.message.edit_text(
            "Настройка сохранена. Применяется к новым обработкам; старые посты не переотправляются.",
            reply_markup=keyboard([("Открыть канал", f"destination:{identifier}")]),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^destination:percent:\d+$"))
    async def destination_percent(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(destination_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.destination_percent)
        await callback.message.edit_text(
            "Введите процент к цене поставщика, например 10. Общие наценки в этом режиме не применяются. /cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.destination_percent)
    async def destination_percent_save(message: Message, state: FSMContext) -> None:
        destination_id = int((await state.get_data())["destination_id"])
        try:
            await save_destination_price(
                destination_id, {"percent": (message.text or "").replace(",", ".")}
            )
        except (ValueError, ArithmeticError) as exc:
            await message.answer(f"Не сохранено: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Процент сохранён.",
            reply_markup=keyboard([("Открыть канал", f"destination:{destination_id}")]),
        )

    @router.callback_query(F.data.regexp(r"^destination:text:\d+$"))
    async def destination_publication_text(callback: CallbackQuery, state: FSMContext) -> None:
        destination_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        async with sessions() as session:
            destination = await session.get(Destination, destination_id)
            prices_setting = await session.get(GlobalSetting, "destination_price_policies")
        if destination is None:
            await callback.answer("Канал не найден", show_alert=True)
            return
        from emoda.services.destination_price import DestinationPricePolicy

        policy = DestinationPricePolicy.model_validate(
            (prices_setting.value if prices_setting else {}).get(str(destination_id), {})
        )
        current = policy.alternative_text or "не задан"
        await state.update_data(destination_id=destination_id)
        await state.set_state(AdminStates.destination_publication_text)
        await callback.message.edit_text(
            "Отправьте альтернативный текст публикации для этого канала. "
            "Он имеет приоритет над текстом источника. Отправьте - для сброса. "
            "/cancel — отмена.\n\n"
            f"Сейчас: {html.escape(current[:3000])}"
        )
        await callback.answer()

    @router.message(AdminStates.destination_publication_text)
    async def destination_publication_text_save(message: Message, state: FSMContext) -> None:
        destination_id = int((await state.get_data())["destination_id"])
        raw = (message.text or "").strip()
        value = None if raw.casefold() in {"-", "none", "нет"} else raw
        try:
            await save_destination_price(destination_id, {"alternative_text": value})
        except (ValueError, LookupError) as exc:
            await message.answer(f"Текст не сохранён: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Альтернативный текст канала сохранён."
            if value
            else "Альтернативный текст канала сброшен.",
            reply_markup=keyboard([("Открыть канал", f"destination:{destination_id}")]),
        )

    @router.callback_query(F.data.startswith("destination:edit:"))
    async def destination_edit(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(destination_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.destination_edit)
        await callback.message.edit_text(
            "Измените нужные поля без JSON, по одной key=value строке:\n"
            "name=Women\ntelegram_channel_id=-100...\nenabled=true\n\n"
            "Формат публикации глобальный и здесь не изменяется."
        )
        await callback.answer()

    @router.message(AdminStates.destination_edit)
    async def destination_edit_save(message: Message, state: FSMContext) -> None:
        try:
            destination_id = int((await state.get_data())["destination_id"])
            raw = parse_key_value_lines(message.text or "")
            unknown = set(raw) - {
                "name",
                "telegram_channel_id",
                "enabled",
            }
            if unknown:
                raise ValueError(f"unknown destination setting: {sorted(unknown)}")
            values: dict[str, Any] = {key: value.replace("\\n", "\n") for key, value in raw.items()}
            if "telegram_channel_id" in values:
                values["telegram_channel_id"] = int(values["telegram_channel_id"])
            if "enabled" in values:
                normalized = str(values["enabled"]).casefold()
                if normalized not in {"true", "false"}:
                    raise ValueError("enabled must be true or false")
                values["enabled"] = normalized == "true"
            async with sessions() as session, session.begin():
                destination = await session.get(Destination, destination_id)
                if destination is None:
                    raise LookupError("Канал не найден")
                if "telegram_channel_id" in values or values.get("enabled") is True:
                    await validate_destination_access(
                        message.bot,
                        values.get("telegram_channel_id", destination.telegram_channel_id),
                    )
                await AdminService(session).update_destination(
                    destination_id, values, actor_id=None
                )
            await state.clear()
            await message.answer(
                "Destination обновлён.",
                reply_markup=keyboard([("Открыть", f"destination:{destination_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Не сохранено: {html.escape(str(exc))}")

    @router.callback_query(F.data == "destination:add")
    async def destination_add(callback: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(AdminStates.destination_name)
        await callback.message.edit_text("Введите название E-MODA destination.")
        await callback.answer()

    @router.message(AdminStates.destination_name)
    async def destination_name(message: Message, state: FSMContext) -> None:
        await state.update_data(name=(message.text or "").strip())
        await state.set_state(AdminStates.destination_channel)
        await message.answer(
            "Введите numeric channel ID (например -100...). Bot должен быть администратором."
        )

    @router.message(AdminStates.destination_channel)
    async def destination_channel(message: Message, state: FSMContext) -> None:
        try:
            data = await state.get_data()
            channel_id = int(message.text or "")
            await validate_destination_access(message.bot, channel_id)
            async with sessions() as session, session.begin():
                destination = await AdminService(session).create_destination(
                    data["name"], channel_id, actor_id=None
                )
                destination_id = destination.id
            await state.clear()
            await message.answer(f"Destination {destination_id} создан.")
        except Exception as exc:
            await message.answer(f"Ошибка: {html.escape(str(exc))}")

    async def show_currency_aliases(callback: CallbackQuery) -> None:
        async with sessions() as session:
            items = await AdminService(session).currency_aliases()
        rows = [
            [
                (
                    f"{'🟢' if item.enabled else '⚪'} {item.alias} → {item.currency}"
                    f"{' 🔒' if item.system else ''}",
                    f"currency:alias:{item.id}",
                )
            ]
            for item in items
        ]
        rows.extend(
            [
                [("➕ Добавить alias", "currency:alias:add")],
                [("⬅️ Валюты", "menu:currencies")],
            ]
        )
        await edit_screen(
            callback,
            "Алиасы валют\nRuntime использует только включённые записи этого registry.",
            keyboard(*rows),
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:currencies")
    async def currencies_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await callback.message.edit_text(
            "Валюты",
            reply_markup=keyboard(
                [("Алиасы валют", "currency:aliases")],
                [("Курсы валют → USD", "currency:rates")],
                [("⬅️ Назад", "menu:main")],
            ),
        )
        await callback.answer()

    @router.callback_query(F.data == "currency:rates")
    async def currency_rates_screen(callback: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(AdminStates.currency_rates)
        async with sessions() as session:
            stored = await session.get(GlobalSetting, "currency_rates")
        rates = stored.value if stored else {}
        lines = [
            "Курсы валют → USD",
            "USD: 1; EUR: 1.2 USD (фиксировано).",
            "Курс задаётся как количество единиц валюты за 1 USD.",
            *[f"{code}: {rate} за 1 USD" for code, rate in sorted(rates.items())],
            "TRY: курс не задан" if "TRY" not in rates else "",
            "\nОтправьте ISO=курс, например TRY=40 (пример, не рыночный курс).",
            "ISO=- удаляет курс. Другие сохранённые валюты не меняются.",
            "Расчёт: цена / курс → наценка → округление вверх. /cancel — отмена.",
        ]
        await edit_screen(callback, "\n".join(lines), keyboard([("⬅️ Валюты", "menu:currencies")]))
        await callback.answer()

    @router.message(AdminStates.currency_rates)
    async def currency_rates_save(message: Message, state: FSMContext) -> None:
        try:
            supplied = parse_key_value_lines(message.text or "")
            if not supplied:
                raise ValueError("Введите ISO=курс")
            async with sessions() as session, session.begin():
                setting = await session.get(GlobalSetting, "currency_rates", with_for_update=True)
                values = dict(setting.value) if setting else {}
                for code, rate in supplied.items():
                    code = code.upper()
                    if code in {"USD", "EUR"} or not re.fullmatch("[A-Z]{3}", code):
                        raise ValueError("USD/EUR фиксированы; нужен трёхбуквенный ISO-код")
                    if rate == "-":
                        values.pop(code, None)
                    else:
                        values[code] = rate.replace(",", ".")
                await AdminService(session).set_setting(
                    "currency_rates", values, actor_id=current_admin_id.get()
                )
        except (ValueError, ArithmeticError) as exc:
            await message.answer(f"Курс не сохранён: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Курсы сохранены. Применяются к новым обработкам и тестам; опубликованные посты не пересчитываются автоматически.",
            reply_markup=keyboard([("Курсы валют → USD", "currency:rates")]),
        )

    @router.callback_query(F.data == "currency:aliases")
    async def currency_aliases_list(callback: CallbackQuery) -> None:
        await show_currency_aliases(callback)

    @router.callback_query(F.data.regexp(r"^currency:alias:\d+$"))
    async def currency_alias_card(callback: CallbackQuery) -> None:
        alias_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        async with sessions() as session:
            item = await session.get(CurrencyAlias, alias_id)
        if item is None:
            await show_currency_aliases(callback)
            return
        rows = [
            [
                ("✏️ Edit", f"currency:alias:edit:{alias_id}"),
                (
                    "⏸ Disable" if item.enabled else "▶️ Enable",
                    f"currency:alias:toggle:{alias_id}",
                ),
            ]
        ]
        if not item.system:
            rows.append([("🗑 Delete", f"currency:alias:delete:{alias_id}")])
        rows.append([("⬅️ Алиасы", "currency:aliases")])
        await edit_screen(
            callback,
            f"Alias: {html.escape(item.alias)}\nCurrency: {item.currency}\n"
            f"Enabled: {'yes' if item.enabled else 'no'}\n"
            f"Type: {'system' if item.system else 'custom'}",
            keyboard(*rows),
        )
        await callback.answer()

    @router.callback_query(F.data == "currency:alias:add")
    async def currency_alias_add(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(currency_alias_id=None)
        await state.set_state(AdminStates.currency_alias)
        await callback.message.edit_text(
            "Новый alias. Отправьте:\n"
            "alias=💰\n"
            "currency=USD\n"
            "enabled=true\n\n"
            "Currency — три латинские буквы. /cancel — отмена."
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^currency:alias:edit:\d+$"))
    async def currency_alias_edit(callback: CallbackQuery, state: FSMContext) -> None:
        alias_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        async with sessions() as session:
            item = await session.get(CurrencyAlias, alias_id)
        if item is None:
            await show_currency_aliases(callback)
            return
        await state.update_data(currency_alias_id=alias_id)
        await state.set_state(AdminStates.currency_alias)
        await callback.message.edit_text(
            "Измените и отправьте:\n"
            f"alias={html.escape(item.alias)}\n"
            f"currency={item.currency}\n"
            f"enabled={'true' if item.enabled else 'false'}\n\n"
            "/cancel — отмена."
        )
        await callback.answer()

    @router.message(AdminStates.currency_alias)
    async def currency_alias_save(message: Message, state: FSMContext) -> None:
        try:
            values = parse_key_value_lines(message.text or "")
            unknown = set(values) - {"alias", "currency", "enabled"}
            if unknown:
                raise ValueError(f"unknown settings: {sorted(unknown)}")
            missing = {"alias", "currency"} - set(values)
            if missing:
                raise ValueError(f"missing settings: {sorted(missing)}")
            enabled_text = values.get("enabled", "true").strip().casefold()
            if enabled_text not in {"true", "false", "yes", "no", "1", "0"}:
                raise ValueError("enabled must be true or false")
            enabled = enabled_text in {"true", "yes", "1"}
            data = await state.get_data()
            alias_id = data.get("currency_alias_id")
            async with sessions() as session, session.begin():
                service = AdminService(session)
                if alias_id is None:
                    item = await service.create_currency_alias(
                        alias=values["alias"],
                        currency=values["currency"],
                        enabled=enabled,
                        actor_id=None,
                    )
                else:
                    item = await service.update_currency_alias(
                        int(alias_id),
                        alias=values["alias"],
                        currency=values["currency"],
                        enabled=enabled,
                        actor_id=None,
                    )
                saved_id = item.id
            await state.clear()
            await message.answer(
                "Alias сохранён.",
                reply_markup=keyboard(
                    [("Открыть", f"currency:alias:{saved_id}")],
                    [("Все aliases", "currency:aliases")],
                ),
            )
        except Exception as exc:
            await message.answer(f"Alias не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^currency:alias:toggle:\d+$"))
    async def currency_alias_toggle(callback: CallbackQuery) -> None:
        alias_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        async with sessions() as session, session.begin():
            item = await session.get(CurrencyAlias, alias_id)
            if item is None:
                await callback.answer("Alias не найден", show_alert=True)
                return
            await AdminService(session).set_currency_alias_enabled(
                alias_id, not item.enabled, actor_id=None
            )
        await currency_alias_card(callback)

    @router.callback_query(F.data.regexp(r"^currency:alias:delete:\d+$"))
    async def currency_alias_delete(callback: CallbackQuery) -> None:
        alias_id = int(cast(str, callback.data).rsplit(":", 1)[1])
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).delete_currency_alias(alias_id, actor_id=None)
        except (LookupError, ValueError) as exc:
            await callback.answer(str(exc)[:180], show_alert=True)
            return
        await show_currency_aliases(callback)

    @router.callback_query(F.data == "menu:markup")
    async def markup(callback: CallbackQuery, state: FSMContext) -> None:
        async with sessions() as session:
            items = list(await session.scalars(select(MarkupRule).order_by(MarkupRule.priority)))
        await state.set_state(AdminStates.markup)
        await callback.message.edit_text(
            "Текущие диапазоны:\n"
            + "\n".join(
                f"{item.min_price}-{item.max_price if item.max_price is not None else 'inf'}: "
                f"{item.markup}{'%' if item.markup_type == 'percent' else ''}"
                for item in items
            )
            + "\n\nОтправьте полный список: min-max:markup или 101-inf:25%."
        )
        await callback.answer()

    @router.message(AdminStates.markup)
    async def markup_save(message: Message, state: FSMContext) -> None:
        try:
            ranges = parse_markup_rules_input(message.text or "")
            async with sessions() as session, session.begin():
                await AdminService(session).replace_markup_rules(ranges, actor_id=None)
            await state.clear()
            await message.answer("Наценки сохранены.", reply_markup=MAIN)
        except (ValueError, InvalidOperation) as exc:
            await message.answer(f"Наценки не сохранены: {html.escape(str(exc))}")

    async def show_catalog_node(callback: CallbackQuery, node_id: int) -> bool:
        async with sessions() as session:
            node = await session.get(CatalogNode, node_id)
            if node is None or node.archived_at is not None:
                return False
            children = list(
                await session.scalars(
                    select(CatalogNode)
                    .where(
                        CatalogNode.parent_id == node_id,
                        CatalogNode.archived_at.is_(None),
                    )
                    .order_by(CatalogNode.position, CatalogNode.id)
                )
            )
        rows = [
            [
                (
                    f"{'🟢' if child.enabled else '⚪'} "
                    f"{'📁' if child.node_type is CatalogNodeType.CATEGORY else '🔗'} "
                    f"{child.text_ru} / {child.text_en}",
                    f"admincat:node:{child.id}",
                )
            ]
            for child in children
        ]
        if node.node_type is CatalogNodeType.CATEGORY:
            rows.extend(
                [
                    [("➕ Подраздел", f"admincat:add:category:{node.id}")],
                    [("➕ Ссылка", f"admincat:add:link:{node.id}")],
                ]
            )
        rows.extend(
            [
                [
                    ("Изменить", f"admincat:edit:{node.id}"),
                    ("Вкл/выкл", f"admincat:toggle:{node.id}"),
                ],
                [
                    ("Архив", f"admincat:archive:{node.id}"),
                    (
                        "⬅️ Назад",
                        f"admincat:node:{node.parent_id}" if node.parent_id else "menu:catalog",
                    ),
                ],
            ]
        )
        details = (
            f"RU: {node.text_ru}\nEN: {node.text_en}\nТип: {node.node_type.value}\n"
            f"Ссылка: {node.url or '—'}\nПорядок: {node.position}\n"
            f"Включён: {node.enabled}"
        )
        await callback.message.edit_text(details, reply_markup=keyboard(*rows))
        return True

    @router.callback_query(F.data == "menu:catalog")
    async def catalog_admin_root(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        async with sessions() as session:
            nodes = list(
                await session.scalars(
                    select(CatalogNode)
                    .where(
                        CatalogNode.parent_id.is_(None),
                        CatalogNode.archived_at.is_(None),
                    )
                    .order_by(CatalogNode.position, CatalogNode.id)
                )
            )
        rows = [
            [
                (
                    f"{'🟢' if node.enabled else '⚪'} "
                    f"{'📁' if node.node_type is CatalogNodeType.CATEGORY else '🔗'} "
                    f"{node.text_ru} / {node.text_en}",
                    f"admincat:node:{node.id}",
                )
            ]
            for node in nodes
        ]
        rows.extend(
            [
                [("➕ Раздел", "admincat:add:category:0"), ("➕ Ссылка", "admincat:add:link:0")],
                [("🗄 Архив", "admincat:archive-list")],
                [("⬅️ Назад", "menu:main")],
            ]
        )
        await callback.message.edit_text(
            "Структура публичного каталога", reply_markup=keyboard(*rows)
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^admincat:node:\d+$"))
    async def catalog_admin_node(callback: CallbackQuery) -> None:
        found = await show_catalog_node(callback, int(callback.data.rsplit(":", 1)[1]))
        await callback.answer(None if found else "Пункт не найден", show_alert=not found)

    @router.callback_query(F.data == "admincat:archive-list")
    async def catalog_admin_archive_list(callback: CallbackQuery) -> None:
        async with sessions() as session:
            nodes = list(
                await session.scalars(
                    select(CatalogNode)
                    .where(CatalogNode.archived_at.is_not(None))
                    .order_by(
                        CatalogNode.parent_id.nulls_first(), CatalogNode.position, CatalogNode.id
                    )
                )
            )
        await callback.message.edit_text(
            "Архив каталога"
            + (
                "\nНажмите пункт, чтобы восстановить его без создания дубля."
                if nodes
                else "\nПусто."
            ),
            reply_markup=build_catalog_archive_markup(nodes),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^admincat:restore:\d+$"))
    async def catalog_admin_restore(callback: CallbackQuery) -> None:
        node_id = int(callback.data.rsplit(":", 1)[1])
        try:
            async with sessions() as session, session.begin():
                await AdminService(session).restore_catalog_node(node_id, actor_id=None)
        except (LookupError, ValueError) as exc:
            await callback.answer(str(exc), show_alert=True)
            return
        await show_catalog_node(callback, node_id)
        await callback.answer("Восстановлено")

    @router.callback_query(F.data.regexp(r"^admincat:add:(category|link):\d+$"))
    async def catalog_admin_add(callback: CallbackQuery, state: FSMContext) -> None:
        _, _, node_type_value, parent_value = callback.data.split(":")
        node_type = CatalogNodeType(node_type_value)
        await state.update_data(
            catalog_node_id=None,
            catalog_parent_id=int(parent_value) or None,
            catalog_node_type=node_type.value,
        )
        await state.set_state(AdminStates.catalog_node)
        prompt = (
            "Введите: название RU|name EN|порядок\nПример: Женская одежда|Women's clothing|1"
            if node_type is CatalogNodeType.CATEGORY
            else "Введите: название RU|name EN|https_url|порядок\n"
            "Пример: Платья|Dresses|https://t.me/e_moda_female|1"
        )
        await callback.message.edit_text(prompt)
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^admincat:edit:\d+$"))
    async def catalog_admin_edit(callback: CallbackQuery, state: FSMContext) -> None:
        node_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session:
            node = await session.get(CatalogNode, node_id)
        if node is None or node.archived_at is not None:
            await callback.answer("Пункт не найден", show_alert=True)
            return
        await state.update_data(
            catalog_node_id=node.id,
            catalog_parent_id=node.parent_id,
            catalog_node_type=node.node_type.value,
        )
        await state.set_state(AdminStates.catalog_node)
        prompt = (
            f"Текущее: {node.text_ru}|{node.text_en}|{node.position}\n"
            "Введите: название RU|name EN|порядок"
            if node.node_type is CatalogNodeType.CATEGORY
            else f"Текущее: {node.text_ru}|{node.text_en}|{node.url}|{node.position}\n"
            "Введите: название RU|name EN|https_url|порядок"
        )
        await callback.message.edit_text(prompt)
        await callback.answer()

    @router.message(AdminStates.catalog_node)
    async def catalog_admin_save(message: Message, state: FSMContext) -> None:
        try:
            state_data = await state.get_data()
            node_type = CatalogNodeType(state_data["catalog_node_type"])
            data = parse_catalog_node_input(message.text or "", node_type)
            node_id = state_data.get("catalog_node_id")
            async with sessions() as session, session.begin():
                service = AdminService(session)
                if node_id:
                    existing = await session.get(CatalogNode, int(node_id))
                    if existing is None:
                        raise LookupError("catalog node not found")
                    data = data.model_copy(update={"enabled": existing.enabled})
                    node = await service.update_catalog_node(int(node_id), data, actor_id=None)
                else:
                    node = await service.create_catalog_node(
                        data,
                        parent_id=state_data.get("catalog_parent_id"),
                        actor_id=None,
                    )
                saved_id = node.id
            await state.clear()
            await message.answer(
                "Пункт каталога сохранён.",
                reply_markup=keyboard([("Открыть", f"admincat:node:{saved_id}")]),
            )
        except Exception as exc:
            await message.answer(f"Пункт не сохранён: {html.escape(str(exc))}")

    @router.callback_query(F.data.regexp(r"^admincat:toggle:\d+$"))
    async def catalog_admin_toggle(callback: CallbackQuery) -> None:
        node_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session, session.begin():
            node = await session.get(CatalogNode, node_id)
            if node is None or node.archived_at is not None:
                await callback.answer("Пункт не найден", show_alert=True)
                return
            await AdminService(session).set_catalog_node_enabled(
                node_id, not node.enabled, actor_id=None
            )
        await show_catalog_node(callback, node_id)
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^admincat:archive:\d+$"))
    async def catalog_admin_archive(callback: CallbackQuery) -> None:
        node_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session, session.begin():
            node = await session.get(CatalogNode, node_id)
            if node is None:
                await callback.answer("Пункт не найден", show_alert=True)
                return
            parent_id = node.parent_id
            await AdminService(session).archive_catalog_node(node_id, actor_id=None)
        if parent_id:
            await show_catalog_node(callback, parent_id)
        else:
            await callback.message.edit_text(
                "Корневой пункт архивирован.",
                reply_markup=keyboard([("К каталогу", "menu:catalog")]),
            )
        await callback.answer("Архивировано")

    @router.callback_query(F.data == "menu:buttons")
    async def buttons(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        bot_username = (await callback.bot.get_me()).username
        async with sessions() as session:
            setting = await session.get(GlobalSetting, PUBLICATION_BUTTONS_SETTING_KEY)
        stored = setting.value if setting else None
        snapshot = publication_keyboard(stored, bot_username=bot_username)
        order = snapshot[0][0]
        catalog = snapshot[1][0]
        price = snapshot[2][0]
        warning = ""
        if order_url_validator is not None:
            try:
                await order_url_validator(order["url"])
            except ValueError as exc:
                warning = f"\n\n⚠️ {html.escape(str(exc))}"
        text = (
            "Кнопки публикации (глобальные, для всех источников)\n\n"
            f"1. {html.escape(order['text'])}\n   {html.escape(order['url'])}\n"
            f"2. {html.escape(catalog['text'])}\n   {html.escape(catalog['url'])}\n"
            f"3. {html.escape(price['text'])}\n   {html.escape(price['url'])}\n\n"
            "В новых публикациях — три отдельные кнопки. «Заказать» и «Уточнить цену» открывают "
            "чат с соответствующим текстом заказа или запроса цены и кодом товара; "
            "клиент сам нажимает «Отправить». "
            "EM-TEST в примере заменяется реальным кодом при публикации. "
            "Если контакт уточнения не задан, используется контакт «Заказать». "
            "Автоподстановка кода поддерживается для личного чата Telegram и WhatsApp; "
            "для другой ссылки задайте отдельный контакт уточнения. "
            "Старые публикации автоматически не меняются.\n\n"
            "URL кнопки «📂 Каталог / Catalog» можно задать явно; пустое значение — "
            "стандартная ссылка на каталог бота."
            + warning
        )
        markup = keyboard(
            [("Изменить URL 🛒 Заказать / Order", "buttons:url:order")],
            [("Изменить URL 📂 Каталог / Catalog", "buttons:url:catalog")],
            [("Контакт 💲 Уточнить цену", "buttons:url:check_price")],
            [("Текст 💲 Уточнить цену", "buttons:price-text")],
            [("Тексты кнопок", "buttons:texts")],
            [("Назад", "menu:main")],
        )
        markup.inline_keyboard.insert(
            0,
            [InlineKeyboardButton(text="🔎 Проверить «Заказать»", url=order["url"])],
        )
        await edit_screen(
            callback,
            text,
            reply_markup=markup,
        )
        await callback.answer()

    async def save_publication_buttons(
        update: dict[str, dict[str, str | None]],
    ) -> dict[str, object]:
        async with sessions() as session, session.begin():
            setting = await session.get(GlobalSetting, PUBLICATION_BUTTONS_SETTING_KEY)
            current = dict(setting.value) if setting else {}
            for key, values in update.items():
                current[key] = {**dict(current.get(key) or {}), **values}
            await AdminService(session).set_setting(
                PUBLICATION_BUTTONS_SETTING_KEY, current, actor_id=None
            )
            return current

    @router.callback_query(F.data.regexp(r"^buttons:url:(order|catalog|check_price)$"))
    async def buttons_url_start(callback: CallbackQuery, state: FSMContext) -> None:
        which = cast(str, callback.data).rsplit(":", 1)[1]
        await state.update_data(buttons_which=which)
        await state.set_state(AdminStates.buttons_url)
        prompt = (
            "Отправьте WhatsApp https://wa.me/номер или публичный Telegram @username менеджера. "
            "Код товара будет добавлен в текст запроса автоматически. «-» — контакт «Заказать»."
            if which == "check_price" else
            "Отправьте публичный Telegram @username (например, @emoda_orders), "
            "ссылку t.me/emoda_orders или полный https:// URL кнопки «🛒 Заказать / Order».\n"
            "Для WhatsApp и личного чата Telegram автоматически добавляется текст заказа с кодом товара.\n"
            "Для Telegram надёжнее использовать публичный @username, а не номер телефона."
            if which == "order"
            else "Отправьте https:// URL кнопки «📂 Каталог / Catalog», "
            "или «-» чтобы использовать ссылку на каталог бота."
        )
        await callback.message.edit_text(prompt + "\n\n/cancel — отмена.")
        await callback.answer()

    @router.message(AdminStates.buttons_url)
    async def buttons_url_save(message: Message, state: FSMContext) -> None:
        which = str((await state.get_data()).get("buttons_which"))
        raw = (message.text or "").strip()
        url: str | None
        if raw == "-":
            if which not in {"catalog", "check_price"}:
                await message.answer("URL кнопки «Заказать / Order» обязателен.")
                return
            url = None
        else:
            try:
                url = normalize_publication_button_url(raw)
                if which == "order" and order_url_validator is not None:
                    await order_url_validator(url)
                if which == "check_price":
                    price_inquiry_url(url, "EM-TEST")
                    if order_url_validator is not None:
                        await order_url_validator(url)
            except ValueError as exc:
                await message.answer(f"Некорректный URL: {html.escape(str(exc))}")
                return
        await save_publication_buttons({which: {"url": url}})
        await state.clear()
        markup = keyboard([("Кнопки", "menu:buttons")])
        if url is not None:
            markup.inline_keyboard.insert(
                0,
                [InlineKeyboardButton(text="🔎 Проверить сохранённую ссылку", url=url)],
            )
        await message.answer("URL сохранён.", reply_markup=markup)

    @router.callback_query(F.data == "buttons:texts")
    async def buttons_texts_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(AdminStates.buttons_texts)
        await state.update_data(buttons_text_key=None)
        await callback.message.edit_text(
            "Отправьте тексты кнопок строками key=value:\n"
            "order=🛒 Заказать / Order\ncatalog=📂 Каталог / Catalog\n"
            "check_price=💲 Уточнить цену / Check Price\n\n"
            "Можно отправить только изменяемые строки. /cancel — отмена."
        )
        await callback.answer()

    @router.callback_query(F.data == "buttons:price-text")
    async def price_button_text_start(callback: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(AdminStates.buttons_texts)
        await state.update_data(buttons_text_key="check_price")
        await edit_screen(
            callback, "Отправьте новый текст кнопки «Уточнить цену». /cancel — отмена.",
            keyboard([("⬅️ Кнопки", "menu:buttons")]),
        )
        await callback.answer()

    @router.message(AdminStates.buttons_texts)
    async def buttons_texts_save(message: Message, state: FSMContext) -> None:
        try:
            key = (await state.get_data()).get("buttons_text_key")
            values = ({str(key): (message.text or "").strip()} if key else parse_key_value_lines(message.text or ""))
            unknown = set(values) - {"order", "catalog", "check_price"}
            if unknown:
                raise ValueError(f"unknown button keys: {sorted(unknown)}")
            if not values or any(not text.strip() or len(text) > 128 or "\n" in text for text in values.values()):
                raise ValueError("Текст кнопки: непустая строка до 128 символов")
            await save_publication_buttons({key: {"text": text} for key, text in values.items()})
        except Exception as exc:
            await message.answer(f"Тексты не сохранены: {html.escape(str(exc))}")
            return
        await state.clear()
        await message.answer(
            "Тексты сохранены.", reply_markup=keyboard([("Кнопки", "menu:buttons")])
        )

    @router.callback_query(F.data == "menu:products")
    async def products(callback: CallbackQuery, state: FSMContext) -> None:
        await state.set_state(AdminStates.product_query)
        await callback.message.edit_text(
            "Введите E-MODA code, article/model или Telegram message ID."
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:review")
    async def review_list(callback: CallbackQuery) -> None:
        async with sessions() as session:
            items = await AdminService(session).review_queue()
        rows = [[(item.emoda_code, f"review:{item.id}")] for item in items]
        rows.append([("Назад", "menu:main")])
        await callback.message.edit_text(
            "Товары на проверке" if items else "Нет товаров на проверке",
            reply_markup=keyboard(*rows),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^review:\d+$"))
    async def review_card(callback: CallbackQuery) -> None:
        product_id = int(callback.data.split(":")[1])
        async with sessions() as session:
            product = await session.get(Product, product_id)
            review_reason = (
                await AdminService(session).product_review_reason(product)
                if product is not None
                else None
            )
            delivery = await session.get(
                GlobalSetting, review_delivery_key(callback.from_user.id, product)
            ) if product is not None else None
        if product is None:
            await callback.answer("Не найден", show_alert=True)
            return
        if product.status is ProductStatus.RECONCILIATION_REQUIRED:
            actions = keyboard(
                [("Подтвердить Telegram IDs", f"reconcile:confirm:{product_id}")],
                [("Осознанно повторить send", f"reconcile:retry:{product_id}")],
                [("Изменить", f"review:edit:{product_id}")],
                [("Назад", "menu:review")],
                [("🔎 Диагностика", f"product:diagnostics:{product_id}")],
            )
        elif product.status is ProductStatus.NEEDS_REVIEW:
            approval_token = review_approval_token(product)
            actions = keyboard(
                [
                    ("Опубликовать", f"review:publish:{product_id}:{approval_token}"),
                    ("Изменить", f"review:edit:{product_id}"),
                ],
                [("Пропустить", f"review:skip:{product_id}"), ("Назад", "menu:review")],
                [("🔎 Диагностика", f"product:diagnostics:{product_id}")],
            )
        else:
            actions = keyboard(
                [("Назад", "menu:review")],
                [("🔎 Диагностика", f"product:diagnostics:{product_id}")],
            )
        review_body = product.final_text[:3300] or product.original_text[:3300]
        if product.status is ProductStatus.SKIPPED:
            review_body = f"Товар уже пропущен.\n\n{review_body[:3200]}"
        elif product.status is ProductStatus.PUBLISHED:
            review_body = f"Товар уже опубликован.\n\n{review_body[:3200]}"
        elif product.status not in {
            ProductStatus.NEEDS_REVIEW,
            ProductStatus.RECONCILIATION_REQUIRED,
        }:
            review_body = f"Текущий статус: {product.status.value}.\n\n{review_body[:3200]}"
        if review_reason:
            review_body = f"Причина review: {review_reason[:650]}\n\n{review_body[:2800]}"
        if delivery is not None and delivery_stopped(delivery.value):
            explanation = (
                "Результат последней отправки не подтверждён; автоматического повтора не будет."
                if delivery.value.get("intent") else
                "Telegram отклонил отправку; лимит автоматических попыток исчерпан."
            )
            review_body = (
                f"Автоматическая доставка уведомления остановлена.\n{explanation}\n"
                f"Действия с товаром доступны ниже.\n\n{review_body[:2800]}"
            )
        await callback.message.edit_text(
            html.escape(review_body), parse_mode="HTML", reply_markup=actions
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("product:diagnostics:"))
    async def product_diagnostics(callback: CallbackQuery) -> None:
        product_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session:
            product = await session.get(Product, product_id)
            if product is None:
                await callback.answer("Товар не найден", show_alert=True)
                return
            cause = await AdminService(session).product_review_reason(product)
        await callback.message.answer(
            f"Товар {product.emoda_code}; status={product.status.value}\n{cause or 'Системных причин REVIEW нет; missing/ambiguous поля используют Check.'}",
            parse_mode=None,
        )
        if product.parsed_result.get("field_diagnostics"):
            result = PipelineResult.model_validate(product.parsed_result)
            trace = dry_run_diagnostics(result)
            for offset in range(0, len(trace), 3900):
                await callback.message.answer(trace[offset : offset + 3900], parse_mode=None)
        else:
            await callback.message.answer(
                "Трассировка полей не сохранена в старой записи. Источник → Тест воспроизводит текущий pipeline."
            )
        await callback.answer()

    @router.callback_query(F.data.startswith("reconcile:confirm:"))
    async def reconciliation_confirm(callback: CallbackQuery, state: FSMContext) -> None:
        await state.update_data(product_id=int(callback.data.rsplit(":", 1)[1]))
        await state.set_state(AdminStates.reconciliation_ids)
        await callback.message.edit_text(
            "После ручной проверки destination введите фактические Telegram message IDs через запятую."
        )
        await callback.answer()

    @router.message(AdminStates.reconciliation_ids)
    async def reconciliation_ids_save(message: Message, state: FSMContext) -> None:
        try:
            message_ids = [
                int(item.strip()) for item in (message.text or "").split(",") if item.strip()
            ]
            if not message_ids:
                raise ValueError("at least one ID is required")
            product_id = int((await state.get_data())["product_id"])
            async with sessions() as session, session.begin():
                product = await session.get(Product, product_id)
                outbox = await session.scalar(
                    select(PublicationOutbox)
                    .where(PublicationOutbox.product_id == product_id)
                    .order_by(PublicationOutbox.created_at.desc())
                    .limit(1)
                )
                if product is None or outbox is None:
                    raise LookupError("product/outbox not found")
                outbox.telegram_message_ids = message_ids
                outbox.status = OutboxStatus.PUBLISHED
                outbox.published_at = datetime.now(UTC)
                product.status = (
                    ProductStatus.READY if product.processing_error else ProductStatus.PUBLISHED
                )
                product.published_at = outbox.published_at
                await AdminService(session).record_action(
                    "publication.reconcile.manual",
                    "product",
                    product_id,
                    after={"telegram_message_ids": message_ids},
                )
            await state.clear()
            await message.answer(
                "Publication подтверждена без повторной отправки.", reply_markup=MAIN
            )
        except Exception as exc:
            await message.answer(f"Не сохранено: {html.escape(str(exc))}")

    @router.callback_query(F.data.startswith("reconcile:retry:"))
    async def reconciliation_retry(callback: CallbackQuery) -> None:
        product_id = int(callback.data.rsplit(":", 1)[1])
        if publish_product is None:
            await callback.answer("Publisher не настроен", show_alert=True)
            return
        invalid_payload: str | None = None
        async with sessions() as session, session.begin():
            outbox = await session.scalar(
                select(PublicationOutbox)
                .where(PublicationOutbox.product_id == product_id)
                .order_by(PublicationOutbox.created_at.desc())
                .limit(1)
            )
            product = await session.get(Product, product_id)
            if outbox is None or product is None:
                await callback.answer("Outbox не найден", show_alert=True)
                return
            try:
                validate_publication_text(
                    product.final_text,
                    code=product.emoda_code,
                    publication_format=product.parsed_result.get(
                        "publication_format", "standard"
                    ),
                )
            except ValueError as exc:
                invalid_payload = str(exc)
            else:
                # The explicit admin decision establishes that another send is safe.
                # FAILED is intentionally used instead of PENDING: PublicationService
                # re-arms this row if its immutable payload is still current; if the
                # admin edited the payload, a new outbox supersedes this row cleanly.
                outbox.status = OutboxStatus.FAILED
                outbox.last_error = "explicit retry authorized in /admin"
                product.status = ProductStatus.READY_TO_PUBLISH
                await AdminService(session).record_action(
                    "publication.retry.authorized", "product", product_id
                )
        if invalid_payload is not None:
            await edit_screen(
                callback,
                "Повторная отправка заблокирована: текущий текст не соответствует "
                f"формату публикации ({html.escape(invalid_payload)}). Сначала исправьте 4 строки.",
                keyboard(
                    [("Изменить", f"review:edit:{product_id}")],
                    [("К списку", "menu:review")],
                ),
            )
            await callback.answer("Сначала исправьте публикацию", show_alert=True)
            return
        await publish_product(product_id, None, resume=True)
        await edit_screen(
            callback,
            "Явно разрешённая повторная отправка выполнена/зафиксирована.",
            keyboard([("К списку", "menu:review")]),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^review:publish:\d+$"))
    async def legacy_review_publish(callback: CallbackQuery) -> None:
        """A pre-deploy card has no snapshot token and cannot approve current data."""
        product_id = int(callback.data.rsplit(":", 1)[1])
        await callback.answer()
        await edit_screen(
            callback,
            "Карточка устарела: товар мог измениться. Откройте актуальную карточку.",
            keyboard(
                [("Открыть актуальную", f"review:{product_id}")], [("К списку", "menu:review")]
            ),
        )

    @router.callback_query(F.data.regexp(r"^review:publish:\d+:[0-9a-f]{16}$"))
    async def review_publish(callback: CallbackQuery) -> None:
        _, _, product_text, expected_token = callback.data.split(":", 3)
        product_id = int(product_text)
        if publish_product is None:
            await callback.answer("Publisher не настроен", show_alert=True)
            return
        # Stop Telegram's callback spinner before publication I/O. The durable
        # product/outbox state below remains the source of truth for the result.
        await callback.answer()
        try:
            result = await publish_product(product_id, expected_token)
        except StaleReviewApproval:
            async with sessions() as session:
                current = await session.get(Product, product_id)
            if current is not None and current.status is ProductStatus.SKIPPED:
                await edit_screen(
                    callback,
                    "Товар уже пропущен. Публикация не выполнялась.",
                    keyboard([("К списку", "menu:review")]),
                )
                return
            async with sessions() as session, session.begin():
                await AdminService(session).record_action(
                    "product.publish.stale",
                    "product",
                    product_id,
                    after={"expected_review_token": expected_token},
                )
            await edit_screen(
                callback,
                "Товар изменился после показа карточки. Ничего не опубликовано; "
                "откройте актуальную версию.",
                keyboard(
                    [("Открыть актуальную", f"review:{product_id}")],
                    [("К списку", "menu:review")],
                ),
            )
            return
        async with sessions() as session, session.begin():
            await AdminService(session).record_action(
                "product.publish.approve",
                "product",
                product_id,
                after={
                    "expected_review_token": expected_token,
                    "outcome": result.outcome,
                    "status": result.status.value,
                    "outbox_id": result.outbox_id,
                },
            )
        if result.outcome == "published":
            text = "Опубликовано. Одно нажатие зафиксировано."
        elif result.outcome == "already_published":
            text = "Уже опубликовано. Повторная отправка не выполнялась."
        elif result.outcome == "in_progress":
            text = "Публикация уже выполняется. Повторная отправка не запускалась."
        else:
            detail = result.detail or "причина записана в REVIEW"
            text = f"Не опубликовано: {detail}"
        if result.outcome == "published":
            result_markup = keyboard(
                [("Проверить повторное нажатие", cast(str, callback.data))],
                [("К списку", "menu:review")],
            )
        elif result.outcome == "failed":
            result_markup = keyboard([("Открыть актуальную", f"review:{product_id}")])
        else:
            result_markup = keyboard([("К списку", "menu:review")])
        await edit_screen(
            callback,
            text[:3900],
            result_markup,
        )

    @router.callback_query(F.data.startswith("review:skip:"))
    async def review_skip(callback: CallbackQuery) -> None:
        product_id = int(callback.data.rsplit(":", 1)[1])
        await callback.answer()
        previous_status: ProductStatus | None = None
        async with sessions() as session, session.begin():
            product = await session.scalar(
                select(Product).where(Product.id == product_id).with_for_update()
            )
            if product is not None:
                previous_status = product.status
                if previous_status is ProductStatus.NEEDS_REVIEW:
                    product.status = ProductStatus.SKIPPED
                    await AdminService(session).record_action("product.skip", "product", product_id)
        if previous_status is ProductStatus.NEEDS_REVIEW:
            text = "Товар пропущен."
        elif previous_status is ProductStatus.SKIPPED:
            text = "Товар уже пропущен."
        elif previous_status is ProductStatus.PUBLISHED:
            text = "Товар уже опубликован; пропуск недоступен."
        elif previous_status is None:
            text = "Товар не найден."
        else:
            text = f"Пропуск недоступен: текущий статус {previous_status.value}."
        await edit_screen(callback, text, keyboard([("К списку", "menu:review")]))

    @router.callback_query(F.data.startswith("review:edit:"))
    async def review_edit(callback: CallbackQuery, state: FSMContext) -> None:
        product_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session:
            product = await session.get(Product, product_id)
        if product is None:
            await callback.answer("Товар не найден", show_alert=True)
            return
        publication_format = product.parsed_result.get("publication_format", "standard")
        await state.update_data(product_id=product_id)
        await state.set_state(AdminStates.product_edit)
        if publication_format == "alternative_text":
            prompt = "Отправьте исправленный альтернативный текст публикации."
        elif publication_format == "supplier_percent":
            prompt = "Отправьте строку специальной цены в формате: <b>Цена:</b> 27.5 USD."
        else:
            prompt = (
                "Отправьте исправленные ровно 4 строки: Код / Code, Цена за шт / Unit price, "
                "Размеры / Sizes, Шт в серии / Pcs per lot. Для неизвестного значения используйте "
                "Уточнить / Check; известная цена должна содержать $."
            )
        await callback.message.edit_text(prompt)
        await callback.answer()

    @router.message(AdminStates.product_edit)
    async def review_edit_save(message: Message, state: FSMContext) -> None:
        product_id = int((await state.get_data())["product_id"])
        proposed = message.text or ""
        async with sessions() as session, session.begin():
            product = await session.get(Product, product_id)
            if product:
                try:
                    validate_publication_text(
                        proposed,
                        code=product.emoda_code,
                        publication_format=product.parsed_result.get(
                            "publication_format", "standard"
                        ),
                    )
                except ValueError as exc:
                    await message.answer(
                        f"Не сохранено: {html.escape(str(exc))}. Исправьте 4 строки и отправьте снова."
                    )
                    return
                product.final_text = proposed
                result = dict(product.parsed_result)
                result["admin_edited_final_text"] = True
                product.parsed_result = result
                await AdminService(session).record_action("product.edit", "product", product_id)
        await state.clear()
        await message.answer(
            "Исправление сохранено; товар остаётся в review.",
            reply_markup=keyboard(
                [("Сохранить correction как example", f"review:example:{product_id}")],
                [("Карточка", f"review:{product_id}")],
            ),
        )

    @router.callback_query(F.data.startswith("review:example:"))
    async def review_example(callback: CallbackQuery) -> None:
        product_id = int(callback.data.rsplit(":", 1)[1])
        async with sessions() as session, session.begin():
            product = await session.get(Product, product_id)
            if product is None:
                await callback.answer("Товар не найден", show_alert=True)
                return
            await AdminService(session).add_example(
                product.source_id,
                product.original_text,
                product.parsed_result,
                f"Admin correction for {product.emoda_code}",
                actor_id=None,
            )
        await callback.message.edit_text(
            "Correction сохранён как source-local example.",
            reply_markup=keyboard([("Карточка", f"review:{product_id}")]),
        )
        await callback.answer()

    async def load_search_reports(query: str = "", product_id: int | None = None) -> Any:
        async with sessions() as session:
            items = (
                await AdminService(session).find_products(query) if product_id is None
                else list(await session.scalars(select(Product).where(Product.id == product_id)))
            )
            reports: list[tuple[str, list[tuple[str, str]], int, bool]] = []
            for product in items:
                source = await session.get(Source, product.source_id)
                if product.source_message_id:
                    source_messages = [await session.get(SourceMessage, product.source_message_id)]
                elif product.album_group_id:
                    source_messages = list(
                        await session.scalars(
                            select(SourceMessage)
                            .where(SourceMessage.album_group_id == product.album_group_id)
                            .order_by(SourceMessage.telegram_message_id)
                        )
                    )
                else:
                    source_messages = []
                source_messages = [item for item in source_messages if item is not None]
                if source_messages:
                    media_rows = list(
                        await session.scalars(
                            select(MediaItem)
                            .where(
                                MediaItem.source_message_id.in_(
                                    [item.id for item in source_messages]
                                ),
                                MediaItem.local_deleted_at.is_(None),
                                MediaItem.active.is_(True),
                            )
                            .order_by(MediaItem.telegram_message_id, MediaItem.position)
                        )
                    )
                    media_count = len(media_rows)
                else:
                    media_rows = []
                    media_count = 0
                outboxes = list(
                    await session.scalars(
                        select(PublicationOutbox)
                        .where(PublicationOutbox.product_id == product.id)
                        .order_by(PublicationOutbox.created_at)
                    )
                )
                reports.append(
                    (
                        f"{product.emoda_code}\nStatus: {product.status}\nSource: {source.name if source else product.source_id}\nSupplier messages: {[item.telegram_message_id for item in source_messages]}\nTelegram date: {source_messages[0].telegram_date if source_messages else '-'}\nEdit dates: {[item.edit_date for item in source_messages if item.edit_date]}\nMedia: {media_count}\nDestination: {product.destination_id}\nOriginal price: {product.raw_price} {product.raw_currency}\nUSD: {product.normalized_usd}; markup: {product.applied_markup}; final: {product.final_price}\nPublished IDs: {[box.telegram_message_ids for box in outboxes]}\nCreated/published: {product.created_at} / {product.published_at}\n\nOriginal:\n{product.original_text[:2200]}",
                        [
                            (str(media_root / item.storage_path), item.media_type)
                            for item in media_rows
                        ],
                        product.id,
                        product.status
                        in {
                            ProductStatus.NEEDS_REVIEW,
                            ProductStatus.RECONCILIATION_REQUIRED,
                        },
                    )
                )
        return reports

    def search_result_markup(report: Any, index: int, ids: list[int]) -> InlineKeyboardMarkup:
        _body, media, product_id, actionable = report
        rows: list[list[tuple[str, str]]] = []
        if actionable:
            rows.append([("Открыть карточку", f"review:{product_id}")])
        if media:
            rows.append([("Показать медиа", f"product:media:{product_id}:0")])
        navigation = []
        if index:
            navigation.append(("◀ Предыдущий", f"product:result:{index - 1}"))
        if index + 1 < len(ids):
            navigation.append(("Следующий ▶", f"product:result:{index + 1}"))
        if navigation:
            rows.append(navigation)
        rows.append([("Новый поиск", "menu:products"), ("Главное меню", "menu:main")])
        return keyboard(*rows)

    @router.message(AdminStates.product_query)
    async def product_search(message: Message, state: FSMContext) -> None:
        reports = await load_search_reports((message.text or "").strip())
        await state.clear()
        if not reports:
            await message.answer("Товары не найдены.", reply_markup=MAIN)
            return
        ids = [report[2] for report in reports]
        await state.update_data(product_result_ids=ids)
        await message.answer(
            html.escape(f"Товар 1 из {len(ids)} (до 20 результатов)\n\n" + reports[0][0][:3700]),
            parse_mode="HTML", reply_markup=search_result_markup(reports[0], 0, ids),
        )

    @router.callback_query(F.data.regexp(r"^product:result:\d+$"))
    async def product_search_page(callback: CallbackQuery, state: FSMContext) -> None:
        ids = (await state.get_data()).get("product_result_ids", [])
        index = int(cast(str, callback.data).rsplit(":", 1)[1])
        if not 0 <= index < len(ids):
            await callback.answer("Откройте новый поиск.", show_alert=True)
            return
        reports = await load_search_reports(product_id=ids[index])
        if not reports:
            await callback.answer("Товар недоступен.", show_alert=True)
            return
        await edit_screen(
            callback, f"Товар {index + 1} из {len(ids)}\n\n" + reports[0][0][:3700],
            search_result_markup(reports[0], index, ids),
        )
        await callback.answer()

    @router.callback_query(F.data.regexp(r"^product:media:\d+:\d+$"))
    async def product_search_media(callback: CallbackQuery) -> None:
        _prefix, _media, product_id, offset = cast(str, callback.data).split(":")
        reports = await load_search_reports(product_id=int(product_id))
        if not reports or not reports[0][1]:
            await callback.answer("Локальные медиа отсутствуют или уже очищены.", show_alert=True)
            return
        media = reports[0][1]
        first = int(offset)
        if not 0 <= first < len(media):
            await callback.answer("Медиа недоступны.", show_alert=True)
            return
        await callback.answer()
        shown = 0
        for path, media_type in media[first:first + 10]:
            if await asyncio.to_thread(usable_admin_media, path):
                await deliver_admin_product_media(cast(Message, callback.message), path, media_type, media_client=media_client)
                shown += 1
        rows = []
        if first + 10 < len(media):
            rows.append([("Следующие медиа", f"product:media:{product_id}:{first + 10}")])
        rows.append([("Новый поиск", "menu:products"), ("Главное меню", "menu:main")])
        await callback.message.answer(
            f"Показано {shown} медиа, позиции {first + 1}–{min(first + 10, len(media))} из {len(media)}.",
            reply_markup=keyboard(*rows),
        )

    @router.callback_query(F.data == "menu:test")
    async def testing(callback: CallbackQuery) -> None:
        await callback.message.edit_text(
            "Выберите Источники → source → Тест. Используется production pipeline с dry_run=true.",
            reply_markup=keyboard([("Источники", "menu:sources")], [("Назад", "menu:main")]),
        )
        await callback.answer()

    @router.callback_query(F.data == "menu:settings")
    async def settings_menu(callback: CallbackQuery, state: FSMContext) -> None:
        await state.clear()
        await callback.message.edit_text(
            "Общие настройки\n\n"
            "EUR→USD: 1.2 — фиксирован бизнес-правилом и не является изменяемой настройкой.",
            reply_markup=keyboard(
                [("Global AUTO", "settings:mode:auto"), ("Global REVIEW", "settings:mode:review")],
                [("Назад", "menu:main")],
            ),
        )
        await callback.answer()

    @router.callback_query(F.data.startswith("settings:mode:"))
    async def global_mode_save(callback: CallbackQuery) -> None:
        value = callback.data.rsplit(":", 1)[1]
        async with sessions() as session, session.begin():
            await AdminService(session).set_setting(
                "default_publication_mode", {"value": value}, actor_id=None
            )
        await callback.message.edit_text(
            f"Global publication mode: {value}. Source override остаётся приоритетным; установите use_global_publication_mode=true для наследования.",
            reply_markup=keyboard([("Назад", "menu:settings")]),
        )
        await callback.answer()

    @router.callback_query()
    async def unknown_admin_callback(callback: CallbackQuery) -> None:
        logger.warning(
            "unhandled admin callback",
            extra={
                "callback_data": callback.data,
                "telegram_user_id": callback.from_user.id,
            },
        )
        await callback.answer(
            "Действие устарело или недоступно. Откройте /admin заново.",
            show_alert=True,
        )

    return router
