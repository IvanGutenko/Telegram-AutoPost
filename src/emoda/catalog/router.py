from __future__ import annotations

from typing import Literal

from aiogram import F, Router
from aiogram.enums import ChatType
from aiogram.filters import CommandStart
from aiogram.types import CallbackQuery, InlineKeyboardButton, InlineKeyboardMarkup, Message
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from emoda.db.models import CatalogNode
from emoda.domain.enums import CatalogNodeType

CatalogLanguage = Literal["ru", "en"]

_COPY = {
    "ru": {
        "title": "📂 Каталог E-MODA",
        "empty_root": "Каталог E-MODA пока пуст.",
        "empty_section": "В этом разделе пока нет доступных пунктов.",
        "back": "⬅️ Назад",
        "home": "🏠 В начало",
        "unavailable": "Раздел недоступен.",
        "private_only": "Каталог открывается в личном чате с ботом.",
    },
    "en": {
        "title": "📂 E-MODA Catalog",
        "empty_root": "The E-MODA catalog is currently empty.",
        "empty_section": "There are no available items in this section yet.",
        "back": "⬅️ Back",
        "home": "🏠 Home",
        "unavailable": "This section is unavailable.",
        "private_only": "The catalog opens in a private chat with the bot.",
    },
}


async def visible_catalog_children(
    session: AsyncSession, parent_id: int | None
) -> list[CatalogNode]:
    return list(
        await session.scalars(
            select(CatalogNode)
            .where(
                CatalogNode.parent_id.is_(None)
                if parent_id is None
                else CatalogNode.parent_id == parent_id,
                CatalogNode.enabled.is_(True),
                CatalogNode.archived_at.is_(None),
            )
            .order_by(CatalogNode.position, CatalogNode.id)
        )
    )


async def visible_catalog_category(session: AsyncSession, node_id: int) -> CatalogNode | None:
    current = await session.get(CatalogNode, node_id)
    selected = current
    visited: set[int] = set()
    while current is not None:
        if (
            current.id in visited
            or current.node_type is not CatalogNodeType.CATEGORY
            or not current.enabled
            or current.archived_at is not None
        ):
            return None
        visited.add(current.id)
        current = await session.get(CatalogNode, current.parent_id) if current.parent_id else None
    return selected


def build_language_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="Русский", callback_data="catalog:ru:0"),
                InlineKeyboardButton(text="English", callback_data="catalog:en:0"),
            ]
        ]
    )


def _node_text(node: CatalogNode, language: CatalogLanguage) -> str:
    return node.text_ru if language == "ru" else node.text_en


def build_catalog_markup(
    nodes: list[CatalogNode], *, current: CatalogNode | None, language: CatalogLanguage
) -> InlineKeyboardMarkup:
    child_buttons: list[InlineKeyboardButton] = []
    for node in sorted(nodes, key=lambda item: (item.position, item.id)):
        text = _node_text(node, language)
        if node.node_type is CatalogNodeType.CATEGORY:
            button = InlineKeyboardButton(
                text=text,
                callback_data=f"catalog:{language}:{node.id}",
            )
        else:
            assert node.url is not None
            button = InlineKeyboardButton(text=text, url=node.url)
        child_buttons.append(button)
    rows = [child_buttons[index : index + 2] for index in range(0, len(child_buttons), 2)]
    if current is not None:
        back_target = current.parent_id or 0
        navigation = [
            InlineKeyboardButton(
                text=_COPY[language]["back"],
                callback_data=f"catalog:{language}:{back_target}",
            )
        ]
        if back_target != 0:
            navigation.append(
                InlineKeyboardButton(
                    text=_COPY[language]["home"],
                    callback_data=f"catalog:{language}:0",
                )
            )
        rows.append(navigation)
    return InlineKeyboardMarkup(inline_keyboard=rows)


def create_catalog_router(sessions: async_sessionmaker[AsyncSession]) -> Router:
    router = Router(name="public-catalog")

    @router.message(CommandStart(), F.chat.type == ChatType.PRIVATE)
    async def catalog_start(message: Message) -> None:
        await message.answer(
            "Выберите язык каталога / Choose catalog language",
            reply_markup=build_language_markup(),
        )

    @router.callback_query(F.data.regexp(r"^catalog:(ru|en):\d+$"))
    async def catalog_open(callback: CallbackQuery) -> None:
        _, language_value, node_id_value = (callback.data or "").split(":")
        language: CatalogLanguage = "ru" if language_value == "ru" else "en"
        callback_message = callback.message
        if (
            not isinstance(callback_message, Message)
            or callback_message.chat.type != ChatType.PRIVATE
        ):
            await callback.answer(_COPY[language]["private_only"], show_alert=True)
            return
        node_id = int(node_id_value)
        async with sessions() as session:
            current = None if node_id == 0 else await visible_catalog_category(session, node_id)
            if node_id != 0 and current is None:
                await callback.answer(_COPY[language]["unavailable"], show_alert=True)
                return
            nodes = await visible_catalog_children(session, node_id or None)
        title = (
            _COPY[language]["title"] if current is None else f"📂 {_node_text(current, language)}"
        )
        if not nodes:
            empty_key = "empty_root" if current is None else "empty_section"
            title += f"\n\n{_COPY[language][empty_key]}"
        await callback_message.edit_text(
            title,
            reply_markup=build_catalog_markup(nodes, current=current, language=language),
        )
        await callback.answer()

    return router
