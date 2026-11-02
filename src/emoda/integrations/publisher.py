from __future__ import annotations

import asyncio
import os
import re
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any, ParamSpec, TypeVar, cast

from aiogram import Bot
from aiogram.exceptions import (
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.types import (
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaLivePhoto,
    InputMediaPhoto,
    InputMediaVideo,
)
from sqlalchemy import text as sql_text
from sqlalchemy.ext.asyncio import AsyncEngine
from telethon import Button

from emoda.services.destination_access import validate_destination_access
from emoda.services.publication import (
    SafePublicationFailure,
    UncertainPublicationFailure,
    expected_message_count,
)
from emoda.services.render import CODE_PREFIX, PUBLICATION_PARSE_MODE, telegram_chunks

P = ParamSpec("P")
R = TypeVar("R")
BOT_API_PHOTO_LIMIT_BYTES = 10 * 1024 * 1024
BOT_API_VIDEO_LIMIT_BYTES = 50 * 1024 * 1024
# Telegram's cloud Bot API can reject a multipart request even when every file
# is individually valid. Leave room for multipart framing below its 50 MiB body.
BOT_API_MULTIPART_LIMIT_BYTES = 49 * 1024 * 1024


def resolve_media_types(payload: dict[str, object], media_paths: list[str]) -> list[str]:
    """Stored media types win when they match the media list; extensions are the fallback."""
    stored = payload.get("media_types")
    if isinstance(stored, list) and len(stored) == len(media_paths):
        return [str(value) for value in stored]
    return ["video" if path.lower().endswith(".mp4") else "photo" for path in media_paths]


def input_media(
    path: str,
    media_type: str,
    caption: str | None = None,
    parse_mode: str | None = None,
) -> InputMediaPhoto | InputMediaVideo:
    """One constructor for Telegram media objects so send and update cannot drift apart."""
    if media_type == "video":
        return InputMediaVideo(media=FSInputFile(path), caption=caption, parse_mode=parse_mode)
    return InputMediaPhoto(media=FSInputFile(path), caption=caption, parse_mode=parse_mode)


class TelegramBotPublisher:
    """Publishes through Bot API and reconciles through a readable MTProto client."""

    def __init__(
        self,
        bot: Bot,
        history_client: Any,
        media_client: Any = None,
        *,
        lock_engine: AsyncEngine | None = None,
    ) -> None:
        self.bot = bot
        self.history_client = history_client
        self.media_client = media_client
        self.lock_engine = lock_engine
        self._destination_locks: dict[int, asyncio.Lock] = {}

    async def validate_destination(self, destination_channel_id: int) -> None:
        await validate_destination_access(self.bot, destination_channel_id)

    @asynccontextmanager
    async def destination_turn(self, channel_id: int) -> AsyncIterator[None]:
        # One complete product owns its destination until ALL media and the
        # trailing caption/buttons are accepted. Different channels remain independent.
        async with self._destination_locks.setdefault(channel_id, asyncio.Lock()):
            if self.lock_engine is None:
                yield
                return
            # Separate transaction: publication commits its durable intent and
            # outcome independently. PostgreSQL also serializes other processes.
            async with self.lock_engine.begin() as connection:
                await connection.execute(
                    sql_text("SELECT pg_advisory_xact_lock(hashtextextended(:destination, 0))"),
                    {"destination": f"emoda:destination-send:{channel_id}"},
                )
                yield

    @staticmethod
    def markup(snapshot: list[list[dict[str, str]]]) -> InlineKeyboardMarkup | None:
        if not snapshot:
            return None
        return InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=item["text"], url=item["url"]) for item in row]
                for row in snapshot
            ]
        )

    @staticmethod
    def mtproto_buttons(snapshot: list[list[dict[str, str]]]) -> list[list[Any]] | None:
        if not snapshot:
            return None
        return [[Button.url(item["text"], item["url"]) for item in row] for row in snapshot]

    @staticmethod
    def requires_mtproto(path: str, media_type: str) -> bool:
        # FSInputFile performs its own missing-file validation during the Bot API call.
        # Keep this routing predicate side-effect free for that ordinary error path.
        if not os.path.isfile(path):
            return False
        limit = BOT_API_VIDEO_LIMIT_BYTES if media_type == "video" else BOT_API_PHOTO_LIMIT_BYTES
        return os.path.getsize(path) > limit

    @classmethod
    def requires_mtproto_batch(cls, media: list[tuple[str, str]]) -> bool:
        if any(cls.requires_mtproto(path, media_type) for path, media_type in media):
            return True
        if len(media) <= 1:
            return False
        total_bytes = sum(
            os.path.getsize(path) for path, _media_type in media if os.path.isfile(path)
        )
        return total_bytes > BOT_API_MULTIPART_LIMIT_BYTES

    async def send_large_file(
        self,
        destination_channel_id: int,
        path: str | list[str],
        *,
        caption: str | None = None,
        keyboard: list[list[dict[str, str]]] | None = None,
    ) -> list[int]:
        if self.media_client is None:
            raise SafePublicationFailure(
                "media exceeds Bot API upload limit and MTProto bot publisher is unavailable"
            )
        message = await self.media_client.send_file(
            destination_channel_id,
            path,
            caption=caption,
            parse_mode="html" if caption else None,
            buttons=self.mtproto_buttons(keyboard or []) if caption else None,
            supports_streaming=(
                path.lower().endswith(".mp4")
                if isinstance(path, str)
                else any(item.lower().endswith(".mp4") for item in path)
            ),
        )
        rows = message if isinstance(message, list) else [message]
        return [int(item.id) for item in rows]

    @staticmethod
    async def _call(function: Callable[P, Awaitable[R]], *args: P.args, **kwargs: P.kwargs) -> R:
        for attempt in range(3):
            try:
                return await function(*args, **kwargs)
            except TelegramRetryAfter as exc:
                if attempt == 2:
                    raise SafePublicationFailure(
                        "Telegram flood wait persisted after bounded retries"
                    ) from exc
                await asyncio.sleep(min(max(float(exc.retry_after), 0), 60))
        raise AssertionError("unreachable")

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        async with self.destination_turn(destination_channel_id):
            return await self._send(destination_channel_id, payload, keyboard)

    async def _send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        text = str(payload["text"])
        media_paths = [str(path) for path in cast(list[object], payload.get("media_paths", []))]
        if not media_paths:
            raise SafePublicationFailure(
                "media_required: publication requires at least one photo or video"
            )
        media_types = resolve_media_types(payload, media_paths)
        if any(media_type not in {"photo", "video"} for media_type in media_types):
            raise SafePublicationFailure("unsupported media type requires manual review")
        markup = self.markup(keyboard)
        ids: list[int] = []
        try:
            if len(media_paths) == 1:
                if self.requires_mtproto(media_paths[0], media_types[0]):
                    if len(text) <= 1024:
                        return await self.send_large_file(
                            destination_channel_id,
                            media_paths[0],
                            caption=text,
                            keyboard=keyboard,
                        )
                    ids.extend(
                        await self.send_large_file(
                            destination_channel_id,
                            media_paths[0],
                        )
                    )
                    chunks = telegram_chunks(text)
                    for index, chunk in enumerate(chunks):
                        message = await self._call(
                            self.bot.send_message,
                            destination_channel_id,
                            chunk,
                            parse_mode=PUBLICATION_PARSE_MODE,
                            reply_markup=markup if index == len(chunks) - 1 else None,
                        )
                        ids.append(message.message_id)
                    return ids
                send_media = (
                    self.bot.send_video if media_types[0] == "video" else self.bot.send_photo
                )
                if len(text) <= 1024:
                    message = await self._call(
                        send_media,
                        destination_channel_id,
                        FSInputFile(media_paths[0]),
                        caption=text,
                        parse_mode=PUBLICATION_PARSE_MODE,
                        reply_markup=markup,
                    )
                    return [message.message_id]
                media_message = await self._call(
                    send_media,
                    destination_channel_id,
                    FSInputFile(media_paths[0]),
                )
                ids.append(media_message.message_id)
                chunks = telegram_chunks(text)
                for index, chunk in enumerate(chunks):
                    message = await self._call(
                        self.bot.send_message,
                        destination_channel_id,
                        chunk,
                        parse_mode=PUBLICATION_PARSE_MODE,
                        reply_markup=markup if index == len(chunks) - 1 else None,
                    )
                    ids.append(message.message_id)
                return ids
            if self.requires_mtproto_batch(list(zip(media_paths, media_types, strict=True))):
                # Keep the complete product on one transport. Mixing a Bot API batch with a
                # later MTProto batch would make partial acceptance harder to reconcile.
                for offset in range(0, len(media_paths), 10):
                    ids.extend(
                        await self.send_large_file(
                            destination_channel_id,
                            media_paths[offset : offset + 10],
                        )
                    )
                chunks = telegram_chunks(text)
                for index, chunk in enumerate(chunks):
                    message = await self._call(
                        self.bot.send_message,
                        destination_channel_id,
                        chunk,
                        parse_mode=PUBLICATION_PARSE_MODE,
                        reply_markup=markup if index == len(chunks) - 1 else None,
                    )
                    ids.append(message.message_id)
                return ids
            for offset in range(0, len(media_paths), 10):
                batch = media_paths[offset : offset + 10]
                batch_types = media_types[offset : offset + 10]
                # send_media_group accepts the wider InputMedia union; input_media only
                # produces photo/video, the only types the renderer's sources can yield.
                media: list[
                    InputMediaAudio
                    | InputMediaDocument
                    | InputMediaLivePhoto
                    | InputMediaPhoto
                    | InputMediaVideo
                ] = []
                for path, media_type in zip(batch, batch_types, strict=True):
                    media.append(input_media(path, media_type))
                messages = await self._call(
                    self.bot.send_media_group, destination_channel_id, media
                )
                ids.extend(message.message_id for message in messages)
            # Bot API media groups cannot own reply markup. The linked text/navigation
            # message carries the exact E-MODA code and immutable keyboard snapshot.
            chunks = telegram_chunks(text)
            for index, chunk in enumerate(chunks):
                message = await self._call(
                    self.bot.send_message,
                    destination_channel_id,
                    chunk,
                    parse_mode=PUBLICATION_PARSE_MODE,
                    reply_markup=markup if index == len(chunks) - 1 else None,
                )
                ids.append(message.message_id)
            return ids
        except (TelegramBadRequest, TelegramForbiddenError) as exc:
            if ids:
                raise UncertainPublicationFailure(str(exc)) from exc
            raise SafePublicationFailure(str(exc)) from exc
        except TelegramNetworkError as exc:
            raise UncertainPublicationFailure(str(exc)) from exc

    async def find_by_code(self, destination_channel_id: int, emoda_code: str) -> list[int] | None:
        if self.history_client is None:
            return None
        found: list[int] = []
        exact_code_line = re.compile(
            rf"(?m)^{re.escape(CODE_PREFIX)}[ \t]+{re.escape(emoda_code)}[ \t]*$"
        )
        async for message in self.history_client.iter_messages(
            destination_channel_id, search=emoda_code, limit=20
        ):
            body = getattr(message, "message", "") or ""
            if exact_code_line.search(body):
                found.append(int(message.id))
        return sorted(found) or None

    async def delete(self, destination_channel_id: int, message_ids: list[int]) -> None:
        """Delete exact stored IDs; retrying this cleanup is safe and idempotent."""

        for message_id in message_ids:
            for attempt in range(3):
                try:
                    await self.bot.delete_message(destination_channel_id, message_id)
                    break
                except TelegramRetryAfter as exc:
                    if attempt == 2:
                        raise SafePublicationFailure(
                            "Telegram flood wait persisted while cleaning a partial publication"
                        ) from exc
                    await asyncio.sleep(min(max(float(exc.retry_after), 0), 60))
                except TelegramBadRequest as exc:
                    if "message to delete not found" in str(exc).casefold():
                        break
                    raise SafePublicationFailure(str(exc)) from exc
                except TelegramForbiddenError as exc:
                    raise SafePublicationFailure(str(exc)) from exc
                except TelegramNetworkError as exc:
                    if attempt == 2:
                        raise SafePublicationFailure(
                            "Telegram network failed while cleaning a partial publication"
                        ) from exc
                    await asyncio.sleep(2**attempt)

    async def _edit_media(
        self, channel_id: int, message_id: int, path: str, media_type: str,
        *, caption: str | None = None,
        keyboard: list[list[dict[str, str]]] | None = None,
    ) -> None:
        if self.requires_mtproto(path, media_type):
            if self.media_client is None:
                raise SafePublicationFailure("large media edit requires MTProto bot publisher")
            await self.media_client.edit_message(
                channel_id, message_id, file=path, text=caption or "",
                parse_mode="html", buttons=self.mtproto_buttons(keyboard or []),
                supports_streaming=media_type == "video",
            )
            return
        kwargs: dict[str, Any] = {}
        if keyboard is not None:
            kwargs["reply_markup"] = self.markup(keyboard)
        try:
            await self._call(self.bot.edit_message_media,
                chat_id=channel_id, message_id=message_id,
                media=input_media(path, media_type, caption=caption,
                    parse_mode=PUBLICATION_PARSE_MODE if caption is not None else None),
                **kwargs)
        except TelegramBadRequest as exc:
            if "message is not modified" not in str(exc).casefold():
                raise

    async def update(
        self,
        destination_channel_id: int,
        message_ids: list[int],
        old_payload: dict[str, object],
        new_payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        old_media = [str(path) for path in cast(list[object], old_payload.get("media_paths", []))]
        new_media = [str(path) for path in cast(list[object], new_payload.get("media_paths", []))]
        if not new_media:
            raise SafePublicationFailure(
                "media_required: publication requires at least one photo or video"
            )
        old_types = resolve_media_types(old_payload, old_media)
        new_types = resolve_media_types(new_payload, new_media)
        # These are Bot API limits, not internal failures, so each reason names the exact
        # structural difference an admin has to resolve before the edit can be replayed.
        if len(old_media) != len(new_media):
            raise SafePublicationFailure(
                f"publication structure changed: {len(old_media)} media -> {len(new_media)} media; "
                "Telegram cannot add or remove messages of a sent publication, manual review required"
            )
        # An in-place edit keeps the publication's message count: it can only fill the messages
        # that already exist, so content that would need more messages cannot be performed.
        old_messages = expected_message_count(old_payload)
        new_messages = expected_message_count(new_payload)
        if new_messages > old_messages:
            raise SafePublicationFailure(
                f"publication structure changed: {old_messages} -> {new_messages} Telegram messages; "
                "a sent publication keeps its message count, manual review required"
            )
        if any(kind not in {"photo", "video"} for kind in new_types):
            raise SafePublicationFailure("unsupported media type requires manual review")
        # Bot API editMessageMedia supports photo/video conversion, including albums.
        text = str(new_payload["text"])
        markup = self.markup(keyboard)
        try:
            if not old_media:
                if len(message_ids) != 1 or len(text) > 4096:
                    raise SafePublicationFailure("multi-message text update requires manual review")
                await self._call(
                    self.bot.edit_message_text,
                    text,
                    chat_id=destination_channel_id,
                    message_id=message_ids[-1],
                    parse_mode=PUBLICATION_PARSE_MODE,
                    reply_markup=markup,
                )
            elif len(old_media) == 1 and expected_message_count(old_payload) == 1:
                if len(text) > 1024:
                    raise SafePublicationFailure(
                        f"updated caption is {len(text)} characters; Telegram allows 1024 on a "
                        "media message, manual review required"
                    )
                if old_media[0] != new_media[0]:
                    # The media message owns both media and caption here, so one atomic call
                    # must carry the replacement media together with the new caption and markup.
                    await self._edit_media(destination_channel_id, message_ids[0],
                        new_media[0], new_types[0], caption=text, keyboard=keyboard)
                else:
                    await self._call(
                        self.bot.edit_message_caption,
                        chat_id=destination_channel_id,
                        message_id=message_ids[0],
                        caption=text,
                        parse_mode=PUBLICATION_PARSE_MODE,
                        reply_markup=markup,
                    )
            else:
                # Album media messages cannot own markup: replaced media goes out bare and the
                # trailing text message keeps the keyboard, exactly as send already documents.
                for position, (old_path, new_path) in enumerate(
                    zip(old_media, new_media, strict=True)
                ):
                    if old_path != new_path:
                        await self._edit_media(destination_channel_id, message_ids[position],
                            new_path, new_types[position])
                await self._call(
                    self.bot.edit_message_text,
                    text,
                    chat_id=destination_channel_id,
                    message_id=message_ids[-1],
                    parse_mode=PUBLICATION_PARSE_MODE,
                    reply_markup=markup,
                )
            return message_ids
        except TelegramBadRequest as exc:
            if "message is not modified" in str(exc).casefold():
                return message_ids
            raise SafePublicationFailure(str(exc)) from exc
        except TelegramForbiddenError as exc:
            raise SafePublicationFailure(str(exc)) from exc
        except TelegramNetworkError as exc:
            raise UncertainPublicationFailure(str(exc)) from exc
