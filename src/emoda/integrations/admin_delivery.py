from __future__ import annotations

import asyncio
import json
import logging
import math
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from aiogram.client.session.middlewares.base import BaseRequestMiddleware
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import SendMessage
from telethon.errors import FloodWaitError

logger = logging.getLogger(__name__)


class AdminChatLimiter(BaseRequestMiddleware):
    """Share private-chat pacing and durable Telegram cooldowns across all senders."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[Any]] = asyncio.sleep,
    ) -> None:
        self.path = path
        self.clock = clock
        self.sleep = sleep
        self.locks: dict[int, asyncio.Lock] = {}
        self.next_send: dict[int, float] = {}
        try:
            self.state: dict[str, dict[str, Any]] = json.loads(path.read_text())
        except FileNotFoundError:
            self.state = {}

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(".tmp")
        temporary.write_text(json.dumps(self.state))
        temporary.chmod(0o600)
        temporary.replace(self.path)

    def remaining(self, chat_id: int) -> int:
        until = float(self.state.get(str(chat_id), {}).get("until", 0))
        return max(0, math.ceil(until - self.clock()))

    def defer(self, chat_id: int, seconds: int) -> None:
        state = self.state.setdefault(str(chat_id), {})
        state["until"] = max(state.get("until", 0), self.clock() + seconds + 1)
        self._save()
        logger.warning("admin chat delivery deferred", extra={"chat_id": chat_id, "retry_after": seconds})

    async def _pace(self, chat_id: int, weight: int) -> None:
        delay = self.next_send.get(chat_id, 0) - self.clock()
        if delay > 0:
            await self.sleep(delay)
        self.next_send[chat_id] = self.clock() + max(1, weight) * 1.1

    async def __call__(self, make_request: Any, bot: Any, method: Any) -> Any:
        raw_chat = getattr(method, "chat_id", None)
        if not isinstance(raw_chat, (str, int)):
            return await make_request(bot, method)
        try:
            chat_id = int(raw_chat)
        except (TypeError, ValueError):
            return await make_request(bot, method)
        # Destination publication has its own durable locking/rate-limit policy.
        if chat_id <= 0:
            return await make_request(bot, method)
        api_method = method.__api_method__
        mutating = api_method.startswith(("send", "copy", "forward", "edit", "delete"))
        if not mutating:
            return await make_request(bot, method)
        async with self.locks.setdefault(chat_id, asyncio.Lock()):
            if self.remaining(chat_id):
                raise TelegramRetryAfter(method=method, message="admin chat cooldown", retry_after=self.remaining(chat_id))
            await self._pace(chat_id, len(getattr(method, "media", None) or []))
            try:
                return await make_request(bot, method)
            except TelegramRetryAfter as exc:
                self.defer(chat_id, exc.retry_after)
                raise

    async def send_file(self, client: Any, chat_id: Any, *args: Any, **kwargs: Any) -> Any:
        if not isinstance(chat_id, int) or chat_id <= 0:
            return await client.send_file(chat_id, *args, **kwargs)
        async with self.locks.setdefault(chat_id, asyncio.Lock()):
            method = SendMessage(chat_id=chat_id, text="media")
            if self.remaining(chat_id):
                raise TelegramRetryAfter(method=method, message="admin chat cooldown", retry_after=self.remaining(chat_id))
            files = args[0] if args else kwargs.get("file")
            await self._pace(chat_id, len(files) if isinstance(files, list) else 1)
            try:
                return await client.send_file(chat_id, *args, **kwargs)
            except FloodWaitError as exc:
                self.defer(chat_id, exc.seconds)
                raise TelegramRetryAfter(method=method, message="MTProto flood wait", retry_after=exc.seconds) from exc
