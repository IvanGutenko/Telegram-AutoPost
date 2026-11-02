"""Durable, explicit admin dispatch to selected source channels."""
from __future__ import annotations

import asyncio
import secrets
from collections.abc import Awaitable, Callable
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from telethon.errors import RPCError

from emoda.db.models import BulkSendBatch, BulkSendTarget, Source


class BulkSendService:
    def __init__(self, sessions: async_sessionmaker[AsyncSession],
                 sender: Callable[[int, str, int], Awaitable[list[int]]]):
        self.sessions = sessions
        self.sender = sender

    async def create(self, admin_id: int, source_ids: list[int], text: str) -> int:
        if not text.strip() or len(text) > 4096:
            raise ValueError("Текст должен содержать 1–4096 символов")
        if not source_ids:
            raise ValueError("Выберите хотя бы один источник")
        async with self.sessions() as session, session.begin():
            sources = list(await session.scalars(select(Source).where(
                Source.id.in_(set(source_ids)), Source.archived_at.is_(None),
            ).order_by(Source.id)))
            if len(sources) != len(set(source_ids)):
                raise ValueError("Один из выбранных источников больше недоступен")
            batch = BulkSendBatch(admin_user_id=admin_id, text=text)
            session.add(batch)
            await session.flush()
            for source in sources:
                session.add(BulkSendTarget(batch_id=batch.id, source_id=source.id,
                    channel_id=source.telegram_channel_id, source_name=source.name,
                    random_id=secrets.randbits(63)))
            return batch.id

    async def send(self, batch_id: int, admin_id: int) -> None:
        async with self.sessions() as session:
            batch = await session.get(BulkSendBatch, batch_id)
            if batch is None or batch.admin_user_id != admin_id:
                raise LookupError("Рассылка не найдена")
            text = batch.text
            ids = list(await session.scalars(select(BulkSendTarget.id).where(
                BulkSendTarget.batch_id == batch_id).order_by(BulkSendTarget.id)))
        for target_id in ids:
            async with self.sessions() as session, session.begin():
                target = await session.get(BulkSendTarget, target_id, with_for_update=True)
                if target is None or target.status != "pending":
                    continue
                target.status = "sending"
                channel_id, random_id = target.channel_id, target.random_id
            # The durable claim precedes Telegram I/O. Unknown acceptance is never retried.
            try:
                message_ids = await asyncio.wait_for(self.sender(channel_id, text, random_id), timeout=45)
                status, error = "sent", None
            except RPCError as exc:
                message_ids, status, error = [], "failed", str(exc)
            except Exception as exc:
                message_ids, status, error = [], "uncertain", str(exc)
            async with self.sessions() as session, session.begin():
                target = await session.get(BulkSendTarget, target_id, with_for_update=True)
                assert target is not None
                target.status, target.error, target.message_ids = status, error, message_ids

    async def results(self, batch_id: int, admin_id: int) -> list[dict[str, Any]]:
        async with self.sessions() as session:
            batch = await session.get(BulkSendBatch, batch_id)
            if batch is None or batch.admin_user_id != admin_id:
                raise LookupError("Рассылка не найдена")
            targets = list(await session.scalars(select(BulkSendTarget).where(
                BulkSendTarget.batch_id == batch_id).order_by(BulkSendTarget.id)))
            return [{"source": target.source_name, "status": target.status,
                     "message_ids": target.message_ids, "error": target.error} for target in targets]
