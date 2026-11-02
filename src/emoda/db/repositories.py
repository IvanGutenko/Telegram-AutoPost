from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

from sqlalchemy import Select, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from emoda.db.models import Source, SourceMessage, SourceMessageVersion


class SourceRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def by_channel(self, channel_id: int, *, enabled_only: bool = True) -> Source | None:
        query: Select[tuple[Source]] = select(Source).where(
            Source.telegram_channel_id == channel_id
        )
        if enabled_only:
            query = query.where(Source.enabled.is_(True), Source.archived_at.is_(None))
        return cast(Source | None, await self.session.scalar(query))


class MessageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def upsert(
        self,
        *,
        source_id: int,
        telegram_message_id: int,
        text: str,
        caption: str,
        telegram_date: datetime,
        edit_date: datetime | None,
        grouped_id: int | None,
        raw_metadata: dict[str, Any],
    ) -> tuple[SourceMessage, bool, bool]:
        existing = await self.session.scalar(
            select(SourceMessage)
            .where(
                SourceMessage.source_id == source_id,
                SourceMessage.telegram_message_id == telegram_message_id,
            )
            .with_for_update()
        )
        if existing is not None:
            changed = self._apply_newer_edit(
                existing,
                text=text,
                caption=caption,
                edit_date=edit_date,
                raw_metadata=raw_metadata,
            )
            return existing, False, changed
        dialect = self.session.bind.dialect.name if self.session.bind else ""
        if dialect == "postgresql":
            statement = (
                insert(SourceMessage)
                .values(
                    source_id=source_id,
                    telegram_message_id=telegram_message_id,
                    text=text,
                    caption=caption,
                    telegram_date=telegram_date,
                    edit_date=edit_date,
                    grouped_id=grouped_id,
                    raw_metadata=raw_metadata,
                )
                .on_conflict_do_nothing(index_elements=["source_id", "telegram_message_id"])
                .returning(SourceMessage)
            )
            inserted = await self.session.scalar(statement)
            if inserted is not None:
                return inserted, True, True
            existing = await self.session.scalar(
                select(SourceMessage)
                .where(
                    SourceMessage.source_id == source_id,
                    SourceMessage.telegram_message_id == telegram_message_id,
                )
                .with_for_update()
            )
            assert existing is not None
            changed = self._apply_newer_edit(
                existing,
                text=text,
                caption=caption,
                edit_date=edit_date,
                raw_metadata=raw_metadata,
            )
            return existing, False, changed
        message = SourceMessage(
            source_id=source_id,
            telegram_message_id=telegram_message_id,
            text=text,
            caption=caption,
            telegram_date=telegram_date,
            edit_date=edit_date,
            grouped_id=grouped_id,
            raw_metadata=raw_metadata,
        )
        self.session.add(message)
        await self.session.flush()
        return message, True, True

    @staticmethod
    def _utc(value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    def _apply_newer_edit(
        self,
        existing: SourceMessage,
        *,
        text: str,
        caption: str,
        edit_date: datetime | None,
        raw_metadata: dict[str, Any],
    ) -> bool:
        incoming_edit = self._utc(edit_date) if edit_date else None
        persisted_edit = self._utc(existing.edit_date) if existing.edit_date else None
        if incoming_edit is None or (
            persisted_edit is not None and incoming_edit <= persisted_edit
        ):
            return False
        self.session.add(
            SourceMessageVersion(
                source_message_id=existing.id,
                text=existing.text,
                caption=existing.caption,
                edit_date=existing.edit_date,
                captured_at=edit_date,
            )
        )
        existing.text = text
        existing.caption = caption
        existing.edit_date = edit_date
        existing.raw_metadata = raw_metadata
        return True
