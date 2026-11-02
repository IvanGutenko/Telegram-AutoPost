from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import exists, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from emoda.db.models import AlbumGroup, Product, SourceMessage
from emoda.domain.enums import AlbumStatus, ProductStatus


class AlbumCoordinator:
    """Stores album debounce state in PostgreSQL, never in process memory."""

    def __init__(self, debounce_seconds: Decimal) -> None:
        self.debounce = timedelta(seconds=float(debounce_seconds))

    async def add_item(
        self, session: AsyncSession, message: SourceMessage, *, now: datetime
    ) -> AlbumGroup:
        if message.grouped_id is None:
            raise ValueError("album item requires grouped_id")
        dialect = session.bind.dialect.name if session.bind else ""
        created = False
        if dialect == "postgresql":
            inserted_id = await session.scalar(
                insert(AlbumGroup)
                .values(
                    source_id=message.source_id,
                    grouped_id=message.grouped_id,
                    status=AlbumStatus.COLLECTING,
                    last_seen_at=now,
                    ready_at=now + self.debounce,
                )
                .on_conflict_do_nothing(index_elements=["source_id", "grouped_id"])
                .returning(AlbumGroup.id)
            )
            created = inserted_id is not None
            group = await session.scalar(
                select(AlbumGroup)
                .where(
                    AlbumGroup.source_id == message.source_id,
                    AlbumGroup.grouped_id == message.grouped_id,
                )
                .with_for_update()
            )
            assert group is not None
        else:
            group = await session.scalar(
                select(AlbumGroup)
                .where(
                    AlbumGroup.source_id == message.source_id,
                    AlbumGroup.grouped_id == message.grouped_id,
                )
                .with_for_update()
            )
        if group is None:
            group = AlbumGroup(
                source_id=message.source_id,
                grouped_id=message.grouped_id,
                status=AlbumStatus.COLLECTING,
                last_seen_at=now,
                ready_at=now + self.debounce,
            )
            session.add(group)
            await session.flush()
            created = True
        if not created:
            await self.reopen(session, group, now=now)
        group.last_seen_at = now
        message.album_group_id = group.id
        message.processing_after = None
        await session.flush()
        return group

    async def attach_item(
        self,
        session: AsyncSession,
        group: AlbumGroup,
        message: SourceMessage,
        *,
        now: datetime,
    ) -> None:
        self._require_same_source(group, message)
        await self.reopen(session, group, now=now)
        group.last_seen_at = now
        message.album_group_id = group.id
        message.processing_after = None
        await session.flush()

    @staticmethod
    def _require_same_source(group: AlbumGroup, message: SourceMessage) -> None:
        """Reject a corrupt cross-source collection link before it reaches SQL.

        Album identifiers and Telegram message identifiers can legitimately be
        identical in different channels.  ``source_id`` is therefore part of
        the runtime identity, not merely metadata attached to the rows.
        """

        if group.source_id != message.source_id:
            raise ValueError("album group and message belong to a different source")

    async def reopen(self, session: AsyncSession, group: AlbumGroup, *, now: datetime) -> None:
        reprocessed = group.status is AlbumStatus.PROCESSED
        group.status = AlbumStatus.COLLECTING
        group.ready_at = now + self.debounce
        if reprocessed and group.product_id:
            product = await session.get(Product, group.product_id)
            if product is not None:
                if product.source_id != group.source_id:
                    raise ValueError("album group and product belong to a different source")
                # The group is reprocessed in full; whether the live post can
                # absorb the change is decided by the publisher, not here.
                product.status = ProductStatus.READY

    async def claim_ready(
        self, session: AsyncSession, *, now: datetime, limit: int = 20, source_id: int | None = None
    ) -> list[AlbumGroup]:
        result = await session.scalars(
            select(AlbumGroup)
            .where(
                AlbumGroup.status == AlbumStatus.COLLECTING,
                AlbumGroup.source_id == source_id if source_id is not None else True,
                AlbumGroup.ready_at <= now,
                ~exists(
                    select(SourceMessage.id).where(
                        SourceMessage.album_group_id == AlbumGroup.id,
                        SourceMessage.media_pending.is_(True),
                    )
                ),
            )
            .order_by(AlbumGroup.ready_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        groups = list(result)
        for group in groups:
            group.status = AlbumStatus.READY
        await session.flush()
        return groups

    async def items(self, session: AsyncSession, group_id: int) -> list[SourceMessage]:
        result = await session.scalars(
            select(SourceMessage)
            .where(SourceMessage.album_group_id == group_id)
            .order_by(SourceMessage.telegram_message_id)
        )
        return list(result)
