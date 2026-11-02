from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import object_session

from emoda.db.base import utcnow
from emoda.db.models import MessageEventTransition, Product, SourceMessage
from emoda.domain.enums import MessageState

OUTCOME_STATES: dict[str, MessageState] = {
    "received": MessageState.RECEIVED,
    "collecting_album": MessageState.COLLECTING,
    "collecting": MessageState.COLLECTING,
    "attached_to_product": MessageState.ATTACHED_TO_PRODUCT,
    "skipped": MessageState.IGNORED,
    "ignored": MessageState.IGNORED,
    "duplicate": MessageState.DUPLICATE,
    "deferred": MessageState.DEFERRED,
    "processing": MessageState.PROCESSING,
    "ready_to_publish": MessageState.PROCESSING,
    "published": MessageState.PUBLISHED,
    "needs_review": MessageState.REVIEW,
    "reconciliation_required": MessageState.REVIEW,
    "failed": MessageState.FAILED,
}


async def set_duplicate_outcome_once(
    session: AsyncSession,
    message: SourceMessage,
    reason: str = "unchanged telegram update",
) -> bool:
    """Record one observable duplicate outcome without periodic catch-up spam."""
    existing = await session.scalar(
        select(MessageEventTransition.id)
        .where(
            MessageEventTransition.source_message_id == message.id,
            MessageEventTransition.state == MessageState.DUPLICATE,
            MessageEventTransition.origin == message.event_origin,
            MessageEventTransition.reason == reason,
        )
        .limit(1)
    )
    if existing is not None:
        return False
    set_message_outcome(message, "duplicate", reason, update_current=False)
    return True


def set_message_outcome(
    message: SourceMessage,
    outcome: str,
    reason: str | None = None,
    *,
    update_current: bool = True,
) -> None:
    state = OUTCOME_STATES.get(outcome, MessageState.FAILED)
    if update_current:
        metadata = {**message.raw_metadata, "outcome": outcome}
        if reason:
            metadata["outcome_reason"] = reason
        else:
            metadata.pop("outcome_reason", None)
        message.raw_metadata = metadata
        message.state = state
        message.state_reason = reason
    session = object_session(message)
    if session is not None and message.id is not None:
        session.add(
            MessageEventTransition(
                source_message_id=message.id,
                source_id=message.source_id,
                telegram_message_id=message.telegram_message_id,
                state=state,
                reason=reason,
                origin=message.event_origin,
                occurred_at=utcnow(),
            )
        )


async def set_product_message_outcome(
    session: AsyncSession,
    product: Product,
    outcome: str,
    reason: str | None = None,
) -> None:
    conditions = []
    if product.source_message_id is not None:
        conditions.append(SourceMessage.id == product.source_message_id)
    if product.album_group_id is not None:
        conditions.append(SourceMessage.album_group_id == product.album_group_id)
    if not conditions:
        return
    rows = list(await session.scalars(select(SourceMessage).where(*conditions).order_by(SourceMessage.id)))
    for row in rows:
        set_message_outcome(row, outcome, reason)
