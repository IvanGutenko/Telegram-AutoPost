from __future__ import annotations

import time
from typing import Any

from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from sqlalchemy.exc import IntegrityError

from emoda.db.models import GlobalSetting, Product
from emoda.services.review import review_approval_token


def review_delivery_key(admin_id: int, product: Product) -> str:
    return f"review_delivery:{admin_id}:{product.id}:{review_approval_token(product)}"


def delivery_stopped(state: dict[str, Any]) -> bool:
    return bool(state.get("intent") or state.get("halted"))


class UncertainReviewDelivery(RuntimeError):
    """A previous send has no confirmed result; automatic repetition is unsafe."""


class ReviewDeliveryProgress:
    """Checkpoint successful notification parts and each recipient in PostgreSQL."""

    def __init__(self, sessions: Any, key: str, state: dict[str, Any]) -> None:
        self.sessions = sessions
        self.key = key
        self.state = state
        self.position = 0

    @classmethod
    async def load(cls, sessions: Any, key: str) -> ReviewDeliveryProgress:
        async with sessions() as session, session.begin():
            row = await session.get(GlobalSetting, key)
            if row is None:
                try:
                    async with session.begin_nested():
                        session.add(GlobalSetting(key=key, value={}))
                        await session.flush()
                except IntegrityError:
                    # Another sender may have created the same recipient/version.
                    pass
                row = await session.get(GlobalSetting, key)
            return cls(sessions, key, dict(row.value) if row else {})

    async def save(self, **values: Any) -> None:
        async with self.sessions() as session, session.begin():
            row = await session.get(GlobalSetting, self.key, with_for_update=True)
            if row is None:
                raise UncertainReviewDelivery("delivery checkpoint disappeared")
            state = dict(row.value)
            state.update(values)
            row.value = state
        self.state = state

    async def prepare(self, step: int, method: str) -> bool:
        # Commit intent before Telegram I/O. A crash, timeout or failed success
        # checkpoint then leaves a durable stop instead of replaying the send.
        async with self.sessions() as session, session.begin():
            row = await session.get(GlobalSetting, self.key, with_for_update=True)
            if row is None:
                raise UncertainReviewDelivery("delivery checkpoint disappeared")
            state = dict(row.value)
            if step < state.get("steps", 0):
                self.state = state
                return False
            if delivery_stopped(state):
                raise UncertainReviewDelivery("previous delivery has no safe automatic retry")
            state["intent"] = {"step": step, "method": method, "started_at": time.time()}
            row.value = state
        self.state = state
        return True

    def wrap(self, client: Any) -> Any:
        progress = self

        class CheckpointClient:
            def __getattr__(self, name: str) -> Any:
                original = getattr(client, name)
                if not name.startswith("send_"):
                    return original

                async def send(*args: Any, **kwargs: Any) -> Any:
                    step = progress.position
                    progress.position += 1
                    if not await progress.prepare(step, name):
                        return None
                    try:
                        result = await original(*args, **kwargs)
                    except TelegramRetryAfter:
                        # This response confirms rejection; the shared limiter
                        # handles the wait, and the same part can safely resume.
                        await progress.save(intent=None)
                        raise
                    except (TelegramBadRequest, TelegramForbiddenError) as exc:
                        failures = progress.state.get("failures", 0) + 1
                        await progress.save(
                            intent=None, failures=failures,
                            halted=failures >= 3 or isinstance(exc, TelegramForbiddenError),
                            last_error_type=type(exc).__name__,
                        )
                        raise
                    # Other errors/cancellation retain intent. The next loop
                    # and a new process must not send this uncertain part again.
                    await progress.save(steps=step + 1, intent=None)
                    return result

                return send

        return CheckpointClient()
