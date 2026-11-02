from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin.router import create_admin_router
from emoda.db.base import Base
from emoda.db.models import Source


@pytest.mark.parametrize("enabled", [[], [True, False, True], [False], [True]])
async def test_sources_header_counts_only_visible_sources(tmp_path, enabled):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        session.add_all([Source(name=f"source{i}", telegram_channel_id=i + 1, enabled=value)
                         for i, value in enumerate(enabled)])
        session.add(Source(name="archived", telegram_channel_id=999, enabled=True,
                           archived_at=datetime.now(UTC)))
    router = create_admin_router(sessions, {900}, media_root=tmp_path,
                                 access_validator=AsyncMock(), test_message_loader=AsyncMock(),
                                 intelligence=None)
    handler = next(h.callback for h in router.callback_query.handlers if h.callback.__name__ == "sources")
    callback = SimpleNamespace(message=SimpleNamespace(edit_text=AsyncMock()), answer=AsyncMock())
    await handler(callback)
    text = callback.message.edit_text.call_args.args[0]
    assert text == (f"Источники\nВсего: {len(enabled)} · Включено: {sum(enabled)} · "
                    f"Выключено: {len(enabled) - sum(enabled)}")
    await engine.dispose()
