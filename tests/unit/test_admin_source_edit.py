from __future__ import annotations

import logging
import re
from types import SimpleNamespace
from typing import Any

import pytest
from aiogram.exceptions import TelegramBadRequest
from aiogram.methods import AnswerCallbackQuery
from aiogram.types import CallbackQuery, User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin.router import (
    AdminAccessMiddleware,
    create_admin_router,
    deliver_admin_product_media,
    edit_screen,
)
from emoda.db.base import Base
from emoda.db.models import (
    AdminUser,
    Destination,
    Product,
    PublicationOutbox,
    Source,
    SourceProfile,
)
from emoda.domain.enums import OutboxStatus, ProductStatus, PublicationMode
from emoda.domain.schemas import SourceProfileData


class FakeMessage:
    def __init__(self) -> None:
        self.edits: list[tuple[str, object | None]] = []

    async def edit_text(
        self, text: str, reply_markup: object | None = None, **_: object
    ) -> None:
        self.edits.append((text, reply_markup))


class FakeCallback:
    def __init__(self, data: str) -> None:
        self.data = data
        self.message = FakeMessage()
        self.from_user = SimpleNamespace(id=900)
        self.answers: list[tuple[str, bool]] = []

    async def answer(self, text: str | None = None, show_alert: bool = False) -> None:
        self.answers.append((text or "", show_alert))


class FakeState:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.state: object | None = None

    async def clear(self) -> None:
        self.data.clear()
        self.state = None

    async def update_data(self, **values: Any) -> None:
        self.data.update(values)

    async def get_data(self) -> dict[str, Any]:
        return dict(self.data)

    async def set_state(self, state: object | None) -> None:
        self.state = state


class FakeBot:
    def __init__(self) -> None:
        self.methods: list[object] = []

    async def __call__(self, method: object) -> bool:
        self.methods.append(method)
        return True


async def test_edit_screen_updates_caption_for_review_card_with_photo() -> None:
    class MediaMessage:
        photo = [object()]
        video = None

        def __init__(self) -> None:
            self.caption_edits: list[tuple[str, object | None]] = []

        async def edit_text(self, text: str, reply_markup: object | None = None) -> None:
            raise AssertionError("photo card must be edited through its caption")

        async def edit_caption(
            self, caption: str, reply_markup: object | None = None
        ) -> None:
            self.caption_edits.append((caption, reply_markup))

    message = MediaMessage()
    markup = object()
    callback = SimpleNamespace(message=message)

    await edit_screen(callback, "Опубликовано", markup)  # type: ignore[arg-type]

    assert message.caption_edits == [("Опубликовано", markup)]


async def test_admin_product_search_sends_large_video_through_mtproto(
    tmp_path, monkeypatch
) -> None:
    video = tmp_path / "large.mp4"
    video.write_bytes(b"large-video")
    monkeypatch.setattr(
        "emoda.integrations.publisher.BOT_API_VIDEO_LIMIT_BYTES", 1
    )

    class SearchMessage:
        chat = SimpleNamespace(id=900)

        async def answer_photo(self, *_: object, **__: object) -> None:
            raise AssertionError("video must not be sent as a photo")

        async def answer_video(self, *_: object, **__: object) -> None:
            raise AssertionError("large video must bypass Bot API")

    class MediaClient:
        def __init__(self) -> None:
            self.calls: list[tuple[int, object, dict[str, object]]] = []

        async def send_file(
            self, chat_id: int, file: object, **kwargs: object
        ) -> None:
            self.calls.append((chat_id, file, kwargs))

    media_client = MediaClient()
    await deliver_admin_product_media(
        SearchMessage(), str(video), "video", media_client=media_client
    )  # type: ignore[arg-type]

    assert media_client.calls == [
        (900, str(video), {"supports_streaming": True})
    ]


async def test_review_editor_rejects_text_outside_global_renderer(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        product = Product(
            emoda_code="AA00001",
            source_id=source_id,
            status=ProductStatus.NEEDS_REVIEW,
            final_text=(
                "<b>Код / Code:</b> AA00001\n"
                "<b>Цена за шт / Unit price:</b> $17\n"
                "<b>Размеры / Sizes:</b> S, M\n"
                "<b>Шт в серии / Pcs per lot:</b> 4"
            ),
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    class InputMessage:
        text = "17 долларов"

        def __init__(self) -> None:
            self.answers: list[str] = []

        async def answer(self, text: str, reply_markup: object | None = None) -> None:
            self.answers.append(text)

    state = FakeState()
    await state.update_data(product_id=product_id)
    message = InputMessage()

    handler = next(
        item.callback
        for item in router.message.handlers
        if item.callback.__name__ == "review_edit_save"
    )
    await handler(message, state)

    async with sessions() as session:
        persisted = await session.get(Product, product_id)
        assert persisted is not None
        assert "$17" in persisted.final_text
    assert state.data == {"product_id": product_id}
    assert message.answers and message.answers[0].startswith("Не сохранено:")
    await engine.dispose()


def callback_handler(router, name: str):
    for handler in router.callback_query.handlers:
        if handler.callback.__name__ == name:
            return handler.callback
    raise AssertionError(f"callback handler is not registered: {name}")


def callback_handler_object(router, name: str):
    for handler in router.callback_query.handlers:
        if handler.callback.__name__ == name:
            return handler
    raise AssertionError(f"callback handler is not registered: {name}")


async def build_router(
    tmp_path,
    *,
    publication_mode: PublicationMode = PublicationMode.REVIEW,
    enabled: bool = False,
    publish_product: Any = None,
):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        admin = AdminUser(telegram_user_id=900, display_name="Admin", enabled=True)
        destination = Destination(name="Main destination", telegram_channel_id=-100900)
        session.add_all([admin, destination])
        await session.flush()
        profile = SourceProfileData(
            publication_mode=publication_mode,
            default_destination_id=destination.id,
        )
        source = Source(
            name="Blind source",
            telegram_channel_id=-1900,
            telegram_ref="@blind_source",
            enabled=enabled,
        )
        source.profile = SourceProfile(
            publication_mode=publication_mode,
            config=profile.model_dump(mode="json"),
        )
        session.add(source)
        await session.flush()
        source_id = source.id

    async def access_validator(_: str) -> tuple[int, str, int]:
        return -1900, "Blind source", 10

    async def test_message_loader(_: str) -> tuple[str, list[str]]:
        return "", []

    router = create_admin_router(
        sessions,
        {900},
        access_validator=access_validator,
        test_message_loader=test_message_loader,
        intelligence=None,
        media_root=tmp_path,
        publish_product=publish_product,
    )
    return engine, sessions, router, source_id


@pytest.mark.asyncio
async def test_reconciliation_retry_authorizes_safe_outbox_rearm(tmp_path) -> None:
    calls: list[tuple[int, str | None, bool]] = []

    async def publish_product(
        product_id: int, expected_token: str | None, *, resume: bool = False
    ) -> object:
        calls.append((product_id, expected_token, resume))
        return object()

    engine, sessions, router, source_id = await build_router(
        tmp_path, publish_product=publish_product
    )
    async with sessions() as session, session.begin():
        destination = await session.scalar(select(Destination))
        assert destination is not None
        product = Product(
            emoda_code="AA01601",
            source_id=source_id,
            destination_id=destination.id,
            status=ProductStatus.RECONCILIATION_REQUIRED,
            final_text=(
                "<b>Код / Code:</b> AA01601\n"
                "<b>Цена за шт / Unit price:</b> $17\n"
                "<b>Размеры / Sizes:</b> S, M\n"
                "<b>Шт в серии / Pcs per lot:</b> 4"
            ),
        )
        session.add(product)
        await session.flush()
        session.add(
            PublicationOutbox(
                product_id=product.id,
                destination_id=destination.id,
                payload={"emoda_code": product.emoda_code, "text": product.final_text},
                payload_hash="retry-fixture",
                status=OutboxStatus.RECONCILIATION_REQUIRED,
                last_error="uncertain fixture",
            )
        )
        product_id = product.id

    callback = FakeCallback(f"reconcile:retry:{product_id}")
    await callback_handler(router, "reconciliation_retry")(callback)

    assert calls == [(product_id, None, True)]
    async with sessions() as session:
        product = await session.get(Product, product_id)
        outbox = await session.scalar(
            select(PublicationOutbox).where(PublicationOutbox.product_id == product_id)
        )
        assert product is not None and product.status is ProductStatus.READY_TO_PUBLISH
        assert outbox is not None and outbox.status is OutboxStatus.FAILED
        assert outbox.last_error == "explicit retry authorized in /admin"
    await engine.dispose()


@pytest.mark.asyncio
async def test_reconciliation_card_allows_editing_before_retry(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        product = Product(
            emoda_code="AA00453",
            source_id=source_id,
            status=ProductStatus.RECONCILIATION_REQUIRED,
            final_text="legacy three-line payload",
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    callback = FakeCallback(f"review:{product_id}")
    await callback_handler(router, "review_card")(callback)

    markup = callback.message.edits[-1][1]
    callbacks = {
        button.callback_data
        for row in markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    }
    assert f"review:edit:{product_id}" in callbacks
    assert f"reconcile:retry:{product_id}" in callbacks
    await engine.dispose()


@pytest.mark.asyncio
async def test_reconciliation_retry_keeps_invalid_payload_safe_for_editing(tmp_path) -> None:
    calls: list[int] = []

    async def publish_product(
        product_id: int, expected_token: str | None, *, resume: bool = False
    ) -> object:
        calls.append(product_id)
        return object()

    engine, sessions, router, source_id = await build_router(
        tmp_path, publish_product=publish_product
    )
    async with sessions() as session, session.begin():
        destination = await session.scalar(select(Destination))
        assert destination is not None
        product = Product(
            emoda_code="AA00453",
            source_id=source_id,
            destination_id=destination.id,
            status=ProductStatus.RECONCILIATION_REQUIRED,
            final_text="Цена указана на фото\nКомиссия E-MODA: +10%\nк цене.",
        )
        session.add(product)
        await session.flush()
        outbox = PublicationOutbox(
            product_id=product.id,
            destination_id=destination.id,
            payload={"emoda_code": product.emoda_code, "text": product.final_text},
            payload_hash="invalid-retry-fixture",
            status=OutboxStatus.RECONCILIATION_REQUIRED,
            last_error="uncertain fixture",
        )
        session.add(outbox)
        await session.flush()
        product_id = product.id
        outbox_id = outbox.id

    callback = FakeCallback(f"reconcile:retry:{product_id}")
    await callback_handler(router, "reconciliation_retry")(callback)

    assert calls == []
    assert "сначала исправьте" in callback.message.edits[-1][0].casefold()
    async with sessions() as session:
        product = await session.get(Product, product_id)
        outbox = await session.get(PublicationOutbox, outbox_id)
        assert product is not None
        assert product.status is ProductStatus.RECONCILIATION_REQUIRED
        assert outbox is not None
        assert outbox.status is OutboxStatus.RECONCILIATION_REQUIRED
        assert outbox.last_error == "uncertain fixture"
    await engine.dispose()


@pytest.mark.asyncio
async def test_product_search_opens_old_reconciliation_card(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        product = Product(
            emoda_code="AA00453",
            source_id=source_id,
            status=ProductStatus.RECONCILIATION_REQUIRED,
            final_text="legacy payload",
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    class SearchMessage:
        text = "AA00453"

        def __init__(self) -> None:
            self.answers: list[tuple[str, object | None]] = []

        async def answer(
            self,
            text: str,
            reply_markup: object | None = None,
            **_: object,
        ) -> None:
            self.answers.append((text, reply_markup))

        async def answer_photo(self, *_: object, **__: object) -> None:
            raise AssertionError("fixture has no media")

    message = SearchMessage()
    await next(
        item.callback
        for item in router.message.handlers
        if item.callback.__name__ == "product_search"
    )(message, FakeState())

    result_markup = message.answers[0][1]
    assert result_markup is not None
    callbacks = {
        button.callback_data
        for row in result_markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    }
    assert callbacks == {f"review:{product_id}", "menu:products", "menu:main"}
    await engine.dispose()


async def test_product_search_returns_one_card_and_pages_without_a_message_burst(tmp_path):
    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        for code in ("AA64522", "AA64523", "AA64524"):
            session.add(Product(emoda_code=code, source_id=source_id, original_text="model 7728"))

    class SearchMessage:
        text = "7728"
        answers = []

        async def answer(self, text, **kwargs):
            self.answers.append((text, kwargs))

        async def answer_photo(self, *args, **kwargs):
            raise AssertionError("search must not upload unsolicited media")

    message = SearchMessage()
    state = FakeState()
    await next(item.callback for item in router.message.handlers if item.callback.__name__ == "product_search")(message, state)
    assert len(message.answers) == 1
    assert "1 из 3" in message.answers[0][0]
    first_ids = state.data["product_result_ids"]
    callback = FakeCallback("product:result:1")
    await callback_handler(router, "product_search_page")(callback, state)
    assert "2 из 3" in callback.message.edits[-1][0]
    assert state.data["product_result_ids"] == first_ids
    await engine.dispose()


async def test_blocked_admin_action_does_not_advance_flow(tmp_path):
    from datetime import UTC, datetime

    from aiogram.types import Chat, Message

    from emoda.integrations.admin_delivery import AdminChatLimiter

    engine, sessions, _, _ = await build_router(tmp_path)
    guard = AdminChatLimiter(tmp_path / "limits.json")
    guard.defer(900, 100)
    middleware = AdminAccessMiddleware(sessions, {900}, chat_limiter=guard)
    message = Message(
        message_id=60001, date=datetime.now(UTC), chat=Chat(id=900, type="private"),
        from_user=User(id=900, is_bot=False, first_name="Admin"), text="/admin",
    )

    async def handler(event, data):
        raise AssertionError("blocked admin flow must not mutate sources or FSM")

    await middleware(handler, message, {"event_from_user": message.from_user})
    await engine.dispose()


async def test_private_cooldown_does_not_block_admin_in_another_chat(tmp_path):
    from datetime import UTC, datetime

    from aiogram.types import Chat, Message

    from emoda.integrations.admin_delivery import AdminChatLimiter

    engine, sessions, _, _ = await build_router(tmp_path)
    guard = AdminChatLimiter(tmp_path / "limits.json")
    guard.defer(900, 100)
    middleware = AdminAccessMiddleware(sessions, {900}, chat_limiter=guard)
    message = Message(
        message_id=42, date=datetime.now(UTC), chat=Chat(id=-100123, type="supergroup"),
        from_user=User(id=900, is_bot=False, first_name="Admin"), text="/admin",
    )
    calls = []

    async def handler(event, data):
        calls.append(event.chat.id)

    await middleware(handler, message, {"event_from_user": message.from_user})
    assert calls == [-100123]
    await engine.dispose()



@pytest.mark.asyncio
async def test_source_edit_callback_opens_registered_screen_and_keeps_source_id(tmp_path) -> None:
    engine, _, router, source_id = await build_router(tmp_path)
    card_callback = FakeCallback(f"source:{source_id}")
    await callback_handler(router, "source_card")(card_callback)
    card_markup = card_callback.message.edits[-1][1]
    card_callbacks = {
        button.callback_data
        for row in card_markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    }
    assert f"source:edit:{source_id}" in card_callbacks

    registered = callback_handler_object(router, "source_edit_screen")
    raw_callback = CallbackQuery(
        id="source-edit-callback",
        from_user=User(id=900, is_bot=False, first_name="Admin"),
        chat_instance="test",
        data=f"source:edit:{source_id}",
    )
    matches, _ = await registered.check(raw_callback)
    assert matches is True

    callback = FakeCallback(f"source:edit:{source_id}")
    state = FakeState()

    await callback_handler(router, "source_edit_screen")(callback, state)

    assert state.data == {"source_id": source_id}
    assert callback.answers == [("", False)]
    assert len(callback.message.edits) == 1
    text, markup = callback.message.edits[0]
    assert "Blind source" in text
    assert "@blind_source" in text
    assert "Enabled: False" in text
    assert "Publication mode: REVIEW" in text
    assert "Destination: Main destination" in text
    callbacks = {
        button.callback_data for row in markup.inline_keyboard for button in row  # type: ignore[union-attr]
    }
    assert f"source:mode:{source_id}:auto" in callbacks
    assert f"source:enabled:{source_id}:true" in callbacks
    assert f"source:identity:{source_id}" in callbacks
    for destination in (
        f"source:collect:{source_id}",
        f"source:rules:{source_id}",
        f"source:clean:{source_id}",
        f"source:gemini:{source_id}",
        f"source:route:{source_id}",
        f"source:examples:{source_id}",
        f"source:test:{source_id}",
        f"source:{source_id}",
    ):
        assert destination in callbacks
    await engine.dispose()


async def test_rename_does_not_revalidate_channel_or_move_history_anchor(tmp_path):
    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        source = await session.get(Source, source_id)
        source.catchup_anchor_message_id = 12
        source.last_seen_message_id = 25
        channel_id = source.telegram_channel_id
        profile_config = source.profile.config
    state = FakeState()
    await state.update_data(source_id=source_id)
    answers = []

    async def answer(text, **kwargs):
        answers.append(text)

    message = SimpleNamespace(text="name=Renamed source", answer=answer)
    handler = next(h.callback for h in router.message.handlers if h.callback.__name__ == "source_edit_save")
    await handler(message, state)
    async with sessions() as session:
        source = await session.get(Source, source_id)
        assert source.name == "Renamed source", answers
        assert source.telegram_channel_id == channel_id
        assert source.catchup_anchor_message_id == 12
        assert source.last_seen_message_id == 25
        assert source.profile.config == profile_config
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("initial", "target"),
    [
        (PublicationMode.REVIEW, PublicationMode.AUTO),
        (PublicationMode.AUTO, PublicationMode.REVIEW),
    ],
)
async def test_source_publication_mode_switch_persists_in_new_session(
    tmp_path, initial: PublicationMode, target: PublicationMode
) -> None:
    engine, sessions, router, source_id = await build_router(
        tmp_path, publication_mode=initial
    )
    callback = FakeCallback(f"source:mode:{source_id}:{target.value}")

    await callback_handler(router, "source_publication_mode_set")(callback)

    async with sessions() as fresh_session:
        source = await fresh_session.get(Source, source_id)
        assert source is not None
        assert source.profile.publication_mode is target
        profile = SourceProfileData.model_validate(source.profile.config)
        assert profile.publication_mode is target
        assert profile.use_global_publication_mode is False
    assert f"Publication mode: {target.value.upper()}" in callback.message.edits[-1][0]
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(("initial", "target"), [(False, True), (True, False)])
async def test_source_enabled_switch_persists_in_new_session(
    tmp_path, initial: bool, target: bool
) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path, enabled=initial)
    callback = FakeCallback(f"source:enabled:{source_id}:{str(target).lower()}")

    await callback_handler(router, "source_enabled_set")(callback)

    async with sessions() as fresh_session:
        source = await fresh_session.get(Source, source_id)
        assert source is not None and source.enabled is target
    assert f"Enabled: {target}" in callback.message.edits[-1][0]
    await engine.dispose()


@pytest.mark.asyncio
async def test_unauthorized_telegram_user_cannot_change_source(tmp_path) -> None:
    engine, sessions, _, source_id = await build_router(tmp_path)
    middleware = AdminAccessMiddleware(sessions, set())
    called = False

    async def mutate_source(_: object, __: dict[str, Any]) -> None:
        nonlocal called
        called = True
        async with sessions() as session, session.begin():
            source = await session.get(Source, source_id)
            assert source is not None
            source.enabled = True

    callback = CallbackQuery(
        id="unauthorized-callback",
        from_user=User(id=901, is_bot=False, first_name="Intruder"),
        chat_instance="test",
        data=f"source:enabled:{source_id}:true",
    ).as_(FakeBot())

    await middleware(
        mutate_source,
        callback,
        {"event_from_user": SimpleNamespace(id=901, full_name="Intruder")},
    )

    assert called is False
    assert callback.bot.methods[0].text == "Доступ запрещён."  # type: ignore[union-attr]
    async with sessions() as fresh_session:
        source = await fresh_session.get(Source, source_id)
        assert source is not None and source.enabled is False
    await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_name", "data", "needs_state"),
    [
        ("source_edit_screen", "source:edit:999999", True),
        ("source_publication_mode_set", "source:mode:999999:auto", False),
        ("source_enabled_set", "source:enabled:999999:true", False),
    ],
)
async def test_deleted_source_callback_returns_visible_error(
    tmp_path, handler_name: str, data: str, needs_state: bool
) -> None:
    engine, _, router, _ = await build_router(tmp_path)
    callback = FakeCallback(data)
    handler = callback_handler(router, handler_name)

    if needs_state:
        await handler(callback, FakeState())
    else:
        await handler(callback)

    assert callback.message.edits == []
    assert callback.answers == [("Источник не найден или удалён.", True)]
    await engine.dispose()


@pytest.mark.asyncio
async def test_callback_exception_is_logged_and_reported_to_admin(tmp_path, caplog) -> None:
    engine, sessions, _, _ = await build_router(tmp_path)
    middleware = AdminAccessMiddleware(sessions, {902})
    bot = FakeBot()
    callback = CallbackQuery(
        id="broken-callback",
        from_user=User(id=902, is_bot=False, first_name="Admin"),
        chat_instance="test",
        data="source:broken:1",
    ).as_(bot)

    async def broken_handler(_: object, __: dict[str, Any]) -> None:
        raise RuntimeError("expected callback failure")

    with caplog.at_level(logging.ERROR, logger="emoda.admin.router"):
        await middleware(
            broken_handler,
            callback,
            {"event_from_user": SimpleNamespace(id=902, full_name="Admin")},
        )

    assert "admin callback handler failed" in caplog.text
    assert re.match(r"Не удалось выполнить действие. Код ошибки: [0-9a-f]{8}$", bot.methods[-1].text)
    assert bot.methods[-1].show_alert is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_stale_callback_lookup_error_is_reported_without_generic_failure(
    tmp_path, caplog
) -> None:
    engine, sessions, _, _ = await build_router(tmp_path)
    middleware = AdminAccessMiddleware(sessions, {902})
    bot = FakeBot()
    callback = CallbackQuery(
        id="stale-callback",
        from_user=User(id=902, is_bot=False, first_name="Admin"),
        chat_instance="test",
        data="source:removed:999999",
    ).as_(bot)

    async def stale_handler(_: object, __: dict[str, Any]) -> None:
        raise LookupError("source not found")

    with caplog.at_level(logging.ERROR, logger="emoda.admin.router"):
        await middleware(
            stale_handler,
            callback,
            {"event_from_user": SimpleNamespace(id=902, full_name="Admin")},
        )

    assert "admin callback handler failed" not in caplog.text
    assert bot.methods[-1].text == "Объект уже отсутствует или недоступен."
    assert bot.methods[-1].show_alert is True
    await engine.dispose()


@pytest.mark.asyncio
async def test_expired_telegram_callback_ack_is_not_reported_as_crud_failure(
    tmp_path, caplog
) -> None:
    engine, sessions, _, _ = await build_router(tmp_path)
    middleware = AdminAccessMiddleware(sessions, {902})
    bot = FakeBot()
    callback = CallbackQuery(
        id="expired-callback",
        from_user=User(id=902, is_bot=False, first_name="Admin"),
        chat_instance="test",
        data="source:profile:1",
    ).as_(bot)

    async def expired_handler(_: object, __: dict[str, Any]) -> None:
        raise TelegramBadRequest(
            method=AnswerCallbackQuery(callback_query_id="expired-callback"),
            message="Bad Request: query is too old and response timeout expired",
        )

    with caplog.at_level(logging.WARNING, logger="emoda.admin.router"):
        await middleware(
            expired_handler,
            callback,
            {"event_from_user": SimpleNamespace(id=902, full_name="Admin")},
        )

    assert "expired admin callback acknowledgment ignored" in caplog.text
    assert "admin callback handler failed" not in caplog.text
    assert bot.methods == []
    await engine.dispose()


@pytest.mark.asyncio
async def test_unknown_admin_callback_returns_visible_error(tmp_path, caplog) -> None:
    engine, _, router, _ = await build_router(tmp_path)
    callback = FakeCallback("source:removed-action:1")

    with caplog.at_level(logging.WARNING, logger="emoda.admin.router"):
        await callback_handler(router, "unknown_admin_callback")(callback)

    assert "unhandled admin callback" in caplog.text
    assert callback.answers == [
        ("Действие устарело или недоступно. Откройте /admin заново.", True)
    ]
    await engine.dispose()


@pytest.mark.asyncio
async def test_stale_publish_for_skipped_product_shows_friendly_noop(tmp_path) -> None:
    from emoda.services.review import StaleReviewApproval

    calls: list[tuple[int, str]] = []

    async def publish_product(product_id: int, expected_token: str) -> object:
        calls.append((product_id, expected_token))
        raise StaleReviewApproval("product is skipped")

    engine, sessions, router, source_id = await build_router(
        tmp_path, publish_product=publish_product
    )
    async with sessions() as session, session.begin():
        product = Product(
            emoda_code="AA00454",
            source_id=source_id,
            status=ProductStatus.SKIPPED,
            final_text="Skipped product",
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    callback = FakeCallback(f"review:publish:{product_id}:0123456789abcdef")
    await callback_handler(router, "review_publish")(callback)

    assert calls == [(product_id, "0123456789abcdef")]
    assert callback.message.edits
    text, markup = callback.message.edits[-1]
    assert "уже пропущен" in text.lower()
    assert all(
        button.callback_data != callback.data
        for row in markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    )
    await engine.dispose()


@pytest.mark.asyncio
async def test_skipped_review_card_has_no_publish_or_skip_actions(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        product = Product(
            emoda_code="AA00455",
            source_id=source_id,
            status=ProductStatus.SKIPPED,
            final_text="Skipped product",
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    callback = FakeCallback(f"review:{product_id}")
    await callback_handler(router, "review_card")(callback)
    text, markup = callback.message.edits[-1]
    callbacks = {
        button.callback_data
        for row in markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
    }
    assert "пропущен" in text.lower()
    assert not any(value.startswith("review:publish:") for value in callbacks)
    assert not any(value.startswith("review:skip:") for value in callbacks)
    await engine.dispose()


async def test_uncertain_notification_is_visible_with_working_review_actions(tmp_path) -> None:
    from emoda.db.models import GlobalSetting
    from emoda.services.admin_delivery import review_delivery_key

    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        product = Product(emoda_code='AA00999', source_id=source_id,
                          status=ProductStatus.NEEDS_REVIEW, final_text='Review item')
        session.add(product)
        await session.flush()
        session.add(GlobalSetting(key=review_delivery_key(900, product), value={
            'intent': {'step': 0, 'method': 'send_photo'},
        }))
        product_id = product.id
    callback = FakeCallback(f'review:{product_id}')
    await callback_handler(router, 'review_card')(callback)
    text, markup = callback.message.edits[-1]
    assert 'Автоматическая доставка уведомления остановлена' in text
    assert 'не подтверждён' in text
    callbacks = {b.callback_data for row in markup.inline_keyboard for b in row}
    assert f'review:skip:{product_id}' in callbacks
    assert f'review:edit:{product_id}' in callbacks
    await engine.dispose()


@pytest.mark.asyncio
async def test_skip_button_does_not_overwrite_published_product(tmp_path) -> None:
    from emoda.db.models import AuditLog

    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        product = Product(
            emoda_code="AA00456",
            source_id=source_id,
            status=ProductStatus.PUBLISHED,
            final_text="Published product",
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    callback = FakeCallback(f"review:skip:{product_id}")
    await callback_handler(router, "review_skip")(callback)

    async with sessions() as session:
        product = await session.get(Product, product_id)
        skip_actions = list(await session.scalars(select(AuditLog).where(AuditLog.action == "product.skip")))
    assert product is not None and product.status is ProductStatus.PUBLISHED
    assert skip_actions == []
    assert "уже опубликован" in callback.message.edits[-1][0].lower()
    await engine.dispose()


@pytest.mark.asyncio
async def test_repeat_skip_button_is_noop(tmp_path) -> None:
    from emoda.db.models import AuditLog

    engine, sessions, router, source_id = await build_router(tmp_path)
    async with sessions() as session, session.begin():
        product = Product(
            emoda_code="AA00457",
            source_id=source_id,
            status=ProductStatus.NEEDS_REVIEW,
            final_text="Review product",
        )
        session.add(product)
        await session.flush()
        product_id = product.id

    callback = FakeCallback(f"review:skip:{product_id}")
    await callback_handler(router, "review_skip")(callback)
    await callback_handler(router, "review_skip")(callback)

    async with sessions() as session:
        product = await session.get(Product, product_id)
        skip_actions = list(await session.scalars(select(AuditLog).where(AuditLog.action == "product.skip")))
    assert product is not None and product.status is ProductStatus.SKIPPED
    assert len(skip_actions) == 1
    assert "уже пропущен" in callback.message.edits[-1][0].lower()
    await engine.dispose()
