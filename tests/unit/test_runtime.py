import asyncio
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from sqlalchemy.exc import TimeoutError as SQLAlchemyTimeoutError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import Product, Source
from emoda.domain.enums import ProductStatus
from emoda.runtime import app as runtime_app
from emoda.runtime.app import periodic_catchup


class FlakyCollector:
    def __init__(self, stop: asyncio.Event) -> None:
        self.stop = stop
        self.calls = 0

    async def catch_up(self) -> None:
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError("temporary Telegram timeout")
        self.stop.set()


async def test_periodic_catchup_continues_after_temporary_failure() -> None:
    stop = asyncio.Event()
    collector = FlakyCollector(stop)

    await periodic_catchup(collector, stop, interval_seconds=0.001)

    assert collector.calls == 2


async def test_task_failure_logs_task_name_and_exception(caplog) -> None:
    async def fail() -> None:
        raise RuntimeError("listener boom")

    stop = asyncio.Event()
    task = asyncio.create_task(fail(), name="collector")
    await asyncio.sleep(0)

    with caplog.at_level(logging.ERROR, logger="emoda.runtime.app"):
        runtime_app.request_stop_on_task_exit(task, stop)

    assert stop.is_set()
    assert "collector" in caplog.text
    assert "listener boom" in caplog.text


class ReviewBot:
    def __init__(self) -> None:
        self.photos: list[tuple[int, object, dict[str, object]]] = []
        self.messages: list[tuple[int, str, dict[str, object]]] = []

    async def send_photo(self, chat_id: int, photo: object, **kwargs: object) -> None:
        self.photos.append((chat_id, photo, kwargs))

    async def send_message(self, chat_id: int, text: str, **kwargs: object) -> None:
        self.messages.append((chat_id, text, kwargs))


class ReviewMediaClient:
    def __init__(self) -> None:
        self.calls: list[tuple[int, object, dict[str, object]]] = []

    async def send_file(self, chat_id: int, file: object, **kwargs: object) -> object:
        self.calls.append((chat_id, file, kwargs))
        return type("Message", (), {"id": 70})()


async def test_review_empty_file_sends_actionable_text_without_upload(tmp_path):
    empty = tmp_path / "empty.jpg"
    empty.touch()
    bot = ReviewBot()
    product = SimpleNamespace(emoda_code="AA07984", final_text="review payload")
    markup = object()
    await runtime_app.deliver_review_notification(
        bot, 123, product, [(str(empty), "photo")], markup
    )
    assert bot.photos == []
    assert len(bot.messages) == 1
    assert bot.messages[0][2]["reply_markup"] is markup
    assert "медиа" in bot.messages[0][1].lower()


async def test_review_retry_resumes_after_successful_parts(review_database, monkeypatch):
    from aiogram.exceptions import TelegramRetryAfter
    from aiogram.methods import SendMessage
    bot = ReviewBot()
    calls = []
    failures = 1

    async def send(chat_id, text, **kwargs):
        nonlocal failures
        if text == "actions" and failures:
            failures -= 1
            raise TelegramRetryAfter(method=SendMessage(chat_id=123, text=text), message="definite rejection", retry_after=1)
        calls.append(text)

    async def deliver(bot, admin_id, product, media, markup, **kwargs):
        await bot.send_message(admin_id, "first album")
        await bot.send_message(admin_id, "actions", reply_markup=markup)

    bot.send_message = send
    monkeypatch.setattr(runtime_app, "deliver_review_notification", deliver)
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path("/tmp"))
    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    assert calls == ["first album", "actions"]
    async with review_database() as session:
        assert await session.scalar(select(Product.review_notified_at)) is not None


async def test_review_lost_response_never_resends_accepted_part(review_database, monkeypatch):
    from emoda.db.models import GlobalSetting

    accepted = []
    bot = ReviewBot()

    async def send(chat_id, text, **kwargs):
        accepted.append(text)
        raise TimeoutError("Telegram accepted the message but its response was lost")

    async def deliver(bot, admin_id, product, media, markup, **kwargs):
        await bot.send_message(admin_id, product.emoda_code)

    bot.send_message = send
    monkeypatch.setattr(runtime_app, "deliver_review_notification", deliver)
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path('/tmp'))
    for _ in range(50):
        await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    assert accepted == ['AA00001']
    async with review_database() as session:
        progress = await session.scalar(select(GlobalSetting))
        assert progress.value['intent']
        assert await session.scalar(select(Product.review_notified_at)) is None


async def test_review_twenty_uncertain_items_do_not_block_new_notification(review_database, monkeypatch):
    async with review_database() as session, session.begin():
        source_id = await session.scalar(select(Source.id))
        for i in range(2, 22):
            session.add(Product(source_id=source_id, emoda_code=f'AA{i:05}',
                status=ProductStatus.NEEDS_REVIEW, final_text='review'))
    accepted = []
    bot = ReviewBot()

    async def send(chat_id, text, **kwargs):
        accepted.append(text)
        if text != 'AA00021':
            raise TimeoutError('unknown delivery result')

    async def deliver(bot, admin_id, product, media, markup, **kwargs):
        await bot.send_message(admin_id, product.emoda_code)

    bot.send_message = send
    monkeypatch.setattr(runtime_app, 'deliver_review_notification', deliver)
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path('/tmp'))
    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    assert len(accepted) == 21 and len(set(accepted)) == 21
    async with review_database() as session:
        product = await session.scalar(select(Product).where(Product.emoda_code == 'AA00021'))
        assert product.review_notified_at is not None


async def test_review_unknown_bad_request_has_finite_retry_budget(review_database, monkeypatch):
    from aiogram.exceptions import TelegramBadRequest
    from aiogram.methods import SendMessage

    from emoda.db.models import GlobalSetting

    attempts = 0
    bot = ReviewBot()

    async def send(chat_id, text, **kwargs):
        nonlocal attempts
        attempts += 1
        raise TelegramBadRequest(method=SendMessage(chat_id=chat_id, text=text), message="can't parse entities")

    async def deliver(bot, admin_id, product, media, markup, **kwargs):
        await bot.send_message(admin_id, product.emoda_code)

    bot.send_message = send
    monkeypatch.setattr(runtime_app, 'deliver_review_notification', deliver)
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path('/tmp'))
    for _ in range(50):
        await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    assert attempts == 3
    async with review_database() as session:
        progress = await session.scalar(select(GlobalSetting))
        assert progress.value['halted'] and progress.value['failures'] == 3
        assert await session.scalar(select(Product.review_notified_at)) is None


async def test_review_failed_success_checkpoint_prevents_replay_after_restart(review_database, monkeypatch):
    from emoda.services.admin_delivery import ReviewDeliveryProgress, UncertainReviewDelivery

    accepted = []

    async def send(chat_id, text, **kwargs):
        accepted.append(text)
        return SimpleNamespace(message_id=77)

    progress = await ReviewDeliveryProgress.load(review_database, 'diagnostic-checkpoint')
    original_save = progress.save

    async def failing_checkpoint(**values):
        if 'steps' in values:
            raise ConnectionError('success checkpoint unavailable after Telegram acceptance')
        await original_save(**values)

    monkeypatch.setattr(progress, 'save', failing_checkpoint)
    client = SimpleNamespace(send_message=send)
    with pytest.raises(ConnectionError):
        await progress.wrap(client).send_message(123, 'accepted')
    restarted = await ReviewDeliveryProgress.load(review_database, 'diagnostic-checkpoint')
    assert restarted.state['intent']
    with pytest.raises(UncertainReviewDelivery):
        await restarted.wrap(client).send_message(123, 'accepted')
    assert accepted == ['accepted']


async def test_review_permanent_media_error_sends_card_once(review_database, monkeypatch):
    from aiogram.exceptions import TelegramBadRequest
    from aiogram.methods import SendPhoto

    bot = ReviewBot()
    attempts = 0

    async def deliver(*args, **kwargs):
        nonlocal attempts
        attempts += 1
        raise TelegramBadRequest(method=SendPhoto(chat_id=123, photo="bad"), message="IMAGE_PROCESS_FAILED")

    monkeypatch.setattr(runtime_app, "deliver_review_notification", deliver)
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path("/tmp"))
    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)
    assert attempts == 1
    assert len(bot.messages) == 1
    assert "отклонил медиа" in bot.messages[0][1]
    assert bot.messages[0][2]["reply_markup"] is not None


async def test_review_notification_with_one_photo_shows_the_product_on_its_card(tmp_path: Path) -> None:
    photo = tmp_path / "photo.jpg"
    photo.write_bytes(b"photo")
    bot = ReviewBot()
    product = SimpleNamespace(
        emoda_code="AA01422",
        final_text=(
            "<b>Код / Code:</b> AA01422\n"
            "<b>Цена за шт / Unit price:</b> $17\n"
            "<b>Размеры / Sizes:</b> S M L\n"
            "<b>Шт в серии / Pcs per lot:</b> 6"
        ),
    )
    markup = object()

    await runtime_app.deliver_review_notification(
        bot, 123, product, [(str(photo), "photo")], markup
    )

    assert len(bot.photos) == 1
    assert bot.photos[0][0] == 123
    assert bot.photos[0][2]["caption"].endswith(product.final_text)
    assert bot.photos[0][2]["parse_mode"] == "HTML"
    assert bot.photos[0][2]["reply_markup"] is markup
    assert bot.messages == []


async def test_review_missing_media_is_text_only(tmp_path: Path) -> None:
    present = tmp_path / "present.jpg"
    present.write_bytes(b"photo")
    missing = tmp_path / "missing.jpg"
    bot = ReviewBot()
    media_client = ReviewMediaClient()
    product = SimpleNamespace(emoda_code="AA03544", final_text="four-line payload")
    markup = object()

    await runtime_app.deliver_review_notification(
        bot,
        123,
        product,
        [(str(present), "photo"), (str(missing), "photo")],
        markup,
        media_client=media_client,
    )

    assert len(bot.messages) == 1
    assert "медиа" in bot.messages[0][1].lower()
    assert bot.messages[0][2]["reply_markup"] is markup
    assert bot.photos == []
    assert media_client.calls == []


async def test_review_notification_shows_large_video_through_mtproto(
    tmp_path, monkeypatch
) -> None:
    video = tmp_path / "large.mp4"
    video.write_bytes(b"large-video")
    monkeypatch.setattr(
        "emoda.integrations.publisher.BOT_API_VIDEO_LIMIT_BYTES", 1
    )
    bot = ReviewBot()
    media_client = ReviewMediaClient()
    product = SimpleNamespace(emoda_code="AA01601", final_text="four-line payload")
    markup = object()

    await runtime_app.deliver_review_notification(
        bot,
        123,
        product,
        [(str(video), "video")],
        markup,
        media_client=media_client,
    )

    assert media_client.calls[0][0:2] == (123, str(video))
    assert media_client.calls[0][2]["supports_streaming"] is True
    assert bot.photos == []
    assert len(bot.messages) == 1
    assert bot.messages[0][1].endswith(product.final_text)
    assert bot.messages[0][2]["reply_markup"] is markup


async def test_review_album_over_aggregate_limit_uses_mtproto(
    tmp_path, monkeypatch
) -> None:
    first = tmp_path / "first.mp4"
    second = tmp_path / "second.mp4"
    first.write_bytes(b"1234")
    second.write_bytes(b"5678")
    monkeypatch.setattr(
        "emoda.integrations.publisher.BOT_API_VIDEO_LIMIT_BYTES", 10
    )
    monkeypatch.setattr(
        "emoda.integrations.publisher.BOT_API_MULTIPART_LIMIT_BYTES", 7
    )
    bot = ReviewBot()
    media_client = ReviewMediaClient()
    product = SimpleNamespace(emoda_code="AA00453", final_text="four-line payload")

    await runtime_app.deliver_review_notification(
        bot,
        123,
        product,
        [(str(first), "video"), (str(second), "video")],
        object(),
        media_client=media_client,
    )

    assert media_client.calls[0][1] == [str(first), str(second)]
    assert len(bot.messages) == 1


@pytest.fixture
async def review_database():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        source = Source(name="Review source", telegram_channel_id=9876, enabled=True)
        session.add(source)
        await session.flush()
        session.add(
            Product(
                source_id=source.id,
                emoda_code="AA00001",
                status=ProductStatus.NEEDS_REVIEW,
                final_text="Four-line review payload",
                review_reasons=[],
            )
        )
    try:
        yield sessions
    finally:
        await engine.dispose()


class CountingSessions:
    def __init__(self, sessions):
        self.sessions = sessions
        self.active = 0

    def __call__(self):
        outer = self
        session = self.sessions()

        class Context:
            async def __aenter__(self):
                outer.active += 1
                return await session.__aenter__()

            async def __aexit__(self, exc_type, exc, tb):
                try:
                    return await session.__aexit__(exc_type, exc, tb)
                finally:
                    outer.active -= 1

        return Context()


async def test_review_send_releases_db_session_before_telegram_await(review_database) -> None:
    sessions = CountingSessions(review_database)
    bot = ReviewBot()

    async def send_message(chat_id: int, text: str, **kwargs: object) -> None:
        assert sessions.active == 0
        await asyncio.sleep(0)
        assert sessions.active == 0
        bot.messages.append((chat_id, text, kwargs))

    bot.send_message = send_message
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path("/tmp"))

    await runtime_app.notify_pending_reviews_once(sessions, settings, bot, None)

    assert len(bot.messages) == 1
    await runtime_app.notify_pending_reviews_once(sessions, settings, bot, None)
    assert len(bot.messages) == 1
    async with review_database() as session:
        notified = await session.scalar(select(Product.review_notified_at))
    assert notified is not None


async def test_review_failed_delivery_remains_pending(review_database) -> None:
    bot = ReviewBot()

    async def fail_send(*args: object, **kwargs: object) -> None:
        raise ConnectionError("Telegram unavailable")

    bot.send_message = fail_send
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path("/tmp"))

    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)

    async with review_database() as session:
        notified = await session.scalar(select(Product.review_notified_at))
    assert notified is None


async def test_review_changed_during_delivery_remains_pending(review_database) -> None:
    bot = ReviewBot()

    async def edit_during_send(chat_id: int, text: str, **kwargs: object) -> None:
        async with review_database() as session, session.begin():
            product = await session.scalar(select(Product))
            assert product is not None
            product.final_text = "Updated review payload"
        bot.messages.append((chat_id, text, kwargs))

    bot.send_message = edit_during_send
    settings = SimpleNamespace(bootstrap_admin_ids=[123], media_root=Path("/tmp"))

    await runtime_app.notify_pending_reviews_once(review_database, settings, bot, None)

    assert len(bot.messages) == 1
    async with review_database() as session:
        product = await session.scalar(select(Product))
    assert product is not None
    assert product.final_text == "Updated review payload"
    assert product.review_notified_at is None


async def test_review_db_timeout_does_not_stop_notification_loop(monkeypatch) -> None:
    stop = asyncio.Event()
    calls = 0

    async def notify_once(*args: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise SQLAlchemyTimeoutError("QueuePool connection timeout")
        stop.set()

    monkeypatch.setattr(runtime_app, "notify_pending_reviews_once", notify_once)
    await runtime_app.review_notification_loop(
        object(), object(), object(), None, stop, interval_seconds=0.001
    )

    assert calls == 2
    assert stop.is_set()

async def test_processing_continues_while_history_is_slow(tmp_path):
    stop = asyncio.Event()
    recovery = asyncio.Event()
    history_started = asyncio.Event()
    history_release = asyncio.Event()
    processed = asyncio.Event()
    class Collector:
        async def catch_up(self):
            history_started.set()
            await history_release.wait()
    class Processor:
        async def recover_jobs(self):
            pass
        async def process_next(self):
            processed.set()
            stop.set()
            return True
    task = asyncio.create_task(runtime_app.processing_worker(
        Processor(), Collector(), stop, recovery, tmp_path/'health', poll_seconds=.001))
    try:
        await asyncio.wait_for(processed.wait(), .2)
        assert recovery.is_set()
    finally:
        history_release.set()
        stop.set()
        await task

async def test_slow_publication_does_not_block_other_ready_products(tmp_path):
    stop=asyncio.Event();recovery=asyncio.Event();release=asyncio.Event();second=asyncio.Event()
    class Processor:
        calls=0
        async def recover_jobs(self):pass
        async def process_next(self):
            self.calls+=1
            if self.calls==1:await release.wait()
            else:second.set();stop.set()
            return True
    task=asyncio.create_task(runtime_app.processing_worker(
        Processor(),object(),stop,recovery,tmp_path/'health',poll_seconds=.001))
    try:await asyncio.wait_for(second.wait(),.2)
    finally:
        release.set();stop.set();await task
