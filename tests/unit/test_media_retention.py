from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import (
    AdminTestBatch,
    AdminTestItem,
    AdminUser,
    Destination,
    MediaItem,
    Product,
    PublicationOutbox,
    Source,
    SourceMessage,
)
from emoda.domain.enums import EventOrigin, MessageState, OutboxStatus, ProductStatus
from emoda.services.media_retention import MediaRetentionService


async def make_database():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    return engine, async_sessionmaker(engine, expire_on_commit=False)


def old(now: datetime) -> datetime:
    return now - timedelta(days=31)


def message(source_id: int, telegram_id: int, when: datetime, **values: object) -> SourceMessage:
    return SourceMessage(
        source_id=source_id,
        telegram_message_id=telegram_id,
        telegram_date=when,
        state=values.pop("state", MessageState.IGNORED),
        event_origin=EventOrigin.LIVE,
        processed_at=values.pop("processed_at", when),
        created_at=when,
        updated_at=when,
        **values,
    )


def media(row: SourceMessage, path: str, when: datetime, **values: object) -> MediaItem:
    return MediaItem(
        source_message_id=row.id,
        telegram_message_id=row.telegram_message_id,
        position=0,
        storage_path=path,
        size_bytes=4,
        sha256="0" * 64,
        created_at=when,
        updated_at=when,
        **values,
    )


async def test_old_successfully_published_media_is_evicted_but_db_path_is_preserved(
    tmp_path: Path,
) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    path = tmp_path / "source-1/message-1/photo.jpg"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        destination = Destination(name="shop", telegram_channel_id=-2001, enabled=True)
        session.add_all([source, destination])
        await session.flush()
        row = message(source.id, 1, when, state=MessageState.PUBLISHED)
        session.add(row)
        await session.flush()
        item = media(row, str(path.relative_to(tmp_path)), when)
        product = Product(
            emoda_code="AA00001",
            source_id=source.id,
            source_message_id=row.id,
            destination_id=destination.id,
            status=ProductStatus.PUBLISHED,
            published_at=when,
            created_at=when,
            updated_at=when,
        )
        session.add_all([item, product])
        await session.flush()
        session.add(
            PublicationOutbox(
                product_id=product.id,
                destination_id=destination.id,
                payload={},
                payload_hash="a" * 64,
                status=OutboxStatus.PUBLISHED,
                published_at=when,
                created_at=when,
                updated_at=when,
            )
        )
        item_id = item.id

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=10
    ).cleanup_batch(now=now)

    assert report.source_deleted_files == 1
    assert report.logical_deleted_bytes == 4
    assert report.physical_reclaimed_bytes == 4
    assert not path.exists()
    async with sessions() as session:
        persisted = await session.get(MediaItem, item_id)
        assert persisted is not None
        assert persisted.storage_path == "source-1/message-1/photo.jpg"
        assert persisted.local_deleted_at is not None
        assert persisted.local_deleted_at.replace(tzinfo=UTC) == now
    await engine.dispose()


async def test_critical_and_recent_media_never_enter_the_cleanup_queue(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        destination = Destination(name="shop", telegram_channel_id=-2001, enabled=True)
        session.add_all([source, destination])
        await session.flush()

        review = message(source.id, 1, when, state=MessageState.REVIEW)
        pending = message(source.id, 2, when, state=MessageState.PUBLISHED)
        downloading = message(
            source.id, 3, when, state=MessageState.IGNORED, media_pending=True
        )
        recent = message(
            source.id,
            4,
            now - timedelta(days=2),
            state=MessageState.IGNORED,
        )
        session.add_all([review, pending, downloading, recent])
        await session.flush()
        rows = [review, pending, downloading, recent]
        paths = []
        for row in rows:
            path = tmp_path / f"source-1/message-{row.telegram_message_id}/photo.jpg"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"data")
            paths.append(path)
            item_when = when if row is not recent else now - timedelta(days=2)
            session.add(media(row, str(path.relative_to(tmp_path)), item_when))

        review_product = Product(
            emoda_code="AA00002",
            source_id=source.id,
            source_message_id=review.id,
            destination_id=destination.id,
            status=ProductStatus.NEEDS_REVIEW,
            created_at=when,
            updated_at=when,
        )
        pending_product = Product(
            emoda_code="AA00003",
            source_id=source.id,
            source_message_id=pending.id,
            destination_id=destination.id,
            status=ProductStatus.PUBLISHED,
            published_at=when,
            created_at=when,
            updated_at=when,
        )
        session.add_all([review_product, pending_product])
        await session.flush()
        session.add(
            PublicationOutbox(
                product_id=pending_product.id,
                destination_id=destination.id,
                payload={},
                payload_hash="b" * 64,
                status=OutboxStatus.FAILED,
                created_at=when,
                updated_at=when,
            )
        )

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=10
    ).cleanup_batch(now=now)

    assert report.source_deleted_files == 0
    assert all(path.exists() for path in paths)
    await engine.dispose()


async def test_source_queue_is_oldest_first_and_bounded(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        session.add(source)
        await session.flush()
        paths: list[Path] = []
        for index, age in enumerate((40, 39, 38), start=1):
            when = now - timedelta(days=age)
            row = message(source.id, index, when)
            session.add(row)
            await session.flush()
            path = tmp_path / f"source-1/message-{index}/photo.jpg"
            path.parent.mkdir(parents=True)
            path.write_bytes(str(index).encode())
            paths.append(path)
            session.add(media(row, str(path.relative_to(tmp_path)), when))

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=2
    ).cleanup_batch(now=now)

    assert report.source_deleted_files == 2
    assert [path.exists() for path in paths] == [False, False, True]
    await engine.dispose()


async def test_disk_pressure_evicts_oldest_safe_media_before_monthly_cutoff(
    tmp_path: Path,
) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        session.add(source)
        await session.flush()
        paths: list[Path] = []
        for index, age in enumerate((10, 9, 0), start=1):
            when = now - timedelta(days=age, hours=12 if age == 0 else 0)
            row = message(source.id, index, when)
            session.add(row)
            await session.flush()
            path = tmp_path / f"source-1/message-{index}/photo.jpg"
            path.parent.mkdir(parents=True)
            path.write_bytes(str(index).encode())
            paths.append(path)
            session.add(media(row, str(path.relative_to(tmp_path)), when))

    report = await MediaRetentionService(
        sessions,
        tmp_path,
        retention_days=30,
        batch_size=1,
        high_watermark_percent=80,
        low_watermark_percent=75,
        pressure_min_age_days=1,
        disk_used_percent=lambda: 85.0,
    ).cleanup_batch(now=now)

    assert report.pressure_cleanup_active is True
    assert report.disk_used_percent == 85.0
    assert report.source_deleted_files == 1
    assert [path.exists() for path in paths] == [False, True, True]
    await engine.dispose()


async def test_disk_pressure_cleanup_uses_hysteresis_and_stops_at_low_watermark(
    tmp_path: Path,
) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        session.add(source)
        await session.flush()
        paths: list[Path] = []
        for index, age in enumerate((10, 9, 8), start=1):
            when = now - timedelta(days=age)
            row = message(source.id, index, when)
            session.add(row)
            await session.flush()
            path = tmp_path / f"source-1/message-{index}/photo.jpg"
            path.parent.mkdir(parents=True)
            path.write_bytes(str(index).encode())
            paths.append(path)
            session.add(media(row, str(path.relative_to(tmp_path)), when))

    used_values = iter((85.0, 78.0, 74.0))
    retention = MediaRetentionService(
        sessions,
        tmp_path,
        retention_days=30,
        batch_size=1,
        high_watermark_percent=80,
        low_watermark_percent=75,
        pressure_min_age_days=1,
        disk_used_percent=lambda: next(used_values),
    )

    first = await retention.cleanup_batch(now=now)
    second = await retention.cleanup_batch(now=now)
    third = await retention.cleanup_batch(now=now)

    assert first.pressure_cleanup_active is True
    assert second.pressure_cleanup_active is True
    assert third.pressure_cleanup_active is False
    assert [path.exists() for path in paths] == [False, False, True]
    await engine.dispose()


async def test_disk_pressure_never_deletes_operationally_critical_media(
    tmp_path: Path,
) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = now - timedelta(days=10)
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        destination = Destination(name="shop", telegram_channel_id=-2001, enabled=True)
        session.add_all([source, destination])
        await session.flush()
        review = message(source.id, 1, when, state=MessageState.REVIEW)
        failed = message(source.id, 2, when, state=MessageState.PUBLISHED)
        safe = message(source.id, 3, when, state=MessageState.IGNORED)
        session.add_all([review, failed, safe])
        await session.flush()
        paths: list[Path] = []
        for row in (review, failed, safe):
            path = tmp_path / f"source-1/message-{row.telegram_message_id}/photo.jpg"
            path.parent.mkdir(parents=True)
            path.write_bytes(b"data")
            paths.append(path)
            session.add(media(row, str(path.relative_to(tmp_path)), when))
        failed_product = Product(
            emoda_code="AA00004",
            source_id=source.id,
            source_message_id=failed.id,
            destination_id=destination.id,
            status=ProductStatus.PUBLISHED,
            published_at=when,
            created_at=when,
            updated_at=when,
        )
        session.add(failed_product)
        await session.flush()
        session.add(
            PublicationOutbox(
                product_id=failed_product.id,
                destination_id=destination.id,
                payload={},
                payload_hash="c" * 64,
                status=OutboxStatus.FAILED,
                created_at=when,
                updated_at=when,
            )
        )

    report = await MediaRetentionService(
        sessions,
        tmp_path,
        retention_days=30,
        batch_size=10,
        high_watermark_percent=80,
        low_watermark_percent=75,
        pressure_min_age_days=1,
        disk_used_percent=lambda: 85.0,
    ).cleanup_batch(now=now)

    assert report.source_deleted_files == 1
    assert [path.exists() for path in paths] == [True, True, False]
    await engine.dispose()


async def test_hard_link_is_unlinked_logically_without_claiming_physical_reclaim(
    tmp_path: Path,
) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    retained = tmp_path / "retained.jpg"
    evicted = tmp_path / "source-1/message-1/photo.jpg"
    evicted.parent.mkdir(parents=True)
    retained.write_bytes(b"data")
    os.link(retained, evicted)
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        session.add(source)
        await session.flush()
        row = message(source.id, 1, when)
        session.add(row)
        await session.flush()
        session.add(media(row, str(evicted.relative_to(tmp_path)), when))

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=10
    ).cleanup_batch(now=now)

    assert report.source_deleted_files == 1
    assert report.logical_deleted_bytes == 4
    assert report.physical_reclaimed_bytes == 0
    assert retained.read_bytes() == b"data"
    await engine.dispose()


async def test_inactive_media_shared_with_live_media_keeps_the_file(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    stored_path = "source-1/message-1/photo.jpg"
    path = tmp_path / stored_path
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        session.add(source)
        await session.flush()
        obsolete = message(source.id, 1, when)
        live = message(source.id, 2, now, state=MessageState.REVIEW)
        session.add_all([obsolete, live])
        await session.flush()
        obsolete_media = media(obsolete, stored_path, when, active=False)
        live_media = media(live, stored_path, now)
        session.add_all([obsolete_media, live_media])
        await session.flush()
        obsolete_id, live_id = obsolete_media.id, live_media.id

    retention = MediaRetentionService(sessions, tmp_path, retention_days=30, batch_size=10)
    first = await retention.cleanup_batch(now=now)
    second = await retention.cleanup_batch(now=now + timedelta(minutes=1))

    assert first.source_deleted_files == second.source_deleted_files == 0
    assert path.read_bytes() == b"data"
    async with sessions() as session:
        obsolete_row = await session.get(MediaItem, obsolete_id)
        live_row = await session.get(MediaItem, live_id)
        assert obsolete_row is not None and obsolete_row.local_deleted_at is not None
        assert live_row is not None and live_row.local_deleted_at is None
    await engine.dispose()


async def test_last_eligible_media_reference_unlinks_shared_file(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    stored_path = "source-1/shared.jpg"
    path = tmp_path / stored_path
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        session.add(source)
        await session.flush()
        first_message = message(source.id, 1, when)
        second_message = message(source.id, 2, when)
        session.add_all([first_message, second_message])
        await session.flush()
        first_media = media(first_message, stored_path, when, active=False)
        second_media = media(second_message, stored_path, when, active=False)
        session.add_all([first_media, second_media])
        await session.flush()
        media_ids = (first_media.id, second_media.id)

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=10
    ).cleanup_batch(now=now)

    assert report.source_deleted_files == 1
    assert report.physical_reclaimed_bytes == 4
    assert not path.exists()
    async with sessions() as session:
        for media_id in media_ids:
            row = await session.get(MediaItem, media_id)
            assert row is not None and row.local_deleted_at is not None
    await engine.dispose()


async def test_source_media_shared_with_admin_test_waits_for_admin_cleanup(
    tmp_path: Path,
) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    stored_path = "source-1/message-1/photo.jpg"
    path = tmp_path / stored_path
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        admin = AdminUser(telegram_user_id=123, enabled=True)
        session.add_all([source, admin])
        await session.flush()
        row = message(source.id, 1, when)
        session.add(row)
        await session.flush()
        obsolete_media = media(row, stored_path, when, active=False)
        batch = AdminTestBatch(
            source_id=source.id,
            admin_user_id=admin.id,
            status="collecting",
            created_at=when,
            updated_at=when,
        )
        session.add_all([obsolete_media, batch])
        await session.flush()
        admin_item = AdminTestItem(
            batch_id=batch.id,
            telegram_chat_id=123,
            telegram_message_id=1,
            media_paths=[str(path)],
            created_at=when,
            updated_at=when,
        )
        session.add(admin_item)
        await session.flush()
        media_id, admin_item_id, batch_id = obsolete_media.id, admin_item.id, batch.id

    retention = MediaRetentionService(sessions, tmp_path, retention_days=30, batch_size=10)
    first = await retention.cleanup_batch(now=now)
    assert first.source_deleted_files == 0
    assert path.read_bytes() == b"data"
    async with sessions() as session, session.begin():
        media_row = await session.get(MediaItem, media_id)
        batch = await session.get(AdminTestBatch, batch_id)
        assert media_row is not None and media_row.local_deleted_at is not None
        assert batch is not None
        batch.status = "completed"
        batch.updated_at = when

    second = await retention.cleanup_batch(now=now + timedelta(minutes=1))
    assert second.admin_deleted_files == 1
    assert not path.exists()
    async with sessions() as session:
        item = await session.get(AdminTestItem, admin_item_id)
        assert item is not None and item.media_paths == []
    await engine.dispose()


async def test_admin_cleanup_preserves_media_still_used_by_source(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    stored_path = "source-1/message-1/photo.jpg"
    path = tmp_path / stored_path
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        admin = AdminUser(telegram_user_id=123, enabled=True)
        session.add_all([source, admin])
        await session.flush()
        row = message(source.id, 1, now, state=MessageState.REVIEW)
        session.add(row)
        await session.flush()
        live_media = media(row, stored_path, now)
        batch = AdminTestBatch(
            source_id=source.id,
            admin_user_id=admin.id,
            status="completed",
            created_at=when,
            updated_at=when,
        )
        session.add_all([live_media, batch])
        await session.flush()
        admin_item = AdminTestItem(
            batch_id=batch.id,
            telegram_chat_id=123,
            telegram_message_id=1,
            media_paths=[str(path)],
            created_at=when,
            updated_at=when,
        )
        session.add(admin_item)
        await session.flush()
        media_id, admin_item_id = live_media.id, admin_item.id

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=10
    ).cleanup_batch(now=now)

    assert report.admin_deleted_files == 0
    assert path.read_bytes() == b"data"
    async with sessions() as session:
        media_row = await session.get(MediaItem, media_id)
        admin_row = await session.get(AdminTestItem, admin_item_id)
        assert media_row is not None and media_row.local_deleted_at is None
        assert admin_row is not None and admin_row.media_paths == []
    await engine.dispose()


async def test_completed_admin_test_media_is_cleared_one_bounded_batch_at_a_time(
    tmp_path: Path,
) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    first = tmp_path / "admin-tests/1/first.jpg"
    second = tmp_path / "admin-tests/1/second.jpg"
    first.parent.mkdir(parents=True)
    first.write_bytes(b"first")
    second.write_bytes(b"second")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        admin = AdminUser(telegram_user_id=123, enabled=True)
        session.add_all([source, admin])
        await session.flush()
        batch = AdminTestBatch(
            source_id=source.id,
            admin_user_id=admin.id,
            status="completed",
            created_at=when,
            updated_at=when,
        )
        session.add(batch)
        await session.flush()
        item = AdminTestItem(
            batch_id=batch.id,
            telegram_chat_id=123,
            telegram_message_id=1,
            media_paths=[str(first), str(second)],
            created_at=when,
            updated_at=when,
        )
        session.add(item)
        await session.flush()
        item_id = item.id

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=1
    ).cleanup_batch(now=now)

    assert report.admin_deleted_files == 1
    assert not first.exists()
    assert second.exists()
    async with sessions() as session:
        persisted = await session.get(AdminTestItem, item_id)
        assert persisted is not None
        assert persisted.media_paths == [str(second)]
    await engine.dispose()


async def test_shared_admin_test_path_is_unlinked_after_last_reference(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    path = tmp_path / "admin-tests/1/shared.jpg"
    path.parent.mkdir(parents=True)
    path.write_bytes(b"data")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        admin = AdminUser(telegram_user_id=123, enabled=True)
        session.add_all([source, admin])
        await session.flush()
        batch = AdminTestBatch(
            source_id=source.id,
            admin_user_id=admin.id,
            status="completed",
            created_at=when,
            updated_at=when,
        )
        session.add(batch)
        await session.flush()
        items = [
            AdminTestItem(
                batch_id=batch.id,
                telegram_chat_id=123,
                telegram_message_id=index,
                media_paths=[str(path)],
                created_at=when,
                updated_at=when,
            )
            for index in (1, 2)
        ]
        session.add_all(items)
        await session.flush()
        item_ids = [item.id for item in items]

    retention = MediaRetentionService(sessions, tmp_path, retention_days=30, batch_size=1)
    first = await retention.cleanup_batch(now=now)
    assert first.admin_deleted_files == 0
    assert path.read_bytes() == b"data"
    second = await retention.cleanup_batch(now=now + timedelta(minutes=1))
    assert second.admin_deleted_files == 1
    assert not path.exists()
    async with sessions() as session:
        for item_id in item_ids:
            row = await session.get(AdminTestItem, item_id)
            assert row is not None and row.media_paths == []
    await engine.dispose()


async def test_collecting_and_recent_admin_tests_are_protected(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    paths = [tmp_path / "admin-tests/1/a.jpg", tmp_path / "admin-tests/2/b.jpg"]
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"data")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        admin = AdminUser(telegram_user_id=123, enabled=True)
        session.add_all([source, admin])
        await session.flush()
        collecting = AdminTestBatch(
            source_id=source.id,
            admin_user_id=admin.id,
            status="collecting",
            created_at=when,
            updated_at=when,
        )
        recent = AdminTestBatch(
            source_id=source.id,
            admin_user_id=admin.id,
            status="completed",
            created_at=when,
            updated_at=now - timedelta(days=2),
        )
        session.add_all([collecting, recent])
        await session.flush()
        session.add_all(
            [
                AdminTestItem(
                    batch_id=collecting.id,
                    telegram_chat_id=123,
                    telegram_message_id=1,
                    media_paths=[str(paths[0])],
                ),
                AdminTestItem(
                    batch_id=recent.id,
                    telegram_chat_id=123,
                    telegram_message_id=2,
                    media_paths=[str(paths[1])],
                ),
            ]
        )

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=10
    ).cleanup_batch(now=now)

    assert report.admin_deleted_files == 0
    assert all(path.exists() for path in paths)
    await engine.dispose()


async def test_unsafe_admin_path_is_never_deleted(tmp_path: Path) -> None:
    engine, sessions = await make_database()
    now = datetime(2026, 9, 10, tzinfo=UTC)
    when = old(now)
    outside = tmp_path.parent / f"{tmp_path.name}-outside.jpg"
    outside.write_bytes(b"keep")
    async with sessions() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=-1001, enabled=True)
        admin = AdminUser(telegram_user_id=123, enabled=True)
        session.add_all([source, admin])
        await session.flush()
        batch = AdminTestBatch(
            source_id=source.id,
            admin_user_id=admin.id,
            status="cancelled",
            created_at=when,
            updated_at=when,
        )
        session.add(batch)
        await session.flush()
        session.add(
            AdminTestItem(
                batch_id=batch.id,
                telegram_chat_id=123,
                telegram_message_id=1,
                media_paths=[str(outside)],
            )
        )

    report = await MediaRetentionService(
        sessions, tmp_path, retention_days=30, batch_size=10
    ).cleanup_batch(now=now)

    assert report.unsafe_files == 1
    assert outside.read_bytes() == b"keep"
    await engine.dispose()
