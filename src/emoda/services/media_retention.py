from __future__ import annotations

import hashlib
import logging
import shutil
import stat
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path

from sqlalchemy import and_, exists, func, or_, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import aliased

from emoda.db.models import (
    AdminTestBatch,
    AdminTestItem,
    AlbumGroup,
    MediaItem,
    Product,
    PublicationOutbox,
    SourceMessage,
)
from emoda.domain.enums import AlbumStatus, MessageState, OutboxStatus, ProductStatus

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class RetentionReport:
    source_deleted_files: int = 0
    admin_deleted_files: int = 0
    logical_deleted_bytes: int = 0
    physical_reclaimed_bytes: int = 0
    missing_files: int = 0
    unsafe_files: int = 0
    failed_files: int = 0
    pressure_cleanup_active: bool = False
    disk_used_percent: float | None = None


@dataclass(frozen=True, slots=True)
class _DeleteResult:
    deleted: bool
    missing: bool = False
    unsafe: bool = False
    failed: bool = False
    logical_bytes: int = 0
    physical_bytes: int = 0


def _bounded_unlink(root: Path, stored_path: str) -> _DeleteResult:
    """Unlink one regular file while rejecting every path outside the media root."""
    raw = Path(stored_path)
    candidate = raw if raw.is_absolute() else root / raw
    try:
        relative = candidate.relative_to(root)
    except ValueError:
        return _DeleteResult(False, unsafe=True)
    if not relative.parts or ".." in relative.parts:
        return _DeleteResult(False, unsafe=True)
    cursor = root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            return _DeleteResult(False, unsafe=True)
    try:
        resolved = candidate.resolve(strict=True)
        metadata = candidate.lstat()
    except FileNotFoundError:
        return _DeleteResult(True, missing=True)
    except OSError:
        return _DeleteResult(False, failed=True)
    if (root != resolved and root not in resolved.parents) or not stat.S_ISREG(metadata.st_mode):
        return _DeleteResult(False, unsafe=True)
    try:
        current = candidate.lstat()
        if (current.st_dev, current.st_ino) != (metadata.st_dev, metadata.st_ino):
            return _DeleteResult(False, failed=True)
        candidate.unlink()
    except FileNotFoundError:
        return _DeleteResult(True, missing=True)
    except OSError:
        return _DeleteResult(False, failed=True)
    parent = candidate.parent
    while parent != root:
        try:
            parent.rmdir()
        except OSError:
            break
        parent = parent.parent
    return _DeleteResult(
        True,
        logical_bytes=metadata.st_size,
        physical_bytes=metadata.st_size if metadata.st_nlink == 1 else 0,
    )


def _storage_key(root: Path, stored_path: str) -> str | None:
    raw = Path(stored_path)
    candidate = raw if raw.is_absolute() else root / raw
    try:
        relative = candidate.relative_to(root)
    except ValueError:
        return None
    if not relative.parts or ".." in relative.parts:
        return None
    return relative.as_posix()


class MediaRetentionService:
    """Drain small durable queues of local media that are no longer operationally critical."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        media_root: Path,
        *,
        retention_days: int = 30,
        batch_size: int = 25,
        high_watermark_percent: int = 80,
        low_watermark_percent: int = 75,
        pressure_min_age_days: int = 1,
        disk_used_percent: Callable[[], float] | None = None,
    ) -> None:
        self.sessions = sessions
        self.media_root = media_root.resolve()
        self.retention_days = retention_days
        self.batch_size = batch_size
        self.high_watermark_percent = high_watermark_percent
        self.low_watermark_percent = low_watermark_percent
        self.pressure_min_age_days = pressure_min_age_days
        self._pressure_cleanup_active = False
        self._disk_used_percent = disk_used_percent or self._filesystem_used_percent

    def _filesystem_used_percent(self) -> float:
        usage = shutil.disk_usage(self.media_root)
        if usage.total <= 0:
            return 0.0
        return usage.used * 100 / usage.total

    def _update_pressure_state(self, report: RetentionReport) -> None:
        try:
            used_percent = self._disk_used_percent()
        except OSError:
            logger.exception("could not read media filesystem usage")
            return
        report.disk_used_percent = used_percent
        if self._pressure_cleanup_active:
            if used_percent <= self.low_watermark_percent:
                self._pressure_cleanup_active = False
        elif used_percent >= self.high_watermark_percent:
            self._pressure_cleanup_active = True
        report.pressure_cleanup_active = self._pressure_cleanup_active

    @staticmethod
    def _record(result: _DeleteResult, report: RetentionReport, *, admin: bool) -> None:
        if result.unsafe:
            report.unsafe_files += 1
        elif result.failed:
            report.failed_files += 1
        elif result.deleted:
            if admin:
                report.admin_deleted_files += 1
            else:
                report.source_deleted_files += 1
            report.logical_deleted_bytes += result.logical_bytes
            report.physical_reclaimed_bytes += result.physical_bytes
            if result.missing:
                report.missing_files += 1

    async def _lock_paths(self, session: AsyncSession, keys: set[str]) -> None:
        if session.get_bind().dialect.name != "postgresql":
            return
        for key in sorted(keys):
            lock_id = int.from_bytes(
                hashlib.blake2b(key.encode(), digest_size=8).digest(), "big", signed=True
            )
            await session.execute(select(func.pg_advisory_xact_lock(lock_id)))

    async def _reference_counts(
        self,
        session: AsyncSession,
        keys: set[str],
        raw_paths: set[str],
    ) -> tuple[Counter[str], Counter[str]]:
        if not keys:
            return Counter(), Counter()
        aliases = raw_paths | keys | {str(self.media_root / key) for key in keys}
        media_paths = await session.scalars(
            select(MediaItem.storage_path).where(
                MediaItem.local_deleted_at.is_(None),
                MediaItem.storage_path.in_(aliases),
            )
        )
        media_counts = Counter(
            key
            for path in media_paths
            if (key := _storage_key(self.media_root, path)) in keys
        )
        admin_paths = await session.scalars(
            select(AdminTestItem.media_paths).where(
                func.json_array_length(AdminTestItem.media_paths) > 0
            )
        )
        admin_counts = Counter(
            key
            for paths in admin_paths
            for path in paths
            if (key := _storage_key(self.media_root, path)) in keys
        )
        return media_counts, admin_counts

    async def cleanup_batch(self, *, now: datetime | None = None) -> RetentionReport:
        current = now or datetime.now(UTC)
        report = RetentionReport()
        self._update_pressure_state(report)
        eligible_age_days = (
            self.pressure_min_age_days
            if report.pressure_cleanup_active
            else self.retention_days
        )
        cutoff = current - timedelta(days=eligible_age_days)
        await self._cleanup_source_media(cutoff, current, report)
        await self._cleanup_admin_media(cutoff, report)
        return report

    async def _cleanup_source_media(
        self, cutoff: datetime, now: datetime, report: RetentionReport
    ) -> None:
        linked_product = aliased(Product)
        product_link = or_(
            linked_product.source_message_id == SourceMessage.id,
            and_(
                SourceMessage.album_group_id.is_not(None),
                linked_product.album_group_id == SourceMessage.album_group_id,
                linked_product.source_id == SourceMessage.source_id,
            ),
        )
        any_linked_product = (
            exists(select(linked_product.id).where(product_link)).correlate(SourceMessage)
        )
        unsafe_outbox = (
            exists(
                select(PublicationOutbox.id).where(
                    PublicationOutbox.product_id == linked_product.id,
                    or_(
                        PublicationOutbox.status != OutboxStatus.PUBLISHED,
                        PublicationOutbox.updated_at >= cutoff,
                    ),
                )
            ).correlate(linked_product)
        )
        old_published_outbox = (
            exists(
                select(PublicationOutbox.id).where(
                    PublicationOutbox.product_id == linked_product.id,
                    PublicationOutbox.status == OutboxStatus.PUBLISHED,
                    PublicationOutbox.updated_at < cutoff,
                )
            ).correlate(linked_product)
        )
        safe_product = (
            exists(
                select(linked_product.id).where(
                    product_link,
                    linked_product.updated_at < cutoff,
                    or_(
                        linked_product.status == ProductStatus.SKIPPED,
                        and_(
                            linked_product.status == ProductStatus.PUBLISHED,
                            old_published_outbox,
                        ),
                    ),
                    ~unsafe_outbox,
                )
            ).correlate(SourceMessage)
        )
        safe_album = or_(
            SourceMessage.album_group_id.is_(None),
            exists(
                select(AlbumGroup.id).where(
                    AlbumGroup.id == SourceMessage.album_group_id,
                    AlbumGroup.status == AlbumStatus.PROCESSED,
                    AlbumGroup.updated_at < cutoff,
                )
            ).correlate(SourceMessage),
        )
        safe_unattached_message = and_(
            ~any_linked_product,
            SourceMessage.state.in_([MessageState.IGNORED, MessageState.DUPLICATE]),
            SourceMessage.processed_at.is_not(None),
            SourceMessage.processed_at < cutoff,
        )
        query = (
            select(MediaItem)
            .join(SourceMessage, SourceMessage.id == MediaItem.source_message_id)
            .where(
                MediaItem.local_deleted_at.is_(None),
                MediaItem.updated_at < cutoff,
                or_(
                    MediaItem.active.is_(False),
                    and_(
                        SourceMessage.media_pending.is_(False),
                        SourceMessage.updated_at < cutoff,
                        safe_album,
                        or_(safe_product, safe_unattached_message),
                    ),
                ),
            )
            .order_by(MediaItem.updated_at, MediaItem.id)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        async with self.sessions() as session, session.begin():
            items = list(await session.scalars(query))
            keys = {
                key
                for item in items
                if (key := _storage_key(self.media_root, item.storage_path)) is not None
            }
            await self._lock_paths(session, keys)
            media_counts, admin_counts = await self._reference_counts(
                session, keys, {item.storage_path for item in items}
            )
            for item in items:
                key = _storage_key(self.media_root, item.storage_path)
                if key is not None and (media_counts[key] > 1 or admin_counts[key] > 0):
                    item.local_deleted_at = now
                    media_counts[key] -= 1
                    continue
                result = _bounded_unlink(self.media_root, item.storage_path)
                self._record(result, report, admin=False)
                if result.deleted:
                    item.local_deleted_at = now
                    if key is not None:
                        media_counts[key] -= 1

    async def _cleanup_admin_media(
        self, cutoff: datetime, report: RetentionReport
    ) -> None:
        query = (
            select(AdminTestItem)
            .join(AdminTestBatch, AdminTestBatch.id == AdminTestItem.batch_id)
            .where(
                AdminTestBatch.status.in_(["completed", "cancelled"]),
                AdminTestBatch.updated_at < cutoff,
                func.json_array_length(AdminTestItem.media_paths) > 0,
            )
            .order_by(AdminTestBatch.updated_at, AdminTestItem.id)
            .limit(self.batch_size)
            .with_for_update(skip_locked=True)
        )
        remaining_budget = self.batch_size
        async with self.sessions() as session, session.begin():
            items = list(await session.scalars(query))
            raw_paths = {path for item in items for path in item.media_paths}
            keys = {
                key
                for path in raw_paths
                if (key := _storage_key(self.media_root, path)) is not None
            }
            await self._lock_paths(session, keys)
            media_counts, admin_counts = await self._reference_counts(session, keys, raw_paths)
            for item in items:
                retained: list[str] = []
                for stored_path in item.media_paths:
                    if remaining_budget <= 0:
                        retained.append(stored_path)
                        continue
                    key = _storage_key(self.media_root, stored_path)
                    if key is not None and (media_counts[key] > 0 or admin_counts[key] > 1):
                        admin_counts[key] -= 1
                        remaining_budget -= 1
                        continue
                    result = _bounded_unlink(self.media_root, stored_path)
                    self._record(result, report, admin=True)
                    if result.deleted:
                        remaining_budget -= 1
                        if key is not None:
                            admin_counts[key] -= 1
                    else:
                        retained.append(stored_path)
                if retained != item.media_paths:
                    item.media_paths = retained
                if remaining_budget <= 0:
                    break
