from __future__ import annotations

import hashlib
import os
import stat
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class DedupReport:
    duplicate_groups: int = 0
    scanned_files: int = 0
    candidate_files: int = 0
    linked_files: int = 0
    already_linked_files: int = 0
    reclaimed_bytes: int = 0
    missing_files: int = 0
    invalid_files: int = 0
    unsafe_files: int = 0


def _safe_regular_file(root: Path, relative_value: str) -> Path | None:
    relative = Path(relative_value)
    if relative.is_absolute() or ".." in relative.parts or not relative.parts:
        return None
    cursor = root
    for part in relative.parts:
        cursor /= part
        if cursor.is_symlink():
            return None
    try:
        resolved = cursor.resolve(strict=True)
        metadata = resolved.lstat()
    except (FileNotFoundError, OSError):
        return cursor
    if root != resolved and root not in resolved.parents:
        return None
    if not stat.S_ISREG(metadata.st_mode):
        return None
    return resolved


def _verified_digest(path: Path) -> tuple[str, os.stat_result] | None:
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except (FileNotFoundError, OSError):
        return None
    try:
        before = os.fstat(descriptor)
        value = hashlib.sha256()
        while chunk := os.read(descriptor, 1024 * 1024):
            value.update(chunk)
        after = os.fstat(descriptor)
    finally:
        os.close(descriptor)
    identity_before = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    identity_after = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if identity_before != identity_after or not stat.S_ISREG(after.st_mode):
        return None
    return value.hexdigest(), after


def _same_identity(path: Path, expected: os.stat_result) -> bool:
    try:
        current = path.lstat()
    except OSError:
        return False
    return (
        current.st_dev,
        current.st_ino,
        current.st_size,
        current.st_mtime_ns,
    ) == (
        expected.st_dev,
        expected.st_ino,
        expected.st_size,
        expected.st_mtime_ns,
    )


def deduplicate_groups(
    media_root: Path,
    groups: Mapping[str, Sequence[str]],
    *,
    dry_run: bool = False,
) -> DedupReport:
    """Replace verified duplicate files with atomic hard links.

    Every logical path remains present. Both the database SHA-256 and the actual
    bytes are checked before replacement, and paths outside ``media_root`` or
    through symlinks are never touched. Re-running the operation is idempotent.
    """
    root = media_root.resolve(strict=True)
    report = DedupReport()
    for expected_digest, raw_paths in groups.items():
        relative_paths = list(dict.fromkeys(raw_paths))
        if len(relative_paths) < 2:
            continue
        report.duplicate_groups += 1
        verified: list[tuple[Path, os.stat_result]] = []
        for relative_value in relative_paths:
            path = _safe_regular_file(root, relative_value)
            if path is None:
                report.unsafe_files += 1
                continue
            if not path.exists():
                report.missing_files += 1
                continue
            checked = _verified_digest(path)
            report.scanned_files += 1
            if checked is None or checked[0] != expected_digest:
                report.invalid_files += 1
                continue
            verified.append((path, checked[1]))
        if len(verified) < 2:
            continue
        canonical, _ = max(verified, key=lambda item: item[1].st_nlink)
        for target, initial_target_stat in verified:
            if target == canonical:
                continue
            try:
                if os.path.samefile(canonical, target):
                    report.already_linked_files += 1
                    continue
            except OSError:
                report.invalid_files += 1
                continue
            report.candidate_files += 1
            if dry_run:
                report.reclaimed_bytes += initial_target_stat.st_size
                continue
            checked_target = _verified_digest(target)
            checked_canonical = _verified_digest(canonical)
            if (
                checked_target is None
                or checked_target[0] != expected_digest
                or checked_canonical is None
                or checked_canonical[0] != expected_digest
            ):
                report.invalid_files += 1
                continue
            temporary = target.with_name(f".{target.name}.dedup-{uuid.uuid4().hex}")
            try:
                os.link(canonical, temporary, follow_symlinks=False)
                linked = _verified_digest(temporary)
                if linked is None or linked[0] != expected_digest:
                    report.invalid_files += 1
                    continue
                if not _same_identity(target, checked_target[1]):
                    report.invalid_files += 1
                    continue
                os.replace(temporary, target)
                report.linked_files += 1
                report.reclaimed_bytes += checked_target[1].st_size
            finally:
                temporary.unlink(missing_ok=True)
    return report
