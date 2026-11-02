import stat
from pathlib import Path

from emoda.integrations.session import secure_session_path


def test_telegram_session_files_and_directory_are_private(tmp_path: Path) -> None:
    session_directory = tmp_path / "session"
    session_directory.mkdir(mode=0o755)
    base = session_directory / "collector"
    session_file = base.with_suffix(".session")
    journal_file = base.with_suffix(".session-journal")
    session_file.write_bytes(b"credential")
    journal_file.write_bytes(b"journal")
    session_file.chmod(0o644)
    journal_file.chmod(0o644)

    secure_session_path(base)

    assert stat.S_IMODE(session_directory.stat().st_mode) == 0o700
    assert stat.S_IMODE(session_file.stat().st_mode) == 0o600
    assert stat.S_IMODE(journal_file.stat().st_mode) == 0o600


def test_compose_healthcheck_does_not_boot_full_application_cli() -> None:
    compose = Path("docker-compose.yml").read_text(encoding="utf-8")

    assert 'test: ["CMD", "emoda", "health"]' not in compose
    assert "$${HEALTH_FILE:-/tmp/emoda-healthy}" in compose
    assert "stat -c %Y" in compose
