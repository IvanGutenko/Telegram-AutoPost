from __future__ import annotations

import os
from pathlib import Path


def secure_session_path(base_path: Path) -> None:
    base_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(base_path.parent, 0o700)
    session_file = (
        base_path if base_path.suffix == ".session" else base_path.with_suffix(".session")
    )
    journal_file = session_file.with_suffix(".session-journal")
    for path in (session_file, journal_file):
        if path.exists():
            os.chmod(path, 0o600)
