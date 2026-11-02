from __future__ import annotations

from pathlib import Path

from alembic.config import Config
from alembic.script import ScriptDirectory


def test_currency_alias_registry_precedes_current_head_and_seeds_defaults() -> None:
    root = Path(__file__).resolve().parents[2]
    migration = (
        root
        / "alembic"
        / "versions"
        / "20260831_0014_currency_alias_registry.py"
    ).read_text()
    config = Config(str(root / "alembic.ini"))
    config.set_main_option("script_location", str(root / "alembic"))

    assert ScriptDirectory.from_config(config).get_heads() == ["20260906_0018"]
    for alias in ('"$"', '"💲"', '"💵"', '"USD"', '"US$"', '"€"', '"EUR"'):
        assert alias in migration
    assert '"💰"' not in migration
