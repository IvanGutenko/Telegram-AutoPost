from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic.script import ScriptDirectory


def load_edit_policy_migration():
    path = (
        Path(__file__).parents[2]
        / "alembic"
        / "versions"
        / "20260829_0012_edit_policy_auto.py"
    )
    spec = importlib.util.spec_from_file_location("edit_policy_auto_0012", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_edit_policy_auto_migration_extends_ledger_backfill_as_single_head() -> None:
    migration = load_edit_policy_migration()
    assert migration.revision == "20260829_0012"
    assert migration.down_revision == "20260827_0011"
    script = ScriptDirectory(str(Path(__file__).parents[2] / "alembic"))
    assert script.get_heads() == ["20260906_0018"]
