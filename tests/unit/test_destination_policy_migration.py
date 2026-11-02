from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic.script import ScriptDirectory


def load_destination_policy_migration():
    path = (
        Path(__file__).parents[2]
        / "alembic"
        / "versions"
        / "20260831_0013_destination_publication_policy.py"
    )
    spec = importlib.util.spec_from_file_location("destination_policy_0013", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_destination_policy_migration_is_single_head_and_has_safe_defaults() -> None:
    migration = load_destination_policy_migration()

    assert migration.revision == "20260831_0013"
    assert migration.down_revision == "20260829_0012"
    assert migration.DEFAULT_POLICY == "standard_product"
    script = ScriptDirectory(str(Path(__file__).parents[2] / "alembic"))
    assert script.get_heads() == ["20260906_0018"]


def test_source_isolation_migration_normalizes_policy_and_adds_composite_keys() -> None:
    path = (
        Path(__file__).parents[2]
        / "alembic"
        / "versions"
        / "20260901_0015_source_isolation_and_global_renderer.py"
    )
    contents = path.read_text(encoding="utf-8")

    assert 'down_revision: str | None = "20260831_0014"' in contents
    assert "SET publication_policy = 'standard_product'" in contents
    assert "'{gemini_enabled}', 'true'::jsonb" in contents
    assert "'{ocr_mode}'" in contents
    assert "jsonb_array_elements" in contents
    assert "fk_source_messages_album_source" in contents
    assert "fk_products_message_source" in contents
    assert "fk_products_album_source" in contents


def test_global_renderer_cleanup_removes_all_dormant_destination_text() -> None:
    path = (
        Path(__file__).parents[2]
        / "alembic"
        / "versions"
        / "20260901_0016_global_renderer_cleanup.py"
    )
    contents = path.read_text(encoding="utf-8")

    assert 'down_revision: str | None = "20260901_0015"' in contents
    assert "fixed_text = ''" in contents
    assert "template = NULL" in contents
    assert "signature_ru = ''" in contents
    assert "signature_en = ''" in contents


def test_obsolete_destination_presentation_schema_is_removed() -> None:
    path = (
        Path(__file__).parents[2]
        / "alembic"
        / "versions"
        / "20260901_0017_drop_destination_presentation.py"
    )
    contents = path.read_text(encoding="utf-8")

    assert 'down_revision: str | None = "20260901_0016"' in contents
    for column in ("signature_ru", "signature_en", "template", "publication_policy", "fixed_text"):
        assert f'op.drop_column("destinations", "{column}")' in contents
