import ast
from pathlib import Path

RUNTIME = Path(__file__).parents[2] / "src" / "emoda"
KNOWN_FIXTURE_MARKERS = {
    "lastpoint",
    "mybee",
    "romaşka",
    "romaska",
    "mortopuk",
    "strong eagle",
    "lufian",
    "calvin klein",
    "prada",
    "aa00029",
}
BEHAVIOR_IDENTIFIERS = {"source_id", "source_name", "channel_id", "telegram_channel_id"}


def test_runtime_contains_no_known_supplier_or_fixture_markers() -> None:
    violations: list[str] = []
    for path in RUNTIME.rglob("*.py"):
        lowered = path.read_text(encoding="utf-8").casefold()
        for marker in KNOWN_FIXTURE_MARKERS:
            if marker in lowered:
                violations.append(f"{path.relative_to(RUNTIME)}: {marker}")
    assert violations == []


def test_runtime_has_no_behavior_branch_on_source_or_channel_identity() -> None:
    violations: list[str] = []
    for path in RUNTIME.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            condition: ast.AST | None = node.test if isinstance(node, ast.If) else None
            if condition is None:
                continue
            names = {
                child.id
                for child in ast.walk(condition)
                if isinstance(child, ast.Name)
            }
            attributes = {
                child.attr
                for child in ast.walk(condition)
                if isinstance(child, ast.Attribute)
            }
            identity_comparison = any(
                isinstance(child, ast.Compare)
                and any(
                    isinstance(comparator, ast.Constant) and comparator.value is not None
                    for comparator in child.comparators
                )
                for child in ast.walk(condition)
            )
            if (names | attributes) & BEHAVIOR_IDENTIFIERS and identity_comparison:
                violations.append(
                    f"{path.relative_to(RUNTIME)}:{node.lineno}: {ast.unparse(condition)}"
                )
    assert violations == []
