from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Sequence
from decimal import Decimal
from typing import Any, get_args

from pydantic import ValidationError
from sqlalchemy import String, cast, delete, exists, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from emoda.admin.context import current_admin_id
from emoda.db.base import utcnow
from emoda.db.models import (
    AuditLog,
    CatalogNode,
    CurrencyAlias,
    Destination,
    DestinationButtonBinding,
    GlobalSetting,
    MarkupRule,
    NavigationButton,
    Product,
    Source,
    SourceExample,
    SourceMessage,
    SourceProfile,
)
from emoda.domain.enums import (
    ButtonAction,
    CatalogNodeType,
    ProductStatus,
    PublicationMode,
)
from emoda.domain.schemas import (
    PRODUCT_FIELD_NAMES,
    CatalogNodeData,
    CategoryRule,
    CleaningRule,
    CollectionBoundaryRule,
    CollectionProfile,
    ComputedRule,
    ExtractionRule,
    FieldProcessingMode,
    FieldProcessingPolicy,
    MarkupRange,
    MultiPriceRule,
    NavigationButtonData,
    ProductFieldName,
    RepeatedPatternRule,
    RoutingRule,
    RuleKind,
    RuleTransform,
    SourceFallbackRule,
    SourceIgnoreRule,
    SourceProfileData,
)
from emoda.rules.currency import normalize_currency_alias_key
from emoda.services.price import PriceEngine

_ADMIN_LINE_BREAK = re.compile(r"\r\n|\r|\n")


def normalize_admin_input(text: str) -> str:
    """Unify real line breaks and leave every other character untouched.

    ``str.splitlines`` also breaks on ``\\x0b \\x0c \\x1c \\x1d \\x1e \\x85``,
    ``U+2028`` and ``U+2029``, so a regex character class containing those
    escapes used to be cut in half before it ever reached the compiler.
    """
    return _ADMIN_LINE_BREAK.sub("\n", text)


def parse_key_value_lines(text: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for number, line in enumerate(normalize_admin_input(text).split("\n"), start=1):
        if not line.strip():
            continue
        if "=" not in line:
            raise ValueError(f"line {number} must use key=value")
        key, value = (part.strip() for part in line.split("=", 1))
        if not key:
            raise ValueError(f"line {number} has an empty key")
        if key in values:
            raise ValueError(f"duplicate key: {key}")
        values[key] = value
    if not values:
        raise ValueError("at least one key=value setting is required")
    return values


def parse_setting_block(
    text: str,
    keys: Sequence[str],
    *,
    label: str,
    opaque_regex_keys: Sequence[str] = (),
) -> dict[str, str] | None:
    """Split admin ``key=value`` input at known setting names only.

    A regex pattern is an opaque value: it may contain spaces, ``=``, ``|``,
    backslashes and quantifiers. Splitting the text on separators instead of on
    the known setting names made the pattern define its own boundaries, so a
    fragment of the pattern ended up validated as ``field``. Here the text is
    cut only immediately before a known setting name, which means every other
    character reaches the value verbatim - regardless of whether Telegram
    delivered the settings on separate lines, collapsed into one line, in a
    different order or with spaces around ``=``.

    Returns ``None`` when the text contains no known setting at all, which lets
    the caller fall back to a legacy positional format.
    """
    normalized = normalize_admin_input(text)
    known = tuple(keys)
    alternatives = "|".join(re.escape(key) for key in sorted(known, key=len, reverse=True))
    candidates = list(re.finditer(rf"(?:(?<=^)|(?<=\s))({alternatives})[ \t]*=", normalized))
    matches: list[re.Match[str]] = []
    opaque = set(opaque_regex_keys)
    current: re.Match[str] | None = None
    for candidate in candidates:
        if current is not None and current.group(1) in opaque:
            fragment = normalized[current.end() : candidate.start()]
            if "\n" not in fragment and not _regex_fragment_is_top_level(fragment):
                # A setting-looking token inside ``(...)`` or ``[...]`` is part
                # of the regex.  In particular, ``(?= field=price)`` must never
                # be re-routed to field validation.
                continue
        matches.append(candidate)
        current = candidate
    for candidate in re.finditer(r"(?m)^[ \t]*([A-Za-z_][A-Za-z0-9_]*)[ \t]*=", normalized):
        if candidate.group(1) not in known:
            raise ValueError(f"{label}: неизвестная настройка {candidate.group(1)}")
    if not matches:
        return None
    prologue = normalized[: matches[0].start()].strip()
    if prologue:
        raise ValueError(f"{label}: уберите текст перед первой настройкой: {prologue}")
    values: dict[str, str] = {}
    for position, match in enumerate(matches):
        key = match.group(1)
        if key in values:
            raise ValueError(f"{label}: настройка {key} указана дважды")
        end = matches[position + 1].start() if position + 1 < len(matches) else len(normalized)
        value = normalized[match.end() : end].strip()
        if "\n" in value:
            raise ValueError(
                f"{label}: значение {key} занимает несколько строк; "
                "каждая строка должна начинаться с названия настройки"
            )
        values[key] = value
    return values


def _regex_fragment_is_top_level(value: str) -> bool:
    """Return whether the end of a regex fragment is outside groups/classes.

    The scanner only decides whether a same-line `` key=`` token can delimit
    the opaque regex value. It deliberately does not validate the regex; the
    production compiler remains the single validator after lossless parsing.
    """

    depth = 0
    in_class = False
    escaped = False
    for character in value:
        if escaped:
            escaped = False
            continue
        if character == "\\":
            escaped = True
            continue
        if in_class:
            if character == "]":
                in_class = False
            continue
        if character == "[":
            in_class = True
        elif character == "(":
            depth += 1
        elif character == ")" and depth:
            depth -= 1
    return depth == 0 and not in_class and not escaped


def parse_navigation_button_input(
    text: str, action: ButtonAction
) -> tuple[NavigationButtonData, list[int]]:
    parts = [part.strip() for part in text.split("|")]
    if action is ButtonAction.URL:
        if len(parts) != 5:
            raise ValueError("URL button format: text|https_url|row|order|destination IDs")
        label, url, row, order, destinations_value = parts
    else:
        if len(parts) != 4:
            raise ValueError("Catalog button format: text|row|order|destination IDs")
        label, row, order, destinations_value = parts
        url = None
    if not destinations_value:
        raise ValueError("at least one destination ID is required")
    destination_ids = [int(item.strip()) for item in destinations_value.split(",")]
    if any(item <= 0 for item in destination_ids):
        raise ValueError("destination IDs must be positive")
    if len(set(destination_ids)) != len(destination_ids):
        raise ValueError("duplicate destination ID")
    return (
        NavigationButtonData(
            text=label,
            action=action,
            url=url,
            row=int(row),
            order=int(order),
        ),
        destination_ids,
    )


def parse_catalog_node_input(text: str, node_type: CatalogNodeType) -> CatalogNodeData:
    parts = [part.strip() for part in text.split("|")]
    if node_type is CatalogNodeType.CATEGORY:
        if len(parts) != 3:
            raise ValueError("Category format: Russian name|English name|position")
        label_ru, label_en, position = parts
        url = None
    else:
        if len(parts) != 4:
            raise ValueError("Link format: Russian name|English name|https_url|position")
        label_ru, label_en, url, position = parts
    return CatalogNodeData(
        text_ru=label_ru,
        text_en=label_en,
        node_type=node_type,
        url=url,
        position=int(position),
    )


def _boolean(value: str) -> bool:
    normalized = value.casefold()
    if normalized in {"true", "yes", "1", "on", "да"}:
        return True
    if normalized in {"false", "no", "0", "off", "нет"}:
        return False
    raise ValueError(f"invalid boolean: {value}")


def _source_field_name(value: str) -> str:
    field = value.strip()
    if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", field):
        raise ValueError(
            "field: одно слово из строчных латинских букв, цифр и подчёркивания "
            f"(1-32 символа); получено: {field}"
        )
    return field


def parse_source_field_name(value: str) -> str:
    """Validate a product field name for the button-driven rule builder."""
    return _source_field_name(value)


RULE_KINDS: tuple[str, ...] = get_args(RuleKind)
RULE_TRANSFORMS: tuple[str, ...] = get_args(RuleTransform)


def _rule_kind(value: str) -> str:
    kind = value.strip()
    if kind not in RULE_KINDS:
        raise ValueError(f"kind: допустимые значения - {', '.join(RULE_KINDS)}; получено: {kind}")
    return kind


def _rule_transform(value: str) -> str:
    transform = value.strip()
    if transform not in RULE_TRANSFORMS:
        raise ValueError(
            f"transform: допустимые значения - {', '.join(RULE_TRANSFORMS)}; получено: {transform}"
        )
    return transform


def validate_regex(pattern: str, *, setting: str = "pattern") -> str:
    """Compile a pattern with the same engine the production parser uses."""
    try:
        re.compile(pattern)
    except re.error as exc:
        raise ValueError(f"Некорректный regex ({setting}): {exc}") from exc
    return pattern


def _integer(value: str, *, setting: str) -> int:
    try:
        return int(value.strip())
    except ValueError as exc:
        raise ValueError(f"{setting}: ожидается целое число; получено: {value.strip()}") from exc


def describe_validation_error(exc: ValidationError) -> str:
    """Render a pydantic error as one admin-readable line."""
    parts: list[str] = []
    for error in exc.errors():
        location = ".".join(str(item) for item in error.get("loc", ()))
        message = str(error.get("msg", "")).removeprefix("Value error, ")
        parts.append(f"{location}: {message}" if location else message)
    return "; ".join(parts) or str(exc)


def _value_map(value: str) -> dict[str, str]:
    result: dict[str, str] = {}
    for item in value.split(";"):
        source, separator, replacement = item.partition("=>")
        if not separator or not source.strip() or not replacement.strip():
            raise ValueError("value_map format: source=>result;source2=>result2")
        result[source.strip()] = replacement.strip()
    return result


def profile_item_token(item: Any) -> str:
    """Short deterministic identity for an item stored inside profile JSON.

    Index-only callbacks are unsafe after a deletion because the next item can
    shift into the same index. The token lets every mutating callback prove it
    still targets the exact rule the admin saw.
    """
    payload = item.model_dump(mode="json") if hasattr(item, "model_dump") else item
    encoded = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.blake2s(encoded.encode("utf-8"), digest_size=5).hexdigest()


def _require_item_token(item: Any, expected_token: str | None, message: str) -> None:
    if expected_token is not None and profile_item_token(item) != expected_token:
        raise LookupError(message)


def parse_settings(text: str, allowed: Sequence[str], *, label: str) -> dict[str, str]:
    """Tokenise ``key=value`` admin input for a known set of settings.

    Shared by every screen whose settings can hold a regex, so a pattern is
    never re-read as another setting.
    """
    values = parse_setting_block(
        text,
        allowed,
        label=label,
        opaque_regex_keys=tuple(key for key in allowed if "pattern" in key),
    )
    if values is None:
        raise ValueError(
            f"{label}: отправьте строки вида ключ=значение; "
            f"допустимые настройки - {', '.join(allowed)}"
        )
    return values


def parse_collection_boundary_input(text: str) -> CollectionBoundaryRule:
    allowed = (
        "name",
        "action",
        "match",
        "pattern",
        "keywords",
        "target",
        "ignore_case",
        "reason",
    )
    values = parse_settings(text, allowed, label="Граница сбора")
    missing = {"name", "action", "match"} - set(values)
    if missing:
        raise ValueError(f"missing collection boundary setting: {sorted(missing)}")
    payload: dict[str, Any] = {
        "name": values["name"],
        "action": values["action"],
        "match": values["match"],
    }
    for key in ("pattern", "target", "reason"):
        if key in values:
            payload[key] = values[key]
    if payload.get("pattern"):
        validate_regex(str(payload["pattern"]))
    if "keywords" in values:
        payload["keywords"] = [
            item.strip() for item in values["keywords"].split(",") if item.strip()
        ]
    if "ignore_case" in values:
        payload["ignore_case"] = _boolean(values["ignore_case"])
    return CollectionBoundaryRule.model_validate(payload)


def parse_repeated_pattern_rule_input(text: str) -> RepeatedPatternRule:
    allowed = (
        "name",
        "pattern",
        "fields",
        "aggregation",
        "separators",
        "priority",
        "ignore_case",
        "authoritative",
    )
    values = parse_settings(text, allowed, label="Повторяющийся шаблон")
    missing = {"name", "pattern", "fields"} - set(values)
    if missing:
        raise ValueError(f"missing repeated-pattern setting: {sorted(missing)}")
    validate_regex(values["pattern"])
    return RepeatedPatternRule.model_validate(
        {
            "name": values["name"],
            "pattern": values["pattern"],
            "fields": _value_map(values["fields"]),
            "aggregation": _value_map(values.get("aggregation", ""))
            if values.get("aggregation")
            else {},
            "separators": _value_map(values.get("separators", ""))
            if values.get("separators")
            else {},
            "priority": _integer(values.get("priority", "100"), setting="priority"),
            "ignore_case": _boolean(values.get("ignore_case", "true")),
            "authoritative": _boolean(values.get("authoritative", "false")),
        }
    )


def parse_cleaning_rule_input(text: str) -> CleaningRule:
    allowed = (
        "kind",
        "value",
        "replacement",
        "scope",
        "language",
        "ignore_case",
        "enabled",
    )
    values = parse_settings(text, allowed, label="Правило очистки")
    missing = {"kind", "value"} - set(values)
    if missing:
        raise ValueError(f"missing cleaning rule setting: {sorted(missing)}")
    if values["kind"].strip() == "regex":
        validate_regex(values["value"], setting="value")
    return CleaningRule.model_validate(
        {
            "kind": values["kind"],
            "value": values["value"],
            "replacement": values.get("replacement", ""),
            "scope": values.get("scope", "line"),
            "language": values.get("language", "both"),
            "ignore_case": _boolean(values.get("ignore_case", "true")),
            "enabled": _boolean(values.get("enabled", "true")),
        }
    )


EXTRACTION_RULE_SETTING_KEYS: tuple[str, ...] = (
    "field",
    "kind",
    "pattern",
    "aliases",
    "priority",
    "ignore_case",
    "transform",
    "token_pattern",
    "value_map",
    "authoritative",
    "enabled",
)

_EXTRACTION_RULE_EDIT_KEYS = EXTRACTION_RULE_SETTING_KEYS


def parse_extraction_rule_values(text: str) -> dict[str, str] | None:
    """Tokenise admin rule input; ``None`` means no known setting was found."""
    return parse_setting_block(
        text,
        EXTRACTION_RULE_SETTING_KEYS,
        label="Правило",
        opaque_regex_keys=("pattern", "token_pattern"),
    )


def _parse_extraction_rule_edit_values(text: str) -> dict[str, str]:
    values = parse_extraction_rule_values(text)
    if values is None:
        raise ValueError(
            "Правило: отправьте строки вида pattern=..., priority=..., "
            f"допустимые настройки - {', '.join(EXTRACTION_RULE_SETTING_KEYS)}"
        )
    return values


def apply_extraction_rule_settings(
    payload: dict[str, Any], values: dict[str, str]
) -> dict[str, Any]:
    """Stage 2: convert already tokenised settings into rule payload fields.

    ``field`` is handled by the caller; every remaining setting is validated on
    its own so one bad value can never be reported as another setting's problem.
    """
    result = dict(payload)
    if "kind" in values:
        result["kind"] = _rule_kind(values["kind"])
    if "pattern" in values:
        pattern = values["pattern"]
        if not pattern:
            raise ValueError("pattern: значение не должно быть пустым")
        result["pattern"] = pattern
    if "transform" in values:
        result["transform"] = _rule_transform(values["transform"])
    if "token_pattern" in values:
        # An empty value means "use the built-in series token pattern" so that
        # sending the whole editor screen back never invents an unusable regex.
        if values["token_pattern"]:
            result["token_pattern"] = values["token_pattern"]
        else:
            result.pop("token_pattern", None)
    if "aliases" in values:
        result["aliases"] = [item.strip() for item in values["aliases"].split(",") if item.strip()]
    if "priority" in values:
        result["priority"] = _integer(values["priority"], setting="priority")
    if "ignore_case" in values:
        result["ignore_case"] = _boolean(values["ignore_case"])
    if "value_map" in values:
        raw_map = values["value_map"]
        result["value_map"] = _value_map(raw_map) if raw_map else {}
    if "authoritative" in values:
        result["authoritative"] = _boolean(values["authoritative"])
    if "enabled" in values:
        result["enabled"] = _boolean(values["enabled"])
    if str(result.get("kind", "")) == "regex":
        validate_regex(str(result.get("pattern", "")))
    if result.get("token_pattern"):
        validate_regex(str(result["token_pattern"]), setting="token_pattern")
    return result


def build_extraction_rule(payload: dict[str, Any]) -> ExtractionRule:
    """Stage 3: model validation with an admin-readable error."""
    try:
        return ExtractionRule.model_validate(payload)
    except ValidationError as exc:
        raise ValueError(describe_validation_error(exc)) from exc


def _parse_legacy_extraction_rule(text: str) -> tuple[str, ExtractionRule]:
    """Historic ``field|kind|pattern`` form, kept for unambiguous input only.

    Reachable only when the text contains no known ``setting=`` at all and the
    middle part is a valid rule kind, so a regex full of ``|`` and ``=`` can
    never be routed here by accident.
    """
    collapsed = normalize_admin_input(text).strip()
    guidance = (
        "Правило: отправьте строки field=..., kind=..., pattern=... "
        "(или старый формат field|kind|pattern одной строкой)"
    )
    if "\n" in collapsed or collapsed.count("|") < 2:
        raise ValueError(guidance)
    field, kind, pattern = collapsed.split("|", 2)
    if kind.strip() not in RULE_KINDS:
        raise ValueError(guidance)
    payload = apply_extraction_rule_settings({}, {"kind": kind, "pattern": pattern.strip()})
    return _source_field_name(field), build_extraction_rule(payload)


def parse_extraction_rule_input(text: str) -> tuple[str, ExtractionRule]:
    """Stage 1: split admin input, then validate field, kind and regex apart."""
    values = parse_extraction_rule_values(text)
    if values is None:
        return _parse_legacy_extraction_rule(text)
    missing = [key for key in ("field", "kind", "pattern") if key not in values]
    if missing:
        raise ValueError(f"Правило: не хватает настроек - {', '.join(missing)}")
    field = _source_field_name(values["field"])
    return field, build_extraction_rule(apply_extraction_rule_settings({}, values))


def parse_extraction_rule_edit_input(
    text: str,
    *,
    current_field: str,
    current_rule: ExtractionRule,
) -> tuple[str, ExtractionRule]:
    """Apply an independent admin edit without resetting untouched settings."""
    values = parse_extraction_rule_values(text)
    if values is None:
        values = {"pattern": text}
    field = _source_field_name(values.get("field", current_field))
    payload = apply_extraction_rule_settings(current_rule.model_dump(mode="python"), values)
    return field, build_extraction_rule(payload)


def parse_computed_rule_input(text: str) -> ComputedRule:
    allowed = (
        "name",
        "target_field",
        "operation",
        "source_field",
        "input_source",
        "input_mode",
        "secondary_target_field",
        "pattern",
        "capture_group",
        "value_group",
        "separator",
        "series_separator_pattern",
        "range_pattern",
        "explicit_list_pattern",
        "explicit_separator_pattern",
        "compact_range_pattern",
        "range_start_group",
        "range_end_group",
        "explicit_item_group",
        "numeric_step",
        "priority",
        "ignore_case",
        "authoritative",
        "conflict_policy",
    )
    values = parse_settings(text, allowed, label="Вычисляемое правило")
    missing = {"name", "target_field", "operation"} - set(values)
    if missing:
        raise ValueError(f"missing computed-rule setting: {sorted(missing)}")
    for key in (
        "pattern",
        "series_separator_pattern",
        "range_pattern",
        "explicit_list_pattern",
        "explicit_separator_pattern",
        "compact_range_pattern",
    ):
        if values.get(key):
            validate_regex(values[key], setting=key)
    separator_aliases = {
        "space": " ",
        "comma": ",",
        "comma_space": ", ",
        "slash": "/",
        "slash_space": " / ",
    }
    payload: dict[str, Any] = {
        "name": values["name"],
        "target_field": values["target_field"],
        "operation": values["operation"],
        "input_source": values.get("input_source"),
        "input_mode": values.get("input_mode", "field_value"),
        "priority": _integer(values.get("priority", "100"), setting="priority"),
        "ignore_case": _boolean(values.get("ignore_case", "true")),
        "authoritative": _boolean(values.get("authoritative", "false")),
        "conflict_policy": values.get("conflict_policy", "ambiguous"),
    }
    for key in (
        "source_field",
        "secondary_target_field",
        "pattern",
        "capture_group",
        "value_group",
        "series_separator_pattern",
        "range_pattern",
        "explicit_list_pattern",
        "explicit_separator_pattern",
        "compact_range_pattern",
        "range_start_group",
        "range_end_group",
        "explicit_item_group",
        "numeric_step",
    ):
        if key in values:
            payload[key] = values[key]
    if "separator" in values:
        payload["separator"] = separator_aliases.get(values["separator"], values["separator"])
    return ComputedRule.model_validate(payload)


def parse_source_fallback_input(text: str) -> tuple[str, SourceFallbackRule | None]:
    allowed = ("field", "value", "authoritative", "when_all_missing", "delete")
    values = parse_settings(text, allowed, label="Значение по умолчанию")
    if "field" not in values:
        raise ValueError("Значение по умолчанию: не хватает настройки field")
    field = _source_field_name(values.pop("field"))
    delete_requested = _boolean(values.pop("delete", "false"))
    if delete_requested:
        if "value" in values:
            raise ValueError("delete fallback must not contain value")
        return field, None
    if "value" not in values:
        raise ValueError("fallback value is required")
    return field, SourceFallbackRule(
        value=values.pop("value"),
        authoritative=_boolean(values.pop("authoritative", "false")),
        when_all_missing=[
            item.strip() for item in values.pop("when_all_missing", "").split(",") if item.strip()
        ],
    )


def parse_source_ignore_rule_input(text: str) -> SourceIgnoreRule:
    allowed = (
        "name",
        "media_required",
        "description_missing_required",
        "found_fields",
        "missing_fields",
        "grouped_id",
        "media_types",
        "min_media",
        "max_media",
        "min_messages",
        "max_messages",
        "enabled",
    )
    values = parse_settings(text, allowed, label="Правило пропуска")
    missing = {"name"} - set(values)
    if missing:
        raise ValueError(f"missing ignore rule setting: {sorted(missing)}")
    return SourceIgnoreRule(
        name=values["name"],
        media_required=_boolean(values.get("media_required", "true")),
        description_missing_required=_boolean(values.get("description_missing_required", "true")),
        found_fields=[
            item.strip() for item in values.get("found_fields", "").split(",") if item.strip()
        ],
        missing_fields=[
            item.strip() for item in values.get("missing_fields", "").split(",") if item.strip()
        ],
        enabled=_boolean(values.get("enabled", "true")),
        **dict[str, Any](
            {
                key: (values[key].split(",") if key == "media_types" else values[key])
                for key in (
                    "grouped_id",
                    "media_types",
                    "min_media",
                    "max_media",
                    "min_messages",
                    "max_messages",
                )
                if key in values
            }
        ),
    )


def parse_multi_price_rule_input(text: str) -> MultiPriceRule:
    allowed = (
        "name",
        "pattern",
        "default_currency",
        "priority",
        "ignore_case",
        "enabled",
        "labels",
        "label_mode",
    )
    values = parse_settings(text, allowed, label="Мульти-цена")
    required = {"name", "pattern"}
    if values.get("label_mode", "mapped") == "mapped":
        required.add("labels")
    missing = required - set(values)
    if missing:
        raise ValueError(f"missing multi-price setting: {sorted(missing)}")
    validate_regex(values["pattern"])
    labels: dict[str, dict[str, str]] = {}
    for item in filter(None, values.get("labels", "").split(";")):
        parts = [part.strip() for part in item.split("=>")]
        if len(parts) != 3 or not all(parts):
            raise ValueError("labels must use supplier=>RU=>EN separated by semicolons")
        original, ru, en = parts
        if original in labels:
            raise ValueError(f"duplicate multi-price label: {original}")
        labels[original] = {"ru": ru, "en": en}
    return MultiPriceRule.model_validate(
        {
            "name": values["name"],
            "pattern": values["pattern"],
            "labels": labels,
            "label_mode": values.get("label_mode", "mapped"),
            "default_currency": values.get("default_currency") or None,
            "priority": _integer(values.get("priority", "100"), setting="priority"),
            "ignore_case": _boolean(values.get("ignore_case", "true")),
            "enabled": _boolean(values.get("enabled", "true")),
        }
    )


def parse_markup_rules_input(
    text: str,
) -> list[tuple[Decimal, Decimal | None, Decimal, str]]:
    parsed: list[tuple[Decimal, Decimal | None, Decimal, str]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        bounds, raw_markup = line.split(":", 1)
        minimum_text, maximum_text = (part.strip() for part in bounds.split("-", 1))
        maximum = (
            None if maximum_text.casefold() in {"inf", "infinity", "∞"} else Decimal(maximum_text)
        )
        raw_markup = raw_markup.strip()
        markup_type = "percent" if raw_markup.endswith("%") else "fixed"
        markup = Decimal(raw_markup.removesuffix("%").lstrip("+"))
        parsed.append((Decimal(minimum_text), maximum, markup, markup_type))
    if not parsed:
        raise ValueError("at least one markup range is required")
    return parsed


def parse_routing_pair_input(text: str) -> tuple[CategoryRule, RoutingRule]:
    """Parse every configurable category/routing field from Telegram input."""
    values = parse_key_value_lines(text)
    allowed = {"category", "keywords", "pattern", "destination_id", "priority"}
    unknown = set(values) - allowed
    if unknown:
        raise ValueError(f"unknown routing settings: {sorted(unknown)}")
    missing = {"category", "destination_id"} - set(values)
    if missing:
        raise ValueError(f"missing routing settings: {sorted(missing)}")
    category_name = values["category"].strip()
    if not category_name:
        raise ValueError("category must not be empty")
    keywords = [item.strip() for item in values.get("keywords", "").split(",") if item.strip()]
    pattern = values.get("pattern", "").strip() or None
    priority = int(values.get("priority", "100"))
    destination_id = int(values["destination_id"])
    if destination_id <= 0:
        raise ValueError("destination_id must be positive")
    category = CategoryRule(
        category=category_name,
        keywords=keywords,
        pattern=pattern,
        priority=priority,
    )
    route = RoutingRule(
        category=category_name,
        keywords=keywords,
        pattern=pattern,
        destination_id=destination_id,
        priority=priority,
    )
    return category, route


def update_profile_from_lines(profile: SourceProfileData, text: str) -> SourceProfileData:
    repeated_keys = {
        "remove_string",
        "remove_line",
        "cleaning_pattern",
        "preserve_pattern",
        "advertising_pattern",
    }
    repeated: dict[str, list[str]] = {}
    unique_lines: list[str] = []
    for line in normalize_admin_input(text).split("\n"):
        key = line.split("=", 1)[0].strip() if "=" in line else ""
        if key in repeated_keys:
            _, value = line.split("=", 1)
            repeated.setdefault(key, []).append(value.strip())
        else:
            unique_lines.append(line)
    unique_text = "\n".join(unique_lines)
    raw = parse_key_value_lines(unique_text) if unique_text.strip() or not repeated else {}
    scalar_fields = {
        "publication_mode",
        "default_currency",
        "default_category",
        "alternative_publication_text",
        "gemini_instruction",
        "ocr_mode",
        "edit_policy",
    }
    boolean_fields = {
        "deduplicate_exact_products",
        "use_global_publication_mode",
        "gemini_enabled",
        "text_priority",
        "advertising_filter_enabled",
    }
    integer_fields = {"default_destination_id", "gemini_example_limit"}
    decimal_fields: set[str] = set()
    list_fields = {
        "remove_string": "remove_strings",
        "remove_line": "remove_lines",
        "cleaning_pattern": "cleaning_patterns",
        "preserve_pattern": "preserve_patterns",
        "advertising_pattern": "advertising_patterns",
    }
    updates: dict[str, Any] = {}
    collection_updates: dict[str, Any] = {}
    for key, value in raw.items():
        if key in scalar_fields:
            updates[key] = None if value.casefold() in {"", "none", "нет"} else value
        elif key in boolean_fields:
            updates[key] = _boolean(value)
        elif key in integer_fields:
            updates[key] = None if value.casefold() in {"", "none", "нет"} else int(value)
        elif key in decimal_fields:
            updates[key] = Decimal(value)
        elif key == "required_fields":
            updates[key] = [item.strip() for item in value.split(",") if item.strip()]
        elif key.startswith("collection_"):
            collection_key = key.removeprefix("collection_")
            aliases = {
                "merge_grouped_ids": "merge_neighbor_grouped_ids",
                "max_time_gap_seconds": "max_time_gap_seconds",
                "max_message_id_gap": "max_message_id_gap",
                "max_messages": "max_messages",
                "max_media": "max_media",
                "allow_multiple_text_blocks": "allow_multiple_text_blocks",
                "merge_text_blocks": "merge_text_blocks",
                "main_text": "main_text",
                "strategy": "strategy",
                "order": "order",
                "stop_on_next_start": "stop_on_next_start",
                "finalize_on_next_start": "finalize_on_next_start",
                "debounce_seconds": "debounce_seconds",
            }
            if collection_key not in aliases:
                raise ValueError(f"unknown collection setting: {key}")
            target = aliases[collection_key]
            if target in {
                "merge_neighbor_grouped_ids",
                "allow_multiple_text_blocks",
                "merge_text_blocks",
                "stop_on_next_start",
                "finalize_on_next_start",
            }:
                collection_updates[target] = _boolean(value)
            elif target in {"max_message_id_gap", "max_messages", "max_media"}:
                collection_updates[target] = int(value)
            elif target in {"max_time_gap_seconds", "debounce_seconds"}:
                collection_updates[target] = Decimal(value)
            else:
                collection_updates[target] = value
        elif key in list_fields:
            field_name = list_fields[key]
            updates[field_name] = [*getattr(profile, field_name), value] if value else []
        else:
            raise ValueError(f"unknown profile setting: {key}")
    for key, values in repeated.items():
        field_name = list_fields[key]
        existing = list(getattr(profile, field_name))
        for value in values:
            if not value:
                existing = []
            elif value not in existing:
                existing.append(value)
        updates[field_name] = existing
    payload = profile.model_dump(mode="python")
    if collection_updates:
        current_collection = profile.effective_collection().model_dump(mode="python")
        current_collection.update(collection_updates)
        updates["collection"] = CollectionProfile.model_validate(current_collection)
        updates["schema_version"] = 2
    payload.update(updates)
    return SourceProfileData.model_validate(payload)


class SourceAlreadyExistsError(ValueError):
    def __init__(self, source: Source) -> None:
        super().__init__(f"source for channel {source.telegram_channel_id} already exists")
        self.source_id = source.id
        self.source_name = source.name
        self.archived = source.archived_at is not None


class AdminService:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    @staticmethod
    def _validated_currency_alias(alias: str, currency: str) -> tuple[str, str, str]:
        clean_alias = alias.strip()
        clean_currency = currency.strip().upper()
        if (
            not clean_alias
            or len(clean_alias) > 64
            or any(character in "\r\n\x00" for character in clean_alias)
        ):
            raise ValueError("alias must contain 1-64 visible characters")
        if not re.fullmatch(r"[A-Z]{3}", clean_currency):
            raise ValueError("currency must use exactly 3 uppercase Latin letters")
        return clean_alias, normalize_currency_alias_key(clean_alias), clean_currency

    async def currency_aliases(self) -> list[CurrencyAlias]:
        return list(
            await self.session.scalars(
                select(CurrencyAlias).order_by(
                    CurrencyAlias.currency,
                    CurrencyAlias.system.desc(),
                    CurrencyAlias.id,
                )
            )
        )

    async def create_currency_alias(
        self,
        *,
        alias: str,
        currency: str,
        enabled: bool,
        actor_id: int | None,
    ) -> CurrencyAlias:
        clean_alias, normalized, clean_currency = self._validated_currency_alias(alias, currency)
        duplicate = await self.session.scalar(
            select(CurrencyAlias.id).where(CurrencyAlias.normalized_alias == normalized)
        )
        if duplicate is not None:
            raise ValueError(f"currency alias already exists: {clean_alias}")
        item = CurrencyAlias(
            alias=clean_alias,
            normalized_alias=normalized,
            currency=clean_currency,
            enabled=enabled,
            system=False,
        )
        self.session.add(item)
        await self.session.flush()
        await self._audit(
            "currency_alias.create",
            "currency_alias",
            item.id,
            None,
            {"alias": clean_alias, "currency": clean_currency, "enabled": enabled},
            actor_id,
        )
        await self.session.flush()
        return item

    async def update_currency_alias(
        self,
        alias_id: int,
        *,
        alias: str,
        currency: str,
        enabled: bool,
        actor_id: int | None,
    ) -> CurrencyAlias:
        item = await self.session.get(CurrencyAlias, alias_id, with_for_update=True)
        if item is None:
            raise LookupError("currency alias not found")
        clean_alias, normalized, clean_currency = self._validated_currency_alias(alias, currency)
        duplicate = await self.session.scalar(
            select(CurrencyAlias.id).where(
                CurrencyAlias.normalized_alias == normalized,
                CurrencyAlias.id != alias_id,
            )
        )
        if duplicate is not None:
            raise ValueError(f"currency alias already exists: {clean_alias}")
        before = {
            "alias": item.alias,
            "currency": item.currency,
            "enabled": item.enabled,
        }
        item.alias = clean_alias
        item.normalized_alias = normalized
        item.currency = clean_currency
        item.enabled = enabled
        await self._audit(
            "currency_alias.update",
            "currency_alias",
            item.id,
            before,
            {"alias": clean_alias, "currency": clean_currency, "enabled": enabled},
            actor_id,
        )
        await self.session.flush()
        return item

    async def set_currency_alias_enabled(
        self, alias_id: int, enabled: bool, *, actor_id: int | None
    ) -> CurrencyAlias:
        item = await self.session.get(CurrencyAlias, alias_id, with_for_update=True)
        if item is None:
            raise LookupError("currency alias not found")
        before = {"enabled": item.enabled}
        item.enabled = enabled
        await self._audit(
            "currency_alias.toggle",
            "currency_alias",
            item.id,
            before,
            {"enabled": enabled},
            actor_id,
        )
        await self.session.flush()
        return item

    async def delete_currency_alias(self, alias_id: int, *, actor_id: int | None) -> None:
        item = await self.session.get(CurrencyAlias, alias_id, with_for_update=True)
        if item is None:
            raise LookupError("currency alias not found")
        if item.system:
            raise ValueError("system currency alias cannot be deleted; disable it instead")
        before = {
            "alias": item.alias,
            "currency": item.currency,
            "enabled": item.enabled,
        }
        await self._audit(
            "currency_alias.delete",
            "currency_alias",
            item.id,
            before,
            None,
            actor_id,
        )
        await self.session.delete(item)
        await self.session.flush()

    async def review_queue(self, *, limit: int = 30) -> list[Product]:
        """Return current REVIEW work newest-first so live incidents stay reachable."""
        return list(
            await self.session.scalars(
                select(Product)
                .where(
                    Product.status.in_(
                        [ProductStatus.NEEDS_REVIEW, ProductStatus.RECONCILIATION_REQUIRED]
                    )
                )
                .order_by(Product.created_at.desc(), Product.id.desc())
                .limit(limit)
            )
        )

    async def recent_source_outcomes(
        self, source_id: int, *, limit: int = 5
    ) -> list[dict[str, object]]:
        messages = list(
            await self.session.scalars(
                select(SourceMessage)
                .where(SourceMessage.source_id == source_id)
                .order_by(SourceMessage.telegram_message_id.desc())
                .limit(limit)
            )
        )
        outcomes: list[dict[str, object]] = []
        for message in messages:
            product_conditions = [Product.source_message_id == message.id]
            if message.album_group_id is not None:
                product_conditions.append(Product.album_group_id == message.album_group_id)
            product = await self.session.scalar(
                select(Product)
                .where(or_(*product_conditions))
                .order_by(Product.updated_at.desc())
                .limit(1)
            )
            outcome = (
                product.status.value
                if product is not None
                else str(message.raw_metadata.get("outcome", "received"))
            )
            reason = (
                await self.product_review_reason(product)
                if product is not None
                and product.status
                in {ProductStatus.NEEDS_REVIEW, ProductStatus.RECONCILIATION_REQUIRED}
                else product.processing_error
                if product is not None and product.processing_error
                else message.raw_metadata.get("outcome_reason")
            )
            outcomes.append(
                {
                    "telegram_message_id": message.telegram_message_id,
                    "outcome": outcome,
                    "reason": str(reason) if reason else None,
                    "emoda_code": product.emoda_code if product is not None else None,
                    "state": message.state.value,
                    "origin": message.event_origin.value,
                    "event_type": message.event_type,
                }
            )
        return outcomes

    async def product_review_reason(self, product: Product) -> str | None:
        from emoda.services.review import review_reason_details

        structured = product.review_reasons or []
        if structured:
            return "; ".join(review_reason_details(item) for item in structured)
        if product.processing_error:
            return product.processing_error
        raw_reasons = product.parsed_result.get("review_reasons", [])
        if isinstance(raw_reasons, list):
            reasons = [str(reason) for reason in raw_reasons if str(reason).strip()]
            if reasons:
                return "; ".join(reasons)
        message_conditions: list[ColumnElement[bool]] = []
        if product.source_message_id is not None:
            message_conditions.append(SourceMessage.id == product.source_message_id)
        if product.album_group_id is not None:
            message_conditions.append(SourceMessage.album_group_id == product.album_group_id)
        if not message_conditions:
            return self._missing_review_reason(product)
        messages = list(
            await self.session.scalars(
                select(SourceMessage)
                .where(or_(*message_conditions))
                .order_by(SourceMessage.telegram_message_id)
            )
        )
        for message in messages:
            reason = message.raw_metadata.get("outcome_reason")
            if reason:
                return str(reason)
        return self._missing_review_reason(product)

    @staticmethod
    def _missing_review_reason(product: Product) -> str | None:
        if product.status in {ProductStatus.NEEDS_REVIEW, ProductStatus.RECONCILIATION_REQUIRED}:
            return "stage=processing; field=не относится к полю; rule=legacy_reason_missing; причина не была сохранена в старой записи"
        return None

    async def _audit(
        self,
        action: str,
        entity_type: str,
        entity_id: object,
        before: dict[str, Any] | None,
        after: dict[str, Any] | None,
        actor_id: int | None,
    ) -> None:
        self.session.add(
            AuditLog(
                admin_user_id=actor_id if actor_id is not None else current_admin_id.get(),
                action=action,
                entity_type=entity_type,
                entity_id=str(entity_id),
                before=before,
                after=after,
                created_at=utcnow(),
            )
        )

    async def record_action(
        self,
        action: str,
        entity_type: str,
        entity_id: object,
        *,
        after: dict[str, Any] | None = None,
    ) -> None:
        await self._audit(action, entity_type, entity_id, None, after, None)
        await self.session.flush()

    async def find_source_by_channel(self, channel_id: int) -> Source | None:
        # Archived sources retain their channel identity, profile and history.
        return await self.session.scalar(
            select(Source).where(Source.telegram_channel_id == channel_id)
        )

    async def create_source(
        self,
        *,
        name: str,
        channel_id: int,
        telegram_ref: str | None,
        destination_id: int | None,
        actor_id: int | None,
        last_seen_message_id: int | None = None,
    ) -> Source:
        existing = await self.find_source_by_channel(channel_id)
        if existing is not None:
            raise SourceAlreadyExistsError(existing)
        profile = SourceProfileData(
            default_destination_id=destination_id,
            collection=CollectionProfile(),
        )
        source = Source(
            name=name.strip(),
            telegram_channel_id=channel_id,
            telegram_ref=telegram_ref,
            enabled=False,
            catchup_anchor_message_id=last_seen_message_id,
            last_seen_message_id=last_seen_message_id,
        )
        source.profile = SourceProfile(
            schema_version=profile.schema_version,
            publication_mode=PublicationMode.REVIEW,
            config=profile.model_dump(mode="json"),
        )
        try:
            # Another admin can add the channel after the initial lookup. Keep
            # a uniqueness conflict from aborting the caller's transaction.
            async with self.session.begin_nested():
                self.session.add(source)
                await self.session.flush()
        except IntegrityError:
            existing = await self.find_source_by_channel(channel_id)
            if existing is None:
                raise
            raise SourceAlreadyExistsError(existing) from None
        await self._audit(
            "source.create",
            "source",
            source.id,
            None,
            {"name": source.name, "channel_id": channel_id},
            actor_id,
        )
        await self.session.flush()
        return source

    async def update_profile(
        self, source_id: int, config: dict[str, Any], *, actor_id: int | None
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        validated = SourceProfileData.model_validate(config)
        destination_ids = {
            destination_id
            for destination_id in [
                validated.default_destination_id,
                *(rule.destination_id for rule in validated.routing_rules),
            ]
            if destination_id is not None
        }
        if destination_ids:
            available = set(
                await self.session.scalars(
                    select(Destination.id).where(
                        Destination.id.in_(destination_ids), Destination.enabled.is_(True)
                    )
                )
            )
            missing = destination_ids - available
            if missing:
                raise ValueError(f"unknown destination IDs: {sorted(missing)}")
        before = dict(source.profile.config)
        source.profile.config = validated.model_dump(mode="json")
        source.profile.schema_version = validated.schema_version
        source.profile.publication_mode = validated.publication_mode
        source.profile.version += 1
        await self._audit(
            "source.profile.update", "source", source_id, before, source.profile.config, actor_id
        )
        await self.session.flush()
        return validated

    async def add_source_rule(
        self,
        source_id: int,
        field: str,
        rule: ExtractionRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        field = _source_field_name(field)
        profile = SourceProfileData.model_validate(source.profile.config)
        rules = {name: list(items) for name, items in profile.extraction_rules.items()}
        rules.setdefault(field, []).append(rule)
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"extraction_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def set_field_processing_mode(
        self,
        source_id: int,
        field: str,
        mode: FieldProcessingMode,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        field = _source_field_name(field)
        if field not in PRODUCT_FIELD_NAMES:
            raise ValueError(f"field processing is not supported for: {field}")
        typed_field: ProductFieldName = field
        if mode not in {"normal", "passthrough"}:
            raise ValueError(f"unknown field processing mode: {mode}")
        profile = SourceProfileData.model_validate(source.profile.config)
        policies = dict(profile.field_policies)
        if mode == "normal":
            policies.pop(typed_field, None)
        else:
            policies[typed_field] = FieldProcessingPolicy(processing_mode=mode)
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"field_policies": policies}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def add_computed_rule(
        self,
        source_id: int,
        rule: ComputedRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if any(item.name == rule.name for item in profile.computed_rules):
            raise ValueError(f"computed rule already exists: {rule.name}")
        return await self.update_profile(
            source_id,
            profile.model_copy(
                update={"computed_rules": [*profile.computed_rules, rule]}
            ).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def add_repeated_pattern_rule(
        self,
        source_id: int,
        rule: RepeatedPatternRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if any(item.name == rule.name for item in profile.repeated_pattern_rules):
            raise ValueError(f"repeated-pattern rule already exists: {rule.name}")
        return await self.update_profile(
            source_id,
            profile.model_copy(
                update={"repeated_pattern_rules": [*profile.repeated_pattern_rules, rule]}
            ).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def add_collection_boundary(
        self,
        source_id: int,
        rule: CollectionBoundaryRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        collection = profile.effective_collection()
        if any(item.name == rule.name for item in collection.boundary_rules):
            raise ValueError(f"collection boundary already exists: {rule.name}")
        collection = collection.model_copy(
            update={"boundary_rules": [*collection.boundary_rules, rule]}
        )
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"schema_version": 2, "collection": collection}).model_dump(
                mode="json"
            ),
            actor_id=actor_id,
        )

    async def add_cleaning_rule(
        self,
        source_id: int,
        rule: CleaningRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        return await self.update_profile(
            source_id,
            profile.model_copy(
                update={"cleaning_rules": [*profile.cleaning_rules, rule]}
            ).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_repeated_pattern_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.repeated_pattern_rules):
            raise LookupError("repeated-pattern rule not found")
        rules = list(profile.repeated_pattern_rules)
        _require_item_token(rules[index], expected_token, "repeated-pattern rule not found")
        del rules[index]
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"repeated_pattern_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_collection_boundary(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        collection = profile.effective_collection()
        if index < 0 or index >= len(collection.boundary_rules):
            raise LookupError("collection boundary not found")
        rules = list(collection.boundary_rules)
        _require_item_token(rules[index], expected_token, "collection boundary not found")
        del rules[index]
        collection = collection.model_copy(update={"boundary_rules": rules})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"schema_version": 2, "collection": collection}).model_dump(
                mode="json"
            ),
            actor_id=actor_id,
        )

    async def set_collection_settings(
        self,
        source_id: int,
        updates: dict[str, Any],
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        """Apply explicit collection-profile changes from the «Сбор товара» screen."""
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        collection = profile.effective_collection().model_copy(update=updates)
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"schema_version": 2, "collection": collection}).model_dump(
                mode="json"
            ),
            actor_id=actor_id,
        )

    async def replace_collection_boundary(
        self,
        source_id: int,
        index: int,
        rule: CollectionBoundaryRule,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        collection = profile.effective_collection()
        if index < 0 or index >= len(collection.boundary_rules):
            raise LookupError("collection boundary not found")
        rules = list(collection.boundary_rules)
        _require_item_token(rules[index], expected_token, "collection boundary not found")
        renamed = [item for item in rules if item.name == rule.name and item is not rules[index]]
        if renamed:
            raise ValueError(f"collection boundary already exists: {rule.name}")
        rules[index] = rule
        collection = collection.model_copy(update={"boundary_rules": rules})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"schema_version": 2, "collection": collection}).model_dump(
                mode="json"
            ),
            actor_id=actor_id,
        )

    async def toggle_collection_boundary(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
        enabled: bool | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        collection = profile.effective_collection()
        if index < 0 or index >= len(collection.boundary_rules):
            raise LookupError("collection boundary not found")
        rules = list(collection.boundary_rules)
        _require_item_token(rules[index], expected_token, "collection boundary not found")
        target = not rules[index].enabled if enabled is None else enabled
        rules[index] = rules[index].model_copy(update={"enabled": target})
        collection = collection.model_copy(update={"boundary_rules": rules})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"schema_version": 2, "collection": collection}).model_dump(
                mode="json"
            ),
            actor_id=actor_id,
        )

    async def toggle_source_rule(
        self,
        source_id: int,
        field: str,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
        enabled: bool | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        field = _source_field_name(field)
        profile = SourceProfileData.model_validate(source.profile.config)
        rules = list(profile.extraction_rules.get(field, []))
        if index < 0 or index >= len(rules):
            raise LookupError("source rule not found")
        _require_item_token(rules[index], expected_token, "source rule not found")
        target = not rules[index].enabled if enabled is None else enabled
        rules[index] = rules[index].model_copy(update={"enabled": target})
        engine_rules = {name: list(items) for name, items in profile.extraction_rules.items()}
        engine_rules[field] = rules
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"extraction_rules": engine_rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def toggle_computed_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
        enabled: bool | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.computed_rules):
            raise LookupError("computed rule not found")
        rules = list(profile.computed_rules)
        _require_item_token(rules[index], expected_token, "computed rule not found")
        target = not rules[index].enabled if enabled is None else enabled
        rules[index] = rules[index].model_copy(update={"enabled": target})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"computed_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def toggle_repeated_pattern_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
        enabled: bool | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.repeated_pattern_rules):
            raise LookupError("repeated-pattern rule not found")
        rules = list(profile.repeated_pattern_rules)
        _require_item_token(rules[index], expected_token, "repeated-pattern rule not found")
        target = not rules[index].enabled if enabled is None else enabled
        rules[index] = rules[index].model_copy(update={"enabled": target})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"repeated_pattern_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def toggle_multi_price_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
        enabled: bool | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.multi_price_rules):
            raise LookupError("multi-price rule not found")
        rules = list(profile.multi_price_rules)
        _require_item_token(rules[index], expected_token, "multi-price rule not found")
        target = not rules[index].enabled if enabled is None else enabled
        rules[index] = rules[index].model_copy(update={"enabled": target})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"multi_price_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def toggle_cleaning_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
        enabled: bool | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.cleaning_rules):
            raise LookupError("cleaning rule not found")
        rules = list(profile.cleaning_rules)
        _require_item_token(rules[index], expected_token, "cleaning rule not found")
        target = not rules[index].enabled if enabled is None else enabled
        rules[index] = rules[index].model_copy(update={"enabled": target})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"cleaning_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def replace_cleaning_rule(
        self,
        source_id: int,
        index: int,
        rule: CleaningRule,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        """Replace the exact cleaning rule shown to the admin, in place."""
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.cleaning_rules):
            raise LookupError("cleaning rule not found")
        rules = list(profile.cleaning_rules)
        _require_item_token(rules[index], expected_token, "cleaning rule not found")
        rules[index] = rule
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"cleaning_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_cleaning_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.cleaning_rules):
            raise LookupError("cleaning rule not found")
        rules = list(profile.cleaning_rules)
        _require_item_token(rules[index], expected_token, "cleaning rule not found")
        del rules[index]
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"cleaning_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_source_rule(
        self,
        source_id: int,
        field: str,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        field = _source_field_name(field)
        profile = SourceProfileData.model_validate(source.profile.config)
        rules = {name: list(items) for name, items in profile.extraction_rules.items()}
        if field not in rules or index < 0 or index >= len(rules[field]):
            raise LookupError("source extraction rule not found")
        _require_item_token(rules[field][index], expected_token, "source extraction rule not found")
        del rules[field][index]
        if not rules[field]:
            del rules[field]
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"extraction_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_computed_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.computed_rules):
            raise LookupError("computed rule not found")
        rules = list(profile.computed_rules)
        _require_item_token(rules[index], expected_token, "computed rule not found")
        del rules[index]
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"computed_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def replace_computed_rule(
        self,
        source_id: int,
        index: int,
        rule: ComputedRule,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.computed_rules):
            raise LookupError("computed rule not found")
        rules = list(profile.computed_rules)
        _require_item_token(rules[index], expected_token, "computed rule not found")
        if any(item.name == rule.name for offset, item in enumerate(rules) if offset != index):
            raise ValueError(f"computed rule already exists: {rule.name}")
        rules[index] = rule.model_copy(update={"enabled": rules[index].enabled})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"computed_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def replace_source_rule(
        self,
        source_id: int,
        field: str,
        index: int,
        new_field: str,
        rule: ExtractionRule,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        """Edit an existing extraction rule; a different field moves the rule."""
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        field = _source_field_name(field)
        new_field = _source_field_name(new_field)
        profile = SourceProfileData.model_validate(source.profile.config)
        rules = {name: list(items) for name, items in profile.extraction_rules.items()}
        if field not in rules or index < 0 or index >= len(rules[field]):
            raise LookupError("source extraction rule not found")
        _require_item_token(rules[field][index], expected_token, "source extraction rule not found")
        if new_field == field:
            # An edit keeps the rule where the admin sees it; only a field change moves it.
            rules[field][index] = rule
        else:
            del rules[field][index]
            if not rules[field]:
                del rules[field]
            rules.setdefault(new_field, []).append(rule)
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"extraction_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def set_source_fixed_field(
        self, source_id: int, field: str, value: str | None, *, actor_id: int | None,
    ) -> SourceProfileData:
        if field not in {"sizes", "quantity"}:
            raise ValueError("Фиксировать можно только sizes и quantity")
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None or source.archived_at is not None:
            raise LookupError("Источник не найден или архивирован")
        profile = SourceProfileData.model_validate(source.profile.config)
        fixed: dict[str, str] = {key: value for key, value in profile.fixed_fields.items()}
        if value is None:
            fixed.pop(field, None)
        else:
            fixed[field] = value
        config = {**profile.model_dump(mode="json"), "fixed_fields": fixed}
        return await self.update_profile(source_id, config, actor_id=actor_id)

    async def set_source_fallback(
        self,
        source_id: int,
        field: str,
        fallback: SourceFallbackRule | None,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        field = _source_field_name(field)
        profile = SourceProfileData.model_validate(source.profile.config)
        fallbacks = dict(profile.fallback_rules)
        if fallback is None:
            if field not in fallbacks:
                raise LookupError("source fallback not found")
            del fallbacks[field]
        else:
            fallbacks[field] = fallback
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"fallback_rules": fallbacks}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def add_source_ignore_rule(
        self,
        source_id: int,
        rule: SourceIgnoreRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if any(item.name == rule.name for item in profile.ignore_rules):
            raise ValueError(f"ignore rule already exists: {rule.name}")
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"ignore_rules": [*profile.ignore_rules, rule]}).model_dump(
                mode="json"
            ),
            actor_id=actor_id,
        )

    async def delete_source_ignore_rule(
        self,
        source_id: int,
        name: str,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        remaining = [item for item in profile.ignore_rules if item.name != name]
        if len(remaining) == len(profile.ignore_rules):
            raise LookupError("source ignore rule not found")
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"ignore_rules": remaining}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def toggle_source_ignore_rule(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
        enabled: bool | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.ignore_rules):
            raise LookupError("source ignore rule not found")
        rules = list(profile.ignore_rules)
        _require_item_token(rules[index], expected_token, "source ignore rule not found")
        target = not rules[index].enabled if enabled is None else enabled
        rules[index] = rules[index].model_copy(update={"enabled": target})
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"ignore_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_source_ignore_rule_at(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.ignore_rules):
            raise LookupError("source ignore rule not found")
        rules = list(profile.ignore_rules)
        _require_item_token(rules[index], expected_token, "source ignore rule not found")
        del rules[index]
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"ignore_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def add_multi_price_rule(
        self,
        source_id: int,
        rule: MultiPriceRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if any(item.name == rule.name for item in profile.multi_price_rules):
            raise ValueError(f"multi-price rule already exists: {rule.name}")
        updated = [*profile.multi_price_rules, rule]
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"multi_price_rules": updated}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def replace_multi_price_rule(
        self,
        source_id: int,
        index: int,
        rule: MultiPriceRule,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        """Replace one existing multi-price rule without changing its list identity."""
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.multi_price_rules):
            raise LookupError("multi-price rule not found")
        rules = list(profile.multi_price_rules)
        current = rules[index]
        _require_item_token(current, expected_token, "multi-price rule not found")
        if rule.name != current.name:
            raise ValueError("multi-price rule name is its identity and cannot be changed")
        rules[index] = rule
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"multi_price_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_multi_price_rule(
        self,
        source_id: int,
        name: str,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        remaining = [item for item in profile.multi_price_rules if item.name != name]
        if len(remaining) == len(profile.multi_price_rules):
            raise LookupError("multi-price rule not found")
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"multi_price_rules": remaining}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_multi_price_rule_at(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.multi_price_rules):
            raise LookupError("multi-price rule not found")
        rules = list(profile.multi_price_rules)
        _require_item_token(rules[index], expected_token, "multi-price rule not found")
        del rules[index]
        return await self.update_profile(
            source_id,
            profile.model_copy(update={"multi_price_rules": rules}).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def set_source_enabled(
        self, source_id: int, enabled: bool, *, actor_id: int | None
    ) -> None:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        before = source.enabled
        source.enabled = enabled
        await self._audit(
            "source.enable" if enabled else "source.disable",
            "source",
            source_id,
            {"enabled": before},
            {"enabled": enabled},
            actor_id,
        )
        await self.session.flush()

    async def update_source(
        self,
        source_id: int,
        *,
        name: str,
        channel_id: int,
        telegram_ref: str | None,
        actor_id: int | None,
        last_seen_message_id: int | None = None,
    ) -> Source:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        cleaned_name = name.strip()
        if not cleaned_name:
            raise ValueError("source name must not be empty")
        before = {
            "name": source.name,
            "telegram_channel_id": source.telegram_channel_id,
            "telegram_ref": source.telegram_ref,
        }
        source.name = cleaned_name
        source.telegram_channel_id = channel_id
        source.telegram_ref = telegram_ref
        if last_seen_message_id is not None:
            source.catchup_anchor_message_id = last_seen_message_id
            source.last_seen_message_id = last_seen_message_id
        await self._audit(
            "source.update",
            "source",
            source_id,
            before,
            {
                "name": source.name,
                "telegram_channel_id": channel_id,
                "telegram_ref": telegram_ref,
            },
            actor_id,
        )
        await self.session.flush()
        return source

    async def archive_source(self, source_id: int, *, actor_id: int | None) -> None:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        if source.archived_at is not None:
            raise LookupError("source is already archived")
        before = {"archived_at": None, "enabled": source.enabled}
        source.enabled = False
        source.archived_at = utcnow()
        await self._audit(
            "source.archive",
            "source",
            source_id,
            before,
            {"archived_at": source.archived_at.isoformat(), "enabled": False},
            actor_id,
        )
        await self.session.flush()

    async def restore_source(self, source_id: int, *, actor_id: int | None) -> Source:
        """Restore the same soft-archived source and leave it safely disabled."""
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        if source.archived_at is None:
            raise LookupError("source is not archived")
        before = {
            "archived_at": source.archived_at.isoformat(),
            "enabled": source.enabled,
        }
        source.archived_at = None
        source.enabled = False
        await self._audit(
            "source.restore",
            "source",
            source_id,
            before,
            {"archived_at": None, "enabled": False},
            actor_id,
        )
        await self.session.flush()
        return source

    async def add_example(
        self,
        source_id: int,
        original: str,
        expected: dict[str, Any],
        comment: str,
        *,
        actor_id: int | None,
    ) -> SourceExample:
        if await self.session.get(Source, source_id) is None:
            raise LookupError("source not found")
        example = SourceExample(
            source_id=source_id,
            original_input=original,
            expected_values=expected,
            admin_comment=comment,
            enabled=True,
        )
        self.session.add(example)
        await self.session.flush()
        await self._audit(
            "source.example.create",
            "source_example",
            example.id,
            None,
            {"source_id": source_id},
            actor_id,
        )
        return example

    async def disable_example(
        self, source_id: int, example_id: int, *, actor_id: int | None
    ) -> None:
        example = await self.session.get(SourceExample, example_id)
        if example is None or example.source_id != source_id:
            raise LookupError("source example not found")
        example.enabled = False
        await self._audit(
            "source.example.disable",
            "source_example",
            example.id,
            {"enabled": True},
            {"enabled": False},
            actor_id,
        )
        await self.session.flush()

    async def add_routing_pair(
        self,
        source_id: int,
        category: CategoryRule,
        route: RoutingRule,
        *,
        actor_id: int | None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        return await self.update_profile(
            source_id,
            profile.model_copy(
                update={
                    "category_rules": [*profile.category_rules, category],
                    "routing_rules": [*profile.routing_rules, route],
                }
            ).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def replace_routing_pair(
        self,
        source_id: int,
        index: int,
        category: CategoryRule,
        route: RoutingRule,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.routing_rules):
            raise LookupError("routing rule not found")
        routes = list(profile.routing_rules)
        current = routes[index]
        _require_item_token(current, expected_token, "routing rule not found")
        categories = list(profile.category_rules)
        matching_category = next(
            (
                category_index
                for category_index, item in enumerate(categories)
                if item.category == current.category
                and item.keywords == current.keywords
                and item.pattern == current.pattern
                and item.priority == current.priority
            ),
            None,
        )
        if matching_category is None:
            categories.append(category)
        else:
            categories[matching_category] = category
        routes[index] = route
        return await self.update_profile(
            source_id,
            profile.model_copy(
                update={"category_rules": categories, "routing_rules": routes}
            ).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def delete_routing_pair(
        self,
        source_id: int,
        index: int,
        *,
        actor_id: int | None,
        expected_token: str | None = None,
    ) -> SourceProfileData:
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None:
            raise LookupError("source not found")
        profile = SourceProfileData.model_validate(source.profile.config)
        if index < 0 or index >= len(profile.routing_rules):
            raise LookupError("routing rule not found")
        routes = list(profile.routing_rules)
        _require_item_token(routes[index], expected_token, "routing rule not found")
        removed = routes.pop(index)
        categories = list(profile.category_rules)
        matching_category = next(
            (
                category_index
                for category_index, category in enumerate(categories)
                if category.category == removed.category
                and category.keywords == removed.keywords
                and category.pattern == removed.pattern
                and category.priority == removed.priority
            ),
            None,
        )
        if matching_category is not None:
            del categories[matching_category]
        return await self.update_profile(
            source_id,
            profile.model_copy(
                update={"category_rules": categories, "routing_rules": routes}
            ).model_dump(mode="json"),
            actor_id=actor_id,
        )

    async def examples(self, source_id: int, limit: int) -> list[SourceExample]:
        rows = await self.session.scalars(
            select(SourceExample)
            .where(SourceExample.source_id == source_id, SourceExample.enabled.is_(True))
            .order_by(SourceExample.id)
            .limit(limit)
        )
        return list(rows)

    async def create_destination(
        self,
        name: str,
        channel_id: int,
        *,
        actor_id: int | None,
    ) -> Destination:
        destination = Destination(
            name=name.strip(),
            telegram_channel_id=channel_id,
        )
        self.session.add(destination)
        await self.session.flush()
        await self._audit(
            "destination.create",
            "destination",
            destination.id,
            None,
            {"name": name, "channel_id": channel_id},
            actor_id,
        )
        return destination

    async def update_destination(
        self, destination_id: int, values: dict[str, Any], *, actor_id: int | None
    ) -> Destination:
        destination = await self.session.get(Destination, destination_id)
        if destination is None:
            raise LookupError("destination not found")
        allowed = {
            "name",
            "telegram_channel_id",
            "enabled",
        }
        unknown = set(values) - allowed
        if unknown:
            raise ValueError(f"unknown destination settings: {sorted(unknown)}")
        normalized_values = dict(values)
        before = {key: getattr(destination, key) for key in allowed}
        for key, value in normalized_values.items():
            setattr(destination, key, value)
        await self._audit(
            "destination.update",
            "destination",
            destination_id,
            before,
            normalized_values,
            actor_id,
        )
        await self.session.flush()
        return destination

    async def replace_markup_rules(
        self,
        ranges: Sequence[
            tuple[Decimal, Decimal, Decimal] | tuple[Decimal, Decimal | None, Decimal, str]
        ],
        *,
        actor_id: int | None,
    ) -> None:
        models: list[MarkupRange] = []
        for index, item in enumerate(ranges):
            minimum, maximum, markup = item[:3]
            markup_type = item[3] if len(item) == 4 else "fixed"
            models.append(
                MarkupRange(
                    min_price=minimum,
                    max_price=maximum,
                    markup=markup,
                    markup_type=markup_type,  # type: ignore[arg-type]
                    priority=index + 1,
                )
            )
        PriceEngine.validate_ranges(models)
        await self.session.execute(delete(MarkupRule))
        self.session.add_all(
            [
                MarkupRule(
                    min_price=item.min_price,
                    max_price=item.max_price,
                    markup=item.markup,
                    markup_type=item.markup_type,
                    priority=item.priority,
                    enabled=True,
                )
                for item in models
            ]
        )
        await self._audit(
            "markup.replace",
            "markup_rules",
            "all",
            None,
            {"ranges": [item.model_dump(mode="json") for item in models]},
            actor_id,
        )
        await self.session.flush()

    async def create_button(
        self, data: NavigationButtonData, destination_ids: list[int], *, actor_id: int | None
    ) -> NavigationButton:
        button = NavigationButton(
            text=data.text,
            action=data.action,
            url=data.url,
            enabled=data.enabled,
        )
        self.session.add(button)
        await self.session.flush()
        for destination_id in destination_ids:
            self.session.add(
                DestinationButtonBinding(
                    destination_id=destination_id,
                    button_id=button.id,
                    row=data.row,
                    order=data.order,
                )
            )
        await self._audit(
            "button.create",
            "navigation_button",
            button.id,
            None,
            data.model_dump(mode="json"),
            actor_id,
        )
        await self.session.flush()
        return button

    async def update_button(
        self,
        button_id: int,
        data: NavigationButtonData,
        destination_ids: list[int],
        *,
        actor_id: int | None,
    ) -> NavigationButton:
        button = await self.session.get(NavigationButton, button_id)
        if button is None:
            raise LookupError("button not found")
        before = {"text": button.text, "action": button.action.value, "url": button.url}
        button.text = data.text
        button.action = data.action
        button.url = data.url
        button.enabled = data.enabled
        await self.session.execute(
            delete(DestinationButtonBinding).where(DestinationButtonBinding.button_id == button_id)
        )
        self.session.add_all(
            [
                DestinationButtonBinding(
                    destination_id=destination_id,
                    button_id=button_id,
                    row=data.row,
                    order=data.order,
                )
                for destination_id in destination_ids
            ]
        )
        await self._audit(
            "button.update",
            "navigation_button",
            button.id,
            before,
            data.model_dump(mode="json"),
            actor_id,
        )
        await self.session.flush()
        return button

    async def create_catalog_node(
        self,
        data: CatalogNodeData,
        *,
        parent_id: int | None,
        actor_id: int | None,
    ) -> CatalogNode:
        if parent_id is not None:
            parent = await self.session.get(CatalogNode, parent_id)
            if (
                parent is None
                or parent.archived_at is not None
                or parent.node_type is not CatalogNodeType.CATEGORY
            ):
                raise ValueError("catalog parent must be an active category")
        node = CatalogNode(
            parent_id=parent_id,
            text_ru=data.text_ru,
            text_en=data.text_en,
            node_type=data.node_type,
            url=data.url,
            position=data.position,
            enabled=data.enabled,
        )
        self.session.add(node)
        await self.session.flush()
        await self._audit(
            "catalog_node.create",
            "catalog_node",
            node.id,
            None,
            {**data.model_dump(mode="json"), "parent_id": parent_id},
            actor_id,
        )
        await self.session.flush()
        return node

    async def update_catalog_node(
        self,
        node_id: int,
        data: CatalogNodeData,
        *,
        actor_id: int | None,
    ) -> CatalogNode:
        node = await self.session.get(CatalogNode, node_id)
        if node is None or node.archived_at is not None:
            raise LookupError("catalog node not found")
        if data.node_type is CatalogNodeType.LINK and node.node_type is CatalogNodeType.CATEGORY:
            has_children = await self.session.scalar(
                select(exists().where(CatalogNode.parent_id == node_id))
            )
            if has_children:
                raise ValueError("category with children cannot become a link")
        before = {
            "text_ru": node.text_ru,
            "text_en": node.text_en,
            "node_type": node.node_type.value,
            "url": node.url,
            "position": node.position,
            "enabled": node.enabled,
        }
        node.text_ru = data.text_ru
        node.text_en = data.text_en
        node.node_type = data.node_type
        node.url = data.url
        node.position = data.position
        node.enabled = data.enabled
        await self._audit(
            "catalog_node.update",
            "catalog_node",
            node.id,
            before,
            data.model_dump(mode="json"),
            actor_id,
        )
        await self.session.flush()
        return node

    async def set_catalog_node_enabled(
        self, node_id: int, enabled: bool, *, actor_id: int | None
    ) -> None:
        node = await self.session.get(CatalogNode, node_id)
        if node is None or node.archived_at is not None:
            raise LookupError("catalog node not found")
        before = node.enabled
        node.enabled = enabled
        await self._audit(
            "catalog_node.enable" if enabled else "catalog_node.disable",
            "catalog_node",
            node_id,
            {"enabled": before},
            {"enabled": enabled},
            actor_id,
        )
        await self.session.flush()

    async def archive_catalog_node(self, node_id: int, *, actor_id: int | None) -> None:
        node = await self.session.get(CatalogNode, node_id)
        if node is None:
            raise LookupError("catalog node not found")
        node.enabled = False
        node.archived_at = utcnow()
        await self._audit(
            "catalog_node.archive",
            "catalog_node",
            node_id,
            None,
            {"archived": True},
            actor_id,
        )
        await self.session.flush()

    async def restore_catalog_node(self, node_id: int, *, actor_id: int | None) -> CatalogNode:
        node = await self.session.get(CatalogNode, node_id)
        if node is None or node.archived_at is None:
            raise LookupError("archived catalog node not found")
        if node.parent_id is not None:
            parent = await self.session.get(CatalogNode, node.parent_id)
            if parent is None or parent.archived_at is not None:
                raise ValueError("cannot restore catalog node: restore its parent first")
        before = {"archived_at": node.archived_at.isoformat(), "enabled": node.enabled}
        node.archived_at = None
        node.enabled = True
        await self._audit(
            "catalog_node.restore",
            "catalog_node",
            node_id,
            before,
            {"archived_at": None, "enabled": True},
            actor_id,
        )
        await self.session.flush()
        return node

    async def archive_button(self, button_id: int, *, actor_id: int | None) -> None:
        button = await self.session.get(NavigationButton, button_id)
        if button is None:
            raise LookupError("button not found")
        button.enabled = False
        button.archived_at = utcnow()
        await self._audit(
            "button.archive", "navigation_button", button.id, None, {"archived": True}, actor_id
        )
        await self.session.flush()

    async def set_button_enabled(
        self, button_id: int, enabled: bool, *, actor_id: int | None
    ) -> None:
        button = await self.session.get(NavigationButton, button_id)
        if button is None or button.archived_at is not None:
            raise LookupError("button not found")
        before = button.enabled
        button.enabled = enabled
        await self._audit(
            "button.enable" if enabled else "button.disable",
            "navigation_button",
            button_id,
            {"enabled": before},
            {"enabled": enabled},
            actor_id,
        )
        await self.session.flush()

    async def set_source_destination(
        self, source_id: int, destination_id: int, *, actor_id: int | None
    ) -> None:
        destination = await self.session.get(Destination, destination_id)
        if destination is None or not destination.enabled:
            raise LookupError("Канал назначения недоступен или выключен")
        source = await self.session.get(Source, source_id, with_for_update=True)
        if source is None or source.archived_at is not None:
            raise LookupError("Источник недоступен")
        await self.update_profile(
            source_id,
            {**source.profile.config, "default_destination_id": destination_id},
            actor_id=actor_id,
        )

    async def set_setting(self, key: str, value: dict[str, Any], *, actor_id: int | None) -> None:
        if key == "destination_price_policies":
            from emoda.services.destination_price import DestinationPricePolicy

            if any(not str(k).isdigit() for k in value):
                raise ValueError("destination policy requires numeric destination IDs")
            value = {
                k: DestinationPricePolicy.model_validate(v).model_dump(mode="json")
                for k, v in value.items()
            }
        if key == "currency_rates":
            value = {k: str(v) for k, v in PriceEngine.validate_currency_rates(value).items()}
        if key == "eur_usd_rate" and Decimal(str(value.get("value"))) != Decimal("1.2"):
            raise ValueError("EUR conversion rate is fixed at 1.2")
        if key == "default_publication_mode":
            PublicationMode(str(value.get("value")))
        setting = await self.session.get(GlobalSetting, key)
        before = None if setting is None else dict(setting.value)
        if setting is None:
            setting = GlobalSetting(key=key, value=value)
            self.session.add(setting)
        else:
            setting.value = value
        await self._audit("setting.update", "global_setting", key, before, value, actor_id)
        await self.session.flush()

    async def find_products(self, query: str, limit: int = 20) -> list[Product]:
        pattern = f"%{query}%"
        clause: ColumnElement[bool] = or_(
            Product.emoda_code.ilike(pattern),
            Product.original_text.ilike(pattern),
            Product.cleaned_text.ilike(pattern),
            Product.final_text.ilike(pattern),
            cast(Product.parsed_result, String).ilike(pattern),
        )
        if query.isdigit():
            telegram_message_id = int(query)
            # Resolve the small set of matching supplier messages once. A
            # correlated EXISTS inside the text-search OR forced a full product
            # scan, even when trigram indexes were available.
            refs = list(await self.session.execute(
                select(SourceMessage.id, SourceMessage.album_group_id).where(
                    SourceMessage.telegram_message_id == telegram_message_id
                )
            ))
            if refs:
                clause = or_(clause, Product.source_message_id.in_([r.id for r in refs]))
                album_ids = {r.album_group_id for r in refs if r.album_group_id is not None}
                if album_ids:
                    clause = or_(clause, Product.album_group_id.in_(album_ids))
        rows = await self.session.scalars(
            select(Product).where(clause).order_by(Product.created_at.desc()).limit(limit)
        )
        return list(rows)
