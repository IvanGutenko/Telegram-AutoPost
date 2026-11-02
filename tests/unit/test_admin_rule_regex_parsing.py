"""Admin parsing of arbitrary regex patterns for extraction rules.

Production evidence (Niccy, source 51) showed that the same rule saved or failed
depending only on how Telegram delivered the text: multi-line with ``field=``
first worked, the identical rule collapsed into one line answered
``field must use 1-32 lowercase latin letters, digits or underscore``. These
tests pin every shape a real admin can send.
"""

from __future__ import annotations

import re

import pytest

from emoda.admin.service import (
    parse_extraction_rule_edit_input,
    parse_extraction_rule_input,
)
from emoda.domain.schemas import ExtractionRule

NICCY_PATTERN = r"(?m)^[ ]*(?P<value>[0-9]+(?:[.,][0-9]+)?)[ ]*[^0-9A-Za-z\r\n]*$"

PATTERN_CLASSES: tuple[tuple[str, str], ...] = (
    ("named-group", r"(?P<value>[0-9]+)"),
    ("whitespace", r"(?i)Price\s*:\s*(?P<value>\d+)"),
    ("anchors", r"(?m)^\s*(?P<value>\d+)\s*$"),
    ("character-class", r"(?P<value>[0-9]+(?:[.,][0-9]+)?)"),
    ("escaped-hex", r"(?P<value>\d+)\x20*\$"),
    ("alternation", r"(?i)(?:PRICE|FIYAT|FİYAT)\s*:?\s*(?P<value>\d+)"),
    ("lookaround", r"(?<=Price: )(?P<value>\d+)"),
    ("literal-equals", r"Price\s*=\s*(?P<value>\d+)"),
    ("literal-pipe", r"(?:Price|Cost)\s*:\s*(?P<value>\d+)"),
    ("curly-quantifier", r"(?P<value>\d{1,5}(?:[.,]\d{1,2})?)"),
    ("negated-class", NICCY_PATTERN),
    ("lazy-tail", r"Sizes\s*:\s*(?P<value>.+?)$"),
    ("literal-words", r"Sizes\s*:\s*STANDART\s*(?P<value>\d+)\s*PCS"),
    ("nested-groups", r"(?i)(?:(?:Fiyat|Price)\s*[:=]?\s*)(?P<value>\d+(?:[.,]\d+)?)"),
    ("backslash-classes", r"(?m)^\s*\W*(?P<value>\d+)\D*$"),
)


def _shapes(pattern: str) -> dict[str, str]:
    """Every text shape a real admin can send for one and the same rule."""
    return {
        "multiline-field-first": (
            f"field=price\nkind=regex\npattern={pattern}\n"
            "transform=identity\npriority=11\nauthoritative=true"
        ),
        "multiline-kind-first": (
            f"kind=regex\nfield=price\npattern={pattern}\n"
            "priority=11\nauthoritative=true"
        ),
        "multiline-pattern-first": (
            f"pattern={pattern}\nfield=price\nkind=regex\npriority=11"
        ),
        "multiline-spaces-around-equals": (
            f"field = price\nkind = regex\npattern = {pattern}\npriority = 11"
        ),
        "single-line": (
            f"field=price kind=regex pattern={pattern} priority=11 authoritative=true"
        ),
        "single-line-pattern-last": (
            f"field=price kind=regex priority=11 pattern={pattern}"
        ),
        "crlf": f"field=price\r\nkind=regex\r\npattern={pattern}\r\npriority=11",
        "blank-lines": f"field=price\n\nkind=regex\n\npattern={pattern}\n",
    }


@pytest.mark.parametrize("shape", sorted(_shapes(NICCY_PATTERN)))
def test_niccy_customer_regex_is_accepted_in_every_admin_input_shape(shape: str) -> None:
    field, rule = parse_extraction_rule_input(_shapes(NICCY_PATTERN)[shape])

    assert field == "price"
    assert rule.kind == "regex"
    assert rule.pattern == NICCY_PATTERN
    assert re.compile(rule.pattern).groupindex == {"value": 1}


@pytest.mark.parametrize(("label", "pattern"), PATTERN_CLASSES, ids=lambda item: item)
def test_every_regex_class_survives_create_and_edit_unchanged(
    label: str, pattern: str
) -> None:
    for shape, text in _shapes(pattern).items():
        field, rule = parse_extraction_rule_input(text)
        assert (field, rule.pattern) == ("price", pattern), (label, shape)

    current = ExtractionRule(kind="regex", pattern="old-pattern", priority=41)
    for shape, text in {
        "multiline": f"pattern={pattern}",
        "single-line": f"pattern={pattern}",
        "with-neighbour-setting": f"pattern={pattern}\npriority=12",
        "single-line-with-neighbour": f"pattern={pattern} priority=12",
    }.items():
        edited_field, edited = parse_extraction_rule_edit_input(
            text, current_field="price", current_rule=current
        )
        assert (edited_field, edited.pattern) == ("price", pattern), (label, shape)
        assert edited.kind == "regex"


def test_exotic_unicode_separators_inside_a_pattern_are_not_line_breaks() -> None:
    """``str.splitlines`` also breaks on \\x0b, \\x1c and U+2028."""
    pattern = "(?P<value>\\d+)[\x0b\x1c\x1e  ]*"
    field, rule = parse_extraction_rule_input(
        f"field=price\nkind=regex\npattern={pattern}\npriority=11"
    )

    assert field == "price"
    assert rule.pattern == pattern


def test_broken_regex_reports_a_regex_error_and_never_blames_the_field() -> None:
    with pytest.raises(ValueError) as error:
        parse_extraction_rule_input(
            "field=price\nkind=regex\npattern=(?P<value>[0-9\npriority=11"
        )

    message = str(error.value)
    assert "Некорректный regex" in message
    assert "unterminated character set" in message
    assert "field" not in message.casefold()


def test_broken_token_pattern_names_the_token_pattern_setting() -> None:
    with pytest.raises(ValueError) as error:
        parse_extraction_rule_input(
            "field=quantity\nkind=regex\npattern=(?P<value>\\d+)\n"
            "transform=series_quantity\ntoken_pattern=(?P<size>[A-Z]+"
        )

    assert "Некорректный regex" in str(error.value)
    assert "token_pattern" in str(error.value)


def test_unknown_kind_is_reported_as_a_kind_problem() -> None:
    with pytest.raises(ValueError) as error:
        parse_extraction_rule_input("field=price\nkind=regexp\npattern=(?P<value>\\d+)")

    assert "kind" in str(error.value)
    assert "regex" in str(error.value)


def test_missing_pattern_is_reported_as_a_missing_setting() -> None:
    with pytest.raises(ValueError) as error:
        parse_extraction_rule_input("field=price\nkind=regex")

    assert "pattern" in str(error.value)


def test_typo_in_a_setting_name_is_reported_instead_of_swallowed() -> None:
    with pytest.raises(ValueError) as error:
        parse_extraction_rule_input(
            "field=price\nkind=regex\npatern=(?P<value>\\d+)\npriority=11"
        )

    assert "patern" in str(error.value)


def test_legacy_positional_rule_format_still_works() -> None:
    assert parse_extraction_rule_input("article|label|Артикул")[0] == "article"
    field, rule = parse_extraction_rule_input(
        r"article|regex|Code:\s*(?=[A-Z])(?P<value>[A-Z0-9]+)"
    )

    assert field == "article"
    assert rule.kind == "regex"
    assert rule.pattern == r"Code:\s*(?=[A-Z])(?P<value>[A-Z0-9]+)"


def test_legacy_format_is_never_chosen_for_structured_input_with_pipes() -> None:
    field, rule = parse_extraction_rule_input(
        "field=price\nkind=regex\npattern=(?:Price|Cost)\\s*:\\s*(?P<value>\\d+)"
    )

    assert (field, rule.pattern) == ("price", r"(?:Price|Cost)\s*:\s*(?P<value>\d+)")


def test_regex_pattern_is_stored_losslessly_for_backslash_heavy_input() -> None:
    pattern = r"(?m)^\s*\\?(?P<value>\d+)\s*(?:TL|₺)?\s*$"
    _, rule = parse_extraction_rule_input(
        f"field=price\nkind=regex\npattern={pattern}"
    )

    assert rule.pattern == pattern
    assert rule.pattern.count("\\") == pattern.count("\\")


def test_editor_round_trip_keeps_the_rule_identical() -> None:
    from emoda.admin.router import extraction_rule_editor_text

    rule = ExtractionRule(
        kind="regex",
        pattern=NICCY_PATTERN,
        aliases=["Fiyat", "Price"],
        priority=11,
        ignore_case=False,
        transform="identity",
        authoritative=True,
        enabled=True,
    )
    rendered = extraction_rule_editor_text("price", rule)

    field, reparsed = parse_extraction_rule_input(rendered)

    assert field == "price"
    assert reparsed.model_dump(mode="json") == rule.model_dump(mode="json")


def test_setting_shaped_text_inside_regex_is_never_reparsed_as_field() -> None:
    """The remaining production branch split ``field=`` inside a lookahead."""
    pattern = r"(?i)Price\s*:\s*(?P<value>\d+)(?= field=price)"
    field, created = parse_extraction_rule_input(
        f"field=price kind=regex pattern={pattern} priority=11"
    )
    edited_field, edited = parse_extraction_rule_edit_input(
        f"pattern={pattern} priority=12",
        current_field="price",
        current_rule=created,
    )

    assert (field, created.pattern) == ("price", pattern)
    assert (edited_field, edited.pattern, edited.priority) == ("price", pattern, 12)


@pytest.mark.parametrize(
    "pattern",
    (
        NICCY_PATTERN,
        r"(?i)Price\s*:\s*(?P<value>\d+)",
        r"(?P<value>\d+)\x20*\$",
        r"(?i)(?:PRICE|FIYAT|FİYAT)\s*:?\s*(?P<value>\d+)",
        r"Price\s*=\s*(?P<value>\d+)",
        r"(?P<value>\d{1,5}(?:[.,]\d{1,2})?)",
    ),
)
@pytest.mark.parametrize("field", ("price", "sizes", "quantity", "article"))
def test_customer_torture_matrix_is_lossless_in_create_and_edit(
    field: str, pattern: str
) -> None:
    created_field, created = parse_extraction_rule_input(
        f"field={field}\nkind=regex\npattern={pattern}\npriority=11"
    )
    edited_field, edited = parse_extraction_rule_edit_input(
        f"pattern={pattern}\npriority=12",
        current_field=created_field,
        current_rule=created,
    )

    assert (created_field, created.pattern) == (field, pattern)
    assert (edited_field, edited.pattern, edited.priority) == (field, pattern, 12)
