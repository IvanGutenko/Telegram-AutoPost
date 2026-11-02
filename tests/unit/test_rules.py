import pytest
from pydantic import ValidationError

from emoda.domain.enums import FieldStatus, Provenance
from emoda.domain.schemas import (
    CleaningRule,
    ComputedRule,
    ExtractionRule,
    FieldProcessingPolicy,
    RepeatedPatternRule,
    SourceProfileData,
)
from emoda.rules.engine import RuleEngine, preview_extraction_rule

NICCY_PRICE_PATTERN = (
    r"(?m)^[ \t]{0,}📌[ \t]{0,}(?P<value>[0-9]+(?:[.,][0-9]+)?)"
    r"[ \t]{0,}[^0-9A-Za-z\r\n ]{1,}[ \t]{0,}$"
)


def test_advertising_filter_uses_only_strong_markers_or_source_patterns() -> None:
    from emoda.rules.engine import advertisement_reason

    default = SourceProfileData()
    custom = SourceProfileData(advertising_patterns=[r"(?im)^Partner promotion:\s"])
    disabled = SourceProfileData(advertising_filter_enabled=False)

    assert advertisement_reason("#реклама\nПодпишитесь на новый сервис", default) == "builtin"
    assert advertisement_reason("Partner promotion: external course", custom) == "source-pattern"
    assert (
        advertisement_reason("Рекламная футболка\nPrice: 10 USD\nSizes: S M\n2 pcs", default)
        is None
    )
    assert advertisement_reason("#реклама\nПодпишитесь", disabled) is None


def profile(**rules: list[ExtractionRule]) -> SourceProfileData:
    return SourceProfileData(extraction_rules=rules)


def test_explicit_price_and_currency_are_extracted() -> None:
    result = RuleEngine(
        profile(
            price=[
                ExtractionRule(kind="regex", pattern=r"Price:\s*(?P<value>\d+)\s*(?P<currency>EUR)")
            ]
        )
    ).parse("Price: 10 EUR")
    assert result.fields["price"].value == "10"
    assert result.fields["price"].metadata["currency"] == "EUR"
    assert result.fields["price"].provenance is Provenance.SOURCE_RULE


def test_niccy_dollar_banknote_alias_survives_price_passthrough_and_preview_parity() -> None:
    rule = ExtractionRule(
        kind="regex",
        pattern=NICCY_PRICE_PATTERN,
        priority=10,
        ignore_case=True,
        transform="identity",
        authoritative=True,
    )
    profile = SourceProfileData(
        default_currency=None,
        field_policies={"price": FieldProcessingPolicy(processing_mode="passthrough")},
        extraction_rules={"price": [rule]},
        gemini_enabled=False,
    )
    text = (
        "Kod: 107144\n\n🔥RPT🔥\n🪬Katmanlı Çift Fermuarlı Takım🪬\n\n"
        "📌1-1/2-2 \n📌(paketler 4 lü)\n📌Krinkıl&Modal Kumaş \n\n"
        "📌25,5 💵\n\n\n\nWhatsaap:+905536138944"
    )

    preview = preview_extraction_rule(profile, "price", rule, text)
    parsed = RuleEngine(profile).parse(text)
    price = parsed.fields["price"]

    assert preview.matched is True
    assert preview.values == ("25,5",)
    assert preview.captures == ("25,5",)
    assert preview.normalized_values == ("25.5",)
    assert preview.currencies == ("USD",)
    assert preview.candidate_statuses == (FieldStatus.FOUND,)
    assert preview.flags == ("IGNORECASE", "MULTILINE")
    assert price.status is FieldStatus.FOUND
    assert price.value == "25.5"
    assert price.metadata["raw_value"] == "25,5"
    assert price.metadata["currency"] == "USD"
    assert price.metadata["rule_id"] == "price:1"
    assert price.metadata["authoritative"] is True


@pytest.mark.parametrize(
    "text",
    [
        "FIYAT : 13💲",
        "FİYAT : 13💲",
        "fiyat : 13💲",
        "fİyat : 13💲",
        "FIYAT : 13$",
        "FIYAT : 13 $",
        "FIYAT : 13 💲",
    ],
)
def test_price_rule_infers_adjacent_usd_alias_from_match_context(text: str) -> None:
    parsed = RuleEngine(
        profile(
            price=[
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?i)F[İI]YAT\s*:?\s*(?P<value>\d+(?:[.,]\d+)?)",
                    priority=1,
                    authoritative=True,
                )
            ]
        )
    ).parse(text)

    assert parsed.fields["price"].status is FieldStatus.FOUND
    assert parsed.fields["price"].value == "13"
    assert parsed.fields["price"].metadata["currency"] == "USD"
    assert parsed.fields["price"].provenance is Provenance.SOURCE_RULE


def test_fully_disabled_configured_price_rules_remain_an_explicit_opt_out() -> None:
    parsed = RuleEngine(
        profile(
            price=[
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?i)F[İI]YAT\s*:?\s*(?P<value>\d+(?:[.,]\d+)?)",
                    enabled=False,
                )
            ]
        )
    ).parse("FIYAT : 13💲")

    assert parsed.fields["price"].status is FieldStatus.MISSING


def test_adjacent_currency_inference_does_not_require_a_cleaning_rule() -> None:
    rule = ExtractionRule(
        kind="regex",
        pattern=r"(?i)F[İI]YAT\s*:?\s*(?P<value>\d+(?:[.,]\d+)?)",
        authoritative=True,
    )
    text = "FIYAT : 13💲"
    unchanged = RuleEngine(SourceProfileData(extraction_rules={"price": [rule]})).parse(text)
    cleaned = RuleEngine(
        SourceProfileData(
            extraction_rules={"price": [rule]},
            cleaning_rules=[
                CleaningRule(
                    kind="regex",
                    value="💲",
                    replacement="$",
                    scope="text",
                    ignore_case=False,
                )
            ],
        )
    ).parse(text)

    assert unchanged.fields["price"].metadata["currency"] == "USD"
    assert cleaned.fields["price"].metadata["currency"] == "USD"
    assert unchanged.fields["price"].value == cleaned.fields["price"].value == "13"


def test_price_rule_remains_source_isolated() -> None:
    rule = ExtractionRule(
        kind="regex",
        pattern=r"(?i)F[İI]YAT\s*:?\s*(?P<value>\d+(?:[.,]\d+)?)",
        authoritative=True,
    )
    text = "FIYAT : 13💲"

    own = RuleEngine(SourceProfileData(extraction_rules={"price": [rule]})).parse(text)
    isolated = RuleEngine(SourceProfileData(extraction_rules={"price": []})).parse(text)

    assert own.fields["price"].provenance is Provenance.SOURCE_RULE
    assert isolated.fields["price"].status is FieldStatus.MISSING


def test_enabled_configured_nonmatch_falls_back_to_builtin_strict_rule() -> None:
    parsed = RuleEngine(
        profile(
            sizes=[
                ExtractionRule(
                    kind="regex",
                    pattern=(
                        r"(?i)\bSizes\s*:\s*(?P<value>(?:XS|XXL|XL|L|M|S)"
                        r"(?:\s+(?:XS|XXL|XL|L|M|S)))\s+\d+\spcs\b"
                    ),
                    priority=10,
                    authoritative=True,
                )
            ],
            quantity=[
                ExtractionRule(
                    kind="regex",
                    pattern=(
                        r"(?i)\bSizes\s*:\s*(?:XS|XXL|XL|L|M|S)"
                        r"(?:\s+(?:XS|XXL|XL|L|M|S))\s+(?P<value>\d+)\spcs\b"
                    ),
                    priority=10,
                    authoritative=True,
                )
            ],
        )
    ).parse("Sizes  : S M L XL  4 pcs")

    assert parsed.fields["sizes"].value == "S M L XL"
    assert parsed.fields["quantity"].value == "4"
    assert parsed.fields["sizes"].provenance is Provenance.STRICT_TEXT
    assert parsed.fields["quantity"].provenance is Provenance.STRICT_TEXT


def test_all_required_size_formats_are_preserved() -> None:
    engine = RuleEngine(SourceProfileData())
    for value in [
        "S M L XL",
        "S-M-L-XL",
        "36/38/40/42",
        "36-38-40-42",
        "2/3/4/5",
        "42 44 46",
        "STD",
        "Standart",
        "Oversize",
        "1 beden / 2 beden",
    ]:
        parsed = engine.parse(f"Sizes: {value}")
        assert parsed.fields["sizes"].value == value


def test_size_count_is_never_quantity() -> None:
    parsed = RuleEngine(SourceProfileData()).parse("Sizes: S M L XL")
    assert parsed.fields["quantity"].status is FieldStatus.MISSING


def test_numeric_size_count_is_never_quantity_without_computed_rule() -> None:
    parsed = RuleEngine(SourceProfileData()).parse("Sizes: 36 37 38 39 40")
    assert parsed.fields["quantity"].status is FieldStatus.MISSING


@pytest.mark.parametrize(
    ("sizes", "expected"),
    [
        ("36 37 38 39 40", "5"),
        ("37 38 39 40 41", "5"),
    ],
)
def test_count_items_computes_quantity_only_when_configured(
    sizes: str, expected: str
) -> None:
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[
                ComputedRule(
                    name="count-numeric-sizes",
                    target_field="quantity",
                    operation="count_items",
                    source_field="sizes",
                    pattern=r"(?P<item>\d+)",
                    capture_group="item",
                    authoritative=True,
                )
            ]
        )
    ).parse(f"Sizes: {sizes}")

    assert parsed.fields["sizes"].value == sizes
    assert parsed.fields["quantity"].value == expected
    assert parsed.fields["quantity"].provenance is Provenance.COMPUTED_RULE
    assert parsed.fields["quantity"].metadata["computed_rule"] == "count-numeric-sizes"


def test_disabled_count_items_does_not_compute_quantity() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[
                ComputedRule(
                    name="disabled-count",
                    target_field="quantity",
                    operation="count_items",
                    source_field="sizes",
                    pattern=r"(?P<item>\d+)",
                    capture_group="item",
                    enabled=False,
                )
            ]
        )
    ).parse("Sizes: 36 37 38 39 40")

    assert parsed.fields["quantity"].status is FieldStatus.MISSING


@pytest.mark.parametrize(
    ("raw_sizes", "expected_sizes", "expected_quantity"),
    [
        ("S. S. M. M. L", "S M L", "5"),
        ("XS. S. S. M. L. XL", "XS S M L XL", "6"),
        ("36 36 38 38 40", "36 38 40", "5"),
    ],
)
def test_computed_rules_can_count_configured_matches_from_canonical_text(
    raw_sizes: str,
    expected_sizes: str,
    expected_quantity: str,
) -> None:
    token = r"XS|XXL|XL|S|M|L|\d{2}"
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[
                ComputedRule(
                    name="unique-sizes",
                    target_field="sizes",
                    operation="unique_matches",
                    input_source="canonical_text",
                    pattern=rf"(?<![A-Za-z0-9])(?P<size>{token})(?![A-Za-z0-9])",
                    capture_group="size",
                    separator=" ",
                    priority=10,
                    authoritative=True,
                    conflict_policy="computed",
                ),
                ComputedRule(
                    name="count-source-size-occurrences",
                    target_field="quantity",
                    operation="count_items",
                    input_source="canonical_text",
                    pattern=rf"(?<![A-Za-z0-9])(?P<item>{token})(?![A-Za-z0-9])",
                    capture_group="item",
                    priority=20,
                    authoritative=True,
                    conflict_policy="explicit",
                ),
            ]
        )
    ).parse(raw_sizes)

    assert parsed.fields["sizes"].value == expected_sizes
    quantity = parsed.fields["quantity"]
    assert quantity.value == expected_quantity
    assert quantity.metadata["input_source"] == "canonical_text"
    assert quantity.metadata["pattern_matches"] == raw_sizes.replace(".", "").split()
    assert quantity.metadata["match_count"] == int(expected_quantity)
    assert quantity.provenance is Provenance.COMPUTED_RULE


def test_computed_input_source_distinguishes_field_raw_candidate_and_canonical_text() -> None:
    raw_sizes = "S. S. M. M. L"
    profile = SourceProfileData(
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?P<value>S\. S\. M\. M\. L)",
                    transform="map",
                    value_map={raw_sizes: "S M L"},
                    authoritative=True,
                )
            ]
        },
        computed_rules=[
            ComputedRule(
                name="field-count",
                target_field="field_count",
                operation="count_items",
                input_source="field",
                source_field="sizes",
            ),
            ComputedRule(
                name="raw-count",
                target_field="raw_count",
                operation="count_items",
                input_source="raw_field_candidate",
                source_field="sizes",
                pattern=r"(?P<size>XS|S|M|L|XL)",
                capture_group="size",
            ),
            ComputedRule(
                name="canonical-count",
                target_field="canonical_count",
                operation="count_items",
                input_source="canonical_text",
                pattern=r"(?P<size>XS|S|M|L|XL)",
                capture_group="size",
            ),
        ],
    )

    parsed = RuleEngine(profile).parse(raw_sizes)

    assert parsed.fields["field_count"].value == "3"
    assert parsed.fields["raw_count"].value == "5"
    assert parsed.fields["canonical_count"].value == "5"


def test_legacy_raw_input_mode_keeps_meaning_raw_field_candidate() -> None:
    with pytest.raises(ValidationError, match="requires source_field"):
        ComputedRule(
            name="legacy-raw-without-field",
            target_field="quantity",
            operation="count_items",
            input_mode="raw",
            pattern=r"(?P<size>S|M|L)",
            capture_group="size",
        )


def _series_count_rule(**overrides: object) -> ComputedRule:
    values: dict[str, object] = {
        "name": "count-raw-size-series",
        "target_field": "quantity",
        "operation": "count_series_items",
        "source_field": "sizes",
        "input_mode": "raw",
        "series_separator_pattern": r"\s+",
        "range_pattern": r"(?P<start>\d+)-(?P<end>\d+)",
        "explicit_list_pattern": r"(?P<item>\d+)",
        "explicit_separator_pattern": r"-",
        "compact_range_pattern": r"(?P<start>\d{2})(?P<end>\d{2})",
        "numeric_step": "2",
        "authoritative": True,
    }
    values.update(overrides)
    return ComputedRule.model_validate(values)


@pytest.mark.parametrize(
    ("raw_sizes", "expected_quantity", "expected_counts"),
    [
        ("42-44-46", "3", [3]),
        ("42-48", "4", [4]),
        ("42-48 50-56", "4", [4, 4]),
        ("4248 5056", "4", [4, 4]),
        ("36-42 42-48", "4", [4, 4]),
        ("36-44", "5", [5]),
    ],
)
def test_count_series_items_counts_each_raw_alternative_without_summing(
    raw_sizes: str, expected_quantity: str, expected_counts: list[int]
) -> None:
    parsed = RuleEngine(
        SourceProfileData(computed_rules=[_series_count_rule()])
    ).parse(f"Sizes: {raw_sizes}")

    quantity = parsed.fields["quantity"]
    assert quantity.status is FieldStatus.FOUND
    assert quantity.value == expected_quantity
    assert quantity.provenance is Provenance.COMPUTED_RULE
    assert quantity.metadata["rule_id"] == "count-raw-size-series"
    assert quantity.metadata["operation"] == "count_series_items"
    assert quantity.metadata["series_counts"] == expected_counts
    assert quantity.metadata["input_mode"] == "raw"


def test_count_series_items_uses_pre_normalization_source_value() -> None:
    raw_sizes = "42-48 50-56"
    flattened = "42, 44, 46, 48, 50, 52, 54, 56"
    profile = SourceProfileData(
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Sizes:\s*(?P<value>42-48 50-56)",
                    transform="map",
                    value_map={raw_sizes: flattened},
                    authoritative=True,
                )
            ]
        },
        computed_rules=[_series_count_rule()],
    )

    parsed = RuleEngine(profile).parse(f"Sizes: {raw_sizes}")

    assert parsed.fields["sizes"].value == flattened
    assert parsed.fields["sizes"].metadata["raw_value"] == raw_sizes
    assert parsed.fields["quantity"].value == "4"
    assert parsed.fields["quantity"].metadata["series_counts"] == [4, 4]


def test_count_series_items_marks_unequal_alternatives_ambiguous() -> None:
    parsed = RuleEngine(
        SourceProfileData(computed_rules=[_series_count_rule()])
    ).parse("Sizes: 42-48 36-44")

    quantity = parsed.fields["quantity"]
    assert quantity.status is FieldStatus.AMBIGUOUS
    assert quantity.value is None
    assert quantity.provenance is Provenance.COMPUTED_RULE
    assert quantity.metadata["series_counts"] == [4, 5]
    assert quantity.metadata["rule_id"] == "count-raw-size-series"


def test_disabled_count_series_items_does_not_compute_quantity() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[_series_count_rule(enabled=False)]
        )
    ).parse("Sizes: 42-48")

    assert parsed.fields["quantity"].status is FieldStatus.MISSING


def test_count_series_items_does_not_guess_unconfigured_or_inexact_ranges() -> None:
    profile = SourceProfileData(computed_rules=[_series_count_rule()])

    assert (
        RuleEngine(profile).parse("Sizes: unknown").fields["quantity"].status
        is FieldStatus.MISSING
    )
    assert (
        RuleEngine(profile).parse("Sizes: 42-47").fields["quantity"].status
        is FieldStatus.MISSING
    )


@pytest.mark.parametrize(
    ("policy", "status", "value"),
    [
        ("explicit", FieldStatus.FOUND, "8"),
        ("computed", FieldStatus.FOUND, "4"),
        ("ambiguous", FieldStatus.AMBIGUOUS, None),
    ],
)
def test_count_series_items_uses_existing_explicit_conflict_policy(
    policy: str, status: FieldStatus, value: str | None
) -> None:
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[_series_count_rule(conflict_policy=policy)]
        )
    ).parse("Sizes: 42-48\nQuantity: 8")

    assert parsed.fields["quantity"].status is status
    assert parsed.fields["quantity"].value == value
    if policy == "computed":
        assert parsed.fields["quantity"].metadata["authoritative"] is True


def test_count_series_items_uses_existing_computed_priority() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[
                _series_count_rule(name="priority-winner", priority=1),
                _series_count_rule(
                    name="later-rule",
                    priority=2,
                    numeric_step="3",
                ),
            ]
        )
    ).parse("Sizes: 42-48")

    assert parsed.fields["quantity"].value == "4"
    assert parsed.fields["quantity"].metadata["rule_id"] == "priority-winner"


def test_count_series_items_is_isolated_to_configured_source_profile() -> None:
    text = "Sizes: 42-48 50-56"
    configured = SourceProfileData(computed_rules=[_series_count_rule()])

    assert RuleEngine(configured).parse(text).fields["quantity"].value == "4"
    assert (
        RuleEngine(SourceProfileData()).parse(text).fields["quantity"].status
        is FieldStatus.MISSING
    )


def test_existing_count_items_still_counts_flattened_items() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            extraction_rules={
                "sizes": [
                    ExtractionRule(
                        kind="regex",
                        pattern=r"Sizes:\s*(?P<value>[^\n]+)",
                        authoritative=True,
                    )
                ]
            },
            computed_rules=[
                ComputedRule(
                    name="legacy-flat-count",
                    target_field="quantity",
                    operation="count_items",
                    source_field="sizes",
                    pattern=r"(?P<item>\d+)",
                    capture_group="item",
                )
            ]
        )
    ).parse("Sizes: 42,44,46,48,50,52,54,56")

    assert parsed.fields["quantity"].value == "8"


@pytest.mark.parametrize(
    ("text", "expected_sizes", "expected_quantity"),
    [
        ("S1 M2 L2 XL1", "S M L XL", "6"),
        ("XS2 S3 M1", "XS S M", "6"),
    ],
)
def test_sum_size_counts_is_generic_and_preserves_input_order(
    text: str, expected_sizes: str, expected_quantity: str
) -> None:
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[
                ComputedRule(
                    name="supplier-size-count-pairs",
                    target_field="quantity",
                    secondary_target_field="sizes",
                    operation="sum_size_counts",
                    pattern=r"(?P<size>[A-Z]+)(?P<count>\d+)",
                    capture_group="size",
                    value_group="count",
                    separator=" ",
                    authoritative=True,
                )
            ]
        )
    ).parse(text)

    assert parsed.fields["sizes"].value == expected_sizes
    assert parsed.fields["quantity"].value == expected_quantity
    assert parsed.fields["sizes"].provenance is Provenance.COMPUTED_RULE
    assert parsed.fields["quantity"].provenance is Provenance.COMPUTED_RULE


def test_sum_matches_and_unique_matches_are_generic_primitives() -> None:
    profile = SourceProfileData(
        computed_rules=[
            ComputedRule(
                name="unique-codes",
                target_field="sizes",
                operation="unique_matches",
                pattern=r"(?P<label>[A-Z]+):(?P<units>\d+)",
                capture_group="label",
                separator=" / ",
            ),
            ComputedRule(
                name="sum-units",
                target_field="quantity",
                operation="sum_matches",
                pattern=r"(?P<label>[A-Z]+):(?P<units>\d+)",
                value_group="units",
            ),
        ]
    )

    parsed = RuleEngine(profile).parse("AA:2 BB:3 AA:1")

    assert parsed.fields["sizes"].value == "AA / BB"
    assert parsed.fields["quantity"].value == "6"


@pytest.mark.parametrize(
    ("policy", "status", "value"),
    [
        ("explicit", FieldStatus.FOUND, "8"),
        ("computed", FieldStatus.FOUND, "6"),
        ("ambiguous", FieldStatus.AMBIGUOUS, None),
    ],
)
def test_computed_rule_conflict_policy_is_explicit(
    policy: str, status: FieldStatus, value: str | None
) -> None:
    parsed = RuleEngine(
        SourceProfileData(
            computed_rules=[
                ComputedRule(
                    name=f"conflict-{policy}",
                    target_field="quantity",
                    secondary_target_field="sizes",
                    operation="sum_size_counts",
                    pattern=r"(?P<size>[A-Z]+)(?P<count>\d+)",
                    conflict_policy=policy,
                )
            ]
        )
    ).parse("S1 M2 L2 XL1\nQuantity: 8")

    assert parsed.fields["quantity"].status is status
    assert parsed.fields["quantity"].value == value


def test_computed_rule_is_isolated_to_its_source_profile() -> None:
    configured = SourceProfileData(
        computed_rules=[
            ComputedRule(
                name="source-a-only",
                target_field="quantity",
                operation="count_items",
                source_field="sizes",
                pattern=r"(?P<item>\d+)",
                capture_group="item",
            )
        ]
    )
    text = "Sizes: 36 37 38 39 40"

    assert RuleEngine(configured).parse(text).fields["quantity"].value == "5"
    assert RuleEngine(SourceProfileData()).parse(text).fields["quantity"].status is FieldStatus.MISSING


@pytest.mark.parametrize(
    "text",
    ["Sizes: S M L XL / 4 шт", "4 pcs", "4 adet"],
)
def test_explicit_quantity_is_extracted(text: str) -> None:
    parsed = RuleEngine(SourceProfileData()).parse(text)
    assert parsed.fields["quantity"].value == "4"


def test_article_is_not_invented() -> None:
    parsed = RuleEngine(SourceProfileData()).parse("Beautiful cotton dress")
    assert parsed.fields["article"].status is FieldStatus.MISSING


def test_cleaning_removes_supplier_contacts_but_preserves_original() -> None:
    p = SourceProfileData(
        remove_strings=["Supplier One"], cleaning_patterns=[r"\+90\s*555\s*000\s*00\s*00"]
    )
    parsed = RuleEngine(p).parse("Dress\nSupplier One\n+90 555 000 00 00")
    assert "Supplier One" in parsed.original_text
    assert "+90 555 000 00 00" in parsed.original_text
    assert "Supplier One" not in parsed.cleaned_text
    assert "+90 555 000 00 00" not in parsed.cleaned_text


def test_typed_cleaning_rules_run_before_strict_extraction() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            cleaning_rules=[
                CleaningRule(kind="exact", value="Price: 99$"),
                CleaningRule(kind="contains", value="REMOVE-SIZES"),
                CleaningRule(kind="regex", value=r"(?im)^Quantity:\s*99$"),
            ]
        )
    ).parse(
        "Price: 99$\nSizes: XL REMOVE-SIZES\nQuantity: 99\n"
        "Price: 10$\nSizes: S M\nQuantity: 2"
    )

    assert parsed.fields["price"].value == "10"
    assert parsed.fields["sizes"].value == "S M"
    assert parsed.fields["quantity"].value == "2"
    assert "Price: 99$" not in parsed.cleaned_text
    assert "REMOVE-SIZES" not in parsed.cleaned_text
    assert "Quantity: 99" not in parsed.cleaned_text
    assert "Price: 99$" in parsed.original_text


def test_disabled_typed_cleaning_rule_does_not_change_extraction() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            cleaning_rules=[
                CleaningRule(kind="exact", value="Price: 99$", enabled=False),
            ]
        )
    ).parse("Price: 99$\nPrice: 10$")

    assert parsed.fields["price"].status is FieldStatus.AMBIGUOUS


def test_cleaning_removes_contact_variants_and_source_blacklist_but_keeps_numeric_sku() -> None:
    parsed = RuleEngine(SourceProfileData(remove_lines=["BUY NOW", "123 Main Street"])).parse(
        "Dress\nSKU 12345678\n@wholesale\nt.me/wholesale\nwa.me/905550001122\n"
        "+90 555 000 11 22\nWhatsApp: +90 555 000 11 22\n"
        "Address: 55 Supplier Road\nBUY NOW\n123 Main Street"
    )
    for unwanted in [
        "@wholesale",
        "t.me/wholesale",
        "wa.me/905550001122",
        "+90 555 000 11 22",
        "WhatsApp:",
        "Address:",
        "BUY NOW",
        "123 Main Street",
    ]:
        assert unwanted not in parsed.cleaned_text
    assert "SKU 12345678" in parsed.cleaned_text


def test_natural_turkish_fabric_word_is_not_a_material_label() -> None:
    text = (
        "9834-ithal show saten ve saçaklı dantel kumaş bel balen detaylı "
        "göğüs gloplu agraflı mini balon etekli korse elbise fiyat 13.5$"
    )

    parsed = RuleEngine(SourceProfileData()).parse(text)

    assert parsed.fields["material"].status is FieldStatus.MISSING


def test_source_profiles_are_isolated_for_one_hundred_sources() -> None:
    for number in range(100):
        marker = f"P{number}:"
        p = profile(
            article=[ExtractionRule(kind="regex", pattern=rf"{marker}(?P<value>[A-Z0-9]+)")]
        )
        own = RuleEngine(p).parse(f"{marker}ABC")
        other = RuleEngine(p).parse(f"P{(number + 1) % 100}:ABC")
        assert own.fields["article"].value == "ABC"
        assert other.fields["article"].status is FieldStatus.MISSING


def test_profile_can_add_supplier_specific_characteristic_without_python_change() -> None:
    p = profile(
        waterproof=[ExtractionRule(kind="regex", pattern=r"Waterproof:\s*(?P<value>[^\n]+)")]
    )
    parsed = RuleEngine(p).parse("Waterproof: 10K")
    assert parsed.fields["waterproof"].value == "10K"


def test_three_radically_different_profiles_parse_only_their_own_rule() -> None:
    text = "A#ZX-9\nB model ZX-9\nC=>ZX-9"
    profiles = [
        profile(article=[ExtractionRule(kind="regex", pattern=r"A#(?P<value>\S+)")]),
        profile(article=[ExtractionRule(kind="label", pattern="B model")]),
        profile(article=[ExtractionRule(kind="regex", pattern=r"C=>(?P<value>\S+)")]),
    ]
    assert [RuleEngine(item).parse(text).fields["article"].evidence for item in profiles] == [
        "A#ZX-9",
        "B model ZX-9",
        "C=>ZX-9",
    ]


@pytest.mark.parametrize(
    ("text", "currency"),
    [("Price: 10 EUR", "EUR"), ("Price: 10 USD", "USD"), ("€5", "EUR"), ("$5", "USD")],
)
def test_currency_spellings_and_symbols_are_preserved(text: str, currency: str) -> None:
    parsed = RuleEngine(SourceProfileData()).parse(text)
    assert parsed.fields["price"].metadata["currency"] == currency


def test_labeled_symbol_price_is_one_unambiguous_value() -> None:
    parsed = RuleEngine(SourceProfileData()).parse("Цена: $10")

    assert parsed.fields["price"].status is FieldStatus.FOUND
    assert parsed.fields["price"].value == "10"
    assert parsed.fields["price"].metadata["currency"] == "USD"


def test_multiple_different_currency_values_are_ambiguous() -> None:
    parsed = RuleEngine(SourceProfileData()).parse("Price: 10 EUR\nPrice: 12 USD")
    assert parsed.fields["price"].status is FieldStatus.AMBIGUOUS


def test_equal_amounts_in_different_currencies_are_ambiguous() -> None:
    parsed = RuleEngine(SourceProfileData()).parse("Price: 10 EUR\nPrice: 10 USD")
    assert parsed.fields["price"].status is FieldStatus.AMBIGUOUS


def test_configured_series_rule_extracts_sizes_and_sums_explicit_counts() -> None:
    series_pattern = r"(?i)\bSeries:\s*(?P<value>(?:[A-Z]+\s*\d+\s*)+)"
    parsed = RuleEngine(
        profile(
            sizes=[
                ExtractionRule(
                    kind="regex",
                    pattern=series_pattern,
                    transform="series_sizes",
                    authoritative=True,
                )
            ],
            quantity=[
                ExtractionRule(
                    kind="regex",
                    pattern=series_pattern,
                    transform="series_quantity",
                    authoritative=True,
                )
            ],
        )
    ).parse("Ff Series: S2 M2 L2")

    assert parsed.fields["sizes"].value == "S, M, L"
    assert parsed.fields["quantity"].value == "6"
    assert parsed.fields["sizes"].provenance is Provenance.SOURCE_RULE
    assert parsed.fields["quantity"].metadata["authoritative"] is True


def test_configured_rules_parse_turkish_standard_series_without_python_branch() -> None:
    parsed = RuleEngine(
        profile(
            sizes=[
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?i)\bSeri\s+\d+\s+li\s+(?P<value>Standart)\s+Beden",
                    transform="map",
                    value_map={"Standart": "Standard"},
                )
            ],
            quantity=[
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?i)\bSeri\s+(?P<value>\d+)\s+li\b",
                )
            ],
        )
    ).parse("Yeni model — Seri 5 li Standart Beden")

    assert parsed.fields["sizes"].value == "Standard"
    assert parsed.fields["quantity"].value == "5"


def test_numeric_size_series_uses_configured_mapping_and_explicit_quantity_fallback() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            extraction_rules={
                "sizes": [
                    ExtractionRule(
                        kind="regex",
                        pattern=r"(?i)Series\s*:\s*(?P<value>\d+(?:/\d+)+)",
                        transform="map",
                        value_map={
                            "38/40/42/44/46/48": "38, 40, 42, 44, 46, 48",
                        },
                        authoritative=True,
                    )
                ]
            },
            fallback_rules={
                "quantity": {
                    "value": "6",
                    "authoritative": True,
                }
            },
        )
    ).parse("Series : 38/40/42/44/46/48")

    assert parsed.fields["sizes"].value == "38, 40, 42, 44, 46, 48"
    assert parsed.fields["sizes"].metadata["authoritative"] is True
    assert parsed.fields["quantity"].value == "6"
    assert parsed.fields["quantity"].metadata["fallback"] is True


def test_explicit_source_fallbacks_apply_only_to_missing_fields() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            fallback_rules={
                "sizes": {"value": "S, M, L", "authoritative": True},
                "quantity": {"value": "6", "authoritative": True},
            }
        )
    ).parse("Lastpoint basic dress\nPrice: 20 USD")

    assert parsed.fields["sizes"].value == "S, M, L"
    assert parsed.fields["quantity"].value == "6"
    assert parsed.fields["sizes"].evidence == "configured source fallback: sizes"
    assert parsed.fields["sizes"].provenance is Provenance.SOURCE_RULE


def test_source_fallback_never_overwrites_ambiguous_evidence() -> None:
    parsed = RuleEngine(
        SourceProfileData(
            extraction_rules={
                "sizes": [
                    ExtractionRule(
                        kind="regex",
                        pattern=r"Sizes:\s*(?P<value>[^\n]+)",
                    )
                ]
            },
            fallback_rules={"sizes": {"value": "S, M, L"}},
        )
    ).parse("Sizes: S\nSizes: M")

    assert parsed.fields["sizes"].status is FieldStatus.AMBIGUOUS


def test_group_fallback_applies_only_when_every_configured_field_is_missing() -> None:
    profile_with_group = SourceProfileData(
        fallback_rules={
            "sizes": {
                "value": "S, M, L",
                "when_all_missing": ["sizes", "quantity"],
            },
            "quantity": {
                "value": "6",
                "when_all_missing": ["sizes", "quantity"],
            },
        }
    )

    fully_missing = RuleEngine(profile_with_group).parse("Lastpoint dress\nPrice: 20 USD")
    explicit_sizes = RuleEngine(profile_with_group).parse("Sizes: XL")

    assert fully_missing.fields["sizes"].value == "S, M, L"
    assert fully_missing.fields["quantity"].value == "6"
    assert explicit_sizes.fields["sizes"].value == "XL"
    assert explicit_sizes.fields["quantity"].status is FieldStatus.MISSING


def test_lower_numeric_priority_wins_and_later_rules_are_fallbacks() -> None:
    parsed = RuleEngine(
        profile(
            sizes=[
                ExtractionRule(
                    kind="regex",
                    pattern=r"Primary:\s*(?P<value>[^\n]+)",
                    priority=10,
                ),
                ExtractionRule(
                    kind="regex",
                    pattern=r"Secondary:\s*(?P<value>[^\n]+)",
                    priority=20,
                ),
            ]
        )
    ).parse("Primary: S\nSecondary: XL")

    assert parsed.fields["sizes"].value == "S"


def test_series_transform_requires_named_size_and_count_groups() -> None:
    with pytest.raises(ValidationError, match="size.*count"):
        ExtractionRule(
            kind="regex",
            pattern=r"Series:\s*(?P<value>[^\n]+)",
            transform="series_quantity",
            token_pattern=r"(?P<label>[A-Z]+)(?P<number>\d+)",
        )


def test_series_transform_rejects_unparsed_text_inside_captured_series() -> None:
    rule = ExtractionRule(
        kind="regex",
        pattern=r"Series:\s*(?P<value>[^\n]+)",
        transform="series_quantity",
    )

    parsed = RuleEngine(profile(quantity=[rule])).parse("Series: S2 unknown M2")

    assert parsed.fields["quantity"].status is FieldStatus.MISSING


def test_series_rules_and_fallbacks_remain_isolated_between_sources() -> None:
    text = "Series: S2 M2 L2\nPrice: 20 USD"
    series = SourceProfileData(
        extraction_rules={
            "sizes": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"Series:\s*(?P<value>[^\n]+)",
                    transform="series_sizes",
                )
            ]
        }
    )
    fallback = SourceProfileData(fallback_rules={"sizes": {"value": "Standard"}})
    untouched = SourceProfileData(extraction_rules={"sizes": []})

    assert RuleEngine(series).parse(text).fields["sizes"].value == "S, M, L"
    assert RuleEngine(fallback).parse(text).fields["sizes"].value == "Standard"
    assert RuleEngine(untouched).parse(text).fields["sizes"].status is FieldStatus.MISSING


def test_repeated_pattern_maps_and_aggregates_multiple_fields_without_source_code() -> None:
    configured = SourceProfileData(
        repeated_pattern_rules=[
            RepeatedPatternRule(
                name="range-and-variant-price",
                pattern=r"(?m)^(?P<size>\d+-\d+)-(?P<price>\d+)$",
                fields={"sizes": "size", "price": "price"},
                aggregation={"sizes": "unique", "price": "join_unique"},
                separators={"price": "/"},
                authoritative=True,
            )
        ],
        default_currency="USD",
    )

    parsed = RuleEngine(configured).parse("42-48-27\n42-48-30")

    assert parsed.fields["sizes"].value == "42-48"
    assert parsed.fields["price"].value == "27/30"
    assert parsed.fields["sizes"].provenance is Provenance.SOURCE_RULE
    assert parsed.fields["price"].metadata["variant_values"] == ["27", "30"]


def test_repeated_pattern_ambiguous_policy_never_selects_conflicting_candidate() -> None:
    configured = SourceProfileData(
        repeated_pattern_rules=[
            RepeatedPatternRule(
                name="ambiguous-article",
                pattern=r"(?m)^SKU=(?P<sku>\w+)$",
                fields={"article": "sku"},
                aggregation={"article": "ambiguous"},
            )
        ]
    )
    parsed = RuleEngine(configured).parse("SKU=A1\nSKU=B2")
    assert parsed.fields["article"].status is FieldStatus.AMBIGUOUS
    assert parsed.fields["article"].value is None
