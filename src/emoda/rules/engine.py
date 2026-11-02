from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from itertools import groupby
from typing import Any, cast

from emoda.domain.enums import FieldStatus, Provenance
from emoda.domain.schemas import (
    ComputedRule,
    ExtractedField,
    ExtractionRule,
    LocalizedLabel,
    MultiPriceRule,
    ParsedProduct,
    ProductFieldName,
    SourceIgnoreRule,
    SourceProfileData,
)
from emoda.rules.currency import CurrencyAliasMatch, CurrencyAliasResolver
from emoda.rules.numbers import normalize_number
from emoda.services.sanitizer import FinalSanitizer

FIELDS = (
    "product_name",
    "price",
    "sizes",
    "quantity",
    "article",
    "model",
    "color",
    "brand",
    "material",
    "composition",
    "characteristics",
)

DEFAULT_RULES: dict[str, list[ExtractionRule]] = {
    "product_name": [
        ExtractionRule(
            kind="regex",
            pattern=r"(?im)^[ \t]*(?:product[ _]name|название товара|ürün adı)[ \t]*:[ \t]*(?P<value>[^\r\n]{1,200})$",
        )
    ],
    "price": [
        ExtractionRule(
            kind="regex",
            pattern=r"(?im)(?:price|цена|fiyat)\s*[:\-]?\s*(?P<currency_before>\$|€)?\s*(?P<value>\d+(?:[.,]\d+)?)\s*(?P<currency_after>USD|EUR|\$|€)?",
        ),
        ExtractionRule(
            kind="regex", pattern=r"(?im)(?P<currency>\$|€)\s*(?P<value>\d+(?:[.,]\d+)?)"
        ),
    ],
    "sizes": [
        ExtractionRule(
            kind="regex",
            pattern=r"(?im)(?:sizes?|размеры?|beden(?:ler)?)\s*[:\-]?\s*(?P<value>(?:(?:Standart|Oversize|XXL|XXS|STD|XL|XS|S|M|L)(?:\s*[-/ ]\s*(?:Standart|Oversize|XXL|XXS|STD|XL|XS|S|M|L))*|\d+(?:\s*beden)?(?:\s*[-/ ]\s*\d+(?:\s*beden)?)*))",
        ),
        ExtractionRule(
            kind="regex",
            pattern=r"(?im)^\s*(?P<value>(?:Standart|Oversize|XXL|XXS|STD|XL|XS|S|M|L)(?:\s*[-/ ]\s*(?:Standart|Oversize|XXL|XXS|STD|XL|XS|S|M|L))*)\s*$",
        ),
    ],
    "quantity": [
        ExtractionRule(
            kind="regex",
            pattern=r"(?im)(?:quantity|количество|qty|упаковка|pack(?:age)?|adet)\s*[:\-]?\s*(?P<value>\d+)\s*(?:шт\.?|pcs?\b|adet\b)?|(?P<value_alt>\d+)\s*(?:шт\.?|pcs?\b|adet\b)",
        )
    ],
    "article": [
        ExtractionRule(
            kind="regex",
            pattern=r"(?im)(?:article|артикул|product\s*code|sku|ref\.?)\s*[:#\-]?\s*(?P<value>[A-ZА-Я0-9][A-ZА-Я0-9._/-]*)",
        )
    ],
    "model": [
        ExtractionRule(
            kind="regex",
            pattern=r"(?im)(?:model(?:\s*no)?|модель)\s*[:#\-]?\s*(?P<value>[A-ZА-Я0-9][A-ZА-Я0-9._/-]*)",
        )
    ],
    "color": [
        ExtractionRule(
            kind="regex", pattern=r"(?im)(?:color|цвет|renk)\s*[:\-]\s*(?P<value>[^\n]+)"
        )
    ],
    "brand": [
        ExtractionRule(
            kind="regex", pattern=r"(?im)(?:brand|бренд|marka)\s*[:\-]\s*(?P<value>[^\n]+)"
        )
    ],
    "material": [
        ExtractionRule(
            kind="regex", pattern=r"(?im)(?:material|материал|kumaş)\s*[:\-]\s*(?P<value>[^\n]+)"
        )
    ],
    "composition": [
        ExtractionRule(
            kind="regex", pattern=r"(?im)(?:composition|состав|içerik)\s*[:\-]\s*(?P<value>[^\n]+)"
        )
    ],
}

BUILTIN_ADVERTISING_PATTERNS = (
    r"(?im)^\s*#(?:реклама|advertisement|sponsored)\b",
    r"(?im)^\s*(?:на\s+правах\s+рекламы|рекламная\s+интеграция|sponsored\s+post|advertisement)\s*[:.!-]?\s*$",
)


@dataclass(frozen=True)
class _ComputedCandidate:
    target: str
    status: FieldStatus
    value: str | None
    evidence: str
    metadata: dict[str, Any]


def _safe_passthrough_text(value: str) -> str:
    """Remove only transport-unsafe controls; do not rewrite field semantics."""

    return "".join(
        character for character in value if character in {"\n", "\r", "\t"} or ord(character) >= 32
    )


def process_field_candidate(
    profile: SourceProfileData,
    field: str,
    candidate: ExtractedField,
) -> ExtractedField:
    """Apply only the configured field policy to an already bounded candidate."""

    policy = profile.field_policies.get(cast(ProductFieldName, field))
    if policy is None or policy.processing_mode == "normal":
        return candidate
    metadata = {
        **candidate.metadata,
        "processing_mode": "passthrough",
        "normalization_bypassed": True,
        "semantic_validation_bypassed": True,
    }
    if candidate.status is not FieldStatus.FOUND or candidate.value is None:
        return candidate.model_copy(update={"metadata": metadata})
    value = _safe_passthrough_text(candidate.value)
    if not value:
        return ExtractedField(
            status=FieldStatus.MISSING,
            metadata={"processing_mode": "passthrough"},
        )
    return candidate.model_copy(update={"value": value, "metadata": metadata})


@dataclass(frozen=True)
class MultiPriceCandidate:
    label_original: str
    label_ru: str
    label_en: str
    value: str
    currency: str | None
    evidence: str


def advertisement_reason(text: str, profile: SourceProfileData) -> str | None:
    if not profile.advertising_filter_enabled or not text.strip():
        return None
    if any(re.search(pattern, text) for pattern in profile.advertising_patterns):
        return "source-pattern"
    if any(re.search(pattern, text) for pattern in BUILTIN_ADVERTISING_PATTERNS):
        return "builtin"
    return None


def extract_multi_price_candidates(
    text: str,
    rules: list[MultiPriceRule],
    currency_aliases: CurrencyAliasResolver | None = None,
) -> tuple[list[MultiPriceCandidate], list[str]]:
    resolver = currency_aliases or CurrencyAliasResolver.system_defaults()
    for rule in sorted((item for item in rules if item.enabled), key=lambda item: item.priority):
        flags = re.IGNORECASE if rule.ignore_case else 0
        label_map = {
            (label.casefold() if rule.ignore_case else label): translated
            for label, translated in rule.labels.items()
        }
        candidates: list[MultiPriceCandidate] = []
        errors: list[str] = []
        seen: dict[str, tuple[str, str | None]] = {}
        for match in re.finditer(rule.pattern, text, flags):
            groups = match.groupdict()
            original_label = groups["label"].strip()
            lookup = original_label.casefold() if rule.ignore_case else original_label
            translated = label_map.get(lookup)
            if rule.label_mode == "dynamic" and original_label:
                translated = LocalizedLabel(ru=original_label, en=original_label)
            if translated is None:
                errors.append(f"multi-price label is not mapped: {original_label}")
                continue
            value = groups["value"].replace(",", ".")
            raw_currency = (
                groups.get("currency")
                or groups.get("currency_before")
                or groups.get("currency_after")
            )
            resolved = resolver.resolve(raw_currency)
            currency = resolved.currency if resolved is not None else rule.default_currency
            identity = (value, currency)
            if lookup in seen and seen[lookup] != identity:
                errors.append(f"conflicting multi-price values for: {original_label}")
                continue
            if lookup in seen:
                continue
            seen[lookup] = identity
            candidates.append(
                MultiPriceCandidate(
                    label_original=original_label,
                    label_ru=translated.ru,
                    label_en=translated.en,
                    value=value,
                    currency=currency,
                    evidence=match.group(0),
                )
            )
        if candidates or errors:
            if len(candidates) < 2:
                errors.append("multi-price rule requires at least two distinct pairs")
            return candidates, errors
    return [], []


def match_structural_ignore_rule(
    rules: list[SourceIgnoreRule],
    *,
    grouped_id: int | None,
    media_type: str | None,
    media_count: int,
    message_count: int,
) -> SourceIgnoreRule | None:
    """Pure transport filters run before collection and never parse/OCR text."""
    for rule in rules:
        if not rule.enabled or not rule.has_structure():
            continue
        if rule.found_fields or rule.missing_fields or rule.description_missing_required:
            continue
        if rule.media_required and not media_count:
            continue
        if rule.grouped_id == "present" and grouped_id is None:
            continue
        if rule.grouped_id == "missing" and grouped_id is not None:
            continue
        if rule.media_types and (media_type or "none") not in rule.media_types:
            continue
        if any(
            low is not None and count < low or high is not None and count > high
            for count, low, high in (
                (media_count, rule.min_media, rule.max_media),
                (message_count, rule.min_messages, rule.max_messages),
            )
        ):
            continue
        return rule
    return None


def match_source_ignore_rule(
    parsed: ParsedProduct,
    *,
    has_media: bool,
    media_types: Iterable[str] | None = None,
    rules: list[SourceIgnoreRule],
) -> SourceIgnoreRule | None:
    product_media_types = tuple(media_types or (() if has_media else ("none",)))
    for rule in rules:
        if not rule.enabled or rule.has_structure():
            continue
        if rule.media_required and not has_media:
            continue
        if rule.media_types and (
            not product_media_types
            or any(media_type not in rule.media_types for media_type in product_media_types)
        ):
            continue
        if not all(
            parsed.fields.get(name) is not None
            and parsed.fields[name].status is FieldStatus.FOUND
            and parsed.fields[name].metadata.get("fallback") is not True
            for name in rule.found_fields
        ):
            continue
        if not all(
            parsed.fields.get(name) is None
            or parsed.fields[name].status is FieldStatus.MISSING
            or parsed.fields[name].metadata.get("fallback") is True
            for name in rule.missing_fields
        ):
            continue
        if rule.description_missing_required and _unstructured_description(parsed):
            continue
        return rule
    return None


def _unstructured_description(parsed: ParsedProduct) -> str:
    remaining = parsed.cleaned_text
    for field in parsed.fields.values():
        if field.status is FieldStatus.FOUND and field.evidence:
            remaining = re.sub(re.escape(field.evidence), "", remaining, flags=re.IGNORECASE)
    return "\n".join(line.strip(" /|:-") for line in remaining.splitlines() if line.strip(" /|:-"))


class RuleEngine:
    def __init__(
        self,
        profile: SourceProfileData,
        currency_aliases: CurrencyAliasResolver | None = None,
    ) -> None:
        self.profile = profile
        self.currency_aliases = currency_aliases or CurrencyAliasResolver.system_defaults()

    def clean_source_text(self, text: str) -> str:
        return FinalSanitizer.apply_source_rules(text, self.profile.cleaning_rules)

    def resolve_routing(self, text: str) -> tuple[ExtractedField, int | None]:
        """Resolve only category/routing, without running any product field parser."""

        extraction_text = self.clean_source_text(text)
        category = self._category(extraction_text)
        return category, self._destination(extraction_text, category.value)

    def parse(self, text: str) -> ParsedProduct:
        extraction_text = self.clean_source_text(text)
        cleaned_text = self._clean(extraction_text)
        names = dict.fromkeys(
            (
                *FIELDS,
                *self.profile.extraction_rules.keys(),
                *self.profile.fallback_rules.keys(),
                *(rule.target_field for rule in self.profile.computed_rules),
                *(
                    rule.secondary_target_field
                    for rule in self.profile.computed_rules
                    if rule.secondary_target_field is not None
                ),
                *(field for rule in self.profile.repeated_pattern_rules for field in rule.fields),
            )
        )
        fields = {name: self._extract(name, extraction_text) for name in names}
        self._apply_repeated_patterns(fields, extraction_text)
        self._apply_computed_rules(fields, extraction_text)
        pairs, pair_errors = (
            extract_multi_price_candidates(
                extraction_text, self.profile.multi_price_rules, self.currency_aliases
            )
            if self.profile.multi_price_rules
            else ([], [])
        )
        self._apply_fallbacks(fields, product_evidence=len(pairs) >= 2 and not pair_errors)
        fields = {
            name: process_field_candidate(self.profile, name, candidate)
            for name, candidate in fields.items()
        }
        # Explicit source configuration overrides extraction, computed rules,
        # fallbacks and field normalization, including an entirely empty caption.
        for name, value in self.profile.fixed_fields.items():
            fields[name] = ExtractedField(
                status=FieldStatus.FOUND,
                value=value,
                evidence=f"configured source constant: {name}",
                provenance=Provenance.SOURCE_RULE,
                metadata={"fixed": True, "authoritative": True, "rule_id": f"{name}:fixed"},
            )
        category = self._category(extraction_text)
        destination = self._destination(extraction_text, category.value)
        return ParsedProduct(
            original_text=text,
            cleaned_text=cleaned_text,
            fields=fields,
            category=category,
            destination_id=destination,
        )

    def _apply_computed_rules(self, fields: dict[str, ExtractedField], text: str) -> None:
        for rule in sorted(self.profile.computed_rules, key=lambda item: item.priority):
            if not rule.enabled:
                continue
            computed = self._compute_rule(rule, fields, text)
            for result in computed:
                candidate = ExtractedField(
                    status=result.status,
                    value=result.value,
                    evidence=result.evidence,
                    provenance=Provenance.COMPUTED_RULE,
                    metadata={
                        "computed_rule": rule.name,
                        "computed_operation": rule.operation,
                        "rule_id": rule.name,
                        "operation": rule.operation,
                        "computed_priority": rule.priority,
                        "source_field": rule.source_field,
                        "input_source": rule.resolved_input_source(),
                        "authoritative": rule.authoritative,
                        "conflict_policy": rule.conflict_policy,
                        **({"input_mode": rule.input_mode} if rule.input_source is None else {}),
                        **result.metadata,
                    },
                )
                self._merge_computed(fields, result.target, candidate, rule)

    @staticmethod
    def _decimal_text(value: Decimal) -> str:
        normalized = value.normalize()
        if normalized == normalized.to_integral_value():
            normalized = normalized.quantize(Decimal("1"))
        return format(normalized, "f")

    def _compute_rule(
        self,
        rule: ComputedRule,
        fields: dict[str, ExtractedField],
        text: str,
    ) -> list[_ComputedCandidate]:
        input_source = rule.resolved_input_source()
        source_text = text
        source_evidence = text.strip()
        if input_source in {"field", "raw_field_candidate"}:
            assert rule.source_field is not None
            source = fields.get(rule.source_field)
            if source is None or source.status is not FieldStatus.FOUND or not source.value:
                return []
            if input_source == "raw_field_candidate":
                raw_value = source.metadata.get("raw_value")
                if not isinstance(raw_value, str) or not raw_value.strip():
                    return []
                source_text = raw_value
                source_evidence = raw_value
            else:
                source_text = source.value
                source_evidence = source.evidence or source.value
        flags = re.IGNORECASE if rule.ignore_case else 0

        if rule.operation == "count_series_items":
            result = self._count_series_items(rule, source_text, source_evidence, flags)
            return [result] if result is not None else []

        if rule.operation == "count_items":
            if rule.pattern is not None:
                matches = list(re.finditer(rule.pattern, source_text, flags))
                values = [
                    match.group(rule.capture_group).strip()
                    for match in matches
                    if match.group(rule.capture_group) is not None
                    and match.group(rule.capture_group).strip()
                ]
            else:
                values = [item for item in re.split(r"[\s,;:/|+\-]+", source_text.strip()) if item]
            if not values:
                return []
            return [
                _ComputedCandidate(
                    target=rule.target_field,
                    status=FieldStatus.FOUND,
                    value=str(len(values)),
                    evidence=source_evidence,
                    metadata={
                        "input_source": input_source,
                        "pattern_matches": values,
                        "match_count": len(values),
                    },
                )
            ]

        assert rule.pattern is not None
        matches = list(re.finditer(rule.pattern, source_text, flags))
        if not matches:
            return []
        evidence = " | ".join(dict.fromkeys(match.group(0).strip() for match in matches))
        if rule.operation == "unique_matches":
            values = [
                match.group(rule.capture_group).strip()
                for match in matches
                if match.group(rule.capture_group) is not None
                and match.group(rule.capture_group).strip()
            ]
            unique_values = list(dict.fromkeys(values))
            if not unique_values:
                return []
            return [
                _ComputedCandidate(
                    target=rule.target_field,
                    status=FieldStatus.FOUND,
                    value=rule.separator.join(unique_values),
                    evidence=evidence,
                    metadata={
                        "input_source": input_source,
                        "pattern_matches": values,
                        "match_count": len(values),
                    },
                )
            ]

        if rule.operation == "sum_matches":
            try:
                numeric_values = [
                    Decimal(match.group(rule.value_group).strip()) for match in matches
                ]
            except (InvalidOperation, AttributeError):
                return []
            if not numeric_values:
                return []
            return [
                _ComputedCandidate(
                    target=rule.target_field,
                    status=FieldStatus.FOUND,
                    value=self._decimal_text(sum(numeric_values, Decimal("0"))),
                    evidence=evidence,
                    metadata={
                        "input_source": input_source,
                        "pattern_matches": [self._decimal_text(value) for value in numeric_values],
                        "match_count": len(numeric_values),
                    },
                )
            ]

        pairs: list[tuple[str, int]] = []
        try:
            for match in matches:
                label = match.group(rule.capture_group).strip()
                count = int(match.group(rule.value_group).strip())
                if not label or count <= 0:
                    return []
                pairs.append((label, count))
        except (AttributeError, TypeError, ValueError):
            return []
        if not pairs or rule.secondary_target_field is None:
            return []
        unique_values = list(dict.fromkeys(label for label, _ in pairs))
        return [
            _ComputedCandidate(
                target=rule.target_field,
                status=FieldStatus.FOUND,
                value=str(sum(count for _, count in pairs)),
                evidence=evidence,
                metadata={
                    "input_source": input_source,
                    "pattern_matches": [f"{label}:{count}" for label, count in pairs],
                    "match_count": len(pairs),
                },
            ),
            _ComputedCandidate(
                target=rule.secondary_target_field,
                status=FieldStatus.FOUND,
                value=rule.separator.join(unique_values),
                evidence=evidence,
                metadata={
                    "input_source": input_source,
                    "pattern_matches": [label for label, _ in pairs],
                    "match_count": len(pairs),
                },
            ),
        ]

    @classmethod
    def _count_series_items(
        cls,
        rule: ComputedRule,
        source_text: str,
        source_evidence: str,
        flags: int,
    ) -> _ComputedCandidate | None:
        assert rule.series_separator_pattern is not None
        series = [
            value.strip()
            for value in re.split(rule.series_separator_pattern, source_text, flags=flags)
            if value.strip()
        ]
        if not series:
            return None
        counts: list[int] = []
        for value in series:
            count = cls._count_one_series(rule, value, flags)
            if count is None:
                return None
            counts.append(count)
        metadata = {"series_counts": counts, "raw_source_value": source_text}
        distinct_counts = set(counts)
        if len(distinct_counts) != 1:
            return _ComputedCandidate(
                target=rule.target_field,
                status=FieldStatus.AMBIGUOUS,
                value=None,
                evidence=source_evidence,
                metadata=metadata,
            )
        return _ComputedCandidate(
            target=rule.target_field,
            status=FieldStatus.FOUND,
            value=str(counts[0]),
            evidence=source_evidence,
            metadata=metadata,
        )

    @classmethod
    def _count_one_series(cls, rule: ComputedRule, value: str, flags: int) -> int | None:
        for pattern in (rule.range_pattern, rule.compact_range_pattern):
            if pattern is None:
                continue
            match = re.fullmatch(pattern, value, flags=flags)
            if match is not None:
                return cls._numeric_range_count(rule, match)
        if rule.explicit_list_pattern is None or rule.explicit_separator_pattern is None:
            return None
        items = list(re.finditer(rule.explicit_list_pattern, value, flags))
        if not items:
            return None
        if not re.fullmatch(r"\s*", value[: items[0].start()]):
            return None
        if not re.fullmatch(r"\s*", value[items[-1].end() :]):
            return None
        for previous, current in zip(items, items[1:], strict=False):
            between = value[previous.end() : current.start()]
            if re.fullmatch(rule.explicit_separator_pattern, between, flags=flags) is None:
                return None
        explicit_values = [match.group(rule.explicit_item_group) for match in items]
        if any(item is None or not item.strip() for item in explicit_values):
            return None
        return len(explicit_values)

    @staticmethod
    def _numeric_range_count(rule: ComputedRule, match: re.Match[str]) -> int | None:
        assert rule.numeric_step is not None
        try:
            start = Decimal(match.group(rule.range_start_group).strip())
            end = Decimal(match.group(rule.range_end_group).strip())
        except (AttributeError, InvalidOperation):
            return None
        distance = end - start
        if distance < 0 or distance % rule.numeric_step != 0:
            return None
        count = distance / rule.numeric_step + 1
        if count != count.to_integral_value() or count <= 0:
            return None
        return int(count)

    @staticmethod
    def _merge_computed(
        fields: dict[str, ExtractedField],
        target: str,
        candidate: ExtractedField,
        rule: ComputedRule,
    ) -> None:
        current = fields.get(target, ExtractedField())
        if current.status is FieldStatus.MISSING:
            fields[target] = candidate
            return
        if (
            current.status is FieldStatus.FOUND
            and candidate.status is FieldStatus.FOUND
            and current.value == candidate.value
        ):
            fields[target] = current.model_copy(
                update={
                    "metadata": {
                        **current.metadata,
                        "computed_rule": rule.name,
                        "computed_operation": rule.operation,
                        "computed_matches_explicit": (
                            current.provenance is not Provenance.COMPUTED_RULE
                        ),
                    }
                }
            )
            return
        if current.provenance is Provenance.COMPUTED_RULE:
            current_priority = int(current.metadata.get("computed_priority", 100))
            if current_priority < rule.priority:
                return
            candidate_values = list(
                dict.fromkeys(
                    value
                    for value in (
                        *current.metadata.get("candidate_values", []),
                        current.value,
                        *candidate.metadata.get("candidate_values", []),
                        candidate.value,
                    )
                    if value is not None
                )
            )
            fields[target] = ExtractedField(
                status=FieldStatus.AMBIGUOUS,
                evidence=" | ".join(
                    dict.fromkeys(
                        value for value in (current.evidence, candidate.evidence) if value
                    )
                ),
                provenance=Provenance.COMPUTED_RULE,
                metadata={
                    "computed_rule": rule.name,
                    "computed_operation": rule.operation,
                    "rule_id": rule.name,
                    "operation": rule.operation,
                    "candidate_values": candidate_values,
                    **candidate.metadata,
                    "authoritative": False,
                    "conflict_policy": "ambiguous",
                },
            )
            return
        if rule.conflict_policy == "explicit":
            fields[target] = current.model_copy(
                update={
                    "metadata": {
                        **current.metadata,
                        "computed_conflict_rule": rule.name,
                        "computed_candidate": candidate.value,
                    }
                }
            )
            return
        if rule.conflict_policy == "computed":
            fields[target] = candidate
            return
        fields[target] = ExtractedField(
            status=FieldStatus.AMBIGUOUS,
            evidence=" | ".join(
                dict.fromkeys(value for value in (current.evidence, candidate.evidence) if value)
            ),
            provenance=Provenance.COMPUTED_RULE,
            metadata={
                "computed_rule": rule.name,
                "computed_operation": rule.operation,
                "rule_id": rule.name,
                "operation": rule.operation,
                "candidate_values": [current.value, candidate.value],
                **candidate.metadata,
                "authoritative": False,
                "conflict_policy": "ambiguous",
            },
        )

    def _apply_repeated_patterns(self, fields: dict[str, ExtractedField], text: str) -> None:
        for rule in sorted(self.profile.repeated_pattern_rules, key=lambda item: item.priority):
            if not rule.enabled:
                continue
            flags = re.IGNORECASE if rule.ignore_case else 0
            matches = list(re.finditer(rule.pattern, text, flags))
            if not matches:
                continue
            for field, capture in rule.fields.items():
                candidates = [
                    (match.group(capture).strip(), match.group(0).strip())
                    for match in matches
                    if match.group(capture) is not None and match.group(capture).strip()
                ]
                if not candidates:
                    continue
                unique_values = list(dict.fromkeys(value for value, _ in candidates))
                policy = rule.aggregation.get(field, "unique")
                evidence = " | ".join(dict.fromkeys(value for _, value in candidates))
                if policy in {"unique", "ambiguous"} and len(unique_values) > 1:
                    candidate = ExtractedField(
                        status=FieldStatus.AMBIGUOUS,
                        evidence=evidence,
                        provenance=Provenance.SOURCE_RULE,
                        metadata={
                            "rule": rule.name,
                            "candidate_values": unique_values,
                            "authoritative": rule.authoritative,
                        },
                    )
                else:
                    value = (
                        rule.separators.get(field, "/").join(unique_values)
                        if policy == "join_unique"
                        else unique_values[0]
                    )
                    metadata: dict[str, Any] = {
                        "rule": rule.name,
                        "rule_id": rule.name,
                        "raw_value": value,
                        "aggregation": policy,
                        "authoritative": rule.authoritative,
                    }
                    if policy == "join_unique":
                        metadata["variant_values"] = unique_values
                    candidate = ExtractedField(
                        status=FieldStatus.FOUND,
                        value=value,
                        evidence=evidence,
                        provenance=Provenance.SOURCE_RULE,
                        metadata=metadata,
                    )
                if field == "price" and candidate.status is FieldStatus.FOUND:
                    currency_matches = [
                        self._currency_from_match_context(match, text)
                        for match in matches
                        if match.group(capture) is not None and match.group(capture).strip()
                    ]
                    currencies = {
                        matched.currency if matched else self.profile.default_currency
                        for matched in currency_matches
                    }
                    # Equal numbers in different (or partly unknown) units are
                    # not the same price. Never let join_unique hide that conflict.
                    if len(currencies) > 1:
                        candidate = candidate.model_copy(
                            update={
                                "status": FieldStatus.AMBIGUOUS,
                                "value": None,
                                "metadata": {
                                    **candidate.metadata,
                                    "currency_candidates": sorted(currencies, key=str),
                                },
                            }
                        )
                    elif currencies != {None}:
                        matched_alias = next((m for m in currency_matches if m), None)
                        candidate.metadata["currency"] = next(iter(currencies))
                        candidate.metadata["currency_source"] = (
                            "alias" if matched_alias else "profile_default"
                        )
                        if matched_alias:
                            candidate.metadata["matched_currency_alias"] = matched_alias.alias
                current = fields.get(field, ExtractedField())
                if current.status is FieldStatus.MISSING or rule.authoritative:
                    fields[field] = candidate
                elif (
                    current.status is FieldStatus.FOUND
                    and candidate.status is FieldStatus.FOUND
                    and current.value == candidate.value
                    and (
                        field != "price"
                        or current.metadata.get("currency") == candidate.metadata.get("currency")
                    )
                ):
                    fields[field] = current.model_copy(
                        update={"metadata": {**current.metadata, **candidate.metadata}}
                    )
                elif current != candidate:
                    fields[field] = ExtractedField(
                        status=FieldStatus.AMBIGUOUS,
                        evidence=" | ".join(
                            dict.fromkeys(
                                value for value in (current.evidence, candidate.evidence) if value
                            )
                        ),
                        provenance=Provenance.SOURCE_RULE,
                        metadata={"rule": rule.name, "authoritative": False},
                    )

    def route(self, text: str, category: str | None) -> int | None:
        return self._destination(text, category)

    def _extract(self, field: str, text: str) -> ExtractedField:
        configured = self.profile.extraction_rules.get(field)
        configured_rules = [rule for rule in (configured or []) if rule.enabled]
        default_rules = [rule for rule in DEFAULT_RULES.get(field, []) if rule.enabled]
        candidates: list[tuple[str, str, ExtractionRule, dict[str, Any]]] = []
        used_configured = False
        rule_sets: list[tuple[list[ExtractionRule], bool]]
        if configured is None:
            rule_sets = [(default_rules, False)]
        elif configured_rules:
            # Source rules have first refusal. If every enabled source rule is a
            # non-match, retain the generic strict parser before asking Gemini.
            rule_sets = [(configured_rules, True), (default_rules, False)]
        else:
            # An explicit empty or fully disabled field is an administrator opt-out.
            rule_sets = []
        for rules, source_configured in rule_sets:
            for _, priority_rules in groupby(
                sorted(rules, key=lambda item: item.priority), key=lambda item: item.priority
            ):
                same_priority: list[tuple[str, str, ExtractionRule, dict[str, Any]]] = []
                for rule in priority_rules:
                    for value, evidence, metadata in self._apply_rule(rule, text, field=field):
                        normalized, normalized_metadata = self._normalize_candidate(
                            field, value, metadata
                        )
                        same_priority.append((normalized, evidence, rule, normalized_metadata))
                if same_priority:
                    candidates = same_priority
                    used_configured = source_configured
                    break
            if candidates:
                break
        if not candidates:
            return ExtractedField()

        def identity(candidate: tuple[str, str, ExtractionRule, dict[str, Any]]) -> object:
            value, _, _, metadata = candidate
            if field != "price":
                return value
            return value, metadata.get("currency")

        distinct = {identity(candidate) for candidate in candidates}
        if len(distinct) > 1:
            return ExtractedField(
                status=FieldStatus.AMBIGUOUS,
                evidence=" | ".join(candidate[1] for candidate in candidates[:5]),
                provenance=(Provenance.SOURCE_RULE if used_configured else Provenance.STRICT_TEXT),
            )
        value, evidence, selected_rule, metadata = candidates[0]
        metadata = dict(metadata)
        metadata["authoritative"] = any(
            candidate[2].authoritative
            for candidate in candidates
            if identity(candidate) == identity(candidates[0])
        )
        metadata["transform"] = candidates[0][2].transform
        selected_identity = identity(candidates[0])
        for candidate in candidates[1:]:
            candidate_metadata = candidate[3]
            if identity(candidate) == selected_identity:
                for key, candidate_metadata_value in candidate_metadata.items():
                    metadata.setdefault(key, candidate_metadata_value)
        if used_configured:
            configured_index = next(
                (
                    index
                    for index, configured_rule in enumerate(configured or [])
                    if configured_rule is selected_rule
                ),
                None,
            )
            if configured_index is not None:
                metadata["rule_id"] = f"{field}:{configured_index + 1}"
                metadata["rule_priority"] = selected_rule.priority
        return ExtractedField(
            status=FieldStatus.FOUND,
            value=value.strip(),
            evidence=evidence.strip(),
            provenance=(Provenance.SOURCE_RULE if used_configured else Provenance.STRICT_TEXT),
            metadata=metadata,
        )

    def _normalize_candidate(
        self,
        field: str,
        value: str,
        metadata: dict[str, Any],
    ) -> tuple[str, dict[str, Any]]:
        """Apply the same post-transform normalization in parsing and tester."""

        normalized_metadata = dict(metadata)
        if field != "price":
            return value, normalized_metadata
        normalized_value = normalize_number(value)
        raw_currency = cast(str | None, normalized_metadata.get("currency"))
        currency_source = normalized_metadata.get("currency_source")
        resolved = self.currency_aliases.resolve(raw_currency)
        if currency_source == "rule_default" and raw_currency is not None:
            normalized_metadata["currency"] = raw_currency
        elif resolved is not None:
            normalized_metadata["currency"] = resolved.currency
            normalized_metadata.setdefault("currency_source", "alias")
            normalized_metadata.setdefault("matched_currency_alias", resolved.alias)
        elif raw_currency is not None:
            normalized_metadata.pop("currency", None)
        elif self.profile.default_currency is not None:
            normalized_metadata["currency"] = self.profile.default_currency
            normalized_metadata["currency_source"] = "profile_default"
        return normalized_value, normalized_metadata

    def _apply_fallbacks(
        self,
        fields: dict[str, ExtractedField],
        *,
        product_evidence: bool = False,
    ) -> None:
        # A source default supplies missing data; it is never product evidence.
        if not product_evidence and not any(
            fields.get(name, ExtractedField()).status is FieldStatus.FOUND
            for name in ("price", "sizes", "article", "product_name")
        ):
            return
        missing_before_fallback = {
            name for name, value in fields.items() if value.status is FieldStatus.MISSING
        }
        for field, fallback in self.profile.fallback_rules.items():
            required_missing = set(fallback.when_all_missing) or {field}
            if (
                field not in missing_before_fallback
                or not required_missing <= missing_before_fallback
            ):
                continue
            fields[field] = ExtractedField(
                status=FieldStatus.FOUND,
                value=fallback.value,
                evidence=f"configured source fallback: {field}",
                provenance=Provenance.SOURCE_RULE,
                metadata={
                    "authoritative": fallback.authoritative,
                    "fallback": True,
                    "when_all_missing": fallback.when_all_missing,
                },
            )

    def _apply_rule(
        self, rule: ExtractionRule, text: str, *, field: str
    ) -> Iterable[tuple[str, str, dict[str, Any]]]:
        flags = re.IGNORECASE if rule.ignore_case else 0
        if rule.kind == "regex":
            for match in re.finditer(rule.pattern, text, flags):
                groups = {
                    key: value for key, value in match.groupdict().items() if value is not None
                }
                value = groups.pop("value", None) or groups.pop("value_alt", None)
                if value:
                    groups["raw_value"] = value
                    currency = groups.pop("currency_before", None) or groups.pop(
                        "currency_after", None
                    )
                    if currency and "currency" not in groups:
                        groups["currency"] = currency
                    if field == "price" and "currency" not in groups:
                        inferred_currency = self._currency_from_match_context(match, text)
                        if inferred_currency is not None:
                            groups["currency"] = inferred_currency.alias
                            groups["currency_source"] = "alias"
                            groups["matched_currency_alias"] = inferred_currency.alias
                    policy = self.profile.field_policies.get(cast(ProductFieldName, field))
                    if policy is not None and policy.processing_mode == "passthrough":
                        transformed = value
                        if rule.transform != "identity":
                            groups["configured_transform_bypassed"] = rule.transform
                    else:
                        transformed = self._transform_value(rule, value)
                    if transformed is not None:
                        yield transformed, match.group(0), groups
            return
        lines = text.splitlines()
        needles = [rule.pattern, *rule.aliases]
        for line in lines:
            candidate = line.casefold() if rule.ignore_case else line
            for needle in needles:
                expected = needle.casefold() if rule.ignore_case else needle
                matched = (
                    (rule.kind in {"label", "line_prefix"} and candidate.startswith(expected))
                    or (rule.kind in {"line_contains", "keyword"} and expected in candidate)
                    or (rule.kind == "exact" and candidate.strip() == expected)
                )
                if matched:
                    value = (
                        line[len(needle) :].lstrip(" :-")
                        if rule.kind in {"label", "line_prefix"}
                        else line.strip()
                    )
                    if value:
                        yield value, line, {"raw_value": value}
                    break

    def _currency_from_match_context(
        self, match: re.Match[str], text: str
    ) -> CurrencyAliasMatch | None:
        """Resolve a currency stated in or immediately beside one price match.

        A source regex is allowed to capture only ``value``. Currency remains
        deterministic when an unambiguous standard alias is part of the full
        match or directly adjacent to it, including Telegram's 💲 and 💵 glyphs.
        """

        before = text[max(0, match.start() - 8) : match.start()]
        after = text[match.end() : min(len(text), match.end() + 8)]
        return self.currency_aliases.find_in_context(
            matched_text=match.group(0),
            before=before,
            after=after,
        )

    @staticmethod
    def _transform_value(rule: ExtractionRule, value: str) -> str | None:
        if rule.transform == "identity":
            return value
        if rule.transform == "normalize_number":
            return normalize_number(value)
        if rule.transform == "map":
            if rule.ignore_case:
                mapped = {
                    key.casefold(): replacement for key, replacement in rule.value_map.items()
                }
                return mapped.get(value.strip().casefold())
            return rule.value_map.get(value.strip())
        flags = re.IGNORECASE if rule.ignore_case else 0
        tokens = list(re.finditer(rule.token_pattern, value, flags))
        if not tokens:
            return None
        cursor = 0
        for token in tokens:
            if not re.fullmatch(r"[\s,;:/|+\-]*", value[cursor : token.start()]):
                return None
            cursor = token.end()
        if not re.fullmatch(r"[\s,;:/|+\-]*", value[cursor:]):
            return None
        parsed_tokens: list[tuple[str, int]] = []
        for token in tokens:
            size = token.group("size").strip()
            count = int(token.group("count"))
            if not size or count <= 0:
                return None
            parsed_tokens.append((size, count))
        if rule.transform == "series_quantity":
            return str(sum(count for _, count in parsed_tokens))
        sizes = list(dict.fromkeys(size for size, _ in parsed_tokens))
        return ", ".join(sizes)

    def _clean(self, text: str) -> str:
        cleaned = text
        preserved: dict[str, str] = {}
        for pattern in self.profile.preserve_patterns:
            for match in list(re.finditer(pattern, cleaned, re.IGNORECASE)):
                token = f"__PRESERVE_{len(preserved)}__"
                preserved[token] = match.group(0)
                cleaned = cleaned.replace(match.group(0), token, 1)
        for value in self.profile.remove_strings:
            cleaned = cleaned.replace(value, "")
        unwanted_lines = {line.casefold() for line in self.profile.remove_lines}
        cleaned = "\n".join(
            line for line in cleaned.splitlines() if line.strip().casefold() not in unwanted_lines
        )
        patterns = [
            r"(?:https?://)?t\.me/\S+",
            r"(?<!\w)@\w{4,}",
            r"(?:https?://)?(?:wa\.me|api\.whatsapp\.com)/\S+",
            r"(?im)^.*\b(?:whatsapp|ватсап|вацап)\b.*$",
            r"(?im)^\s*(?:address|адрес|adres)\s*[:\-].*$",
            r"(?<!\w)(?:\+\d(?:[ \t().-]*\d){7,}|(?=[\d \t().-]{8,}(?!\w))(?=[\d \t().-]*[ \t().-])\d(?:[ \t().-]*\d){7,}(?!\w))",
            *self.profile.cleaning_patterns,
        ]
        for pattern in patterns:
            cleaned = re.sub(pattern, "", cleaned, flags=re.IGNORECASE)
        cleaned = "\n".join(line.rstrip() for line in cleaned.splitlines() if line.strip()).strip()
        for token, value in preserved.items():
            cleaned = cleaned.replace(token, value)
        return cleaned

    def _category(self, text: str) -> ExtractedField:
        for rule in sorted(self.profile.category_rules, key=lambda item: item.priority):
            evidence = next(
                (word for word in rule.keywords if word.casefold() in text.casefold()), None
            )
            if evidence is None and rule.pattern and re.search(rule.pattern, text, re.IGNORECASE):
                evidence = rule.pattern
            if evidence:
                return ExtractedField(
                    status=FieldStatus.FOUND,
                    value=rule.category,
                    evidence=evidence,
                    provenance=Provenance.SOURCE_RULE,
                )
        if self.profile.default_category:
            return ExtractedField(
                status=FieldStatus.FOUND,
                value=self.profile.default_category,
                evidence="source default",
                provenance=Provenance.SOURCE_RULE,
            )
        return ExtractedField()

    def _destination(self, text: str, category: str | None) -> int | None:
        for rule in sorted(self.profile.routing_rules, key=lambda item: item.priority):
            category_match = rule.category is None or rule.category == category
            keyword_match = not rule.keywords or any(
                word.casefold() in text.casefold() for word in rule.keywords
            )
            pattern_match = (
                rule.pattern is None or re.search(rule.pattern, text, re.IGNORECASE) is not None
            )
            if category_match and keyword_match and pattern_match:
                return rule.destination_id
        return self.profile.default_destination_id


@dataclass(frozen=True)
class RulePreview:
    """Result of trying one extraction rule against admin-supplied text."""

    matched: bool
    values: tuple[str, ...]
    captures: tuple[str, ...]
    normalized_values: tuple[str, ...]
    currencies: tuple[str | None, ...]
    candidate_statuses: tuple[FieldStatus, ...]
    matches: tuple[str, ...]
    cleaned_text: str
    flags: tuple[str, ...]


def preview_extraction_rule(
    profile: SourceProfileData,
    field: str,
    rule: ExtractionRule,
    text: str,
    currency_aliases: CurrencyAliasResolver | None = None,
) -> RulePreview:
    """Run a single rule through the production engine for the /admin tester.

    Uses the same cleaning rules, flags and named-group semantics as parsing, so
    a MATCH here means the rule matches during real publication too.
    """
    engine = RuleEngine(profile, currency_aliases)
    cleaned = engine.clean_source_text(text)
    results = list(engine._apply_rule(rule, cleaned, field=field))
    normalized = [
        engine._normalize_candidate(field, value, metadata) for value, _, metadata in results
    ]
    compiled_flags = (
        re.compile(rule.pattern, re.IGNORECASE if rule.ignore_case else 0).flags
        if rule.kind == "regex"
        else (re.IGNORECASE if rule.ignore_case else 0)
    )
    flag_names = tuple(
        name
        for name, flag in (
            ("IGNORECASE", re.IGNORECASE),
            ("MULTILINE", re.MULTILINE),
            ("DOTALL", re.DOTALL),
            ("VERBOSE", re.VERBOSE),
            ("ASCII", re.ASCII),
        )
        if compiled_flags & flag
    )
    return RulePreview(
        matched=bool(results),
        values=tuple(value for value, _, _ in results),
        captures=tuple(str(metadata.get("raw_value", value)) for value, _, metadata in results),
        normalized_values=tuple(value for value, _ in normalized),
        currencies=tuple(cast(str | None, metadata.get("currency")) for _, metadata in normalized),
        candidate_statuses=tuple(FieldStatus.FOUND for _ in results),
        matches=tuple(raw for _, raw, _ in results),
        cleaned_text=cleaned,
        flags=flag_names,
    )
