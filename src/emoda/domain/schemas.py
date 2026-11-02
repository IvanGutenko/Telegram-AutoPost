from __future__ import annotations

import re
from decimal import Decimal
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from emoda.domain.enums import (
    ButtonAction,
    CatalogNodeType,
    EditPolicy,
    FieldStatus,
    OCRMode,
    Provenance,
    PublicationMode,
    ReviewReasonCode,
)

RuleKind = Literal["regex", "label", "line_prefix", "line_contains", "keyword", "exact"]
RuleTransform = Literal["identity", "series_sizes", "series_quantity", "map", "normalize_number"]
FieldProcessingMode = Literal["normal", "passthrough"]
CandidateAggregation = Literal["first", "unique", "join_unique", "ambiguous"]
ComputedOperation = Literal[
    "count_items",
    "count_series_items",
    "sum_matches",
    "sum_size_counts",
    "unique_matches",
]
ComputedInputMode = Literal["field_value", "raw"]
ComputedInputSource = Literal["field", "raw_field_candidate", "canonical_text"]
ComputedConflictPolicy = Literal["explicit", "computed", "ambiguous"]
MarkupType = Literal["fixed", "percent"]
ProductFieldName = Literal[
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
]
PRODUCT_FIELD_NAMES: tuple[ProductFieldName, ...] = (
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
DEFAULT_REQUIRED_FIELDS: tuple[ProductFieldName, ...] = ("price", "sizes", "quantity")


def default_required_fields() -> list[ProductFieldName]:
    return list(DEFAULT_REQUIRED_FIELDS)


DEFAULT_SERIES_TOKEN_PATTERN = r"(?P<size>[A-Za-zА-Яа-я]+)\s*(?P<count>\d+)"


class ReviewReason(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: ReviewReasonCode
    detail: str | None = None
    field: str | None = None


class CollectionBoundaryRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    action: Literal["start", "continue", "end", "hard_boundary", "soft_boundary", "ignore"]
    match: Literal[
        "regex",
        "keyword",
        "has_text",
        "has_media",
        "captioned_media",
        "grouped_id_change",
        "always",
    ]
    pattern: str | None = None
    keywords: list[str] = Field(default_factory=list)
    target: Literal["incoming", "current", "combined"] = "incoming"
    ignore_case: bool = True
    reason: str | None = None
    enabled: bool = True

    @model_validator(mode="after")
    def match_configuration_is_complete(self) -> CollectionBoundaryRule:
        if self.match == "regex":
            if not self.pattern:
                raise ValueError("regex boundary requires pattern")
            try:
                re.compile(self.pattern)
            except re.error as exc:
                raise ValueError(f"invalid boundary regex: {exc}") from exc
        elif self.pattern is not None:
            raise ValueError("pattern is only valid for regex boundaries")
        if self.match == "keyword":
            if not self.keywords or any(not item.strip() for item in self.keywords):
                raise ValueError("keyword boundary requires non-empty keywords")
        elif self.keywords:
            raise ValueError("keywords are only valid for keyword boundaries")
        return self


class CollectionProfile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    strategy: Literal["single_message", "single_grouped_id", "multi_grouped_id"] = (
        "single_grouped_id"
    )
    order: Literal["any", "media_before_text", "text_before_media", "text_between_media"] = "any"
    merge_neighbor_grouped_ids: bool = False
    max_time_gap_seconds: Decimal = Field(default=Decimal("0"), ge=0, le=604800)
    max_message_id_gap: int = Field(default=0, ge=0, le=10000)
    debounce_seconds: Decimal = Field(default=Decimal("1.8"), ge=Decimal("0.5"), le=300)
    max_messages: int = Field(default=32, ge=1, le=200)
    max_media: int = Field(default=20, ge=0, le=200)
    allow_multiple_text_blocks: bool = False
    merge_text_blocks: bool = False
    main_text: Literal["first", "last", "longest", "caption", "all"] = "all"
    stop_on_next_start: bool = True
    finalize_on_next_start: bool = False
    boundary_rules: list[CollectionBoundaryRule] = Field(default_factory=list)

    @field_validator("max_time_gap_seconds", "debounce_seconds", mode="before")
    @classmethod
    def reject_float_decimals(cls, value: object) -> object:
        if isinstance(value, float):
            raise ValueError("Decimal values must not be supplied as float")
        return value

    @model_validator(mode="after")
    def collection_configuration_is_safe(self) -> CollectionProfile:
        if self.strategy == "multi_grouped_id" and not self.merge_neighbor_grouped_ids:
            raise ValueError("multi_grouped_id requires merge_neighbor_grouped_ids=true")
        names = [rule.name for rule in self.boundary_rules]
        if len(names) != len(set(names)):
            raise ValueError("collection boundary rule names must be unique")
        return self


class CleaningRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: Literal["exact", "contains", "prefix", "suffix", "regex"]
    value: str = Field(min_length=1)
    replacement: str = ""
    scope: Literal["line", "text"] = "line"
    language: Literal["both", "ru", "en"] = "both"
    ignore_case: bool = True
    enabled: bool = True

    @model_validator(mode="after")
    def regex_compiles(self) -> CleaningRule:
        if self.kind == "regex":
            try:
                re.compile(self.value)
            except re.error as exc:
                raise ValueError(f"invalid cleaning regex: {exc}") from exc
        return self


class RepeatedPatternRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    pattern: str
    fields: dict[str, str] = Field(min_length=1)
    aggregation: dict[str, CandidateAggregation] = Field(default_factory=dict)
    separators: dict[str, str] = Field(default_factory=dict)
    priority: int = 100
    ignore_case: bool = True
    authoritative: bool = False
    enabled: bool = True

    @model_validator(mode="after")
    def captures_are_valid(self) -> RepeatedPatternRule:
        try:
            compiled = re.compile(self.pattern)
        except re.error as exc:
            raise ValueError(f"invalid repeated-pattern regex: {exc}") from exc
        missing = set(self.fields.values()) - set(compiled.groupindex)
        if missing:
            raise ValueError(f"repeated-pattern capture groups missing: {sorted(missing)}")
        unknown = (set(self.aggregation) | set(self.separators)) - set(self.fields)
        if unknown:
            raise ValueError(f"repeated-pattern fields are not mapped: {sorted(unknown)}")
        if any(not field.strip() or not group.strip() for field, group in self.fields.items()):
            raise ValueError("repeated-pattern field and group names must not be blank")
        return self


class ExtractionRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    kind: RuleKind
    pattern: str
    aliases: list[str] = Field(default_factory=list)
    priority: int = 100
    ignore_case: bool = True
    transform: RuleTransform = "identity"
    token_pattern: str = DEFAULT_SERIES_TOKEN_PATTERN
    value_map: dict[str, str] = Field(default_factory=dict)
    authoritative: bool = False
    enabled: bool = True

    @field_validator("pattern")
    @classmethod
    def valid_pattern(cls, value: str, info: Any) -> str:
        if not value.strip():
            raise ValueError("pattern must not be empty")
        if info.data.get("kind") == "regex":
            try:
                re.compile(value)
            except re.error as exc:
                raise ValueError(f"invalid regex: {exc}") from exc
        return value

    @field_validator("token_pattern")
    @classmethod
    def valid_token_pattern(cls, value: str) -> str:
        try:
            compiled = re.compile(value)
        except re.error as exc:
            raise ValueError(f"invalid token regex: {exc}") from exc
        if not {"size", "count"} <= set(compiled.groupindex):
            raise ValueError("series token_pattern requires named size and count groups")
        return value

    @model_validator(mode="after")
    def transform_configuration_is_complete(self) -> ExtractionRule:
        if self.transform != "identity" and self.kind != "regex":
            raise ValueError("transforms require a regex extraction rule")
        if self.transform != "identity" and "value" not in re.compile(self.pattern).groupindex:
            raise ValueError("transformed regex requires a named value group")
        if self.transform == "map":
            if not self.value_map:
                raise ValueError("map transform requires value_map")
            if any(not key.strip() or not value.strip() for key, value in self.value_map.items()):
                raise ValueError("value_map keys and values must not be empty")
        return self


class FieldProcessingPolicy(BaseModel):
    """Source-scoped policy for one extracted field.

    ``normal`` is the implicit default and preserves all existing processing.
    ``passthrough`` retains the bounded candidate text while still requiring
    normal evidence/provenance and conflict resolution.
    """

    model_config = ConfigDict(extra="forbid")

    processing_mode: FieldProcessingMode = "normal"


class ComputedRule(BaseModel):
    """A source-scoped transformation that derives fields from configured evidence."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    target_field: str
    operation: ComputedOperation
    source_field: str | None = None
    input_source: ComputedInputSource | None = None
    input_mode: ComputedInputMode = "field_value"
    secondary_target_field: str | None = None
    pattern: str | None = None
    capture_group: str = "size"
    value_group: str = "count"
    separator: str = ", "
    series_separator_pattern: str | None = None
    range_pattern: str | None = None
    explicit_list_pattern: str | None = None
    explicit_separator_pattern: str | None = None
    compact_range_pattern: str | None = None
    range_start_group: str = "start"
    range_end_group: str = "end"
    explicit_item_group: str = "item"
    numeric_step: Decimal | None = Field(default=None, gt=0)
    priority: int = 100
    ignore_case: bool = True
    authoritative: bool = False
    conflict_policy: ComputedConflictPolicy = "ambiguous"
    enabled: bool = True

    @field_validator("target_field", "source_field", "secondary_target_field", mode="before")
    @classmethod
    def valid_field_name(cls, value: object) -> object:
        if value is None:
            return value
        if not isinstance(value, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value.strip()):
            raise ValueError(
                "computed field names must use 1-32 lowercase latin letters, digits or underscore"
            )
        return value.strip()

    @field_validator(
        "capture_group",
        "value_group",
        "range_start_group",
        "range_end_group",
        "explicit_item_group",
    )
    @classmethod
    def valid_group_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", cleaned):
            raise ValueError("computed capture group name is invalid")
        return cleaned

    @field_validator("separator")
    @classmethod
    def separator_is_not_empty(cls, value: str) -> str:
        if value == "":
            raise ValueError("computed separator must not be empty")
        return value

    @field_validator("numeric_step", mode="before")
    @classmethod
    def numeric_step_is_not_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise ValueError("numeric_step must not be supplied as float")
        return value

    @model_validator(mode="after")
    def operation_configuration_is_complete(self) -> ComputedRule:
        if (
            self.operation in {"count_items", "count_series_items"}
            and self.input_source is None
            and self.source_field is None
        ):
            raise ValueError(f"{self.operation} requires source_field")
        if self.input_source in {"field", "raw_field_candidate"} and self.source_field is None:
            raise ValueError(f"input_source={self.input_source} requires source_field")
        if self.input_source == "canonical_text" and self.source_field is not None:
            raise ValueError("input_source=canonical_text must not use source_field")
        if self.operation == "count_series_items":
            if self.resolved_input_source() not in {"field", "raw_field_candidate"}:
                raise ValueError(
                    "count_series_items requires input_source=field or raw_field_candidate"
                )
            if self.series_separator_pattern is None:
                raise ValueError("count_series_items requires series_separator_pattern")
            if self.range_pattern is None:
                raise ValueError("count_series_items requires range_pattern")
            if self.numeric_step is None:
                raise ValueError("count_series_items requires numeric_step")
        if self.operation not in {"count_items", "count_series_items"} and self.pattern is None:
            raise ValueError(f"{self.operation} requires pattern")
        if self.operation == "sum_size_counts" and self.secondary_target_field is None:
            raise ValueError("sum_size_counts requires secondary_target_field")
        if self.secondary_target_field == self.target_field:
            raise ValueError("computed target fields must be different")
        if self.pattern is None:
            compiled = None
        else:
            try:
                compiled = re.compile(self.pattern)
            except re.error as exc:
                raise ValueError(f"invalid computed regex: {exc}") from exc
            required_groups: set[str] = set()
            if self.operation in {"count_items", "unique_matches", "sum_size_counts"}:
                required_groups.add(self.capture_group)
            if self.operation in {"sum_matches", "sum_size_counts"}:
                required_groups.add(self.value_group)
            missing = required_groups - set(compiled.groupindex)
            if missing:
                raise ValueError(f"computed regex capture groups missing: {sorted(missing)}")
        regex_settings = {
            "series_separator_pattern": self.series_separator_pattern,
            "range_pattern": self.range_pattern,
            "explicit_list_pattern": self.explicit_list_pattern,
            "explicit_separator_pattern": self.explicit_separator_pattern,
            "compact_range_pattern": self.compact_range_pattern,
        }
        compiled_settings: dict[str, re.Pattern[str]] = {}
        for name, value in regex_settings.items():
            if value is None:
                continue
            try:
                compiled_settings[name] = re.compile(value)
            except re.error as exc:
                raise ValueError(f"invalid {name}: {exc}") from exc
        series_separator = compiled_settings.get("series_separator_pattern")
        if series_separator is not None and series_separator.match("") is not None:
            raise ValueError("series_separator_pattern must not match empty text")
        for name in ("range_pattern", "compact_range_pattern"):
            configured_pattern = compiled_settings.get(name)
            if configured_pattern is None:
                continue
            required = {self.range_start_group, self.range_end_group}
            missing = required - set(configured_pattern.groupindex)
            if missing:
                raise ValueError(f"{name} capture groups missing: {sorted(missing)}")
        explicit_pattern = compiled_settings.get("explicit_list_pattern")
        if (
            explicit_pattern is not None
            and self.explicit_item_group not in explicit_pattern.groupindex
        ):
            raise ValueError(
                f"explicit_list_pattern capture group missing: {self.explicit_item_group}"
            )
        if (self.explicit_list_pattern is None) != (self.explicit_separator_pattern is None):
            raise ValueError(
                "explicit_list_pattern and explicit_separator_pattern must be configured together"
            )
        return self

    def resolved_input_source(self) -> ComputedInputSource:
        """Resolve explicit input selection while preserving every legacy profile."""

        if self.input_source is not None:
            return self.input_source
        if self.source_field is None:
            return "canonical_text"
        if self.input_mode == "raw":
            return "raw_field_candidate"
        return "field"


class SourceFallbackRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    value: str = Field(min_length=1)
    authoritative: bool = False
    when_all_missing: list[str] = Field(default_factory=list)

    @field_validator("value")
    @classmethod
    def non_blank_value(cls, value: str) -> str:
        cleaned = value.strip()
        if not cleaned:
            raise ValueError("fallback value must not be blank")
        return cleaned

    @field_validator("when_all_missing")
    @classmethod
    def valid_missing_fields(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("when_all_missing contains duplicate fields")
        for value in values:
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value):
                raise ValueError("when_all_missing contains an invalid field name")
        return values


class SourceIgnoreRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    media_required: bool = True
    description_missing_required: bool = True
    found_fields: list[str] = Field(default_factory=list)
    missing_fields: list[str] = Field(default_factory=list)
    grouped_id: Literal["any", "present", "missing"] = "any"
    media_types: list[Literal["photo", "video", "document", "none"]] = Field(default_factory=list)
    min_media: int | None = Field(default=None, ge=0, le=200)
    max_media: int | None = Field(default=None, ge=0, le=200)
    min_messages: int | None = Field(default=None, ge=1, le=200)
    max_messages: int | None = Field(default=None, ge=1, le=200)
    enabled: bool = True

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", cleaned):
            raise ValueError("ignore rule name must use letters, digits, underscore or hyphen")
        return cleaned

    @field_validator("found_fields", "missing_fields")
    @classmethod
    def valid_fields(cls, values: list[str]) -> list[str]:
        cleaned = [value.strip() for value in values]
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("ignore rule contains duplicate fields")
        for value in cleaned:
            if not re.fullmatch(r"[a-z][a-z0-9_]{0,31}", value):
                raise ValueError("ignore rule contains an invalid field name")
        return cleaned

    @model_validator(mode="after")
    def conditions_are_logically_valid(self) -> SourceIgnoreRule:
        if self.has_transport_structure() and (
            self.found_fields or self.missing_fields or self.description_missing_required
        ):
            raise ValueError(
                "group/count structural IGNORE requires description_missing_required=false and no field conditions; create a separate text rule"
            )
        if not self.found_fields and not self.missing_fields and not self.has_structure():
            raise ValueError("ignore rule requires at least one found or missing field")
        overlap = set(self.found_fields) & set(self.missing_fields)
        if overlap:
            raise ValueError(f"ignore rule fields overlap: {sorted(overlap)}")
        for low, high in ((self.min_media, self.max_media), (self.min_messages, self.max_messages)):
            if low is not None and high is not None and low > high:
                raise ValueError("ignore minimum must not exceed maximum")
        return self

    def has_structure(self) -> bool:
        """Whether this rule is safe to execute before product collection.

        A media type by itself is a transport filter.  Once it is combined with
        parsed-field or description conditions it becomes a post-collection rule,
        while grouped/count constraints remain transport-only.
        """
        has_text_conditions = bool(
            self.found_fields or self.missing_fields or self.description_missing_required
        )
        return self.has_transport_structure() or (bool(self.media_types) and not has_text_conditions)

    def has_transport_structure(self) -> bool:
        return (
            self.grouped_id != "any"
            or any(
                value is not None
                for value in (self.min_media, self.max_media, self.min_messages, self.max_messages)
            )
        )


class LocalizedLabel(BaseModel):
    model_config = ConfigDict(extra="forbid")

    ru: str = Field(min_length=1, max_length=100)
    en: str = Field(min_length=1, max_length=100)


class MultiPriceRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=64)
    pattern: str
    labels: dict[str, LocalizedLabel] = Field(default_factory=dict)
    label_mode: Literal["mapped", "dynamic"] = "mapped"
    default_currency: Literal["USD", "EUR", "TRY"] | None = None
    priority: int = 100
    ignore_case: bool = True
    enabled: bool = True

    @model_validator(mode="after")
    def mapped_labels_required(self) -> MultiPriceRule:
        if self.label_mode == "mapped" and not self.labels:
            raise ValueError("mapped multi-price requires labels")
        return self

    @field_validator("name")
    @classmethod
    def valid_name(cls, value: str) -> str:
        cleaned = value.strip()
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", cleaned):
            raise ValueError("multi-price rule name is invalid")
        return cleaned

    @field_validator("pattern")
    @classmethod
    def valid_pattern(cls, value: str) -> str:
        try:
            compiled = re.compile(value)
        except re.error as exc:
            raise ValueError(f"invalid multi-price regex: {exc}") from exc
        if not {"label", "value"} <= set(compiled.groupindex):
            raise ValueError("multi-price regex requires named label and value groups")
        return value

    @field_validator("labels")
    @classmethod
    def valid_labels(cls, value: dict[str, LocalizedLabel]) -> dict[str, LocalizedLabel]:
        if any(not label.strip() for label in value):
            raise ValueError("multi-price labels must not be blank")
        return value


class RoutingRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: str | None = None
    keywords: list[str] = Field(default_factory=list)
    pattern: str | None = None
    destination_id: int
    priority: int = 100

    @field_validator("pattern")
    @classmethod
    def valid_regex(cls, value: str | None) -> str | None:
        if value is not None:
            re.compile(value)
        return value


class CategoryRule(BaseModel):
    model_config = ConfigDict(extra="forbid")

    category: str
    keywords: list[str] = Field(default_factory=list)
    pattern: str | None = None
    priority: int = 100

    @field_validator("pattern")
    @classmethod
    def valid_regex(cls, value: str | None) -> str | None:
        if value is not None:
            re.compile(value)
        return value


class SourceProfileData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal[1, 2] = 2
    publication_mode: PublicationMode = PublicationMode.REVIEW
    use_global_publication_mode: bool = False
    default_currency: Literal["USD", "EUR", "TRY"] | None = None
    required_fields: list[ProductFieldName] = Field(
        default_factory=default_required_fields,
        min_length=1,
    )
    field_policies: dict[ProductFieldName, FieldProcessingPolicy] = Field(default_factory=dict)
    extraction_rules: dict[str, list[ExtractionRule]] = Field(default_factory=dict)
    computed_rules: list[ComputedRule] = Field(default_factory=list)
    repeated_pattern_rules: list[RepeatedPatternRule] = Field(default_factory=list)
    fallback_rules: dict[str, SourceFallbackRule] = Field(default_factory=dict)
    fixed_fields: dict[Literal["sizes", "quantity"], str] = Field(default_factory=dict)
    ignore_rules: list[SourceIgnoreRule] = Field(default_factory=list)
    multi_price_rules: list[MultiPriceRule] = Field(default_factory=list)
    advertising_filter_enabled: bool = True
    deduplicate_exact_products: bool = True
    advertising_patterns: list[str] = Field(default_factory=list)
    remove_strings: list[str] = Field(default_factory=list)
    remove_lines: list[str] = Field(default_factory=list)
    cleaning_patterns: list[str] = Field(default_factory=list)
    cleaning_rules: list[CleaningRule] = Field(default_factory=list)
    preserve_patterns: list[str] = Field(default_factory=list)
    category_rules: list[CategoryRule] = Field(default_factory=list)
    routing_rules: list[RoutingRule] = Field(default_factory=list)
    default_category: str | None = None
    default_destination_id: int | None = None
    alternative_publication_text: str | None = Field(default=None, max_length=4096)
    gemini_enabled: bool = False
    translation_enabled: bool = True
    gemini_instruction: str = ""
    gemini_example_limit: int = Field(default=5, ge=0, le=20)
    ocr_mode: OCRMode = OCRMode.MISSING_ONLY
    text_priority: bool = False
    edit_policy: EditPolicy = EditPolicy.AUTO
    album_debounce_seconds: Decimal = Field(
        default=Decimal("1.8"), ge=Decimal("0.5"), le=Decimal("30")
    )
    adjacent_media_merge_seconds: Decimal = Field(
        default=Decimal("0"), ge=Decimal("0"), le=Decimal("300")
    )
    adjacent_text_merge_seconds: Decimal = Field(
        default=Decimal("0"), ge=Decimal("0"), le=Decimal("300")
    )
    product_collection_mode: Literal["off", "text_before_media", "media_before_text"] = "off"
    product_collection_window_seconds: Decimal = Field(
        default=Decimal("0"), ge=Decimal("0"), le=Decimal("300")
    )
    collection: CollectionProfile | None = None
    eur_usd_rate: Decimal = Decimal("1.2")

    @field_validator("fixed_fields")
    @classmethod
    def validate_fixed_fields(cls, values: dict[str, str]) -> dict[str, str]:
        normalized = {}
        for name, value in values.items():
            value = value.strip()
            if not value or len(value) > 500 or "\n" in value or "\r" in value:
                raise ValueError("Фиксированное значение: непустая строка до 500 символов")
            if name == "quantity":
                if not re.fullmatch(r"[0-9]{1,9}", value) or int(value) <= 0:
                    raise ValueError("Количество: положительное целое число до 9 цифр")
                value = str(int(value))
            normalized[name] = value
        return normalized

    @field_validator("alternative_publication_text")
    @classmethod
    def normalize_alternative_publication_text(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip()
        return normalized or None

    @field_validator(
        "eur_usd_rate",
        "album_debounce_seconds",
        "adjacent_media_merge_seconds",
        "adjacent_text_merge_seconds",
        "product_collection_window_seconds",
        mode="before",
    )
    @classmethod
    def reject_binary_float(cls, value: object) -> object:
        if isinstance(value, float):
            raise ValueError("Decimal values must not be supplied as float")
        return value

    @field_validator("eur_usd_rate")
    @classmethod
    def fixed_eur_rate(cls, value: Decimal) -> Decimal:
        if value != Decimal("1.2"):
            raise ValueError('EUR conversion rate is fixed at Decimal("1.2")')
        return value

    @field_validator("cleaning_patterns", "preserve_patterns", "advertising_patterns")
    @classmethod
    def patterns_compile(cls, values: list[str]) -> list[str]:
        for value in values:
            re.compile(value)
        return values

    @field_validator("required_fields")
    @classmethod
    def required_fields_are_unique(cls, values: list[ProductFieldName]) -> list[ProductFieldName]:
        if len(values) != len(set(values)):
            raise ValueError("required_fields must not contain duplicates")
        return values

    @model_validator(mode="after")
    def fallback_conditions_include_target(self) -> SourceProfileData:
        for field, fallback in self.fallback_rules.items():
            if fallback.when_all_missing and field not in fallback.when_all_missing:
                raise ValueError(f"fallback {field} must include itself in when_all_missing")
        names = [rule.name for rule in self.ignore_rules]
        if len(names) != len(set(names)):
            raise ValueError("ignore rule names must be unique within a source")
        multi_price_names = [rule.name for rule in self.multi_price_rules]
        if len(multi_price_names) != len(set(multi_price_names)):
            raise ValueError("multi-price rule names must be unique within a source")
        computed_names = [rule.name for rule in self.computed_rules]
        if len(computed_names) != len(set(computed_names)):
            raise ValueError("computed rule names must be unique within a source")
        if self.product_collection_mode != "off" and self.product_collection_window_seconds <= 0:
            raise ValueError("enabled product collection requires a positive window")
        return self

    def effective_collection(self) -> CollectionProfile:
        if self.collection is not None:
            return self.collection
        if self.product_collection_mode == "off":
            return CollectionProfile(
                strategy="single_grouped_id",
                debounce_seconds=self.album_debounce_seconds,
                max_time_gap_seconds=max(
                    self.adjacent_media_merge_seconds,
                    self.adjacent_text_merge_seconds,
                ),
            )
        return CollectionProfile(
            strategy="multi_grouped_id",
            order=self.product_collection_mode,
            merge_neighbor_grouped_ids=True,
            max_time_gap_seconds=self.product_collection_window_seconds,
            debounce_seconds=self.album_debounce_seconds,
            allow_multiple_text_blocks=False,
        )


class ExtractedField(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: FieldStatus = FieldStatus.MISSING
    value: str | None = None
    evidence: str | None = None
    provenance: Provenance | None = None
    confidence: Decimal | None = Field(default=None, ge=0, le=1)
    metadata: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def found_has_evidence(self) -> ExtractedField:
        if self.status is FieldStatus.FOUND and (
            not self.value or not self.evidence or not self.provenance
        ):
            raise ValueError("found fields require value, evidence and provenance")
        if self.status is FieldStatus.MISSING and self.value is not None:
            raise ValueError("missing field cannot have a value")
        return self


class ParsedProduct(BaseModel):
    original_text: str
    cleaned_text: str
    fields: dict[str, ExtractedField]
    category: ExtractedField = Field(default_factory=ExtractedField)
    destination_id: int | None = None


class MarkupRange(BaseModel):
    min_price: Decimal = Field(ge=0)
    max_price: Decimal | None = Field(default=None, ge=0)
    markup: Decimal = Field(ge=0)
    markup_type: MarkupType = "fixed"
    priority: int
    enabled: bool = True

    @model_validator(mode="after")
    def ordered(self) -> MarkupRange:
        if self.max_price is not None and self.min_price > self.max_price:
            raise ValueError("min_price must be <= max_price")
        return self


class PriceResult(BaseModel):
    raw_price: Decimal
    raw_currency: str = Field(pattern=r"^[A-Z]{3}$")
    normalized_usd: Decimal
    markup: Decimal
    final_usd: Decimal


class MultiPriceLineItem(BaseModel):
    label_original: str
    label_ru: str
    label_en: str
    evidence: str
    price: PriceResult


def validate_safe_https_url(value: str) -> str:
    if any(character.isspace() for character in value):
        raise ValueError("button URL must not contain whitespace")
    parsed = urlparse(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.hostname.startswith(".")
        or parsed.hostname.endswith(".")
        or ".." in parsed.hostname
    ):
        raise ValueError("button URL must be an absolute HTTPS URL")
    try:
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("button URL contains an invalid port") from exc
    return value


class NavigationButtonData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=64)
    action: ButtonAction = ButtonAction.URL
    url: str | None = None
    row: int = Field(ge=0, le=99)
    order: int = Field(ge=0)
    enabled: bool = True

    @field_validator("url")
    @classmethod
    def safe_https(cls, value: str | None) -> str | None:
        return None if value is None else validate_safe_https_url(value)

    @model_validator(mode="after")
    def action_matches_url(self) -> NavigationButtonData:
        if self.action is ButtonAction.URL and self.url is None:
            raise ValueError("URL button requires url")
        if self.action is ButtonAction.CATALOG and self.url is not None:
            raise ValueError("catalog button URL is generated by the bot")
        return self


class CatalogNodeData(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text_ru: str = Field(min_length=1, max_length=64)
    text_en: str = Field(min_length=1, max_length=64)
    node_type: CatalogNodeType
    url: str | None = None
    position: int = Field(default=0, ge=0, le=9999)
    enabled: bool = True

    @field_validator("url")
    @classmethod
    def safe_https(cls, value: str | None) -> str | None:
        return None if value is None else validate_safe_https_url(value)

    @model_validator(mode="after")
    def type_matches_url(self) -> CatalogNodeData:
        if self.node_type is CatalogNodeType.LINK and self.url is None:
            raise ValueError("catalog link requires url")
        if self.node_type is CatalogNodeType.CATEGORY and self.url is not None:
            raise ValueError("catalog category cannot have url")
        return self


class GeminiExtraction(BaseModel):
    fields: dict[str, ExtractedField] = Field(default_factory=dict)
    description: ExtractedField = Field(default_factory=ExtractedField)
    category: ExtractedField = Field(default_factory=ExtractedField)

    @field_validator("fields")
    @classmethod
    def supported_fields_only(cls, value: dict[str, ExtractedField]) -> dict[str, ExtractedField]:
        allowed = {
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
        }
        unknown = set(value) - allowed
        if unknown:
            raise ValueError(f"unsupported fields: {sorted(unknown)}")
        return value
