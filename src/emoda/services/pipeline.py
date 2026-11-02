from __future__ import annotations

from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from typing import Protocol

from pydantic import BaseModel
from pydantic import Field as PydanticField

from emoda.domain.enums import FieldStatus, OCRMode, Provenance
from emoda.domain.schemas import (
    ExtractedField,
    GeminiExtraction,
    MarkupRange,
    MultiPriceLineItem,
    ParsedProduct,
    PriceResult,
    SourceProfileData,
)
from emoda.rules.currency import CurrencyAliasResolver
from emoda.rules.engine import (
    RuleEngine,
    advertisement_reason,
    extract_multi_price_candidates,
    match_source_ignore_rule,
    process_field_candidate,
)
from emoda.rules.numbers import normalize_number
from emoda.services.destination_price import DestinationPricePolicy, SupplierQuote, supplier_quote
from emoda.services.price import PriceEngine, PriceUnresolved
from emoda.services.render import render_publication


class IntelligenceAdapter(Protocol):
    async def extract(
        self,
        *,
        text: str,
        media_paths: list[str],
        missing_fields: list[str],
        instruction: str,
        examples: list[dict[str, object]],
    ) -> GeminiExtraction: ...

    async def translate(self, text: str, target_language: str, instruction: str = "") -> str: ...


@dataclass(frozen=True)
class PipelineInput:
    source_id: int
    text: str
    media_paths: list[str]
    emoda_code: str
    profile: SourceProfileData
    markup_rules: list[MarkupRange]
    media_types: list[str] = field(default_factory=list)
    currency_rates: dict[str, Decimal] = field(default_factory=dict)
    destination_price_policies: dict[str, dict[str, object]] = field(default_factory=dict)
    examples: list[dict[str, object]] = field(default_factory=list)
    currency_aliases: CurrencyAliasResolver = field(
        default_factory=CurrencyAliasResolver.system_defaults
    )


class FieldDiagnostic(BaseModel):
    """Persisted, customer-visible trace of one field through the real pipeline."""

    status: FieldStatus
    candidate_status: FieldStatus
    source: str | None = None
    rule_id: str | None = None
    raw: str | None = None
    normalized: str | None = None
    currency: str | None = None
    currency_source: str | None = None
    matched_currency_alias: str | None = None
    final: str | None = None
    stage: str
    reason: str | None = None


@dataclass(frozen=True)
class _PriceResolution:
    result: PriceResult | None
    candidate_status: FieldStatus
    stage: str
    reason: str | None = None


class PipelineResult(BaseModel):
    publication_format: str = "standard"
    supplier_price: SupplierQuote | None = None
    parsed: ParsedProduct
    price: PriceResult | None
    line_items: list[MultiPriceLineItem] = []
    description_ru: str
    description_en: str
    rendered_text: str
    media_count: int
    needs_review: bool
    review_reasons: list[str]
    publication_requested: bool
    publication_blocked_reason: str | None = None
    ignored: bool = False
    ignore_reason: str | None = None
    field_diagnostics: dict[str, FieldDiagnostic] = PydanticField(default_factory=dict)


class Pipeline:
    """Extraction pipeline for the global four-field publication renderer.

    Field data quality (missing/ambiguous price, sizes, quantity, Gemini or OCR
    degradation) NEVER produces a review reason: the renderer falls back to
    «Уточнить / Check» and the product continues to AUTO. Review reasons are
    reserved for systemic failures handled outside this pipeline.
    """

    def __init__(self, intelligence: IntelligenceAdapter | None) -> None:
        self.intelligence = intelligence

    async def run(self, input: PipelineInput, *, dry_run: bool) -> PipelineResult:
        rule_engine = RuleEngine(input.profile, input.currency_aliases)
        routing_category, routed_destination_id = rule_engine.resolve_routing(input.text)
        parsed = rule_engine.parse(input.text)
        extraction_text = rule_engine.clean_source_text(input.text)
        ad_reason = advertisement_reason(extraction_text, input.profile)
        if ad_reason is not None:
            return PipelineResult(
                parsed=parsed,
                price=None,
                line_items=[],
                description_ru="",
                description_en="",
                rendered_text="",
                media_count=len(input.media_paths),
                needs_review=False,
                review_reasons=[],
                publication_requested=False,
                ignored=True,
                ignore_reason=f"advertising:{ad_reason}",
            )
        candidates, multi_price_errors = extract_multi_price_candidates(
            extraction_text,
            input.profile.multi_price_rules,
            input.currency_aliases,
        )
        multi_price_extracted = bool(len(candidates) >= 2 and not multi_price_errors)
        if multi_price_extracted:
            parsed.fields["price"] = ExtractedField(
                status=FieldStatus.FOUND,
                value="multi-price",
                evidence="\n".join(candidate.evidence for candidate in candidates),
                provenance=Provenance.SOURCE_RULE,
                metadata={"multi_price": True, "item_count": len(candidates)},
            )
        media_types = input.media_types or [
            "video" if path.casefold().endswith((".mp4", ".mov", ".mkv", ".webm")) else "photo"
            for path in input.media_paths
        ]
        matched_ignore_rule = match_source_ignore_rule(
            parsed,
            has_media=bool(input.media_paths),
            media_types=media_types,
            rules=input.profile.ignore_rules,
        )
        if matched_ignore_rule is not None:
            return PipelineResult(
                parsed=parsed,
                price=None,
                line_items=[],
                description_ru="",
                description_en="",
                rendered_text="",
                media_count=len(input.media_paths),
                needs_review=False,
                review_reasons=[],
                publication_requested=False,
                ignored=True,
                ignore_reason=matched_ignore_rule.name,
            )
        line_items: list[MultiPriceLineItem] = []
        for candidate in candidates:
            if candidate.currency not in {"USD", "EUR", *input.currency_rates}:
                # Unresolvable currency degrades to the check value, not REVIEW.
                continue
            try:
                calculated = PriceEngine(
                    input.profile.eur_usd_rate, currency_rates=input.currency_rates
                ).calculate(Decimal(candidate.value), candidate.currency, input.markup_rules)
            except (InvalidOperation, PriceUnresolved, ValueError):
                continue
            line_items.append(
                MultiPriceLineItem(
                    label_original=candidate.label_original,
                    label_ru=candidate.label_ru,
                    label_en=candidate.label_en,
                    evidence=candidate.evidence,
                    price=calculated,
                )
            )
        multi_price_complete = bool(multi_price_extracted and len(line_items) == len(candidates))
        critical = input.profile.required_fields
        missing = [
            name
            for name, value in parsed.fields.items()
            if value.status is not FieldStatus.FOUND
            and not (name == "price" and multi_price_complete)
        ]
        requested_fields = list(missing)
        if input.media_paths and input.profile.ocr_mode is not OCRMode.OFF:
            requested_fields.extend(
                name
                for name in critical
                if name not in requested_fields
                and name not in input.profile.fixed_fields
                and not (name == "price" and multi_price_complete)
            )
        should_ask = bool(
            self.intelligence
            and input.profile.gemini_enabled
            and (
                any(name in missing for name in critical)
                or input.profile.ocr_mode is OCRMode.ALWAYS
                or (input.profile.ocr_mode is OCRMode.MISSING_ONLY and bool(input.media_paths))
            )
        )
        if should_ask and self.intelligence:
            try:
                extracted = await self.intelligence.extract(
                    text=extraction_text,
                    media_paths=input.media_paths,
                    missing_fields=requested_fields,
                    instruction=input.profile.gemini_instruction,
                    examples=input.examples[: input.profile.gemini_example_limit],
                )
                self._merge(
                    parsed,
                    extracted,
                    input.media_paths,
                    profile=input.profile,
                    requested_fields=set(requested_fields),
                    text_priority=input.profile.text_priority,
                )
                parsed.destination_id = rule_engine.route(extraction_text, parsed.category.value)
            except Exception:
                # Gemini is optional for publication: unresolved fields simply
                # fall back to «Уточнить / Check».
                pass
        price_resolution = (
            _PriceResolution(
                result=None,
                candidate_status=FieldStatus.FOUND,
                stage="resolved",
            )
            if multi_price_complete
            else self._resolve_price(parsed, input)
        )
        price = price_resolution.result
        destination_id = parsed.destination_id or input.profile.default_destination_id
        destination_policy = DestinationPricePolicy.model_validate(
            input.destination_price_policies.get(str(destination_id), {})
        )
        alternative_text = (
            destination_policy.alternative_text or input.profile.alternative_publication_text
        )
        publication_format = "alternative_text" if alternative_text else destination_policy.mode
        quote = None
        if destination_policy.mode == "supplier_percent":
            quote = supplier_quote(parsed, destination_policy, input.currency_rates)
            # Global markup results never leak into this explicitly selected mode.
            price = None
            line_items = []
        rendered = render_publication(
            code=input.emoda_code,
            parsed=parsed,
            price=price,
            line_items=line_items if multi_price_complete else [],
            publication_format=publication_format,
            supplier_price=quote,
            alternative_text=alternative_text,
        )
        diagnostics = self._field_diagnostics(
            parsed,
            input.profile,
            price_resolution,
            line_items=line_items if multi_price_complete else [],
        )
        if destination_policy.mode == "supplier_percent":
            trace = diagnostics["price"]
            diagnostics["price"] = trace.model_copy(
                update={
                    "candidate_status": FieldStatus.FOUND if quote else FieldStatus.MISSING,
                    "final": str(quote.amount) if quote else None,
                    "currency": quote.currency if quote else trace.currency,
                    "stage": "resolved" if quote else "supplier_price",
                    "reason": None if quote else "supplier_price_unresolved",
                }
            )
        return PipelineResult(
            publication_format=publication_format,
            supplier_price=quote,
            parsed=parsed,
            price=price,
            line_items=line_items,
            description_ru="",
            description_en="",
            rendered_text=rendered,
            media_count=len(input.media_paths),
            needs_review=False,
            review_reasons=[],
            publication_requested=not dry_run and bool(input.media_paths),
            publication_blocked_reason=None if input.media_paths else "media_required",
            field_diagnostics=diagnostics,
        )

    @staticmethod
    def _merge(
        parsed: ParsedProduct,
        extracted: GeminiExtraction,
        media_paths: list[str],
        *,
        profile: SourceProfileData,
        requested_fields: set[str],
        text_priority: bool,
    ) -> None:
        for name, candidate in extracted.fields.items():
            if name in profile.fixed_fields:
                continue
            if name not in requested_fields or name not in parsed.fields:
                continue
            if candidate.status is FieldStatus.MISSING:
                continue
            candidate = process_field_candidate(profile, name, candidate)
            valid = False
            if candidate.provenance is Provenance.GEMINI_TEXT:
                valid = bool(
                    candidate.evidence
                    and candidate.evidence.casefold() in parsed.original_text.casefold()
                )
            elif candidate.provenance is Provenance.GEMINI_OCR:
                valid = bool(media_paths and candidate.evidence)
            if not valid:
                continue
            current = parsed.fields[name]
            if (
                current.status is FieldStatus.AMBIGUOUS
                and current.provenance is Provenance.COMPUTED_RULE
            ):
                # A deterministic computed rule has already established that
                # the source evidence has no single safe result. Gemini may
                # extract evidence, but must not guess away that ambiguity.
                continue
            source_rule_wins = bool(
                current.status is FieldStatus.FOUND
                and current.provenance in {Provenance.SOURCE_RULE, Provenance.COMPUTED_RULE}
                and current.metadata.get("authoritative") is True
            )
            if candidate.status is FieldStatus.AMBIGUOUS:
                if source_rule_wins or (
                    text_priority
                    and current.status is FieldStatus.FOUND
                    and current.provenance
                    in {
                        Provenance.STRICT_TEXT,
                        Provenance.SOURCE_RULE,
                        Provenance.COMPUTED_RULE,
                    }
                    and candidate.provenance is Provenance.GEMINI_OCR
                ):
                    continue
                parsed.fields[name] = candidate
                continue
            if current.status is FieldStatus.FOUND and current.value != candidate.value:
                if not (
                    source_rule_wins
                    or (
                        text_priority
                        and current.provenance
                        in {
                            Provenance.STRICT_TEXT,
                            Provenance.SOURCE_RULE,
                            Provenance.COMPUTED_RULE,
                        }
                        and candidate.provenance is Provenance.GEMINI_OCR
                    )
                ):
                    # Conflicting evidence keeps the structured ambiguity; the
                    # renderer publishes the check value instead of a guess.
                    parsed.fields[name] = current.model_copy(
                        update={
                            "status": FieldStatus.AMBIGUOUS,
                            "value": None,
                            "evidence": f"{current.evidence} | {candidate.evidence}",
                        }
                    )
            elif current.status is not FieldStatus.FOUND:
                parsed.fields[name] = candidate
        if (
            extracted.category.status is FieldStatus.FOUND
            and parsed.category.status is not FieldStatus.FOUND
        ):
            category_evidence = extracted.category.evidence
            category_valid = (
                extracted.category.provenance is Provenance.GEMINI_TEXT
                and category_evidence is not None
                and category_evidence.casefold() in parsed.original_text.casefold()
            ) or (
                extracted.category.provenance is Provenance.GEMINI_OCR
                and bool(media_paths)
                and bool(category_evidence)
            )
            if category_valid:
                parsed.category = extracted.category

    @staticmethod
    def _resolve_price(parsed: ParsedProduct, input: PipelineInput) -> _PriceResolution:
        field = parsed.fields["price"]
        if field.status is FieldStatus.AMBIGUOUS:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.AMBIGUOUS,
                stage="conflict_policy",
                reason="field_ambiguous",
            )
        if field.status is FieldStatus.MISSING or not field.value:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.MISSING,
                stage="field_merge",
                reason="field_missing",
            )
        # A configured repeated-pattern rule may intentionally collect several
        # legitimate price variants. There is no safe single selling price to
        # select, so publish the standard check fallback without REVIEW.
        if len(field.metadata.get("variant_values", [])) > 1:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.AMBIGUOUS,
                stage="conflict_policy",
                reason="multiple_price_variants",
            )
        try:
            field.value = normalize_number(field.value)
            raw = Decimal(field.value)
        except InvalidOperation:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.MISSING,
                stage="numeric_normalization",
                reason="invalid_decimal",
            )
        currency = field.metadata.get("currency") or input.profile.default_currency
        if currency is None:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.MISSING,
                stage="currency",
                reason="currency_missing",
            )
        if currency == "TRY" and currency not in input.currency_rates:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.MISSING,
                stage="currency",
                reason="currency_rate_missing:TRY",
            )
        if currency not in {"USD", "EUR", *input.currency_rates}:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.MISSING,
                stage="currency",
                reason=f"currency_unsupported:{currency}",
            )
        try:
            calculated = PriceEngine(
                input.profile.eur_usd_rate, currency_rates=input.currency_rates
            ).calculate(raw, str(currency), input.markup_rules)
        except (PriceUnresolved, ValueError) as exc:
            return _PriceResolution(
                result=None,
                candidate_status=FieldStatus.MISSING,
                stage="markup",
                reason=f"markup_unresolved:{exc}",
            )
        return _PriceResolution(
            result=calculated,
            candidate_status=FieldStatus.FOUND,
            stage="resolved",
        )

    @staticmethod
    def _decimal_text(value: Decimal) -> str:
        normalized = value.normalize()
        if normalized == normalized.to_integral_value():
            normalized = normalized.quantize(Decimal("1"))
        return format(normalized, "f")

    @classmethod
    def _field_diagnostics(
        cls,
        parsed: ParsedProduct,
        profile: SourceProfileData,
        price_resolution: _PriceResolution,
        *,
        line_items: list[MultiPriceLineItem],
    ) -> dict[str, FieldDiagnostic]:
        diagnostics: dict[str, FieldDiagnostic] = {}
        for name, extracted_field in parsed.fields.items():
            metadata = extracted_field.metadata
            rule_id = metadata.get("rule_id")
            if rule_id is None:
                rule_id = metadata.get("computed_rule") or metadata.get("rule")
            if rule_id is None and metadata.get("fallback") is True:
                rule_id = f"{name}:fallback"
            if rule_id is None and extracted_field.provenance is Provenance.STRICT_TEXT:
                rule_id = "global_strict"
            raw = metadata.get("raw_value")
            if raw is None and extracted_field.status is FieldStatus.FOUND:
                raw = extracted_field.value
            reason = None
            if extracted_field.status is FieldStatus.MISSING:
                reason = "field_missing"
            elif extracted_field.status is FieldStatus.AMBIGUOUS:
                reason = "field_ambiguous"
            diagnostics[name] = FieldDiagnostic(
                status=extracted_field.status,
                candidate_status=extracted_field.status,
                source=(
                    extracted_field.provenance.value
                    if extracted_field.provenance is not None
                    else None
                ),
                rule_id=str(rule_id) if rule_id is not None else None,
                raw=str(raw) if raw is not None else None,
                normalized=extracted_field.value,
                final=(
                    extracted_field.value if extracted_field.status is FieldStatus.FOUND else None
                ),
                stage=(
                    "resolved" if extracted_field.status is FieldStatus.FOUND else "field_merge"
                ),
                reason=reason,
            )

        price_field = parsed.fields["price"]
        price_metadata = price_field.metadata
        price_trace = diagnostics["price"]
        if price_resolution.result is not None:
            final = cls._decimal_text(price_resolution.result.final_usd)
        elif line_items:
            final = "; ".join(cls._decimal_text(item.price.final_usd) for item in line_items)
        else:
            final = None
        diagnostics["price"] = price_trace.model_copy(
            update={
                "candidate_status": price_resolution.candidate_status,
                "currency": price_metadata.get("currency") or profile.default_currency,
                "currency_source": price_metadata.get("currency_source"),
                "matched_currency_alias": price_metadata.get("matched_currency_alias"),
                "final": final,
                "stage": price_resolution.stage,
                "reason": price_resolution.reason,
            }
        )
        return diagnostics
