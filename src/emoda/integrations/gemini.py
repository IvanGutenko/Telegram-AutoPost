from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path
from typing import Any, Literal

from google import genai
from google.genai import types
from pydantic import BaseModel

from emoda.domain.enums import FieldStatus, Provenance
from emoda.domain.schemas import ExtractedField, GeminiExtraction

GeminiFieldName = Literal[
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

_MEDIA_MIME_TYPES = {
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".png": "image/png",
    ".webp": "image/webp",
    ".mp4": "video/mp4",
}


class _StructuredValue(BaseModel):
    status: FieldStatus
    value: str | None
    evidence: str | None
    provenance: Literal[Provenance.GEMINI_TEXT, Provenance.GEMINI_OCR] | None

    def to_domain(self) -> ExtractedField:
        return ExtractedField.model_validate(self.model_dump(exclude={"name"}))


class _StructuredNamedValue(_StructuredValue):
    name: GeminiFieldName
    currency: Literal["USD", "EUR"] | None

    def to_domain(self) -> ExtractedField:
        payload = self.model_dump(exclude={"name", "currency"})
        if self.name == "quantity" and self.status is FieldStatus.FOUND and self.value:
            quantity = re.fullmatch(
                r"\s*(\d+)\s*(?:pcs?|pieces?|шт\.?|штук[аи]?|adet|ad\.?)\s*",
                self.value,
                flags=re.IGNORECASE,
            )
            if quantity:
                payload["value"] = quantity.group(1)
        if self.name == "price" and self.currency:
            payload["metadata"] = {"currency": self.currency}
        return ExtractedField.model_validate(payload)


class _StructuredExtraction(BaseModel):
    fields: list[_StructuredNamedValue]
    description: _StructuredValue
    category: _StructuredValue

    def to_domain(self) -> GeminiExtraction:
        fields: dict[str, ExtractedField] = {}
        for item in self.fields:
            candidate = item.to_domain()
            current = fields.get(item.name)
            if current is None:
                fields[item.name] = candidate
                continue
            if current == candidate:
                continue
            evidence = list(
                dict.fromkeys(value for value in (current.evidence, candidate.evidence) if value)
            )
            fields[item.name] = ExtractedField(
                status=FieldStatus.AMBIGUOUS,
                evidence=" | ".join(evidence) or None,
                provenance=(
                    current.provenance if current.provenance == candidate.provenance else None
                ),
            )
        return GeminiExtraction(
            fields=fields,
            description=self.description.to_domain(),
            category=self.category.to_domain(),
        )


class GeminiUnavailable(RuntimeError):
    def __init__(self, message: str, *, code: int | None = None) -> None:
        super().__init__(message)
        self.code = code


class GoogleGeminiAdapter:
    def __init__(
        self,
        api_key: str,
        model: str,
        *,
        ocr_model: str | None = None,
        attempts: int = 3,
    ) -> None:
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.ocr_model = ocr_model or model
        self.attempts = attempts

    async def extract(
        self,
        *,
        text: str,
        media_paths: list[str],
        missing_fields: list[str],
        instruction: str,
        examples: list[dict[str, object]],
    ) -> GeminiExtraction:
        result = await self._extract_once(
            text=text,
            media_paths=media_paths,
            missing_fields=missing_fields,
            instruction=instruction,
            examples=examples,
        )
        critical = {"price", "sizes", "quantity"}
        focused_fields = [
            name
            for name in missing_fields
            if name in critical
            and (name not in result.fields or result.fields[name].status is FieldStatus.MISSING)
        ]
        if not media_paths or not focused_fields:
            return result
        try:
            focused = await self._extract_once(
                text=text,
                media_paths=media_paths,
                missing_fields=focused_fields,
                instruction=(
                    f"{instruction}\n"
                    "This is a bounded verification pass for critical fields missed by the first "
                    "scan. Re-scan every supplied image and the full timeline of every supplied video. "
                    "Quote every distinct visible candidate in evidence; if more than one candidate "
                    "exists, use ambiguous. Use missing only when no explicit candidate is visible. "
                    "Quantity still requires an explicit quantity/pack/pcs label and must not be "
                    "derived from sizes."
                ).strip(),
                examples=examples,
            )
        except Exception:
            return result
        fields = dict(result.fields)
        for name in focused_fields:
            candidate = focused.fields.get(name)
            if candidate is not None and candidate.status is not FieldStatus.MISSING:
                fields[name] = candidate
        return result.model_copy(update={"fields": fields})

    async def _extract_once(
        self,
        *,
        text: str,
        media_paths: list[str],
        missing_fields: list[str],
        instruction: str,
        examples: list[dict[str, object]],
    ) -> GeminiExtraction:
        media_instruction = (
            "First perform OCR on every supplied image and across the full timeline of every "
            "supplied video; read all visible labels and text. "
            if media_paths
            else ""
        )
        prompt = (
            f"{media_instruction}"
            "Extract only the requested product facts explicitly supported by the supplied text/media. "
            "Return one fields item for every requested field, including missing or ambiguous fields. "
            "For every item, emit status, value, evidence, provenance, and currency (null unless it is a price). "
            "For a found image value, evidence must quote the exact visible text and provenance must be "
            "gemini_ocr. For a found message-text value, evidence must quote the exact supplied text and "
            "provenance must be gemini_text. Price value must be only the decimal amount; currency is USD "
            "or EUR only when explicitly shown, otherwise null. Preserve sizes exactly as shown. Quantity "
            "requires an explicit quantity/pack/pcs value, and its value must contain only the integer count; "
            "it must never be inferred from the number of sizes. "
            "Never infer or invent a product fact. Conflicting values are ambiguous. Use missing only when "
            "the requested fact is genuinely absent.\n"
            f"Requested fields: {json.dumps(missing_fields)}\n"
            f"Source instruction: {instruction}\n"
            f"Examples from this source only: {json.dumps(examples, ensure_ascii=False)}\n"
            f"Current text:\n{text}"
        )
        parts: list[Any] = [prompt]
        for path_value in media_paths:
            path = Path(path_value)
            data = await asyncio.to_thread(path.read_bytes)
            try:
                mime = _MEDIA_MIME_TYPES[path.suffix.lower()]
            except KeyError as exc:
                raise GeminiUnavailable(
                    f"Unsupported Gemini media type: {path.suffix.lower() or '<none>'}"
                ) from exc
            parts.append(types.Part.from_bytes(data=data, mime_type=mime))
        generation_config = types.GenerateContentConfig(
            response_mime_type="application/json",
            response_schema=_StructuredExtraction,
            temperature=0,
        )
        selected_model = self.ocr_model if media_paths else self.model
        try:
            response = await self._retry(
                self.client.aio.models.generate_content,
                model=selected_model,
                contents=parts,
                config=generation_config,
            )
        except GeminiUnavailable as exc:
            if not media_paths or selected_model == self.model or exc.code != 429:
                raise
            response = await self._retry(
                self.client.aio.models.generate_content,
                model=self.model,
                contents=parts,
                config=generation_config,
            )
        if not response.text:
            raise GeminiUnavailable("Gemini returned an empty extraction")
        return _StructuredExtraction.model_validate_json(response.text).to_domain()

    async def translate(
        self, text: str, target_language: str, instruction: str = ""
    ) -> str:
        language_name = {
            "RU": "Russian (ru-RU)",
            "EN": "English (en-US)",
        }.get(target_language.upper(), target_language)
        source_guidance = (
            "Source-specific verified guidance (apply only when relevant to translation): "
            f"{instruction}\n"
            if instruction.strip()
            else ""
        )
        response = await self._retry(
            self.client.aio.models.generate_content,
            model=self.model,
            contents=(
                f"Translate this supplier product description into natural {language_name}. "
                "First detect the source language. Translate every descriptive word by meaning, "
                "including words written in ALL CAPS. Never transliterate source-language words. "
                "Only strings matching __P<number>__ are protected placeholders; copy those strings "
                "byte-for-byte and do not output the word placeholder otherwise. Add no facts. "
                "Return only the translated description, with no explanation.\n"
                f"{source_guidance}"
                f"{text}"
            ),
            config=types.GenerateContentConfig(temperature=0),
        )
        if not response.text:
            raise GeminiUnavailable("Gemini returned an empty translation")
        return str(response.text).strip()

    async def _retry(self, function: Any, **kwargs: Any) -> Any:
        last_error: Exception | None = None
        for attempt in range(self.attempts):
            try:
                return await asyncio.wait_for(function(**kwargs), timeout=60)
            except Exception as exc:  # SDK exposes multiple transport exception families
                last_error = exc
                if attempt + 1 < self.attempts:
                    await asyncio.sleep(2**attempt)
        code = getattr(last_error, "code", None)
        status = getattr(last_error, "status", None)
        if code == 429:
            detail = f"{code} {status or 'RESOURCE_EXHAUSTED'}"
            raise GeminiUnavailable(
                f"Gemini quota/rate limit exhausted ({detail})", code=code
            ) from last_error
        if code in {401, 403}:
            raise GeminiUnavailable(
                f"Gemini authentication/permission failed ({code})", code=code
            ) from last_error
        if code == 404:
            raise GeminiUnavailable("Gemini model is unavailable (404)", code=code) from last_error
        if isinstance(last_error, TimeoutError):
            raise GeminiUnavailable("Gemini timed out after bounded retries") from last_error
        error_type = type(last_error).__name__ if last_error else "unknown error"
        raise GeminiUnavailable(
            f"Gemini unavailable after bounded retries ({error_type})"
        ) from last_error
