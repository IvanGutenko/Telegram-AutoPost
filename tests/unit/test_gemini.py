from types import SimpleNamespace
from typing import Any

import pytest

from emoda.domain.enums import FieldStatus, Provenance
from emoda.integrations.gemini import (
    GeminiUnavailable,
    GoogleGeminiAdapter,
    _StructuredExtraction,
)


class FakeModels:
    def __init__(self, response_text: str | None = None) -> None:
        self.response_text = response_text or (
            '{"fields": [{"name": "price", "status": "found", '
            '"value": "20", "evidence": "20 USD", '
            '"provenance": "gemini_text", "currency": "USD"}], '
            '"description": {"status": "missing", "value": null, '
            '"evidence": null, "provenance": null}, '
            '"category": {"status": "missing", "value": null, '
            '"evidence": null, "provenance": null}}'
        )

    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(text=self.response_text)


class RecordingModels(FakeModels):
    def __init__(self) -> None:
        super().__init__()
        self.calls: list[dict[str, Any]] = []

    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        self.calls.append(kwargs)
        return await super().generate_content(**kwargs)


class SequentialModels:
    def __init__(self, responses: list[str]) -> None:
        self.responses = iter(responses)

    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        return SimpleNamespace(text=next(self.responses))


class QuotaError(Exception):
    code = 429
    status = "RESOURCE_EXHAUSTED"


class QuotaModels:
    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        raise QuotaError("request contained operational quota details")


class OcrQuotaFallbackModels(FakeModels):
    def __init__(self) -> None:
        super().__init__()
        self.models: list[str] = []

    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        model = str(kwargs["model"])
        self.models.append(model)
        if model == "ocr-model":
            raise QuotaError("OCR model quota exhausted")
        return await super().generate_content(**kwargs)


class SemanticTranslationModels:
    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        instruction = str(kwargs["contents"])
        normalized = instruction.casefold()
        if (
            "russian (ru-ru)" in normalized
            and "never transliterate" in normalized
            and "only strings matching __p<number>__" in normalized
        ):
            return SimpleNamespace(text="Платье с декоративной пряжкой, новая коллекция.")
        return SimpleNamespace(text="ТОКА ДЕТАЙ ЕЛЬБИСЕ НЬЮ СЕЗОН")


class SourceGuidanceTranslationModels:
    async def generate_content(self, **kwargs: Any) -> SimpleNamespace:
        instruction = str(kwargs["contents"]).casefold()
        if (
            "source-specific verified guidance" in instruction
            and "dress with buckle detail" in instruction
        ):
            return SimpleNamespace(text="Платье с декоративной пряжкой.")
        return SimpleNamespace(text="ПЛАТЬЕ С ДЕТАЛЯМИ ТОКА")


@pytest.mark.asyncio
async def test_extract_converts_gemini_list_schema_to_domain_mapping() -> None:
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(aio=SimpleNamespace(models=FakeModels()))  # type: ignore[assignment]

    result = await adapter.extract(
        text="Price: 20 USD",
        media_paths=[],
        missing_fields=["price"],
        instruction="",
        examples=[],
    )

    assert result.fields["price"].status is FieldStatus.FOUND
    assert result.fields["price"].value == "20"
    assert result.fields["price"].evidence == "20 USD"
    assert result.fields["price"].provenance is Provenance.GEMINI_TEXT
    assert result.fields["price"].metadata == {"currency": "USD"}


def test_structured_schema_requires_complete_field_state() -> None:
    schema = _StructuredExtraction.model_json_schema()
    named_value = schema["$defs"]["_StructuredNamedValue"]

    assert set(named_value["required"]) == {
        "name",
        "status",
        "value",
        "evidence",
        "provenance",
        "currency",
    }


@pytest.mark.asyncio
async def test_extract_maps_structured_price_currency_to_domain_metadata() -> None:
    response = (
        '{"fields": [{"name": "price", "status": "found", "value": "10", '
        '"evidence": "$10", "provenance": "gemini_ocr", "currency": "USD"}], '
        '"description": {"status": "missing", "value": null, "evidence": null, '
        '"provenance": null}, "category": {"status": "missing", "value": null, '
        '"evidence": null, "provenance": null}}'
    )
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=FakeModels(response))
    )

    result = await adapter.extract(
        text="",
        media_paths=[],
        missing_fields=["price"],
        instruction="",
        examples=[],
    )

    assert result.fields["price"].value == "10"
    assert result.fields["price"].metadata == {"currency": "USD"}


@pytest.mark.asyncio
async def test_extract_normalizes_explicit_quantity_unit_to_count() -> None:
    response = (
        '{"fields": [{"name": "quantity", "status": "found", "value": "4 PCS", '
        '"evidence": "QUANTITY 4 PCS", "provenance": "gemini_ocr", "currency": null}], '
        '"description": {"status": "missing", "value": null, "evidence": null, '
        '"provenance": null}, "category": {"status": "missing", "value": null, '
        '"evidence": null, "provenance": null}}'
    )
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=FakeModels(response))
    )

    result = await adapter.extract(
        text="",
        media_paths=[],
        missing_fields=["quantity"],
        instruction="",
        examples=[],
    )

    assert result.fields["quantity"].value == "4"


@pytest.mark.asyncio
async def test_extract_uses_dedicated_ocr_model_for_images(tmp_path: Any) -> None:
    image_path = tmp_path / "tag.jpg"
    image_path.write_bytes(b"image bytes")
    models = RecordingModels()
    adapter = GoogleGeminiAdapter(
        "test-key",
        "text-model",
        ocr_model="ocr-model",
        attempts=1,
    )
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=models)
    )

    await adapter.extract(
        text="",
        media_paths=[str(image_path)],
        missing_fields=["price"],
        instruction="",
        examples=[],
    )

    assert models.calls[0]["model"] == "ocr-model"


@pytest.mark.asyncio
async def test_extract_sends_telegram_video_with_video_mime_type(tmp_path: Any) -> None:
    video_path = tmp_path / "supplier-video.mp4"
    video_path.write_bytes(b"video bytes")
    models = RecordingModels()
    adapter = GoogleGeminiAdapter("test-key", "text-model", ocr_model="ocr-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=models)
    )

    await adapter.extract(
        text="",
        media_paths=[str(video_path)],
        missing_fields=["price"],
        instruction="",
        examples=[],
    )

    video_part = models.calls[0]["contents"][1]
    assert video_part.inline_data.mime_type == "video/mp4"


@pytest.mark.asyncio
async def test_extract_falls_back_to_text_model_when_ocr_quota_is_exhausted(
    tmp_path: Any,
) -> None:
    image_path = tmp_path / "supplier-card.jpg"
    image_path.write_bytes(b"image bytes")
    models = OcrQuotaFallbackModels()
    adapter = GoogleGeminiAdapter(
        "test-key",
        "text-model",
        ocr_model="ocr-model",
        attempts=1,
    )
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=models)
    )

    result = await adapter.extract(
        text="",
        media_paths=[str(image_path)],
        missing_fields=["price"],
        instruction="",
        examples=[],
    )

    assert models.models == ["ocr-model", "text-model"]
    assert result.fields["price"].value == "20"


@pytest.mark.asyncio
async def test_extract_rechecks_missing_critical_ocr_fields_with_focused_prompt(
    tmp_path: Any,
) -> None:
    image_path = tmp_path / "supplier-card.jpg"
    image_path.write_bytes(b"image bytes")
    initial = (
        '{"fields": ['
        '{"name": "price", "status": "missing", "value": null, '
        '"evidence": null, "provenance": null, "currency": null},'
        '{"name": "sizes", "status": "found", "value": "36-42", '
        '"evidence": "36-42", "provenance": "gemini_ocr", "currency": null},'
        '{"name": "quantity", "status": "missing", "value": null, '
        '"evidence": null, "provenance": null, "currency": null}], '
        '"description": {"status": "missing", "value": null, "evidence": null, '
        '"provenance": null}, "category": {"status": "missing", "value": null, '
        '"evidence": null, "provenance": null}}'
    )
    focused = (
        '{"fields": ['
        '{"name": "price", "status": "ambiguous", "value": null, '
        '"evidence": "27 $\\n50 $", "provenance": "gemini_ocr", "currency": null},'
        '{"name": "quantity", "status": "missing", "value": null, '
        '"evidence": null, "provenance": null, "currency": null}], '
        '"description": {"status": "missing", "value": null, "evidence": null, '
        '"provenance": null}, "category": {"status": "missing", "value": null, '
        '"evidence": null, "provenance": null}}'
    )
    adapter = GoogleGeminiAdapter("test-key", "text-model", ocr_model="ocr-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=SequentialModels([initial, focused]))
    )

    result = await adapter.extract(
        text="",
        media_paths=[str(image_path)],
        missing_fields=["price", "sizes", "quantity"],
        instruction="",
        examples=[],
    )

    assert result.fields["price"].status is FieldStatus.AMBIGUOUS
    assert result.fields["price"].evidence == "27 $\n50 $"
    assert result.fields["sizes"].value == "36-42"
    assert result.fields["quantity"].status is FieldStatus.MISSING


@pytest.mark.asyncio
async def test_extract_marks_conflicting_duplicate_structured_fields_ambiguous() -> None:
    response = (
        '{"fields": ['
        '{"name": "price", "status": "found", "value": "20 USD", '
        '"evidence": "20 USD", "provenance": "gemini_text", "currency": "USD"},'
        '{"name": "price", "status": "found", "value": "25 USD", '
        '"evidence": "25 USD", "provenance": "gemini_ocr", "currency": "USD"}],'
        '"description": {"status": "missing", "value": null, "evidence": null, '
        '"provenance": null}, "category": {"status": "missing", "value": null, '
        '"evidence": null, "provenance": null}}'
    )
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=FakeModels(response))
    )

    result = await adapter.extract(
        text="Price: 20 USD",
        media_paths=[],
        missing_fields=["price"],
        instruction="",
        examples=[],
    )

    assert result.fields["price"].status is FieldStatus.AMBIGUOUS
    assert result.fields["price"].value is None
    assert result.fields["price"].evidence == "20 USD | 25 USD"


@pytest.mark.asyncio
async def test_extract_deduplicates_identical_structured_fields() -> None:
    response = (
        '{"fields": ['
        '{"name": "brand", "status": "found", "value": "Nike", '
        '"evidence": "Nike", "provenance": "gemini_text", "currency": null},'
        '{"name": "brand", "status": "found", "value": "Nike", '
        '"evidence": "Nike", "provenance": "gemini_text", "currency": null}],'
        '"description": {"status": "missing", "value": null, "evidence": null, '
        '"provenance": null}, "category": {"status": "missing", "value": null, '
        '"evidence": null, "provenance": null}}'
    )
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=FakeModels(response))
    )

    result = await adapter.extract(
        text="Nike",
        media_paths=[],
        missing_fields=["brand"],
        instruction="",
        examples=[],
    )

    assert result.fields["brand"].status is FieldStatus.FOUND
    assert result.fields["brand"].value == "Nike"


@pytest.mark.asyncio
async def test_quota_failure_is_reported_without_hiding_the_root_cause() -> None:
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=QuotaModels())
    )

    with pytest.raises(
        GeminiUnavailable,
        match=r"Gemini quota/rate limit exhausted \(429 RESOURCE_EXHAUSTED\)",
    ):
        await adapter.translate("Dress", "RU")


@pytest.mark.asyncio
async def test_translation_requests_semantic_target_language_instead_of_transliteration() -> None:
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=SemanticTranslationModels())
    )

    translated = await adapter.translate("TOKA DETAY ELBISE NEW SEZON", "RU")

    assert translated == "Платье с декоративной пряжкой, новая коллекция."


@pytest.mark.asyncio
async def test_translation_includes_source_profile_guidance_when_provided() -> None:
    adapter = GoogleGeminiAdapter("test-key", "test-model", attempts=1)
    adapter.client = SimpleNamespace(  # type: ignore[assignment]
        aio=SimpleNamespace(models=SourceGuidanceTranslationModels())
    )

    translated = await adapter.translate(
        "TOKA DETAY ELBISE",
        "RU",
        instruction="Translate as a dress with buckle detail; never transliterate toka.",
    )

    assert translated == "Платье с декоративной пряжкой."
