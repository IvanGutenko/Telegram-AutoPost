import pytest

from emoda.domain.schemas import CleaningRule
from emoda.services.sanitizer import FinalSanitationError, FinalSanitizer


def test_final_sanitizer_applies_global_and_typed_source_rules() -> None:
    result = FinalSanitizer().sanitize(
        "Order Line: wa.me/901234567890\nRPT\nService: internal\nElegant dress",
        language="en",
        rules=[
            CleaningRule(kind="exact", value="RPT"),
            CleaningRule(kind="prefix", value="Service:"),
        ],
        validate_language=True,
    )
    assert result == "Elegant dress"


@pytest.mark.parametrize(
    "model_text",
    [
        "Please provide the descriptive product text to translate.",
        "I cannot translate without more context.",
        "```json\n{\"translation\": \"dress\"}\n```",
        "__DEBUG_PLACEHOLDER_1__",
    ],
)
def test_final_sanitizer_rejects_model_meta_and_debug_output(model_text: str) -> None:
    with pytest.raises(FinalSanitationError, match="model meta|placeholder|debug"):
        FinalSanitizer().sanitize(
            model_text,
            language="en",
            rules=[],
            validate_language=False,
        )


def test_final_sanitizer_rejects_wrong_target_language() -> None:
    with pytest.raises(FinalSanitationError, match="not Russian"):
        FinalSanitizer().sanitize(
            "Elegant summer dress",
            language="ru",
            rules=[],
            validate_language=True,
        )
