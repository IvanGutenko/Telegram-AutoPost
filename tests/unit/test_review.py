from emoda.domain.enums import ReviewReasonCode
from emoda.services.review import classify_review_messages, reason


def test_review_messages_are_classified_to_stable_codes_with_details() -> None:
    classified = classify_review_messages(
        [
            "ambiguous fields: price, sizes",
            "text/image conflict: color",
            "translation language: RU target language validation failed",
            "price currency unresolved",
        ]
    )
    assert [item.code for item in classified] == [
        ReviewReasonCode.PARSER_AMBIGUOUS,
        ReviewReasonCode.UNSAFE_OCR_CONFLICT,
        ReviewReasonCode.TRANSLATION_FAILED,
        ReviewReasonCode.CONFLICTING_PRICE,
    ]
    assert classified[0].field == "price, sizes"


def test_explicit_review_reason_uses_enum_not_free_text() -> None:
    item = reason(
        ReviewReasonCode.SOURCE_PUBLICATION_MODE_REVIEW,
        detail="source is configured for manual approval",
    )
    assert item.model_dump(mode="json")["code"] == "source_publication_mode_review"
