from enum import StrEnum


class FieldStatus(StrEnum):
    FOUND = "found"
    MISSING = "missing"
    AMBIGUOUS = "ambiguous"


class Provenance(StrEnum):
    STRICT_TEXT = "strict_text"
    SOURCE_RULE = "source_rule"
    COMPUTED_RULE = "computed_rule"
    GEMINI_TEXT = "gemini_text"
    GEMINI_OCR = "gemini_ocr"
    ADMIN = "admin_correction"


class PublicationMode(StrEnum):
    AUTO = "auto"
    REVIEW = "review"


class ButtonAction(StrEnum):
    URL = "url"
    CATALOG = "catalog"


class CatalogNodeType(StrEnum):
    CATEGORY = "category"
    LINK = "link"


class OCRMode(StrEnum):
    OFF = "off"
    MISSING_ONLY = "missing_only"
    ALWAYS = "always"


class ProductStatus(StrEnum):
    RECEIVED = "received"
    COLLECTING_ALBUM = "collecting_album"
    READY = "ready"
    PROCESSING = "processing"
    NEEDS_REVIEW = "needs_review"
    READY_TO_PUBLISH = "ready_to_publish"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    SKIPPED = "skipped"
    FAILED = "failed"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class OutboxStatus(StrEnum):
    PENDING = "pending"
    PUBLISHING = "publishing"
    PUBLISHED = "published"
    FAILED = "failed"
    RECONCILIATION_REQUIRED = "reconciliation_required"


class AlbumStatus(StrEnum):
    COLLECTING = "collecting"
    READY = "ready"
    PROCESSED = "processed"


class EditPolicy(StrEnum):
    AUTO = "auto"
    REVIEW = "review"
    AUTO_BEFORE_PUBLICATION = "auto_before_publication"
    IGNORE = "ignore"


class ReviewReasonCode(StrEnum):
    SOURCE_PUBLICATION_MODE_REVIEW = "source_publication_mode_review"
    PARSER_AMBIGUOUS = "parser_ambiguous"
    CONFLICTING_PRICE = "conflicting_price"
    GEMINI_EXTRACTION_FAILED = "gemini_extraction_failed"
    GEMINI_EVIDENCE_REJECTED = "gemini_evidence_rejected"
    TRANSLATION_FAILED = "translation_failed"
    DESTINATION_MISSING = "destination_missing"
    UNSAFE_OCR_CONFLICT = "unsafe_ocr_conflict"
    PUBLICATION_RECONCILIATION = "publication_reconciliation"
    PUBLICATION_FAILED = "publication_failed"
    MEDIA_DOWNLOAD_FAILED = "media_download_failed"
    SOURCE_EDIT_REVIEW = "source_edit_review"
    COLLECTION_AMBIGUOUS = "collection_ambiguous"
    SYSTEM_ERROR = "system_error"
    MANUAL_RULE = "manual_rule"


class MessageState(StrEnum):
    RECEIVED = "received"
    COLLECTING = "collecting"
    ATTACHED_TO_PRODUCT = "attached_to_product"
    IGNORED = "ignored"
    DUPLICATE = "duplicate"
    DEFERRED = "deferred"
    PROCESSING = "processing"
    PUBLISHED = "published"
    REVIEW = "review"
    FAILED = "failed"


class EventOrigin(StrEnum):
    LIVE = "live"
    CATCHUP = "catchup"
    RECOVERY = "recovery"
    ADMIN = "admin"
