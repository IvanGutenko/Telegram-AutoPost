from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, cast

from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    column,
    event,
    insert,
    text,
)
from sqlalchemy import cast as sql_cast
from sqlalchemy.orm import Mapped, mapped_column, relationship

from emoda.db.base import Base, TimestampMixin
from emoda.domain.enums import (
    AlbumStatus,
    ButtonAction,
    CatalogNodeType,
    EventOrigin,
    MessageState,
    OutboxStatus,
    ProductStatus,
    PublicationMode,
)
from emoda.rules.currency import SYSTEM_CURRENCY_ALIASES, normalize_currency_alias_key


def enum_type(enum: type[Any], name: str) -> Enum:
    return Enum(
        enum,
        name=name,
        native_enum=False,
        values_callable=lambda members: [item.value for item in members],
    )


class AdminUser(Base, TimestampMixin):
    __tablename__ = "admin_users"
    id: Mapped[int] = mapped_column(primary_key=True)
    telegram_user_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    display_name: Mapped[str | None] = mapped_column(String(255))
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")


class Destination(Base, TimestampMixin):
    __tablename__ = "destinations"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True)
    telegram_channel_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")


class BulkSendBatch(Base, TimestampMixin):
    __tablename__ = "bulk_send_batches"
    id: Mapped[int] = mapped_column(primary_key=True)
    admin_user_id: Mapped[int] = mapped_column(ForeignKey("admin_users.id"))
    text: Mapped[str] = mapped_column(Text)


class BulkSendTarget(Base, TimestampMixin):
    __tablename__ = "bulk_send_targets"
    __table_args__ = (UniqueConstraint("batch_id", "source_id"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(ForeignKey("bulk_send_batches.id"), index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    channel_id: Mapped[int] = mapped_column(BigInteger)
    source_name: Mapped[str] = mapped_column(String(255))
    random_id: Mapped[int] = mapped_column(BigInteger, unique=True)
    status: Mapped[str] = mapped_column(String(20), default="pending")
    message_ids: Mapped[list[int]] = mapped_column(JSON, default=list)
    error: Mapped[str | None] = mapped_column(Text)


class Source(Base, TimestampMixin):
    __tablename__ = "sources"
    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(255))
    telegram_channel_id: Mapped[int] = mapped_column(BigInteger, unique=True, index=True)
    telegram_ref: Mapped[str | None] = mapped_column(String(255))
    enabled: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    catchup_anchor_message_id: Mapped[int | None] = mapped_column(BigInteger)
    catchup_cursor_message_id: Mapped[int | None] = mapped_column(BigInteger)
    catchup_scan_before_id: Mapped[int | None] = mapped_column(BigInteger)
    catchup_error: Mapped[str | None] = mapped_column(Text)
    last_seen_message_id: Mapped[int | None] = mapped_column(BigInteger)
    last_catchup_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    profile: Mapped[SourceProfile] = relationship(
        back_populates="source", cascade="all, delete-orphan", uselist=False, lazy="selectin"
    )


class SourceProfile(Base, TimestampMixin):
    __tablename__ = "source_profiles"
    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(
        ForeignKey("sources.id", ondelete="CASCADE"), unique=True
    )
    schema_version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    publication_mode: Mapped[PublicationMode] = mapped_column(
        enum_type(PublicationMode, "publication_mode"), default=PublicationMode.REVIEW
    )
    config: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    version: Mapped[int] = mapped_column(Integer, default=1, server_default="1")
    source: Mapped[Source] = relationship(back_populates="profile")


class SourceExample(Base, TimestampMixin):
    __tablename__ = "source_examples"
    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"), index=True)
    original_input: Mapped[str] = mapped_column(Text)
    expected_values: Mapped[dict[str, Any]] = mapped_column(JSON)
    admin_comment: Mapped[str | None] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")


class AlbumGroup(Base, TimestampMixin):
    __tablename__ = "album_groups"
    __table_args__ = (
        UniqueConstraint("source_id", "grouped_id"),
        UniqueConstraint("id", "source_id", name="uq_album_groups_id_source"),
        Index("ix_album_ready", "status", "ready_at"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(
        ForeignKey("sources.id", ondelete="RESTRICT"), index=True
    )
    grouped_id: Mapped[int] = mapped_column(BigInteger)
    status: Mapped[AlbumStatus] = mapped_column(
        enum_type(AlbumStatus, "album_status"), default=AlbumStatus.COLLECTING
    )
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ready_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    product_id: Mapped[int | None] = mapped_column(
        ForeignKey("products.id", use_alter=True, name="fk_album_product")
    )


class SourceMessage(Base, TimestampMixin):
    __tablename__ = "source_messages"
    __table_args__ = (
        UniqueConstraint("source_id", "telegram_message_id"),
        UniqueConstraint("id", "source_id", name="uq_source_messages_id_source"),
        ForeignKeyConstraint(
            ["album_group_id", "source_id"],
            ["album_groups.id", "album_groups.source_id"],
            name="fk_source_messages_album_source",
        ),
        Index("ix_source_messages_album_message", "album_group_id", "telegram_message_id"),
        Index(
            "ix_source_messages_pending_media", "album_group_id", "id",
            postgresql_where=text("media_pending IS TRUE"),
        ),
        Index(
            "ix_source_messages_unprocessed_single", "source_id", "telegram_date",
            "telegram_message_id",
            postgresql_where=text(
                "grouped_id IS NULL AND album_group_id IS NULL "
                "AND processed_at IS NULL AND media_pending IS FALSE"
            ),
        ),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(
        ForeignKey("sources.id", ondelete="RESTRICT"), index=True
    )
    telegram_message_id: Mapped[int] = mapped_column(BigInteger, index=True)
    grouped_id: Mapped[int | None] = mapped_column(BigInteger, index=True)
    album_group_id: Mapped[int | None] = mapped_column(
        ForeignKey("album_groups.id", ondelete="SET NULL")
    )
    text: Mapped[str] = mapped_column(Text, default="", server_default="")
    caption: Mapped[str] = mapped_column(Text, default="", server_default="")
    telegram_date: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    edit_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    product_link: Mapped[str | None] = mapped_column(Text)
    raw_metadata: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    state: Mapped[MessageState] = mapped_column(
        enum_type(MessageState, "message_state"),
        default=MessageState.RECEIVED,
        server_default="received",
    )
    state_reason: Mapped[str | None] = mapped_column(Text)
    event_origin: Mapped[EventOrigin] = mapped_column(
        enum_type(EventOrigin, "event_origin"),
        default=EventOrigin.LIVE,
        server_default="live",
    )
    event_type: Mapped[str] = mapped_column(String(20), default="new", server_default="new")
    media_pending: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    processing_after: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MessageEventTransition(Base):
    __tablename__ = "message_event_transitions"
    __table_args__ = (
        Index("ix_message_event_correlation", "source_id", "telegram_message_id"),
        ForeignKeyConstraint(
            ["source_message_id", "source_id"],
            ["source_messages.id", "source_messages.source_id"],
            name="fk_message_transitions_message_source",
        ),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    source_message_id: Mapped[int] = mapped_column(
        ForeignKey("source_messages.id", ondelete="CASCADE"), index=True
    )
    source_id: Mapped[int] = mapped_column(
        ForeignKey("sources.id", ondelete="RESTRICT"), index=True
    )
    telegram_message_id: Mapped[int] = mapped_column(BigInteger)
    state: Mapped[MessageState] = mapped_column(enum_type(MessageState, "transition_state"))
    reason: Mapped[str | None] = mapped_column(Text)
    origin: Mapped[EventOrigin] = mapped_column(enum_type(EventOrigin, "transition_origin"))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class SourceMessageVersion(Base):
    __tablename__ = "source_message_versions"
    id: Mapped[int] = mapped_column(primary_key=True)
    source_message_id: Mapped[int] = mapped_column(
        ForeignKey("source_messages.id", ondelete="CASCADE"), index=True
    )
    text: Mapped[str] = mapped_column(Text)
    caption: Mapped[str] = mapped_column(Text)
    edit_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    captured_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


class MediaItem(Base, TimestampMixin):
    __tablename__ = "media_items"
    __table_args__ = (
        UniqueConstraint("source_message_id", "position"),
        Index("ix_media_retention_queue", "local_deleted_at", "updated_at"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    source_message_id: Mapped[int] = mapped_column(
        ForeignKey("source_messages.id", ondelete="CASCADE"), index=True
    )
    telegram_message_id: Mapped[int] = mapped_column(BigInteger)
    position: Mapped[int] = mapped_column(Integer)
    media_type: Mapped[str] = mapped_column(String(50), default="photo")
    storage_path: Mapped[str] = mapped_column(Text)
    telegram_file_id: Mapped[str | None] = mapped_column(Text)
    size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    sha256: Mapped[str | None] = mapped_column(String(64))
    metadata_json: Mapped[dict[str, Any]] = mapped_column("metadata", JSON, default=dict)
    active: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    local_deleted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class Product(Base, TimestampMixin):
    __tablename__ = "products"
    __table_args__ = (
        UniqueConstraint("source_message_id"),
        UniqueConstraint("album_group_id"),
        ForeignKeyConstraint(
            ["source_message_id", "source_id"],
            ["source_messages.id", "source_messages.source_id"],
            name="fk_products_message_source",
        ),
        ForeignKeyConstraint(
            ["album_group_id", "source_id"],
            ["album_groups.id", "album_groups.source_id"],
            name="fk_products_album_source",
        ),
        Index("ix_products_status", "status"),
        Index(
            "ix_products_admin_search_trgm",
            "emoda_code", "original_text", "cleaned_text", "final_text",
            sql_cast(column("parsed_result"), String).label("parsed_result_search"),
            postgresql_using="gin",
            postgresql_ops={name: "gin_trgm_ops" for name in (
                "emoda_code", "original_text", "cleaned_text", "final_text", "parsed_result_search"
            )},
        ),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    emoda_code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    source_id: Mapped[int] = mapped_column(
        ForeignKey("sources.id", ondelete="RESTRICT"), index=True
    )
    source_message_id: Mapped[int | None] = mapped_column(
        ForeignKey("source_messages.id", ondelete="RESTRICT")
    )
    album_group_id: Mapped[int | None] = mapped_column(
        ForeignKey("album_groups.id", ondelete="RESTRICT")
    )
    destination_id: Mapped[int | None] = mapped_column(
        ForeignKey("destinations.id", ondelete="RESTRICT")
    )
    status: Mapped[ProductStatus] = mapped_column(
        enum_type(ProductStatus, "product_status"), default=ProductStatus.RECEIVED
    )
    original_text: Mapped[str] = mapped_column(Text, default="", server_default="")
    cleaned_text: Mapped[str] = mapped_column(Text, default="", server_default="")
    final_text: Mapped[str] = mapped_column(Text, default="", server_default="")
    parsed_result: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    review_reasons: Mapped[list[dict[str, Any]]] = mapped_column(JSON, default=list)
    raw_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    raw_currency: Mapped[str | None] = mapped_column(String(3))
    normalized_usd: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    applied_markup: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    final_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 0))
    processing_error: Mapped[str | None] = mapped_column(Text)
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    review_notified_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class MarkupRule(Base, TimestampMixin):
    __tablename__ = "markup_rules"
    id: Mapped[int] = mapped_column(primary_key=True)
    min_price: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    max_price: Mapped[Decimal | None] = mapped_column(Numeric(18, 4))
    markup: Mapped[Decimal] = mapped_column(Numeric(18, 4))
    markup_type: Mapped[str] = mapped_column(String(16), default="fixed", server_default="fixed")
    priority: Mapped[int] = mapped_column(Integer, unique=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")


class NavigationButton(Base, TimestampMixin):
    __tablename__ = "navigation_buttons"
    __table_args__ = (
        CheckConstraint(
            "action IN ('url', 'catalog') AND ((action = 'url' AND url IS NOT NULL) OR (action = 'catalog' AND url IS NULL))",
            name="ck_navigation_buttons_action",
        ),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    text: Mapped[str] = mapped_column(String(64))
    action: Mapped[ButtonAction] = mapped_column(
        enum_type(ButtonAction, "button_action"), default=ButtonAction.URL, server_default="url"
    )
    url: Mapped[str | None] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class CatalogNode(Base, TimestampMixin):
    __tablename__ = "catalog_nodes"
    __table_args__ = (
        CheckConstraint("position >= 0", name="ck_catalog_nodes_position"),
        CheckConstraint(
            "node_type IN ('category', 'link') AND ((node_type = 'category' AND url IS NULL) OR (node_type = 'link' AND url IS NOT NULL))",
            name="ck_catalog_nodes_type_url",
        ),
        Index("ix_catalog_nodes_parent_position", "parent_id", "position", "id"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    parent_id: Mapped[int | None] = mapped_column(
        ForeignKey("catalog_nodes.id", ondelete="RESTRICT"), index=True
    )
    text_ru: Mapped[str] = mapped_column(String(64))
    text_en: Mapped[str] = mapped_column(String(64))
    node_type: Mapped[CatalogNodeType] = mapped_column(
        enum_type(CatalogNodeType, "catalog_node_type")
    )
    url: Mapped[str | None] = mapped_column(Text)
    position: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    archived_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class DestinationButtonBinding(Base):
    __tablename__ = "destination_button_bindings"
    __table_args__ = (UniqueConstraint("destination_id", "button_id"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    destination_id: Mapped[int] = mapped_column(ForeignKey("destinations.id", ondelete="CASCADE"))
    button_id: Mapped[int] = mapped_column(ForeignKey("navigation_buttons.id", ondelete="CASCADE"))
    row: Mapped[int] = mapped_column(Integer)
    order: Mapped[int] = mapped_column(Integer)


class PublicationOutbox(Base, TimestampMixin):
    __tablename__ = "publication_outbox"
    __table_args__ = (
        UniqueConstraint("product_id", "destination_id", "payload_hash"),
        Index("ix_outbox_status", "status"),
    )
    id: Mapped[int] = mapped_column(primary_key=True)
    product_id: Mapped[int] = mapped_column(
        ForeignKey("products.id", ondelete="RESTRICT"), index=True
    )
    destination_id: Mapped[int] = mapped_column(ForeignKey("destinations.id", ondelete="RESTRICT"))
    payload: Mapped[dict[str, Any]] = mapped_column(JSON)
    payload_hash: Mapped[str] = mapped_column(String(64))
    keyboard_snapshot: Mapped[list[list[dict[str, str]]]] = mapped_column(JSON, default=list)
    status: Mapped[OutboxStatus] = mapped_column(
        enum_type(OutboxStatus, "outbox_status"), default=OutboxStatus.PENDING
    )
    telegram_message_ids: Mapped[list[int]] = mapped_column(JSON, default=list)
    random_ids: Mapped[list[int]] = mapped_column(JSON, default=list)
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)
    publishing_started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class PublicationAttempt(Base):
    __tablename__ = "publication_attempts"
    id: Mapped[int] = mapped_column(primary_key=True)
    outbox_id: Mapped[int] = mapped_column(
        ForeignKey("publication_outbox.id", ondelete="CASCADE"), index=True
    )
    attempt_no: Mapped[int] = mapped_column(Integer)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    outcome: Mapped[str | None] = mapped_column(String(50))
    error: Mapped[str | None] = mapped_column(Text)


class TestRun(Base, TimestampMixin):
    __tablename__ = "test_runs"
    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"), index=True)
    admin_user_id: Mapped[int] = mapped_column(ForeignKey("admin_users.id", ondelete="RESTRICT"))
    original_input: Mapped[str] = mapped_column(Text)
    result: Mapped[dict[str, Any]] = mapped_column(JSON, default=dict)
    corrected_result: Mapped[dict[str, Any] | None] = mapped_column(JSON)


class AdminTestBatch(Base, TimestampMixin):
    __tablename__ = "admin_test_batches"
    __table_args__ = (Index("ix_admin_test_retention_queue", "status", "updated_at"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id", ondelete="CASCADE"), index=True)
    admin_user_id: Mapped[int] = mapped_column(
        ForeignKey("admin_users.id", ondelete="RESTRICT"), index=True
    )
    status: Mapped[str] = mapped_column(
        String(20), default="collecting", server_default="collecting"
    )
    test_run_id: Mapped[int | None] = mapped_column(ForeignKey("test_runs.id", ondelete="SET NULL"))


class AdminTestItem(Base, TimestampMixin):
    __tablename__ = "admin_test_items"
    __table_args__ = (UniqueConstraint("batch_id", "telegram_chat_id", "telegram_message_id"),)
    id: Mapped[int] = mapped_column(primary_key=True)
    batch_id: Mapped[int] = mapped_column(
        ForeignKey("admin_test_batches.id", ondelete="CASCADE"), index=True
    )
    telegram_chat_id: Mapped[int] = mapped_column(BigInteger)
    telegram_message_id: Mapped[int] = mapped_column(BigInteger)
    media_group_id: Mapped[str | None] = mapped_column(String(255))
    text: Mapped[str] = mapped_column(Text, default="", server_default="")
    media_paths: Mapped[list[str]] = mapped_column(JSON, default=list)
    media_pending: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")
    media_error: Mapped[str | None] = mapped_column(Text)


class GlobalSetting(Base, TimestampMixin):
    __tablename__ = "global_settings"
    key: Mapped[str] = mapped_column(String(100), primary_key=True)
    value: Mapped[dict[str, Any]] = mapped_column(JSON)


class CurrencyAlias(Base, TimestampMixin):
    __tablename__ = "currency_aliases"
    id: Mapped[int] = mapped_column(primary_key=True)
    alias: Mapped[str] = mapped_column(String(64))
    normalized_alias: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    currency: Mapped[str] = mapped_column(String(3), index=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, server_default="true")
    system: Mapped[bool] = mapped_column(Boolean, default=False, server_default="false")


@event.listens_for(CurrencyAlias.__table__, "after_create")
def _seed_currency_aliases_for_metadata_create(
    _target: object, connection: Any, **_kwargs: object
) -> None:
    """Keep test/dev ``metadata.create_all`` equivalent to the Alembic seed."""

    connection.execute(
        insert(cast(Any, CurrencyAlias.__table__)),
        [
            {
                "alias": alias,
                "normalized_alias": normalize_currency_alias_key(alias),
                "currency": currency,
                "enabled": True,
                "system": True,
            }
            for alias, currency in SYSTEM_CURRENCY_ALIASES
        ],
    )


class CodeCounter(Base):
    __tablename__ = "code_counters"
    name: Mapped[str] = mapped_column(String(100), primary_key=True)
    next_value: Mapped[int] = mapped_column(BigInteger, default=1, server_default="1")


class AuditLog(Base):
    __tablename__ = "audit_log"
    id: Mapped[int] = mapped_column(primary_key=True)
    admin_user_id: Mapped[int | None] = mapped_column(
        ForeignKey("admin_users.id", ondelete="SET NULL")
    )
    action: Mapped[str] = mapped_column(String(100), index=True)
    entity_type: Mapped[str] = mapped_column(String(100))
    entity_id: Mapped[str] = mapped_column(String(100))
    before: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    after: Mapped[dict[str, Any] | None] = mapped_column(JSON)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
