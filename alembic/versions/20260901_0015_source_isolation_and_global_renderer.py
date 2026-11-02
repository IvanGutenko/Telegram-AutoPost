"""Enforce source identity and retire fixed-text publication bypass.

Revision ID: 20260901_0015
Revises: 20260831_0014
"""

from collections.abc import Sequence

from alembic import op

revision: str = "20260901_0015"
down_revision: str | None = "20260831_0014"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Older deployments may still carry the short-lived fixed-text policy.
    # Those profiles previously skipped extraction and commonly contain only
    # media, so enable their existing admin-visible OCR primitives before
    # normalizing the destination. Selection is entirely data-driven: no
    # supplier or channel identity is encoded here.
    op.execute(
        """
        UPDATE source_profiles AS sp
        SET config = jsonb_set(
            jsonb_set(sp.config::jsonb, '{gemini_enabled}', 'true'::jsonb, true),
            '{ocr_mode}',
            '"always"'::jsonb,
            true
        )::json
        WHERE EXISTS (
            SELECT 1
            FROM destinations AS d
            WHERE d.publication_policy = 'media_fixed_text'
              AND (
                  sp.config->>'default_destination_id' = d.id::text
                  OR EXISTS (
                      SELECT 1
                      FROM jsonb_array_elements(
                          COALESCE(sp.config::jsonb->'routing_rules', '[]'::jsonb)
                      ) AS route
                      WHERE route->>'destination_id' = d.id::text
                  )
              )
        )
        """
    )

    # Normalize the old policy before the new runtime starts so every
    # destination reaches the one global four-line renderer.
    op.execute(
        "UPDATE destinations SET publication_policy = 'standard_product' "
        "WHERE publication_policy <> 'standard_product'"
    )

    op.create_unique_constraint(
        "uq_album_groups_id_source", "album_groups", ["id", "source_id"]
    )
    op.create_unique_constraint(
        "uq_source_messages_id_source", "source_messages", ["id", "source_id"]
    )
    op.create_foreign_key(
        "fk_source_messages_album_source",
        "source_messages",
        "album_groups",
        ["album_group_id", "source_id"],
        ["id", "source_id"],
    )
    op.create_foreign_key(
        "fk_message_transitions_message_source",
        "message_event_transitions",
        "source_messages",
        ["source_message_id", "source_id"],
        ["id", "source_id"],
    )
    op.create_foreign_key(
        "fk_products_message_source",
        "products",
        "source_messages",
        ["source_message_id", "source_id"],
        ["id", "source_id"],
    )
    op.create_foreign_key(
        "fk_products_album_source",
        "products",
        "album_groups",
        ["album_group_id", "source_id"],
        ["id", "source_id"],
    )


def downgrade() -> None:
    op.drop_constraint("fk_products_album_source", "products", type_="foreignkey")
    op.drop_constraint("fk_products_message_source", "products", type_="foreignkey")
    op.drop_constraint(
        "fk_message_transitions_message_source",
        "message_event_transitions",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_source_messages_album_source", "source_messages", type_="foreignkey"
    )
    op.drop_constraint("uq_source_messages_id_source", "source_messages", type_="unique")
    op.drop_constraint("uq_album_groups_id_source", "album_groups", type_="unique")
