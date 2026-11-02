"""Add admin-configurable Telegram catalog menu.

Revision ID: 20260825_0004
Revises: 20260825_0003
"""

from collections.abc import Sequence

import sqlalchemy as sa

from alembic import op

revision: str = "20260825_0004"
down_revision: str | None = "20260825_0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("navigation_buttons") as batch:
        batch.add_column(
            sa.Column(
                "action",
                sa.Enum("url", "catalog", name="button_action", native_enum=False),
                server_default="url",
                nullable=False,
            )
        )
        batch.alter_column("url", existing_type=sa.Text(), nullable=True)
        batch.create_check_constraint(
            "ck_navigation_buttons_action",
            "action IN ('url', 'catalog') AND ((action = 'url' AND url IS NOT NULL) OR (action = 'catalog' AND url IS NULL))",
        )

    op.create_table(
        "catalog_nodes",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("parent_id", sa.Integer(), nullable=True),
        sa.Column("text", sa.String(length=64), nullable=False),
        sa.Column(
            "node_type",
            sa.Enum("category", "link", name="catalog_node_type", native_enum=False),
            nullable=False,
        ),
        sa.Column("url", sa.Text(), nullable=True),
        sa.Column("position", sa.Integer(), server_default="0", nullable=False),
        sa.Column("enabled", sa.Boolean(), server_default="true", nullable=False),
        sa.Column("archived_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("CURRENT_TIMESTAMP"),
            nullable=False,
        ),
        sa.CheckConstraint("position >= 0", name="ck_catalog_nodes_position"),
        sa.CheckConstraint(
            "node_type IN ('category', 'link') AND ((node_type = 'category' AND url IS NULL) OR (node_type = 'link' AND url IS NOT NULL))",
            name="ck_catalog_nodes_type_url",
        ),
        sa.ForeignKeyConstraint(["parent_id"], ["catalog_nodes.id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_catalog_nodes_parent_id", "catalog_nodes", ["parent_id"])
    op.create_index(
        "ix_catalog_nodes_parent_position",
        "catalog_nodes",
        ["parent_id", "position", "id"],
    )


def downgrade() -> None:
    op.drop_index("ix_catalog_nodes_parent_position", table_name="catalog_nodes")
    op.drop_index("ix_catalog_nodes_parent_id", table_name="catalog_nodes")
    op.drop_table("catalog_nodes")
    with op.batch_alter_table("navigation_buttons") as batch:
        batch.drop_constraint("ck_navigation_buttons_action", type_="check")
    op.execute(
        "UPDATE navigation_buttons SET url = 'https://t.me/' WHERE action = 'catalog' AND url IS NULL"
    )
    with op.batch_alter_table("navigation_buttons") as batch:
        batch.alter_column("url", existing_type=sa.Text(), nullable=False)
        batch.drop_column("action")
