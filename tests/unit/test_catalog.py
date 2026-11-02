from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin import router as admin_router
from emoda.catalog import router as catalog_router
from emoda.catalog.router import build_catalog_markup, visible_catalog_children
from emoda.db.base import Base
from emoda.db.models import CatalogNode
from emoda.domain.enums import ButtonAction, CatalogNodeType
from emoda.domain.schemas import CatalogNodeData, NavigationButtonData


def test_catalog_publication_button_does_not_require_external_url() -> None:
    button = NavigationButtonData(
        text="📂 Каталог",
        action=ButtonAction.CATALOG,
        row=0,
        order=1,
    )

    assert button.action is ButtonAction.CATALOG
    assert button.url is None


def test_url_publication_button_still_requires_safe_https_url() -> None:
    with pytest.raises(ValidationError):
        NavigationButtonData(text="🛒 Заказать", action=ButtonAction.URL, row=0, order=0)


def test_catalog_category_and_link_have_distinct_value_requirements() -> None:
    category = CatalogNodeData(
        text_ru="Женская одежда",
        text_en="Women's clothing",
        node_type=CatalogNodeType.CATEGORY,
    )
    link = CatalogNodeData(
        text_ru="Платья",
        text_en="Dresses",
        node_type=CatalogNodeType.LINK,
        url="https://t.me/e_moda_female",
        position=2,
    )

    assert category.url is None
    assert category.text_en == "Women's clothing"
    assert link.url == "https://t.me/e_moda_female"
    with pytest.raises(ValidationError):
        CatalogNodeData(
            text_ru="Пустая ссылка",
            text_en="Empty link",
            node_type=CatalogNodeType.LINK,
        )
    with pytest.raises(ValidationError):
        CatalogNodeData(
            text_ru="Небезопасная ссылка",
            text_en="Unsafe link",
            node_type=CatalogNodeType.LINK,
            url="javascript:alert(1)",
        )


def test_catalog_node_requires_both_language_labels() -> None:
    with pytest.raises(ValidationError):
        CatalogNodeData(text_ru="Одежда", node_type=CatalogNodeType.CATEGORY)


def test_catalog_node_model_has_self_referencing_tree() -> None:
    table = CatalogNode.__table__

    assert next(iter(table.c.parent_id.foreign_keys)).target_fullname == "catalog_nodes.id"
    assert {column.name for column in table.columns} >= {
        "parent_id",
        "text_ru",
        "text_en",
        "node_type",
        "url",
        "position",
        "enabled",
        "archived_at",
    }


def test_admin_archive_markup_restores_existing_catalog_node_id() -> None:
    archived = CatalogNode(
        id=42,
        text_ru="Женская одежда",
        text_en="Women's clothing",
        node_type=CatalogNodeType.CATEGORY,
        archived_at=datetime.now(UTC),
    )

    markup = admin_router.build_catalog_archive_markup([archived])

    assert markup.inline_keyboard[0][0].text == "↩️ 📁 Женская одежда / Women's clothing"
    assert markup.inline_keyboard[0][0].callback_data == "admincat:restore:42"
    assert markup.inline_keyboard[-1][0].callback_data == "menu:catalog"


def test_catalog_language_selector_opens_each_localized_root() -> None:
    markup = catalog_router.build_language_markup()

    assert markup.inline_keyboard[0][0].text == "Русский"
    assert markup.inline_keyboard[0][0].callback_data == "catalog:ru:0"
    assert markup.inline_keyboard[0][1].text == "English"
    assert markup.inline_keyboard[0][1].callback_data == "catalog:en:0"


def test_catalog_markup_uses_selected_language_and_preserves_it_in_callbacks() -> None:
    category = CatalogNode(
        id=10,
        text_ru="Одежда",
        text_en="Clothing",
        node_type=CatalogNodeType.CATEGORY,
        position=2,
    )
    link = CatalogNode(
        id=11,
        text_ru="Обувь",
        text_en="Shoes",
        node_type=CatalogNodeType.LINK,
        url="https://t.me/shoes",
        position=1,
    )

    markup = build_catalog_markup([category, link], current=None, language="en")

    assert markup.inline_keyboard[0][0].text == "Shoes"
    assert markup.inline_keyboard[0][0].url == "https://t.me/shoes"
    assert markup.inline_keyboard[0][1].text == "Clothing"
    assert markup.inline_keyboard[0][1].callback_data == "catalog:en:10"
    assert [len(row) for row in markup.inline_keyboard] == [2]


def test_catalog_markup_places_odd_last_child_in_its_own_row() -> None:
    nodes = [
        CatalogNode(
            id=node_id,
            text_ru=f"Раздел {node_id}",
            text_en=f"Section {node_id}",
            node_type=CatalogNodeType.CATEGORY,
            position=node_id,
        )
        for node_id in (1, 2, 3)
    ]

    markup = build_catalog_markup(nodes, current=None, language="ru")

    assert [len(row) for row in markup.inline_keyboard] == [2, 1]
    assert [button.text for row in markup.inline_keyboard for button in row] == [
        "Раздел 1",
        "Раздел 2",
        "Раздел 3",
    ]


def test_catalog_markup_renders_russian_labels_for_russian_selection() -> None:
    category = CatalogNode(
        id=12,
        text_ru="Мужская одежда",
        text_en="Men's clothing",
        node_type=CatalogNodeType.CATEGORY,
    )

    markup = build_catalog_markup([category], current=None, language="ru")

    assert markup.inline_keyboard[0][0].text == "Мужская одежда"
    assert markup.inline_keyboard[0][0].callback_data == "catalog:ru:12"


def test_nested_catalog_markup_has_private_back_and_home_navigation() -> None:
    current = CatalogNode(
        id=10,
        parent_id=7,
        text_ru="Платья",
        text_en="Dresses",
        node_type=CatalogNodeType.CATEGORY,
    )

    markup = build_catalog_markup([], current=current, language="en")

    assert markup.inline_keyboard[-1][0].text == "⬅️ Back"
    assert markup.inline_keyboard[-1][0].callback_data == "catalog:en:7"
    assert markup.inline_keyboard[-1][1].text == "🏠 Home"
    assert markup.inline_keyboard[-1][1].callback_data == "catalog:en:0"


def test_nested_catalog_children_are_paired_before_separate_navigation_row() -> None:
    current = CatalogNode(
        id=20,
        parent_id=7,
        text_ru="Одежда",
        text_en="Clothing",
        node_type=CatalogNodeType.CATEGORY,
    )
    children = [
        CatalogNode(
            id=node_id,
            parent_id=20,
            text_ru=f"Подраздел {node_id}",
            text_en=f"Subsection {node_id}",
            node_type=CatalogNodeType.CATEGORY,
            position=node_id,
        )
        for node_id in (21, 22, 23)
    ]

    markup = build_catalog_markup(children, current=current, language="ru")

    assert [len(row) for row in markup.inline_keyboard] == [2, 1, 2]
    assert [button.text for button in markup.inline_keyboard[-1]] == [
        "⬅️ Назад",
        "🏠 В начало",
    ]


async def test_public_catalog_query_hides_disabled_and_archived_nodes() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session:
        session.add_all(
            [
                CatalogNode(
                    text_ru="Второй",
                    text_en="Second",
                    node_type=CatalogNodeType.CATEGORY,
                    position=2,
                    enabled=True,
                ),
                CatalogNode(
                    text_ru="Первый",
                    text_en="First",
                    node_type=CatalogNodeType.CATEGORY,
                    position=1,
                    enabled=True,
                ),
                CatalogNode(
                    text_ru="Отключён",
                    text_en="Disabled",
                    node_type=CatalogNodeType.CATEGORY,
                    position=0,
                    enabled=False,
                ),
                CatalogNode(
                    text_ru="Архив",
                    text_en="Archived",
                    node_type=CatalogNodeType.CATEGORY,
                    position=0,
                    enabled=True,
                    archived_at=datetime.now(UTC),
                ),
            ]
        )
        await session.commit()

        nodes = await visible_catalog_children(session, None)

    await engine.dispose()
    assert [node.text_en for node in nodes] == ["First", "Second"]
