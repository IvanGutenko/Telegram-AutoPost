from emoda.admin.router import build_source_grid_rows, build_sources_markup
from emoda.db.models import Source


def source(source_id: int, name: str, *, enabled: bool = True) -> Source:
    return Source(
        id=source_id,
        name=name,
        telegram_channel_id=-100_000 - source_id,
        enabled=enabled,
    )


def source_row_names(rows: list[list[tuple[str, str]]]) -> list[list[str]]:
    return [[text.removeprefix("🟢 ").removeprefix("⚪ ") for text, _ in row] for row in rows]


def test_source_grid_uses_three_columns_for_short_names() -> None:
    items = [
        source(1, "Mybee"),
        source(2, "X-TOP"),
        source(3, "Squalo"),
        source(4, "Romaşka"),
        source(5, "Polo"),
        source(6, "Mortopuk"),
    ]

    assert source_row_names(build_source_grid_rows(items)) == [
        ["Mybee", "X-TOP", "Squalo"],
        ["Romaşka", "Polo", "Mortopuk"],
    ]


def test_source_grid_uses_two_columns_for_long_names() -> None:
    items = [
        source(1, "Strong Eagle"),
        source(2, "Supplier Two"),
        source(3, "Long Source"),
        source(4, "Another Source"),
    ]

    assert source_row_names(build_source_grid_rows(items)) == [
        ["Strong Eagle", "Supplier Two"],
        ["Long Source", "Another Source"],
    ]


def test_source_grid_mixes_two_and_three_column_rows_predictably() -> None:
    items = [
        source(1, "Strong Eagle"),
        source(2, "Mybee"),
        source(3, "X-TOP"),
        source(4, "Squalo"),
        source(5, "Romaşka"),
    ]

    assert source_row_names(build_source_grid_rows(items)) == [
        ["Strong Eagle", "Mybee"],
        ["X-TOP", "Squalo", "Romaşka"],
    ]


def test_source_grid_odd_count_avoids_an_unnecessary_single_button_row() -> None:
    five_items = [source(index, name) for index, name in enumerate("A B C D E".split(), 1)]
    four_items = five_items[:4]

    assert [len(row) for row in build_source_grid_rows(five_items)] == [3, 2]
    assert [len(row) for row in build_source_grid_rows(four_items)] == [2, 2]


def test_source_markup_keeps_pagination_and_service_buttons_outside_grid() -> None:
    page = [source(index, name) for index, name in enumerate("A B C D E".split(), 11)]
    pagination = [[("⬅️", "menu:sources:page:1"), ("➡️", "menu:sources:page:3")]]

    markup = build_sources_markup(page, navigation_rows=pagination)
    rows = markup.inline_keyboard

    assert [len(row) for row in rows] == [3, 2, 1, 1, 2, 1]
    assert [button.callback_data for button in rows[-4]] == ["source:add"]
    assert [button.callback_data for button in rows[-3]] == ["menu:sources:archived"]
    assert [button.callback_data for button in rows[-2]] == [
        "menu:sources:page:1",
        "menu:sources:page:3",
    ]
    assert [button.callback_data for button in rows[-1]] == ["menu:main"]


def test_source_grid_preserves_enabled_indicator_and_callback_identity() -> None:
    items = [
        source(91, "Mybee"),
        source(37, "X-TOP", enabled=False),
        source(808, "Squalo"),
    ]

    rows = build_source_grid_rows(items)

    assert rows == [
        [
            ("🟢 Mybee", "source:91"),
            ("⚪ X-TOP", "source:37"),
            ("🟢 Squalo", "source:808"),
        ]
    ]
