"""Button-driven rule editing must never lose, duplicate or misplace a rule.

Every case here is a click sequence a real admin produces by accident: going
back, saving twice, editing one of two rules of the same field, moving a rule to
another field, resetting an optional setting, and cancelling.
"""

from __future__ import annotations

from test_admin_rule_wizard import (
    NICCY_PATTERN,
    FakeCallback,
    FakeIncoming,
    FakeState,
    build_router,
    callback_handler,
    callbacks_of,
    message_handler,
    stored_profile,
)

from emoda.admin.service import profile_item_token
from emoda.domain.schemas import DEFAULT_SERIES_TOKEN_PATTERN, ExtractionRule


async def open_builder(router, source_id: int, state: FakeState) -> FakeCallback:
    opened = FakeCallback(f"source:rule:{source_id}")
    await callback_handler(router, "source_rule_start")(opened, state)
    return opened


async def test_back_button_returns_to_the_draft_without_losing_it(tmp_path) -> None:
    engine, _, router, source_id = await build_router(tmp_path)
    state = FakeState()
    await open_builder(router, source_id, state)
    await callback_handler(router, "rule_builder_set")(FakeCallback("rb:field:sizes"), state)
    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:pattern"), state)

    returned = FakeCallback("rb:back")
    await callback_handler(router, "rule_builder_back")(returned, state)
    text, markup = returned.message.edits[-1]

    assert "field=sizes" in text
    assert "rb:save" in callbacks_of(markup)
    assert state.data["rule_awaiting"] is None
    await engine.dispose()


async def test_saving_twice_does_not_create_a_second_rule(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()
    await open_builder(router, source_id, state)
    await message_handler(router, "source_rule_save")(
        FakeIncoming(f"field=price kind=regex pattern={NICCY_PATTERN} priority=11"), state
    )
    repeated = FakeCallback("rb:save")
    await callback_handler(router, "rule_builder_save")(repeated, state)

    profile = await stored_profile(sessions, source_id)
    assert len(profile.extraction_rules["price"]) == 1
    assert any(answer[1] for answer in repeated.answers)
    await engine.dispose()


async def test_editing_the_second_rule_of_a_field_leaves_the_first_untouched(tmp_path) -> None:
    first = ExtractionRule(kind="regex", pattern=NICCY_PATTERN, priority=11)
    second = ExtractionRule(kind="regex", pattern=r"(?P<value>\d+)\s*TL", priority=90)
    engine, sessions, router, source_id = await build_router(
        tmp_path, rules={"price": [first, second]}
    )
    state = FakeState()
    token = profile_item_token(second)

    await callback_handler(router, "source_rule_edit_start")(
        FakeCallback(f"r:e:{source_id}:price:1:{token}"), state
    )
    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:priority"), state)
    await message_handler(router, "source_rule_edit_save")(FakeIncoming("42"), state)
    await callback_handler(router, "rule_builder_save")(FakeCallback("rb:save"), state)

    stored = (await stored_profile(sessions, source_id)).extraction_rules["price"]
    assert [rule.pattern for rule in stored] == [first.pattern, second.pattern]
    assert [rule.priority for rule in stored] == [11, 42]
    await engine.dispose()


async def test_changing_the_field_moves_the_rule_and_keeps_the_pattern(tmp_path) -> None:
    rule = ExtractionRule(kind="regex", pattern=NICCY_PATTERN, priority=11)
    engine, sessions, router, source_id = await build_router(tmp_path, rules={"price": [rule]})
    state = FakeState()

    await callback_handler(router, "source_rule_edit_start")(
        FakeCallback(f"r:e:{source_id}:price:0:{profile_item_token(rule)}"), state
    )
    await callback_handler(router, "rule_builder_set")(FakeCallback("rb:field:article"), state)
    await callback_handler(router, "rule_builder_save")(FakeCallback("rb:save"), state)

    profile = await stored_profile(sessions, source_id)
    assert "price" not in profile.extraction_rules
    assert profile.extraction_rules["article"][0].pattern == NICCY_PATTERN
    await engine.dispose()


async def test_dash_resets_an_optional_setting_to_its_default(tmp_path) -> None:
    rule = ExtractionRule(
        kind="regex",
        pattern=r"(?i)Series:\s*(?P<value>(?:[A-Z]+\d+\s*)+)",
        transform="series_sizes",
        token_pattern=r"(?P<size>[A-Z]+)(?P<count>\d+)",
        priority=11,
    )
    engine, sessions, router, source_id = await build_router(tmp_path, rules={"sizes": [rule]})
    state = FakeState()

    await callback_handler(router, "source_rule_edit_start")(
        FakeCallback(f"r:e:{source_id}:sizes:0:{profile_item_token(rule)}"), state
    )
    await callback_handler(router, "rule_builder_ask")(
        FakeCallback("rb:ask:token_pattern"), state
    )
    await message_handler(router, "source_rule_edit_save")(FakeIncoming("-"), state)
    await callback_handler(router, "rule_builder_save")(FakeCallback("rb:save"), state)

    stored = (await stored_profile(sessions, source_id)).extraction_rules["sizes"][0]
    assert stored.token_pattern == DEFAULT_SERIES_TOKEN_PATTERN
    assert stored.pattern == rule.pattern
    await engine.dispose()


async def test_cancel_always_answers_even_without_an_open_wizard(tmp_path) -> None:
    engine, _, router, _ = await build_router(tmp_path)
    state = FakeState()
    message = FakeIncoming("/cancel")
    await message_handler(router, "cancel_state")(message, state)

    assert message.answers
    assert "Нечего отменять" in message.answers[-1][0]
    await engine.dispose()


async def test_wizard_of_one_source_never_writes_into_another(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()
    await open_builder(router, source_id, state)
    await callback_handler(router, "rule_builder_set")(FakeCallback("rb:field:price"), state)
    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:pattern"), state)
    await message_handler(router, "source_rule_save")(FakeIncoming(NICCY_PATTERN), state)

    stale = FakeCallback("rb:save")
    await state.update_data(source_id=source_id + 5000)
    await callback_handler(router, "rule_builder_save")(stale, state)

    profile = await stored_profile(sessions, source_id)
    assert profile.extraction_rules == {}
    await engine.dispose()
