"""Button-only creation and editing of regex extraction rules in /admin.

A regular admin must be able to add a complex regex rule without knowing the
``key=value`` syntax, test it against real text, and edit any single setting of
an existing rule - all through Telegram buttons.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin.router import create_admin_router
from emoda.admin.service import profile_item_token
from emoda.db.base import Base
from emoda.db.models import AdminUser, Source, SourceProfile
from emoda.domain.enums import PublicationMode
from emoda.domain.schemas import ExtractionRule, SourceProfileData

NICCY_PATTERN = r"(?m)^[ ]*(?P<value>[0-9]+(?:[.,][0-9]+)?)[ ]*[^0-9A-Za-z\r\n]*$"
NICCY_PRODUCTION_PATTERN = (
    r"(?m)^[ \t]{0,}📌[ \t]{0,}(?P<value>[0-9]+(?:[.,][0-9]+)?)"
    r"[ \t]{0,}[^0-9A-Za-z\r\n ]{1,}[ \t]{0,}$"
)


class FakeMessage:
    def __init__(self) -> None:
        self.edits: list[tuple[str, object | None]] = []

    async def edit_text(self, text: str, reply_markup: object | None = None) -> None:
        self.edits.append((text, reply_markup))


class FakeCallback:
    def __init__(self, data: str) -> None:
        self.data = data
        self.message = FakeMessage()
        self.from_user = SimpleNamespace(id=900)
        self.answers: list[tuple[str, bool]] = []

    async def answer(self, text: str | None = None, show_alert: bool = False) -> None:
        self.answers.append((text or "", show_alert))


class FakeIncoming:
    def __init__(self, text: str) -> None:
        self.text = text
        self.from_user = SimpleNamespace(id=900)
        self.answers: list[tuple[str, object | None]] = []

    async def answer(self, text: str, reply_markup: object | None = None) -> None:
        self.answers.append((text, reply_markup))


class FakeState:
    def __init__(self) -> None:
        self.data: dict[str, Any] = {}
        self.state: object | None = None

    async def clear(self) -> None:
        self.data.clear()
        self.state = None

    async def update_data(self, **values: Any) -> None:
        self.data.update(values)

    async def get_data(self) -> dict[str, Any]:
        return dict(self.data)

    async def set_state(self, state: object | None) -> None:
        self.state = state

    async def get_state(self) -> object | None:
        return self.state


def callback_handler(router, name: str):
    for handler in router.callback_query.handlers:
        if handler.callback.__name__ == name:
            return handler.callback
    raise AssertionError(f"callback handler is not registered: {name}")


def message_handler(router, name: str):
    for handler in router.message.handlers:
        if handler.callback.__name__ == name:
            return handler.callback
    raise AssertionError(f"message handler is not registered: {name}")


def callbacks_of(markup: object) -> set[str]:
    return {
        button.callback_data
        for row in markup.inline_keyboard  # type: ignore[union-attr]
        for button in row
        if button.callback_data
    }


async def build_router(tmp_path, *, rules: dict[str, list[ExtractionRule]] | None = None):
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    async with sessions() as session, session.begin():
        session.add(AdminUser(telegram_user_id=900, display_name="Admin", enabled=True))
        source = Source(name="Niccy", telegram_channel_id=-1951, enabled=False)
        source.profile = SourceProfile(
            publication_mode=PublicationMode.REVIEW,
            config=SourceProfileData(extraction_rules=rules or {}).model_dump(mode="json"),
        )
        session.add(source)
        await session.flush()
        source_id = source.id

    async def access_validator(_: str) -> tuple[int, str, int]:
        return -1951, "Niccy", 10

    async def test_message_loader(_: str) -> tuple[str, list[str]]:
        return "", []

    router = create_admin_router(
        sessions,
        {900},
        access_validator=access_validator,
        test_message_loader=test_message_loader,
        intelligence=None,
        media_root=tmp_path,
    )
    return engine, sessions, router, source_id


async def stored_profile(sessions, source_id: int) -> SourceProfileData:
    async with sessions() as session:
        source = await session.get(Source, source_id)
        assert source is not None
        return SourceProfileData.model_validate(source.profile.config)


async def test_regex_button_roundtrip_preserves_literal_spaces_and_newlines(tmp_path):
    original = ExtractionRule(kind="regex", pattern="old")
    engine, sessions, router, source_id = await build_router(tmp_path, rules={"sizes": [original]})
    state = FakeState()
    token = profile_item_token(original)
    await callback_handler(router, "source_rule_edit_start")(FakeCallback(f"r:e:{source_id}:sizes:0:{token}"), state)
    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:pattern"), state)
    pattern = "  (?P<value>STANDART)\\s*4PCS\n  "
    await message_handler(router, "source_rule_edit_save")(FakeIncoming(pattern), state)
    await callback_handler(router, "rule_builder_save")(FakeCallback("rb:save"), state)
    profile = await stored_profile(sessions, source_id)
    assert profile.extraction_rules["sizes"][0].pattern == pattern
    await engine.dispose()


async def test_admin_builds_the_customer_regex_rule_with_buttons_only(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()

    opened = FakeCallback(f"source:rule:{source_id}")
    await callback_handler(router, "source_rule_start")(opened, state)
    prompt, markup = opened.message.edits[-1]
    assert "&lt;" not in prompt and "&gt;" not in prompt
    assert "(?P<value>" in prompt
    controls = callbacks_of(markup)
    for control in ("rb:fields", "rb:kinds", "rb:ask:pattern", "rb:test", "rb:save"):
        assert control in controls, control

    field_picker = FakeCallback("rb:fields")
    await callback_handler(router, "rule_builder_picker")(field_picker, state)
    assert "rb:field:price" in callbacks_of(field_picker.message.edits[-1][1])
    await callback_handler(router, "rule_builder_set")(FakeCallback("rb:field:price"), state)
    kind_picker = FakeCallback("rb:kinds")
    await callback_handler(router, "rule_builder_picker")(kind_picker, state)
    assert "rb:kind:regex" in callbacks_of(kind_picker.message.edits[-1][1])
    await callback_handler(router, "rule_builder_set")(FakeCallback("rb:kind:regex"), state)

    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:pattern"), state)
    pattern_message = FakeIncoming(NICCY_PATTERN)
    await message_handler(router, "source_rule_save")(pattern_message, state)
    assert state.data["rule_draft"]["pattern"] == NICCY_PATTERN

    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:priority"), state)
    await message_handler(router, "source_rule_save")(FakeIncoming("11"), state)
    await callback_handler(router, "rule_builder_toggle")(
        FakeCallback("rb:toggle:authoritative"), state
    )

    await callback_handler(router, "rule_builder_test")(FakeCallback("rb:test"), state)
    probe = FakeIncoming("Niccy\n1250 \n")
    await message_handler(router, "source_rule_save")(probe, state)
    verdict = probe.answers[-1][0]
    assert "MATCH" in verdict and "NO MATCH" not in verdict
    assert "value = 1250" in verdict

    saved = FakeCallback("rb:save")
    await callback_handler(router, "rule_builder_save")(saved, state)

    profile = await stored_profile(sessions, source_id)
    rules = profile.extraction_rules["price"]
    assert len(rules) == 1
    assert rules[0].pattern == NICCY_PATTERN
    assert (rules[0].kind, rules[0].priority, rules[0].authoritative) == ("regex", 11, True)
    await engine.dispose()


async def test_regex_tester_reports_no_match_without_touching_the_profile(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()
    await callback_handler(router, "source_rule_start")(
        FakeCallback(f"source:rule:{source_id}"), state
    )
    await callback_handler(router, "rule_builder_set")(FakeCallback("rb:field:price"), state)
    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:pattern"), state)
    await message_handler(router, "source_rule_save")(
        FakeIncoming(r"(?i)Price\s*:\s*(?P<value>\d+)"), state
    )
    await callback_handler(router, "rule_builder_test")(FakeCallback("rb:test"), state)
    probe = FakeIncoming("Sizes : S M L")
    await message_handler(router, "source_rule_save")(probe, state)

    assert "NO MATCH" in probe.answers[-1][0]
    profile = await stored_profile(sessions, source_id)
    assert profile.extraction_rules == {}
    await engine.dispose()


async def test_regex_tester_shows_production_price_diagnostics_for_niccy(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()
    await callback_handler(router, "source_rule_start")(
        FakeCallback(f"source:rule:{source_id}"), state
    )
    await callback_handler(router, "rule_builder_set")(FakeCallback("rb:field:price"), state)
    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:pattern"), state)
    await message_handler(router, "source_rule_save")(
        FakeIncoming(NICCY_PRODUCTION_PATTERN), state
    )
    await callback_handler(router, "rule_builder_toggle")(
        FakeCallback("rb:toggle:authoritative"), state
    )
    await callback_handler(router, "rule_builder_test")(FakeCallback("rb:test"), state)
    probe = FakeIncoming("Kod: 107144\n📌25,5 💵")
    await message_handler(router, "source_rule_save")(probe, state)

    verdict = probe.answers[-1][0]
    assert "🧪 MATCH" in verdict
    assert "value = 25,5" in verdict
    assert "Captured: 25,5" in verdict
    assert "Normalized: 25.5" in verdict
    assert "Currency: USD" in verdict
    assert "Candidate: FOUND" in verdict
    assert "Flags: IGNORECASE, MULTILINE" in verdict
    profile = await stored_profile(sessions, source_id)
    assert profile.extraction_rules == {}
    await engine.dispose()


async def test_invalid_pattern_in_the_wizard_reports_a_regex_error(tmp_path) -> None:
    engine, _, router, source_id = await build_router(tmp_path)
    state = FakeState()
    await callback_handler(router, "source_rule_start")(
        FakeCallback(f"source:rule:{source_id}"), state
    )
    await callback_handler(router, "rule_builder_ask")(FakeCallback("rb:ask:pattern"), state)
    broken = FakeIncoming("(?P<value>[0-9")
    await message_handler(router, "source_rule_save")(broken, state)

    answer = broken.answers[-1][0]
    assert "Некорректный regex" in answer
    assert "field" not in answer.casefold()
    assert state.data["rule_draft"].get("pattern", "") == ""
    await engine.dispose()


async def test_admin_edits_one_setting_of_an_existing_rule_with_buttons(tmp_path) -> None:
    rule = ExtractionRule(
        kind="regex",
        pattern=NICCY_PATTERN,
        priority=11,
        ignore_case=True,
        authoritative=True,
    )
    other = ExtractionRule(kind="regex", pattern=r"(?P<value>\d+)\s*TL", priority=90)
    engine, sessions, router, source_id = await build_router(
        tmp_path, rules={"price": [rule, other]}
    )
    state = FakeState()
    token = profile_item_token(rule)

    opened = FakeCallback(f"r:e:{source_id}:price:0:{token}")
    await callback_handler(router, "source_rule_edit_start")(opened, state)
    text, markup = opened.message.edits[-1]
    assert "Текущее правило" in text
    assert f"pattern={NICCY_PATTERN}" in text
    controls = callbacks_of(markup)
    assert {"rb:toggle:ignore_case", "rb:ask:pattern", "rb:test", "rb:save"} <= controls

    await callback_handler(router, "rule_builder_toggle")(
        FakeCallback("rb:toggle:ignore_case"), state
    )
    await callback_handler(router, "rule_builder_save")(FakeCallback("rb:save"), state)

    profile = await stored_profile(sessions, source_id)
    stored = profile.extraction_rules["price"]
    assert len(stored) == 2
    assert stored[0].ignore_case is False
    assert stored[0].pattern == NICCY_PATTERN
    assert stored[0].priority == 11 and stored[0].authoritative is True
    assert stored[1].model_dump(mode="json") == other.model_dump(mode="json")
    await engine.dispose()


async def test_stale_builder_callback_reports_instead_of_doing_nothing(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()

    for name, data in (
        ("rule_builder_save", "rb:save"),
        ("rule_builder_toggle", "rb:toggle:enabled"),
        ("rule_builder_ask", "rb:ask:pattern"),
    ):
        callback = FakeCallback(data)
        await callback_handler(router, name)(callback, state)
        assert callback.answers, name
        assert any(answer[1] for answer in callback.answers), name

    profile = await stored_profile(sessions, source_id)
    assert profile.extraction_rules == {}
    await engine.dispose()


async def test_single_message_key_value_rule_still_saves_immediately(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()
    await callback_handler(router, "source_rule_start")(
        FakeCallback(f"source:rule:{source_id}"), state
    )
    message = FakeIncoming(
        f"field=price kind=regex pattern={NICCY_PATTERN} priority=11 authoritative=true"
    )
    await message_handler(router, "source_rule_save")(message, state)

    assert "Правило сохранено" in message.answers[-1][0]
    profile = await stored_profile(sessions, source_id)
    assert profile.extraction_rules["price"][0].pattern == NICCY_PATTERN
    await engine.dispose()


async def test_admin_computed_rule_selects_canonical_text_and_has_tester(tmp_path) -> None:
    engine, sessions, router, source_id = await build_router(tmp_path)
    state = FakeState()

    await callback_handler(router, "source_computed_start")(
        FakeCallback(f"source:computed:{source_id}")
    )
    operation = FakeCallback(f"source:computed-op:{source_id}:count_items")
    await callback_handler(router, "source_computed_operation")(operation, state)
    assert f"source:computed-input:{source_id}:canonical_text" in callbacks_of(
        operation.message.edits[-1][1]
    )

    selected = FakeCallback(f"source:computed-input:{source_id}:canonical_text")
    await callback_handler(router, "source_computed_input")(selected, state)
    prompt = selected.message.edits[-1][0]
    assert "Input source: Исходный текст" in prompt
    assert "input_source=canonical_text" in prompt

    saved = FakeIncoming(
        "input_source=canonical_text\nname=count-source-sizes\ntarget_field=quantity\n"
        "pattern=(?P<size>XS|S|M|L|XL)\ncapture_group=size"
    )
    await message_handler(router, "source_computed_save")(saved, state)
    profile = await stored_profile(sessions, source_id)
    rule = profile.computed_rules[0]
    assert rule.operation == "count_items"
    assert rule.input_source == "canonical_text"

    token = profile_item_token(rule)
    tester = FakeCallback(f"source:computed-test:{source_id}:0:{token}")
    await callback_handler(router, "source_computed_test_start")(tester, state)
    probe = FakeIncoming("S. S. M. M. L")
    await message_handler(router, "source_computed_test_run")(probe, state)
    verdict = probe.answers[-1][0]
    assert "Input source: canonical_text" in verdict
    assert "Pattern matches: [S, S, M, M, L]" in verdict
    assert "Count: 5" in verdict
    assert "Candidate: FOUND" in verdict
    assert "Provenance: computed_rule" in verdict
    await engine.dispose()
