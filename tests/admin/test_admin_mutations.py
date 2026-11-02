"""Regression suite for /admin mutating controls driven through the real router.

Reproduces the production failure mode: a mutation commits in the database and
the handler then crashes while re-rendering the screen (the historical cause
was mutating the frozen aiogram ``CallbackQuery.data``), so the admin sees a
generic error while the database silently changed. Every test here drives the
actual callback handlers through ``Dispatcher.feed_update`` and asserts both
the rendered result and the persisted database state.
"""
from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from aiogram import Bot, Dispatcher
from aiogram.types import CallbackQuery, Chat, Message, Update, User
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin import service as admin_service
from emoda.admin.router import create_admin_router
from emoda.db.base import Base
from emoda.db.models import Destination, Source
from emoda.domain.enums import PublicationMode
from emoda.domain.schemas import (
    CleaningRule,
    CollectionBoundaryRule,
    CollectionProfile,
    ExtractionRule,
    SourceProfileData,
)

ADMIN_TG_ID = 990_001


@pytest.mark.asyncio
async def test_admin_mutations(tmp_path: Path) -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)

    async with maker() as session, session.begin():
        destination = Destination(name="Mutations", telegram_channel_id=-100777)
        session.add(destination)
        await session.flush()
        for name, channel in (("Source A", -100111), ("Source B", -100222)):
            source = Source(
                name=name,
                telegram_channel_id=channel,
                telegram_ref=f"@{name.lower().replace(' ', '')}",
                enabled=True,
            )
            profile = SourceProfileData(
                publication_mode=PublicationMode.REVIEW,
                collection=CollectionProfile(
                    boundary_rules=[
                        CollectionBoundaryRule(
                            name="next",
                            action="start",
                            match="regex",
                            pattern=r"(?m)^NEXT\b",
                        )
                    ],
                ),
                extraction_rules={
                    "sizes": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"SIZE:\s*(?P<value>[^\n]+)",
                            authoritative=True,
                        )
                    ],
                    "quantity": [
                        ExtractionRule(
                            kind="regex",
                            pattern=r"QTY:\s*(?P<value>\d+)",
                            authoritative=True,
                        ),
                        ExtractionRule(
                            kind="regex",
                            pattern=r"PACK:\s*(?P<value>\d+)",
                            authoritative=True,
                        ),
                    ],
                },
                cleaning_rules=[CleaningRule(kind="exact", value="NOISE")],
            )
            source.profile = __import__("emoda.db.models", fromlist=["SourceProfile"]).SourceProfile(
                schema_version=profile.schema_version,
                publication_mode=profile.publication_mode,
                config=profile.model_dump(mode="json"),
            )
            session.add(source)
            await session.flush()

    async def access_validator(reference: str) -> tuple[int, str, int]:
        return -100111, "@sourcea", 1

    async def message_loader(reference: str) -> tuple[str, list[str]]:
        return "", []

    router = create_admin_router(
        maker,
        {ADMIN_TG_ID},
        access_validator=access_validator,
        test_message_loader=message_loader,
        intelligence=None,
        media_root=tmp_path,
    )
    dispatcher = Dispatcher()
    dispatcher.include_router(router)
    bot = Bot(token="42:TEST")

    edits: list[str] = []
    markups: list[Any] = []
    answers: list[str] = []

    async def fake_edit_text(self: Message, text: str = "", **kwargs: Any) -> Message:
        edits.append(text)
        markups.append(kwargs.get("reply_markup"))
        return self

    async def fake_answer_message(self: Message, text: str = "", **kwargs: Any) -> Message:
        edits.append(text)
        markups.append(kwargs.get("reply_markup"))
        return self

    async def fake_answer_callback(self: CallbackQuery, text: str = "", **kwargs: Any) -> bool:
        answers.append(text)
        return True

    original_edit = Message.edit_text
    original_answer_message = Message.answer
    original_answer_callback = CallbackQuery.answer
    Message.edit_text = fake_edit_text  # type: ignore[method-assign]
    Message.answer = fake_answer_message  # type: ignore[method-assign]
    CallbackQuery.answer = fake_answer_callback  # type: ignore[method-assign]
    try:

        async def drive(data: str, message_id: int = 50) -> None:
            user = User(id=ADMIN_TG_ID, is_bot=False, first_name="Tester")
            message = Message(
                chat=Chat(id=ADMIN_TG_ID, type="private"),
                message_id=message_id,
                date=datetime.now(UTC),
                from_user=user,
            )
            callback = CallbackQuery(
                id=f"cb{len(edits)}{len(answers)}",
                from_user=user,
                message=message,
                data=data,
                chat_instance="test",
            )
            update = Update(update_id=len(edits) + len(answers) + 1, callback_query=callback)
            await dispatcher.feed_update(bot, update)

        async def profile_of(source_id: int) -> SourceProfileData:
            async with maker() as session:
                source = await session.get(Source, source_id)
                assert source is not None
                return SourceProfileData.model_validate(source.profile.config)

        async def source_row(source_id: int) -> Source:
            async with maker() as session:
                source = await session.get(Source, source_id)
                assert source is not None
                return source

        async def item_token(source_id: int, collection: str, index: int, field: str = "") -> str:
            profile = await profile_of(source_id)
            if collection == "rule":
                item = profile.extraction_rules[field][index]
            elif collection == "cleaning":
                item = profile.cleaning_rules[index]
            elif collection == "boundary":
                item = profile.effective_collection().boundary_rules[index]
            elif collection == "ignore":
                item = profile.ignore_rules[index]
            elif collection == "computed":
                item = profile.computed_rules[index]
            elif collection == "multi_price":
                item = profile.multi_price_rules[index]
            elif collection == "routing":
                item = profile.routing_rules[index]
            else:
                raise AssertionError(collection)
            token = getattr(admin_service, "profile_item_token", None)
            assert token is not None
            return token(item)

        # --- Per-field processing: passthrough, reopen, normal, isolation --
        edits.clear()
        await drive("source:field-modes:1")
        assert any("Режим обработки полей" in text for text in edits)
        assert any(
            button.callback_data == "source:field-mode:1:sizes:passthrough"
            for markup in markups
            if markup is not None
            for row in markup.inline_keyboard
            for button in row
        )
        edits.clear()
        await drive("source:field-mode:1:sizes:passthrough")
        profile = await profile_of(1)
        assert profile.field_policies["sizes"].processing_mode == "passthrough"
        assert (await profile_of(2)).field_policies == {}
        assert any("RAW / Passthrough" in text for text in edits)
        edits.clear()
        await drive("source:field-modes:1")
        assert any("RAW / Passthrough" in text for text in edits)
        await drive("source:field-mode:1:sizes:normal")
        assert (await profile_of(1)).field_policies == {}

        # --- Parsing rule: toggle off -------------------------------------
        edits.clear()
        sizes_token = await item_token(1, "rule", 0, "sizes")
        disable_sizes = f"r:s:1:sizes:0:0:{sizes_token}"
        await drive(disable_sizes)
        profile = await profile_of(1)
        assert profile.extraction_rules["sizes"][0].enabled is False
        assert any("выкл" in text for text in edits), "screen must show the new state"

        # Repeating the callback after a post-mutation UI failure is idempotent.
        await drive(disable_sizes)
        profile = await profile_of(1)
        assert profile.extraction_rules["sizes"][0].enabled is False

        # --- Parsing rule: toggle back on ----------------------------------
        edits.clear()
        sizes_token = await item_token(1, "rule", 0, "sizes")
        await drive(f"r:s:1:sizes:0:1:{sizes_token}")
        profile = await profile_of(1)
        assert profile.extraction_rules["sizes"][0].enabled is True

        # --- Parsing rule: edit only one setting at a time ------------------
        edits.clear()
        sizes_token = await item_token(1, "rule", 0, "sizes")
        stale_sizes_edit = f"r:e:1:sizes:0:{sizes_token}"
        await drive(stale_sizes_edit)
        assert any("Отправьте" in text or "правил" in text.lower() for text in edits)
        user = User(id=ADMIN_TG_ID, is_bot=False, first_name="Tester")
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=51,
            date=datetime.now(UTC),
            from_user=user,
            text=r"pattern=(?i)Sizes\s*:\s*(?P<value>STANDART)",
        )
        update = Update(update_id=990, message=message)
        await dispatcher.feed_update(bot, update)
        profile = await profile_of(1)
        edited_rule = profile.extraction_rules["sizes"][0]
        assert edited_rule.pattern == r"(?i)Sizes\s*:\s*(?P<value>STANDART)"
        assert edited_rule.kind == "regex"
        assert edited_rule.priority == 100
        assert edited_rule.ignore_case is True
        assert edited_rule.authoritative is True
        assert edited_rule.enabled is True

        async def edit_sizes_setting(text: str, update_id: int) -> None:
            current_token = await item_token(1, "rule", 0, "sizes")
            await drive(f"r:e:1:sizes:0:{current_token}")
            await dispatcher.feed_update(
                bot,
                Update(
                    update_id=update_id,
                    message=Message(
                        chat=Chat(id=ADMIN_TG_ID, type="private"),
                        message_id=update_id,
                        date=datetime.now(UTC),
                        from_user=user,
                        text=text,
                    ),
                ),
            )

        await edit_sizes_setting("priority=7", 991)
        await edit_sizes_setting("ignore_case=false", 992)
        await edit_sizes_setting("authoritative=false", 993)
        await edit_sizes_setting("enabled=false", 994)
        profile = await profile_of(1)
        independently_edited = profile.extraction_rules["sizes"][0]
        assert independently_edited.pattern == edited_rule.pattern
        assert independently_edited.kind == "regex"
        assert independently_edited.priority == 7
        assert independently_edited.ignore_case is False
        assert independently_edited.authoritative is False
        assert independently_edited.enabled is False

        # The pre-edit token is stale and cannot reopen or overwrite the rule.
        answers.clear()
        await drive(stale_sizes_edit)
        assert any("устар" in text.lower() or "измен" in text.lower() for text in answers)
        profile = await profile_of(1)
        assert profile.extraction_rules["sizes"][0] == independently_edited

        # --- Parsing rule: delete -------------------------------------------
        quantity_token = await item_token(1, "rule", 0, "quantity")
        stale_quantity_delete = f"r:d:1:quantity:0:{quantity_token}"
        await drive(stale_quantity_delete)
        profile = await profile_of(1)
        assert [rule.pattern for rule in profile.extraction_rules["quantity"]] == [
            r"PACK:\s*(?P<value>\d+)"
        ]

        # --- Stale callback cannot delete the rule that shifted into index 0 --
        edits.clear()
        answers.clear()
        await drive(stale_quantity_delete)
        profile = await profile_of(1)
        assert [rule.pattern for rule in profile.extraction_rules["quantity"]] == [
            r"PACK:\s*(?P<value>\d+)"
        ]
        assert any("уже" in text for text in answers), answers

        # --- Stale callback cannot toggle the shifted rule -------------------
        answers.clear()
        await drive(f"r:s:1:quantity:0:0:{quantity_token}")
        profile = await profile_of(1)
        assert profile.extraction_rules["quantity"][0].enabled is True
        assert any("уже" in text for text in answers), answers

        # --- Unknown source id does not crash -------------------------------
        answers.clear()
        await drive(f"r:d:9999:sizes:0:{sizes_token}")
        assert any("Не найден" in text for text in answers), answers

        # --- Source isolation: A mutations never touch B ---------------------
        sizes_token = await item_token(1, "rule", 0, "sizes")
        await drive(f"r:s:1:sizes:0:0:{sizes_token}")
        profile_b = await profile_of(2)
        assert profile_b.extraction_rules["sizes"][0].enabled is True
        assert "quantity" in profile_b.extraction_rules
        profile_a = await profile_of(1)
        assert profile_a.extraction_rules["sizes"][0].enabled is False

        # --- Computed rule: add/edit/toggle/delete through the real UI -----
        edits.clear()
        await drive("source:computed:1")
        assert any("Выберите понятный тип вычисления" in text for text in edits)
        assert any(
            button.text == "Количество в размерном ряду"
            for markup in markups
            if markup is not None
            for row in markup.inline_keyboard
            for button in row
        )
        await drive("source:computed-op:1:count_series_items")
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=55,
            date=datetime.now(UTC),
            from_user=user,
            text=(
                "name=count-series\ntarget_field=quantity\nsource_field=sizes\n"
                "input_mode=raw\nseries_separator_pattern=\\s+\n"
                "range_pattern=(?P<start>\\d+)-(?P<end>\\d+)\n"
                "explicit_list_pattern=(?P<item>\\d+)\n"
                "explicit_separator_pattern=-\nnumeric_step=2\n"
                "priority=9\nauthoritative=true\nconflict_policy=explicit"
            ),
        )
        await dispatcher.feed_update(bot, Update(update_id=994, message=message))
        profile = await profile_of(1)
        assert profile.computed_rules[0].operation == "count_series_items"
        assert profile.computed_rules[0].input_mode == "raw"
        assert profile.computed_rules[0].numeric_step == Decimal("2")
        assert profile.computed_rules[0].authoritative is True
        assert (await profile_of(2)).computed_rules == []

        computed_token = await item_token(1, "computed", 0)
        await drive(f"source:computed-set:1:0:false:{computed_token}")
        profile = await profile_of(1)
        assert profile.computed_rules[0].enabled is False

        computed_token = await item_token(1, "computed", 0)
        await drive(f"source:computed-edit:1:0:{computed_token}")
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=56,
            date=datetime.now(UTC),
            from_user=user,
            text=(
                "name=count-series-v2\ntarget_field=quantity\n"
                "operation=count_series_items\nsource_field=sizes\ninput_mode=raw\n"
                "series_separator_pattern=\\s+\n"
                "range_pattern=(?P<start>\\d+)-(?P<end>\\d+)\n"
                "explicit_list_pattern=(?P<item>\\d+)\n"
                "explicit_separator_pattern=-\nnumeric_step=2\n"
                "priority=3\nauthoritative=true\nconflict_policy=explicit"
            ),
        )
        await dispatcher.feed_update(bot, Update(update_id=995, message=message))
        profile = await profile_of(1)
        assert profile.computed_rules[0].name == "count-series-v2"
        assert profile.computed_rules[0].priority == 3
        assert profile.computed_rules[0].enabled is False

        computed_token = await item_token(1, "computed", 0)
        await drive(f"source:computed-set:1:0:true:{computed_token}")
        profile = await profile_of(1)
        assert profile.computed_rules[0].enabled is True
        computed_token = await item_token(1, "computed", 0)
        stale_computed_delete = f"source:computed-del:1:0:{computed_token}"
        await drive(stale_computed_delete)
        await drive(stale_computed_delete)
        assert (await profile_of(1)).computed_rules == []

        # --- Routing: full create/edit/delete through Telegram /admin -----
        edits.clear()
        await drive("source:route-add:1")
        assert any("category=" in text and "pattern=" in text for text in edits)
        await dispatcher.feed_update(
            bot,
            Update(
                update_id=996,
                message=Message(
                    chat=Chat(id=ADMIN_TG_ID, type="private"),
                    message_id=57,
                    date=datetime.now(UTC),
                    from_user=user,
                    text=(
                        "category=dress\nkeywords=dress,gown\n"
                        "pattern=(?i)\\b(dress|gown)\\b\n"
                        "destination_id=1\npriority=8"
                    ),
                ),
            ),
        )
        profile = await profile_of(1)
        assert profile.category_rules[0].pattern == r"(?i)\b(dress|gown)\b"
        assert profile.routing_rules[0].destination_id == 1
        assert (await profile_of(2)).routing_rules == []

        route_token = await item_token(1, "routing", 0)
        stale_route_edit = f"source:route-edit:1:0:{route_token}"
        edits.clear()
        await drive(stale_route_edit)
        assert any("Текущее routing rule" in text for text in edits)
        await dispatcher.feed_update(
            bot,
            Update(
                update_id=997,
                message=Message(
                    chat=Chat(id=ADMIN_TG_ID, type="private"),
                    message_id=58,
                    date=datetime.now(UTC),
                    from_user=user,
                    text=(
                        "category=bag\nkeywords=bag,purse\n"
                        "pattern=(?i)clutch\ndestination_id=1\npriority=3"
                    ),
                ),
            ),
        )
        profile = await profile_of(1)
        assert profile.category_rules[0].category == "bag"
        assert profile.routing_rules[0].keywords == ["bag", "purse"]
        assert profile.routing_rules[0].priority == 3
        assert (await profile_of(2)).routing_rules == []

        answers.clear()
        await drive(stale_route_edit)
        assert any("устар" in text.lower() or "измен" in text.lower() for text in answers)
        current_route_token = await item_token(1, "routing", 0)
        await drive(f"source:route-del:1:0:{current_route_token}")
        assert (await profile_of(1)).routing_rules == []
        assert (await profile_of(1)).category_rules == []

        # --- Source enabled target action is idempotent ---------------------
        await drive("source:enabled:1:false")
        await drive("source:enabled:1:false")
        row = await source_row(1)
        assert row.enabled is False
        await drive("source:enabled:1:true")
        row = await source_row(1)
        assert row.enabled is True

        # --- Source mode AUTO/REVIEW ----------------------------------------
        await drive("source:mode:1:auto")
        profile = await profile_of(1)
        assert profile.publication_mode is PublicationMode.AUTO
        await drive("source:mode:1:review")
        profile = await profile_of(1)
        assert profile.publication_mode is PublicationMode.REVIEW

        # --- Cleaning rule: toggle and delete --------------------------------
        cleaning_token = await item_token(1, "cleaning", 0)
        await drive(f"source:clean-set:1:0:false:{cleaning_token}")
        profile = await profile_of(1)
        assert profile.cleaning_rules[0].enabled is False
        cleaning_token = await item_token(1, "cleaning", 0)
        await drive(f"source:clean-del:1:0:{cleaning_token}")
        profile = await profile_of(1)
        assert profile.cleaning_rules == []

        # --- Cleaning rule: add via FSM ---------------------------------------
        edits.clear()
        await drive("source:clean-rule:1")
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=52,
            date=datetime.now(UTC),
            from_user=user,
            text="kind=contains\nvalue=REKLAM\nscope=text",
        )
        await dispatcher.feed_update(bot, Update(update_id=991, message=message))
        profile = await profile_of(1)
        assert any(rule.value == "REKLAM" for rule in profile.cleaning_rules)

        # --- Cleaning rule: edit the existing object without recreating it ---
        cleaning_token = await item_token(1, "cleaning", 0)
        edits.clear()
        markups.clear()
        await drive("source:clean:1")
        clean_callbacks = {
            button.callback_data
            for markup in markups
            if markup is not None
            for row in markup.inline_keyboard
            for button in row
        }
        assert f"source:clean-edit:1:0:{cleaning_token}" in clean_callbacks
        await drive(f"source:clean-edit:1:0:{cleaning_token}")
        assert any("Текущие значения" in text for text in edits)
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=57,
            date=datetime.now(UTC),
            from_user=user,
            text=(
                "kind=regex\nvalue=NEW\\s+NOISE\nreplacement=\nscope=text\n"
                "language=both\nignore_case=false\nenabled=false"
            ),
        )
        await dispatcher.feed_update(bot, Update(update_id=996, message=message))
        profile = await profile_of(1)
        assert len(profile.cleaning_rules) == 1
        assert profile.cleaning_rules[0].kind == "regex"
        assert profile.cleaning_rules[0].value == r"NEW\s+NOISE"
        assert profile.cleaning_rules[0].enabled is False
        from emoda.services.sanitizer import FinalSanitizer

        assert FinalSanitizer.apply_source_rules(
            "NEW NOISE\nKEEP", profile.cleaning_rules
        ) == "NEW NOISE\nKEEP"

        # The old edit callback is stale and cannot overwrite the edited rule.
        answers.clear()
        await drive(f"source:clean-edit:1:0:{cleaning_token}")
        profile = await profile_of(1)
        assert profile.cleaning_rules[0].value == r"NEW\s+NOISE"
        assert any("устар" in text.lower() or "измен" in text.lower() for text in answers)
        current_cleaning_token = await item_token(1, "cleaning", 0)
        await drive(f"source:clean-set:1:0:true:{current_cleaning_token}")
        profile = await profile_of(1)
        assert FinalSanitizer.apply_source_rules(
            "NEW NOISE\nKEEP", profile.cleaning_rules
        ).strip() == "KEEP"

        # --- Multi-price: Create and Edit are separate UI flows --------------
        edits.clear()
        await drive("source:multi-price:1")
        assert any("создание" in text.lower() for text in edits)
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=58,
            date=datetime.now(UTC),
            from_user=user,
            text=(
                "name=items\n"
                "pattern=(?P<label>A|B)\\s*(?P<value>\\d+)\\$\n"
                "default_currency=USD\npriority=40\n"
                "labels=A=>А=>A;B=>Б=>B"
            ),
        )
        await dispatcher.feed_update(bot, Update(update_id=997, message=message))
        profile = await profile_of(1)
        assert len(profile.multi_price_rules) == 1
        original_multi_token = await item_token(1, "multi_price", 0)

        edits.clear()
        markups.clear()
        await drive("source:rules:1")
        rule_callbacks = {
            button.callback_data
            for markup in markups
            if markup is not None
            for row in markup.inline_keyboard
            for button in row
        }
        assert f"source:multi-price-edit:1:0:{original_multi_token}" in rule_callbacks
        await drive(f"source:multi-price-edit:1:0:{original_multi_token}")
        assert any("Редактирование существующего" in text for text in edits)
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=59,
            date=datetime.now(UTC),
            from_user=user,
            text=(
                "name=items\n"
                "pattern=(?P<label>C|D)\\s*(?P<value>\\d+)\\s*EUR\n"
                "default_currency=EUR\npriority=3\nignore_case=false\nenabled=false\n"
                "labels=C=>Топ=>Top;D=>Юбка=>Skirt"
            ),
        )
        await dispatcher.feed_update(bot, Update(update_id=998, message=message))
        profile = await profile_of(1)
        assert len(profile.multi_price_rules) == 1
        edited_multi = profile.multi_price_rules[0]
        assert edited_multi.name == "items"
        assert edited_multi.pattern == r"(?P<label>C|D)\s*(?P<value>\d+)\s*EUR"
        assert {key: value.ru for key, value in edited_multi.labels.items()} == {
            "C": "Топ",
            "D": "Юбка",
        }
        assert edited_multi.default_currency == "EUR"
        assert edited_multi.priority == 3
        assert edited_multi.enabled is False
        from emoda.rules.engine import extract_multi_price_candidates

        candidates, errors = extract_multi_price_candidates(
            "C 10 EUR D 20 EUR", profile.multi_price_rules
        )
        assert candidates == [] and errors == []

        # Stale edit is visible and cannot create a duplicate or mutate a neighbor.
        answers.clear()
        await drive(f"source:multi-price-edit:1:0:{original_multi_token}")
        assert len((await profile_of(1)).multi_price_rules) == 1
        assert any("устар" in text.lower() or "измен" in text.lower() for text in answers)

        edited_multi_token = await item_token(1, "multi_price", 0)
        await drive(f"source:multi-price-set:1:0:true:{edited_multi_token}")
        enabled_profile = await profile_of(1)
        assert enabled_profile.multi_price_rules[0].enabled is True
        candidates, errors = extract_multi_price_candidates(
            "C 10 EUR D 20 EUR", enabled_profile.multi_price_rules
        )
        assert errors == []
        assert [(item.label_ru, item.value, item.currency) for item in candidates] == [
            ("Топ", "10", "EUR"),
            ("Юбка", "20", "EUR"),
        ]
        edited_multi_token = await item_token(1, "multi_price", 0)
        await drive(f"source:multi-price-set:1:0:false:{edited_multi_token}")
        assert (await profile_of(1)).multi_price_rules[0].enabled is False

        # --- Collection boundary: toggle and delete ---------------------------
        boundary_token = await item_token(1, "boundary", 0)
        await drive(f"collect:bset:1:0:false:{boundary_token}")
        profile = await profile_of(1)
        assert profile.effective_collection().boundary_rules[0].enabled is False
        boundary_token = await item_token(1, "boundary", 0)
        await drive(f"collect:bdel:1:0:{boundary_token}")
        profile = await profile_of(1)
        assert profile.effective_collection().boundary_rules == []

        # --- Gemini toggle and OCR mode (same frozen-mutation bug) -------------
        await drive("gemini:enabled:1:true")
        await drive("gemini:enabled:1:true")
        profile = await profile_of(1)
        assert profile.gemini_enabled is True
        await drive("gemini:ocr:1:always")
        profile = await profile_of(1)
        from emoda.domain.enums import OCRMode

        assert profile.ocr_mode is OCRMode.ALWAYS
        await drive("gemini:ocr:1:missing_only")
        profile = await profile_of(1)
        assert profile.ocr_mode is OCRMode.MISSING_ONLY

        # --- Fallback delete ----------------------------------------------------
        await drive("source:fallback:1")
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=53,
            date=datetime.now(UTC),
            from_user=user,
            text="field=sizes\nvalue=S, M, L\nauthoritative=true",
        )
        await dispatcher.feed_update(bot, Update(update_id=992, message=message))
        profile = await profile_of(1)
        assert profile.fallback_rules["sizes"].value == "S, M, L"
        await drive("source:fallback-del:1:sizes")
        await drive("source:fallback-del:1:sizes")
        profile = await profile_of(1)
        assert "sizes" not in profile.fallback_rules

        # --- Missing-only IGNORE add/toggle/delete -------------------------
        await drive("source:ignore:1")
        message = Message(
            chat=Chat(id=ADMIN_TG_ID, type="private"),
            message_id=54,
            date=datetime.now(UTC),
            from_user=user,
            text=(
                "name=missing-product-fields\nmedia_required=false\n"
                "description_missing_required=false\nmissing_fields=price,sizes"
            ),
        )
        await dispatcher.feed_update(bot, Update(update_id=993, message=message))
        profile = await profile_of(1)
        assert profile.ignore_rules[0].found_fields == []
        ignore_token = await item_token(1, "ignore", 0)
        await drive(f"source:ignore-set:1:0:false:{ignore_token}")
        profile = await profile_of(1)
        assert profile.ignore_rules[0].enabled is False
        ignore_token = await item_token(1, "ignore", 0)
        await drive(f"source:ignore-set:1:0:true:{ignore_token}")
        ignore_token = await item_token(1, "ignore", 0)
        await drive(f"source:ignore-del:1:0:{ignore_token}")
        profile = await profile_of(1)
        assert profile.ignore_rules == []

        # --- Multi-price delete path is safe on missing rules -------------------
        answers.clear()
        await drive("source:multi-price-del:1:0:0000000000")
        assert any(("не найден" in text.lower() or "отсутств" in text.lower()) for text in answers), answers

        # --- Archive requires confirmation; restore is in-place and safe ----
        source_b_profile_before = (await profile_of(2)).model_dump(mode="json")
        await drive("source:archive:2")
        row = await source_row(2)
        assert row.archived_at is None
        assert any("Архивировать Source B?" in text for text in edits)

        await drive("source:archive-do:2")
        row = await source_row(2)
        assert row.archived_at is not None
        assert row.enabled is False

        edits.clear()
        markups.clear()
        await drive("menu:sources:archived")
        assert any("Архивированные источники" in text for text in edits)
        archived_callbacks = {
            button.callback_data
            for markup in markups
            if markup is not None
            for markup_row in markup.inline_keyboard
            for button in markup_row
        }
        assert "source:restore:2" in archived_callbacks

        await drive("source:restore:2")
        row = await source_row(2)
        assert row.archived_at is None
        assert row.enabled is False
        assert (await profile_of(2)).model_dump(mode="json") == source_b_profile_before

        answers.clear()
        await drive("source:restore:2")
        row = await source_row(2)
        assert row.archived_at is None and row.enabled is False
        assert any("уже" in text.lower() for text in answers)

        # --- Destination publication is permanently the global renderer ----
        edits.clear()
        markups.clear()
        await drive("destination:1")
        assert any("глобальный E-MODA (4 поля)" in text for text in edits)
        destination_card_text = edits[-1]
        assert "signature" not in destination_card_text.casefold()
        assert "template" not in destination_card_text.casefold()
        callbacks = {
            button.callback_data
            for markup in markups
            if markup is not None
            for row in markup.inline_keyboard
            for button in row
        }
        assert all(
            callback is None or not callback.startswith("destination:policy")
            for callback in callbacks
        )
        assert all(
            callback is None or not callback.startswith("destination:fixed-text")
            for callback in callbacks
        )
        edits.clear()
        await drive("destination:edit:1")
        destination_edit_text = edits[-1]
        assert "name=" in destination_edit_text
        assert "telegram_channel_id=" in destination_edit_text
        assert "enabled=" in destination_edit_text
        assert "signature" not in destination_edit_text.casefold()
        assert "template" not in destination_edit_text.casefold()

        edits.clear()
        await drive("source:profile:1")
        profile_edit_text = edits[-1]
        assert "translation_enabled" not in profile_edit_text
        assert "album_debounce_seconds" not in profile_edit_text
        assert "product_collection_mode" not in profile_edit_text
        assert "Сбор товара" in profile_edit_text

        edits.clear()
        markups.clear()
        await drive("menu:settings")
        assert any("EUR→USD: 1.2" in text and "фиксирован" in text for text in edits)
        settings_callbacks = {
            button.callback_data
            for markup in markups
            if markup is not None
            for row in markup.inline_keyboard
            for button in row
        }
        assert "settings:eur" not in settings_callbacks
        async with maker() as session:
            destination = await session.get(Destination, 1)
            assert destination is not None
            assert not hasattr(destination, "publication_policy")
            assert not hasattr(destination, "fixed_text")

    finally:
        Message.edit_text = original_edit  # type: ignore[method-assign]
        Message.answer = original_answer_message  # type: ignore[method-assign]
        CallbackQuery.answer = original_answer_callback  # type: ignore[method-assign]
        await bot.session.close()
        await dispatcher.storage.close()
        await engine.dispose()
