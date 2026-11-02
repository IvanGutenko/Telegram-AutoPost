from __future__ import annotations

from decimal import Decimal

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.admin.service import AdminService
from emoda.db.base import Base
from emoda.db.models import CurrencyAlias
from emoda.domain.schemas import ExtractionRule, MarkupRange, SourceProfileData
from emoda.rules.currency import CurrencyAliasResolver
from emoda.services.pipeline import Pipeline, PipelineInput


@pytest.fixture
async def session() -> AsyncSession:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


async def test_zoom_money_bag_alias_reaches_markup_and_diagnostics() -> None:
    resolver = CurrencyAliasResolver({"💰": "USD"})
    profile = SourceProfileData(
        gemini_enabled=False,
        extraction_rules={
            "price": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?P<value>\d+(?:[.,]\d+)?)\s*💰",
                    authoritative=True,
                )
            ]
        },
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=71,
            text="27 💰",
            media_paths=[],
            emoda_code="EM-TEST",
            profile=profile,
            markup_rules=[
                MarkupRange(
                    min_price=Decimal("21"),
                    max_price=Decimal("30.99"),
                    markup=Decimal("5"),
                    priority=1,
                )
            ],
            currency_aliases=resolver,
        ),
        dry_run=True,
    )

    assert result.price is not None
    assert result.price.raw_currency == "USD"
    assert result.price.final_usd == Decimal("32")
    assert result.parsed.fields["price"].metadata["currency_source"] == "alias"
    assert result.parsed.fields["price"].metadata["matched_currency_alias"] == "💰"
    assert result.field_diagnostics["price"].currency_source == "alias"
    assert result.field_diagnostics["price"].matched_currency_alias == "💰"


async def test_unknown_money_emoji_is_not_guessed() -> None:
    resolver = CurrencyAliasResolver({"💰": "USD"})
    profile = SourceProfileData(
        gemini_enabled=False,
        extraction_rules={
            "price": [
                ExtractionRule(
                    kind="regex",
                    pattern=r"(?P<value>\d+)\s*🪙",
                    authoritative=True,
                )
            ]
        },
    )
    result = await Pipeline(None).run(
        PipelineInput(
            source_id=72,
            text="27 🪙",
            media_paths=[],
            emoda_code="EM-TEST",
            profile=profile,
            markup_rules=[],
            currency_aliases=resolver,
        ),
        dry_run=True,
    )

    assert result.price is None
    assert result.field_diagnostics["price"].stage == "currency"
    assert result.field_diagnostics["price"].reason == "currency_missing"


async def test_currency_alias_admin_crud_and_system_delete_protection(
    session: AsyncSession,
) -> None:
    service = AdminService(session)
    created = await service.create_currency_alias(
        alias="💰", currency="USD", enabled=True, actor_id=None
    )
    assert created.system is False

    updated = await service.update_currency_alias(
        created.id, alias="MONEY", currency="EUR", enabled=True, actor_id=None
    )
    assert (updated.alias, updated.currency) == ("MONEY", "EUR")

    await service.set_currency_alias_enabled(created.id, False, actor_id=None)
    persisted = await session.get(CurrencyAlias, created.id)
    assert persisted is not None and persisted.enabled is False
    await service.delete_currency_alias(created.id, actor_id=None)
    assert await session.get(CurrencyAlias, created.id) is None

    system = await session.scalar(
        select(CurrencyAlias).where(CurrencyAlias.normalized_alias == "$")
    )
    assert system is not None
    try:
        await service.delete_currency_alias(system.id, actor_id=None)
    except ValueError as exc:
        assert "system" in str(exc).casefold()
    else:
        raise AssertionError("system alias deletion must be rejected")

    enabled = list(
        await session.scalars(
            select(CurrencyAlias).where(CurrencyAlias.enabled.is_(True))
        )
    )
    assert "$" in [item.alias for item in enabled]
    assert "MONEY" not in [item.alias for item in enabled]
