from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin.service import AdminService
from emoda.db.base import Base
from emoda.domain.enums import PublicationMode
from emoda.domain.schemas import ExtractionRule, MarkupRange, SourceProfileData
from emoda.services.pipeline import Pipeline, PipelineInput
from emoda.services.processor import resolve_publication_mode

FIXED_TEXT = (
    "Цена указана на фото поставщика.\n"
    "Комиссия E-MODA: +10%\n"
    "к указанной цене."
)


def pipeline_input(
    *,
    source_id: int = 1,
    destination_id: int = 50,
    code: str = "AA00001",
    text: str = "Price: 99 EUR\nSizes: S M L\nQuantity: 3",
) -> PipelineInput:
    return PipelineInput(
        source_id=source_id,
        text=text,
        media_paths=[f"source-{source_id}.jpg"],
        emoda_code=code,
        profile=SourceProfileData(
            publication_mode=PublicationMode.AUTO,
            default_destination_id=destination_id,
            gemini_enabled=False,
        ),
        markup_rules=[
            MarkupRange(
                min_price=Decimal("0"),
                max_price=None,
                markup=Decimal("999"),
                priority=1,
            )
        ],
    )


async def test_legacy_fixed_text_policy_cannot_bypass_global_renderer() -> None:
    result = await Pipeline(None).run(pipeline_input(), dry_run=False)

    assert result.rendered_text == (
        "<b>Код / Code:</b> AA00001\n"
        "<b>Цена за шт / Unit price:</b> $1118\n"
        "<b>Размеры / Sizes:</b> S M L\n"
        "<b>Шт в серии / Pcs per lot:</b> 3"
    )
    assert FIXED_TEXT not in result.rendered_text
    assert result.rendered_text.count("\n") == 3


async def test_parallel_destinations_keep_source_profiles_isolated() -> None:
    first = pipeline_input(
        source_id=101,
        destination_id=501,
        code="AA00101",
        text="FIRST=10 USD\nSIZE_A=S\nPACK_A=2",
    )
    second = pipeline_input(
        source_id=202,
        destination_id=502,
        code="AA00202",
        text="SECOND=20 EUR\nSIZE_B=XL\nPACK_B=7",
    )
    first_profile = first.profile.model_copy(
        update={
            "extraction_rules": {
                "price": [
                    ExtractionRule(
                        kind="regex",
                        pattern=r"FIRST=(?P<value>\d+) (?P<currency>USD)",
                        authoritative=True,
                    )
                ],
                "sizes": [
                    ExtractionRule(
                        kind="regex", pattern=r"SIZE_A=(?P<value>S)", authoritative=True
                    )
                ],
                "quantity": [
                    ExtractionRule(
                        kind="regex", pattern=r"PACK_A=(?P<value>2)", authoritative=True
                    )
                ],
            }
        }
    )
    second_profile = second.profile.model_copy(
        update={
            "extraction_rules": {
                "price": [
                    ExtractionRule(
                        kind="regex",
                        pattern=r"SECOND=(?P<value>\d+) (?P<currency>EUR)",
                        authoritative=True,
                    )
                ],
                "sizes": [
                    ExtractionRule(
                        kind="regex", pattern=r"SIZE_B=(?P<value>XL)", authoritative=True
                    )
                ],
                "quantity": [
                    ExtractionRule(
                        kind="regex", pattern=r"PACK_B=(?P<value>7)", authoritative=True
                    )
                ],
            }
        }
    )
    first = PipelineInput(**{**first.__dict__, "profile": first_profile})
    second = PipelineInput(**{**second.__dict__, "profile": second_profile})

    first_result, second_result = await asyncio.gather(
        Pipeline(None).run(first, dry_run=False),
        Pipeline(None).run(second, dry_run=False),
    )

    assert "<b>Цена за шт / Unit price:</b> $1009" in first_result.rendered_text
    assert "<b>Размеры / Sizes:</b> S" in first_result.rendered_text
    assert "<b>Шт в серии / Pcs per lot:</b> 2" in first_result.rendered_text
    assert "SECOND" not in first_result.parsed.original_text
    assert "<b>Цена за шт / Unit price:</b> $1023" in second_result.rendered_text
    assert "<b>Размеры / Sizes:</b> XL" in second_result.rendered_text
    assert "<b>Шт в серии / Pcs per lot:</b> 7" in second_result.rendered_text
    assert "FIRST" not in second_result.parsed.original_text
    assert first_result.parsed.destination_id == 501
    assert second_result.parsed.destination_id == 502


def test_destination_never_overrides_source_publication_mode() -> None:
    profile = SourceProfileData(
        publication_mode=PublicationMode.REVIEW,
        use_global_publication_mode=False,
    )

    assert resolve_publication_mode(profile, PublicationMode.AUTO) is PublicationMode.REVIEW


async def test_admin_rejects_fixed_text_publication_mode() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with maker() as session, session.begin():
            service = AdminService(session)
            destination = await service.create_destination(
                "Global renderer only",
                -100900,
                actor_id=None,
            )
            with pytest.raises(ValueError, match="unknown destination settings"):
                await service.update_destination(
                    destination.id,
                    {
                        "publication_policy": "media_fixed_text",
                        "fixed_text": FIXED_TEXT,
                    },
                    actor_id=None,
                )
            assert not hasattr(destination, "publication_policy")
            assert not hasattr(destination, "fixed_text")
    finally:
        await engine.dispose()
