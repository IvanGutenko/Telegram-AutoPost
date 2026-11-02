from decimal import Decimal

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from emoda.admin.service import AdminService
from emoda.db.base import Base
from emoda.db.models import Destination, Source
from emoda.domain.enums import PublicationMode
from emoda.domain.schemas import (
    CleaningRule,
    CollectionBoundaryRule,
    CollectionProfile,
    ExtractionRule,
    MarkupRange,
    SourceProfileData,
)
from emoda.services.orchestrator import ProcessingMode, process_product
from emoda.services.pipeline import Pipeline, PipelineInput


@pytest.mark.asyncio
async def test_clean_database_onboards_40_profiles_through_admin_service_without_code_changes() -> None:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    source_ids: list[int] = []
    async with maker() as session, session.begin():
        destination = Destination(name="Self-service", telegram_channel_id=-1009001)
        session.add(destination)
        await session.flush()
        service = AdminService(session)
        for index in range(40):
            source = await service.create_source(
                name=f"Blind profile {index}",
                channel_id=50_000 + index,
                telegram_ref=f"@blind_{index}",
                destination_id=destination.id,
                actor_id=None,
            )
            strategy = ("single_message", "single_grouped_id", "multi_grouped_id")[index % 3]
            collection = CollectionProfile(
                strategy=strategy,
                order=("any", "media_before_text", "text_before_media", "text_between_media")[
                    index % 4
                ],
                merge_neighbor_grouped_ids=strategy == "multi_grouped_id",
                max_time_gap_seconds=Decimal(str(30 + index)),
                max_message_id_gap=5 + index,
                max_messages=10 + index % 20,
                max_media=10 + index % 20,
                allow_multiple_text_blocks=index % 2 == 0,
                merge_text_blocks=index % 2 == 0,
                boundary_rules=[
                    CollectionBoundaryRule(
                        name=f"next-{index}",
                        action="start",
                        match="regex",
                        pattern=rf"(?m)^ITEM-{index}\b",
                    )
                ],
            )
            profile = SourceProfileData(
                publication_mode=PublicationMode.AUTO,
                translation_enabled=False,
                default_currency="USD",
                default_destination_id=destination.id,
                collection=collection,
                extraction_rules={
                    "price": [
                        ExtractionRule(
                            kind="regex",
                            pattern=rf"COST-{index}:\s*(?P<value>\d+)\s*(?P<currency>USD)",
                            authoritative=True,
                        )
                    ],
                    "sizes": [
                        ExtractionRule(
                            kind="regex",
                            pattern=rf"SIZE-{index}:\s*(?P<value>[^\n]+)",
                            authoritative=True,
                        )
                    ],
                    "quantity": [
                        ExtractionRule(
                            kind="regex",
                            pattern=rf"QTY-{index}:\s*(?P<value>\d+)",
                            authoritative=True,
                        )
                    ],
                },
                cleaning_rules=[CleaningRule(kind="exact", value=f"NOISE-{index}")],
                gemini_instruction=f"Vocabulary guidance {index}",
            )
            await service.update_profile(
                source.id,
                profile.model_dump(mode="json"),
                actor_id=None,
            )
            await service.set_source_enabled(source.id, True, actor_id=None)
            source_ids.append(source.id)

    async with maker() as session:
        for index, source_id in enumerate(source_ids):
            source = await session.get(Source, source_id)
            assert source is not None and source.enabled is True
            profile = SourceProfileData.model_validate(source.profile.config)
            result = await process_product(
                Pipeline(None),
                PipelineInput(
                    source_id=source_id,
                    text=(
                        f"ITEM-{index}\nCOST-{index}: {10 + index} USD\n"
                        f"SIZE-{index}: S M\nQTY-{index}: 2\nNOISE-{index}"
                    ),
                    media_paths=[],
                    emoda_code=f"ZZ{index:05d}",
                    profile=profile,
                    markup_rules=[
                        MarkupRange(
                            min_price=Decimal("0"),
                            max_price=Decimal("1000"),
                            markup=Decimal("3"),
                            priority=1,
                        )
                    ],
                ),
                ProcessingMode.DRY,
            )
            assert result.parsed.fields["price"].value == str(10 + index)
            assert result.parsed.fields["sizes"].value == "S M"
            assert result.parsed.fields["quantity"].value == "2"
            assert f"NOISE-{index}" not in result.rendered_text
            assert result.needs_review is False
    await engine.dispose()
