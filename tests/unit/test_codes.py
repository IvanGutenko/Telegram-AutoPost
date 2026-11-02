import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import CodeCounter, Product, Source
from emoda.domain.enums import ProductStatus
from emoda.services.codes import CodeAllocator


@pytest.fixture
async def session() -> AsyncSession:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as db:
        yield db
    await engine.dispose()


async def test_codes_are_monotonic_and_configurable(session: AsyncSession) -> None:
    allocator = CodeAllocator()
    assert await allocator.next(session, prefix="EM", width=6) == "EM-000001"
    assert await allocator.next(session, prefix="EM", width=6) == "EM-000002"
    assert await allocator.next(session, prefix="SKU", width=3) == "SKU-003"


async def test_alpha2_numeric5_codes_are_monotonic_across_letter_boundary(
    session: AsyncSession,
) -> None:
    allocator = CodeAllocator()
    assert await allocator.next_alpha2_numeric5(session) == "AA00000"
    assert await allocator.next_alpha2_numeric5(session) == "AA00001"
    counter = await session.scalar(
        select(CodeCounter).where(CodeCounter.name == "products_alpha2_numeric5")
    )
    assert counter is not None
    counter.next_value = 100_000
    await session.flush()
    assert await allocator.next_alpha2_numeric5(session) == "AB00000"


async def test_alpha2_numeric5_allocator_stops_instead_of_reusing_codes(
    session: AsyncSession,
) -> None:
    session.add(CodeCounter(name="products_alpha2_numeric5", next_value=67_599_999))
    await session.flush()
    allocator = CodeAllocator()
    assert await allocator.next_alpha2_numeric5(session) == "ZZ99999"
    with pytest.raises(RuntimeError, match="E-MODA code space exhausted"):
        await allocator.next_alpha2_numeric5(session)


async def test_deleting_product_does_not_make_its_code_reusable(session: AsyncSession) -> None:
    allocator = CodeAllocator()
    first_code = await allocator.next_alpha2_numeric5(session)
    source = Source(name="supplier", telegram_channel_id=123)
    session.add(source)
    await session.flush()
    product = Product(
        emoda_code=first_code,
        source_id=source.id,
        status=ProductStatus.RECEIVED,
    )
    session.add(product)
    await session.flush()
    await session.delete(product)
    await session.flush()

    assert await allocator.next_alpha2_numeric5(session) == "AA00001"


async def test_alpha2_numeric5_allocator_skips_code_already_present_in_products(
    session: AsyncSession,
) -> None:
    source = Source(name="supplier", telegram_channel_id=124)
    session.add(source)
    await session.flush()
    session.add(
        Product(
            emoda_code="AA00000",
            source_id=source.id,
            status=ProductStatus.RECEIVED,
        )
    )
    await session.flush()

    assert await CodeAllocator().next_alpha2_numeric5(session) == "AA00001"
