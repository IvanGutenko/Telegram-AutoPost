from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from emoda.db.models import CodeCounter, Product

ALPHA2_NUMERIC5_CAPACITY = 26 * 26 * 100_000


class CodeAllocator:
    async def next(
        self, session: AsyncSession, *, prefix: str, width: int, name: str = "products"
    ) -> str:
        if width < 1 or width > 20:
            raise ValueError("code width must be between 1 and 20")
        if not prefix or len(prefix) > 20:
            raise ValueError("code prefix must be 1..20 characters")
        dialect = session.bind.dialect.name if session.bind else ""
        if dialect == "postgresql":
            await session.execute(
                insert(CodeCounter)
                .values(name=name, next_value=1)
                .on_conflict_do_nothing(index_elements=["name"])
            )
        counter = await session.scalar(
            select(CodeCounter).where(CodeCounter.name == name).with_for_update()
        )
        if counter is None:
            counter = CodeCounter(name=name, next_value=1)
            session.add(counter)
            await session.flush()
        value = counter.next_value
        counter.next_value += 1
        await session.flush()
        return f"{prefix}-{value:0{width}d}"

    async def next_alpha2_numeric5(
        self,
        session: AsyncSession,
        *,
        name: str = "products_alpha2_numeric5",
    ) -> str:
        dialect = session.bind.dialect.name if session.bind else ""
        if dialect == "postgresql":
            await session.execute(
                insert(CodeCounter)
                .values(name=name, next_value=0)
                .on_conflict_do_nothing(index_elements=["name"])
            )
        counter = await session.scalar(
            select(CodeCounter).where(CodeCounter.name == name).with_for_update()
        )
        if counter is None:
            counter = CodeCounter(name=name, next_value=0)
            session.add(counter)
            await session.flush()

        while counter.next_value < ALPHA2_NUMERIC5_CAPACITY:
            ordinal = counter.next_value
            counter.next_value += 1
            letter_pair = ordinal // 100_000
            code = (
                f"{chr(ord('A') + letter_pair // 26)}"
                f"{chr(ord('A') + letter_pair % 26)}"
                f"{ordinal % 100_000:05d}"
            )
            occupied = await session.scalar(select(Product.id).where(Product.emoda_code == code))
            if occupied is None:
                await session.flush()
                return code

        raise RuntimeError("E-MODA code space exhausted")
