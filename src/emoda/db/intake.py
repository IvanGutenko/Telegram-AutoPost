"""Transaction guards shared by collector and processing, never held during downloads."""
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, TimeoutError
from sqlalchemy.ext.asyncio import AsyncSession


def transient_database_error(error: Exception) -> bool:
    if isinstance(error, TimeoutError):
        return True
    if isinstance(error, DBAPIError):
        original = error.orig
        code = getattr(original, 'sqlstate', None) or getattr(original, 'pgcode', None)
        return bool(error.connection_invalidated or code in {'40P01', '40001', '55P03', '08006'})
    return False


async def lock_source(session: AsyncSession, source_id: int, *, wait: bool = True) -> bool:
    if session.bind is None or session.bind.dialect.name != 'postgresql':
        return True
    function = 'pg_advisory_xact_lock' if wait else 'pg_try_advisory_xact_lock'
    result = await session.scalar(text(f'SELECT {function}(694206, :source_id)'), {'source_id': source_id})
    return True if wait else bool(result)
