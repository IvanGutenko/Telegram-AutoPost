from contextvars import ContextVar

current_admin_id: ContextVar[int | None] = ContextVar("current_admin_id", default=None)
