from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import UTC, datetime

from alembic.config import Config
from sqlalchemy import select
from telethon import TelegramClient

from alembic import command
from emoda.config import Settings
from emoda.db.models import AdminUser
from emoda.db.session import create_engine, create_session_factory
from emoda.integrations.session import secure_session_path
from emoda.runtime.app import run


def migrate() -> None:
    command.upgrade(Config("alembic.ini"), "head")


async def login(settings: Settings) -> None:
    if (
        settings.telegram_api_id is None
        or settings.telegram_api_hash is None
        or not settings.telegram_phone
    ):
        raise RuntimeError("TELEGRAM_API_ID, TELEGRAM_API_HASH and TELEGRAM_PHONE are required")
    secure_session_path(settings.telegram_session_path)
    client = TelegramClient(
        str(settings.telegram_session_path),
        settings.telegram_api_id,
        settings.telegram_api_hash.get_secret_value(),
    )
    try:
        await client.start(phone=settings.telegram_phone)
        me = await client.get_me()
        print(f"Collector authorized as {getattr(me, 'id', 'unknown')}")
    finally:
        await client.disconnect()
        secure_session_path(settings.telegram_session_path)


async def bootstrap_admin(settings: Settings, telegram_id: int, display_name: str | None) -> None:
    engine = create_engine(settings)
    sessions = create_session_factory(engine)
    async with sessions() as session, session.begin():
        admin = await session.scalar(
            select(AdminUser).where(AdminUser.telegram_user_id == telegram_id)
        )
        if admin is None:
            session.add(
                AdminUser(telegram_user_id=telegram_id, display_name=display_name, enabled=True)
            )
        else:
            admin.enabled = True
            admin.display_name = display_name or admin.display_name
    await engine.dispose()
    print(f"Admin {telegram_id} enabled")


def health(settings: Settings) -> int:
    try:
        age = datetime.now(UTC).timestamp() - settings.health_file.stat().st_mtime
    except FileNotFoundError:
        return 1
    return 0 if age < 90 else 1


def main() -> None:
    parser = argparse.ArgumentParser(prog="emoda")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("run")
    sub.add_parser("migrate")
    sub.add_parser("login")
    sub.add_parser("health")
    admin = sub.add_parser("bootstrap-admin")
    admin.add_argument("telegram_id", type=int)
    admin.add_argument("--name")
    args = parser.parse_args()
    settings = Settings()
    if args.command == "run":
        migrate()
        asyncio.run(run(settings))
    elif args.command == "migrate":
        migrate()
    elif args.command == "login":
        asyncio.run(login(settings))
    elif args.command == "bootstrap-admin":
        asyncio.run(bootstrap_admin(settings, args.telegram_id, args.name))
    elif args.command == "health":
        sys.exit(health(settings))


if __name__ == "__main__":
    main()
