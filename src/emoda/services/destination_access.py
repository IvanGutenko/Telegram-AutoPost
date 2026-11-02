from __future__ import annotations

from aiogram import Bot
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError


class DestinationAccessError(ValueError):
    """Read-only preflight failed; no publication has been attempted."""


async def validate_destination_access(bot: Bot | None, channel_id: int) -> str:
    """Check current rights without caching or sending a probe to the channel."""
    action = (
        f"Канал {channel_id}. Проверьте ID и добавьте бота администратором с правом "
        "публикации. /admin → Каналы E-MODA → Проверить доступ. "
        "После исправления повторите публикацию из очереди проверки."
    )
    if bot is None:
        raise DestinationAccessError(f"Бот не подключён. {action}")
    try:
        chat = await bot.get_chat(channel_id)
        if chat.type != "channel":
            raise DestinationAccessError(f"Назначение должно быть каналом. {action}")
        bot_user = await bot.get_me()
        member = await bot.get_chat_member(channel_id, bot_user.id)
        if member.status != "creator" and (
            member.status != "administrator" or not getattr(member, "can_post_messages", False)
        ):
            raise DestinationAccessError(f"У бота нет права публикации. {action}")
    except DestinationAccessError:
        raise
    except (TelegramBadRequest, TelegramForbiddenError) as exc:
        raise DestinationAccessError(f"Канал недоступен боту. {action}") from exc
    except Exception as exc:
        raise DestinationAccessError(
            f"Не удалось проверить доступ к каналу {channel_id}: Telegram временно недоступен. "
            "Отправка не начата. Повторите /admin → Каналы E-MODA → Проверить доступ."
        ) from exc
    return chat.title or str(channel_id)
