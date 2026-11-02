"""Read-only collection explanation, using the production boundary engine."""
from __future__ import annotations

from emoda.domain.schemas import CollectionProfile
from emoda.services.collection import CollectionEngine, CollectionMessage


def collection_report(
    messages: list[CollectionMessage],
    profile: CollectionProfile,
    *,
    stored_groups: dict[int, int | None] | None = None,
) -> str:
    engine = CollectionEngine(profile)
    partition = engine.partition(messages)
    lines = [
        'Тестер сборки — без публикации и Gemini',
        f'Сообщений: {len(messages)}; media: {sum(m.media_count for m in messages)}; '
        f'товаров по текущему профилю: {len(partition.products)}',
        f'Стратегия: {profile.strategy}; порядок: {profile.order}',
        f'Debounce: {profile.debounce_seconds} с; окно: {profile.max_time_gap_seconds} с; '
        f'ждать следующий старт: {"да" if profile.finalize_on_next_start else "нет"}',
        'Это разбор выбранного окна; начало/конец товара могут находиться вне него.',
    ]
    if stored_groups is not None:
        transport: dict[str, set[int | None]] = {}
        for message in messages:
            if message.grouped_id is not None:
                transport.setdefault(str(message.grouped_id), set()).add(stored_groups.get(message.message_id))
        for grouped_id, groups in transport.items():
            if len(groups) > 1:
                lines.append(f'⚠ РАЗРЫВ: Telegram album {grouped_id} → сохранённые сборки {sorted(str(g) for g in groups)}')
    previous: list[CollectionMessage] = []
    for index, batch in enumerate(partition.products, 1):
        ids = ', '.join(str(m.message_id) for m in batch.messages)
        albums = ', '.join(sorted({str(m.grouped_id) for m in batch.messages if m.grouped_id is not None})) or 'нет'
        text = engine.merged_text(batch.messages)
        decision = engine.decide(previous, batch.messages[0])
        lines.extend([
            f'\nТовар {index} — media: {sum(m.media_count for m in batch.messages)}',
            f'Сообщения: {ids}', f'Telegram albums: {albums}',
            f'Начало: {decision.reason}' + (f' / {decision.rule_name}' if decision.rule_name else ''),
            f'Текст: {text[:350]}' if text else 'Текст: отсутствует — парсер не получит описание/цену',
        ])
        if stored_groups is not None:
            actual = sorted({str(stored_groups.get(m.message_id)) for m in batch.messages})
            lines.append(f'Фактические сборки в БД: {", ".join(actual)}')
        previous = batch.messages
    if partition.ignored:
        lines.append('\nИсключены границами IGNORE: ' + ', '.join(str(m.message_id) for m in partition.ignored))
    return '\n'.join(lines)
