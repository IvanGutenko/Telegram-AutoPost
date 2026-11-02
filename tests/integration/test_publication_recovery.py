import re
from html import unescape

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from emoda.db.base import Base
from emoda.db.models import Destination, Product, PublicationOutbox, Source
from emoda.domain.enums import OutboxStatus, ProductStatus
from emoda.services.publication import PublicationService, UncertainPublicationFailure


class ProcessDeath(BaseException):
    pass


class DestinationHistory:
    def __init__(self) -> None:
        self.messages: dict[tuple[int, str], list[int]] = {}
        self.send_count = 0
        self.fail_after_accept: BaseException | Exception | None = None
        self.payload_texts: list[str] = []

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        self.send_count += 1
        key = (destination_channel_id, str(payload["emoda_code"]))
        text = str(payload["text"])
        self.payload_texts.append(text)
        telegram_plain_text = unescape(re.sub(r"</?b>", "", text))
        exact_code_line = re.compile(
            rf"(?m)^Код / Code:[ \t]+{re.escape(str(payload['emoda_code']))}[ \t]*$"
        )
        if exact_code_line.search(telegram_plain_text):
            self.messages[key] = [501]
        if self.fail_after_accept:
            raise self.fail_after_accept
        return [501]

    async def find_by_code(self, destination_channel_id: int, emoda_code: str) -> list[int] | None:
        return self.messages.get((destination_channel_id, emoda_code))

    async def update(
        self,
        destination_channel_id: int,
        message_ids: list[int],
        old_payload: dict[str, object],
        new_payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        return message_ids


class EditDuringSendHistory(DestinationHistory):
    def __init__(self, maker: async_sessionmaker[AsyncSession], product_id: int) -> None:
        super().__init__()
        self.maker = maker
        self.product_id = product_id

    async def send(
        self,
        destination_channel_id: int,
        payload: dict[str, object],
        keyboard: list[list[dict[str, str]]],
    ) -> list[int]:
        result = await super().send(destination_channel_id, payload, keyboard)
        async with self.maker() as concurrent_session, concurrent_session.begin():
            product = await concurrent_session.get(Product, self.product_id)
            assert product is not None
            product.status = ProductStatus.READY
            product.processing_error = "supplier edit arrived during send"
        return result


@pytest.fixture
async def publication_db(
    tmp_path: object,
) -> tuple[async_sessionmaker[AsyncSession], int, int]:
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    maker = async_sessionmaker(engine, expire_on_commit=False)
    async with maker() as session, session.begin():
        source = Source(name="supplier", telegram_channel_id=123, enabled=True)
        destination = Destination(name="E-MODA", telegram_channel_id=-1001)
        session.add_all([source, destination])
        await session.flush()
        product = Product(
            emoda_code="EM-000777",
            source_id=source.id,
            destination_id=destination.id,
            final_text=(
                "<b>Код / Code:</b> EM-000777\n"
                "<b>Цена за шт / Unit price:</b> $23\n"
                "<b>Размеры / Sizes:</b> S, M, L\n"
                "<b>Шт в серии / Pcs per lot:</b> 6"
            ),
            status=ProductStatus.READY_TO_PUBLISH,
        )
        session.add(product)
        await session.flush()
        product_id, destination_id = product.id, destination.id
    yield maker, product_id, destination_id
    await engine.dispose()


async def create_outbox(
    maker: async_sessionmaker[AsyncSession],
    product_id: int,
    destination_id: int,
    service: PublicationService,
) -> int:
    async with maker() as session:
        product = await session.get(Product, product_id)
        destination = await session.get(Destination, destination_id)
        assert product is not None and destination is not None
        outbox = await service.create(
            session,
            product=product,
            destination=destination,
            payload={
                "emoda_code": product.emoda_code,
                "text": product.final_text,
                "media_paths": [],
            },
            keyboard=[],
        )
        return outbox.id


async def test_fresh_process_reconciles_acceptance_without_second_send(
    publication_db: tuple[async_sessionmaker[AsyncSession], int, int],
) -> None:
    maker, product_id, destination_id = publication_db
    history = DestinationHistory()
    service = PublicationService(history)
    outbox_id = await create_outbox(maker, product_id, destination_id, service)
    history.fail_after_accept = ProcessDeath()
    async with maker() as session:
        with pytest.raises(ProcessDeath):
            await service.publish(session, outbox_id)
    history.fail_after_accept = None
    restarted_service = PublicationService(history)
    async with maker() as restarted_session:
        await restarted_service.reconcile(restarted_session)
    async with maker() as verification_session:
        outbox = await verification_session.get(PublicationOutbox, outbox_id)
        product = await verification_session.get(Product, product_id)
        assert outbox is not None and product is not None
        assert outbox.status is OutboxStatus.PUBLISHED
        assert outbox.telegram_message_ids == [501]
        assert product.status is ProductStatus.PUBLISHED
    assert history.send_count == 1
    assert history.payload_texts == [
        "<b>Код / Code:</b> EM-000777\n"
        "<b>Цена за шт / Unit price:</b> $23\n"
        "<b>Размеры / Sizes:</b> S, M, L\n"
        "<b>Шт в серии / Pcs per lot:</b> 6"
    ]


async def test_uncertain_timeout_is_reconciled_before_any_retry(
    publication_db: tuple[async_sessionmaker[AsyncSession], int, int],
) -> None:
    maker, product_id, destination_id = publication_db
    history = DestinationHistory()
    service = PublicationService(history)
    outbox_id = await create_outbox(maker, product_id, destination_id, service)
    history.fail_after_accept = UncertainPublicationFailure("response lost")
    async with maker() as session:
        result = await service.publish(session, outbox_id)
        assert result.status is OutboxStatus.RECONCILIATION_REQUIRED
    history.fail_after_accept = None
    async with maker() as restarted_session:
        await PublicationService(history).reconcile(restarted_session)
    async with maker() as verification_session:
        outbox = await verification_session.get(PublicationOutbox, outbox_id)
        assert outbox is not None
        assert outbox.status is OutboxStatus.PUBLISHED
    assert history.send_count == 1


async def test_supplier_edit_state_is_not_lost_when_it_arrives_during_send(
    publication_db: tuple[async_sessionmaker[AsyncSession], int, int],
) -> None:
    maker, product_id, destination_id = publication_db
    history = EditDuringSendHistory(maker, product_id)
    service = PublicationService(history)
    outbox_id = await create_outbox(maker, product_id, destination_id, service)
    async with maker() as session:
        await service.publish(session, outbox_id)
    async with maker() as verification_session:
        product = await verification_session.get(Product, product_id)
        outbox = await verification_session.get(PublicationOutbox, outbox_id)
        assert product is not None and outbox is not None
        assert outbox.status is OutboxStatus.PUBLISHED
        assert product.published_at is not None
        assert product.status is ProductStatus.READY
        assert product.processing_error == "supplier edit arrived during send"
