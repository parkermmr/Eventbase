import asyncio
import logging
import signal

from eventbase import Courier, Message, S3Store

logger = logging.getLogger("pipeline")


async def handle(message: Message) -> None:
    """Your work. Raise to leave the message unacknowledged."""
    ...


async def drain(courier: Courier, stop: asyncio.Event) -> None:
    """Poll, handle, acknowledge, until stop is set."""
    while not stop.is_set():
        messages = await courier.poll(limit=50, timeout=1.0)
        for message in messages:
            try:
                await handle(message)
            except Exception:
                logger.exception("failed %s", message.id)
                raise
            await courier.acknowledge(message)


async def run(stop: asyncio.Event) -> None:
    """Keep a courier alive, reconnecting with backoff when it drops."""
    delay = 1.0
    while not stop.is_set():
        courier = Courier.kafka(
            "events",
            store=S3Store("events", endpoint="http://minio:9000"),
            bootstrap="kafka:9092",
            group="my-pipeline",
            parallel=16,
        )
        try:
            await courier.connect()
            delay = 1.0
            await drain(courier, stop)
        except Exception:
            logger.exception("courier dropped, retrying in %.0fs", delay)
            await asyncio.wait([asyncio.create_task(stop.wait())], timeout=delay)
            delay = min(delay * 2, 30.0)
        finally:
            await courier.disconnect()


async def main() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await run(stop)


if __name__ == "__main__":
    asyncio.run(main())
