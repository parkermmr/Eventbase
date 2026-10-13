"""
Runner keeps a handler running over a courier until told to stop.
"""

import asyncio
import inspect
import logging
from signal import SIGINT, SIGTERM
from typing import Awaitable, Callable

from eventbase.courier import Courier
from eventbase.models.message import Message

logger = logging.getLogger(__name__)

Handler = Callable[[Message], Awaitable[None] | None]


class Runner:
    """
    Polls a courier, hands each message to handle, and acknowledges it
    once handled. Delivery is at least once, so handle should tolerate
    seeing a message twice. A plain function handler runs on a worker
    thread; a coroutine function runs on the loop.

    Messages are acknowledged in bus order and never past one that
    failed, because a kafka acknowledgement also covers everything
    earlier in its partition. A failed message is released with the
    rest of its batch and retried after a pause. Once it has failed
    retries more times it is given up on: published to dead, a
    dead letter courier, when there is one, then acknowledged.
    concurrency above one handles a batch concurrently, and successes
    behind a failure are handled again on redelivery.

    Failures outside the handler, from the broker, the store or dead,
    disconnect the courier and reconnect after a doubling backoff that
    starts at pause and tops out at ceiling. stop, or SIGINT and
    SIGTERM when run installs signals, finishes the batch in hand and
    returns.
    """

    def __init__(
        self,
        courier: Courier,
        handle: Handler,
        limit: int = 50,
        timeout: float = 1.0,
        concurrency: int = 1,
        retries: int = 3,
        dead: Courier | None = None,
        pause: float = 1.0,
        ceiling: float = 30.0,
    ) -> None:
        if concurrency < 1:
            raise ValueError("concurrency must be at least 1")
        self.courier = courier
        self.handle = handle
        self.limit = limit
        self.timeout = timeout
        self.concurrency = concurrency
        self.retries = retries
        self.dead = dead
        self.pause = pause
        self.ceiling = ceiling
        self.attempts: dict[str, int] = {}
        self.delay = pause
        self.gate = asyncio.Semaphore(concurrency)
        self.stopping = asyncio.Event()

    def stop(self) -> None:
        """Finish the batch in hand, then return from run."""
        self.stopping.set()

    async def run(self, signals: bool = True) -> None:
        """Run until stop, reconnecting whenever the courier fails."""
        installed = self.listen() if signals else []
        try:
            while not self.stopping.is_set():
                try:
                    await self.open()
                    while not self.stopping.is_set():
                        await self.step()
                        self.delay = self.pause
                except Exception:
                    logger.exception("courier failed, reconnecting in %.1fs", self.delay)
                    await self.wait(self.delay)
                    self.delay = min(self.delay * 2, self.ceiling)
                finally:
                    await self.close()
        finally:
            self.unlisten(installed)

    async def step(self) -> None:
        """One poll, handled and acknowledged up to the first failure."""
        messages = await self.courier.poll(self.limit, timeout=self.timeout)
        if self.concurrency == 1:
            for index, message in enumerate(messages):
                if self.stopping.is_set():
                    return
                if not await self.settle(message, await self.attempt(message)):
                    return await self.retry(messages[index:])
            return
        errors = await asyncio.gather(*(self.attempt(message) for message in messages))
        for index, (message, error) in enumerate(zip(messages, errors)):
            if not await self.settle(message, error):
                return await self.retry(messages[index:])

    async def attempt(self, message: Message) -> Exception | None:
        """Handle one message; the error it raised, or None."""
        async with self.gate:
            try:
                if inspect.iscoroutinefunction(self.handle):
                    await self.handle(message)
                else:
                    await asyncio.to_thread(self.handle, message)
            except Exception as error:
                return error
        return None

    async def settle(self, message: Message, error: Exception | None) -> bool:
        """
        Acknowledge a handled message, or one given up on after its
        retries. False means it should be retried.
        """
        if error is not None:
            failures = self.attempts.get(message.id, 0) + 1
            if failures <= self.retries:
                self.attempts[message.id] = failures
                logger.warning(
                    "message %s failed, retry %d of %d: %r",
                    message.id, failures, self.retries, error,
                )
                return False
            await self.bury(message, error, failures)
        self.attempts.pop(message.id, None)
        await self.courier.acknowledge(message)
        return True

    async def bury(self, message: Message, error: Exception, failures: int) -> None:
        """Publish a message given up on to dead, or drop it."""
        if self.dead is None:
            logger.error("message %s dropped after %d failures: %r", message.id, failures, error)
            return
        metadata = {**message.metadata, "error": repr(error), "failures": failures}
        await self.dead.publish(message.content, metadata, key=message.id)
        logger.error("message %s dead lettered after %d failures: %r", message.id, failures, error)

    async def retry(self, messages: list[Message]) -> None:
        """Release messages for redelivery and pause before the next poll."""
        await self.courier.release(messages)
        await self.wait(self.pause)

    async def open(self) -> None:
        """Connect the courier and the dead letter courier."""
        await self.courier.connect()
        if self.dead is not None:
            await self.dead.connect()

    async def close(self) -> None:
        """Disconnect both couriers, logging rather than raising."""
        for courier in (self.dead, self.courier):
            if courier is None:
                continue
            try:
                await courier.disconnect()
            except Exception:
                logger.exception("disconnect failed")

    async def wait(self, delay: float) -> None:
        """Sleep for delay, waking early on stop."""
        try:
            await asyncio.wait_for(self.stopping.wait(), delay)
        except TimeoutError:
            pass

    def listen(self) -> list:
        """Install stop as the SIGINT and SIGTERM handler where the loop allows."""
        loop = asyncio.get_running_loop()
        installed = []
        for number in (SIGINT, SIGTERM):
            try:
                loop.add_signal_handler(number, self.stop)
            except (NotImplementedError, RuntimeError, ValueError):
                continue
            installed.append(number)
        return installed

    def unlisten(self, installed: list) -> None:
        """Remove the signal handlers listen installed."""
        loop = asyncio.get_running_loop()
        for number in installed:
            loop.remove_signal_handler(number)
