import asyncio
from asyncio import get_event_loop, sleep
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator

import errors


class QueueBase:
    """
    A base for basic Lifo and Fifo queues.
    """

    def __init__(
        self,
        iterable: Iterable = [],
        *,
        maxlen: int = None,
    ):

        if isinstance(maxlen, int) and maxlen < 0:
            maxlen = None

        self.maxlen = maxlen
        self._deque = deque(iterable, maxlen=maxlen)

    # Timeout helper

    @staticmethod
    async def _check_expiry_date(started_at: datetime, timeout: timedelta):
        now = datetime.now(tz=timezone.utc)

        if now - started_at > timeout:
            raise asyncio.TimeoutError("Timeout reached")

    # Add stuff

    async def put(self, item, timeout: timedelta = None):
        """
        Adds an item to the queue.
        If the queue is full, will wait until a slot is free
        """
        if timeout is not None:
            started_at = datetime.now(tz=timezone.utc)

        if self._maxlen is not None:
            while len(self.deque) > self._maxlen:
                if timeout is None:
                    await asyncio.sleep(0)
                else:
                    await self._check_expiry_date(started_at, timeout)

        self._deque.append(item)

    def put_nowait(self, item):
        """
        Ads a single item to the queue.
        Raises Full if the queue is full.
        """
        if len(self._queue) < self._maxlen or float("inf"):
            self._deque.append(item)
        else:
            raise errors.Full()

    async def puts(self, *items, atomic: bool = False):
        """
        Adds as many items as possible to the queue.
        If the queue is full and atomic is set to False, will add items as slots are freed
        else, will wait until enough slots are free and add them at once when it happens.
        NOTE: Contiguous addition is not guaranteed if not atomic
        """
        if atomic:
            # Hangs until enough room is made
            while len(items) > (self.maxlen or float("inf")) - len(self._deque):
                await asyncio.sleep(0)

            self._deque.extend(items)

        else:
            # Add everything if no maxlen was provided
            while True:
                # Adds as many items as possible
                free_slots = (self.maxlen or len(items) + len(self._deque)) - len(
                    self._deque
                )

                self._deque.extend(items[:free_slots])
                items = items[free_slots:]

                if not items:
                    return

                await asyncio.sleep(0)

    # Remove stuff

    def _get(self):
        """
        Needs to be overriden by subclasses
        (popping left or right turns a FIFO into a LIFO)
        """
        raise NotImplementedError()

    async def get(self, timeout: timedelta = None):
        """
        Removes and gets an item from the queue.
        If the queue is empty, will wait until an item is put
        """
        started_at = datetime.now(timezone.utc)

        while not self._deque:
            if timeout is None:
                await asyncio.sleep(0)
            else:
                await self._check_expiry_date(started_at, timeout)

        return self._get()

    async def gets(
        self,
        atleast: int = 0,
        atmost: int = None,
        atomic: bool = False,
    ) -> list:

        """
        Retrieves multiple items from the queue.
        If there isn't enough items, will wait until just enough items are added.
        Will always try to fetch `atmost` items if enough are directly available
        NOTE: Contiguous fetching is not guaranteed if not atomic
        """
        if atleast < 1:
            raise ValueError("Must fetch atleast 1 item")

        if atmost is None:
            atmost = atleast

        if atleast > atmost:
            raise ValueError("Parameter atleast cannot be bigger than parameter atmost")

        if atleast == 1:
            return await self.get()

        out = []

        if atomic:
            # Hangs until enough items are present
            while len(self._deque) < atleast:
                await asyncio.sleep(0)

            while self._deque and len(out) < atmost:
                item = self._get()
                out.append(item)

            return out

        # Hangs until it collected enough items

        while len(out) < atleast:
            while self._deque and len(out) < atleast:
                item = self._get()
                out.append(item)

            await asyncio.sleep(0)

        # Collects items to reach the objective if possible
        while self._deque and len(out) < atmost:
            item = self._get()
            out.append(item)

        return out

    def get_nowait(func, self):
        """
        Retrieves a single item from the queue.
        Raises Empty if the queue is empty.
        """
        try:
            return self._get()
        except IndexError:
            raise errors.Empty()

    # Deque-like interface

    def __len__(self) -> int:
        return self._deque.__len__()

    def __bool__(self) -> bool:
        return self.__len__() == 0

    def clear(self):
        """
        Clears the queue
        """
        self._deque.clear()

    def copy(self) -> "QueueBase":
        """
        Returns a shallow copy of this queue
        """
        return self.__class__(self._deque.copy(), maxlen=self.maxlen)

    def count(self, x) -> int:
        """
        Counts the amount of elements equal to x
        """
        return self._deque.count(x)

    # Queue-like interface

    def qsize(self) -> int:
        """
        Returns this queue's current size
        """
        return self.__len__()

    def is_empty(self) -> bool:
        """
        Returns True if the queue is currently empty, False otherwise.
        """
        return self.__len__() == 0

    def is_full(self) -> bool:
        """
        Returns True if the queue is full. False otherwise.
        """
        if self.maxlen is None:
            return False

        return self.__len__() == self.maxlen

    def __getitem__(self, index: int):
        return self._deque.__getitem__(index)

    def __iter__(self) -> Iterator:
        return self._deque.__iter__()

    async def stream(self):
        """
        Returns a stream, so that

        async for x in queue.stream():
            ...

        is equivalent to

        while True:
            x = await queue.get()
        """
        while True:
            yield await self.get()

    # Formatting
    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({list(map(repr, self._deque))}, maxlen={self.maxlen}))"


class AsyncQueue(QueueBase):
    """
    An async version of the Queue class found in the stdlib queue module.
    Acts as a FIFO queue.
    """

    def _get(self):
        return self._deque.popleft()


class AsyncLifoQueue(QueueBase):
    """
    An async version of the Queue class found in the stdlib queue module.
    Acts as a LIFO queue.
    """

    def _get(self):
        return self._deque.pop()
