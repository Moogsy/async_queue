import asyncio
from collections import defaultdict, deque
from datetime import datetime, timedelta, timezone
from typing import Iterable, Iterator, Optional

import errors
from events import Events

class QueueBase:
    """
    A base for basic Lifo and Fifo queues.
    """

    def __init__(
        self,
        iterable: Iterable = [],
        *,
        maxlen: Optional[int] = None,
    ):

        if isinstance(maxlen, int) and maxlen < 0:
            maxlen = None

        # Storage
        self.maxlen = maxlen
        self._deque = deque(iterable, maxlen=maxlen)

        self._loop = asyncio.get_event_loop()

        # Events
        self._listeners = defaultdict(list)

    # Timeout helper

    @staticmethod
    async def _check_expiry_date(started_at: datetime, timeout: timedelta):
        now = datetime.now(tz=timezone.utc)

        if now - started_at > timeout:
            raise asyncio.TimeoutError("Timeout reached")

    # Add stuff

    async def put(self, item, timeout: Optional[timedelta] = None):
        """
        Adds an item to the queue.
        If the queue is full, will wait until a slot is free
        """
        if timeout is not None:
            started_at = datetime.now(tz=timezone.utc)

        if self.maxlen is not None:
            while self.is_full():
                if timeout is None:
                    await asyncio.sleep(0)
                else:
                    await self._check_expiry_date(started_at, timeout)  # type: ignore

        self._deque.append(item)

        if self.is_full():
            self.dispatch_event(Events.Full)

    def put_nowait(self, item):
        """
        Ads a single item to the queue.
        Raises Full if the queue is full.
        """
        if self.is_full():
            raise errors.Full()
        else:
            self._deque.append(item)

            if self.is_full():
                self.dispatch_event(Events.Full)

    async def puts(self, *items, atomic: bool = False):
        """
        Adds as many items as possible to the queue.
        If the queue is full and atomic is set to False, will add items as slots are freed
        else, will wait until enough slots are free and add them at once when it happens.
        NOTE: Contiguous addition is not guaranteed if not atomic
        """
        if self.maxlen is None:
            self._deque.extend(items)

        elif atomic:
            # Hangs until enough room is made
            while not self.has_enough_room_for(len(items)):
                await asyncio.sleep(0)

            self._deque.extend(items)

            if self.is_full():
                self.dispatch_event(Events.Full)

        else:
            # Add everything if no maxlen was provided
            while True:
                # Adds as many items as possible
                free_slots = self.maxlen - len(self._deque)

                self._deque.extend(items[:free_slots])
                items = items[free_slots:]

                if not items:
                    return

                if self.is_full():
                    self.dispatch_event(Events.Full)

                await asyncio.sleep(0)

    # Remove stuff

    def _get(self):
        """
        Needs to be overriden by subclasses
        (popping left or right turns a FIFO into a LIFO)
        """
        raise NotImplementedError()

    async def get(self, timeout: Optional[timedelta] = None):
        """
        Removes and gets an item from the queue.
        If the queue is empty, will wait until an item is put
        """
        started_at = datetime.now(timezone.utc)

        while self.is_empty():
            if timeout is None:
                await asyncio.sleep(0)
            else:
                await self._check_expiry_date(started_at, timeout)

        if len(self) == 1:
            self.dispatch_event(Events.Empty)

        return self._get()

    async def gets(
        self,
        atleast: int = 0,
        atmost: Optional[int] = None,
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

            if self.is_empty():
                self.dispatch_event(Events.Empty)

        # Hangs until it collected enough items
        else:
            while len(out) < atleast:
                while not self.is_empty() and len(out) < atleast:
                    item = self._get()
                    out.append(item)

                if self.is_empty():
                    self.dispatch_event(Events.Empty)

                await asyncio.sleep(0)

            await asyncio.sleep(0)

            # Collects items to reach the objective if possible
            while self._deque and len(out) < atmost:
                item = self._get()
                out.append(item)

            if self.is_empty():
                self.dispatch_event(Events.Empty)

        return out

    def get_nowait(self):
        """
        Retrieves a single item from the queue.
        Raises Empty if the queue is empty.
        """
        try:
            if len(self._deque) == 1:
                self.dispatch_event(Events.Empty)

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

        return self.__len__() >= self.maxlen

    def has_enough_room_for(self, n: int):
        """
        Returns True if the queue has enough space to add n items
        """
        if self.maxlen is None:
            return True

        free = self.maxlen - len(self._deque)

        return n <= free

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

    # Events
    def dispatch_event(self, event: Events):
        coro = self._dispatch_event(event)
        self._loop.create_task(coro)

    async def _dispatch_event(self, event: Events):
        if isinstance(event, Events):
            event = event.value

        for future in self._listeners[event]:
            future.set_result(None)

        self._listeners[event].clear()

    def wait_for(self, event: Events, timeout: Optional[float] = None):
        if isinstance(event, Events):
            event = event.value

        future = self._loop.create_future()

        self._listeners[event].append(future)

        return asyncio.wait_for(future, timeout)


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
