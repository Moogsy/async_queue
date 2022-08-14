from asyncio import sleep
from collections import deque
from functools import partial
from typing import Iterable

import errors


class QueueBase:
    """
    A base for Lifo and Fifo queues.
    """
    def __init__(self, iterable: Iterable = [], *, maxlen: int = None):
        if isinstance(maxlen, int) and maxlen < 0:
            maxlen = None

        self._maxlen = maxlen
        self._deque = deque(iterable, maxlen=maxlen)

    # Attributes
    @property
    def maxlen(self) -> int:
        return self._maxlen

    # Add stuff

    async def put(self, item):
        """
        Adds an item to the queue.
        If the queue is full, will wait until a slot is free
        """
        if self._maxlen is not None:
            while len(self.deque) > self._maxlen:
                await sleep(0)

        self._deque.append(item)

    def put_nowait(self, item):
        """
        Ads a single item to the queue.
        Raises Full if the queue is full.
        """
        maxlen = self.maxlen or float("inf")

        if len(self._queue) < maxlen:
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
            maxlen = self.maxlen or float("inf")

            while len(items) > maxlen - len(self._deque):
                await sleep(0)

            self._deque.extend(items)

        else:
            # Add everything if no maxlen was provided
            maxlen = self.maxlen or len(items) + len(self._deque)

            while True:
                # Adds as many items as possible
                free_slots = maxlen - len(self._deque)
                self._deque.extend(items[:free_slots])
                items = items[free_slots:]

                if not items:
                    return

                await sleep(0)

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

    def empty(self) -> bool:
        """
        Returns True if the queue is currently empty, False otherwise.
        """
        return self.__len__() == 0

    def full(self) -> bool:
        """
        Returns True if the queue is full. False otherwise.
        """
        if self.maxlen is None:
            return False

        return self.__len__() == self.maxlen


async def _get(func, self):
    """
    Removes and gets an item from the queue.
    If the queue is empty, will wait until an item is put
    """
    while not self._deque:
        await sleep(0)

    return func(self._deque)


async def _gets(
    func,
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

    if atleast > atmost:
        raise ValueError("Parameter atleast cannot be bigger than parameter atmost")

    out = []

    if atomic:
        # Hangs until enough items are present
        while len(self._deque) < atleast:
            await sleep(0)

        while self._deque and len(out) < atmost:
            item = func(self._deque)
            out.append(item)

        return out

    # Hangs until it collected enough items

    while len(out) < atleast:
        while self._deque and len(out) < atleast:
            item = func(self._deque)
            out.append(item)

        await sleep(0)

    # Collects items to reach the objective if possible
    while self._deque and len(out) < atmost:
        item = func(self._deque)
        out.append(item)

    return out


def _get_nowait(func, self):
    """
    Retrieves a single item from the queue.
    Raises Empty if the queue is empty.
    """
    try:
        return func(self._deque)
    except IndexError:
        raise errors.Empty()


class AsyncQueue(QueueBase):
    """
    An async version of the Queue class found in the stdlib queue module.
    Acts as a FIFO queue.
    """

    get = partial(_get, deque.popleft)
    get_nowait = partial(_get_nowait, deque.popleft)

    gets = partial(_gets, deque.popleft)


class AsyncLifoQueue(QueueBase):
    """
    An async version of the Queue class found in the stdlib queue module.
    Acts as a LIFO queue.
    """
    get = partial(_get, deque.pop)
    gets = partial(_gets, deque.pop)
