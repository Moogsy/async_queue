import asyncio
from typing import Union
from datetime import datetime, timedelta, timezone


class Timeout:

    @staticmethod
    def now():
        return datetime.now(tz=timezone.utc)

    def _convert_timeout(self, timeout: Union[int, timedelta]) -> datetime:

        if isinstance(timeout, timedelta):
            return self.now() + timeout

        if isinstance(timeout, (float, int)):
            return self.now() + timedelta(seconds=timeout)

        raise TypeError(f"timeout must be a float or a timedelta, got {type(timeout)}")

    def __init__(self, loop: asyncio.LifoQueue, timeout: Union[int, timedelta]):
        self.expires_at = self._convert_timeout(timeout)
        self.is_out = False

    async def __aenter__(self):
        self.started_at = self.now()
        return self

    async def __aexit__(self, *args, **kwargs):
        print("out")
        self.is_out = True


async def main():

    async with Timeout(3):
        print("execute")

    print("done")


asyncio.run(main())