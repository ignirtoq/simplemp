from asyncio import Queue, get_event_loop


class MessageQueue:
    def __init__(self, *, loop=None):
        self._loop = get_event_loop() if loop is None else loop
        self._queue = Queue(loop=self._loop)
        self._done = object()

    async def stop(self):
        await self._queue.put(self._done)

    def stop_nowait(self):
        self._queue.put_nowait(self._done)

    async def put(self, item):
        await self._queue.put(item)

    def __aiter__(self):
        return self

    async def __anext__(self):
        message = await self._queue.get()
        if message is self._done:
            raise StopAsyncIteration
        return message
