from asyncio import get_event_loop, CancelledError
from logging import getLogger

from .brokers import PublishSubscribeBroker, RequestResponseBroker
from .connections import BaseConnection, Disconnected, create_client_connection
from .messages import (TYPE_PUBLICATION, TYPE_REQUEST, TYPE_RESPONSE,
                       get_message_topic, get_message_type)


__all__ = [
    'connect',
]


class Client:
    def __init__(self, connection: BaseConnection, on_disconnect=None,
                 loop=None):
        self._loop = get_event_loop() if loop is None else loop
        self._log = getLogger(__name__)
        self._reqresp = RequestResponseBroker(connection.send, loop=loop)
        self._pubsub = PublishSubscribeBroker(connection.send, loop=loop)
        self._conn = connection
        self._on_disconnect = on_disconnect
        self._handlers = {
            TYPE_PUBLICATION: self._pubsub.handle_message,
            TYPE_REQUEST: self._reqresp.handle_message,
            TYPE_RESPONSE: self._reqresp.handle_message,
        }
        self._listen_task = self._loop.create_task(self._listen_to_connection())

    async def disconnect(self):
        if (self._listen_task is not None and not self._listen_task.done()
                and not self._listen_task.cancelled()):
            self._listen_task.cancel()
            await self._listen_task
        self._log.debug('disconnecting from server')
        await self._conn.close()
        self._log.debug('disconnected from server')

    async def register(self, topic, handler):
        self._log.debug('sending registration for %s', topic)
        await self._reqresp.register(topic, handler)

    async def unregister(self, topic):
        self._log.debug('cancelling registration for %s', topic)
        await self._reqresp.unregister(topic)

    async def request(self, topic, handler, content=None):
        self._log.debug('requesting %s', topic)
        await self._reqresp.request(topic, handler, content)

    async def publish(self, topic, content=None):
        self._log.debug('publishing %s', topic)
        await self._pubsub.publish(topic, content)

    async def subscribe(self, topic, handler):
        self._log.debug('subscribing to %s', topic)
        await self._pubsub.subscribe(topic, handler)

    async def unsubscribe(self, topic):
        self._log.debug('cancelling subscription for %s', topic)
        await self._pubsub.unsubscribe(topic)

    async def _listen_to_connection(self):
        self._log.debug('client listener spawned')
        while True:
            try:
                message = await self._conn.recv()
            except Disconnected:
                self._log.debug('received disconnect from server')
                if self._on_disconnect is not None:
                    self._on_disconnect()
                break
            except CancelledError:
                self._log.debug('listener cancelled')
                break
            except Exception as e:
                self._log.exception('unexpected exception: %s', e)
                break
            else:
                msg_type = get_message_type(message)
                msg_topic = get_message_topic(message)
                self._log.debug('%s message received for %s',
                                msg_type, msg_topic)
                if msg_type in self._handlers:
                    self._loop.create_task(self._handlers[msg_type](message))


async def connect(url: str, on_disconnect=None, loop=None, **kwargs) -> Client:
    loop = get_event_loop() if loop is None else loop

    connection = await create_client_connection(
        url, loop=loop, **kwargs
    )

    client = Client(connection, on_disconnect=on_disconnect, loop=loop)

    return client
