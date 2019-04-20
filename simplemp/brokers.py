from asyncio import ensure_future, get_event_loop
from asyncio.coroutines import iscoroutinefunction
from collections import defaultdict
from logging import getLogger
from typing import Dict, List, Callable, Optional, Coroutine, Any

from .messagequeue import MessageQueue
from .messages import (TYPE_PUBLICATION, TYPE_REQUEST, TYPE_REQUEST_COMPLETE,
                       TYPE_RESPONSE,
                       create_publish_message, create_register_message,
                       create_response_message, create_request_message,
                       create_subscribe_message, create_unregister_message,
                       create_unsubscribe_message, get_new_sequence,
                       unpack_message)


class AlreadyRegistered(Exception):
    """Raised when a topic has already been registered."""


async def call_as_coroutine(func, *args, **kwargs):
    """
    Function call and coroutine await abstraction.
    """
    if iscoroutinefunction(func):
        return await func(*args, **kwargs)
    else:
        return func(*args, **kwargs)


class BrokerBase:
    def __init__(self, send, loop=None):
        self._loop = get_event_loop() if loop is None else loop
        self._send = send
        self._log = getLogger(__name__)

    def handle_message(self, message):
        pass

    def _schedule_handler(self, handler, topic, content):
        return ensure_future(self._handler_task(handler, topic, content),
                             loop=self._loop)

    async def _handler_task(self, handler, topic, content):
        """
        Call a handler with the content argument only if it is non-None.
        """
        try:
            return await call_as_coroutine(handler, topic, content)
        except Exception as e:
            self._log.exception('caught unexpected exception in handler: %s', e)


class RequestResponseBroker(BrokerBase):
    def __init__(self, send, loop=None):
        super().__init__(send, loop=loop)
        self._request_queues: Dict[str, MessageQueue] = {}
        self._registered_handlers: Dict[str, Callable[[str, Optional[Any]],
                                                      Coroutine]] = {}
        self._message_route = {
            TYPE_REQUEST: self._handle_request,
            TYPE_REQUEST_COMPLETE: self._handle_request_complete,
            TYPE_RESPONSE: self._handle_response,
        }

    async def register(self, topic, handler):
        if topic in self._registered_handlers:
            raise AlreadyRegistered
        self._registered_handlers[topic] = handler
        await self._send(create_register_message(topic))

    async def unregister(self, topic):
        await self._send(create_unregister_message(topic))
        self._registered_handlers.pop(topic)

    async def request(self, topic, content=None):
        sequence = get_new_sequence()
        while sequence in self._request_queues:
            sequence = get_new_sequence()
        queue = MessageQueue(loop=self._loop)
        self._request_queues[sequence] = queue
        self._log.debug("sending '%s' request", topic)
        await self._send(create_request_message(topic, sequence=sequence,
                                                content=content))
        return queue

    def on_disconnect(self):
        for queue in self._request_queues.values():
            queue.stop_nowait()

    async def _handle_request(self, topic, sequence, content=None):
        handler = self._registered_handlers.get(topic)
        if handler is not None:
            response = await self._schedule_handler(handler, topic, content)
            await self._send(create_response_message(topic, sequence,
                                                     content=response))

    async def _handle_response(self, _, sequence, content=None):
        queue = self._request_queues.get(sequence, None)
        if queue is not None:
            await queue.put(content)

    async def _handle_request_complete(self, _, sequence, content=None):
        queue = self._request_queues.pop(sequence, None)
        if queue is not None:
            self._log.debug('request %s complete', sequence)
            await queue.stop()

    async def handle_message(self, message):
        type, topic, sequence, content = unpack_message(message)
        if type in self._message_route:
            handler = self._message_route[type]
            await handler(topic, sequence, content=content)
        else:
            self._log.warning("received message of unknown type '%s'",
                              type)


class PublishSubscribeBroker(BrokerBase):
    def __init__(self, send, loop=None):
        super().__init__(send, loop=loop)
        self._subscriptions: Dict[str, List[MessageQueue]] = defaultdict(list)
        self._message_route = {
            TYPE_PUBLICATION: self._handle_publication,
        }

    async def publish(self, topic, content=None):
        await self._send(create_publish_message(topic, content=content))

    async def subscribe(self, topic):
        send_message = topic not in self._subscriptions
        new_queue = MessageQueue(loop=self._loop)
        self._subscriptions[topic].append(new_queue)
        if send_message:
            await self._send(create_subscribe_message(topic))
        return new_queue

    async def unsubscribe(self, topic):
        if topic in self._subscriptions:
            await self._send(create_unsubscribe_message(topic))
            queues = self._subscriptions.pop(topic)
            for queue in queues:
                await queue.stop()

    def on_disconnect(self):
        for subscription in self._subscriptions.values():
            for queue in subscription:
                queue.stop_nowait()
        self._subscriptions = {}

    async def _handle_publication(self, topic, content=None):
        if topic in self._subscriptions:
            for queue in self._subscriptions[topic]:
                await queue.put(content)

    async def handle_message(self, message):
        type, topic, sequence, content = unpack_message(message)
        if type in self._message_route:
            handler = self._message_route[type]
            await handler(topic, content=content)
