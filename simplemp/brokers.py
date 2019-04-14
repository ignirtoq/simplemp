from asyncio import ensure_future, get_event_loop
from asyncio.coroutines import iscoroutinefunction
from collections import defaultdict
from logging import getLogger

from .messages import (TYPE_PUBLICATION, TYPE_REGISTRATIONS,
                       TYPE_REGISTRATIONS_UPDATE, TYPE_REQUEST, TYPE_RESPONSE,
                       create_publish_message, create_register_message,
                       create_response_message, create_request_message,
                       create_subscribe_message, create_unregister_message,
                       create_unsubscribe_message, get_new_sequence,
                       unpack_message)


class NoTopicRegistrations(Exception):
    """Raised on requesting a topic for which no responders have registered."""


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
            if content is None:
                return await call_as_coroutine(handler, topic)
            else:
                return await call_as_coroutine(handler, topic, content)
        except Exception as e:
            self._log.exception('caught unexpected exception in handler: %s', e)


class RequestResponseBroker(BrokerBase):
    def __init__(self, send, loop=None):
        super().__init__(send, loop=loop)
        self._pending_requests = {}
        self._handled_topics = defaultdict(list)
        self._message_route = {
            TYPE_REGISTRATIONS: self._handle_registrations,
            TYPE_REGISTRATIONS_UPDATE: self._handle_registrations_update,
            TYPE_REQUEST: self._handle_request,
            TYPE_RESPONSE: self._handle_response,
        }
        self._responder_counts = {}
        self._registrations_received = loop.create_future()

    async def register(self, topic, handler):
        send_msg = topic not in self._handled_topics
        self._handled_topics[topic].append(handler)
        if send_msg:
            await self._send(create_register_message(topic))

    async def unregister(self, topic):
        await self._send(create_unregister_message(topic))
        self._handled_topics.pop(topic)

    async def request(self, topic, handler, content=None):
        if not self._registrations_received.done():
            self._log.debug('server registrations not yet received; awaiting')
            await self._registrations_received
        num_responders = self._responder_counts.get(topic, 0)
        if num_responders < 1:
            raise NoTopicRegistrations
        sequence = get_new_sequence()
        while sequence in self._pending_requests:
            sequence = get_new_sequence()
        self._pending_requests[sequence] = handler
        self._log.debug("sending '%s' request", topic)
        await self._send(create_request_message(topic, sequence=sequence,
                                                content=content))

    async def _handle_request(self, topic, sequence, content=None):
        if topic in self._handled_topics:
            for handler in self._handled_topics[topic]:
                response = await self._schedule_handler(handler, topic, content)
                await self._send(create_response_message(topic, sequence,
                                                         content=response))

    async def _handle_response(self, topic, sequence, content=None):
        handler = self._pending_requests.pop(sequence, None)
        if handler is not None:
            self._schedule_handler(handler, topic, content)

    async def _handle_registrations(self, _, __, content=None):
        self._responder_counts = content
        self._log.debug('responder registrations: %s', self._responder_counts)
        if not self._registrations_received.done():
            self._log.debug('acknowledging registrations received')
            self._registrations_received.set_result(None)

    async def _handle_registrations_update(self, topic, _, content=None):
        if topic:
            self._responder_counts[topic] = content
        else:
            self._responder_counts.update(content)
        self._log.debug('responder registrations: %s', self._responder_counts)

    async def handle_message(self, message):
        type, topic, sequence, content = unpack_message(message)
        if type in self._message_route:
            handler = self._message_route[type]
            await handler(topic, sequence, content=content)


class PublishSubscribeBroker(BrokerBase):
    def __init__(self, send, loop=None):
        super().__init__(send, loop=loop)
        self._handled_messages = defaultdict(list)
        self._message_route = {
            TYPE_PUBLICATION: self._handle_publication,
        }

    async def publish(self, topic, content=None):
        await self._send(create_publish_message(topic, content=content))

    async def subscribe(self, topic, handler):
        send_message = topic not in self._handled_messages
        self._handled_messages[topic].append(handler)
        if send_message:
            await self._send(create_subscribe_message(topic))

    async def unsubscribe(self, topic):
        if topic in self._handled_messages:
            await self._send(create_unsubscribe_message(topic))
            self._handled_messages.pop(topic)

    async def _handle_publication(self, topic, content=None):
        if topic in self._handled_messages:
            for handler in self._handled_messages[topic]:
                self._schedule_handler(handler, topic, content=content)

    async def handle_message(self, message):
        type, topic, sequence, content = unpack_message(message)
        if type in self._message_route:
            handler = self._message_route[type]
            await handler(topic, content=content)
