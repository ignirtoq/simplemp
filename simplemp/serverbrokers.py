from asyncio import get_event_loop
from collections import defaultdict
from functools import partial
from logging import getLogger
from typing import Any, Callable, Optional, Dict, Set

from .connections import BaseConnection
from .messages import (create_request_complete_message,
                       get_message_sequence, get_message_topic)


class PendingRequest:
    def __init__(self, requester: BaseConnection, responders: set,
                 on_complete: Callable, *, loop=None):
        self.loop = get_event_loop() if loop is None else loop
        self.sequence = None
        self.topic = None
        self.requester = requester
        self.responders = responders.copy()
        self.on_complete = on_complete

    async def send_request(self, message):
        self.sequence = get_message_sequence(message)
        self.topic = get_message_topic(message)
        for responder in self.responders:
            await responder.send(message)

    async def send_response(self, message, connection):
        if connection in self.responders:
            await self.requester.send(message)
            self.remove_responder(connection)

    def remove_responder(self, connection):
        self.responders.remove(connection)
        if len(self.responders) == 0:
            complete_msg = create_request_complete_message(self.topic,
                                                           self.sequence)
            self.loop.create_task(self.requester.send(complete_msg))
            self.on_complete()

    def remove_requester(self):
        self.on_complete()


class RequestPool:
    def __init__(self, on_complete: Optional[Callable] = None, *, loop=None):
        self.loop = get_event_loop() if loop is None else loop
        self.log = getLogger(__name__)
        self.requests: Dict[Any, PendingRequest] = dict()
        self.requesters: Dict[BaseConnection, Set[Any]] = defaultdict(set)
        self.responders: Dict[BaseConnection, Set[Any]] = defaultdict(set)
        self._on_complete = on_complete

    async def add_request(self, requester: BaseConnection,
                          message, responders: Set[BaseConnection]):
        sequence = get_message_sequence(message)
        pending_request = PendingRequest(
            requester, responders, partial(self.on_complete, sequence),
            loop=self.loop
        )
        self.requests[sequence] = pending_request
        self.requesters[requester].add(sequence)
        for responder in responders:
            self.responders[responder].add(sequence)
        await pending_request.send_request(message)

    async def send_response(self, responder: BaseConnection, message):
        sequence = get_message_sequence(message)
        self.remove_responder_sequence(responder, sequence)
        self.log.debug('sending response')
        await self.requests[sequence].send_response(message, responder)

    def remove_connection(self, connection: BaseConnection):
        if connection in self.requesters:
            requester_sequences = self.requesters.pop(connection)
            for sequence in requester_sequences:
                if sequence in self.requests:
                    self.requests[sequence].remove_requester()
        if connection in self.responders:
            responder_sequences = self.responders.pop(connection)
            for sequence in responder_sequences:
                if sequence in self.requests:
                    self.requests[sequence].remove_responder(connection)

    def remove_requester_sequence(self, sequence):
        requester = self.requests.pop(sequence).requester
        requester_sequences = self.responders[requester]
        if sequence in requester_sequences:
            requester_sequences.remove(sequence)
        if not len(requester_sequences):
            self.responders.pop(requester)

    def remove_responder_sequence(self, responder: BaseConnection, sequence):
        responder_sequences = self.responders[responder]
        if sequence in responder_sequences:
            responder_sequences.remove(sequence)
        if not len(responder_sequences):
            self.responders.pop(responder)

    def on_complete(self, sequence):
        self.remove_requester_sequence(sequence)


class RegistrationAssociations:
    def __init__(self):
        self.log = getLogger(__name__)
        self.topic_to_responders: Dict[str, Set[BaseConnection]] = (
            defaultdict(set)
        )
        self.responder_to_topics: Dict[BaseConnection, Set[str]] = (
            defaultdict(set)
        )

    def get_topic_responders(self, topic, default=None):
        return self.topic_to_responders.get(topic, default)

    def get_topic_responder_counts(self):
        return {
            t: len(r) for t, r in self.topic_to_responders.items()
        }

    def add_to_topic(self, topic, connection) -> Optional[int]:
        topic_responders = self.topic_to_responders[topic]
        if connection in topic_responders:
            return

        topic_responders.add(connection)
        self.responder_to_topics[connection].add(topic)

    def remove_from_topic(self, topic, connection) -> Optional[int]:
        responder_topics = self.responder_to_topics.get(connection)
        if responder_topics is not None and topic in responder_topics:
            responder_topics.remove(topic)
        topic_responders = self.topic_to_responders.get(topic)
        if topic_responders is None:
            return

        if connection not in topic_responders:
            return

        topic_responders.remove(connection)

    def remove_connection(self, connection) -> None:
        topics = self.responder_to_topics.pop(connection, None)
        if topics is None:
            return

        self.log.debug('%s disconnected and removed from %s',
                       connection.remote_address, topics)
        for topic in topics:
            self.topic_to_responders[topic].remove(connection)
            if not len(self.topic_to_responders[topic]):
                self.topic_to_responders.pop(topic)


class RequestResponse:
    def __init__(self, *, loop=None):
        self.loop = get_event_loop() if loop is None else loop
        self.log = getLogger(__name__)
        self.requests = RequestPool(loop=self.loop)
        self.registrations = RegistrationAssociations()

    async def handle_request(self, message, connection):
        topic = get_message_topic(message)
        responders = self.registrations.get_topic_responders(topic)
        if responders is None:
            self.log.info(("received '%s' request with no "
                           "registered responders") % topic)
            sequence = get_message_sequence(message)
            await connection.send(create_request_complete_message(topic,
                                                                  sequence))
            return

        await self.requests.add_request(connection, message, responders)

    async def handle_response(self, message, connection: BaseConnection):
        await self.requests.send_response(connection, message)

    async def handle_register(self, message, connection: BaseConnection):
        topic = get_message_topic(message)
        self.registrations.add_to_topic(topic, connection)

    async def handle_unregister(self, message, connection: BaseConnection):
        topic = get_message_topic(message)
        self.registrations.remove_from_topic(topic, connection)

    def handle_disconnect(self, connection: BaseConnection):
        self.log.debug('handling %s disconnect', connection.remote_address)
        self.requests.remove_connection(connection)
        self.registrations.remove_connection(connection)


class PublishSubscribe:
    def __init__(self):
        self._log = getLogger(__name__)
        self._subs = defaultdict(set)
        self._conns = defaultdict(set)

    async def handle_publish(self, message, _: BaseConnection):
        topic = get_message_topic(message)
        self._log.info("received '%s' publication", topic)
        for connection in self._subs[topic]:
            await connection.send(message)

    async def handle_subscribe(self, message, connection: BaseConnection):
        topic = get_message_topic(message)
        self._log.info("received '%s' subscription", topic)
        self._subs[topic].add(connection)
        self._conns[connection].add(topic)

    async def handle_unsubscribe(self, message, connection: BaseConnection):
        topic = get_message_topic(message)
        self._log.info("received '%s' unsubscription", topic)

        connection_subscriptions = self._conns[connection]
        if topic in connection_subscriptions:
            connection_subscriptions.remove(topic)
        else:
            self._log.warning("connection not in subscription list for "
                              "'%s'", topic)

        topic_connections = self._subs[topic]
        if connection in topic_connections:
            topic_connections.remove(connection)
        else:
            self._log.warning(f"'{topic}' not in subscription list for "
                              f"connection")

    def handle_disconnect(self, connection: BaseConnection):
        self._log.debug('handling %s disconnect', connection.remote_address)
        if connection in self._conns:
            subscriptions = self._conns.pop(connection)
            self._log.debug('removing %s from subscription list',
                            connection.remote_address)
            for topic in subscriptions:
                self._subs[topic].remove(connection)
