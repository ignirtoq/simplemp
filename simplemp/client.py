from asyncio import get_event_loop, AbstractEventLoop, CancelledError
from logging import getLogger
from typing import Any, Callable, Coroutine

from .brokers import PublishSubscribeBroker, RequestResponseBroker
from .connections import BaseConnection, Disconnected, create_client_connection
from .messagequeue import MessageQueue
from .messages import (TYPE_PUBLICATION, TYPE_REQUEST, TYPE_RESPONSE,
                       TYPE_REQUEST_COMPLETE, get_message_topic,
                       get_message_type)


__all__ = [
    'connect',
    'Connection',
]


class Client:
    def __init__(self,
                 connection: BaseConnection,
                 on_disconnect: Callable[[], Any]=None,
                 loop: AbstractEventLoop=None
                 ):
        self._loop = get_event_loop() if loop is None else loop
        self._log = getLogger(__name__)
        self._reqresp = RequestResponseBroker(connection.send, loop=loop)
        self._pubsub = PublishSubscribeBroker(connection.send, loop=loop)
        self._conn = connection
        self._disconnect_cb = on_disconnect
        self._handlers = {
            TYPE_PUBLICATION: self._pubsub.handle_message,
            TYPE_REQUEST: self._reqresp.handle_message,
            TYPE_REQUEST_COMPLETE: self._reqresp.handle_message,
            TYPE_RESPONSE: self._reqresp.handle_message,
        }
        self._listen_task = self._loop.create_task(self._listen_to_connection())

    async def disconnect(self) -> None:
        """
        *This function is a coroutine.*

        Disconnect from the server and terminate all open message iterators.

        Returns
        -------
        None
        """
        if (self._listen_task is not None and not self._listen_task.done()
                and not self._listen_task.cancelled()):
            self._listen_task.cancel()
            await self._listen_task
        self._log.debug('disconnecting from server')
        await self._conn.close()
        self._log.debug('disconnected from server')

    async def register(self,
                       topic: str,
                       handler: Callable[[str, Any], Coroutine]
                       ) -> None:
        """
        *This function is a coroutine.*

        Register with the server as a responder to the specified topic.

        Parameters
        ----------
        topic : str
            Type of message to respond to.  Used by the message bus to identify
            which clients to forward the request to.
        handler : Callable[str, Any]
            Function or coroutine to be called when receiving a request.

        Returns
        -------
        None

        Notes
        -----
        The handler will be called with two positional arguments:
        the request topic and the request content.  If no content is provided,
        None will be passed as the second argument.

        If the handler is not a coroutine, it will be wrapped in a coroutine.
        The handler coroutine will then be scheduled in its own task, so it
        should handle all error conditions as appropriate.

        Only one handler can be registered per topic.  To replace the handler,
        the topic must first be unregistered.

        The return value of the handler will be used as the payload of the
        response message.  A return value of None will result in a response
        message with no payload.
        """
        self._log.debug('sending registration for %s', topic)
        return await self._reqresp.register(topic, handler)

    async def unregister(self,
                         topic: str
                         ) -> None:
        """
        *This function is a coroutine.*

        Unregister for the topic with the message bus server.

        Parameters
        ----------
        topic : str
            Type of message to no longer handle.

        Returns
        -------
        None
        """
        self._log.debug('cancelling registration for %s', topic)
        return await self._reqresp.unregister(topic)

    async def request(self,
                      topic: str,
                      content: Any=None
                      ) -> MessageQueue:
        """
        *This function is a coroutine.*

        Send a request for the specified topic to the message bus.

        Parameters
        ----------
        topic : str
            Type of message to request.
        content : Any, optional
            Optional payload to send with the topic.  This can be any
            JSON-serializable object.

        Returns
        -------
        asynchronous iterator
            Response message queue in the form of an asynchronous iterator.

        Notes
        -----
        The returned iterator will provide the payload to zero or more response
        messages until the server indicates that all responders have responded
        to the request.

        If a response message does not contain a payload, the value of None
        will be provided by the iterator.  If no responders are registered
        for the topic requested, the iterator will stop before providing any
        payloads.

        Published messages of the same topic will not be output from
        request-response iterators.
        """
        self._log.debug('requesting %s', topic)
        return await self._reqresp.request(topic, content)

    async def publish(self,
                      topic: str,
                      content: Any=None
                      ) -> None:
        """
        *This function is a coroutine.*

        Publish a message of the specified type.

        Parameters
        ----------
        topic : str
            Type of the message to be published.
        content : Any, optional
            Payload to attach with the message.

        Returns
        -------
        None
        """
        self._log.debug('publishing %s', topic)
        return await self._pubsub.publish(topic, content)

    async def subscribe(self,
                        topic: str
                        ) -> MessageQueue:
        """
        *This function is a coroutine.*

        Subscribe to messages of the specified type.

        Parameters
        ----------
        topic : str
            Type of message to receive.

        Returns
        -------
        asynchronous iterator
            Subscription queue in the form of an asynchronous iterator.

        Notes
        -----
        The returned iterator will provide the payload to all published
        messages of the specified topic until the topic is unsubscribed or
        the connection is closed.

        If a published message does not contain a payload, the value of None
        will be provided by the iterator.

        Response messages of the same topic will not be output by
        subscription iterators.
        """
        self._log.debug('subscribing to %s', topic)
        return await self._pubsub.subscribe(topic)

    async def unsubscribe(self,
                          topic: str
                          ) -> None:
        """
        *This function is a coroutine.*

        Unsubscribe from messages of the specified type.

        Parameters
        ----------
        topic : str
            Type of message to stop receiving.

        Returns
        -------
        None.

        Notes
        -----
        A message is sent to the server to unsubscribe from the specified
        topic, and all subscription iterators are terminated.
        """
        self._log.debug('cancelling subscription for %s', topic)
        return await self._pubsub.unsubscribe(topic)

    def _on_disconnect(self):
        self._pubsub.on_disconnect()
        self._reqresp.on_disconnect()
        if self._disconnect_cb is not None:
            self._log.debug('disconnect callback called')
            try:
                self._disconnect_cb()
            except Exception as e:
                self._log.exception('caught unexpected exception from '
                                    'disconnect callback: %s', e)

    async def _listen_to_connection(self):
        self._log.debug('client listener spawned')
        while True:
            try:
                message = await self._conn.recv()
            except Disconnected:
                self._log.debug('received disconnect from server')
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
                self._log.debug("'%s' type message received for topic '%s'",
                                msg_type, msg_topic)
                if msg_type in self._handlers:
                    self._loop.create_task(self._handlers[msg_type](message))
                else:
                    self._log.warning('unknown message type received: %s',
                                      msg_type)

        self._on_disconnect()
        self._log.debug('client listener exiting')


async def connect(
        url: str,
        on_disconnect: Callable[[], Any]=None,
        loop: AbstractEventLoop=None,
        **kwargs
) -> Client:
    """
    *This function is a coroutine.*

    Connect to the message bus and create an interface.

    Parameters
    ----------
    url : string
        Client connection URL for the message bus server.
    on_disconnect : callable, optional
        Function to be executed upon disconnect from the server.
        Will be called with no arguments under all disconnect cases, including
        server-initiated and client-initiated.  Will *not* be awaited, so this
        should not be a coroutine function.
    loop : event loop, optional
        Execution loop.
    kwargs : additional connection arguments, optional
        Any additional keyword arguments will be passed to the underlying
        socket interface class.  A basic TCP connection will ignore any
        arguments, while other protocols, suck as Web Sockets, may accept their
        own.

    Returns
    -------
    interface : Client
        Combined request-response, publish-subscribe interface.
        See help for the Client for more information.
    """
    loop = get_event_loop() if loop is None else loop

    connection = await create_client_connection(
        url, loop=loop, **kwargs
    )

    client = Client(connection, on_disconnect=on_disconnect, loop=loop)

    return client


class Connection:
    """
    Asynchronous context manager for a simplemp client connection.

    See documentation for connect() for arguments.
    """
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs
        self._client: Client

    async def __aenter__(self):
        self._client = await connect(*self._args, **self._kwargs)
        return self._client

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._client.disconnect()
