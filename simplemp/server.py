from asyncio import CancelledError, get_event_loop
from logging import getLogger
from typing import Optional

from .connections import BaseConnection, BaseServer, Disconnected, create_server
from .messages import (TYPE_REQUEST, TYPE_RESPONSE, TYPE_REGISTRATION,
                       TYPE_UNREGISTRATION, TYPE_PUBLICATION, TYPE_SUBSCRIPTION,
                       TYPE_UNSUBSCRIPTION, get_message_topic, get_message_type)
from .serverbrokers import RequestResponse, PublishSubscribe

__all__ = ['serve']


class Server:
    def __init__(self, loop=None):
        self._loop = get_event_loop() if loop is None else loop
        self._log = getLogger(__name__)
        self._request_response = RequestResponse(self._broadcast)
        self._publish_subscribe = PublishSubscribe()
        self._handlers = {
            TYPE_REQUEST: self._request_response.handle_request,
            TYPE_RESPONSE: self._request_response.handle_response,
            TYPE_REGISTRATION: self._request_response.handle_register,
            TYPE_UNREGISTRATION: self._request_response.handle_unregister,
            TYPE_PUBLICATION: self._publish_subscribe.handle_publish,
            TYPE_SUBSCRIPTION: self._publish_subscribe.handle_subscribe,
            TYPE_UNSUBSCRIPTION: self._publish_subscribe.handle_unsubscribe,
        }
        self._server: Optional[BaseServer] = None

    async def shutdown(self):
        if self._server is None:
            self._log.debug('shutting down server')
            await self._server.shutdown()

    async def _broadcast(self, message):
        if self._server is not None:
            await self._server.broadcast(message)

    async def _on_client_connect(self, connection: BaseConnection):
        self._log.info('client %s connected', connection.remote_address)
        while True:
            try:
                message = await connection.recv()
            except Disconnected:
                self._log.info('client %s disconnected',
                               connection.remote_address)
                break
            except CancelledError:
                break
            except Exception as e:
                self._log.exception('unexpected exception: %s', e)
                break
            else:
                self._loop.create_task(self._on_message(message, connection))
        self._on_disconnect(connection)

    async def _on_message(self, message, connection: BaseConnection):
        msg_type = get_message_type(message)
        msg_topic = get_message_topic(message)
        self._log.debug("received %s '%s' message from %s",
                        msg_type, msg_topic, connection.remote_address)
        handler = self._handlers.get(msg_type)
        if handler is not None:
            await handler(message, connection)
        else:
            self._log.warning(f"received message with unsupported type "
                              f"{msg_type}")

    def _on_disconnect(self, connection: BaseConnection):
        self._request_response.handle_disconnect(connection)
        self._publish_subscribe.handle_disconnect(connection)

    def _set_server(self, server):
        self._server = server


async def serve(url, loop=None):
    loop = get_event_loop() if loop is None else loop

    message_server = Server(loop=loop)

    connection_server = await create_server(
        url, on_client_connect=message_server._on_client_connect, loop=loop
    )

    message_server._set_server(connection_server)

    return message_server
