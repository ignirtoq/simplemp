from asyncio import (CancelledError, IncompleteReadError, StreamReader,
                     StreamWriter, Task, get_event_loop, open_connection,
                     start_server, wait, gather)
from json import dumps, loads
from logging import getLogger
from struct import Struct
from typing import Dict, Optional, Set, Type

try:
    import websockets
except ImportError:
    HAVE_WEBSOCKETS = False
else:
    HAVE_WEBSOCKETS = True

shutdown_timeout = 3.0


class Disconnected(Exception):
    """Raised when the connection is closed."""


class BaseConnection:
    url_prefix = None

    def __init__(self, *args, loop=None, **kwargs):
        self.loop = get_event_loop() if loop is None else loop
        self.log = getLogger(__name__)

    async def recv(self):
        raise NotImplementedError

    async def send(self, message):
        raise NotImplementedError

    async def close(self):
        raise NotImplementedError

    @property
    def remote_address(self):
        raise NotImplementedError

    @classmethod
    async def create(cls, url, loop=None, **kwargs):
        raise NotImplementedError


class BaseServer:
    url_prefix = None

    def __init__(self, on_client_connect=None, loop=None, **kwargs):
        self.loop = get_event_loop() if loop is None else loop
        self.log = getLogger(__name__)
        self.on_client_connect = on_client_connect
        self.clients: Set[BaseConnection] = set()

    async def shutdown(self):
        raise NotImplementedError

    @classmethod
    async def create(cls, url, on_client_connect=None, loop=None, **kwargs):
        raise NotImplementedError

    async def broadcast(self, message):
        broadcast_tasks = set()
        for client in self.clients:
            broadcast_tasks.add(self.loop.create_task(
                client.send(message)
            ))

        await gather(*broadcast_tasks, return_exceptions=True)


class TcpConnection(BaseConnection):
    url_prefix = 'tcp://'
    separator = ':'
    msg_prefix = Struct('!I')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._remote_address = None
        self._rd: Optional[StreamReader] = None
        self._wr: Optional[StreamWriter] = None

    async def recv(self):
        try:
            msg_len_bytes = await self._rd.readexactly(self.msg_prefix.size)
            msg_len, = self.msg_prefix.unpack(msg_len_bytes)
            msg_bytes = await self._rd.readexactly(msg_len)
        except IncompleteReadError:
            raise Disconnected
        else:
            return loads(msg_bytes.decode('utf-8'))

    async def send(self, message):
        msg_bytes = dumps(message).encode('utf-8')
        msg_len_bytes = self.msg_prefix.pack(len(msg_bytes))
        self._wr.write(msg_len_bytes)
        self._wr.write(msg_bytes)
        await self._wr.drain()

    async def close(self):
        self.log.debug('closing connection to server')
        self._wr.write_eof()
        await self._wr.drain()

    @property
    def remote_address(self):
        return self._remote_address

    def assign_streams(self, reader, writer):
        self._rd = reader
        self._wr = writer
        self._remote_address = self._wr.get_extra_info('peername')

    @classmethod
    def create_from_streams(cls, reader, writer, *, loop=None, **kwargs):
        inst = cls(loop=loop, **kwargs)
        inst.assign_streams(reader, writer)
        return inst

    @classmethod
    async def create(cls, url, loop=None, **kwargs):
        host, port = cls.parse_url(url)
        reader, writer = await open_connection(host, port, loop=loop)
        return cls.create_from_streams(reader, writer, loop=loop, **kwargs)

    @classmethod
    def parse_url(cls, url: str):
        err_msg = ('connection url must be of the form '
                   '%shost%sport' % (cls.url_prefix, cls.separator))
        if not url.startswith(cls.url_prefix):
            raise ValueError(err_msg)

        _, origin = url.split(cls.url_prefix, 1)

        parts = origin.split(cls.separator, 1)
        if len(parts) != 2:
            raise ValueError(err_msg)
        host, port = parts
        port, *_ = port.split('/', 1)
        return host, int(port)


class TcpServer(BaseServer):
    url_prefix = TcpConnection.url_prefix

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._server = None
        self.listening_tasks = set()

    async def shutdown(self):
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

        tasks_to_cancel = self.listening_tasks.copy()
        for task in tasks_to_cancel:
            task.cancel()
        if len(tasks_to_cancel):
            await wait(tasks_to_cancel, timeout=shutdown_timeout,
                       loop=self.loop)
        for task in tasks_to_cancel:
            await task

    @classmethod
    async def create(cls, url, on_client_connect=None, loop=None, **kwargs):
        host, port = TcpConnection.parse_url(url)

        inst = cls(on_client_connect=on_client_connect, loop=loop, **kwargs)
        server = await start_server(inst.spawn_handler, host, port,
                                    loop=loop)
        inst._set_server(server)

        return inst

    def spawn_handler(self, reader, writer):
        connection = TcpConnection.create_from_streams(reader, writer)
        self.clients.add(connection)
        self.loop.create_task(self.wrap_handler(connection))

    async def wrap_handler(self, connection):
        self.listening_tasks.add(Task.current_task(self.loop))
        try:
            await self.on_client_connect(connection)
        except CancelledError:
            pass
        except Exception as e:
            self.log.exception('caught unexpected exception: %s', e)
        self.clients.remove(connection)
        await connection.close()
        self.listening_tasks.remove(Task.current_task(self.loop))

    def _set_server(self, server):
        self._server = server


async def create_client_connection(url, *, loop=None, **kwargs):
    log = getLogger(__name__)
    loop = get_event_loop() if loop is None else loop

    connection_class = None
    for prefix in supported_client_protocols:
        if url.startswith(prefix):
            connection_class = supported_client_protocols[prefix]
            break
    if connection_class is None:
        raise ValueError(f"unsupported protocol for URL '{url}'")

    connection = await connection_class.create(url, loop=loop, **kwargs)
    log.debug('established connection with %s', url)
    return connection


async def create_server(url, *, on_client_connect, loop=None, **kwargs):
    log = getLogger(__name__)
    loop = get_event_loop() if loop is None else loop

    connection_class = None
    for prefix in supported_server_protocols:
        if url.startswith(prefix):
            connection_class = supported_server_protocols[prefix]
            break
    if connection_class is None:
        raise ValueError(f"unsupported protocol for URL '{url}'")

    server = await connection_class.create(
        url, loop=loop, on_client_connect=on_client_connect, **kwargs
    )
    log.debug('server created at %s', url)
    return server


if HAVE_WEBSOCKETS:
    class WebSocketConnection(BaseConnection):
        url_prefix = ['ws://', 'wss://']

        def __init__(self, connection, *, loop=None):
            super().__init__(loop=loop)
            self.ws = connection
            self._remote_address = connection.remote_address

        async def send(self, message):
            msg_str = dumps(message)
            await self.ws.send(msg_str)

        async def recv(self):
            try:
                msg_str = await self.ws.recv()
            except websockets.exceptions.ConnectionClosed as e:
                raise Disconnected from e
            return loads(msg_str)

        async def close(self):
            await self.ws.close()

        @property
        def remote_address(self):
            return self._remote_address

        @classmethod
        async def create(cls, url, loop=None, **kwargs):
            loop = get_event_loop() if loop is None else loop
            connection = await websockets.connect(url, **kwargs)
            return cls(connection, loop=loop)


    class WebSocketServer(BaseServer):
        url_prefix = WebSocketConnection.url_prefix
        separator = ':'

        def __init__(self, on_client_connect=None, *, loop=None, **kwargs):
            super().__init__(on_client_connect, loop=loop, **kwargs)
            self.listening_tasks = set()
            self.server = None

        async def shutdown(self):
            self.server.close()
            await self.server.wait_closed()

        @classmethod
        async def create(cls, url, on_client_connect=None, loop=None, **kwargs):
            loop = get_event_loop() if loop is None else loop
            inst = cls(on_client_connect, loop=loop, **kwargs)
            host, port = cls.parse_url(url)

            ws_server = await websockets.serve(
                inst.wrap_handler, host, port, loop=loop, **kwargs
            )
            inst._set_server(ws_server)
            return inst

        @classmethod
        def parse_url(cls, url: str):
            forms = ['%shost%sport' % (p, cls.separator)
                     for p in cls.url_prefix]
            err_msg = ('server url must be one of the forms: '
                       ', '.join(forms))

            prefix = None
            for p in cls.url_prefix:
                if url.startswith(p):
                    prefix = p
                    break

            if prefix is None:
                raise ValueError(err_msg)

            _, origin = url.split(prefix, 1)

            parts = origin.split(cls.separator, 1)
            if len(parts) != 2:
                raise ValueError(err_msg)
            host, port = parts
            port, *_ = port.split('/', 1)
            return host, int(port)

        async def wrap_handler(self, connection, _):
            self.listening_tasks.add(Task.current_task(self.loop))
            client = WebSocketConnection(connection)
            self.clients.add(client)
            try:
                await self.on_client_connect(client)
            except CancelledError:
                pass
            except Exception as e:
                self.log.exception('caught unexpected exception: %s', e)
            self.clients.remove(client)
            await client.close()
            self.listening_tasks.remove(Task.current_task(self.loop))

        def _set_server(self, server):
            self.server = server


supported_client_protocols = {}
for cls in BaseConnection.__subclasses__():
    prefix = cls.url_prefix
    if prefix is None:
        continue
    if not isinstance(prefix, list):
        prefix = [prefix]
    for p in prefix:
        supported_client_protocols[p] = cls


supported_server_protocols = {}
for cls in BaseServer.__subclasses__():
    prefix = cls.url_prefix
    if prefix is None:
        continue
    if not isinstance(prefix, list):
        prefix = [prefix]
    for p in prefix:
        supported_server_protocols[p] = cls
