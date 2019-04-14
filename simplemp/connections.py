from asyncio import (CancelledError, IncompleteReadError, StreamReader,
                     StreamWriter, Task, get_event_loop, open_connection,
                     start_server, wait, gather)
from json import dumps, loads
from logging import getLogger
from struct import Struct
from typing import Dict, Optional, Set, Type

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

    async def broadcast(self, message):
        raise NotImplementedError

    async def shutdown(self):
        raise NotImplementedError

    @classmethod
    async def create(cls, url, on_client_connect=None, loop=None, **kwargs):
        raise NotImplementedError


class TcpConnection(BaseConnection):
    url_prefix = 'tcp://'
    separator = ':'
    msg_prefix = Struct('!I')

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
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
        if self._wr is None:
            return None
        return self._wr.get_extra_info('peername')

    def assign_streams(self, reader, writer):
        self._rd = reader
        self._wr = writer

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
        err_msg = (f'connection url must be of the form '
                   f'{cls.url_prefix}host{cls.separator}port')
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
        self.clients: Set[BaseConnection] = set()

    async def broadcast(self, message):
        broadcast_tasks = set()
        for client in self.clients:
            broadcast_tasks.add(self.loop.create_task(
                client.send(message)
            ))

        await gather(*broadcast_tasks, return_exceptions=True)

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


supported_client_protocols: Dict[str, Type[BaseConnection]] = {
    cls.url_prefix: cls
    for cls in BaseConnection.__subclasses__() if cls.url_prefix is not None
}


supported_server_protocols: Dict[str, Type[BaseServer]] = {
    cls.url_prefix: cls
    for cls in BaseServer.__subclasses__() if cls.url_prefix is not None
}


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

