# Copyright 2018 Palantir Technologies, Inc.
import asyncio
import logging
import socket

try:
    import ujson as json
except Exception:  # pylint: disable=broad-except
    import json

from ..streams import JsonRpcStreamReader as JsonRpcStreamReaderNonAsync


# Note: if using a messaging library, you don't need anything in this module.
# Just pass the deserialized message to the consumer directly.


log = logging.getLogger(__name__)


class WritePipeProto(asyncio.streams.FlowControlMixin):
    def __init__(self, reader, loop):
        super().__init__(loop=loop)
        self.loop = loop
        self._stream_reader = reader
        self.pipe = None

    def connection_made(self, transport):
        self.pipe = transport


class ReadPipeProto(WritePipeProto):
    def data_received(self, data):
        self._stream_reader.feed_data(data)


class JsonRpcStreamReader(asyncio.streams.StreamReader):
    def __init__(self, limit, loop):
        super().__init__(limit, loop)

    def close(self):
        self._transport.close()

    # FIXME (maybe) the original _variant in ..streams is only called by
    # .listen() and so does not need to exhibit any symmetry with Writer.write;
    # but this one serves double duty: manual use and via serve_forever; so it
    # probably *should* return a deserialized message by default
    async def read_message(self):
        """Reads the contents of a message.

        Returns:
            body of message if parsable else None
        """
        line = await self.readline()

        if not line:
            return None

        content_length = self._content_length(line)

        # Blindly consume all header lines
        while line and line.strip():
            line = await self.readline()

        if not line:
            return None

        # Grab the body
        return await self.read(content_length or 0)

    _content_length = staticmethod(JsonRpcStreamReaderNonAsync._content_length)


class JsonRpcStreamWriter(asyncio.streams.StreamWriter):
    def __init__(self, write_transport, protocol, reader, loop, **json_dumps_args):
        super().__init__(write_transport, protocol, reader, loop)
        self._wfile_lock = asyncio.Lock()
        self._json_dumps_args = json_dumps_args

    def close(self):
        with self._wfile_lock:
            super().close()  # Close stderr?

    @classmethod
    def from_stream_writer(cls, writer, **kwargs):
        kwargs.update(
            protocol=writer._protocol,
            reader=writer._reader,
            loop=writer._loop
        )
        return cls(writer.transport, **kwargs)

    async def write_message(self, message):
        async with self._wfile_lock:
            if self._transport.is_closing():  # self.is_closing >= 3.7
                return
            # Below is nearly identical to non-async; too bad can't encapsulate
            try:
                body = json.dumps(message, **self._json_dumps_args)

                # Ensure we get the byte length, not the character length
                content_length = (
                    len(body) if isinstance(body, bytes) else len(body.encode("utf-8"))
                )

                response = (
                    "Content-Length: {}\r\n"
                    "Content-Type: application/vscode-jsonrpc; charset=utf8\r\n\r\n"
                    "{}".format(content_length, body)
                )

                self.write(response.encode("utf-8"))
            except Exception:  # pylint: disable=broad-except
                log.exception("Failed to write message to output file %s", message)


LIMIT = asyncio.streams._DEFAULT_LIMIT


async def connect_pipes(rfile, wfile, loop=None):
    if not loop:
        loop = asyncio.get_event_loop()
    reader = JsonRpcStreamReader(limit=LIMIT, loop=loop)
    # Returns asyncio.unix_events._UnixReadPipeTransport as transport on Unix
    read_transport, protocol = await loop.connect_read_pipe(
        lambda: ReadPipeProto(reader, loop), rfile
    )
    write_transport, protocol = await loop.connect_write_pipe(
        lambda: WritePipeProto(reader, loop), wfile
    )
    writer = JsonRpcStreamWriter(write_transport, protocol, reader, loop)
    return reader, writer


async def connect_tcp(host, port, loop=None):
    """Bind and listen to host:port, return server, async iterator

    Use like::

        async def handler(reader, writer):
            msg = json.loads(await reader.read_message())
            await writer.write_message({'id': msg['id'], 'result': 'hw'})

        server, connections = await connect_tcp('127.0.0.1', 8888)
        async with server:  # 3.7+
            async for reader, writer in connections:
                asyncio.create_task(handler(reader, writer))

    """
    if not loop:
        loop = asyncio.get_event_loop()
    reader = JsonRpcStreamReader(limit=LIMIT, loop=loop)

    queue = asyncio.Queue()

    class Ait:  # no yield in async func till 3.6
        def __aiter__(self):
            return self

        async def __anext__(self):
            return await queue.get()

    def connected_cb(_reader, plain_writer):
        assert _reader is reader
        writer = JsonRpcStreamWriter.from_stream_writer(plain_writer)
        queue.put_nowait((_reader, writer))

    protocol = asyncio.streams.StreamReaderProtocol(
        reader, connected_cb, loop=loop
    )
    server = await loop.create_server(
        lambda: protocol, host=host, port=port
    )
    return server, Ait()


async def connect_oneoff(loop=None):
    """Return a server reader/writer pair and connected client socket"""
    if not loop:
        loop = asyncio.get_event_loop()
    client_sock, server_sock = socket.socketpair()
    reader = JsonRpcStreamReader(limit=LIMIT, loop=loop)
    transport, protocol = await loop.connect_accepted_socket(
        lambda: asyncio.streams.StreamReaderProtocol(reader, loop=loop),
        server_sock
    )
    writer = JsonRpcStreamWriter(transport, protocol, reader, loop)
    return (reader, writer), client_sock


async def serve_forever(message_consumer, reader):
    """Blocking call to listen for messages on the rfile.

    Args:
        message_consumer (fn): message handler passed msg as string
        reader: obj with a read_message method returning (awaitable) bytes
    """
    while True:
        try:
            request_str = await reader.read_message()
        except ValueError:
            log.exception("Failed to read from reader")

        if request_str is None:
            return
        try:
            await message_consumer(json.loads(request_str.decode("utf-8")))
        except ValueError:
            # Test depends on this msg format string
            log.exception("Failed to parse JSON message %r", request_str)
            continue
