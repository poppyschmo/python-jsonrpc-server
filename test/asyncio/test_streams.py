# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
import asyncio
import socket
import sys
import os
import json
import pytest

pytest.mark.skipif(sys.version_info < (3, 5, 2), reason="Compat")

# pylint: disable=wrong-import-position
from pyls_jsonrpc.asyncio.streams import connect_oneoff, serve_forever  # noqa: E402

from .conftest import get_async_mock, await_assertion, await_condition, kv_chk  # noqa: E402

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def rw_conn():
    return await connect_oneoff()


async def test_reader(rw_conn):
    (reader, __), s = rw_conn
    s.send(
        b"Content-Length: 49\r\n"
        b"Content-Type: application/vscode-jsonrpc; charset=utf8\r\n"
        b"\r\n"
        b'{"id": "hello", "method": "method", "params": {}}'
    )

    got = await reader.read_message()

    assert json.loads(got.decode()) == (
        {"id": "hello", "method": "method", "params": {}}
    )


async def test_reader_bad_message(rw_conn):
    (reader, __), s = rw_conn
    s.send(b"Hello world")

    # Blocks foever till newline or EOF, which is what we want from async, no?
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(reader.read_message(), timeout=0.001)

    assert reader._buffer == b"Hello world"  # data fed


async def test_serve_forever_bad_json(rw_conn, caplog):
    caplog.set_level("INFO")
    (reader, __), s = rw_conn
    data = (
        b"Content-Length: 8\r\n"
        b"Content-Type: application/vscode-jsonrpc; charset=utf8\r\n"
        b"\r\n"
        b"{hello}}"
    )
    s.send(data)

    consumer = get_async_mock()
    loop = asyncio.get_event_loop()
    task = loop.create_task(serve_forever(consumer, reader))

    def find():
        val = caplog.handler.stream.getvalue().lower()
        assert "json" in val
        assert "{hello}}" in val

    await await_assertion(find)
    task.cancel()
    assert reader._buffer == b""


async def test_writer(rw_conn):
    (__, writer), s = rw_conn
    msg = {"id": "hello", "method": "method", "params": {}}
    await writer.write_message(msg)

    if os.name == "nt":
        assert s.recv(4096) == (
            b"Content-Length: 49\r\n"
            b"Content-Type: application/vscode-jsonrpc; charset=utf8\r\n"
            b"\r\n"
            b'{"id": "hello", "method": "method", "params": {}}'
        )
    else:
        await await_condition(lambda: s.recv(1, socket.MSG_PEEK))
        expected = s.recv(4096)
        assert expected.startswith(
            b"Content-Length: 44\r\n"
            b"Content-Type: application/vscode-jsonrpc; charset=utf8\r\n"
            b"\r\n"
        )
        assert kv_chk(msg, expected.splitlines()[-1])


async def test_writer_bad_message(rw_conn):
    # A datetime isn't serializable(or poorly serializable),
    # ensure the write method doesn't throw
    import datetime
    (__, writer), s = rw_conn

    await writer.write_message(
        datetime.datetime(year=2019, month=1, day=1, hour=1, minute=1, second=1,)
    )

    if os.name == "nt":
        assert s.recv(4096) == b""
    else:
        await await_condition(lambda: s.recv(1, socket.MSG_PEEK))
        assert s.recv(4096) == (
            b"Content-Length: 10\r\n"
            b"Content-Type: application/vscode-jsonrpc; charset=utf8\r\n"
            b"\r\n"
            b"1546304461"
        )
