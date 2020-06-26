import os
import sys
import pytest
import asyncio

from pexpect import EOF
from pyls_jsonrpc.asyncio.streams import JsonRpcStreamWriter, connect_pipes

pytest.importorskip("pexpect")
pytest_plugins = "pytester"


@pytest.mark.asyncio
async def test_connect_tcp(testdir, unused_tcp_port):

    # CI unprivileged user runs pip install w. --user -> .local/lib
    testdir.monkeypatch.setenv("PYTHONPATH", ":".join(sys.path))

    testdir.makepyfile(
        main="""
        import sys
        import asyncio

        print(sys.path)

        from pyls_jsonrpc.asyncio.streams import connect_tcp

        async def main():
            server, gen = await connect_tcp('127.0.0.1', {port})
            try:
                print('listening on 127.0.0.1:{port}')
                async for reader, writer in gen:
                    msg = await reader.read_message()
                    print(msg)
                    msg = {{'id': 'hello', 'result': 'success'}}
                    await writer.write_message(msg)
                    await asyncio.sleep(0.1)
                    return
            finally:
                server.close()
                await server.wait_closed()

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        """.format(port=unused_tcp_port)
    )
    child = testdir.spawn("python main.py")
    child.expect("listening")
    reader, _writer = await asyncio.open_connection(
        "127.0.0.1", unused_tcp_port
    )
    writer = JsonRpcStreamWriter.from_stream_writer(_writer)
    msg = {"id": "hello", "method": "method", "params": {}}
    await writer.write_message(msg)
    child.expect("params")
    got = bytearray()
    async for line in reader:
        got.extend(line)

    assert got == (
        b'Content-Length: 33\r\nContent-Type: application/vscode-jsonrpc;'
        b' charset=utf8\r\n\r\n{"id":"hello","result":"success"}'
    ) or got == (
        b'Content-Length: 33\r\nContent-Type: application/vscode-jsonrpc;'
        b' charset=utf8\r\n\r\n{"result":"success","id":"hello"}'
    )
    child.expect(EOF)


@pytest.mark.skipif(os.name != "posix", reason="Platform not Unix")
@pytest.mark.asyncio
async def test_connect_pipes(testdir):

    # CI
    testdir.monkeypatch.setenv("PYTHONPATH", ":".join(sys.path))

    # Ptyprocess reaps inherited fds via os.closerange, so use named pipes
    # instead; ordering must be right w/o O_NONBLOCK to avoid deadlock
    _rpipe = testdir.tmpdir / "server_out.fifo"
    _wpipe = testdir.tmpdir / "server_in.fifo"
    os.mkfifo(_rpipe.strpath)
    os.mkfifo(_wpipe.strpath)  # .strpath 3.5 compat

    testdir.makepyfile(
        main="""
        import os
        import sys
        import asyncio

        print("pid:", os.getpid(), flush=True)
        print("reading from:", {r!r}, flush=True)
        print("writing to:", {w!r}, flush=True)
        rpipe = open({r!r}, "r")
        wpipe = open({w!r}, "w")

        from pyls_jsonrpc.asyncio.streams import connect_pipes

        async def main():
            reader, writer = await connect_pipes(rpipe, wpipe)
            print('listening on stdin')
            msg = await reader.read_message()
            print(msg)
            msg = {{'id': 'hello', 'result': 'success'}}
            await writer.write_message(msg)
            await asyncio.sleep(0.1)
            print("Exiting")

        loop = asyncio.get_event_loop()
        loop.run_until_complete(main())
        """.format(r=_wpipe.strpath, w=_rpipe.strpath)  # .strpath bec repr
    )
    child = testdir.spawn("python main.py")
    child.expect("writing")

    wpipe = open(_wpipe.strpath, "w")
    rpipe = open(_rpipe.strpath, "r")

    child.expect("listening")

    reader, writer = await connect_pipes(rpipe, wpipe)

    msg = {"id": "hello", "method": "method", "params": {}}
    await writer.write_message(msg)
    child.expect("params")

    msg = await reader.read_message()
    assert msg in (
        b'{"id":"hello","result":"success"}',
        b'{"result":"success","id":"hello"}',
    )
    child.expect(EOF)
