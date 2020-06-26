# Copyright 2018 Palantir Technologies, Inc.
# pylint: disable=redefined-outer-name
import sys
import asyncio
import mock
import pytest

pytest.mark.skipif(sys.version_info < (3, 5, 2), reason="Compat")

from pyls_jsonrpc import exceptions  # noqa: E402
from pyls_jsonrpc.asyncio.endpoint import Endpoint  # noqa: E402

from .conftest import async_noop, await_assertion, await_condition  # noqa: E402
from ..test_endpoint import assert_consumer_error  # noqa: E402

MSG_ID = "id"


pytestmark = pytest.mark.asyncio


@pytest.fixture()
def dispatcher():
    return {}


@pytest.fixture()
def consumer():
    return mock.Mock(wraps=async_noop)


@pytest.fixture()
def endpoint(dispatcher, consumer):
    return Endpoint(dispatcher, consumer, id_generator=lambda: MSG_ID)


async def test_bad_message(endpoint):
    await endpoint.consume({"key": "value"})


async def test_notify(endpoint, consumer):
    await endpoint.notify("methodName", {"key": "value"})
    consumer.assert_called_once_with(
        {"jsonrpc": "2.0", "method": "methodName", "params": {"key": "value"}}
    )


async def test_notify_none_params(endpoint, consumer):
    await endpoint.notify("methodName", None)
    consumer.assert_called_once_with(
        {"jsonrpc": "2.0", "method": "methodName"}
    )


async def test_request(endpoint, consumer):
    future = await endpoint.request("methodName", {"key": "value"})

    assert not future.done()
    assert isinstance(future, asyncio.Future)  # not a Task

    consumer.assert_called_once_with(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )

    # Send the response back to the endpoint
    result = 1234
    await endpoint.consume({"jsonrpc": "2.0", "id": MSG_ID, "result": result})
    assert result == await future


async def test_request_error(endpoint, consumer):
    future = await endpoint.request("methodName", {"key": "value"})
    assert not future.done()

    consumer.assert_called_once_with(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )

    # Send an error back from the client
    error = exceptions.JsonRpcInvalidRequest(data=1234)
    await endpoint.consume({"jsonrpc": "2.0", "id": MSG_ID, "error": error.to_dict()})

    # Verify the exception raised by the future is the same as the error the client serialized
    with pytest.raises(exceptions.JsonRpcException) as exc_info:
        await future
    assert exc_info.type == exceptions.JsonRpcInvalidRequest
    assert exc_info.value == error


async def test_request_cancel(endpoint, consumer):
    future = await endpoint.request("methodName", {"key": "value"})
    assert not future.done()

    consumer.assert_called_once_with(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )
    future.cancel()

    # Cancel the request
    with pytest.raises(asyncio.CancelledError):
        await future

    await await_assertion(
        lambda: consumer.assert_any_call(
            {"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": MSG_ID}},
        )
    )
    # XXX see note atop endpoints module and namesake test in ../test_endpoint.py
    with pytest.raises(asyncio.CancelledError):
        future.exception()


async def test_consume_notification(endpoint, dispatcher):
    handler = mock.Mock(wraps=async_noop)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {"jsonrpc": "2.0", "method": "methodName", "params": {"key": "value"}}
    )
    handler.assert_called_once_with({"key": "value"})


async def test_consume_notification_error(endpoint, dispatcher):
    handler = mock.Mock(wraps=async_noop, side_effect=ValueError)
    dispatcher["methodName"] = handler
    # Verify the consume doesn't throw
    await endpoint.consume(
        {"jsonrpc": "2.0", "method": "methodName", "params": {"key": "value"}}
    )
    handler.assert_called_once_with({"key": "value"})


async def test_consume_notification_method_not_found(endpoint):
    # Verify consume doesn't throw for method not found
    await endpoint.consume(
        {"jsonrpc": "2.0", "method": "methodName", "params": {"key": "value"}}
    )


async def test_consume_async_notification_error(endpoint, dispatcher, consumer):
    def some_callable():
        raise ValueError()

    handler = mock.Mock(return_value=some_callable)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {"jsonrpc": "2.0", "method": "methodName", "params": {"key": "value"}}
    )
    handler.assert_called_once_with({"key": "value"})
    consumer.assert_not_called()


async def test_consume_non_awaitable_request(endpoint, consumer, dispatcher):
    result = 1234
    handler = mock.Mock(return_value=result)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )

    handler.assert_called_once_with({"key": "value"})
    consumer.assert_called_once_with({"jsonrpc": "2.0", "id": MSG_ID, "result": result})


async def test_consume_future_request(endpoint, consumer, dispatcher, event_loop):
    fut = event_loop.create_future()
    handler = mock.Mock(return_value=fut)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )

    handler.assert_called_once_with({"key": "value"})
    consumer.assert_not_called()
    fut.set_result(1234)
    await await_assertion(
        lambda: consumer.assert_called_once_with(
            {"jsonrpc": "2.0", "id": MSG_ID, "result": 1234}
        )
    )


async def test_consume_request_reject_callable(endpoint, consumer, dispatcher):
    def some_callable():
        return 1234

    handler = mock.Mock(return_value=some_callable)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )

    handler.assert_called_once_with({"key": "value"})
    await await_assertion(lambda: consumer.assert_called_once())
    arg = consumer.call_args[0][0]
    assert arg["error"]["message"].startswith("ValueError")
    arg["error"]["message"] = "..."
    arg["error"]["data"]["traceback"] = "..."
    assert arg == {
        "jsonrpc": "2.0",
        "id": "id",
        "error": {"code": -32602, "message": "...", "data": {"traceback": "..."}},
    }


@pytest.mark.parametrize(
    "exc_type, error",
    [
        (ValueError, exceptions.JsonRpcInternalError(message="ValueError")),
        (KeyError, exceptions.JsonRpcInternalError(message="KeyError")),
        (exceptions.JsonRpcMethodNotFound, exceptions.JsonRpcMethodNotFound()),
    ],
)
async def test_consume_coro_request_error(
    exc_type, error, endpoint, consumer, dispatcher
):

    async def handler(msg):
        raise exc_type()

    handler = mock.Mock(wraps=handler)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )

    handler.assert_called_once_with({"key": "value"})
    await await_assertion(lambda: assert_consumer_error(consumer, error))


async def test_consume_request_method_not_found(endpoint, consumer):
    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )
    assert_consumer_error(consumer, exceptions.JsonRpcMethodNotFound.of("methodName"))


@pytest.mark.parametrize(
    "exc_type, error",
    [
        (ValueError, exceptions.JsonRpcInternalError(message="ValueError")),
        (KeyError, exceptions.JsonRpcInternalError(message="KeyError")),
        (exceptions.JsonRpcMethodNotFound, exceptions.JsonRpcMethodNotFound()),
    ],
)
async def test_consume_request_error(exc_type, error, endpoint, consumer, dispatcher):
    # Error during initial handler call, not return value processing
    handler = mock.Mock(side_effect=exc_type)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )

    handler.assert_called_once_with({"key": "value"})
    await await_assertion(lambda: assert_consumer_error(consumer, error))


async def test_consume_request_cancel(endpoint, dispatcher, consumer):
    # caplog.set_level("DEBUG")
    count = []

    async def async_handler(msg):
        assert msg == {"key": "value"}
        for c in "abc":
            count.append(c)
            await asyncio.sleep(0.1)

    handler = mock.Mock(wraps=async_handler)
    dispatcher["methodName"] = handler

    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )
    handler.assert_called_once_with({"key": "value"})
    pending = endpoint._client_requests.get(MSG_ID)
    assert pending
    assert not pending.future.cancelled()
    await await_condition(lambda: count)  # ensure it's actually running

    await endpoint.consume(
        {"jsonrpc": "2.0", "method": "$/cancelRequest", "params": {"id": MSG_ID}}
    )
    await await_condition(pending.future.cancelled)
    await await_condition(lambda: not endpoint._client_requests.get(MSG_ID))
    await await_condition(lambda: len(consumer.mock_calls))

    assert_consumer_error(consumer, exceptions.JsonRpcRequestCancelled())


async def test_consume_request_cancel_unknown(endpoint):
    # Verify consume doesn't throw
    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "method": "$/cancelRequest",
            "params": {"id": "unknown identifier"},
        }
    )


@pytest.fixture
def safe_housekeeping():
    from pyls_jsonrpc.asyncio import endpoint as module
    orig_esecs = module.MAX_ESECS
    orig_period = module.HOUSEKEEPING_PERIOD
    try:
        yield
    finally:
        module.MAX_ESECS = orig_esecs
        module.HOUSEKEEPING_PERIOD = orig_period


async def test_resquest_expire(
    endpoint, dispatcher, consumer, safe_housekeeping
):
    from pyls_jsonrpc.asyncio import endpoint as module
    module.MAX_ESECS = 0.2
    module.HOUSEKEEPING_PERIOD = 0.5

    count = []

    async def async_handler(msg):
        assert msg == {"key": "value"}
        for c in "abc":
            count.append(c)
            await asyncio.sleep(0.01)
        await asyncio.sleep(5)

    handler = mock.Mock(wraps=async_handler)
    dispatcher["methodName"] = handler

    # start housekeeping
    housekeeping_task = endpoint._loop.create_task(endpoint.run_housekeeping())

    await endpoint.consume(
        {
            "jsonrpc": "2.0",
            "id": MSG_ID,
            "method": "methodName",
            "params": {"key": "value"},
        }
    )
    handler.assert_called_once_with({"key": "value"})
    pending = endpoint._client_requests.get(MSG_ID)
    assert pending
    assert not pending.future.cancelled()
    await await_condition(lambda: count)  # ensure it's actually running
    pending._expires -= module.MAX_ESECS

    await await_condition(pending.future.cancelled)
    await await_condition(lambda: not endpoint._client_requests.get(MSG_ID))
    await await_condition(lambda: len(consumer.mock_calls) == 1)

    expected = exceptions.JsonRpcInternalError(message="Method hander expired for id")
    assert_consumer_error(consumer, expected)
    housekeeping_task.cancel()
