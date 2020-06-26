# Copyright 2018 Palantir Technologies, Inc.
import asyncio
import collections
import inspect
import logging
import sys
import time
import uuid

# TODO test housekeeping stuff more thoroughly

# A cancelled asyncio.Future doesn't want its exception set to anything other
# than an asyncio.CancelledError.  For "server requests" (requests originating
# from this process to peer/client), this module's API diverges from its
# concurrent.futures counterpart. (Compare tests named test_request_cancel.) If
# the caller of Endpoint.request() expects to retrieve JsonRpcRequestCancelled
# by calling Future.exception(), they'll need some other means of doing so: one
# option is having Endpoint.request return a custom, future-like object instead
# of a vanilla asyncio.Future, (see asyncio.tasks._GatheringFuture).
from ..exceptions import (
    JsonRpcException,
    JsonRpcRequestCancelled,
    JsonRpcInternalError,
    JsonRpcMethodNotFound,
)

log = logging.getLogger(__name__)

JSONRPC_VERSION = "2.0"
CANCEL_METHOD = "$/cancelRequest"

# FIXME see if housekeeping approach is sensible and maybe tune these values;
# See if valid datasci-related overage cases exist
MAX_ESECS = 20.0  # max secs request handler allowed to run
HOUSEKEEPING_PERIOD = 5.0  # seconds


def _is_awaitable(obj):
    # These are the same tests used by ensure_future
    return (
        asyncio.iscoroutine(obj) or
        asyncio.futures.isfuture(obj) or
        inspect.isawaitable(obj)
    )


class Pending:
    """Data class containing unresolved request or notification info

    This is agnostic about whether a msg is incoming or outgoing.
    """
    # Could create notification and request subclasses
    def __init__(self, future, msg_id=None, exception=None):
        self._started = time.time()
        self._expires = self._started + MAX_ESECS
        self.future = future
        self.msg_id = msg_id
        self.exception = exception

    @property
    def expired(self):
        return self._expires < time.time()

    @classmethod
    def from_coro(cls, coro, loop=None, **init_kwargs):
        if not loop:
            loop = asyncio.get_event_loop()
        task = loop.create_task(coro)
        return cls(task, **init_kwargs)


class Endpoint:
    def __init__(
        self,
        dispatcher,
        consumer,
        id_generator=lambda: str(uuid.uuid4()),
        max_workers=None,
    ):
        """A JSON RPC endpoint for managing messages sent to/from the client.

        Args:
            dispatcher (dict): A dictionary of method name to handler
                function.  Request handler functions should return either
                the result or an awaitable resolving to the result.
                Callables and coroutine functions are not accepted.
            consumer (fn): A function that consumes JSON RPC message
                dicts and sends them to the client. It must return a
                future-like (awaitable).
            id_generator (fn, optional): A function used to generate
                request IDs.  Defaults to the string value of
                :func:`uuid.uuid4`.
        """
        self._dispatcher = dispatcher
        self._consumer = consumer
        self._id_generator = id_generator

        self._client_requests = {}        # msg_id -> Pending
        self._server_requests = {}
        self._notification_tasks = set()  # Pending

        self._loop = asyncio.get_event_loop()
        self._recent = collections.deque(maxlen=10)  # fulfilled request ids

    # FIXME maybe cancel all pending tasks
    def shutdown(self):
        raise NotImplementedError

    def _cull_obsolete(self, tasks, done_cb=None):
        """Cancel running tasks that have expired

        Call done_cb, if callable, with each expired Pending obj.
        """

        for pending in list(tasks):
            future = pending.future
            if future.done():
                if future.cancelled():
                    future.exception()
                if done_cb:
                    done_cb(pending)
                continue
            if pending.expired:
                log.warning(
                    "Cancelling task, allotted time exceeded: %s", future
                )
                pending.exception = JsonRpcInternalError(
                    message="Method hander expired for "
                    "%s" % pending.msg_id or "notification"
                )
                future.cancel()

    async def run_housekeeping(self):
        # None of the tasks in these "pending" collections are awaited
        # anywhere, so they must be wrangled somehow.  This function should be
        # awaited alongside the main loop (or kill the root future on failure,
        # e.g., by setting its exception via done callback)
        while True:
            await asyncio.sleep(HOUSEKEEPING_PERIOD)
            self._cull_obsolete(
                self._notification_tasks,
                done_cb=self._notification_tasks.discard
            )
            self._cull_obsolete(self._server_requests.values())
            self._cull_obsolete(self._client_requests.values())

    async def notify(self, method, params=None):
        """Send a JSON RPC notification to the client.

         Args:
             method (str): The method name of the notification to send
             params (any): The payload of the notification
         """
        log.debug("Sending notification: %s %s", method, params)

        message = {
            "jsonrpc": JSONRPC_VERSION,
            "method": method,
        }
        if params is not None:
            message["params"] = params

        await self._consumer(message)

    async def request(self, method, params=None):
        """Send a JSON RPC request to the client.

        Args:
            method (str): The method name of the message to send
            params (any): The payload of the message

        Returns:
            Future that will resolve into a response once received
        """
        msg_id = self._id_generator()
        log.debug("Sending request with id %s: %s %s", msg_id, method, params)

        message = {
            "jsonrpc": JSONRPC_VERSION,
            "id": msg_id,
            "method": method,
        }
        if params is not None:
            message["params"] = params

        # get_running_loop >= 3.7
        fut = self._loop.create_future()
        fut.add_done_callback(self._cancel_callback(msg_id))
        self._server_requests[msg_id] = Pending(fut, msg_id=msg_id)
        await self._consumer(message)

        return fut

    def _cancel_callback(self, request_id):
        """Construct a cancellation callback for the given request ID."""
        def callback(future):
            if not future.cancelled():
                return
            self._loop.create_task(self.notify(CANCEL_METHOD, {"id": request_id}))
            del self._server_requests[request_id]
        return callback

    async def consume(self, message):
        """Consume a JSON RPC message from the client.

        Args:
            message (dict): The JSON RPC message sent by the client
        """
        if "jsonrpc" not in message or message["jsonrpc"] != JSONRPC_VERSION:
            log.warning("Unknown message type %s", message)
            return

        if "id" not in message:
            log.debug("Handling notification from client %s", message)
            await self._handle_notification(message["method"], message.get("params"))
        elif "method" not in message:
            log.debug("Handling response from client %s", message)
            self._handle_response(
                message["id"], message.get("result"), message.get("error")
            )
        else:
            try:
                log.debug("Handling request from client %s", message)
                await self._handle_request(
                    message["id"], message["method"], message.get("params")
                )
            except JsonRpcException as e:
                log.exception("Failed to handle request %s", message["id"])
                await self._consumer(
                    {
                        "jsonrpc": JSONRPC_VERSION,
                        "id": message["id"],
                        "error": e.to_dict(),
                    }
                )
            except Exception:  # pylint: disable=broad-except
                log.exception("Failed to handle request %s", message["id"])
                await self._consumer(
                    {
                        "jsonrpc": JSONRPC_VERSION,
                        "id": message["id"],
                        "error": JsonRpcInternalError.of(sys.exc_info()).to_dict(),
                    }
                )

    async def _handle_notification(self, method, params):
        """Handle a notification from the client."""
        if method == CANCEL_METHOD:
            await self._handle_cancel_notification(params["id"])
            return

        try:
            handler = self._dispatcher[method]
        except KeyError:
            log.warning("Ignoring notification for unknown method %s", method)
            return

        try:
            handler_result = handler(params)
            if _is_awaitable(handler_result):
                coro = self._run_notification_handler(method, params, handler_result)
                self._notification_tasks.add(Pending.from_coro(coro))
        except Exception:  # pylint: disable=broad-except
            log.exception("Failed to handle notification %s: %s", method, params)
            return

    async def _run_notification_handler(self, method, params, awaitable):
        try:
            await awaitable
        except asyncio.CancelledError:
            log.debug("Notification cancelled %s: %s", method, params)
            raise
        except Exception:  # pylint: disable=broad-except
            log.exception("Failed to handle notification %s: %s", method, params)

    async def _handle_cancel_notification(self, msg_id):
        """Handle a cancel notification from the client."""
        # Don't pop here, runner will remove
        pending_request = self._client_requests.get(msg_id, None)

        if not pending_request:
            if msg_id not in self._recent:
                log.warning(
                    "Received cancel notification for unknown message id %s",
                    msg_id
                )
            return

        if time.time() - pending_request._started < 0.01:
            log.warning("Rapid cancellation from client for message id %s", msg_id)
            await asyncio.sleep(0.0)

        pending_request.exception = JsonRpcRequestCancelled(data={"id": msg_id})
        log.debug("Cancelling message with id %s", msg_id)
        pending_request.future.cancel()

    async def _handle_request(self, msg_id, method, params):
        """Handle a request from the client."""
        try:
            handler = self._dispatcher[method]
        except KeyError:
            raise JsonRpcMethodNotFound.of(method)

        handler_result = handler(params)

        if _is_awaitable(handler_result):
            try:
                pending = Pending.from_coro(
                    handler_result,
                    loop=self._loop,
                    msg_id=msg_id,
                )
            except (TypeError, AssertionError):
                pending = Pending(handler_result, msg_id=msg_id)
            self._client_requests[msg_id] = pending
            self._loop.create_task(self._run_request_handler(msg_id, pending.future))
            log.debug("(%s) Creating task for handler: %s", msg_id, handler_result)
        elif callable(handler_result):
            raise ValueError("Callable returned by handler %r" % handler)
        else:
            log.debug("(%s) Result from request handler: %s", msg_id, handler_result)
            await self._consumer(
                {"jsonrpc": JSONRPC_VERSION, "id": msg_id, "result": handler_result}
            )

    async def _run_request_handler(self, request_id, future):
        """Construct a request callback for the given request ID."""
        message = {
            "jsonrpc": JSONRPC_VERSION,
            "id": request_id,
        }
        try:
            result = await future
            message["result"] = result
            log.debug(
                "(%s) Result from async handler: %s", request_id, result
            )
        except asyncio.CancelledError:
            pending = self._client_requests[request_id]
            if not pending.exception:
                # XXX not sure what to do here: a response should always be
                # returned, but cancellations must be propagated ASAP; for now,
                # assume server process is shutting down and raise
                log.warn("(%s) Request cancelled externally", request_id)
                raise
            message["error"] = pending.exception.to_dict()
        except JsonRpcException as e:
            log.exception("Failed to handle request %s", request_id)
            message["error"] = e.to_dict()
        except Exception:  # pylint: disable=broad-except
            log.exception("Failed to handle request %s", request_id)
            message["error"] = JsonRpcInternalError.of(sys.exc_info()).to_dict()
        finally:
            self._client_requests.pop(request_id)
            self._recent.append(request_id)

        await self._consumer(message)

    def _handle_response(self, msg_id, result=None, error=None):
        """Handle a response from the client."""
        pending = self._server_requests.pop(msg_id, None)

        if not pending:
            log.warning("Received response to unknown message id %s", msg_id)
            return

        if pending.future.cancelled():
            log.warning("Future already cancelled for id %s", msg_id)
            return

        if error is not None:
            log.debug("Received error response to message %s: %s", msg_id, error)
            pending.future.set_exception(JsonRpcException.from_dict(error))
            return

        log.debug("Received result for message %s: %s", msg_id, result)
        pending.future.set_result(result)
