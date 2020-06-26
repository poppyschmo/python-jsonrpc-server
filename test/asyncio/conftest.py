import asyncio
import mock
import json

# Should maybe rename this file: subpackage conftests are for fixtures and
# pytest settings, but this turned out to be just another common utils module.


from functools import partial


async def async_noop(*a, **kw):
    """Mock wrap object"""


# No async mock in < 3.8
def get_async_mock(f=None):
    return mock.Mock(wraps=f or async_noop)


async def await_assertion(condition, timeout=3.0, interval=0.1, exc=None, asbool=False):
    if timeout <= 0:
        raise exc if exc else AssertionError(
            "Failed to wait for condition %s" % condition
        )
    try:
        if asbool:
            assert condition()
        else:
            condition()
    except AssertionError as e:
        await asyncio.sleep(interval)
        await await_assertion(
            condition,
            timeout=(timeout - interval),
            interval=interval,
            exc=e,
            asbool=asbool,
        )


await_condition = partial(await_assertion, asbool=True)


# XXX nondeterministic serialized dict ordering detected in 3.5.9 even though
# sorted dicts introduced in 3.5. Maybe related to ujson==1.3.5 lib?
# Note: this may fail w. nested JSON objects and lists, e.g. .error.data,
# because separator spacing may differ.
def kv_chk(dictobj, haystack):
    pats = {
        ('"%s":%s' % (k, json.dumps(v))).encode("utf-8") for k, v in dictobj.items()
    }
    return all(s in haystack for s in pats)
