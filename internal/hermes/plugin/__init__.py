"""Reports each Chat Completions response of a session's own conversation to the adapter.

The report rides the gateway's client stream as ``plugin.acp-go-hermes.call``, written
after the response's streamed deltas and before Hermes records the response's usage, so
it precedes every native event that follows from that response.

Only the ``hermes serve`` the adapter starts sets ``ACP_GO_HERMES_CALL_REPORTS``; every
other Hermes process sharing the home loads the plugin and registers nothing.

That process also collects and then freezes its garbage collector's tracked objects at its
first model call. A collection writes to every object it traverses, so the objects the process
built to start, which live as long as it does, would have their memory rewritten by each
collection Hermes runs after a turn. Frozen objects are never traversed; collecting first keeps
startup garbage out of the frozen set, where it could never be freed.
"""

import gc
import os
import re
import sys

from hermes_cli.plugin_events import broadcast_plugin_event
from hermes_constants import PARTIAL_STREAM_STUB_ID
from openai.types import CompletionUsage

PLUGIN_ID = "acp-go-hermes"
CALL_EVENT = "call"
ENABLE_ENV = "ACP_GO_HERMES_CALL_REPORTS"
GATEWAY_MODULE = "tui_gateway.server"

# Hermes names a reassembled stream "stream-<uuid4>" when no chunk carried an id.
_STREAM_FALLBACK_ID = re.compile(
    r"stream-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)


_frozen = False


def register(ctx):
    if os.environ.get(ENABLE_ENV) == "1":
        ctx.register_middleware("llm_execution", report_call)


def report_call(request, next_call, session_id="", task_id="", api_mode="", model="", **_context):
    """Run the provider call once and return its response untouched.

    The gateway runs a session's turns with the session key as the task id; review forks
    and delegated children run under another task or session id and are not reported. A
    process whose gateway server is not loaded has no client to report to, and importing
    the server there would start its background threads.
    """
    freeze_startup_objects()
    response = next_call(request)
    if session_id and task_id == session_id and api_mode == "chat_completions" and GATEWAY_MODULE in sys.modules:
        try:
            report = call_report(response)
            if report is not None:
                report["session_id"] = session_id
                report["model"] = model
                broadcast_plugin_event(PLUGIN_ID, CALL_EVENT, report)
        except Exception:
            pass
    return response


def freeze_startup_objects():
    """Collect, then freeze every tracked object, once the first model call shows startup is done."""
    global _frozen
    if not _frozen:
        _frozen = True
        gc.collect()
        gc.freeze()


def call_report(response):
    """The response's id and the usage members its gateway sent, or None.

    Only usage the OpenAI SDK parsed from the gateway's body is reported, so a response a
    native adapter assembled with its own figures is never mistaken for the gateway's.
    """
    usage = getattr(response, "usage", None)
    if not isinstance(usage, CompletionUsage):
        return None
    report = {"usage": usage.model_dump(mode="json", exclude_unset=True)}
    response_id = getattr(response, "id", None)
    if (
        isinstance(response_id, str)
        and response_id
        and response_id != PARTIAL_STREAM_STUB_ID
        and not _STREAM_FALLBACK_ID.fullmatch(response_id)
    ):
        report["response_id"] = response_id
    return report
