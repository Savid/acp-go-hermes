"""Reports each Chat Completions response of a session's own conversation to the adapter.

The report rides the gateway's client stream as ``plugin.acp-go-hermes.call``, written
after the response's streamed deltas and before Hermes records the response's usage, so
it precedes every native event that follows from that response.
"""

import re

from hermes_cli.plugin_events import broadcast_plugin_event
from hermes_constants import PARTIAL_STREAM_STUB_ID
from openai.types import CompletionUsage

PLUGIN_ID = "acp-go-hermes"
CALL_EVENT = "call"

# Hermes names a reassembled stream "stream-<uuid4>" when no chunk carried an id.
_STREAM_FALLBACK_ID = re.compile(
    r"stream-[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}"
)


def register(ctx):
    ctx.register_middleware("llm_execution", report_call)


def report_call(request, next_call, session_id="", task_id="", api_mode="", **_context):
    """Run the provider call once and return its response untouched.

    The gateway runs a session's turns with the session key as the task id; review forks
    and delegated children run under another task or session id and are not reported.
    """
    response = next_call(request)
    if session_id and task_id == session_id and api_mode == "chat_completions":
        try:
            report = call_report(response)
            if report is not None:
                report["session_id"] = session_id
                broadcast_plugin_event(PLUGIN_ID, CALL_EVENT, report)
        except Exception:
            pass
    return response


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
