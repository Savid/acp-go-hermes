"""Checks the adapter plugin against stand-ins for the Hermes and OpenAI modules it imports.

Run as ``python3 plugin_check.py <plugin dir>``; exits non-zero on the first failed check.
"""

import importlib.util
import sys
import types
import unittest
import uuid

broadcasts = []


class CompletionUsage:
    """Keeps the members a gateway sent, as the OpenAI SDK's parsed model does."""

    def __init__(self, **members):
        self.members = members

    def model_dump(self, mode="python", exclude_unset=False):
        assert mode == "json" and exclude_unset
        return dict(self.members)


def install_stand_ins():
    events = types.ModuleType("hermes_cli.plugin_events")
    events.broadcast_plugin_event = lambda plugin, event, payload: broadcasts.append((plugin, event, payload))
    cli = types.ModuleType("hermes_cli")
    cli.plugin_events = events
    constants = types.ModuleType("hermes_constants")
    constants.PARTIAL_STREAM_STUB_ID = "partial-stream-stub"
    openai_types = types.ModuleType("openai.types")
    openai_types.CompletionUsage = CompletionUsage
    openai = types.ModuleType("openai")
    openai.types = openai_types
    sys.modules.update({
        "hermes_cli": cli, "hermes_cli.plugin_events": events, "hermes_constants": constants,
        "openai": openai, "openai.types": openai_types,
    })


def load_plugin(directory):
    spec = importlib.util.spec_from_file_location("acp_go_hermes_plugin", directory + "/__init__.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


USAGE = {"prompt_tokens": 1000, "completion_tokens": 20, "prompt_tokens_details": {"cached_tokens": 0}}
OWN = {"session_id": "s1", "task_id": "s1", "api_mode": "chat_completions"}


class Response:
    def __init__(self, response_id, usage):
        self.id = response_id
        self.usage = usage


class PluginChecks(unittest.TestCase):
    def setUp(self):
        broadcasts.clear()

    def run_call(self, response, **context):
        calls = []

        def next_call(request):
            calls.append(request)
            return response

        request = {"messages": []}
        result = plugin.report_call(request, next_call, **{**OWN, **context})
        self.assertEqual([request], calls, "the wrapped request runs exactly once")
        self.assertIs(response, result, "the response is returned unchanged")
        return [payload for _, _, payload in broadcasts]

    def test_registers_execution_middleware(self):
        registered = []
        plugin.register(types.SimpleNamespace(register_middleware=lambda kind, fn: registered.append((kind, fn))))
        self.assertEqual([("llm_execution", plugin.report_call)], registered)

    def test_reports_id_and_sent_members(self):
        reports = self.run_call(Response("gen-1", CompletionUsage(**USAGE)))
        self.assertEqual([{"session_id": "s1", "response_id": "gen-1", "usage": USAGE}], reports)
        self.assertEqual(("acp-go-hermes", "call"), broadcasts[0][:2])

    def test_drops_ids_hermes_made(self):
        for made in ("stream-" + str(uuid.uuid4()), "partial-stream-stub", "", None):
            broadcasts.clear()
            reports = self.run_call(Response(made, CompletionUsage(**USAGE)))
            self.assertEqual([{"session_id": "s1", "usage": USAGE}], reports, made)

    def test_keeps_gateway_ids_resembling_the_fallback(self):
        reports = self.run_call(Response("stream-not-a-uuid", CompletionUsage(**USAGE)))
        self.assertEqual("stream-not-a-uuid", reports[0]["response_id"])

    def test_reports_only_the_sessions_own_chat_completions(self):
        for context in ({"task_id": "review-fork"}, {"session_id": "child", "task_id": "child-task"},
                        {"session_id": "", "task_id": ""}, {"api_mode": "anthropic_messages"}):
            self.assertEqual([], self.run_call(Response("gen-1", CompletionUsage(**USAGE)), **context), context)

    def test_ignores_usage_the_sdk_did_not_parse(self):
        self.assertEqual([], self.run_call(Response("chatcmpl-made", types.SimpleNamespace(prompt_tokens=1))))
        self.assertEqual([], self.run_call(Response("gen-1", None)))

    def test_each_attempt_is_its_own_call(self):
        for index in range(2):
            self.run_call(Response(f"gen-{index}", CompletionUsage(**USAGE)))
        self.assertEqual(["gen-0", "gen-1"], [payload["response_id"] for _, _, payload in broadcasts])

    def test_failed_and_cancelled_calls_propagate(self):
        for error in (RuntimeError("provider failed"), InterruptedError()):
            def next_call(request, error=error):
                raise error

            with self.assertRaises(type(error)) as raised:
                plugin.report_call({}, next_call, **OWN)
            self.assertIs(error, raised.exception)
        self.assertEqual([], broadcasts)

    def test_a_failed_report_keeps_the_response(self):
        original = plugin.broadcast_plugin_event
        plugin.broadcast_plugin_event = lambda *_: (_ for _ in ()).throw(RuntimeError("no client"))
        try:
            response = Response("gen-1", CompletionUsage(**USAGE))
            self.assertIs(response, plugin.report_call({}, lambda request: response, **OWN))
        finally:
            plugin.broadcast_plugin_event = original


if __name__ == "__main__":
    install_stand_ins()
    plugin = load_plugin(sys.argv.pop(1))
    unittest.main(verbosity=1)
