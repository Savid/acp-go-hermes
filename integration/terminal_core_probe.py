#!/usr/bin/env python3
"""Deterministically verify Hermes terminal-core session CWD isolation."""

from __future__ import annotations

import json
import os
import pathlib
import shutil
import sys
import threading


if len(sys.argv) != 3:
    raise SystemExit("usage: terminal_core_probe.py <scratch-root> <hermes-agent-root>")

# Both roots arrive in argv so this probe claims no name in the adapter's
# governed environment namespace, which is reserved for real options.
ROOT = pathlib.Path(sys.argv[1])
AGENT_ROOT = pathlib.Path(sys.argv[2])
WORK_A = ROOT / "work-a"
WORK_B = ROOT / "work-b"


def fail(message: str) -> None:
    raise AssertionError(message)


def main() -> None:
    if not (AGENT_ROOT / "tools/terminal_tool.py").is_file():
        fail(f"Hermes terminal core not found under {AGENT_ROOT}")

    shutil.rmtree(ROOT, ignore_errors=True)
    WORK_A.mkdir(parents=True)
    WORK_B.mkdir(parents=True)

    os.environ["HERMES_HOME"] = str(ROOT / "home")
    os.environ["TERMINAL_ENV"] = "local"
    os.environ["TERMINAL_CWD"] = str(ROOT)
    sys.path.insert(0, str(AGENT_ROOT))

    from tools import terminal_tool as tt

    def call(task_id: str, command: str) -> dict:
        result = json.loads(
            tt.terminal_tool(command=command, task_id=task_id, force=True)
        )
        if result.get("exit_code") != 0:
            fail(f"{task_id} command failed: {result}")
        return result

    tt.cleanup_all_environments()
    tt._task_env_overrides.clear()
    tt.register_task_env_overrides("session-a", {"cwd": str(WORK_A)})
    tt.register_task_env_overrides("session-b", {"cwd": str(WORK_B)})

    resolved_a = tt._resolve_container_task_id("session-a")
    resolved_b = tt._resolve_container_task_id("session-b")
    if (resolved_a, resolved_b) != ("default", "default"):
        fail(
            "CWD-only sessions no longer collapse to one terminal environment; "
            "rerun the complete Hermes isolation qualification"
        )

    call("session-b", "pwd")

    original_resolve = tt._resolve_command_cwd
    a_at_resolve = threading.Event()
    b_claimed_owner = threading.Event()
    release_b = threading.Event()
    failures: list[BaseException] = []

    def forced_resolve(*, workdir, default_cwd, session_key=None, env_type=None):
        if default_cwd == str(WORK_A):
            a_at_resolve.set()
            if not b_claimed_owner.wait(timeout=5):
                fail("B did not reach the forced owner interleaving")
        elif default_cwd == str(WORK_B):
            b_claimed_owner.set()
            if not release_b.wait(timeout=5):
                fail("B was not released after A completed")
        return original_resolve(
            workdir=workdir,
            default_cwd=default_cwd,
            session_key=session_key,
            env_type=env_type,
        )

    def run(task_id: str, filename: str) -> None:
        try:
            call(task_id, f"pwd > {filename}")
        except BaseException as exc:
            failures.append(exc)

    tt._resolve_command_cwd = forced_resolve
    try:
        thread_a = threading.Thread(
            target=run,
            args=("session-a", "deterministic-a.txt"),
            name="session-a",
        )
        thread_a.start()
        if not a_at_resolve.wait(timeout=5):
            fail("A did not reach the forced owner interleaving")

        thread_b = threading.Thread(
            target=run,
            args=("session-b", "deterministic-b.txt"),
            name="session-b",
        )
        thread_b.start()

        thread_a.join(timeout=5)
        if thread_a.is_alive():
            fail("A did not finish after B claimed the shared owner")
        release_b.set()
        thread_b.join(timeout=5)
        if thread_b.is_alive():
            fail("B did not finish after release")
    finally:
        release_b.set()
        tt._resolve_command_cwd = original_resolve
        tt.cleanup_all_environments()

    if failures:
        raise failures[0]

    a_in_a = (WORK_A / "deterministic-a.txt").is_file()
    a_in_b = (WORK_B / "deterministic-a.txt").is_file()
    b_in_b = (WORK_B / "deterministic-b.txt").is_file()
    if not a_in_a or a_in_b or not b_in_b:
        fail(
            "forced interleaving violated session-keyed cwd isolation: "
            f"a_in_a={a_in_a} a_in_b={a_in_b} b_in_b={b_in_b}"
        )

    print("TERMINAL_CORE_CWD_ISOLATED=true")
    print("VERDICT=session-keyed-terminal-cwd-isolated")


if __name__ == "__main__":
    main()
