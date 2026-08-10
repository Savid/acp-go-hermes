#!/bin/sh

# The privileged proof lane is stronger than the production binder policy: it
# must see the Linux initial PID namespace itself, even when its entrypoint is
# namespace PID 1. Keep the exact inode decision in one sourceable predicate so
# the outer harness can pin both acceptance and refusal without a test bypass.
is_initial_pid_namespace_inode() {
	[ "$1" = 4026531836 ]
}
