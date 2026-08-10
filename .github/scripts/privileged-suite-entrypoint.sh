#!/bin/sh
# Entrypoint for the privileged suite container. Runs inside the container that
# run-privileged-suite.sh starts; see that script for why the suite needs one.
#
# This is a committed file rather than something piped in on stdin: a container
# whose shell reads an empty stdin exits 0 having run nothing, which reports a
# suite that never executed as a pass.
set -eu

script_dir=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$script_dir/initial-pid-namespace.sh"

target=${1:?usage: privileged-suite-entrypoint.sh <make-target>}
: "${ACP_GO_PRIVILEGED_SHARD:?privileged shard identity is required}"
: "${ACP_GO_PRIVILEGED_MODULE:?privileged module identity is required}"
: "${ACP_GO_PRIVILEGED_PACKAGES:?privileged shard package selector is required}"
: "${ACP_GO_PRIVILEGED_COVERAGE_OUT:?privileged shard coverage output is required}"
: "${ACP_GO_PRIVILEGED_REQUIRED_CLASSES:?trusted-supervisor required classes are required}"

case "$target" in
coverage-check)
	private_target=_privileged-shard-coverage
	;;
test-trusted-supervisor)
	private_target=_privileged-shard-trusted-supervisor
	;;
*)
	echo "unsupported privileged suite target $target" >&2
	exit 1
	;;
esac

# Pinned because the suite asserts on exact directory modes. A umask of 0077
# turns a 0755 fixture into 0700 and breaks the traversal cases; a slacker one
# breaks the inverse.
umask 022

# The authority tree arrives as a volume, and docker creates those mode 0755.
# The suite requires it root-owned and 0700 before it will run at all.
chown 0:0 /var/lib/acp-go /var/lib/acp-go/agent-identities
chmod 0700 /var/lib/acp-go /var/lib/acp-go/agent-identities

echo "::group::privileged suite environment"
id
echo "pid namespace inode: $(stat -Lc '%i' /proc/self/ns/pid) (initial is 4026531836)"
echo "entrypoint pid:       $$"
echo "pid 1:               $(cat /proc/1/comm 2>/dev/null || echo unknown)"
echo "umask:               $(umask)"
printf 'temp root:           %s fstype=%s mode=%s owner=%s:%s\n' \
	"$TMPDIR" "$(stat -f -c '%T' "$TMPDIR")" \
	"$(stat -c '%a' "$TMPDIR")" "$(stat -c '%u' "$TMPDIR")" "$(stat -c '%g' "$TMPDIR")"
echo "authority root:      mode=$(stat -c '%a' /var/lib/acp-go/agent-identities) owner=$(stat -c '%u:%g' /var/lib/acp-go/agent-identities)"
echo "package shard:       $ACP_GO_PRIVILEGED_SHARD"
echo "module:              $ACP_GO_PRIVILEGED_MODULE"
echo "packages:            $ACP_GO_PRIVILEGED_PACKAGES"
echo "setsid:              $(command -v setsid || echo missing)"
echo "go:                  $(go version)"
echo "go build cache:      $(go env GOCACHE)"
echo "go module cache:     $(go env GOMODCACHE)"
echo "GOFLAGS:             ${GOFLAGS:-unset}"
echo "::endgroup::"

# The workspace is writable — the suites leave coverage.out and fixtures in it —
# so run-privileged-suite.sh pins go.mod and go.sum read-only one file at a
# time. A stray -mod=mod once rewrote a pushed module file in place from in
# here, and GOFLAGS alone cannot stop that: an explicit command-line -mod= beats
# it. Prove the read-only mounts are actually armed before running the target,
# rather than diffing the damage after it. `true` rather than `:` on purpose: a
# redirection failure on a special builtin exits the shell outright and would
# report this refusal as an unexplained crash. The append opens for writing
# without writing a byte, so an armed guard leaves the file untouched.
for module_file in /src/go.mod /src/go.sum; do
	if true 2>/dev/null >>"$module_file"; then
		echo "$module_file is writable inside the privileged suite container" >&2
		echo "run-privileged-suite.sh must bind-mount it read-only: the suite may never rewrite a pushed module file" >&2
		exit 1
	fi
done

# The initial PID namespace is the whole reason this container exists. Without
# it the supervised-native cases skip rather than fail, and a skipped case still
# starves the 100% coverage gate, so refuse up front instead of reporting a
# confusing coverage shortfall later.
namespace=$(stat -Lc '%i' /proc/self/ns/pid)
if ! is_initial_pid_namespace_inode "$namespace"; then
	echo "privileged suite is not in the initial PID namespace (inode $namespace)" >&2
	echo "the host daemon must support --pid=host for this suite to establish agent authority" >&2
	exit 1
fi

exec make \
	ACP_GO_PRIVILEGED_INTERNAL=1 \
	"ACP_GO_PRIVILEGED_SHARD=$ACP_GO_PRIVILEGED_SHARD" \
	"ACP_GO_PRIVILEGED_MODULE=$ACP_GO_PRIVILEGED_MODULE" \
	"ACP_GO_PRIVILEGED_PACKAGES=$ACP_GO_PRIVILEGED_PACKAGES" \
	"ACP_GO_PRIVILEGED_COVERAGE_OUT=$ACP_GO_PRIVILEGED_COVERAGE_OUT" \
	"ACP_GO_PRIVILEGED_REQUIRED_CLASSES=$ACP_GO_PRIVILEGED_REQUIRED_CLASSES" \
	"$private_target"
