#!/bin/sh
# Entrypoint for the privileged suite container. Runs inside the container that
# run-privileged-suite.sh starts; see that script for why the suite needs one.
#
# This is a committed file rather than something piped in on stdin: a container
# whose shell reads an empty stdin exits 0 having run nothing, which reports a
# suite that never executed as a pass.
set -eu

target=${1:?usage: privileged-suite-entrypoint.sh <make-target>}

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
echo "pid 1:               $(cat /proc/1/comm 2>/dev/null || echo unknown)"
echo "umask:               $(umask)"
printf 'temp root:           %s fstype=%s mode=%s owner=%s:%s\n' \
	"$TMPDIR" "$(stat -f -c '%T' "$TMPDIR")" \
	"$(stat -c '%a' "$TMPDIR")" "$(stat -c '%u' "$TMPDIR")" "$(stat -c '%g' "$TMPDIR")"
echo "authority root:      mode=$(stat -c '%a' /var/lib/acp-go/agent-identities) owner=$(stat -c '%u:%g' /var/lib/acp-go/agent-identities)"
echo "setsid:              $(command -v setsid || echo missing)"
echo "go:                  $(go version)"
echo "::endgroup::"

# The initial PID namespace is the whole reason this container exists. Without
# it the supervised-native cases skip rather than fail, and a skipped case still
# starves the 100% coverage gate, so refuse up front instead of reporting a
# confusing coverage shortfall later.
namespace=$(stat -Lc '%i' /proc/self/ns/pid)
if [ "$namespace" != 4026531836 ] && [ "$$" != 1 ]; then
	echo "privileged suite is not in the initial PID namespace (inode $namespace)" >&2
	echo "the host daemon must support --pid=host for this suite to establish agent authority" >&2
	exit 1
fi

exec make "$target"
