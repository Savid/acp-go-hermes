#!/bin/sh
# Hold the host privileged-suite lock while running a command.
#
# Every lane that runs a container with --pid=host shares one PID namespace and
# claims fixed identities such as uid 65534, so two of them on one host corrupt
# each other's authority domain. That covers both the containerized suites and
# the native-browser canary, which reaches the host namespace the same way.
# Serialize all of them on one lock.
#
# The lock file has to live on a mount the host provides, so that every runner
# container on the host contends on a single inode. Resolve it from the runner
# container's own mount table rather than guessing a path: a lock that silently
# lands on the container's own overlay would still take cleanly and serialize
# nothing.
set -eu

[ "$#" -gt 0 ] || {
	echo "usage: with-privileged-lock.sh <command> [args...]" >&2
	exit 1
}

for tool in docker flock; do
	command -v "$tool" >/dev/null 2>&1 || {
		echo "privileged lock requires $tool on the runner" >&2
		exit 1
	}
done

workspace=${GITHUB_WORKSPACE:-$(pwd)}

inspect=$(mktemp)
trap 'rm -f "$inspect"' EXIT HUP INT TERM
if ! docker inspect "$(hostname)" \
	--format '{{range .Mounts}}{{.Destination}} {{.Source}}{{"\n"}}{{end}}' >"$inspect" 2>/dev/null; then
	echo "cannot inspect the runner container: the docker socket does not expose it" >&2
	exit 1
fi

best_destination=
best_source=
while read -r destination source; do
	[ -n "$destination" ] || continue
	case "$workspace" in
	"$destination" | "$destination"/*) ;;
	*) continue ;;
	esac
	if [ "${#destination}" -gt "${#best_destination}" ]; then
		best_destination=$destination
		best_source=$source
	fi
done <"$inspect"

if [ -z "$best_destination" ]; then
	echo "no runner mount contains $workspace; the workspace is not reachable from the host daemon" >&2
	sed 's/^/  mount: /' "$inspect" >&2
	exit 1
fi

ACP_GO_WORKSPACE_HOST=$best_source${workspace#"$best_destination"}
ACP_GO_PRIVILEGED_LOCK=$best_destination/acp-go-privileged.lock
export ACP_GO_WORKSPACE_HOST ACP_GO_PRIVILEGED_LOCK

echo "::group::privileged lock"
echo "workspace:       $workspace"
echo "workspace(host): $ACP_GO_WORKSPACE_HOST"
echo "lock:            $ACP_GO_PRIVILEGED_LOCK -> $best_source/acp-go-privileged.lock"
echo "::endgroup::"

: >>"$ACP_GO_PRIVILEGED_LOCK"
exec 9>>"$ACP_GO_PRIVILEGED_LOCK"
echo "waiting for the host privileged-suite lock"
flock 9
echo "holding the host privileged-suite lock"

exec "$@"
