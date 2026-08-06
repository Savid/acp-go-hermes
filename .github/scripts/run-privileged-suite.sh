#!/bin/sh
# Run a privileged suite inside a purpose-built container.
#
# The self-hosted runners are themselves containers. That costs the suite three
# things it genuinely requires, none of which the product should relax:
#
#   * The initial PID namespace. A runner container sits in a nested namespace
#     and is not its PID 1, so validateAgentStandaloneBinder refuses to
#     establish agent authority and every supervised-native case either fails or
#     skips. Skips are just as fatal here: coverage-check demands 100%.
#   * A temp root on a filesystem with local lock semantics whose ancestry is
#     root-owned and not group- or other-writable. A container's /tmp is
#     overlayfs and mode 01777.
#   * An authority root at /var/lib/acp-go the suite can own outright, and a
#     /tmp on a real filesystem. Several fixtures name /tmp directly, and the
#     home lock refuses overlayfs because it has no local lock semantics.
#
# The runner's docker socket is the host daemon, which is how the native-browser
# canary already reaches the host PID namespace. Use the same door: run the
# suite in a container started with --pid=host, root, a tmpfs temp root, and a
# fresh volume for the authority tree.
#
# Two privileged suites on one host must never overlap. They share the host PID
# namespace and both claim fixed identities such as uid 65534, so a concurrent
# pair corrupts each other's authority domain. Serialize on a lock file that
# lives on the shared runner work mount, so the exclusion spans every runner
# container on the host rather than just this one.
set -eu

target=${1:?usage: run-privileged-suite.sh <make-target>}

GO_IMAGE=${ACP_GO_PRIVILEGED_IMAGE:-golang:1.26.5-bookworm}
GO_CACHE_VOLUME=acp-go-privileged-gocache

for tool in docker flock; do
	command -v "$tool" >/dev/null 2>&1 || {
		echo "privileged suite requires $tool on the runner" >&2
		exit 1
	}
done

workspace=${GITHUB_WORKSPACE:-$(pwd)}

# The daemon behind the socket is the host's, so every bind mount has to be
# named in host terms. Recover this container's mount table and rewrite the
# workspace path through whichever mount contains it.
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

workspace_host=$best_source${workspace#"$best_destination"}
lock=$best_destination/acp-go-privileged.lock
lock_host=$best_source/acp-go-privileged.lock

echo "::group::privileged suite placement"
echo "target:          $target"
echo "workspace:       $workspace"
echo "workspace(host): $workspace_host"
echo "lock:            $lock -> $lock_host"
echo "runner pid ns:   $(stat -Lc '%i' /proc/self/ns/pid)"
echo "::endgroup::"

# Serialize every privileged suite on this host. The lock file lives on the
# shared work mount, so runner containers on the same host contend on one inode.
: >>"$lock"
exec 9>>"$lock"
echo "waiting for the host privileged-suite lock"
flock 9
echo "holding the host privileged-suite lock"

authority_volume=$(docker volume create)
cleanup() {
	docker volume rm "$authority_volume" >/dev/null 2>&1 || true
	rm -f "$inspect"
}
trap cleanup EXIT HUP INT TERM

docker volume create "$GO_CACHE_VOLUME" >/dev/null

# umask is pinned because the suite asserts on exact directory modes: a runner
# that hands over 0077 turns a 0755 fixture into 0700 and breaks traversal
# cases, and one that hands over 0000 breaks the inverse.
docker run --rm \
	--pid=host \
	--user 0:0 \
	--cap-add SYS_PTRACE \
	--security-opt seccomp=unconfined \
	--security-opt apparmor=unconfined \
	--tmpfs /acp-go-tmp:rw,exec,mode=0755,size=4g \
	--tmpfs /tmp:rw,exec,size=2g \
	--mount "type=volume,source=$authority_volume,target=/var/lib/acp-go/agent-identities" \
	--mount "type=volume,source=$GO_CACHE_VOLUME,target=/gocache" \
	--volume "$workspace_host:/src" \
	--workdir /src \
	--env TMPDIR=/acp-go-tmp \
	--env TMP=/acp-go-tmp \
	--env TEMP=/acp-go-tmp \
	--env GOCACHE=/gocache/build \
	--env GOMODCACHE=/gocache/mod \
	--env GOTOOLCHAIN=local \
	--env HOME=/root \
	"$GO_IMAGE" \
	sh -eu /src/.github/scripts/privileged-suite-entrypoint.sh "$target"
