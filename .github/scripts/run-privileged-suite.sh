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
set -eu

target=${1:?usage: run-privileged-suite.sh <make-target>}

# with-privileged-lock.sh owns both the host lock and the workspace host-path
# resolution, so every --pid=host lane serializes on one inode. Re-enter through
# it when this script was invoked directly.
if [ -z "${ACP_GO_PRIVILEGED_LOCK:-}" ]; then
	exec "$(dirname "$0")/with-privileged-lock.sh" "$0" "$target"
fi

GO_IMAGE=${ACP_GO_PRIVILEGED_IMAGE:-golang:1.26.5-bookworm}
GO_CACHE_VOLUME=acp-go-privileged-gocache

authority_volume=$(docker volume create)
cleanup() { docker volume rm "$authority_volume" >/dev/null 2>&1 || true; }
trap cleanup EXIT HUP INT TERM

docker volume create "$GO_CACHE_VOLUME" >/dev/null

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
	--volume "$ACP_GO_WORKSPACE_HOST:/src" \
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
