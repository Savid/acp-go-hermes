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
# What the container must NOT be able to do is change the checkout it is
# testing. /src stays writable because the suites write into it — coverage-check
# leaves coverage.out there — so the module files are pinned individually
# instead: go.mod and go.sum are bind-mounted read-only over the workspace, and
# GOFLAGS carries -mod=readonly. Both, because they fail at different layers. A
# stray -mod=mod on a command line beats GOFLAGS, and it once rewrote a pushed
# go.mod/go.sum in place from inside this container; the read-only mounts make
# that a kernel refusal rather than a silent edit, and the entrypoint proves the
# refusal is armed before it runs anything.
set -eu

target=${1:?usage: run-privileged-suite.sh <make-target>}

# with-privileged-lock.sh owns both the host lock and the workspace host-path
# resolution, so every --pid=host lane serializes on one inode. Re-enter through
# it when this script was invoked directly.
if [ -z "${ACP_GO_PRIVILEGED_LOCK:-}" ]; then
	exec "$(dirname "$0")/with-privileged-lock.sh" "$0" "$target"
fi

GO_IMAGE=${ACP_GO_PRIVILEGED_IMAGE:-golang:1.26.5-bookworm}

# The Go caches persist between runs for speed, but they are keyed per module
# path and never shared across siblings. Every sibling bind-mounts its own
# checkout at the same /src, so one cache namespace for all six gives colliding
# path-derived build- and module-cache keys to six different modules. That has
# already resolved a sibling's import inside another sibling's build
# ("no required module provides package github.com/savid/acp-go-<sibling>") in a
# tree that builds cleanly against a private cache. One volume per module path
# keeps the warm cache and removes the collision domain.
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
module_path=$(sed -n 's/^module[[:space:]]\{1,\}//p' "$repo_root/go.mod" | head -n 1)
[ -n "$module_path" ] || {
	echo "cannot read the module path from $repo_root/go.mod" >&2
	echo "the privileged Go caches are keyed per module and must not fall back to a shared name" >&2
	exit 1
}
GO_CACHE_VOLUME=acp-go-privileged-gocache-$(printf '%s' "$module_path" | tr -c 'A-Za-z0-9_.-' '-')

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
	--volume "$ACP_GO_WORKSPACE_HOST/go.mod:/src/go.mod:ro" \
	--volume "$ACP_GO_WORKSPACE_HOST/go.sum:/src/go.sum:ro" \
	--workdir /src \
	--env TMPDIR=/acp-go-tmp \
	--env TMP=/acp-go-tmp \
	--env TEMP=/acp-go-tmp \
	--env GOCACHE=/gocache/build \
	--env GOMODCACHE=/gocache/mod \
	--env GOFLAGS=-mod=readonly \
	--env GOTOOLCHAIN=local \
	--env HOME=/root \
	"$GO_IMAGE" \
	sh -eu /src/.github/scripts/privileged-suite-entrypoint.sh "$target"
