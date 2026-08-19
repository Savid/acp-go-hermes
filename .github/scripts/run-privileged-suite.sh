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
# suite in a container started with --pid=host and root, with one fresh local
# volume per shard for its temp root and one for its authority tree.
#
# This file is family-owned and byte-identical across every sibling, so the
# strictest sibling sets the mount shape for all of them. Shared-home tests
# inherit TMPDIR=/acp-go-tmp and refuse a tmpfs residence outright — they fail,
# they do not degrade — so that mount is a local volume. /tmp stays tmpfs for
# fixtures that name it directly. The entrypoint proves both properties of
# TMPDIR before any target runs, because a volume only inherits whatever
# filesystem and mount options the daemon's data root happens to have.
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

case "$target" in
coverage-check | test-trusted-supervisor) ;;
*)
	echo "unsupported privileged suite target $target" >&2
	exit 1
	;;
esac

script_dir=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
. "$script_dir/initial-pid-namespace.sh"

is_initial_pid_namespace_inode 4026531836 || {
	echo "initial PID namespace parser refused the exact Linux initial inode" >&2
	exit 1
}
if is_initial_pid_namespace_inode 4026531835; then
	echo "initial PID namespace parser accepted a non-initial inode" >&2
	exit 1
fi

# with-privileged-lock.sh owns both the host lock and the workspace host-path
# resolution, so every --pid=host lane serializes on one inode. Re-enter through
# it when this script was invoked directly.
if [ -z "${ACP_GO_PRIVILEGED_LOCK:-}" ]; then
	exec "$(dirname "$0")/with-privileged-lock.sh" "$0" "$target"
fi

# The Go caches persist between runs for speed, but they are keyed per module
# path and never shared across siblings. Every sibling bind-mounts its own
# checkout at the same /src, so one cache namespace for all six gives colliding
# path-derived build- and module-cache keys to six different modules. That has
# already resolved a sibling's import inside another sibling's build
# ("no required module provides package github.com/savid/acp-go-<sibling>") in a
# tree that builds cleanly against a private cache. One volume per module path
# keeps the warm cache and removes the collision domain.
repo_root=$(cd "$(dirname "$0")/../.." && pwd)
module_path=$(cd "$repo_root" && go list -mod=readonly -m)

# The container toolchain is the checkout's own Go directive, never a second
# literal. A hardcoded tag drifts silently the moment the pin moves, and the
# suite would then prove coverage under a toolchain the module never compiles
# with. go.mod is the sole in-repo copy of that pin, so read it here.
go_directive=$(awk '$1 == "go" { print $2; exit }' "$repo_root/go.mod")
printf '%s\n' "$go_directive" | grep -qE '^[0-9]+\.[0-9]+\.[0-9]+$' || {
	echo "go.mod directive $go_directive is not an exact three-component Go version" >&2
	exit 1
}
GO_IMAGE=${ACP_GO_PRIVILEGED_IMAGE:-golang:$go_directive-bookworm}
base_provider_required='TrustedSupervisor SupervisorGuardianSIGKILL SupervisorGuardianSIGKILLBeforeNativeLaunchRefusesStartAndCompletesAfterECHILD SupervisorLivenessSIGKILL GeneratedNative BorrowedIdentityAdoption BorrowedDomainAdoption BorrowedDisposition AgentIdentityLock AgentStandalone AuthorityDomain IdentityDisposition CommandCreatorThread SecurityLimits ProcessIsolationActual'
case "$module_path" in
github.com/savid/acp-go-amp)
	provider_name=amp
	root_required='GeneratedNative'
	provider_required=$base_provider_required
	;;
github.com/savid/acp-go-claude)
	provider_name=claude
	root_required='GeneratedNative ProcessIsolationActual'
	provider_required=$base_provider_required
	;;
github.com/savid/acp-go-codex)
	provider_name=codex
	root_required='GeneratedNative'
	provider_required="$base_provider_required PersistentProof SupervisorConfigIsSealed ProviderCreator"
	;;
github.com/savid/acp-go-hermes)
	provider_name=hermes
	root_required='NativeOwnedDirectory'
	provider_required=$base_provider_required
	;;
github.com/savid/acp-go-opencode)
	provider_name=opencode
	root_required='NativeOwnedDirectory'
	provider_required="$base_provider_required PersistentProof SupervisorConfigIsSealed ProviderCreator"
	;;
github.com/savid/acp-go-pi)
	provider_name=pi
	root_required='GeneratedNative'
	provider_required=$base_provider_required
	;;
*)
	echo "unrecognized privileged-suite module $module_path" >&2
	exit 1
	;;
esac
[ -n "$root_required" ] && [ -n "$provider_required" ] || {
	echo "privileged-suite class map is incomplete for $module_path" >&2
	exit 1
}
GO_CACHE_VOLUME=acp-go-privileged-gocache-$(printf '%s' "$module_path" | tr -c 'A-Za-z0-9_.-' '-')

resource_nonce=$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')
case "$resource_nonce" in
'' | *[!0-9a-f]*)
	echo "privileged-suite resource nonce is not lowercase hexadecimal" >&2
	exit 1
	;;
esac
[ "${#resource_nonce}" -eq 32 ] || {
	echo "privileged-suite resource nonce has the wrong length" >&2
	exit 1
}

resource_run=acp-go-privileged-$provider_name-$target-$resource_nonce
resource_label=com.savid.acp-go.privileged-run
root_authority_volume=$resource_run-root-authority
provider_authority_volume=$resource_run-provider-authority
private_authority_volume=$resource_run-private-authority
root_temp_volume=$resource_run-root-tmp
provider_temp_volume=$resource_run-provider-tmp
root_container=$resource_run-root
provider_container=$resource_run-provider
private_container=$resource_run-private-pid1
root_log="$repo_root/.tmp/privileged-$target-root-$$.log"
provider_log="$repo_root/.tmp/privileged-$target-provider-$$.log"
private_log="$repo_root/.tmp/privileged-$target-private-pid1-$$.log"
root_coverage=".tmp/coverage-$target-root-$$.out"
provider_coverage=".tmp/coverage-$target-provider-$$.out"
root_job=
provider_job=

remove_container() {
	container_name=$1
	if ! container_run=$(docker container inspect --format "{{ index .Config.Labels \"$resource_label\" }}" "$container_name" 2>/dev/null); then
		docker info >/dev/null 2>&1 || return 1

		return 0
	fi
	if [ "$container_run" != "$resource_run" ]; then
		echo "refusing to remove container $container_name with foreign privileged-run label $container_run" >&2

		return 1
	fi

	docker container rm --force "$container_name" >/dev/null
}

remove_volume() {
	volume_name=$1
	if ! volume_run=$(docker volume inspect --format "{{ index .Labels \"$resource_label\" }}" "$volume_name" 2>/dev/null); then
		docker info >/dev/null 2>&1 || return 1

		return 0
	fi
	if [ "$volume_run" != "$resource_run" ]; then
		echo "refusing to remove volume $volume_name with foreign privileged-run label $volume_run" >&2

		return 1
	fi

	docker volume rm "$volume_name" >/dev/null
}

cleanup() {
	status=$?
	trap - EXIT HUP INT TERM
	cleanup_status=0
	for job in "$root_job" "$provider_job"; do
		if [ -n "$job" ]; then
			kill "$job" 2>/dev/null || true
		fi
	done
	for container_name in "$root_container" "$provider_container" "$private_container"; do
		remove_container "$container_name" || cleanup_status=1
	done
	for volume_name in \
		"$root_authority_volume" "$provider_authority_volume" "$private_authority_volume" \
		"$root_temp_volume" "$provider_temp_volume"; do
		remove_volume "$volume_name" || cleanup_status=1
	done
	rm -f "$root_log" "$provider_log" "$private_log" "$repo_root/$root_coverage" "$repo_root/$provider_coverage" \
		"$repo_root/coverage.out.tmp.$$"
	if [ "$status" -eq 0 ] && [ "$cleanup_status" -ne 0 ]; then
		status=$cleanup_status
	fi

	exit "$status"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

create_volume() {
	volume_name=$1
	volume_role=$2
	docker volume create \
		--driver local \
		--label "$resource_label=$resource_run" \
		--label "com.savid.acp-go.privileged-role=$volume_role" \
		"$volume_name" >/dev/null
	volume_run=$(docker volume inspect --format "{{ index .Labels \"$resource_label\" }}" "$volume_name")
	[ "$volume_run" = "$resource_run" ] || {
		echo "privileged-suite volume $volume_name did not retain its exact run label" >&2
		exit 1
	}
	volume_driver=$(docker volume inspect --format '{{ .Driver }}' "$volume_name")
	[ "$volume_driver" = local ] || {
		echo "privileged-suite volume $volume_name uses driver $volume_driver, want local" >&2
		exit 1
	}
}

create_volume "$root_authority_volume" root-authority
create_volume "$provider_authority_volume" provider-authority
create_volume "$private_authority_volume" private-authority
create_volume "$root_temp_volume" root-tmp
create_volume "$provider_temp_volume" provider-tmp

# The cache outlives one run, so it takes the family label under a persistent
# role rather than this run's name, and cleanup never touches it. It is still
# driver-pinned: a cache on a remote driver would make every build depend on a
# volume plugin the suite never validated.
docker volume create \
	--driver local \
	--label "$resource_label=persistent" \
	--label "com.savid.acp-go.privileged-role=go-cache" \
	"$GO_CACHE_VOLUME" >/dev/null
cache_driver=$(docker volume inspect --format '{{ .Driver }}' "$GO_CACHE_VOLUME")
[ "$cache_driver" = local ] || {
	echo "privileged-suite Go cache volume uses driver $cache_driver, want local" >&2
	exit 1
}
mkdir -p "$repo_root/.tmp"

module_packages=$(cd "$repo_root" && go list -mod=readonly ./...)
provider_package="$module_path/internal/$provider_name"
root_packages=
package_count=0
provider_count=0
root_count=0
for package in $module_packages; do
	package_count=$((package_count + 1))
	if [ "$package" = "$provider_package" ]; then
		provider_count=$((provider_count + 1))
		continue
	fi
	root_packages="$root_packages $package"
	root_count=$((root_count + 1))
done
root_packages=${root_packages# }
[ "$provider_count" -eq 1 ] || {
	echo "privileged package discovery found $provider_count copies of $provider_package, want exactly 1" >&2
	exit 1
}
[ -n "$root_packages" ] && [ "$root_count" -gt 0 ] || {
	echo "privileged root shard discovered no packages" >&2
	exit 1
}
[ "$package_count" -eq $((root_count + provider_count)) ] || {
	echo "privileged package partition is incomplete" >&2
	exit 1
}

# A normal Docker container command is PID 1 in a fresh private PID namespace.
# That topology is valid under the production binder policy but is forbidden in
# this external proof lane. Exercise the real entrypoint without --pid=host and
# require its namespace refusal before the requested make target can run.
private_status=0
docker run \
	--name "$private_container" \
	--label "$resource_label=$resource_run" \
	--user 0:0 \
	--mount "type=volume,source=$private_authority_volume,target=/var/lib/acp-go/agent-identities" \
	--volume "$ACP_GO_WORKSPACE_HOST:/src" \
	--volume "$ACP_GO_WORKSPACE_HOST/go.mod:/src/go.mod:ro" \
	--volume "$ACP_GO_WORKSPACE_HOST/go.sum:/src/go.sum:ro" \
	--workdir /src \
	--env TMPDIR=/tmp \
	--env ACP_GO_PRIVILEGED_SHARD=private-pid1-refusal \
	--env "ACP_GO_PRIVILEGED_MODULE=$module_path" \
	--env ACP_GO_PRIVILEGED_PACKAGES=./... \
	--env ACP_GO_PRIVILEGED_COVERAGE_OUT=.tmp/coverage-private-pid1-must-not-run.out \
	--env ACP_GO_PRIVILEGED_REQUIRED_CLASSES=private-pid1-must-not-run \
	"$GO_IMAGE" \
	sh -eu /src/.github/scripts/privileged-suite-entrypoint.sh "$target" >"$private_log" 2>&1 || private_status=$?

echo "::group::privileged private-PID1 refusal fixture"
cat "$private_log"
echo "::endgroup::"
[ "$private_status" -ne 0 ] || {
	echo "privileged entrypoint admitted a private PID namespace whose command was PID 1" >&2
	exit 1
}
grep -Eq '^entrypoint pid:[[:space:]]+1$' "$private_log" || {
	echo "private PID namespace refusal fixture did not execute as namespace PID 1" >&2
	exit 1
}
grep -q 'privileged suite is not in the initial PID namespace' "$private_log" || {
	echo "private PID namespace fixture failed for a reason other than the namespace proof gate" >&2
	exit 1
}

run_shard() {
	shard=$1
	authority_volume=$2
	packages=$3
	coverage=$4
	required=$5
	temp_volume=$6
	container_name=$7

	docker run \
		--name "$container_name" \
		--label "$resource_label=$resource_run" \
		--pid=host \
		--user 0:0 \
		--cap-add SYS_PTRACE \
		--security-opt seccomp=unconfined \
		--security-opt apparmor=unconfined \
		--tmpfs /tmp:rw,exec,size=2g \
		--mount "type=volume,source=$temp_volume,target=/acp-go-tmp,volume-nocopy" \
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
		--env "ACP_GO_PRIVILEGED_SHARD=$shard" \
		--env "ACP_GO_PRIVILEGED_MODULE=$module_path" \
		--env "ACP_GO_PRIVILEGED_PACKAGES=$packages" \
		--env "ACP_GO_PRIVILEGED_COVERAGE_OUT=$coverage" \
		--env "ACP_GO_PRIVILEGED_REQUIRED_CLASSES=$required" \
		"$GO_IMAGE" \
		sh -eu /src/.github/scripts/privileged-suite-entrypoint.sh "$target"
}

run_shard root "$root_authority_volume" "$root_packages" "$root_coverage" "$root_required" "$root_temp_volume" "$root_container" >"$root_log" 2>&1 &
root_job=$!
run_shard provider "$provider_authority_volume" "$provider_package" "$provider_coverage" "$provider_required" "$provider_temp_volume" "$provider_container" >"$provider_log" 2>&1 &
provider_job=$!

root_status=0
provider_status=0
wait "$root_job" || root_status=$?
root_job=
wait "$provider_job" || provider_status=$?
provider_job=

echo "::group::privileged root shard"
cat "$root_log"
echo "::endgroup::"
echo "::group::privileged provider shard"
cat "$provider_log"
echo "::endgroup::"

[ "$root_status" -eq 0 ] || exit "$root_status"
[ "$provider_status" -eq 0 ] || exit "$provider_status"

if [ "$target" = coverage-check ]; then
	for profile in "$repo_root/$root_coverage" "$repo_root/$provider_coverage"; do
		[ "$(sed -n '1p' "$profile")" = 'mode: atomic' ] || {
			echo "privileged coverage shard $profile is not atomic" >&2
			exit 1
		}
	done

	merged="$repo_root/coverage.out.tmp.$$"
	{
		echo 'mode: atomic'
		sed '1d' "$repo_root/$root_coverage"
		sed '1d' "$repo_root/$provider_coverage"
	} >"$merged"
	mv "$merged" "$repo_root/coverage.out"
	(cd "$repo_root" && make _privileged-coverage-gate)
fi
