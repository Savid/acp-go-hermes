#!/usr/bin/env bash
set -euo pipefail

provider=hermes
packages=(./internal/hermes)
selector='^Test(ProcessIsolationOmissionResolvesAndRunsOnWindows|WindowsChildResolvesTheConflictingCaseEnvironmentThisAdapterBuilt|WindowsInheritedBlockWithTwoPathSpellingsResolvesLastWins|ExecutableExtensionListFollowsPathext|WindowsOrdinaryRules.*|EnvValueFoldReadsAnInheritedEnvironmentBlock|OrdinaryExecutableRulesMatchThisPlatform|OrdinaryEnvironment.*|AmbientEnvironmentSnapshotFoldsWindowsSpellings|ProcessEnvironmentPhasesFoldWindowsNames|OrdinaryExecutableResolutionAcceptsAnOrdinaryShellEnvironment|OrdinaryDirectChildCompletion.*|ScrubOrdinaryEnvironmentKeyIsCaseInsensitive|NonUnixContainmentRefusesEveryStrongerBoundary|ContainmentOperationsAreDarwinOnly|ValidateProcessContainmentGatesOnlyTheDarwinBackend)$'

result_log="$(mktemp)"
cleanup() { rm -f "$result_log"; }
trap cleanup EXIT HUP INT TERM

expected=0
for package in "${packages[@]}"; do
  discovered="$(go test -list "$selector" "$package" | grep -Ec '^Test' || true)"
  [ "$discovered" -gt 0 ] || {
    printf '%s: selector discovered no tests in %s\n' "$provider" "$package" >&2
    exit 1
  }
  expected="$((expected + discovered))"
done

status=0
if [ "$provider" = amp ]; then
  go test -race -count=1 -json -timeout="${GO_TEST_TIMEOUT:-40m}" -run "$selector" "${packages[@]}" >"$result_log" || status="$?"
else
  go test -count=1 -json -run "$selector" "${packages[@]}" >"$result_log" || status="$?"
fi
cat "$result_log"
[ "$status" -eq 0 ] || exit "$status"

passed="$(grep -Ec '"Action":"pass","Package":"[^"]+","Test":"Test[^/"]*"' "$result_log" || true)"
skipped="$(grep -Ec '"Action":"skip","Package":"[^"]+","Test":"Test[^"]+"' "$result_log" || true)"
[ "$passed" -eq "$expected" ] || {
  printf '%s: selected tests passed %s of %s\n' "$provider" "$passed" "$expected" >&2
  exit 1
}
[ "$skipped" -eq 0 ] || {
  printf '%s: selected tests skipped %s cases\n' "$provider" "$skipped" >&2
  exit 1
}
