#!/usr/bin/env bash
set -euo pipefail

provider=hermes
packages=(./internal/hermes)
selector='^(TestDarwinSharedHomeFilesystemLocalityAllowlist|TestOrdinaryExecutableRulesMatchThisPlatform|TestOrdinaryEnvironmentInheritsOnlyTheAllowlist|TestOrdinaryNativeNaturalCompletionIsNotReclassified|TestOrdinaryNativeWaitCancellationDetachesUntilRevoke)$'

# The hosted image supplies MinGW-w64; Go's race runtime requires its
# synchronization library (https://go.dev/doc/articles/race_detector).
if [ "${RUNNER_OS:-}" = Windows ]; then
  export PATH="/c/mingw64/bin:$PATH"
  export CGO_ENABLED=1
  export CC="${CC:-gcc}"
  synchronization="$("$CC" --print-file-name libsynchronization.a)"
  [ "$synchronization" != libsynchronization.a ] && [ -f "$synchronization" ] || {
    echo 'Windows race proof requires a MinGW-w64 compiler with libsynchronization.a' >&2
    exit 1
  }
fi

result_log="$(mktemp)"
cleanup() { rm -f "$result_log"; }
trap cleanup EXIT HUP INT TERM

expected=0
for package in "${packages[@]}"; do
  listed="$(go test -count=1 -timeout="${GO_TEST_TIMEOUT:-10m}" -list "$selector" "$package")"
  discovered="$(printf '%s\n' "$listed" | grep -Ec '^Test' || true)"
  [ "$discovered" -gt 0 ] || {
    printf '%s: selector discovered no tests in %s\n' "$provider" "$package" >&2
    exit 1
  }
  expected="$((expected + discovered))"
done

status=0
go test -race -count=1 -json -timeout="${GO_TEST_TIMEOUT:-10m}" -run "$selector" "${packages[@]}" >"$result_log" || status="$?"
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
