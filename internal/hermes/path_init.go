package hermes

import (
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	hermesPathInitFileName    = ".acp-go-hermes-path-init.sh"
	hermesPathInitEnvironment = "ACP_GO_HERMES_PATH_DIR_"
	hermesPathInitCountEnv    = hermesPathInitEnvironment + "COUNT"
	hermesPathInitWindowsEnv  = hermesPathInitEnvironment + "WINDOWS"
	hermesBashEnvKey          = "BASH_ENV"
	hermesShellEnvKey         = "ENV"
)

// hermesPathInitScript is sourced by Bash through its supported BASH_ENV hook.
// Hermes then prepends its configured rc-file sources to the snapshot command,
// so a temporary private DEBUG trap reasserts the complete requested prefix
// after those sources. The prefix comparison makes the hook idempotent with the
// serve-process PATH while preserving duplicates explicitly requested by the
// caller. Windows native paths are converted directly to Git Bash form. Only
// PATH is exported: BASH_ENV and every carrier are removed before the snapshot.
var hermesPathInitScript = []byte(`if [ -z "${BASH_EXECUTION_STRING-}" ]; then
  # An installed hermes executable may itself be a Bash script which execs the
  # Python gateway. Preserve the process-local carrier through that launcher;
  # the later Bash -c snapshot is the first shell authorized to consume it.
  return 0
fi
set +u
unset BASH_ENV
__acp_go_hermes_path_count=${ACP_GO_HERMES_PATH_DIR_COUNT-0}
__acp_go_hermes_path_windows=${ACP_GO_HERMES_PATH_DIR_WINDOWS-0}
case "$__acp_go_hermes_path_count" in
  ''|*[!0-9]*) __acp_go_hermes_path_count=0 ;;
esac
if [ "$__acp_go_hermes_path_count" -eq 0 ]; then
  unset ACP_GO_HERMES_PATH_DIR_COUNT ACP_GO_HERMES_PATH_DIR_WINDOWS
  unset __acp_go_hermes_path_count __acp_go_hermes_path_windows
  return 0
fi
__acp_go_hermes_path_prefix=
while [ "$__acp_go_hermes_path_count" -gt 0 ]; do
  __acp_go_hermes_path_name="ACP_GO_HERMES_PATH_DIR_${__acp_go_hermes_path_count}"
  __acp_go_hermes_path_dir=${!__acp_go_hermes_path_name}
  if [ "$__acp_go_hermes_path_windows" = 1 ]; then
    __acp_go_hermes_path_drive=${__acp_go_hermes_path_dir:0:2}
    case "$__acp_go_hermes_path_drive" in
      [A-Za-z]:)
        __acp_go_hermes_path_drive=${__acp_go_hermes_path_drive%:}
        case "$__acp_go_hermes_path_drive" in
          A|a) __acp_go_hermes_path_drive=a ;; B|b) __acp_go_hermes_path_drive=b ;;
          C|c) __acp_go_hermes_path_drive=c ;; D|d) __acp_go_hermes_path_drive=d ;;
          E|e) __acp_go_hermes_path_drive=e ;; F|f) __acp_go_hermes_path_drive=f ;;
          G|g) __acp_go_hermes_path_drive=g ;; H|h) __acp_go_hermes_path_drive=h ;;
          I|i) __acp_go_hermes_path_drive=i ;; J|j) __acp_go_hermes_path_drive=j ;;
          K|k) __acp_go_hermes_path_drive=k ;; L|l) __acp_go_hermes_path_drive=l ;;
          M|m) __acp_go_hermes_path_drive=m ;; N|n) __acp_go_hermes_path_drive=n ;;
          O|o) __acp_go_hermes_path_drive=o ;; P|p) __acp_go_hermes_path_drive=p ;;
          Q|q) __acp_go_hermes_path_drive=q ;; R|r) __acp_go_hermes_path_drive=r ;;
          S|s) __acp_go_hermes_path_drive=s ;; T|t) __acp_go_hermes_path_drive=t ;;
          U|u) __acp_go_hermes_path_drive=u ;; V|v) __acp_go_hermes_path_drive=v ;;
          W|w) __acp_go_hermes_path_drive=w ;; X|x) __acp_go_hermes_path_drive=x ;;
          Y|y) __acp_go_hermes_path_drive=y ;; Z|z) __acp_go_hermes_path_drive=z ;;
        esac
        __acp_go_hermes_path_rest=${__acp_go_hermes_path_dir:2}
        case "$__acp_go_hermes_path_rest" in
          [\\/]*) __acp_go_hermes_path_rest=${__acp_go_hermes_path_rest//\\//}
                  __acp_go_hermes_path_dir="/${__acp_go_hermes_path_drive}${__acp_go_hermes_path_rest}" ;;
          *) __acp_go_hermes_path_dir= ;;
        esac ;;
      '\\'|'//') __acp_go_hermes_path_dir=${__acp_go_hermes_path_dir//\\//} ;;
      *) __acp_go_hermes_path_dir= ;;
    esac
  fi
  if [ -n "$__acp_go_hermes_path_dir" ]; then
    __acp_go_hermes_path_prefix="${__acp_go_hermes_path_dir}${__acp_go_hermes_path_prefix:+:${__acp_go_hermes_path_prefix}}"
  fi
  unset "$__acp_go_hermes_path_name"
  __acp_go_hermes_path_count=$((__acp_go_hermes_path_count - 1))
done
unset ACP_GO_HERMES_PATH_DIR_COUNT
unset ACP_GO_HERMES_PATH_DIR_WINDOWS
unset __acp_go_hermes_path_count __acp_go_hermes_path_windows __acp_go_hermes_path_name __acp_go_hermes_path_dir __acp_go_hermes_path_drive __acp_go_hermes_path_rest
_acp_go_hermes_path_restore() {
  __acp_go_hermes_path_wrapped=":${PATH}:"
  __acp_go_hermes_path_needle=":${__acp_go_hermes_path_prefix}:"
  __acp_go_hermes_path_without=${__acp_go_hermes_path_wrapped/"$__acp_go_hermes_path_needle"/:}
  __acp_go_hermes_path_without=${__acp_go_hermes_path_without#:}
  __acp_go_hermes_path_without=${__acp_go_hermes_path_without%:}
  PATH="${__acp_go_hermes_path_prefix}${__acp_go_hermes_path_without:+:${__acp_go_hermes_path_without}}"
  export PATH
}
_acp_go_hermes_path_restore
trap '_acp_go_hermes_path_restore' DEBUG
`)

func hermesPathInitWrite(home string) seedWrite {
	return seedWrite{
		relative: hermesPathInitFileName,
		target:   filepath.Join(home, hermesPathInitFileName),
		bytes:    hermesPathInitScript,
	}
}

// installHermesPathCarrier removes every untrusted spelling of the shell hooks
// and the managed namespace, then installs validated directory values in
// numbered slots. Values remain data rather than shell source, so spaces and
// shell metacharacters in an absolute directory cannot change the init script
// syntax.
func installHermesPathCarrier(environment []string, home string, dirs []string) []string {
	managed := make([]string, 0, len(environment)+len(dirs)+3)
	for _, entry := range environment {
		key, _, ok := strings.Cut(entry, "=")
		if ok && (processEnvironmentKeyMatches(key, hermesBashEnvKey) ||
			processEnvironmentKeyMatches(key, hermesShellEnvKey) || hermesPathCarrierEnvironmentKey(key)) {
			continue
		}

		managed = append(managed, entry)
	}

	if len(dirs) == 0 {
		return managed
	}

	managed = append(managed,
		hermesBashEnvKey+"="+filepath.ToSlash(filepath.Join(home, hermesPathInitFileName)),
		hermesPathInitCountEnv+"="+strconv.Itoa(len(dirs)),
	)

	if Platform == processPlatformWindows {
		managed = append(managed, hermesPathInitWindowsEnv+"=1")
	}

	for index, dir := range dirs {
		managed = append(managed, fmt.Sprintf("%s%d=%s", hermesPathInitEnvironment, index+1, dir))
	}

	return managed
}

func hermesPathCarrierEnvironmentKey(key string) bool {
	return strings.HasPrefix(strings.ToUpper(key), hermesPathInitEnvironment)
}

func validateSessionEnvironmentNoPath(env map[string]string) error {
	for key := range env {
		if processEnvironmentKeyMatches(key, envPath) {
			return errors.New("session environment must not contain PATH")
		}
	}

	return validatePathCarrierEnvironment(env)
}

func validatePathCarrierEnvironment(env map[string]string) error {
	for key := range env {
		switch {
		case processEnvironmentKeyMatches(key, hermesBashEnvKey):
			return errors.New("environment must not contain BASH_ENV")
		case processEnvironmentKeyMatches(key, hermesShellEnvKey):
			return errors.New("environment must not contain ENV")
		case hermesPathCarrierEnvironmentKey(key):
			return fmt.Errorf("environment must not contain adapter-managed path variable %q", key)
		}
	}

	return nil
}
